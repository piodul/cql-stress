use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use tokio::fs::File;
use tokio::time::Instant;

use cql_stress::configuration::OperationContext;
use cql_stress::sharded_stats;

use crate::args::ScyllaBenchArgs;
use crate::gocompat::strconv::format_duration;
use crate::histogram_log_writer::{HistogramLogOptions, HistogramLogWriter};

pub type ShardedStats = sharded_stats::ShardedStats<StatsFactory>;

pub struct StatsFactory {
    measure_latency: bool,
    latency_sig_fig: u8,
    latency_resolution: u64,
}

impl StatsFactory {
    pub(crate) fn new(args: &ScyllaBenchArgs) -> Self {
        StatsFactory {
            measure_latency: args.measure_latency,
            latency_sig_fig: args.hdr_latency_sig_fig as u8,
            latency_resolution: args.hdr_latency_resolution,
        }
    }

    fn create_histogram(&self) -> Histogram<u64> {
        Histogram::new(self.latency_sig_fig).unwrap()
    }
}

impl sharded_stats::StatsFactory for StatsFactory {
    type Stats = Stats;

    fn create(&self) -> Stats {
        Stats {
            operations: 0,
            clustering_rows: 0,
            errors: 0,
            latencies: self.measure_latency.then(|| LatencyHistograms {
                raw: self.create_histogram(),
                co_fixed: self.create_histogram(),
            }),

            latency_resolution: self.latency_resolution,
        }
    }
}

pub struct Stats {
    pub operations: u64,
    pub clustering_rows: u64,
    pub errors: u64,

    pub latencies: Option<LatencyHistograms>,

    // Do not change in workloads, this should be constant
    pub latency_resolution: u64,
}

pub struct LatencyHistograms {
    // Latency, measured both with and without the coordinated omission fix
    pub raw: Histogram<u64>,
    pub co_fixed: Histogram<u64>,
}

impl sharded_stats::Stats for Stats {
    fn clear(&mut self) {
        self.operations = 0;
        self.clustering_rows = 0;
        self.errors = 0;
        if let Some(ls) = &mut self.latencies {
            ls.raw.reset();
            ls.co_fixed.reset();
        }
    }

    fn combine(&mut self, other: &Self) {
        self.operations += other.operations;
        self.clustering_rows += other.clustering_rows;
        self.errors += other.errors;
        if let (Some(ls1), Some(ls2)) = (&mut self.latencies, &other.latencies) {
            ls1.raw.add(&ls2.raw).unwrap();
            ls1.co_fixed.add(&ls2.co_fixed).unwrap();
        }
    }
}

impl Stats {
    pub fn account_op(&mut self, ctx: &OperationContext, result: &Result<()>, rows: usize) {
        self.operations += 1;
        match result {
            Ok(()) => {
                self.clustering_rows += rows as u64;
                self.account_latency(ctx);
            }
            Err(_) => {
                self.errors += 1;
            }
        }
    }

    pub fn account_latency(&mut self, ctx: &OperationContext) {
        if let Some(ls) = &mut self.latencies {
            let now = Instant::now();
            let _ = ls
                .raw
                .record((now - ctx.actual_start_time).as_nanos() as u64 / self.latency_resolution);
            let _ = ls.co_fixed.record(
                (now - ctx.scheduled_start_time).as_nanos() as u64 / self.latency_resolution,
            );
        }
    }

    pub fn get_histogram(&self, typ: LatencyType) -> Option<&Histogram<u64>> {
        let ls = self.latencies.as_ref()?;
        let histogram = match typ {
            LatencyType::Raw => &ls.raw,
            LatencyType::AdjustedForCoordinatorOmission => &ls.co_fixed,
        };
        Some(histogram)
    }
}

#[derive(Clone, Copy)]
pub enum LatencyType {
    Raw,
    AdjustedForCoordinatorOmission,
}

type HistogramWriter = HistogramLogWriter<File>;

// TODO: Should we have two impls, one with latency and another without?
pub struct StatsPrinter {
    start_time: Instant,
    previous_time: Instant,
    latency_type: Option<LatencyType>,
    histogram_writer: Option<HistogramWriter>,
}

impl StatsPrinter {
    pub async fn new(
        latency_type: Option<LatencyType>,
        latency_file_name: Option<&str>,
    ) -> Result<Self> {
        let histogram_writer = if let Some(latency_file_name) = latency_file_name {
            Some(init_hdr_log_writer(latency_file_name).await?)
        } else {
            None
        };

        let now = Instant::now();
        Ok(Self {
            start_time: now,
            previous_time: now,
            latency_type,
            histogram_writer,
        })
    }

    pub fn print_header(&self, out: &mut impl Write) -> Result<()> {
        if self.latency_type.is_some() {
            writeln!(
                out,
                "{:9} {:>7} {:>7} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
                "time",
                "ops/s",
                "rows/s",
                "errors",
                "max",
                "99.9th",
                "99th",
                "95th",
                "90th",
                "median",
                "mean"
            )?;
        } else {
            writeln!(
                out,
                "{:6} {:>7} {:>7} {:>6}",
                "time", "ops/s", "rows/s", "errors",
            )?;
        }

        Ok(())
    }

    pub async fn print_partial(&mut self, stats: &Stats, out: &mut impl Write) -> Result<()> {
        let now = Instant::now();
        let time = now - self.start_time;

        if let Some(typ) = self.latency_type {
            let histogram = stats.get_histogram(typ).unwrap();

            let to_duration =
                |d: u64| -> Duration { Duration::from_nanos(d * stats.latency_resolution) };

            let p50 = to_duration(histogram.value_at_quantile(0.5));
            let p90 = to_duration(histogram.value_at_quantile(0.9));
            let p95 = to_duration(histogram.value_at_quantile(0.95));
            let p99 = to_duration(histogram.value_at_quantile(0.99));
            let p999 = to_duration(histogram.value_at_quantile(0.999));
            let max = to_duration(histogram.max());
            let mean = to_duration(histogram.mean() as u64);
            writeln!(
                out,
                "{:9} {:>7} {:>7} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
                format_duration(time),
                stats.operations,
                stats.clustering_rows,
                stats.errors,
                format_duration(max),
                format_duration(p999),
                format_duration(p99),
                format_duration(p95),
                format_duration(p90),
                format_duration(p50),
                format_duration(mean),
            )?;
        } else {
            writeln!(
                out,
                "{:6} {:>7} {:>7} {:>6}",
                format_duration(time),
                stats.operations,
                stats.clustering_rows,
                stats.errors,
            )?;
        }

        if let (Some(latencies), Some(writer)) = (&stats.latencies, &mut self.histogram_writer) {
            let prev_time = self.previous_time - self.start_time;
            write_to_latencies_file(
                writer,
                latencies,
                prev_time.as_secs_f64()..time.as_secs_f64(),
            )
            .await?;
        }

        self.previous_time = now;

        Ok(())
    }

    pub fn print_final(
        &self,
        stats: &Stats,
        errors: &[anyhow::Error],
        out: &mut impl Write,
    ) -> Result<()> {
        let time = Instant::now() - self.start_time;
        writeln!(out)?;
        writeln!(out, "Results:")?;
        writeln!(out, "Time (avg):\t{}", format_duration(time))?;
        writeln!(out, "Total ops:\t{}", stats.operations)?;
        writeln!(out, "Total rows:\t{}", stats.clustering_rows)?;
        if stats.errors != 0 {
            writeln!(out, "Total errors:\t{}", stats.errors)?;
        }

        let ops_per_second = stats.operations as f64 / time.as_secs_f64();
        writeln!(out, "Operations/s:\t{}", ops_per_second)?;

        let rows_per_second = stats.clustering_rows as f64 / time.as_secs_f64();
        writeln!(out, "Rows/s:\t\t{}", rows_per_second)?;

        if let Some(ls) = &stats.latencies {
            self.print_final_latency_histogram("raw latency", &ls.raw, out)?;
            self.print_final_latency_histogram("c-o fixed latency", &ls.co_fixed, out)?;
        }

        if !errors.is_empty() {
            writeln!(
                out,
                "\nFollowing critical errors were caught during the run:"
            )?;
            for err in errors {
                // The {:#} syntax makes sure that the error is printed in one line
                writeln!(out, "    {:#}", err)?;
            }
        }

        Ok(())
    }

    fn print_final_latency_histogram(
        &self,
        name: &str,
        latency: &Histogram<u64>,
        out: &mut impl Write,
    ) -> Result<()> {
        // TODO: Use non-shortened version of the format_duration
        writeln!(out, "{}:", name)?;

        let p50 = Duration::from_nanos(latency.value_at_quantile(0.5));
        let p90 = Duration::from_nanos(latency.value_at_quantile(0.9));
        let p95 = Duration::from_nanos(latency.value_at_quantile(0.95));
        let p99 = Duration::from_nanos(latency.value_at_quantile(0.99));
        let p999 = Duration::from_nanos(latency.value_at_quantile(99.9));
        let max = Duration::from_nanos(latency.max());
        let mean = Duration::from_nanos(latency.mean() as u64);

        writeln!(out, "  max:\t\t{}", format_duration(max))?;
        writeln!(out, "  99.9th:\t{}", format_duration(p999))?;
        writeln!(out, "  99h:\t\t{}", format_duration(p99))?;
        writeln!(out, "  95h:\t\t{}", format_duration(p95))?;
        writeln!(out, "  90h:\t\t{}", format_duration(p90))?;
        writeln!(out, "  median:\t{}", format_duration(p50))?;
        writeln!(out, "  mean:\t\t{}", format_duration(mean))?;

        Ok(())
    }
}

async fn init_hdr_log_writer(file_name: &str) -> Result<HistogramWriter> {
    let dir_path = Path::new(file_name)
        .parent()
        .ok_or_else(|| anyhow::anyhow!("could not get parent dir of the histogram file path"))?;
    let abs_dir_path = dir_path
        .canonicalize()
        .with_context(|| "Failed to canonicalize the hdr latency file directory")?;
    tokio::fs::create_dir_all(abs_dir_path)
        .await
        .with_context(|| "Failed to recursively create all directories for the hdr latency file")?;

    let file = File::create(file_name).await?;
    let mut log_writer = HistogramLogWriter::new(file);

    log_writer.output_log_format_version().await?;
    log_writer
        .output_comment("Logging op latencies for scylla-bench")
        .await?;

    let base_time = SystemTime::now();
    log_writer.output_base_time(base_time).await?;
    log_writer.output_start_time(base_time).await?;
    log_writer.output_legend().await?;

    Ok(log_writer)
}

async fn write_to_latencies_file(
    writer: &mut HistogramWriter,
    latencies: &LatencyHistograms,
    interval_seconds: Range<f64>,
) -> Result<()> {
    let opts_co_fixed = HistogramLogOptions {
        interval_seconds: interval_seconds.clone(),
        tag: "co-fixed",
    };
    writer
        .output_interval_histogram(&latencies.co_fixed, opts_co_fixed)
        .await?;

    let opts_raw = HistogramLogOptions {
        interval_seconds,
        tag: "raw",
    };
    writer
        .output_interval_histogram(&latencies.raw, opts_raw)
        .await?;

    Ok(())
}
