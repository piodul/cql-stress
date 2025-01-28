use std::iter::Iterator;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use scylla::load_balancing::{DefaultPolicy, LoadBalancingPolicy};
use scylla::statement::Consistency;

use crate::distribution::{parse_distribution, Distribution, Fixed};
use crate::gocompat::flags::{GoValue, ParserBuilder};
use crate::gocompat::strconv::format_duration;
use crate::stats::LatencyType;

// Explicitly marked as `pub(crate)`, because with `pub` rustc doesn't
// complain about fields which are never read
pub(crate) struct ScyllaBenchArgs {
    pub workload: WorkloadType,
    pub consistency_level: Consistency,
    pub replication_factor: i64,
    pub nodes: Vec<String>,
    pub ca_cert_file: String,
    pub client_cert_file: String,
    pub client_key_file: String,
    pub server_name: String,
    pub host_verification: bool,
    pub client_compression: bool,
    pub shard_connection_count: NonZeroUsize,
    pub page_size: i64,
    pub partition_offset: i64,

    // (Timeseries-related parameters)
    pub write_rate: u64,
    pub distribution: TimeseriesDistribution,
    pub start_timestamp: u64,

    pub host_selection_policy: Arc<dyn LoadBalancingPolicy>,
    pub tls_encryption: bool,
    pub keyspace_name: String,
    pub table_name: String,
    pub counter_table_name: String,
    pub username: String,
    pub password: String,
    pub mode: Mode,
    pub latency_type: LatencyType,
    pub max_consecutive_errors_per_op: u64,
    pub max_errors_in_total: u64,
    pub concurrency: u64,
    pub maximum_rate: u64,

    pub test_duration: Duration,
    pub partition_count: u64,
    pub clustering_row_count: u64,
    pub clustering_row_size_dist: Arc<dyn Distribution>,

    pub rows_per_request: u64,
    pub provide_upper_bound: bool,
    pub in_restriction: bool,
    pub select_order_by: Vec<OrderBy>,
    pub no_lower_bound: bool,
    pub bypass_cache: bool,

    pub range_count: u64,
    pub timeout: Duration,
    pub iterations: u64,
    // // Any error response that comes with delay greater than errorToTimeoutCutoffTime
    // // to be considered as timeout error and recorded to histogram as such
    pub measure_latency: bool,
    pub hdr_latency_file: String,
    pub hdr_latency_resolution: u64,
    pub hdr_latency_sig_fig: u64,
    pub validate_data: bool,
}

// Parses and validates scylla bench params.
pub(crate) fn parse_scylla_bench_args<I, S>(
    mut args: I,
    print_usage_on_fail: bool,
) -> Option<ScyllaBenchArgs>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let program_name = args.next().unwrap();

    let mut flag = ParserBuilder::new();

    let workload = flag.string_var("workload", "", "workload: sequential, uniform, timeseries");
    let consistency_level = flag.string_var("consistency-level", "quorum", "consistency level");
    let replication_factor = flag.i64_var("replication-factor", 1, "replication factor");

    let nodes = flag.string_var("nodes", "127.0.0.1:9042", "cluster contact nodes");
    let server_name = flag.string_var(
        "tls-server-name",
        "",
        "TLS server hostname (currently unimplemented)",
    );
    let host_verification =
        flag.bool_var("tls-host-verification", false, "verify server certificate");
    let client_compression = flag.bool_var(
        "client-compression",
        true,
        "use compression for client-coordinator communication",
    );
    let shard_connection_count = flag.u64_var(
        "shard-connection-count",
        1,
        "number of connections per shard",
    );
    let ca_cert_file = flag.string_var(
        "tls-ca-cert-file",
        "",
        "path to CA certificate file, needed to enable encryption",
    );
    let client_cert_file = flag.string_var(
        "tls-client-cert-file",
        "",
        "path to client certificate file, needed to enable client certificate authentication",
    );
    let client_key_file = flag.string_var(
        "tls-client-key-file",
        "",
        "path to client key file, needed to enable client certificate authentication",
    );

    let _connection_count = flag.i64_var(
        "connection-count",
        4,
        "number of connections (currently ignored)",
    );
    let page_size = flag.i64_var("page-size", 1000, "page size");
    let partition_offset = flag.i64_var(
        "partition-offset",
        0,
        "start of the partition range (only for sequential workload)",
    );

    let write_rate = flag.u64_var(
        "write-rate",
        0,
        "rate of writes (relevant only for time series reads)",
    );
    let distribution = flag.string_var(
        "distribution",
        "uniform",
        "distribution of keys (relevant only for time series reads): uniform, hnormal",
    );
    let start_timestamp = flag.u64_var(
        "start-timestamp",
        0,
        "start timestamp of the write load (relevant only for time series reads)",
    );

    let host_selection_policy = flag.string_var(
        "host-selection-policy",
        "token-aware",
        "set the driver host selection policy \
        (round-robin,token-aware,dc-aware:name-of-local-dc),default 'token-aware'",
    );
    let tls_encryption = flag.bool_var(
        "tls",
        false,
        "use TLS encryption for clien-coordinator communication",
    );
    let keyspace_name = flag.string_var("keyspace", "scylla_bench", "keyspace to use");
    let table_name = flag.string_var("table", "test", "table to use");
    let counter_table_name =
        flag.string_var("counter-table", "test_counters", "counter table to use");
    let username = flag.string_var("username", "", "cql username for authentication");
    let password = flag.string_var("password", "", "cql password for authentication");
    let mode = flag.string_var(
        "mode",
        "",
        "operating mode: write, read, counter_update, counter_read, scan",
    );
    let latency_type = flag.string_var(
        "latency-type",
        "raw",
        "type of the latency to print during the run: raw, fixed-coordinated-omission",
    );
    let max_errors_at_row = flag.u64_var(
        "error-at-row-limit",
        0,
        "the maximum number of consecutive errors allowed. \
        After exceeding it, the workflow will terminate with an error. \
        Set to 0 if you want to disable this limit",
    );
    let max_errors = flag.u64_var(
        "error-limit",
        0,
        "the number of total errors after which the workflow should stop and fail; \
        set it to 0 (the default) to disable this limit",
    );
    let concurrency = flag.u64_var("concurrency", 16, "number of used tasks");
    let maximum_rate = flag.u64_var(
        "max-rate",
        0,
        "the maximum rate of outbound requests in op/s (0 for unlimited)",
    );

    let test_duration = flag.duration_var(
        "duration",
        Duration::ZERO,
        "duration of the test in seconds (0 for unlimited)",
    );
    let partition_count = flag.u64_var("partition-count", 10_000, "number of partitions");
    let clustering_row_count = flag.u64_var(
        "clustering-row-count",
        100,
        "number of clustering rows in a partition",
    );
    let default_dist: Arc<dyn Distribution> = Arc::new(Fixed(4));
    let clustering_row_size_dist = flag.var(
        "clustering-row-size",
        ScyllaBenchDistribution(default_dist),
        "size of a single clustering row, can use random values",
    );

    let rows_per_request =
        flag.u64_var("rows-per-request", 1, "clustering rows per single request");
    let provide_upper_bound = flag.bool_var(
        "provide-upper-bound",
        false,
        "whether read requests should provide an upper bound",
    );
    let in_restriction = flag.bool_var(
        "in-restriction",
        false,
        "use IN restriction in read requests",
    );
    let select_order_by = flag.string_var(
        "select-order-by",
        "none",
        "controls order part 'order by ck asc/desc' of the read query, \
        you can set it to: none,asc,desc or to the list of them, i.e. 'none,asc', \
        in such case it will run queries with these orders one by one",
    );
    let no_lower_bound = flag.bool_var(
        "no-lower-bound",
        false,
        "do not provide lower bound in read requests",
    );
    let bypass_cache = flag.bool_var(
        "bypass-cache",
        false,
        "Execute queries with the \"BYPASS CACHE\" CQL clause",
    );

    let range_count = flag.u64_var(
        "range-count",
        1,
        "number of ranges to split the token space into (relevant only for scan mode)",
    );
    let timeout = flag.duration_var("timeout", Duration::from_secs(5), "request timeout");
    let iterations = flag.u64_var(
        "iterations",
        1,
        "number of iterations to run (0 for unlimited, relevant only for workloads \
        that have a defined number of ops to execute)",
    );

    let measure_latency = flag.bool_var("measure-latency", true, "measure request latency");

    let hdr_latency_file = flag.string_var(
        "hdr-latency-file",
        "",
        "log co-fixed and raw latency hdr histograms into a file",
    );
    let hdr_latency_units = flag.string_var(
        "hdr-latency-units",
        "ns",
        "ns (nano seconds), us (microseconds), ms (milliseconds)",
    );
    let hdr_latency_sig_fig = flag.u64_var(
        "hdr-latency-sig",
        3,
        "significant figures of the hdr histogram, number from 1 to 5 (default: 3)",
    );

    let validate_data = flag.bool_var(
        "validate-data",
        false,
        "write meaningful data and validate while reading",
    );

    let (parser, desc) = flag.build();

    let result = move || -> Result<ScyllaBenchArgs> {
        parser.parse_args(args)?;

        let nodes = nodes.get().split(',').map(str::to_string).collect();
        let mode = parse_mode(&mode.get())?;
        let workload = if mode == Mode::Scan {
            anyhow::ensure!(
                workload.get() == "",
                "workload type cannot be specified for scan mode",
            );
            WorkloadType::Scan
        } else {
            parse_workload(&workload.get())?
        };
        let consistency_level = parse_consistency_level(&consistency_level.get())?;
        let shard_connection_count = NonZeroUsize::new(shard_connection_count.get() as usize)
            .context("shard connection count cannot be 0")?;
        let distribution = parse_timeseries_distribution(&distribution.get())?;
        let mut start_timestamp = start_timestamp.get();
        if start_timestamp == 0 {
            start_timestamp = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64;
        }
        let host_selection_policy = parse_host_selection_policy(&host_selection_policy.get())?;
        let select_order_by = parse_order_by_chain(&select_order_by.get())?;
        let write_rate = write_rate.get();
        let concurrency = concurrency.get();
        let partition_count = partition_count.get();
        let maximum_rate = maximum_rate.get();

        if workload == WorkloadType::Timeseries {
            if mode == Mode::Read {
                anyhow::ensure!(
                    write_rate != 0,
                    "Write rate must be provided for time series reads loads",
                );
                anyhow::ensure!(
                    start_timestamp != 0,
                    "Start timestamp must be provided for time series reads loads",
                );
            } else if mode == Mode::Write {
                anyhow::ensure!(
                    concurrency <= partition_count,
                    "Time series writes require concurrency less than or equal partition count",
                );
                anyhow::ensure!(
                    maximum_rate != 0,
                    "max-rate must be provided for time series write loads"
                );
            }
        }

        let latency_type = match latency_type.get().as_str() {
            "raw" => LatencyType::Raw,
            "fixed-coordinated-omission" => LatencyType::AdjustedForCoordinatorOmission,
            s => return Err(anyhow::anyhow!("Unsupported latency type: {}; supported types are: raw, fixed-coordinated-omission", s)),
        };

        // Zero means unlimited tries,
        // and #tries == #retries + 1,
        // therefore just subtract with wraparound and treat u64::MAX as infinity
        let max_consecutive_errors_per_op = max_errors_at_row.get().wrapping_sub(1);

        // Similar to above
        let max_errors_in_total = max_errors.get().wrapping_sub(1);

        let hdr_latency_resolution = match hdr_latency_units.get().as_str() {
            "ns" => 1,
            "us" => 1000,
            "ms" => 1000 * 1000,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported units for hdr-latency-units, supported units are: ns, us,ms"
                ))
            }
        };

        let hdr_latency_sig_fig = hdr_latency_sig_fig.get();
        if !(1..=5).contains(&hdr_latency_sig_fig) {
            return Err(anyhow::anyhow!(
                "hdr-latency-sig must be an integer between 1 and 5"
            ));
        }

        Ok(ScyllaBenchArgs {
            workload,
            consistency_level,
            replication_factor: replication_factor.get(),
            nodes,
            ca_cert_file: ca_cert_file.get(),
            client_cert_file: client_cert_file.get(),
            client_key_file: client_key_file.get(),
            server_name: server_name.get(),
            host_verification: host_verification.get(),
            client_compression: client_compression.get(),
            shard_connection_count,
            page_size: page_size.get(),
            partition_offset: partition_offset.get(),
            write_rate,
            distribution,
            start_timestamp,
            host_selection_policy,
            tls_encryption: tls_encryption.get(),
            keyspace_name: keyspace_name.get(),
            table_name: table_name.get(),
            counter_table_name: counter_table_name.get(),
            username: username.get(),
            password: password.get(),
            mode,
            concurrency,
            latency_type,
            max_consecutive_errors_per_op,
            max_errors_in_total,
            maximum_rate,
            test_duration: test_duration.get(),
            partition_count,
            clustering_row_count: clustering_row_count.get(),
            clustering_row_size_dist: clustering_row_size_dist.get().0,
            rows_per_request: rows_per_request.get(),
            provide_upper_bound: provide_upper_bound.get(),
            in_restriction: in_restriction.get(),
            select_order_by,
            no_lower_bound: no_lower_bound.get(),
            bypass_cache: bypass_cache.get(),
            range_count: range_count.get(),
            timeout: timeout.get(),
            iterations: iterations.get(),
            measure_latency: measure_latency.get(),
            hdr_latency_file: hdr_latency_file.get(),
            hdr_latency_sig_fig,
            hdr_latency_resolution,
            validate_data: validate_data.get(),
        })
    }();

    match result {
        Ok(config) => Some(config),
        Err(err) => {
            eprintln!("Failed to parse flags: {:?}", err);
            if print_usage_on_fail {
                desc.print_help(&mut std::io::stderr(), program_name.as_ref())
                    .unwrap();
            }
            None
        }
    }
}

impl ScyllaBenchArgs {
    pub fn print_configuration(&self) {
        println!("Configuration");
        println!("Mode:\t\t\t {}", show_mode(&self.mode));
        println!("Workload:\t\t {}", show_workload(&self.workload));
        println!("Timeout:\t\t {}", format_duration(self.timeout));
        if self.max_consecutive_errors_per_op == u64::MAX {
            println!("Max error number at row: unlimited");
        } else {
            println!(
                "Max error number at row: {}",
                self.max_consecutive_errors_per_op as u128 + 1,
            );
        }
        if self.max_errors_in_total == u64::MAX {
            println!("Max error number:\t unlimited");
        } else {
            println!(
                "Max error number:\t {}",
                self.max_errors_in_total as u128 + 1,
            );
        }
        println!(
            "Consistency level:\t {}",
            show_consistency_level(&self.consistency_level)
        );
        println!("Partition count:\t {}", self.partition_count);
        if self.workload == WorkloadType::Sequential && self.partition_offset != 0 {
            println!("Partition offset:\t {}", self.partition_offset);
        }
        println!("Clustering rows:\t {}", self.clustering_row_count);
        println!(
            "Clustering row size:\t {}",
            self.clustering_row_size_dist.describe()
        );
        println!("Rows per request:\t {}", self.rows_per_request);
        if self.mode == Mode::Read {
            println!("Provide upper bound:\t {}", self.provide_upper_bound);
            println!("IN queries:\t\t {}", self.in_restriction);
            println!(
                "Order by:\t\t {}",
                show_order_by_chain(&self.select_order_by)
            );
            println!("No lower bound:\t\t {}", self.no_lower_bound);
        }
        println!("Page size:\t\t {}", self.page_size);
        println!("Concurrency:\t\t {}", self.concurrency);
        // println!("Connections:\t\t {}", self.connection_count);
        if self.maximum_rate > 0 {
            println!("Maximum rate:\t\t {}ops/s", self.maximum_rate);
        } else {
            println!("Maximum rate:\t\t unlimited");
        }
        println!("Client compression:\t {}", self.client_compression);
        println!("Shard connection count:\t {}", self.shard_connection_count);
        if self.workload == WorkloadType::Timeseries {
            println!("Start timestamp:\t {}", self.start_timestamp);
            println!(
                "Write rate:\t\t {}",
                self.maximum_rate / self.partition_count
            );
        }

        // println!("Hdr memory consumption:\t", results.GetHdrMemoryConsumption(concurrency), "bytes");
    }
}

struct ScyllaBenchDistribution(Arc<dyn Distribution>);

impl GoValue for ScyllaBenchDistribution {
    fn parse(s: &str) -> Result<Self> {
        let dist = parse_distribution(s)?.into();
        Ok(ScyllaBenchDistribution(dist))
    }

    fn to_string(&self) -> String {
        self.0.describe()
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum OrderBy {
    None,
    Asc,
    Desc,
}

fn parse_order_by_chain(s: &str) -> Result<Vec<OrderBy>> {
    if s.is_empty() {
        return Ok(vec![OrderBy::None]);
    }

    s.split(',')
        .enumerate()
        .map(|(idx, s)| {
            parse_order_by(s)
                .with_context(|| format!("failed to parse part {} of the order by chain", idx))
        })
        .collect()
}

fn parse_order_by(s: &str) -> Result<OrderBy> {
    match s.to_lowercase().as_str() {
        "none" => Ok(OrderBy::None),
        "asc" => Ok(OrderBy::Asc),
        "desc" => Ok(OrderBy::Desc),
        _ => Err(anyhow::anyhow!(
            "invalid order-by specifier: {} \
            (expected \"none\", \"asc\" or \"desc\")",
            s,
        )),
    }
}

fn show_order_by_chain(chain: &[OrderBy]) -> String {
    let mut s = String::new();
    for (idx, part) in chain.iter().enumerate() {
        if idx > 0 {
            s.push(',');
        }
        s.push_str(show_order_by(part));
    }
    s
}

fn show_order_by(order: &OrderBy) -> &'static str {
    match order {
        OrderBy::None => "none",
        OrderBy::Asc => "asc",
        OrderBy::Desc => "desc",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    Write,
    Read,
    CounterUpdate,
    CounterRead,
    Scan,
}

fn parse_mode(s: &str) -> Result<Mode> {
    match s {
        "write" => Ok(Mode::Write),
        "read" => Ok(Mode::Read),
        "counter_update" => Ok(Mode::CounterUpdate),
        "counter_read" => Ok(Mode::CounterRead),
        "scan" => Ok(Mode::Scan),
        "" => Err(anyhow::anyhow!("mode needs to be specified")),
        _ => Err(anyhow::anyhow!("unknown mode: {}", s)),
    }
}

fn show_mode(m: &Mode) -> &'static str {
    match m {
        Mode::Write => "write",
        Mode::Read => "read",
        Mode::CounterUpdate => "counter_update",
        Mode::CounterRead => "counter_read",
        Mode::Scan => "scan",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkloadType {
    Sequential,
    Uniform,
    Timeseries,
    Scan,
}

fn parse_workload(s: &str) -> Result<WorkloadType> {
    match s {
        "sequential" => Ok(WorkloadType::Sequential),
        "uniform" => Ok(WorkloadType::Uniform),
        "timeseries" => Ok(WorkloadType::Timeseries),
        // scan workload cannot be specified through CLI
        "" => Err(anyhow::anyhow!("workload type needs to be specified")),
        _ => Err(anyhow::anyhow!("unknown workload type: {}", s)),
    }
}

fn show_workload(w: &WorkloadType) -> &'static str {
    match w {
        WorkloadType::Sequential => "sequential",
        WorkloadType::Uniform => "uniform",
        WorkloadType::Timeseries => "timeseries",
        WorkloadType::Scan => "scan",
    }
}

fn parse_consistency_level(s: &str) -> Result<Consistency> {
    let level = match s {
        "any" => Consistency::Any,
        "one" => Consistency::One,
        "two" => Consistency::Two,
        "three" => Consistency::Three,
        "quorum" => Consistency::Quorum,
        "all" => Consistency::All,
        "local_quorum" => Consistency::LocalQuorum,
        "each_quorum" => Consistency::EachQuorum,
        "local_one" => Consistency::LocalOne,
        "serial" => Consistency::Serial,
        "local_serial" => Consistency::LocalSerial,
        _ => return Err(anyhow::anyhow!("Unknown consistency level: {}", s)),
    };
    Ok(level)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeseriesDistribution {
    Uniform,
    HalfNormal,
}

fn parse_timeseries_distribution(s: &str) -> Result<TimeseriesDistribution> {
    match s {
        "uniform" => Ok(TimeseriesDistribution::Uniform),
        "hnormal" => Ok(TimeseriesDistribution::HalfNormal),
        _ => Err(anyhow::anyhow!("Unknown timeseries distribution: {}", s)),
    }
}

fn show_consistency_level(cl: &Consistency) -> &'static str {
    match cl {
        Consistency::Any => "any",
        Consistency::One => "one",
        Consistency::Two => "two",
        Consistency::Three => "three",
        Consistency::Quorum => "quorum",
        Consistency::All => "all",
        Consistency::LocalQuorum => "local_quorum",
        Consistency::EachQuorum => "each_quorum",
        Consistency::LocalOne => "local_one",
        Consistency::Serial => "serial",
        Consistency::LocalSerial => "local_serial",
    }
}

fn parse_host_selection_policy(s: &str) -> Result<Arc<dyn LoadBalancingPolicy>> {
    // host-pool is unsupported
    let policy: Arc<dyn LoadBalancingPolicy> = match s {
        "round-robin" => DefaultPolicy::builder().token_aware(false).build(),
        "token-aware" => DefaultPolicy::builder().token_aware(true).build(),
        // dc-aware is unimplemented in the original s-b, so here is
        // my interpretation of it
        _ => match s.strip_prefix("dc-aware:") {
            Some(local_dc) => DefaultPolicy::builder()
                .token_aware(false)
                .prefer_datacenter(local_dc.to_owned())
                .build(),
            None => return Err(anyhow::anyhow!("Unknown host selection policy: {}", s)),
        },
    };
    Ok(policy)
}
