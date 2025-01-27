use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use futures::TryStreamExt;
use scylla::{prepared_statement::PreparedStatement, Session};

use cql_stress::configuration::{make_runnable, Operation, OperationContext, OperationFactory};

use crate::args::ScyllaBenchArgs;
use crate::operation::ReadContext;
use crate::stats::ShardedStats;

struct SharedState {
    pub next_range_idx: AtomicU64,
}

pub(crate) struct ScanOperationFactory {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statement: PreparedStatement,
    args: Arc<ScyllaBenchArgs>,

    shared_state: Arc<SharedState>,
}

struct ScanOperation {
    session: Arc<Session>,
    stats: Arc<ShardedStats>,
    statement: PreparedStatement,
    args: Arc<ScyllaBenchArgs>,

    shared_state: Arc<SharedState>,
}

impl ScanOperationFactory {
    pub async fn new(
        session: Arc<Session>,
        stats: Arc<ShardedStats>,
        args: Arc<ScyllaBenchArgs>,
    ) -> Result<Self> {
        let statement_str = format!(
            "SELECT pk, ck, v FROM {} WHERE token(pk) >= ? AND token(pk) <= ?",
            args.table_name,
        );
        let mut statement = session.prepare(statement_str).await?;
        statement.set_consistency(args.consistency_level);
        statement.set_request_timeout(Some(args.timeout));

        let shared_state = Arc::new(SharedState {
            next_range_idx: AtomicU64::new(0),
        });

        Ok(Self {
            session,
            stats,
            statement,
            args,

            shared_state,
        })
    }
}

impl OperationFactory for ScanOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(ScanOperation {
            session: Arc::clone(&self.session),
            stats: Arc::clone(&self.stats),
            statement: self.statement.clone(),
            args: self.args.clone(),

            shared_state: self.shared_state.clone(),
        })
    }
}

make_runnable!(ScanOperation);
impl ScanOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        let mut rctx = ReadContext::default();

        let range_idx = self
            .shared_state
            .next_range_idx
            .fetch_add(1, Ordering::Relaxed);

        let range_idx = range_idx % self.args.range_count;

        let calc_bound = |idx: u64| {
            let shifted = (idx as u128) << 64;
            let biased = shifted / self.args.range_count as u128;
            biased as i64 + i64::MIN
        };

        let range_begin = calc_bound(range_idx);
        let range_end = calc_bound(range_idx + 1);

        let result = self.do_execute(&mut rctx, range_begin, range_end).await;

        if let Err(err) = &result {
            rctx.failed_scan(err, range_begin, range_end);
        }

        let mut stats_lock = self.stats.get_shard_mut();
        let stats = &mut *stats_lock;
        stats.operations += 1;
        stats.errors += rctx.errors;
        stats.clustering_rows += rctx.rows_read;
        stats_lock.account_latency(ctx);

        result
    }
}

impl ScanOperation {
    async fn do_execute(
        &mut self,
        rctx: &mut ReadContext,
        first: i64,
        last: i64,
    ) -> Result<ControlFlow<()>> {
        let pager = self
            .session
            .execute_iter(self.statement.clone(), (first, last))
            .await?;

        let mut iter = pager.rows_stream::<(i64, i64, Vec<u8>)>()?;

        while let Some((pk, ck, v)) = iter.try_next().await? {
            rctx.row_read();
            if self.args.validate_data {
                if let Err(err) = super::validate_row_data(pk, ck, &v) {
                    rctx.data_corruption(pk, ck, &err);
                }
            }
        }

        Ok(ControlFlow::Continue(()))
    }
}
