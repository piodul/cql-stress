mod counter_write;
mod mixed;
mod read;
mod row_generator;
#[cfg(feature = "user-profile")]
mod user;
mod write;

use anyhow::Context;
use anyhow::Result;
use cql_stress::configuration::Operation;
use cql_stress::configuration::OperationContext;
use cql_stress::configuration::OperationFactory;
use cql_stress::make_runnable;
#[cfg(feature = "user-profile")]
use rand_distr::{Distribution as _, WeightedIndex};
use scylla::Session;
use std::future::Future;
use std::num::Wrapping;
use std::ops::ControlFlow;
use std::sync::Arc;

pub use mixed::MixedOperationFactory;
pub use row_generator::RowGeneratorFactory;
use scylla::{
    frame::response::result::{CqlValue, Row},
    QueryResult,
};
#[cfg(feature = "user-profile")]
pub use user::UserOperationFactory;

#[cfg(feature = "user-profile")]
use crate::java_generate::distribution::{Distribution, DistributionFactory};
use crate::settings::CassandraStressSettings;
use crate::stats::ShardedStats;

use self::row_generator::RowGenerator;

const DEFAULT_TABLE_NAME: &str = "standard1";
const DEFAULT_COUNTER_TABLE_NAME: &str = "counter1";

/// A specific CassandraStress operation.
///
/// The operation implementing this trait should handle
/// sending the actual query to the database.
///
/// This trait is intended to be used by [`GenericCassandraStressOperation`]
/// which encapsulates the specific operation and handles the common logic.
///
/// ## Result of [`CassandraStressOperation::execute`]
/// ### Operation retries
/// During the operation retry (i.e. when `execute` returned and error),
/// we will make use of the same row that we originally used in the previous try.
///
/// We only generate a new row ([`CassandraStressOperation::generate_row`])
/// during the first try to perform an operation.
/// ### Stats recording
/// The result of `execute` is recorded
/// to [`ShardedStats`] - even if the operation failed, so we keep track
/// of number of errors that appeared during the benchmark.
pub trait CassandraStressOperation: Sync + Send {
    type Factory: CassandraStressOperationFactory<Operation = Self>;

    fn execute(&self, row: &[CqlValue]) -> impl Future<Output = Result<ControlFlow<()>>> + Send;
    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue>;
}

pub trait CassandraStressOperationFactory: Sync + Send + Sized {
    type Operation: CassandraStressOperation<Factory = Self>;

    fn create(&self) -> Self::Operation;
}

/// Generic CassandraStress operation.
///
/// It handles the common logic for all of the operations, such as:
/// - checking whether `max_operations` operations have already been performed
/// - caching the row for operation retries
/// - recording operation result to statistics structure
///
/// Delegates the specific logic to `cs_operation`.
pub struct GenericCassandraStressOperation<O: CassandraStressOperation> {
    cs_operation: O,
    stats: Arc<ShardedStats>,
    workload: RowGenerator,
    max_operations: Option<u64>,
    // The operation may need to be retried.
    // This is why we cache the row so it can be used
    // during the retry.
    cached_row: Option<Vec<CqlValue>>,
}

make_runnable!(GenericCassandraStressOperation<O: CassandraStressOperation>);
impl<O: CassandraStressOperation> GenericCassandraStressOperation<O> {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        if self
            .max_operations
            .is_some_and(|max_ops| ctx.operation_id >= max_ops)
        {
            return Ok(ControlFlow::Break(()));
        }

        let row = self
            .cached_row
            .get_or_insert_with(|| self.cs_operation.generate_row(&mut self.workload));

        let op_result = self.cs_operation.execute(row).await;
        self.stats
            .get_shard_mut()
            .account_operation(ctx, &op_result);

        if op_result.is_ok() {
            // Operation was successful - we will generate new row
            // for the next operation.
            self.cached_row = None;
        }

        op_result
    }
}

pub struct GenericCassandraStressOperationFactory<O: CassandraStressOperation> {
    cs_operation_factory: O::Factory,
    workload_factory: RowGeneratorFactory,
    max_operations: Option<u64>,
    stats: Arc<ShardedStats>,
}

pub type WriteOperationFactory = GenericCassandraStressOperationFactory<write::WriteOperation>;
pub type CounterWriteOperationFactory =
    GenericCassandraStressOperationFactory<counter_write::CounterWriteOperation>;
pub type RegularReadOperationFactory =
    GenericCassandraStressOperationFactory<read::RegularReadOperation>;
pub type CounterReadOperationFactory =
    GenericCassandraStressOperationFactory<read::CounterReadOperation>;

impl WriteOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let max_operations = settings.command_params.common.operation_count;
        let cs_operation_factory = write::WriteOperationFactory::new(settings, session).await?;

        Ok(Self {
            cs_operation_factory,
            max_operations,
            workload_factory,
            stats,
        })
    }
}

impl CounterWriteOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let max_operations = settings.command_params.common.operation_count;
        let cs_operation_factory =
            counter_write::CounterWriteOperationFactory::new(settings, session).await?;

        Ok(Self {
            cs_operation_factory,
            max_operations,
            workload_factory,
            stats,
        })
    }
}

impl RegularReadOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let max_operations = settings.command_params.common.operation_count;
        let cs_operation_factory =
            read::RegularReadOperationFactory::new(settings, session, DEFAULT_TABLE_NAME).await?;

        Ok(Self {
            cs_operation_factory,
            max_operations,
            workload_factory,
            stats,
        })
    }
}

impl CounterReadOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        let max_operations = settings.command_params.common.operation_count;
        let cs_operation_factory =
            read::CounterReadOperationFactory::new(settings, session, DEFAULT_COUNTER_TABLE_NAME)
                .await?;

        Ok(Self {
            cs_operation_factory,
            max_operations,
            workload_factory,
            stats,
        })
    }
}

impl<O: CassandraStressOperation + 'static> OperationFactory
    for GenericCassandraStressOperationFactory<O>
{
    fn create(&self) -> Box<dyn Operation> {
        let cs_operation = self.cs_operation_factory.create();

        Box::new(GenericCassandraStressOperation {
            cs_operation,
            stats: Arc::clone(&self.stats),
            workload: self.workload_factory.create(),
            max_operations: self.max_operations,
            cached_row: None,
        })
    }
}

/// See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/generate/PartitionIterator.java#L725.
fn recompute_seed(seed: i64, partition_key: &CqlValue) -> i64 {
    match partition_key {
        CqlValue::Blob(key) => {
            let mut wrapped = Wrapping(seed);
            for byte in key {
                wrapped = (wrapped * Wrapping(31)) + Wrapping((*byte as i8) as i64);
            }
            wrapped.0
        }
        _ => todo!("Implement recompute_seed for other CqlValues"),
    }
}

fn extract_single_row_from_query_result(query_result: QueryResult) -> Result<Row> {
    let rows_result = query_result
        .into_rows_result()
        .context("Failed to convert to Rows result")?;

    // Note that row-generation logic behaves in a way that given partition_key,
    // there is exactly one row with this partition_key.
    rows_result
        .single_row::<Row>()
        .context("Failed to extract a single row from the result")
}

pub trait RowValidator: Sync + Send + Default {
    fn validate_row(&self, generated_row: &[CqlValue], query_result: QueryResult) -> Result<()>;
}

#[derive(Default)]
pub struct EqualRowValidator;
impl RowValidator for EqualRowValidator {
    fn validate_row(&self, generated_row: &[CqlValue], query_result: QueryResult) -> Result<()> {
        let first_row = extract_single_row_from_query_result(query_result)?;

        anyhow::ensure!(
            first_row.columns.len() == generated_row.len(),
            "Expected row's ({:?}) length: {}. Result row's ({:?}) length: {}",
            generated_row,
            generated_row.len(),
            first_row.columns,
            first_row.columns.len(),
        );

        let result =
            first_row
                .columns
                .iter()
                .zip(generated_row.iter())
                .all(|(maybe_result, expected)| match maybe_result {
                    Some(result) => result == expected,
                    // TODO: For now, we don't permit NULLs.
                    None => false,
                });

        anyhow::ensure!(
            result,
            "The data doesn't match. Result: {:?}. Expected: {:?}.",
            first_row.columns,
            generated_row,
        );
        Ok(())
    }
}

#[derive(Default)]
pub struct ExistsRowValidator;
impl RowValidator for ExistsRowValidator {
    fn validate_row(&self, _generated_row: &[CqlValue], query_result: QueryResult) -> Result<()> {
        // We only check that the row with given PK exists, which is equivalent to
        // successfully extracting the first row from the query result.
        let _first_row = extract_single_row_from_query_result(query_result)?;
        Ok(())
    }
}

/// A sampler created based on a ratio map and a counter distribution.
///
/// How the sampler works?
/// One iteration consists of:
/// - sampling an item based on ratio map. `current_item_index` is sampled from `item_index_dist`.
///   The item can then be retrieved via this index from `items` vector.
/// - sampling a counter which says how many times to return the current item.
///   The counter is sampled from `counter_dist` distribution.
///
/// The user then can sample the items via `sample` or `previous_sample` method.
///
/// The `sample` method will decrease the counter by 1, and return current item.
/// If the counter reaches 0, new iteration starts.
///
/// The `previous_sample` method returns a current item without decreasing the counter.
/// This is helpful when the user wants to, for example, retry an operation that was
/// sampled before, but failed for some reason.
#[cfg(feature = "user-profile")]
struct OperationSampler<T> {
    counter_dist: Box<dyn Distribution>,
    items: Vec<T>,
    item_index_dist: WeightedIndex<f64>,
    current_item_remaining: u8,
    current_item_index: usize,
}

#[cfg(feature = "user-profile")]
impl<T> OperationSampler<T> {
    pub fn new(
        weights: impl Iterator<Item = (T, f64)>,
        counter_dist_factory: &dyn DistributionFactory,
    ) -> Self {
        let (items, weights): (Vec<_>, Vec<_>) = weights.unzip();
        // We verify the ratio properties during parsing.
        let item_index_dist = WeightedIndex::new(weights).unwrap_or_else(|err| {
            panic!("Failed to create a WeightedIntex from provided ratios: {err}")
        });

        Self {
            counter_dist: counter_dist_factory.create(),
            items,
            item_index_dist,
            current_item_remaining: 0,
            current_item_index: 0,
        }
    }

    pub fn sample(&mut self) -> &T {
        if self.current_item_remaining == 0 {
            self.current_item_index = self.item_index_dist.sample(&mut rand::thread_rng());
            self.current_item_remaining = (self.counter_dist.next_i64() as u8).max(1);
        }
        self.current_item_remaining -= 1;
        &self.items[self.current_item_index]
    }

    pub fn previous_sample(&self) -> &T {
        &self.items[self.current_item_index]
    }
}
