use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::time::Instant;

use crate::run::WorkerSession;

/// Defines the configuration of a benchmark.
pub struct Configuration {
    /// The maximum duration of the test.
    ///
    /// Depending on the workload, the test may finish earlier than
    /// the specified duration, but it will be immediately stopped if it lasts
    /// longer than `max_duration`.
    ///
    /// If `None`, the test duration is unlimited.
    pub max_duration: Option<Duration>,

    /// The concurrency with which the benchmark operations will be performed.
    ///
    /// The tool will spawn as many tokio tasks as this number specifies,
    /// and each task will sequentially perform the benchmark operations.
    ///
    /// Must not be zero.
    pub concurrency: u64,

    /// The maximum number of operations to be performed per second.
    /// If `None`, then there is no rate limit imposed.
    pub rate_limit_per_second: Option<f64>,

    /// A factory which creates operations that will be executed'
    /// during the stress.
    pub operation_factory: Arc<dyn OperationFactory>,

    /// The maximum number of consecutive errors allowed before giving up.
    pub max_consecutive_errors_per_op: u64,

    /// The maximum, global number of errors allowed during the test.
    /// After exceeding this number, the bench will be stopped.
    pub max_errors_in_total: u64,
}

/// Contains all necessary context needed to execute an Operation.
pub struct OperationContext {
    /// The current ID of the operation being performed.
    ///
    /// The tool tries to issue operation IDs sequentially, however because
    /// of the parallelism the operations can be reordered. To be more precise,
    /// if an operation with ID `X` > 0 was issued, then the tool has attempted
    /// or will attempt to execute operations of IDs less than `X`.
    pub operation_id: u64,

    /// The time of the supposed operation start time.
    ///
    /// If rate limiting is enabled, then each operation has a scheduled
    /// start time. If the run does not keep up and operations take longer
    /// than expected, operations will be executed past their schedule.
    /// In order to account for the coordinated omission problem, latency
    /// should be measured as the duration between the scheduled start time
    /// and the actual operation end time.
    ///
    /// If rate limiting is disabled, this will always be equal to `now`.
    pub scheduled_start_time: Instant,

    /// The time when the operation actually started executing.
    ///
    /// Unless rate limiting is enabled and the run does not keep
    /// with configured rate, this will be either equal or close
    /// to `scheduled_start_time`.
    pub actual_start_time: Instant,
}

/// Creates operations which can later be used by workers during the stress.
pub trait OperationFactory: Send + Sync {
    /// Creates an Operation.
    ///
    /// The single operation will be used from within a single worker.
    /// It can have its own state.
    fn create(&self) -> Box<dyn Operation>;
}

/// Represents an operation which runs its own operation loop.
/// Implementing this interface instead of Operation leads to more efficient
/// code because Rust, for now, forces us to Box futures returned
/// from Operation::execute. This trait only incurs one allocation
/// per running worker.
#[async_trait]
pub trait Operation: Send + Sync {
    /// Classes that implement this trait should have the following, non-trait
    /// method defined:
    ///
    /// async fn execute(&mut self, ctx: OperationContext) -> Result<ControlFlow<()>>;
    ///
    /// and they should use make_runnable!(TraitName) macro to generate
    /// the implementation of the run() method.
    ///
    /// The operation should behave deterministically, i.e. the same action
    /// should be performed when given exactly the same OperationContext.
    /// This enables deterministic behavior of the tool and makes it possible
    /// to control the retry logic outside the Operation.
    ///
    /// Returns ControlFlow::Break if it should finish work, for example
    /// if the operation ID has exceeded the configured operation count.
    /// In other cases, it returns ControlFlow::Continue.
    async fn run(&mut self, session: WorkerSession) -> Result<()>;
}

/// Implements Operation for a type which implements an execute method.
/// Although we could put execute() into the Operation trait, doing what we
/// are doing here has better performance because asynchronous traits require
/// putting returned futures in a Box due to current language limitations.
/// Boxing the futures imply an allocation per operation and those allocations
/// can be clearly visible on the flamegraphs.
#[macro_export]
macro_rules! make_runnable {
    ($op:ident$(<$($targ:tt: $tbound:tt),+>)?) => {
        #[async_trait]
        impl$(<$($targ: $tbound),+>)? $crate::configuration::Operation for $op$(<$($targ),*>)? {
            async fn run(&mut self, mut session: $crate::run::WorkerSession) -> anyhow::Result<()> {
                while let Some(ctx) = session.start_operation().await {
                    let result = self.execute(&ctx).await;
                    if let std::ops::ControlFlow::Break(_) = session.end_operation(result)? {
                        return Ok(());
                    }
                }
                Ok(())
            }
        }
    };
}

pub use make_runnable;
