use std::{ops::ControlFlow, sync::Arc};

use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};

use anyhow::{Context, Result};
use scylla::{prepared_statement::PreparedStatement, Session};

use crate::settings::CassandraStressSettings;

use super::{
    row_generator::{RowGenerator, RowGeneratorFactory},
    validate_row,
};

pub struct ReadOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload: RowGenerator,
    max_operations: Option<u64>,
}

pub struct ReadOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload_factory: RowGeneratorFactory,
    max_operations: Option<u64>,
}

impl OperationFactory for ReadOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        Box::new(ReadOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            workload: self.workload_factory.create(),
            max_operations: self.max_operations,
        })
    }
}

impl ReadOperationFactory {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        workload_factory: RowGeneratorFactory,
    ) -> Result<Self> {
        let statement_str = "SELECT * FROM standard1 WHERE KEY=?";
        let mut statement = session
            .prepare(statement_str)
            .await
            .context("Failed to prepare statement")?;

        statement.set_is_idempotent(true);
        statement.set_consistency(settings.command_params.basic_params.consistency_level);
        statement.set_serial_consistency(Some(
            settings
                .command_params
                .basic_params
                .serial_consistency_level,
        ));

        Ok(Self {
            session,
            statement,
            workload_factory,
            max_operations: settings.command_params.basic_params.operation_count,
        })
    }
}

make_runnable!(ReadOperation);
impl ReadOperation {
    async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        if self
            .max_operations
            .is_some_and(|max_ops| ctx.operation_id >= max_ops)
        {
            return Ok(ControlFlow::Break(()));
        }

        let row = self.workload.generate_row();
        let pk = &row[0];

        let result = self.session.execute(&self.statement, (pk,)).await;
        if let Err(err) = result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?pk,
                "read error",
            );
        }

        let validation_result = validate_row(&row, result?);
        if let Err(err) = validation_result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?pk,
                "read validation error",
            );
        }
        validation_result
            .with_context(|| format!("Row with partition_key: {:?} could not be validated.", pk))?;

        Ok(ControlFlow::Continue(()))
    }
}