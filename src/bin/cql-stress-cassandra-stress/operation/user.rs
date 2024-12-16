use std::{collections::HashMap, ops::ControlFlow, sync::Arc};

use cql_stress::{
    configuration::{Operation, OperationContext, OperationFactory},
    make_runnable,
};
use scylla::{
    frame::response::result::CqlValue, prepared_statement::PreparedStatement,
    transport::topology::Table, Session,
};

use anyhow::{Context, Result};

use crate::{
    java_generate::{
        distribution::{Distribution, DistributionFactory},
        values::{Generator, GeneratorConfig, ValueGeneratorFactory},
    },
    settings::{CassandraStressSettings, OpWeight, PREDEFINED_INSERT_OPERATION},
    stats::ShardedStats,
};

use super::{
    row_generator::RowGenerator, CassandraStressOperation, CassandraStressOperationFactory,
    OperationSampler,
};

const SEED_STR: &str = "seed for stress";

pub struct UserDefinedOperation {
    session: Arc<Session>,
    statement: PreparedStatement,
    argument_index: Vec<usize>,
}

impl CassandraStressOperation for UserDefinedOperation {
    type Factory = UserDefinedOperationFactory;

    async fn execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
        let mut bound_row = Vec::with_capacity(self.argument_index.len());

        for i in &self.argument_index {
            bound_row.push(&row[*i]);
        }

        // User can provide a custom query here. In addition, we don't care
        // about the result of this query. This is why we can use `execute_unpaged`.
        self.session
            .execute_unpaged(&self.statement, bound_row)
            .await?;

        Ok(ControlFlow::Continue(()))
    }

    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue> {
        row_generator.generate_row()
    }
}

pub struct UserDefinedOperationFactory {
    session: Arc<Session>,
    statement: PreparedStatement,
    argument_index: Vec<usize>,
}

impl CassandraStressOperationFactory for UserDefinedOperationFactory {
    type Operation = UserDefinedOperation;

    fn create(&self) -> Self::Operation {
        UserDefinedOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            argument_index: self.argument_index.clone(),
        }
    }
}

pub struct UserOperation {
    sampler: OperationSampler<UserDefinedOperation>,
    workload: RowGenerator,
    stats: Arc<ShardedStats>,
    max_operations: Option<u64>,
    cached_row: Option<Vec<CqlValue>>,
}

make_runnable!(UserOperation);
impl UserOperation {
    pub async fn execute(&mut self, ctx: &OperationContext) -> Result<ControlFlow<()>> {
        if self
            .max_operations
            .is_some_and(|max_ops| ctx.operation_id >= max_ops)
        {
            return Ok(ControlFlow::Break(()));
        }

        let (op, row) = match &mut self.cached_row {
            Some(cached_row) => (self.sampler.previous_sample(), cached_row),
            None => {
                let op = self.sampler.sample();
                let row = self.cached_row.insert(op.generate_row(&mut self.workload));
                (op, row)
            }
        };

        let op_result = op.execute(row).await;

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

pub struct UserOperationFactory {
    session: Arc<Session>,
    pk_seed_distribution: Arc<dyn Distribution>,
    stats: Arc<ShardedStats>,
    table_metadata: Table,
    queries_payload: HashMap<String, (PreparedStatement, OpWeight)>,
    pk_generator_factory: Box<dyn ValueGeneratorFactory>,
    column_generator_factories: Vec<Box<dyn ValueGeneratorFactory>>,
    max_operations: Option<u64>,
    clustering: Arc<dyn DistributionFactory>,
}

impl UserOperationFactory {
    async fn prepare_insert_statement(
        session: &Arc<Session>,
        table_name: &str,
        table_metadata: &Table,
    ) -> Result<PreparedStatement> {
        let column_names = table_metadata
            .columns
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();

        let column_list_str = column_names.join(", ");
        let column_values_str = std::iter::repeat("?")
            .take(column_names.len())
            .collect::<Vec<_>>()
            .join(", ");

        let statement_str =
            format!("INSERT INTO {table_name} ({column_list_str}) VALUES ({column_values_str})");
        session
            .prepare(statement_str)
            .await
            .context("Failed to prepare statement for 'insert' operation.")
    }

    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        stats: Arc<ShardedStats>,
    ) -> Result<Self> {
        // We parsed a user command. This unwrap is safe.
        let user_profile = settings.command_params.user.as_ref().unwrap();

        let query_definitions = &user_profile.queries_payload;
        let cluster_data = session.get_cluster_data();
        let table_metadata = cluster_data
            .get_keyspace_info()
            .get(&user_profile.keyspace)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Cannot find keyspace {} in cluster data.",
                    user_profile.keyspace
                )
            })?
            .tables
            .get(&user_profile.table)
            .ok_or_else(|| {
                anyhow::anyhow!("Cannot find table {} in cluster data.", user_profile.table)
            })?
            .clone();

        anyhow::ensure!(
            table_metadata.partition_key.len() == 1,
            "Compound partition keys are not yet supported by the tool!"
        );

        let queries_payload = {
            let mut queries_payload = HashMap::new();
            for (q_name, (q_def, weight)) in query_definitions {
                queries_payload.insert(
                    q_name.to_owned(),
                    (q_def.to_prepared_statement(&session).await?, *weight),
                );
            }
            // Handle 'insert' operation separately.
            if let Some(insert_weight) = &user_profile.insert_operation_weight {
                let insert_statement =
                    Self::prepare_insert_statement(&session, &user_profile.table, &table_metadata)
                        .await?;
                queries_payload.insert(
                    PREDEFINED_INSERT_OPERATION.to_owned(),
                    (insert_statement, *insert_weight),
                );
            }

            println!("\n========================");
            println!("Operations to be performed and their sample ratio weights:\n");
            for (q_name, (statement, q_weight)) in queries_payload.iter() {
                println!(
                    "- {}: {{ 'cql': '{}', 'weight': {} }}",
                    q_name,
                    statement.get_statement(),
                    q_weight
                );
            }
            println!("========================\n");

            queries_payload
        };

        let pk_seed_distribution = settings.population.pk_seed_distribution.create().into();
        let max_operations = settings.command_params.common.operation_count;

        let pk_name = &table_metadata.partition_key[0];
        let pk_generator_factory = Generator::new_generator_factory_from_cql_type(
            &table_metadata
                .columns
                .get(pk_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Table::columns does not contain info about pk {}. Probably a server bug.",
                        pk_name
                    )
                })?
                .type_,
        )?;
        let column_generator_factories = table_metadata
            .columns
            .iter()
            .filter(|&(col_name, _col_def)| (*col_name != *pk_name))
            .map(|(_col_name, col_def)| {
                Generator::new_generator_factory_from_cql_type(&col_def.type_)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            session,
            pk_seed_distribution,
            stats,
            table_metadata,
            queries_payload,
            max_operations,
            pk_generator_factory,
            column_generator_factories,
            clustering: user_profile.clustering.clone(),
        })
    }

    fn create_workload(&self) -> RowGenerator {
        let pk_name = &self.table_metadata.partition_key[0];
        let pk_generator = Generator::new(
            self.pk_generator_factory.create(),
            GeneratorConfig::new(&format!("{}{}", SEED_STR, pk_name), None, None),
            pk_name.clone(),
        );

        let column_generators = self
            .table_metadata
            .columns
            .iter()
            .filter(|(col_name, _col_def)| **col_name != *pk_name)
            .zip(self.column_generator_factories.iter())
            .map(|((col_name, _), gen_factory)| {
                Generator::new(
                    gen_factory.create(),
                    GeneratorConfig::new(&format!("{}{}", SEED_STR, col_name), None, None),
                    col_name.to_owned(),
                )
            })
            .collect::<Vec<_>>();

        RowGenerator::new(
            Arc::clone(&self.pk_seed_distribution),
            pk_generator,
            column_generators,
        )
    }
}

impl OperationFactory for UserOperationFactory {
    fn create(&self) -> Box<dyn Operation> {
        let workload = self.create_workload();

        let weights_iter =
            self.queries_payload
                .iter()
                .map(|(_op_name, (stmt, weight))| {
                    let variable_metadata = stmt.get_variable_col_specs();
                    let argument_index = variable_metadata
                        .iter()
                        .map(|col_spec| {
                            workload.row_index_of_column_with_name(col_spec.name()).expect(
                            "Prepared statement metadata is inconsistent with cluster metadata.",
                        )
                        })
                        .collect::<Vec<_>>();
                    (
                        UserDefinedOperation {
                            session: Arc::clone(&self.session),
                            statement: stmt.clone(),
                            argument_index,
                        },
                        *weight,
                    )
                });

        let sampler = OperationSampler::new(weights_iter, self.clustering.as_ref());

        Box::new(UserOperation {
            workload,
            stats: Arc::clone(&self.stats),
            max_operations: self.max_operations,
            sampler,
            cached_row: None,
        })
    }
}
