use scylla::_macro_internal::CqlValue;

use crate::{
    java_generate::{
        distribution::{fixed::FixedDistribution, sequence::SeqDistribution, Distribution},
        values::{Blob, Generator, GeneratorConfig, HexBlob},
    },
    settings::CassandraStressSettings,
};
use std::sync::Arc;

use super::recompute_seed;

/// A row generator structure.
///
/// Row-generation logic:
/// - sample the `pk_seed` from `pk_seed_distribution`
/// - seed the `pk_generator` with sampled `pk_seed`
/// - generate the partition key with `pk_generator`
/// - compute the seed for the `column_generators` based on generated pk
/// - generate the rest of the row (seeding the `column_generators` with computed seed)
///
/// I think it's a great place to address how read and write workloads cooperate.
/// For reference, see: https://github.com/scylladb/cql-stress/pull/43#discussion_r1304274035.
///
/// At first, we need to notice that as long as `pk_seed_distribution` is a DETERMINISTIC distribution,
/// the row-generation logic is also deterministic.
/// The `pk_seed_distribution` is the one provided via CLI with either `-pop seq` or `-pop dist` option.
/// Notice that `-pop seq=1..5` is short for `-pop dist=SEQ(1..5)`.
///
/// Note: a deterministic distribution in this case, is a distribution which samples the exact same values
/// in each execution.
/// In c-s, there are only two deterministic distributions:
/// - FIXED
/// - SEQ
/// We call a distribution non-deterministic if the values it samples in each run may differ. It's the case
/// for all of the distributions that depend on some RNG (which is by default seeded with current time in millis)
/// e.g. UniformDistribution, GaussianDistribution (not yet implemented).
///
/// For example, each time we execute the command:
/// ```
/// ./cassandra-stress write n=100 -pop dist=SEQ(1..100)
/// ```
/// it will result in generating the exact same set of 100 rows (no matter the order of sampling - this distribution is shared across multiple threads).
///
/// Now, one can validate the inserted data with a read routine.
/// Each of these commands will be successful (meaning, the data will be successfully validated):
/// ```
/// ./cassandra-stress read n=100 -pop dist=SEQ(1..100)
/// ./cassandra-stress read n=100 -pop dist=UNIFORM(1..100)
/// ./cassandra-stress read n=100 -pop dist=GAUSSIAN(30..70)
/// ```
///
/// To be more precise: any read workload that samples the partition_key seeds from the distribution
/// sampling the values from the subset of range 1..100 (one used in the write routine) will successfully validate the data.
///
/// This also means, that if we used a NON-DETERMINISTIC distribution e.g. `UniformDistribution` (which is by default seeded with current time as millis)
/// in the write workload, most of the times, the read workload will result in a validation error.
/// For example:
/// ```
/// ./cassandra-stress write n=100 -pop dist=UNIFORM(1..100)
/// ./cassandra-stress read n=100 -pop dist=UNIFORM(1..100)
/// ```
/// will fail with a high probability.
///
/// There was a proposal to seed non-deterministic distributions with operation_id.
/// Consider introducing this improvement in the future. This would result in c-s frontend being fully deterministic,
/// no matter the distribution we sample the pk seeds from. I think it's a great improvement - unfortunately,
/// it's not how Java's c-s behaves.
/// Ref: https://github.com/scylladb/cql-stress/pull/45#discussion_r1312627399.
///
/// This is why, the write workload is almost always executed with the deterministic distribution
/// such as `SeqDistribution`. See usage examples in https://github.com/scylladb/scylla-cluster-tests.
///
/// Notice that, this also means we can insert the data using cql-stress' c-s frontend,
/// and then validate it using Java's implementation of c-s (and vice-versa).
pub struct RowGenerator {
    pk_seed_distribution: Arc<dyn Distribution>,
    pk_generator: Generator<HexBlob>,
    column_generators: Vec<Generator<Blob>>,
}

pub struct RowGeneratorFactory {
    pk_seed_distribution: Arc<dyn Distribution>,
    // TODO: Use settings to define pk_generator and column_generators
    // once -pop and -col options are supported.
    _settings: Arc<CassandraStressSettings>,
}

impl RowGenerator {
    pub fn generate_row(&mut self) -> Vec<CqlValue> {
        // +1 for partition_key.
        let row_length = self.column_generators.len() + 1;
        let mut result = Vec::with_capacity(row_length);

        // Sample the partition_key seed from the shared distribution.
        let pk_seed = self.pk_seed_distribution.next_i64();
        self.pk_generator.set_seed(pk_seed);
        let key = self.pk_generator.generate();

        // Compute the seed used for generating the rest of the row.
        let columns_seed = recompute_seed(0, &key);
        result.push(key);

        for column_generator in self.column_generators.iter_mut() {
            column_generator.set_seed(columns_seed);
            result.push(column_generator.generate());
        }

        result
    }
}

impl RowGeneratorFactory {
    pub fn new(settings: Arc<CassandraStressSettings>) -> Self {
        // TODO: adjust when `-pop` option is supported.
        let default_seq_range_end = settings
            .command_params
            .basic_params
            .operation_count
            .unwrap_or(1000000);
        let pk_seed_distribution =
            Arc::new(SeqDistribution::new(1, default_seq_range_end as i64).unwrap());

        Self {
            pk_seed_distribution,
            _settings: settings,
        }
    }

    pub fn create(&self) -> RowGenerator {
        // See https://github.com/scylladb/scylla-tools-java/blob/master/tools/stress/src/org/apache/cassandra/stress/settings/SettingsCommandPreDefined.java#L77.
        let pk_generator = Generator::new(
            HexBlob,
            GeneratorConfig::new(
                "randomstrkey",
                None,
                Some(Box::new(FixedDistribution::new(10))),
            ),
        );

        // TODO: adjust when `-col` option is supported.
        let column_generators = (0..5)
            .map(|n| {
                Generator::new(
                    Blob::default(),
                    GeneratorConfig::new(
                        &format!("randomstrC{}", n),
                        None,
                        Some(Box::new(FixedDistribution::new(34))),
                    ),
                )
            })
            .collect();

        RowGenerator {
            pk_seed_distribution: Arc::clone(&self.pk_seed_distribution),
            pk_generator,
            column_generators,
        }
    }
}