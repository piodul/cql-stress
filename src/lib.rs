#[macro_use]
extern crate async_trait;

#[cfg(test)]
pub(crate) mod test_util;

pub mod configuration;
pub mod distribution;
pub mod run;
pub mod sharded_stats;
pub mod version;

#[cfg(test)]
mod tests {
    use crate::test_util::new_test_session;
    use scylla::transport::PagingState;

    #[tokio::test]
    async fn test_can_connect() {
        let s = new_test_session().await;
        s.query_single_page("SELECT * FROM system.local", (), PagingState::start())
            .await
            .unwrap();
    }
}
