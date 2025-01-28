#[derive(Debug)]
pub struct VersionInfo {
    pub cql_stress_version: &'static str,
    pub cql_stress_build_date: &'static str,
    pub cql_stress_git_sha: &'static str,
    pub scylla_driver_version: &'static str,
    pub scylla_driver_date: &'static str,
    pub scylla_driver_sha: &'static str,
}

mod version_info {
    include!(concat!(env!("OUT_DIR"), "/version_info.rs"));
}

pub fn get_version_info() -> VersionInfo {
    VersionInfo {
        cql_stress_version: version_info::PKG_VERSION,
        cql_stress_build_date: version_info::COMMIT_DATE,
        cql_stress_git_sha: version_info::GIT_SHA,
        scylla_driver_version: version_info::SCYLLA_VERSION,
        scylla_driver_date: version_info::SCYLLA_RELEASE_DATE,
        scylla_driver_sha: version_info::SCYLLA_SHA,
    }
}

pub fn format_version_info() -> String {
    let info = get_version_info();
    format!(
        r#"{{"cql-stress":{{"version":"{}","commit_date":"{}","commit_sha":"{}"}},"scylla-driver":{{"version":"{}","commit_date":"{}","commit_sha":"{}"}}}}"#,
        info.cql_stress_version,
        info.cql_stress_build_date,
        info.cql_stress_git_sha,
        info.scylla_driver_version,
        info.scylla_driver_date,
        info.scylla_driver_sha,
    )
}
