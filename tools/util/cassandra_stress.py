import subprocess
import os
import random
from datetime import datetime
from os.path import dirname
from collections import namedtuple


# scylla-tools-java is based on 3.* version of Apache Cassandra.
# Notice that Apache c-s changed generation logic in version 4.0.0.
# See: https://github.com/apache/cassandra/commit/f1f5f194620d3f9e11492f0051b6b71018033413
DEFAULT_CASSANDRA_VERSION = "3.11.18"
ROOT_DIRECTORY = dirname(dirname(dirname(__file__)))
DOWNLOAD_DIRECTORY_NAME = os.path.join(ROOT_DIRECTORY, "cassandra-download")

DEFAULT_TIMESTAMP_FORMAT = "%Y%m%d_%Hh%Mm%Ss"

TEST_PROFILES_DIRECTORY = os.path.join(
    ROOT_DIRECTORY, "tools", "util", "profiles")


Keyspaces = namedtuple("Keyspaces", ["ks_cassandra", "ks_cqlstress"])


CSCliRuntimeArguments = namedtuple("CSCliRuntimeArguments", [
                                   "workload_size", "concurrency"])
DEFAULT_RUNTIME_ARGUMENTS = CSCliRuntimeArguments(
    workload_size=100, concurrency=1)


def prepare_args(command, node_ip, keyspace, runtime_args: CSCliRuntimeArguments = DEFAULT_RUNTIME_ARGUMENTS):
    return [command, "no-warmup", f"n={runtime_args.workload_size}",
            "-node", node_ip,
            "-rate", f"threads={runtime_args.concurrency}",
            "-schema", f"keyspace={keyspace}"]


# Default query name used in test profiles.
DEFAULT_TEST_QUERY_NAME = "test_query"


def prepare_user_args(node_ip, profile_name, runtime_args: CSCliRuntimeArguments = DEFAULT_RUNTIME_ARGUMENTS):
    return ["user", f"profile={profile_name}",
            "no-warmup", f"n={runtime_args.workload_size}",
            "-node", node_ip,
            "-rate", f"threads={runtime_args.concurrency}"]


def generate_random_keyspaces(timestamp_format=DEFAULT_TIMESTAMP_FORMAT):
    r = random.randint(0, 100000)
    now = datetime.now().strftime(timestamp_format)
    ks_cassandra = f"ks_cassandra_{r}_{now}"
    ks_cqlstress = f"ks_cqlstress_{r}_{now}"
    return Keyspaces(ks_cassandra=ks_cassandra, ks_cqlstress=ks_cqlstress)


class CSCliRunner:
    def __init__(self, stress_cmd):
        self.stress_cmd = stress_cmd

    def run(self, command, node_ip, keyspace, runtime_args: CSCliRuntimeArguments):
        args = self.stress_cmd + \
            prepare_args(command, node_ip, keyspace, runtime_args)
        subprocess.run(args=args, check=True)

    def prepare_user_args(self, node_ip, profile_name, query_name=DEFAULT_TEST_QUERY_NAME, runtime_args: CSCliRuntimeArguments = DEFAULT_RUNTIME_ARGUMENTS):
        full_profile_name = os.path.join(TEST_PROFILES_DIRECTORY, profile_name)
        return ["user", f"profile={full_profile_name}", f"ops({query_name}=1)",
                "no-warmup", f"n={runtime_args.workload_size}",
                "-node", node_ip,
                "-rate", f"threads={runtime_args.concurrency}",
                f"-pop seq=1..{runtime_args.workload_size}"]

    def run_user(self, node_ip, profile_name, runtime_args: CSCliRuntimeArguments):
        args = self.stress_cmd + \
            self.prepare_user_args(
                node_ip=node_ip, profile_name=profile_name, runtime_args=runtime_args)
        subprocess.run(args=args, check=True)


class CassandraStress(CSCliRunner):
    def __init__(self, cassandra_version=DEFAULT_CASSANDRA_VERSION):
        cassandra_dir = f"apache-cassandra-{cassandra_version}"
        cassandra_tar = f"{cassandra_dir}-bin.tar.gz"

        abs_cassandra_dir = os.path.join(
            DOWNLOAD_DIRECTORY_NAME, cassandra_dir)
        abs_cassandra_tar = os.path.join(
            DOWNLOAD_DIRECTORY_NAME, cassandra_tar)

        if os.path.exists(abs_cassandra_dir):
            # Cassandra already fetched.
            print(
                f"Cassandra {cassandra_version} already installed. Skipping the download phase.")
        else:
            # Fetch cassandra.
            print(
                f"Fetching cassandra {cassandra_version} to {DOWNLOAD_DIRECTORY_NAME}")
            # https://dlcdn.apache.org/cassandra/3.11.18/apache-cassandra-3.11.18-bin.tar.gz
            cassandra_url = f"https://dlcdn.apache.org/cassandra/{cassandra_version}/{cassandra_tar}"
            subprocess.run(args=["wget", "-P", DOWNLOAD_DIRECTORY_NAME,
                           "-N", "--no-verbose", cassandra_url], check=True)

            # Extract cassandra
            print(f"Extracting cassandra {cassandra_version}")
            subprocess.run(args=["tar", "-xzf", abs_cassandra_tar,
                           "--directory", DOWNLOAD_DIRECTORY_NAME], check=True)
            print(f"Extracted cassandra to {abs_cassandra_dir}")

        stress_cmd = [os.path.join(
            abs_cassandra_dir, "tools", "bin", "cassandra-stress")]
        super().__init__(stress_cmd=stress_cmd)


class CqlStressCassandraStress(CSCliRunner):
    def __init__(self):
        stress_cmd = ["cargo", "run", "--features", "user-profile", "--bin",
                      "cql-stress-cassandra-stress", "--"]
        super().__init__(stress_cmd=stress_cmd)
