"""
common.spark_helper
===================
Shared utilities for Spark session creation, HDFS operations,
and MySQL helpers used across all pipeline scripts.
"""

import os
import glob
import subprocess

from pyspark.sql import SparkSession
from config_env import env, env_bool, env_int


# ────────────────────── Environment defaults ──────────────────
PROJECT_DIR          = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
HIVE_DB              = env("TD_CHURN_HIVE_DB", "td_churn")
WAREHOUSE_DIR        = env("TD_CHURN_HIVE_WAREHOUSE_DIR", "/user/hive/warehouse")
HIVE_METASTORE_URIS  = env("TD_CHURN_HIVE_METASTORE_URIS", "thrift://master:9083")


def _find_mysql_jdbc_jar():
    """
    Locate MySQL JDBC connector JAR for spark.jars config.
    Priority: TD_CHURN_MYSQL_JAR env > auto-detect in common paths.
    Returns absolute path or None.
    """
    explicit = env("TD_CHURN_MYSQL_JAR", "")
    if explicit and os.path.isfile(explicit):
        return explicit

    search_patterns = [
        "/usr/local/spark/jars/mysql-connector*.jar",
        "/opt/spark/jars/mysql-connector*.jar",
        "/usr/share/java/mysql-connector-java*.jar",
        "/usr/share/java/mysql-connector-j*.jar",
    ] # Normally MySQL-connector's location
    # Also try SPARK_HOME if set
    spark_home = os.environ.get("SPARK_HOME", "")
    if spark_home:
        search_patterns.insert(0, os.path.join(spark_home, "jars", "mysql-connector*.jar"))

    for pattern in search_patterns:
        hits = sorted(glob.glob(pattern))
        if hits:
            return hits[-1]   # newest version
    return None


def build_spark_session(app_name, enable_hive=True, shuffle_partitions=None):
    """
    Build and return a SparkSession with common configs.

    Parameters
    ----------
    app_name : str
        Spark application name.
    enable_hive : bool
        Whether to enable Hive support (metastore, warehouse).
    shuffle_partitions : int or None
        Override spark.sql.shuffle.partitions. None uses env default.

    Returns
    -------
    SparkSession
    """
    if shuffle_partitions is None:
        shuffle_partitions = env_int("TD_CHURN_SPARK_SHUFFLE_PARTITIONS", 200)

    # Auto-detect MySQL JDBC JAR for spark.jars
    mysql_jar = _find_mysql_jdbc_jar()
    if mysql_jar:
        print(f"[SPARK] MySQL JDBC JAR found: {mysql_jar}")
    else:
        print("[WARN] MySQL JDBC JAR not found. JDBC writes may fail with "
              "ClassNotFoundException. Set TD_CHURN_MYSQL_JAR or install the "
              "JAR into $SPARK_HOME/jars/.")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.orc.impl", "native")
        # Memory configs (env-overridable, safe defaults for small VM cluster)
        .config("spark.driver.memory",
                env("TD_CHURN_SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory",
                env("TD_CHURN_SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.executor.memoryOverhead",
                env("TD_CHURN_SPARK_EXECUTOR_OVERHEAD", "512m"))
        .config("spark.driver.maxResultSize",
                env("TD_CHURN_SPARK_MAX_RESULT_SIZE", "512m"))
        # Suppress verbose INFO during Spark/YARN init
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.rootCategory=WARN,console")
        .config("spark.executor.extraJavaOptions",
                "-Dlog4j.rootCategory=WARN,console")
    )

    if mysql_jar:
        builder = builder.config("spark.jars", mysql_jar)

    if enable_hive:
        builder = (
            builder
            .enableHiveSupport()
            .config("hive.metastore.uris", HIVE_METASTORE_URIS)
            .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if enable_hive:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
        spark.sql(f"USE {HIVE_DB}")

    return spark


# ────────────────────── Shell / HDFS helpers ──────────────────

def run_cmd(cmd, check=True, capture_output=False):
    """
    Execute a shell command. Raises RuntimeError on failure by default.
    """
    if capture_output:
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if check and result.returncode != 0:
            raise RuntimeError(
                f"Command failed: {' '.join(cmd)}\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )
        return result
    else:
        return subprocess.run(cmd, check=check)


def hdfs_path_exists(path):
    """Return True if the HDFS path exists."""
    result = subprocess.run(["hdfs", "dfs", "-test", "-e", path])
    return result.returncode == 0


def reset_hdfs_dir(path):
    """
    Remove and recreate an HDFS directory.
    Suitable for full-refresh of external non-partitioned tables.
    """
    run_cmd(["hdfs", "dfs", "-rm", "-r", "-f", path], check=False)
    run_cmd(["hdfs", "dfs", "-mkdir", "-p", path], check=True)


# ────────────────────── MySQL helpers ─────────────────────────

def mysql_exec_sql(spark, url, user, password, sql):
    """
    Execute a single MySQL statement (DELETE / TRUNCATE etc.) via
    the mysql CLI client, bypassing JVM classloader issues entirely.
    The ``spark`` parameter is kept for API compatibility but not used.
    """
    import re, subprocess
    m = re.search(r'jdbc:mysql://([^:/]+):?(\d+)?/([^?]+)', url)
    if not m:
        raise ValueError(f"Cannot parse JDBC URL: {url}")
    host = m.group(1)
    port = m.group(2) or '3306'
    db   = m.group(3)

    result = subprocess.run(
        ['mysql', '-h', host, '-P', port, '-u', user,
         f'--password={password}', '--batch', db, '-e', sql],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"mysql CLI failed: {result.stderr.strip()}")


def write_mysql_idempotent(spark, sdf, table, url, user, password,
                           delete_sql=None):
    """
    Write a Spark DataFrame to MySQL idempotently:
    1. Execute optional delete_sql (e.g. DELETE WHERE stat_date=...).
    2. Append rows.

    Parameters
    ----------
    spark : SparkSession
    sdf : DataFrame
    table : str
    url, user, password : JDBC credentials
    delete_sql : str or None
        SQL to run before writing (for idempotent upsert).
    """
    if delete_sql:
        mysql_exec_sql(spark, url, user, password, delete_sql)

    sdf.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
