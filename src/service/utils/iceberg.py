from enum import Enum

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, min, max

from service.utils.schema.registry_manager import *

class TimeBoundary(Enum):
    EARLIEST = "Earlies"
    LATEST = "Latest"

def init_catalog(spark:SparkSession, namespace:str, table_name: str = '', is_drop: bool = False):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    if not is_drop:
        return

    if len(table_name) > 0:
        spark.sql(f'drop table if exists {namespace}.{table_name} purge')
        return
    
    for table in [row.tableName for row in spark.sql(f'show tables in {namespace}').collect()]:
        spark.sql(f'drop table if exists {namespace}.{table} purge')
        print(f'drop done: {namespace}.{table}')
        # spark.sql(f'DESCRIBE FORMATTED {namespace}.{table}').show()

def write_iceberg(spark_session: SparkSession, df: DataFrame, dst_table_identifier: str, mode:str = '') -> None:

    if mode == '':
        raise TypeError(f"write_iceberg() missing 1 required positional argument: 'mode'. mode must be one of 'w' or 'a'")
    
    if not spark_session.catalog.tableExists(dst_table_identifier):
        print(f"{dst_table_identifier} does not exist")
        df.writeTo(dst_table_identifier).create()
        print(f"{dst_table_identifier} has been created")
        return

    if mode == 'w':
        df.writeTo(dst_table_identifier).create()
        print(f"{dst_table_identifier} has been replaced")

    elif mode == 'a':
        df.writeTo(dst_table_identifier).append()
        print(f"{dst_table_identifier} has been appended")
    
    elif mode == 'o':
        # TODO: To overwrite, modify parameter (add `condition`).
        # df.writeTo(dst_table_identifier).overwrite()
        # print(f"{dst_table_identifier} has overwrittend: {df.count()}")
        pass
    
    return

def get_snapshot_details(df: DataFrame, boundary: str) -> Optional[dict]:
    if df.isEmpty(): return None
    order_col = col("committed_at").asc() if boundary == TimeBoundary.EARLIEST else col("committed_at").desc()
    row = df.orderBy(order_col).select("snapshot_id", "committed_at").first()
    return {"snapshot_id": row["snapshot_id"], "committed_at": row["committed_at"]} if row else None

def get_last_processed_snapshot_id(spark: SparkSession, table: str, job: str, src: str) -> Optional[int]:
    if not spark.catalog.tableExists(table): return None
    row = spark.read.table(table).filter(f"app_name = '{job}' AND src = '{src}'").first()
    return row["last_processed_snapshot_id"] if row else None

def get_snapshot_df(spark: SparkSession, table: str) -> DataFrame:
    return spark.sql(f"SELECT * FROM {table}.snapshots")

def get_snapshot_id_by_time_boundary(snapshots_df: DataFrame, time_boundary: TimeBoundary):
    try:
        if snapshots_df.count() == 0:
            print("스냅샷이 존재하지 않습니다.")
            return None
        agg_func = min if time_boundary == TimeBoundary.EARLIEST else max
        target_timestamps = snapshots_df.select(
            agg_func(col("committed_at")).alias("target_timestamp")
        ).first()

        target_ts = target_timestamps["target_timestamp"]
        target_snapshot_id = snapshots_df.filter(col("committed_at") == target_ts).select("snapshot_id").first()[0]
        print(f"{time_boundary.value} committed_at: {target_ts} 에 해당하는 snapshot_id: {target_snapshot_id}")
        return target_snapshot_id

    except Exception as e:
        print(f"오류가 발생했습니다: {e}")
        return None, None