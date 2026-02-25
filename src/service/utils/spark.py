import uuid
import traceback
from logging import Logger
from typing import Iterable, Union, List

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.utils import StreamingQueryException, AnalysisException

from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, expr, concat_ws, struct
from pyspark.sql.avro.functions import from_avro

from config.spark import *
from config.kafka import *

from service.producer.silver import SparkProducer
from service.utils.logger import write_log, Logger

def stop_streams(query_list: List[StreamingQuery], logger: Logger) -> None:
    for query in query_list:
        if query.isActive:
            try:
                query.stop()
                logger.info(f"Successfully stopped query: {query.name} (ID: {query.id})")
            except Exception as e:
                logger.error(f"Failed to stop query {query.name} (ID: {query.id}): {e}", exc_info=True)
        else:
            logger.debug(f"Query {query.name} (ID: {query.id}) is already inactive. Status: {query.status}")

def get_serialized_df(serializer_udfs: dict[str, ], transformed_df: DataFrame, producer_class: SparkProducer):
    serializer_udf = serializer_udfs.get(producer_class.dst_topic)
    if not serializer_udf:
        raise ValueError(f"Warning: Serializer UDF for destination topic '{producer_class.dst_topic}' not found. Skipping.")
        
    df_to_publish = transformed_df.select(
        concat_ws("-", *[col(c).cast("string") for c in producer_class.key_column]).alias("key"),
        # struct('*')를 사용하여 데이터프레임의 모든 컬럼을 value_struct로 만듬
        struct(*transformed_df.columns).alias("value_struct")
    )

    return df_to_publish.withColumn(
        "value", serializer_udf(col("value_struct"))
    )

def get_spark_session(app_name: str=None, dev=False) -> SparkSession:
    """
    confs

    - spark.conf.set("spark.sql.shuffle.partitions", "50")
        : 셔플 작업의 파티션 수. 스트리밍 병렬 처리 성능에 영향.
    
    - spark.conf.set("spark.sql.streaming.commitIntervalMs", "5000")
        : Kafka 오프셋 커밋 주기(ms). 빠른 커밋 vs 안정성 조절.

    - spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "268435456") // 256MB
        : minIO에 업로드 시 멀티파트 크기. 큰 데이터 전송 성능 최적화.

    """
        # .set("spark.eventLog.enabled", "true") \
        # .set("spark.eventLog.dir", "file:///opt/spark/logs/") \
        # .set("spark.history.fs.logDirectory", "file:///opt/spark/logs/") \
        # .set("spark.history.ui.port", "18080") \
        # .set("spark.metrics.appStatusSource.enabled", "true") \
    dev_conf = SparkConf().setAppName("MySparkApp") \
        .set("spark.sql.adaptive.enabled", "false") \
        .set("spark.sql.streaming.metricsEnabled", "true") \
        .set("spark.driver.memory", "2g") \
        .set("spark.driver.cores", "2") \
        .set("spark.driver.maxResultSize", "2g") \
        .set("spark.executor.instances", "1") \
        .set("spark.executor.cores", "4") \
        .set("spark.executor.memory", "3g") \
        .set("spark.dynamicAllocation.enabled", "true") \
        .set("spark.dynamicAllocation.minExecutors", "1") \
        .set("spark.dynamicAllocation.maxExecutors", "3") \
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .set("spark.sql.catalog.warehousedev", "org.apache.iceberg.spark.SparkCatalog") \
        .set("spark.sql.catalog.warehousedev.type", "rest") \
        .set("spark.sql.catalog.warehousedev.uri", "http://rest-catalog:8181") \
        .set("spark.sql.catalog.warehousedev.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .set("spark.sql.catalog.warehousedev.warehouse", "s3://warehousedev") \
        .set("spark.sql.catalog.warehousedev.s3.endpoint", "http://minio:9000") \
        .set("spark.sql.catalog.warehousedev.s3.path-style-access", "true") \
        .set("spark.sql.catalog.warehousedev.s3.region", "us-east-1") \
        .set("spark.sql.catalog.warehousedev.s3.access-key-id", "minioadmin") \
        .set("spark.sql.catalog.warehousedev.s3.secret-access-key", "minioadmin") \
        .set("spark.sql.defaultCatalog", "warehousedev") \
        .set("spark.sql.catalogImplementation", "in-memory") \
        .set("spark.executor.extraJavaOptions", "-Daws.region=us-east-1") \
        .set("spark.executorEnv.AWS_REGION", "us-east-1") \
        .set("spark.executorEnv.AWS_ACCESS_KEY_ID", "minioadmin") \
        .set("spark.executorEnv.AWS_SECRET_ACCESS_KEY", "minioadmin") \
        .set("spark.driver.extraJavaOptions", "-Daws.region=us-east-1") \
        .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .set("spark.hadoop.fs.s3a.path.style.access", "true") \
        .set("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .set("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # .set("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # 추가

    if not dev:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        # spark = SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC").getOrCreate()
            
    else:
        spark = SparkSession.builder.config(conf=dev_conf).getOrCreate()
        # spark = SparkSession.builder.config(conf=dev_conf).config("spark.sql.session.timeZone", "UTC").getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def start_console_stream(df: DataFrame, output_mode: str='append', checkpoint_dir: str = '') -> None:
    # .queryName(query_name)
    options = {
        "truncate": "True",
        "numRows": "100",
        "checkpointLocation": f"s3a://tmp/{uuid.uuid4() if len(checkpoint_dir) == 0 else checkpoint_dir}",
        "trigger": {"processingTime": "5 second"}
    }

    df.writeStream \
        .outputMode(output_mode) \
        .format("console") \
        .options(**options) \
        .start() \
        .awaitTermination()

def get_kafka_stream_df(spark_session: SparkSession, topic_names: Union[Iterable[str], str], max_offset_per_trigger: int=50, use_interal:bool = False) -> DataFrame:
    """
    - .option("maxOffsetsPerTrigger", "10000")
        : 배치당 처리할 Kafka 오프셋 최대 수. 처리량 제어.
    """
    # The type of `spark_session.readStream` is `DataStreamReader`
    # .option("maxOffsetsPerTrigger", "20000")  # 배치당 최대 오프셋, 조정 필요
    # 추가: spark.streaming.kafka.maxRatePerPartition (파티션당 초당 메시지 수 제한, 예: 1000) 설정으로 입력 속도 제어.
    # .option("groupId", "bronze2silver") \

    if isinstance(topic_names, str):
        _topic_names = [topic_names]
    else:
        _topic_names = topic_names

    kafka_bootstrap_servers = BOOTSTRAP_SERVERS_EXTERNAL if not use_interal else BOOTSTRAP_SERVERS_INTERNAL
    
    src_stream_df = spark_session.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", ','.join(_topic_names)) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", f"{max_offset_per_trigger}") \
        .load()
    return src_stream_df.select(col("key"), col("value"), col("topic"))


def get_deserialized_avro_stream_df(kafka_stream_df:DataFrame, key_column: str, avsc_schema_str: str) -> DataFrame:
    deserialized_key = col('key').cast("string").alias(key_column)
    deserialized_value = \
        from_avro(
            expr("substring(value, 6, length(value)-5)"), # Magic byte + schema id 제거
            avsc_schema_str,
            DESERIALIZE_OPTIONS
        ).alias("data")
    return kafka_stream_df.select(deserialized_key, deserialized_value).select(key_column, "data.*")


def run_stream_queries(spark_session: SparkSession, query_list: List[StreamingQuery], logger: Logger):
    logger.info(f"Started {len(query_list)} streaming queries")
    
    while any(q.isActive for q in query_list):
        try:
            # 모든 쿼리의 종료를 병렬로 감지 (타임아웃 적용)
            terminated = spark_session.streams.awaitAnyTermination()  # 밀리초 단위
            if not terminated:
                logger.debug("No query terminated in this cycle, checking health...")
                for query in query_list:
                    if query.isActive:
                        logger.debug(f"Query {query.name} (ID: {query.id}) is active. Last progress: {query.lastProgress}")
                    else:
                        logger.info(f"Query {query.name} (ID: {query.id}) terminated. Status: {query.status}")
                        if query.exception():
                            logger.error(f"Query {query.name} failed with exception: {query.exception()}")
                continue
            
            # 비활성 쿼리 정리
            _cleanup_inactive_queries(query_list, logger)
                
        except StreamingQueryException as e:
            logger.error(f"StreamingQueryException: {e}", exc_info=True)
            _cleanup_inactive_queries(query_list, logger)
            
        except AnalysisException as e:
            logger.error(f"AnalysisException in streaming query: {e}", exc_info=True)
            _cleanup_inactive_queries(query_list, logger)
            
        except KeyboardInterrupt:
            logger.info("Shutdown requested by user...")
            stop_streams(query_list, logger)
            return
            
        except Exception as e:
            logger.error(f"Unexpected error in stream manager: {e}", exc_info=True)
            _cleanup_inactive_queries(query_list, logger)
    
    logger.info("All streaming queries have terminated.")


def _cleanup_inactive_queries(query_list: List[StreamingQuery], logger: Logger):
    """죽은 쿼리 정리 헬퍼"""
    for q in query_list[:]:
        if not q.isActive:
            exception = q.exception() or "Unknown"
            status_msg = q.status.get('message', 'No message') if q.status else 'No status'
            logger.error(f"Query {q.name} failed or stopped. "
                         f"Exception: {exception}, Status: {status_msg}")
            if q.lastProgress:
                logger.error(f"Last progress: {q.lastProgress}")
            query_list.remove(q)