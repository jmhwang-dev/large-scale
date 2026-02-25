import time
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from confluent_kafka.schema_registry.error import SchemaRegistryError
from pyspark.sql.streaming.query import StreamingQuery

from service.utils.schema.reader import AvscReader
from service.utils.spark import get_kafka_stream_df, stop_streams, run_stream_queries, start_console_stream
from service.producer.bronze import BronzeAvroSchema
from service.pipeline.stream.base import BaseStream

def load_stream_to_iceberg(deserialized_df: DataFrame, dst_table_identifier: str, option: dict[str, str]) -> StreamingQuery:
    """
    options
    # spark.sql.streaming.checkpointLocation
    - df.writeStream.option("checkpointLocation", "s3a://your-bucket/checkpoints")
        : 스트리밍 체크포인트 저장 경로. Kafka 오프셋, 상태 등을 저장해 장애 복구 지원.

    - df.writeStream.trigger(processingTime="X seconds")
        : 마이크로 배치 실행 간격. 데이터 처리 주기를 조절.
    
    -  df.writeStream.option("fanout.enabled", "false")
        : Iceberg 스트리밍 쓰기 시 파티션별 파일 생성 방식 제어. false로 파일 수 감소.

    # ceberg 테이블 파일 compaction 및 오래된 스냅샷 삭제. 스트리밍 후 소규모 파일 문제 해결.
        - spark.sql("CALL iceberg_catalog.system.rewrite_data_files('your_table')") // OPTIMIZE
        - spark.sql("CALL iceberg_catalog.system.expire_snapshots('your_table', TIMESTAMP '2025-08-13 00:00:00')") // expire_snapshots
    """

    # # TODO: partition 유무 성능 확인
    # SPARK_SESSION.sql(f"""
    #     CREATE TABLE IF NOT EXISTS {table_identifier}
    #     USING iceberg
    # """)

    s3_uri = dst_table_identifier.replace('.', '/') # ex) bronze/customer
    return deserialized_df.writeStream \
        .queryName(f"Load to {dst_table_identifier}") \
        .outputMode("append") \
        .format("iceberg") \
        .option("checkpointLocation", f"s3a://warehousedev/{option['app_name']}/{option['dst_env']}/{s3_uri}/checkpoint/{option['query_version']}") \
        .option("fanout.enabled", "false") \
        .trigger(processingTime=option['process_time']) \
        .toTable(dst_table_identifier)


def load_cdc_stream(spark_session: SparkSession, option_dict: dict[str, str], logger) -> List[StreamingQuery]:

    try:
        query_list: List[StreamingQuery] = []
        src_avsc_filenames = BronzeAvroSchema.get_all_filenames()

        for avsc_filename in src_avsc_filenames:
            
            dst_avsc_reader = AvscReader(avsc_filename)
            deserialized_stream = BaseStream.get_topic_df(
                get_kafka_stream_df(spark_session, dst_avsc_reader.table_name, 200),
                dst_avsc_reader
            )

            query = load_stream_to_iceberg(deserialized_stream, dst_avsc_reader.dst_table_identifier, option_dict)
            query_list.append(query)
        
        run_stream_queries(spark_session, query_list, logger)

    except SchemaRegistryError as e:
        # `AvscReader()`에서 스키마가 없을 때 발생하는 예외
        print(e)
        stop_streams(spark_session, query_list)
        exit()

def load_medallion_layer(micro_batch_df:DataFrame, batch_id: int):
    print(f"Processing Batch ID: {batch_id}")

    for topic_name in BronzeAvroSchema.get_all_filenames():
        try:            
            dst_avsc_reader = AvscReader(topic_name, False)
            topic_df = micro_batch_df.filter(F.col("topic") == topic_name)
            deserialized_df = BaseStream.get_topic_df(topic_df, dst_avsc_reader)

            record_count = deserialized_df.count()
            if record_count == 0:
                print(f"[{dst_avsc_reader.dst_table_identifier:<36}] No records")
                continue
            
            start = time.time()
            deserialized_df.write \
                .format("iceberg") \
                .mode("append") \
                .saveAsTable(dst_avsc_reader.dst_table_identifier)
            end = time.time()
            print(f"[{dst_avsc_reader.dst_table_identifier:<36}] has saved {record_count} rows (Processing time: {end-start:.2f} sec)")

        except Exception as e:
            print(f"[{dst_avsc_reader.dst_table_identifier:<36}] {e}")

def load_cdc_batch(spark_session: SparkSession, option_dict: dict[str, str]) -> None:
    """
    checkpointLocation convetion
    - f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    참고
    - micro batch로 모든 토픽을 순차적으로 처리하므로, `{self.dst_layer}/{self.dst_name}`는 `bronze/batch`로 고정
    """
    src_avsc_filenames = BronzeAvroSchema.get_all_filenames()
    kafka_stream = get_kafka_stream_df(spark_session, src_avsc_filenames, max_offset_per_trigger=1000)

    query = kafka_stream.writeStream \
        .foreachBatch(load_medallion_layer) \
        .queryName("load_cdc") \
        .option("checkpointLocation", f"s3a://warehousedev/{option_dict['app_name']}/{option_dict['dst_env']}/bronze/batch/checkpoint/{option_dict['query_version']}") \
        .trigger(processingTime="20 seconds") \
        .start()
        
    query.awaitTermination()
