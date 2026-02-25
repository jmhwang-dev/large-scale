import time

from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro

from service.utils.iceberg import *
from service.utils.schema.reader import AvscReader
from service.utils.spark import get_deserialized_avro_stream_df
from service.producer.bronze import *

from config.kafka import BOOTSTRAP_SERVERS_INTERNAL, BOOTSTRAP_SERVERS_EXTERNAL


class BaseStream(ABC):
    """
    Base Stream Job for Silver
    Input: Message
    Output: Table or Message
    """
    spark_session: Optional[SparkSession] = None
    dst_env: Optional[str] = None
    query_name: Optional[str] = None
    query_version: Optional[str] = None

    src_df: Optional[DataFrame] = None
    output_df: Optional[DataFrame] = None
    dst_avsc_reader: Optional[AvscReader] = None
    checkpoint_path: Optional[str] = None
    process_time: str = '5 seconds'

    @abstractmethod
    def extract(self,):
        pass

    @abstractmethod
    def transform(self,):
        pass

    def set_byte_stream(self, key_column:str, value_columns: List[str]):
        self.output_df = self.output_df.select(
            F.col(key_column).cast("string").alias("key"),
            to_avro(
                F.struct(*value_columns),
                self.dst_avsc_reader.schema_str).alias('value'))
        
        schema_id_hex = f"{self.dst_avsc_reader.schema_id:08x}"  # AvscReader에 schema_id 추가 필요
        self.output_df = self.output_df.withColumn(
            "value",
            F.concat(
                F.unhex(F.lit("00")),  # Magic byte
                F.unhex(F.lit(schema_id_hex)),  # Schema ID
                F.col("value")  # Avro binary data
            )
        )

    def get_query(self, use_interal:bool = False):
        self.extract()
        self.transform()

        bootstrap_server = BOOTSTRAP_SERVERS_EXTERNAL if not use_interal else BOOTSTRAP_SERVERS_INTERNAL
        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_server) \
            .option("topic", self.dst_avsc_reader.table_name) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()
    
    @staticmethod
    def get_topic_df(micro_batch:DataFrame, dst_avsc_reader: AvscReader) -> DataFrame:
        ser_df = micro_batch.filter(F.col("topic") == dst_avsc_reader.table_name)
        key_column = BaseStream.get_avro_key_column(dst_avsc_reader.table_name)
        return get_deserialized_avro_stream_df(ser_df, key_column, dst_avsc_reader.schema_str)
    
    @staticmethod
    def get_avro_key_column(topic_name):
        if topic_name == OrderStatusBronzeProducer.dst_topic:
            return OrderStatusBronzeProducer.key_column

        elif topic_name == ProductBronzeProducer.dst_topic:
            return ProductBronzeProducer.key_column
        
        elif topic_name == CustomerBronzeProducer.dst_topic:
            return CustomerBronzeProducer.key_column
        
        elif topic_name == SellerBronzeProducer.dst_topic:
            return SellerBronzeProducer.key_column
        
        elif topic_name == GeolocationBronzeProducer.dst_topic:
            return GeolocationBronzeProducer.key_column
        
        elif topic_name == EstimatedDeliberyDateBronzeProducer.dst_topic:
            return EstimatedDeliberyDateBronzeProducer.key_column
        
        elif topic_name == OrderItemBronzeProducer.dst_topic:
            return OrderItemBronzeProducer.key_column
        
        elif topic_name == PaymentBronzeProducer.dst_topic:
            return PaymentBronzeProducer.key_column
        
        elif topic_name == ReviewBronzeProducer.dst_topic:
            return ReviewBronzeProducer.key_column
        
        elif topic_name == ReviewBronzeProducer.dst_topic:
            return ReviewBronzeProducer.key_column
        
        # silver
        # TODO: resolve hard coding
        elif topic_name == 'geo_coord':
            return 'zip_code'
        
        elif topic_name in ['order_event', 'customer_order']:
            return 'order_id'
        
        elif topic_name == 'olist_user':
            return 'user_id'
        
        elif topic_name == 'product_metadata':
            return 'product_id'