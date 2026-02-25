from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws
from service.producer.base.common import *

from config.kafka import BOOTSTRAP_SERVERS_INTERNAL

class SparkProducer(BaseProducer):
    @classmethod
    def generate_message(cls, data: DataFrame) -> DataFrame:
        """Generate Kafka message by adding a message key column."""
        return data.withColumn(cls.message_key_col, concat_ws('-', *[col(c).cast("string") for c in cls.key_column]))

    @classmethod
    def publish(cls, serialized_df: DataFrame):
        """
        직렬화가 완료된 DataFrame (key, value 컬럼 포함)을
        Spark의 내장 Kafka Sink를 사용하여 발행합니다.
        """
        if serialized_df.isEmpty():
            return
            
        (serialized_df
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL)
            .option("topic", cls.dst_topic) # .value 추가 (Enum 객체이므로)
            .save())
        
        print(f"# of published records: {serialized_df.count()}. Topic name: {cls.dst_topic}")