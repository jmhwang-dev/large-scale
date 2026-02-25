import uuid
import time

from pprint import pprint
from typing import Optional
from abc import ABC, abstractmethod
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from functools import reduce

from service.utils.schema.reader import AvscReader
from service.utils.schema.avsc import *
from service.utils.spark import get_spark_session

from service.utils.iceberg import write_iceberg


class BaseBatch(ABC):
    """
    Base Batch Job for Silver
    Input: Table
    Output: Table
    """
    is_debug: bool = False
    spark_session: Optional[SparkSession] = None
    app_name: Optional[str] = None
    output_df: Optional[DataFrame] = None

    src_table_identifier: str = ''  # to update watermark table
    dst_avsc_reader: Optional[AvscReader] = None

    def __init__(self, app_name: str, dst_avsc_filename: str, spark_session: Optional[SparkSession] = None, is_stream: bool=False):
        self.app_name = app_name
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.app_name)

        self.dst_avsc_reader = AvscReader(dst_avsc_filename, use_internal=False, is_stream=is_stream)
        dst_scheam = BaseBatch.get_schema(self.spark_session, self.dst_avsc_reader)
        BaseBatch.initialize_dst_table(self.spark_session, dst_scheam, self.dst_avsc_reader.dst_table_identifier)

    @abstractmethod
    def extract(self,):
        pass

    @abstractmethod
    def transform(self,):
        pass

    @abstractmethod
    def load(self,):
        """
        upsert table
        """
        pass

    @staticmethod
    def check_table(spark_session:SparkSession, dst_table_identifier: str):
        while True:
            if spark_session.catalog.tableExists(dst_table_identifier):
                break
            spark_session.catalog.refreshTable(dst_table_identifier)
            time.sleep(5)

    @staticmethod
    def get_schema(spark_session: SparkSession, avsc_reader: AvscReader) -> StructType:
        # 키 컬럼 매핑
        KEY_COL_MAPPING = {
            BronzeAvroSchema.CUSTOMER: StructField('customer_id', StringType(), False),
            BronzeAvroSchema.ESTIMATED_DELIVERY_DATE: StructField('order_id', StringType(), False),
            BronzeAvroSchema.GEOLOCATION: StructField('zip_code', IntegerType(), False),
            BronzeAvroSchema.ORDER_ITEM: StructField('order_id', StringType(), False),
            BronzeAvroSchema.ORDER_STATUS: StructField('order_id', StringType(), False),
            BronzeAvroSchema.PAYMENT: StructField('order_id', StringType(), False),
            BronzeAvroSchema.PRODUCT: StructField('product_id', StringType(), False),
            BronzeAvroSchema.REVIEW: StructField('review_id', StringType(), False),
            BronzeAvroSchema.SELLER: StructField('seller_id', StringType(), False),

            SilverAvroSchema.CUSTOMER_ORDER: StructField('order_id', StringType(), False),
            SilverAvroSchema.GEO_COORD: StructField('zip_code', IntegerType(), False),
            SilverAvroSchema.OLIST_USER: StructField('user_id', StringType(), False),
            SilverAvroSchema.ORDER_EVENT: StructField('order_id', StringType(), False),
            SilverAvroSchema.PRODUCT_METADATA: StructField('product_id', StringType(), False),
            SilverAvroSchema.REVIEW_METADATA: StructField('order_id', StringType(), False),     # consider repartition

            SilverAvroSchema.WATERMARK: None,
            GoldAvroSchema.DIM_USER_LOCATION: None,
            GoldAvroSchema.FACT_ORDER_LEAD_DAYS: None,
            GoldAvroSchema.FACT_REVIEW_ANSWER_LEAD_DAYS: None,
            GoldAvroSchema.FACT_ORDER_DETAIL: None,
            GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT: None,
        }
        
        # Avro 스키마 파싱
        jvm = spark_session._jvm
        try:
            avro_schema_java = jvm.org.apache.avro.Schema.Parser().parse(avsc_reader.schema_str)
            converted = jvm.org.apache.spark.sql.avro.SchemaConverters.toSqlType(avro_schema_java)
            dst_table_schema = StructType.fromJson(json.loads(converted.dataType().json()))
        except Exception as e:
            raise ValueError(f"Failed to convert Avro schema to Spark schema: {str(e)}")
        
        # ingest_time 필드 제거
        filtered_fields = [field for field in dst_table_schema.fields if field.name != 'ingest_time']
        
        # 키 컬럼 추가 (key 컬럼에서 None은 제외)
        key_col = KEY_COL_MAPPING.get(avsc_reader.table_name)
        if key_col is None:
            # return StructType(filtered_fields)  # 키 컬럼 없이 반환
            return dst_table_schema
        if not key_col:
            raise ValueError(f"Unknown table name: {avsc_reader.table_name}")
        
        # 새로운 스키마 반환
        # return StructType(dst_table_schema.fields + [key_col])
        return StructType(filtered_fields + [key_col])

    @staticmethod
    def initialize_dst_table(spark_session:SparkSession, dst_table_schema:StructType, dst_table_identifier:str) -> None:
        if spark_session.catalog.tableExists(dst_table_identifier):
            # spark_session.catalog.refreshTable(dst_table_identifier)
            return
        
        dst_df = spark_session.createDataFrame([], dst_table_schema)
        write_iceberg(spark_session, dst_df, dst_table_identifier, mode='w')

    def get_current_dst_table(self, spark_session:SparkSession, batch_id:int=-1, debug=False, line_number=200):
        dst_df = spark_session.read.table(self.dst_avsc_reader.dst_table_identifier)

        if debug:
            conditions = [F.col(c).isNull() for c in dst_df.columns]
            final_condition = reduce(lambda x, y: x | y, conditions)
            null_rows = dst_df.filter(final_condition)
            null_rows.show(n=100, truncate=False)
            dst_df.show(n=line_number, truncate=False)
        
        if batch_id == -1:
            info_message = ''
        else:
            info_message = f'batch_id: {batch_id} '

        print(f"{info_message}Current # of {self.dst_avsc_reader.dst_table_identifier }: ", dst_df.count())