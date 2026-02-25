from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from .base import BaseStream
from ..batch.silver import *
from ..common.silver import *

from service.utils.schema.avsc import BronzeAvroSchema, SilverAvroSchema
from service.utils.spark import get_kafka_stream_df, start_console_stream
from service.utils.schema.reader import AvscReader

class SilverStream(BaseStream):
    dst_layer: Optional[str] = None
    dst_name: Optional[str] = None

    def __init__(self, is_dev:bool, process_time:str, spark_session: Optional[SparkSession] = None):
        self.process_time = process_time
        self.dst_layer = 'silver'
        self.dst_env = 'prod' if not is_dev else 'dev'
        self.spark_session = spark_session

class GeoCoordStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.GEO_COORD
        self.dst_avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        geo_avsc_reader = AvscReader(BronzeAvroSchema.GEOLOCATION)
        self.geo_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, geo_avsc_reader.table_name),
            geo_avsc_reader
        )

    def transform(self):
        self.geo_stream = self.geo_stream \
            .select('zip_code', 'lng', 'lat', 'ingest_time') \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
            .withWatermark('ingest_time', '1 days')
        
        self.output_df = GeoCoordBase.transform(self.geo_stream)
        self.set_byte_stream('zip_code', ['lng', 'lat', 'ingest_time'])
    
class OlistUserStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.OLIST_USER
        self.dst_avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        customer_avsc_reader = AvscReader(BronzeAvroSchema.CUSTOMER)
        self.customer_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, customer_avsc_reader.table_name),
            customer_avsc_reader
        )

        seller_avsc_reader = AvscReader(BronzeAvroSchema.SELLER)
        self.seller_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, seller_avsc_reader.table_name),
            seller_avsc_reader
        )

    def transform(self):
        self.customer_stream = self.customer_stream.withWatermark('ingest_time', '1 days')
        self.seller_stream = self.seller_stream.withWatermark('ingest_time', '1 days')

        self.output_df = OlistUserBase.transform(self.customer_stream, self.seller_stream)
        self.set_byte_stream('user_id', ['user_type', 'zip_code', 'ingest_time'])
    
class OrderEventStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        
        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.ORDER_EVENT
        self.dst_avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        est_avsc_reader = AvscReader(BronzeAvroSchema.ESTIMATED_DELIVERY_DATE)
        self.estimated_stream = BaseStream.get_topic_df(get_kafka_stream_df(self.spark_session, est_avsc_reader.table_name), est_avsc_reader)

        order_item_avsc_reader = AvscReader(BronzeAvroSchema.ORDER_ITEM)
        self.order_item_stream = BaseStream.get_topic_df(get_kafka_stream_df(self.spark_session, order_item_avsc_reader.table_name), order_item_avsc_reader)

        order_status_avsc_reader = AvscReader(BronzeAvroSchema.ORDER_STATUS)
        self.order_status_stream = BaseStream.get_topic_df(get_kafka_stream_df(self.spark_session, order_status_avsc_reader.table_name), order_status_avsc_reader)
            
    def transform(self):
        shippimt_limit_df = self.order_item_stream \
            .select('order_id', 'shipping_limit_date', 'ingest_time')

        self.output_df = OrderEventBase.transform(self.estimated_stream, shippimt_limit_df, self.order_status_stream)
        self.set_byte_stream('order_id', ["data_type", "timestamp", "ingest_time"])

class ProductMetadataStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        
        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.PRODUCT_METADATA
        self.dst_avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
        
    def extract(self):
        product_asvc_reader = AvscReader(BronzeAvroSchema.PRODUCT)
        self.product_df = BaseStream.get_topic_df(get_kafka_stream_df(self.spark_session, product_asvc_reader.table_name), product_asvc_reader)

        order_item_asvc_reader = AvscReader(BronzeAvroSchema.ORDER_ITEM)
        self.order_item_df = BaseStream.get_topic_df(get_kafka_stream_df(self.spark_session, order_item_asvc_reader.table_name), order_item_asvc_reader)

    def transform(self, ):
        product_category = self.product_df \
            .withWatermark('ingest_time', '30 days') \
            .select('product_id', 'category', 'ingest_time') \
            .dropDuplicates(['product_id'])  # latest ingest_time 기준 자동 정렬 필요 시 sort 추가

        product_seller = self.order_item_df \
            .withWatermark('ingest_time', '30 days') \
            .select('product_id', 'seller_id', 'ingest_time') \
            .dropDuplicates(['product_id'])
        
        self.output_df = product_category.alias('p_cat').join(
            product_seller.alias('p_sel'),
            on=F.expr("""
                p_cat.product_id = p_sel.product_id and
                p_cat.ingest_time >= p_sel.ingest_time - interval 30 days AND
                p_cat.ingest_time <= p_sel.ingest_time + interval 30 days
            """),
            how='fullouter'
            ).select(
                F.col('p_cat.category').alias('category'),
                F.col('p_cat.product_id').alias('product_id'),
                F.col('p_sel.seller_id').alias('seller_id'),
                F.greatest('p_cat.ingest_time', 'p_sel.ingest_time').alias('ingest_time'),
            ).dropna()
                    
        self.set_byte_stream('product_id', ["category", "seller_id", "ingest_time"])
    
class CustomerOrderStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        
        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.CUSTOMER_ORDER
        self.dst_avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        order_item_reader = AvscReader(BronzeAvroSchema.ORDER_ITEM)
        self.order_item_stream = BaseStream.get_topic_df(get_kafka_stream_df(self.spark_session, order_item_reader.table_name), order_item_reader)
        
        payment_reader = AvscReader(BronzeAvroSchema.PAYMENT)
        self.payment_stream = BaseStream.get_topic_df(get_kafka_stream_df(self.spark_session, payment_reader.table_name), payment_reader)
        
    def transform(self):
        """
        Non-windowed groupBy: 전체 스트림 기간 동안 누적 집로, 상태가 무한이 증가한다. (unbounded state)
        - watermark는 배치에서 late data를 처리하는 목적이다.
        - 따라서 watermark가 있어도 상태를 drop할 시간 경계가 없어서 상태가 finalized 되지 않음
        - 결국 OOM으로 이어질 가능성이 매우 높아지기 때문에, 다음과 같은 예외를 발생시킨다.
        (AnalysisException: "Append output mode not supported for streaming aggregations without watermark")

        대안1. window 함수

        대안2. Append 대신 update (변경된 행만 출력) 또는 complete (전체 결과 재출력) mode로 전환
        - 상태 drop: Update mode에서 watermark 설정 시 오래된 상태 정리 가능 (complete는 모든 상태 유지, drop 안 함).
        - 한계: Append가 필요한 싱크 (e.g., Kafka append-only)에서 불가. Complete는 대규모 데이터에서 비효율적 (전체 재출력).

        대안 3: mapGroupsWithState 또는 flatMapGroupsWithState로 커스텀 상태 관리
        """
        # `seller_id`` 포함 이유: 동일 `product_id`를 다수의 `seller_id`가 판매할 수 있음
        # order_item_price: 워터마크 유지
        order_item_price = self.order_item_stream \
            .withWatermark('ingest_time', '3 days') \
            .select('order_id', 'order_item_id', 'product_id', 'price', 'seller_id', 'ingest_time') \
            .dropna()

        # windowed aggregation
        aggregated_df = order_item_price \
            .groupBy(
                F.window("ingest_time", "5 minutes"),
                "order_id", "product_id", "price", 'seller_id'
            ).agg(
                F.count("order_item_id").alias("quantity"),
                F.max("ingest_time").alias('agg_ingest_time')
            ).withColumnRenamed("price", "unit_price")  # window 유지

        # order_customer: 워터마크 추가
        order_customer = self.payment_stream \
            .withWatermark('ingest_time', '3 days') \
            .select('order_id', 'customer_id', 'ingest_time') \
            .dropDuplicates(['order_id'])

        # fullouter 조인에 시간 범위 조건 추가
        self.output_df = aggregated_df.alias('agg').join(
            order_customer.alias('oc'),
            F.expr("""
                agg.order_id = oc.order_id AND
                agg.agg_ingest_time >= oc.ingest_time - interval 3 days AND
                agg.agg_ingest_time <= oc.ingest_time + interval 3 days
            """),
            "fullouter"
        ).select(
            F.col('agg.order_id').alias('order_id'),
            F.col('oc.customer_id').alias('customer_id'),
            F.col('agg.product_id').alias('product_id'),
            F.col('agg.unit_price').alias('unit_price'),
            F.col('agg.seller_id').alias('seller_id'),
            F.col('agg.quantity').alias('quantity'),
            F.greatest('agg.agg_ingest_time', 'oc.ingest_time').alias('ingest_time')
        ).dropna()

        self.set_byte_stream('order_id', ["customer_id", "product_id", "seller_id", "unit_price", "quantity", "ingest_time"])

class ReviewMetadataStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        
        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.REVIEW_METADATA
        self.dst_avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        review_avsc_reader = AvscReader(BronzeAvroSchema.REVIEW)

        self.review_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, review_avsc_reader.table_name),
            review_avsc_reader)
    
    def transform(self,):
        self.output_df = ReviewMetadataBase.transform(self.review_stream)
        # TODO: check key column
        self.set_byte_stream('order_id', ["review_id", "review_creation_date", "review_answer_timestamp", "review_score"])