import pandas as pd

from typing import Optional, Dict, Union
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, IntegerType
from pyspark.sql.streaming.state import GroupStateTimeout

from .base import BaseStream
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema
from service.pipeline.batch.gold import DimUserLocationBatch, FactOrderDetailBatch, FactOrderLeadDaysBatch
from service.pipeline.batch.base import BaseBatch

from service.utils.spark import get_kafka_stream_df, start_console_stream
from service.utils.schema.reader import AvscReader

class GoldStream(BaseStream):
    dst_layer: Optional[str] = None
    dst_name: Optional[str] = None

    def __init__(self, is_dev:bool, process_time:str, spark_session: Optional[SparkSession] = None):
        self.process_time = process_time
        self.dst_layer = 'gold'
        self.dst_env = 'prod' if not is_dev else 'dev'
        self.spark_session = spark_session

class FactOrderLeadDaysStream(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.FACT_ORDER_LEAD_DAYS
        self.dst_avsc_reader = AvscReader(self.dst_name)

        # Get schema to set `state_schema` in `self.transform`
        self.output_schema = BaseBatch.get_schema(self.spark_session, self.dst_avsc_reader)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
        
    def extract(self):
        order_event_avsc_reader = AvscReader(SilverAvroSchema.ORDER_EVENT)
        self.order_event_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, order_event_avsc_reader.table_name),
            order_event_avsc_reader
        )

    def transform(self):
        timestamp_field_list = ["purchase", "approve", "delivered_carrier", "delivered_customer", "shipping_limit", "estimated_delivery"]
        lead_timestamp_map = {
            "purchase_to_approval_days": ["approve", "purchase"],
            "approval_to_carrier_days": ["delivered_carrier", "approve"],
            "carrier_to_customer_days": ["delivered_customer", "delivered_carrier"],
            "carrier_delivery_delay_days": ["delivered_carrier", "shipping_limit"],
            "customer_delivery_delay_days": ["delivered_customer", "estimated_delivery"],
        }        
        state_schema = StructType([field for field in self.output_schema.fields if field.name != 'order_id'])

        # seconds
        mock_lead_time = 7200

        def stateful_func(key, pdf_iter, state):
            
            if not state.exists:
                init_state_dict = {}
                for field in state_schema.fields:
                    if field.name in timestamp_field_list:
                        state_value = None
                    else:
                        state_value = 0  # 초기화: 초 단위 0
                    init_state_dict[field.name] = state_value

                state.update(tuple(init_state_dict.values()))
            
            dst_state_dict: Dict[str, Optional[Union[pd.Timestamp, int, str]]] = {}
            for field, cur_state in zip(state_schema.fields, state.get):
                dst_state_dict[field.name] = cur_state
            
            # 새로운 데이터
            pdf_list = [pdf for pdf in pdf_iter]
            
            if len(pdf_list) > 0:
                all_pdf = pd.concat(pdf_list)
        
                src_state_dict = {}
                for timestamp_field in timestamp_field_list:
                    src_state_series = all_pdf[all_pdf['data_type'] == timestamp_field]['timestamp']
                    if src_state_series.empty:
                        src_state_dict[timestamp_field] = None
                        continue
                    src_state_dict[timestamp_field] = src_state_series.max().tz_localize(None)
                
                # 상태 업데이트: 날짜 주입
                for timestamp_field, src_state_value in src_state_dict.items():
                    if dst_state_dict[timestamp_field] is None and src_state_value is not None:
                        dst_state_dict[timestamp_field] = src_state_value

                # lead days 계산 (초 단위)
                for state_name, minuend_subtrahend in lead_timestamp_map.items():                
                    minuend = minuend_subtrahend[0]
                    subtrahend = minuend_subtrahend[1]

                    if dst_state_dict[subtrahend] is not None and dst_state_dict[minuend] is not None:
                        diff = dst_state_dict[minuend] - dst_state_dict[subtrahend]
                        dst_state_dict[state_name] = int(diff.total_seconds())  # 초 단위 저장

            # lead_days 누적
            # 모든 리드타임 계산 전까지만 갱신
            if not all(dst_state_dict[state_name] is not None for state_name in timestamp_field_list):
                state.setTimeoutDuration(1000)  # 1초 고정

            if state.hasTimedOut:
                for state_name, minuend_subtrahend in lead_timestamp_map.items():
                    minuend = minuend_subtrahend[0]
                    subtrahend = minuend_subtrahend[1]

                    # purchase_to_approval_days 완료 여부 체크
                    if state_name == "approval_to_carrier_days" and \
                       (dst_state_dict["approve"] is None or dst_state_dict["purchase"] is None):
                        continue  # purchase_to_approval_days 미완료 시 스킵

                    # approval_to_carrier_days 완료 여부 체크
                    if state_name == "carrier_to_customer_days" and \
                       (dst_state_dict["delivered_carrier"] is None or dst_state_dict["approve"] is None):
                        continue  # approval_to_carrier_days 미완료 시 스킵

                    if state_name == "carrier_delivery_delay_days" and \
                       (dst_state_dict["delivered_carrier"] is None or dst_state_dict["shipping_limit"] is None):
                        continue  # carrier_delivery_delay_days 미완료 시 스킵
                    
                    if state_name == "customer_delivery_delay_days" and \
                       (dst_state_dict["delivered_customer"] is None or dst_state_dict["estimated_delivery"] is None):
                        continue  # customer_delivery_delay_days 미완료 시 스킵

                    if dst_state_dict[subtrahend] is not None and dst_state_dict[minuend] is None:
                        dst_state_dict[state_name] += mock_lead_time  # 1시간 추가 (초 단위)

            state.update(tuple(dst_state_dict.values()))
            dst_state_dict['order_id'] = key[0]

            # 결과: 초를 일 수로 변환 (Iceberg LongType 저장)
            for state_name in lead_timestamp_map.keys():
                if dst_state_dict[state_name] is not None:
                    dst_state_dict[state_name] = dst_state_dict[state_name] // 86400  # 초 -> 일 수 변환

            state_output_df = pd.DataFrame([dst_state_dict])

            # 결과 출력
            yield state_output_df
        
        # Stateful 처리
        self.output_df = self.order_event_stream.withWatermark("ingest_time", "60 day") \
            .groupBy("order_id") \
            .applyInPandasWithState(
                func=stateful_func,
                outputStructType=self.output_schema,
                stateStructType=state_schema,
                outputMode="update",
                timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
            )

    def load(self, micro_batch: DataFrame, batch_id):
        FactOrderLeadDaysBatch(micro_batch.sparkSession, is_stream=True).load(micro_batch, batch_id)
        
    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .outputMode('update') \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()
    
class DimUserLocationStream(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.DIM_USER_LOCATION
        self.dst_avsc_reader = AvscReader(self.dst_name)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
    
    def extract(self):
        geo_coord_avsc_reader = AvscReader(SilverAvroSchema.GEO_COORD)
        self.geo_coord_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, geo_coord_avsc_reader.table_name),
            geo_coord_avsc_reader
        )

        olist_user_avsc_reader = AvscReader(SilverAvroSchema.OLIST_USER)
        self.olist_user_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, olist_user_avsc_reader.table_name),
            olist_user_avsc_reader
        )

    def transform(self,):
        self.geo_coord_stream = self.geo_coord_stream.withWatermark('ingest_time', '30 days')
        self.olist_user_stream = self.olist_user_stream.withWatermark('ingest_time', '30 days')

        joined_df = self.geo_coord_stream \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())).alias('geo_coord') \
            .join(self.olist_user_stream.alias('olist_user'), 
                F.expr("""
                geo_coord.zip_code = olist_user.zip_code AND
                geo_coord.ingest_time >= olist_user.ingest_time - INTERVAL 30 DAYS AND
                geo_coord.ingest_time <= olist_user.ingest_time + INTERVAL 30 DAYS
                """), "fullouter")

        self.output_df = joined_df.select(
            F.coalesce(F.col("geo_coord.zip_code"), F.col("olist_user.zip_code")).alias("zip_code"),
            F.col("geo_coord.lat").alias("lat"),
            F.col("geo_coord.lng").alias("lng"),
            F.col("olist_user.user_id").alias("user_id"),
            F.col("olist_user.user_type").alias("user_type")
        ).dropDuplicates()
        
    def load(self, micro_batch:DataFrame, batch_id: int):
        DimUserLocationBatch(micro_batch.sparkSession, is_stream=True).load(micro_batch, batch_id)
        
    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()

class FactOrderDetailStream(GoldStream):
    def __init__(self, is_dev: bool, process_time: str, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.FACT_ORDER_DETAIL
        self.dst_avsc_reader = AvscReader(self.dst_name)
        
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
        
    def extract(self):
        customer_avsc_reader = AvscReader(SilverAvroSchema.CUSTOMER_ORDER)
        self.customer_order_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, customer_avsc_reader.table_name),
            customer_avsc_reader
        )

        product_avsc_reader = AvscReader(SilverAvroSchema.PRODUCT_METADATA)
        self.product_metadata_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, product_avsc_reader.table_name),
            product_avsc_reader
        )

    def transform(self):
        self.customer_order_stream = self.customer_order_stream.withWatermark('ingest_time', '30 days')
        self.product_metadata_stream = self.product_metadata_stream.withWatermark('ingest_time', '30 days')
        
        # join 후 바로 중복 컬럼 병합 (product_id는 co 쪽 선택, ingest_time은 greatest)
        self.output_df = self.customer_order_stream.alias('co').join(
            self.product_metadata_stream.alias('pm'),
            on=F.expr("""
                co.product_id = pm.product_id AND
                co.seller_id = pm.seller_id AND
                co.ingest_time >= pm.ingest_time - INTERVAL 30 DAYS AND
                co.ingest_time <= pm.ingest_time + INTERVAL 30 DAYS
            """),
            how='fullouter'
        ).select(
            F.col('co.order_id').alias('order_id'),
            F.col('co.customer_id').alias('customer_id'),
            F.col('pm.seller_id').alias('seller_id'),
            F.col('co.product_id').alias('product_id'),  # co.product_id 선택 (pm drop)
            F.col('pm.category').alias('category'),
            F.col('co.quantity').alias('quantity'),
            F.col('co.unit_price').alias('unit_price'),
            F.greatest(F.col('co.ingest_time'), F.col('pm.ingest_time')).alias('ingest_time')
        ).dropna().drop('ingest_time').dropDuplicates()
    
    def load(self, micro_batch:DataFrame, batch_id: int):
        FactOrderDetailBatch(micro_batch.sparkSession, is_stream=True).load(micro_batch, batch_id)

    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()