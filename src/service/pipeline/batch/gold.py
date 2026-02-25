from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from .base import BaseBatch
from service.utils.schema.reader import AvscReader
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema

class DimUserLocationBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_stream: bool=False):
        super().__init__(self.__class__.__name__, GoldAvroSchema.DIM_USER_LOCATION, spark_session, is_stream)

    def extract(self):
        geo_coord_avsc_reader = AvscReader(SilverAvroSchema.GEO_COORD)
        self.check_table(self.spark_session, geo_coord_avsc_reader.dst_table_identifier)
        self.geo_coord_df = self.spark_session.read.table(geo_coord_avsc_reader.dst_table_identifier)

        olist_user_avsc_reader = AvscReader(SilverAvroSchema.OLIST_USER)
        self.check_table(self.spark_session, olist_user_avsc_reader.dst_table_identifier)
        self.olist_user = self.spark_session.read.table(olist_user_avsc_reader.dst_table_identifier)

    def transform(self,):
        unique_olist_user_df = self.olist_user.dropDuplicates()
        unique_geo_coord_df = self.geo_coord_df.dropDuplicates()
        self.output_df = unique_geo_coord_df \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
            .join(unique_olist_user_df, on='zip_code', how='inner')
        
    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.user_id = s.user_id AND t.zip_code = s.zip_code
            WHEN NOT MATCHED THEN
                INSERT (user_id, zip_code, lng, lat, user_type)
                VALUES (s.user_id, s.zip_code, s.lng, s.lat, s.user_type)
            """)
        
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)

class FactOrderLeadDaysBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_stream: bool=False):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_ORDER_LEAD_DAYS, spark_session, is_stream)

    def extract(self):
        order_event_avsc_reader = AvscReader(SilverAvroSchema.ORDER_EVENT)
        self.check_table(self.spark_session, order_event_avsc_reader.dst_table_identifier)
        self.order_event_df = self.spark_session.read.table(order_event_avsc_reader.dst_table_identifier)

    def transform(self,):
        # self.output_df = FactOrderLeadDaysBase.transform(self.order_event_df)
        order_timeline_df = self.order_event_df \
            .groupBy('order_id') \
            .agg(
                F.max(F.when(F.col('data_type') == 'purchase', F.col('timestamp'))).alias('purchase'),
                F.max(F.when(F.col('data_type') == 'approve', F.col('timestamp'))).alias('approve'),
                F.max(F.when(F.col('data_type') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier'),
                F.max(F.when(F.col('data_type') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer'),
                F.max(F.when(F.col('data_type') == 'shipping_limit', F.col('timestamp'))).alias('shipping_limit'),
                F.max(F.when(F.col('data_type') == 'estimated_delivery', F.col('timestamp'))).alias('estimated_delivery'),
            )

        self.output_df = order_timeline_df.withColumns({
            "purchase_to_approval_days": F.date_diff("approve", "purchase"),
            "approval_to_carrier_days": F.date_diff("delivered_carrier", "approve"),
            "carrier_to_customer_days": F.date_diff("delivered_customer", "delivered_carrier"),
            "carrier_delivery_delay_days": F.date_diff("delivered_carrier", "shipping_limit"),
            "customer_delivery_delay_days": F.date_diff("delivered_customer", "estimated_delivery")
        })

    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.purchase = COALESCE(s.purchase, t.purchase),
                    t.approve = COALESCE(s.approve, t.approve),
                    t.delivered_carrier = COALESCE(s.delivered_carrier, t.delivered_carrier),
                    t.delivered_customer = COALESCE(s.delivered_customer, t.delivered_customer),
                    t.shipping_limit = COALESCE(s.shipping_limit, t.shipping_limit),
                    t.estimated_delivery = COALESCE(s.estimated_delivery, t.estimated_delivery),
                    t.purchase_to_approval_days = COALESCE(s.purchase_to_approval_days, t.purchase_to_approval_days),
                    t.approval_to_carrier_days = COALESCE(s.approval_to_carrier_days, t.approval_to_carrier_days),
                    t.carrier_to_customer_days = COALESCE(s.carrier_to_customer_days, t.carrier_to_customer_days),
                    t.carrier_delivery_delay_days = COALESCE(s.carrier_delivery_delay_days, t.carrier_delivery_delay_days),
                    t.customer_delivery_delay_days = COALESCE(s.customer_delivery_delay_days, t.customer_delivery_delay_days)
            WHEN NOT MATCHED THEN
                INSERT *
            """)
        self.get_current_dst_table(output_df.sparkSession, batch_id)

class FactOrderDetailBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_stream: bool=False):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_ORDER_DETAIL, spark_session, is_stream)

    def extract(self):
        customer_order_avsc_reader = AvscReader(SilverAvroSchema.CUSTOMER_ORDER)
        self.check_table(self.spark_session, customer_order_avsc_reader.dst_table_identifier)
        self.customer_order_df = self.spark_session.read.table(customer_order_avsc_reader.dst_table_identifier)

        product_metadata_avsc_reader = AvscReader(SilverAvroSchema.PRODUCT_METADATA)
        self.check_table(self.spark_session, product_metadata_avsc_reader.dst_table_identifier)
        self.product_metadata_df = self.spark_session.read.table(product_metadata_avsc_reader.dst_table_identifier)

    def transform(self,):

        self.output_df = self.customer_order_df.alias('co').join(
            self.product_metadata_df.alias('pm'),
            on=['product_id', 'seller_id'],
            how='inner')
        
    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.order_id = s.order_id and t.product_id = s.product_id and t.seller_id = s.seller_id and t.unit_price = s.unit_price
            
            WHEN MATCHED and t.quantity != s.quantity or t.category != s.category THEN
                UPDATE SET
                    t.quantity = s.quantity,
                    t.category = s.category

            WHEN NOT MATCHED THEN
                INSERT (order_id, customer_id, seller_id, product_id, category, quantity, unit_price)
                VALUES (s.order_id, s.customer_id, s.seller_id, s.product_id, s.category, s.quantity, s.unit_price)
            """)
        
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)

class FactMonthlySalesByProductBatch(BaseBatch):
    """
    [Batch Layer: Product Context Generation for Hybrid Dashboard]

    1. 목적 (Technical Objective):
        - 실시간 배송 관제 시스템(Operational View)에 결합될 상품별 분석 컨텍스트(Analytical Context)를 생성함.
        - OLTP 성격의 '실시간 주문 상태' 정보와 OLAP 성격의 '상품 가치 평가' 정보를 통합하여 의사결정의 입체성을 확보.

    2. 데이터 처리 로직 (OLAP - BCG Matrix Re-interpretation):
        - 대용량 트랜잭션 이력(History Data)을 집계하여 상품의 비즈니스 등급을 산정.
        - X축 (Traffic): 판매량 (Sales Quantity) -> 플랫폼 내 트래픽 점유율
        - Y축 (Value): 평균 단가 (Mean Unit Price) -> 객단가 기반 수익성
          * Note: 초기 모델의 Y축인 '총 매출(Revenue)'은 판매량과 강한 양의 상관관계(Multicollinearity)를 보여
            4분면 편향이 발생함. 이를 해결하기 위해 독립 변수인 '단가'로 축을 변경함.

    3. 분류 기준 및 보정 (Long-tail Calibration):
        - 현상: 전체 상품의 50% 이상이 '월 판매량 1개'에 집중된 롱테일(Long-tail) 분포 확인 (Median=1).
        - 보정: 단순 중앙값 분할(>=) 시 변별력 상실을 방지하기 위해, 성과 기준을 '초과(> 1)'로 상향 조정.

        [Result: Product Grade Context]
        - Star Products: High Vol (>1) / High Price (>=Med)
        - Volume Drivers: High Vol (>1) / Low Price (<Med)
        - Niche Gems: Low Vol (<=1) / High Price (>=Med)
        - Question Marks: Low Vol (<=1) / Low Price (<Med)
    """
    def __init__(self, spark_session: Optional[SparkSession] = None, is_stream: bool=False):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT, spark_session, is_stream)

    def extract(self):
        fact_order_timeline_avsc_reader = AvscReader(GoldAvroSchema.FACT_ORDER_LEAD_DAYS)
        self.check_table(self.spark_session, fact_order_timeline_avsc_reader.dst_table_identifier)
        self.fact_order_lead_days = self.spark_session.read.table(fact_order_timeline_avsc_reader.dst_table_identifier)

        fact_order_detail_avsc_reader = AvscReader(GoldAvroSchema.FACT_ORDER_DETAIL)
        self.check_table(self.spark_session, fact_order_detail_avsc_reader.dst_table_identifier)
        self.fact_order_detail_df = self.spark_session.read.table(fact_order_detail_avsc_reader.dst_table_identifier)

    def transform(self):
        # 1. 기본 집계 데이터 생성
        sales_period_df = self.fact_order_lead_days \
            .filter(F.col('delivered_customer').isNotNull()) \
            .select('order_id', 'delivered_customer') \
            .withColumn('sales_period', F.date_format(F.col('delivered_customer'), 'yyyy-MM')) \
            .drop('delivered_customer')
        
        period_fact_order_detail_df = sales_period_df.join(self.fact_order_detail_df, on='order_id', how='inner')

        # 제품/기간별 판매량 및 매출액 집계
        product_period_sales_df = period_fact_order_detail_df.groupBy('product_id', 'category', 'sales_period') \
            .agg(
                F.sum('quantity').alias('total_sales_quantity'),
                F.sum(F.col('quantity') * F.col('unit_price')).alias('total_sales_amount')
            )
        
        # 평균 단가(Unit Price) 계산
        # 안전한 나눗셈을 위해 NULLIF 처리를 하면 좋으나, 로직상 quantity가 0일 수 없으므로 그대로 둠
        fact_monthly_sales_by_product_df = product_period_sales_df.withColumn(
            'mean_sales', 
            F.col('total_sales_amount') / F.col('total_sales_quantity')
        )
        
        metrics_stats_df = fact_monthly_sales_by_product_df.groupBy('category', 'sales_period').agg(
            F.percentile_approx('total_sales_quantity', 0.5).alias('median_vol'),
            # ★ 여기를 수정했습니다 (total_sales_amount -> mean_sales)
            F.percentile_approx('mean_sales', 0.5).alias('median_price') 
        )

        # 3. 기준값 조인 및 4분면 그룹 분류 (Long-tail 보정 로직 적용)
        self.output_df = fact_monthly_sales_by_product_df.join(
            metrics_stats_df, 
            on=['category', 'sales_period'], 
            how='inner'
        ).withColumn(
            "group",
            F.when(
                # Star: 많이 팔림(>1) AND 비쌈(>=Median)
                (F.col("total_sales_quantity") > F.col("median_vol")) & 
                (F.col("mean_sales") >= F.col("median_price")), 
                "Star Products"
            ).when(
                # Volume Driver: 많이 팔림(>1) BUT 쌈(<Median)
                (F.col("total_sales_quantity") > F.col("median_vol")) & 
                (F.col("mean_sales") < F.col("median_price")), 
                "Volume Drivers"
            ).when(
                # Niche Gem: 적게 팔림(<=1) BUT 비쌈(>=Median)
                (F.col("total_sales_quantity") <= F.col("median_vol")) & 
                (F.col("mean_sales") >= F.col("median_price")), 
                "Niche Gems"
            ).otherwise("Question Marks") # 적게 팔림(<=1) AND 쌈(<Median)
        ).drop("median_vol", "median_price")

    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        # (기존 load 로직 동일)
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        self.output_df.createOrReplaceTempView("updates")
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.product_id = source.product_id AND target.sales_period = source.sales_period
            WHEN MATCHED THEN
            UPDATE SET
                target.total_sales_quantity = source.total_sales_quantity,
                target.total_sales_amount = source.total_sales_amount,
                target.mean_sales = source.mean_sales,
                target.category = source.category,
                target.group = source.group
            WHEN NOT MATCHED THEN
            INSERT (product_id, sales_period, total_sales_quantity, total_sales_amount, mean_sales, category, group)
            VALUES (source.product_id, source.sales_period, source.total_sales_quantity, source.total_sales_amount, source.mean_sales, source.category, source.group)
        """)
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)

class FactReviewAnswerLeadDaysBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_stream: bool=False):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_REVIEW_ANSWER_LEAD_DAYS, spark_session, is_stream)

    def extract(self):
        review_metadata_avsc_reader = AvscReader(SilverAvroSchema.REVIEW_METADATA)
        self.check_table(self.spark_session, review_metadata_avsc_reader.dst_table_identifier)
        self.review_metadata_df = self.spark_session.read.table(review_metadata_avsc_reader.dst_table_identifier)

    def transform(self):
        self.output_df = self.review_metadata_df.withColumn('until_answer_lead_days',F.date_diff('review_answer_timestamp', 'review_creation_date'))
        
    def load(self):
        self.output_df.createOrReplaceTempView("updates")
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.review_id = source.review_id AND target.order_id = source.order_id
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        self.get_current_dst_table(self.output_df.sparkSession, debug=self.is_debug)