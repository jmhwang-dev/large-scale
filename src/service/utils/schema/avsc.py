from typing import List
from enum import Enum

class BaseAvroSchema:
    """Base class for Kafka topic definitions with common methods."""
    @classmethod
    def get_all_filenames(cls) -> List[str]:
        """모든 토픽 이름을 prefix와 함께 반환"""
        topics = []
        for attr_name in dir(cls):
            # 메타클래스에서 이미 값을 변환했으므로, 값 자체를 추가하기만 하면 됨
            attr_value = getattr(cls, attr_name)
            if (isinstance(attr_value, str) and
                    attr_name.isupper() and
                    not attr_name.startswith('_') and
                    not attr_name.startswith('TOPIC_')):
                topics.append(attr_value)
        return topics

class BronzeAvroSchema(BaseAvroSchema):
    PRODUCT = "product" 
    CUSTOMER = "customer"
    SELLER = "seller"
    GEOLOCATION = "geolocation"
    ESTIMATED_DELIVERY_DATE = "estimated_delivery_date"
    ORDER_ITEM = "order_item"    
    ORDER_STATUS = "order_status"
    PAYMENT = "payment"
    REVIEW = "review"

class SilverAvroSchema(BaseAvroSchema):
    # schema for batch and stream
    REVIEW_CLEAN_COMMENT = "review_clean_comment" 
    REVIEW_METADATA = "review_metadata"
    ORDER_EVENT = "order_event"
    CUSTOMER_ORDER = "customer_order"
    GEO_COORD = "geo_coord"

    # schema for batch
    OLIST_USER = 'olist_user'
    PRODUCT_METADATA = "product_metadata"

    # watermark
    WATERMARK = 'watermark'

class GoldAvroSchema(BaseAvroSchema):
    # schema for batch and stream
    DIM_USER_LOCATION = "dim_user_location" 
    FACT_ORDER_LEAD_DAYS = "fact_order_lead_days"
    FACT_ORDER_DETAIL = "fact_order_detail"
    FACT_REVIEW_ANSWER_LEAD_DAYS = "fact_review_answer_lead_days"

    # batch
    FACT_MONTHLY_SALES_BY_PRODUCT = "fact_monthly_sales_by_product"

    # strema
    DELIVERY_STATUS = "delivery_status"