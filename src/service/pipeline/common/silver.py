from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from abc import ABC, abstractmethod

class CommonSilverTask(ABC):
    @abstractmethod
    def transform(self, ):
        pass

class GeoCoordBase(CommonSilverTask):
    
    @classmethod
    def transform(cls, geo_df: DataFrame):

        # 브라질 위경도 범위 상수
        BRAZIL_BOUNDS = {
            "min_lat": -33.69111,
            "max_lat": 2.81972,
            "min_lon": -72.89583,
            "max_lon": -34.80861
        }

        return geo_df \
            .filter(
                (F.col("lat").between(BRAZIL_BOUNDS["min_lat"], BRAZIL_BOUNDS["max_lat"])) &
                (F.col("lng").between(BRAZIL_BOUNDS["min_lon"], BRAZIL_BOUNDS["max_lon"]))
            )

class OlistUserBase(CommonSilverTask):

    @classmethod
    def transform(cls, customer_df:DataFrame, seller_df:DataFrame ):
        _customer_df = customer_df \
            .withColumnRenamed('customer_id', 'user_id') \
            .withColumn('user_type', F.lit('customer'))

        _seller_df = seller_df \
            .withColumnRenamed('seller_id', 'user_id') \
            .withColumn('user_type', F.lit('seller'))
        
        return _customer_df.unionByName(_seller_df)
    
class OrderEventBase(CommonSilverTask):
    
    @classmethod
    def transform(cls, estimated_df:DataFrame, shippimt_limit_df:DataFrame, order_status_df:DataFrame):
        # TODO: chage timezone to UTC. (현재는 KST로 들어감)
        renamed_est_delivery_df = estimated_df \
            .withColumnRenamed('estimated_delivery_date', 'timestamp') \
            .withColumn('data_type', F.lit('estimated_delivery'))
        
        renamed_shippimt_limit_df = shippimt_limit_df \
            .withColumnRenamed('shipping_limit_date', 'timestamp') \
            .withColumn('data_type', F.lit('shipping_limit')) \
            .dropDuplicates()
        
        renamed_order_status_df = order_status_df \
            .replace({"approved": "approve"}, subset=["status"]) \
            .withColumnRenamed('status', 'data_type')
        
        return renamed_order_status_df.unionByName(renamed_est_delivery_df).unionByName(renamed_shippimt_limit_df)
    
class ReviewMetadataBase(CommonSilverTask):
    
    @classmethod
    def transform(cls, review_stream:DataFrame):
        return review_stream \
            .drop('review_comment_title', 'review_comment_message', 'ingest_time') \
            .dropDuplicates()