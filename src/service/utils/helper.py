from typing import Optional
# from service.pipeline.batch import base, silver, gold
from service.producer.bronze import *
# from service.pipeline import *
from service.pipeline import batch
from service.pipeline import stream

from service.utils.schema.avsc import GoldAvroSchema

def get_producer(topic_name):
    if topic_name == OrderStatusBronzeProducer.dst_topic:
        return OrderStatusBronzeProducer
    
    elif topic_name == ProductBronzeProducer.dst_topic:
        return ProductBronzeProducer
    
    elif topic_name == CustomerBronzeProducer.dst_topic:
        return CustomerBronzeProducer
    
    elif topic_name == SellerBronzeProducer.dst_topic:
        return SellerBronzeProducer
    
    elif topic_name == GeolocationBronzeProducer.dst_topic:
        return GeolocationBronzeProducer
    
    elif topic_name == EstimatedDeliberyDateBronzeProducer.dst_topic:
        return EstimatedDeliberyDateBronzeProducer
    
    elif topic_name == OrderItemBronzeProducer.dst_topic:
        return OrderItemBronzeProducer
    
    elif topic_name == PaymentBronzeProducer.dst_topic:
        return PaymentBronzeProducer
    
    elif topic_name == ReviewBronzeProducer.dst_topic:
        return ReviewBronzeProducer
    
def get_batch_pipeline(target_pipeline: str) -> List[Optional[batch.base.BaseBatch]]:
    """
    Get the list of batch job classes for the specified pipeline.

    Parameters
    ----------
    target_pipeline : str
        - `*AvroSchema` name (ex: SilverAvroSchema.CUSTOMER_ORDER)
        - Use 'all' for the full pipeline.

    Returns
    -------
    list
        `[]` or list for batch job classes to run for the given pipeline
    """
    
    silver_jobs = [
        batch.silver.GeoCoordBatch,
        batch.silver.OlistUserBatch,
        batch.silver.ReviewMetadataBatch,
        batch.silver.ProductMetadataBatch,
        batch.silver.CustomerOrderBatch,
        batch.silver.OrderEventBatch,
    ]

    gold_jobs = [
        batch.gold.DimUserLocationBatch,
        batch.gold.FactOrderDetailBatch,
        batch.gold.FactOrderLeadDaysBatch,
        batch.gold.FactMonthlySalesByProductBatch,
        batch.gold.FactReviewAnswerLeadDaysBatch,
    ]

    # 전체 pipeline 구성
    app_dict = {
        'all': silver_jobs + gold_jobs,

        GoldAvroSchema.FACT_REVIEW_ANSWER_LEAD_DAYS: [
            batch.silver.ReviewMetadataBatch,
            batch.gold.FactReviewAnswerLeadDaysBatch
        ],

        GoldAvroSchema.FACT_ORDER_LEAD_DAYS: [
            batch.silver.OrderEventBatch,
            batch.gold.FactOrderLeadDaysBatch
        ],

        GoldAvroSchema.FACT_ORDER_DETAIL: [
            batch.silver.ProductMetadataBatch,
            batch.silver.CustomerOrderBatch,
            batch.gold.FactOrderDetailBatch
        ],

        GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT: [
            batch.silver.ProductMetadataBatch,
            batch.silver.CustomerOrderBatch,
            batch.gold.FactOrderDetailBatch,

            batch.silver.OrderEventBatch,
            batch.gold.FactOrderLeadDaysBatch,

            batch.gold.FactMonthlySalesByProductBatch
        ],

        GoldAvroSchema.DIM_USER_LOCATION: [
            batch.silver.GeoCoordBatch,
            batch.silver.OlistUserBatch,
            batch.gold.DimUserLocationBatch
        ],
    }

    return app_dict.get(target_pipeline, [])

def get_stream_pipeline(target_pipeline: str) -> List[Optional[stream.base.BaseStream]]:
    """
    Get the list of batch job classes for the specified pipeline.

    Parameters
    ----------
    target_pipeline : str
        - `*AvroSchema` name (ex: SilverAvroSchema.CUSTOMER_ORDER)
        - Use 'all' for the full pipeline.

    Returns
    -------
    list
        `[]` or list for batch job classes to run for the given pipeline
    """
    
    silver_jobs = [
        stream.silver.GeoCoordStream,
        stream.silver.OlistUserStream,
        stream.silver.ReviewMetadataStream,
        stream.silver.ProductMetadataStream,
        stream.silver.CustomerOrderStream,
        stream.silver.OrderEventStream,
    ]

    gold_jobs = [
        stream.gold.DimUserLocationStream,
        stream.gold.FactOrderDetailStream,
        stream.gold.FactOrderLeadDaysStream,
    ]

    # TODO
    # - Inference to translate (Portuguess to English)
    # - Add `ReviewMetadataStream`` when class for translation is done
    # ex)review_comment = review_stream_df.select('review_id', 'review_comment_title', 'review_comment_message').dropna()
    # ReviewMetadataStream_job = [
    #     silver.ReviewMetadataStream,
    #     gold.

    # 전체 pipeline 구성
    app_dict = {
        'all': silver_jobs + gold_jobs,

        GoldAvroSchema.FACT_ORDER_LEAD_DAYS: [
            stream.silver.OrderEventStream,
            stream.gold.FactOrderLeadDaysStream,
        ],

        GoldAvroSchema.FACT_ORDER_DETAIL: [
            stream.silver.ProductMetadataStream,
            stream.silver.CustomerOrderStream,
            stream.gold.FactOrderDetailStream,
        ],

        GoldAvroSchema.DIM_USER_LOCATION: [
            stream.silver.GeoCoordStream,
            stream.silver.OlistUserStream,
            stream.gold.DimUserLocationStream,
        ],
    }

    return app_dict.get(target_pipeline, [])