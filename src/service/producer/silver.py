from service.producer.base.spark import SparkProducer
from service.utils.schema.avsc import SilverAvroSchema

class ReviewCleanCommentSilverProducer(SparkProducer):
    dst_topic = SilverAvroSchema.REVIEW_CLEAN_COMMENT
    key_column = ['review_id']

class ReviewMetadataSilverProducer(SparkProducer):
    dst_topic = SilverAvroSchema.REVIEW_METADATA
    key_column = ['review_id']