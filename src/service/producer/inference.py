from service.utils.schema.avsc import InferenceTopic
from service.producer.base.pandas import PandasProducer

class ReviewConflictSentimentSilverProducer(PandasProducer):
    dst_topic = InferenceTopic.REVIEW_CONFLICT_SENTIMENT
    key_column = ['review_id']

class ReviewConsistentSentimentSilverProducer(PandasProducer):
    dst_topic = InferenceTopic.REVIEW_CONSISTENT_SENTIMENT
    key_column = ['review_id']