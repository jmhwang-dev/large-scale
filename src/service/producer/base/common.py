from typing import Iterable, Optional
from confluent_kafka import SerializingProducer

class BaseProducer:
    dst_topic: str = ''
    key_column: str = ''
    producer: Optional[SerializingProducer] = None
    message_key_col: str = 'message_key'
    producer_class_name: str = ''

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.producer_class_name = cls.__name__