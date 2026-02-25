from typing import Union, List, Optional, Tuple
import numpy as np
import pandas as pd
from confluent_kafka.serialization import SerializationError

from service.producer.base.common import BaseProducer
from service.utils.kafka import get_confluent_kafka_producer, get_confluent_serializer_conf
from service.utils.schema.avsc import BronzeAvroSchema

class PandasProducer(BaseProducer):

    @classmethod
    def get_record(cls, dataum: pd.Series) -> dict[str, dict]:
        message = dataum.where(pd.notnull(dataum), None)

        return {
            'key': str(message[cls.key_column]),
            'value': message.drop(index=cls.key_column).to_dict()
        }

    @classmethod
    def generate_message(cls, event: Union[pd.DataFrame, pd.Series]) -> List[dict[str, dict]]:
        """
        CAUTION:
            - Avro 스키마에서 null 타입은 Python의 None만 인식
            - pandas의 `np.nan`은 Python의 `None`이 아닌 `float` 타입이므로 변환 필요: `np.nan` -> `None`
        """
        if isinstance(event, pd.Series):
            record = cls.get_record(event)
            return [record]
        
        producer_record_list = []
        for _, ev in event.iterrows():
            record = cls.get_record(ev)
            producer_record_list.append(record)
        return producer_record_list

    @classmethod
    def init_producer(cls, use_internal: bool = True):
        """
        - DLQ : json
        - 그 외 : avro
        """
        if cls.producer is None:
            serializer, bootstrap_server_list = get_confluent_serializer_conf(cls.dst_topic, use_internal)
            cls.producer = get_confluent_kafka_producer(bootstrap_server_list, serializer)

    @classmethod
    def publish(cls, event: Union[pd.Series, pd.DataFrame, None], use_internal=False) -> None:
        if event is None or event.empty:
            # print(f"Empty event - {cls.producer_class_name}")
            return
        
        cls.init_producer(use_internal)
        producer_record_list = cls.generate_message(event)

        for producer_record in producer_record_list:
            try:
                cls.producer.produce(cls.dst_topic, key=producer_record['key'], value=producer_record['value'])
                # if cls.dst_topic != BronzeAvroSchema.ORDER_STATUS:
                #     continue
                # if producer_record['value']['status'] != 'delivered_customer':
                #     continue
                # print(f"Published to {cls.dst_topic} | {cls.key_column}: {producer_record['key']} | status: {producer_record['value']['status']}")
                print(f"ingest_time: {producer_record['value']['ingest_time']} | published to {cls.dst_topic:<25} | {cls.key_column:<14} | {producer_record['key']:<35}")

            except SerializationError:
                print(f'[{cls.producer_class_name}]: schema 검증 실패')
        
        cls.producer.flush()

    @staticmethod
    def add_mock_ingest_time(log: pd.DataFrame, current_time:pd.Timestamp) -> Tuple[Optional[pd.DataFrame], Optional[pd.Timestamp]]:
        """
        개발 편의성을 위해 컨슈머가 아닌 프로듀서에서 이벤트 생성 시간을 의미하는 임의의 `ingest_time`을 모방한 시간을 추가함.
        - 이유: `micro_batch`에서 이벤트 시간이 없는 메시지의 `ingest_time`을 컨슈머에서 모방하기 복잡하여 개발 편의성을 높이고자 프로듀서가 임의로 `ingest_time`을 추가함.
        - 실제 운영시에는 컨슈머에서 토픽을 소비하는 시점에 실제 시간을 `ingest_time`으로 사용함.

        HOW-TO
        - `event timestamp` 가 없는 데이터는 이 `최근 데이터의 주입 시간`으로 주입 시간 생성
        - `최근 데이터의 주입 시간`에서 50ms ~ 100ms 사이의 시간을 부여하여 주입 시간 기록
        """

        if log.empty:
            return None, current_time
        
        _current_time = current_time
        log_added_ingest_time = log.copy()
        if isinstance(log, pd.Series):
            random_timedelta = pd.Timedelta(milliseconds=np.random.uniform(10, 200))
            mock_ingest_time = _current_time + random_timedelta
            log_added_ingest_time['ingest_time'] = mock_ingest_time

        else:
            for i, _ in log.iterrows():
                random_timedelta = pd.Timedelta(f"{50 + np.random.rand() * 50}ms")
                mock_ingest_time = _current_time + random_timedelta
                log_added_ingest_time.loc[i, 'ingest_time'] = mock_ingest_time
                _current_time = mock_ingest_time

        return log_added_ingest_time, mock_ingest_time
    

    @staticmethod
    def calc_mock_ingest_time(log: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        개발 편의성을 위해 컨슈머가 아닌 프로듀서에서 이벤트 생성 시간을 의미하는 임의의 `ingest_time`을 모방한 시간을 추가함.
        - 이유: `micro_batch`에서 이벤트 시간이 없는 메시지의 `ingest_time`을 컨슈머에서 모방하기 복잡하여 개발 편의성을 높이고자 프로듀서가 임의로 `ingest_time`을 추가함.
        - 실제 운영시에는 컨슈머에서 토픽을 소비하는 시점에 실제 시간을 `ingest_time`으로 사용함.
        
        HOW-TO
        - `event timestamp` 가 기록된 데이터는 이 기록을 기준으로 주입 시간 생성
        - 네트워크 전송 등을 고려하여 50ms ~ 100ms 사이의 시간을 부여하여 데이터 전송 시간을 기록
        """
        if log is None:
            return

        log_added_ingest_time = log.copy()
        timestamp_col = 'timestamp' if 'timestamp' in log.columns else 'review_creation_date'
        for i, row in log.iterrows():
            random_timedelta = pd.Timedelta(f"{50 + np.random.rand() * 50}ms")
            log_added_ingest_time.loc[i, 'ingest_time'] = row[timestamp_col] + random_timedelta

        return log_added_ingest_time