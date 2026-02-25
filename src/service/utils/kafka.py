from typing import Iterable, Union, Tuple

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import NewTopic, AdminClient
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka import KafkaException
from confluent_kafka.error import KafkaError

from service.utils.schema.registry_manager import *
from config.kafka import *

import time

def get_confluent_kafka_admin_client(bootstrp_servers: str) -> AdminClient:
    bootstrp_server_list = bootstrp_servers.split(",")
    admin_config = {
        'bootstrap.servers': bootstrp_server_list[0]
    }
    
    return AdminClient(admin_config)

def get_confluent_kafka_consumer(group_id: str, topic_names:list, use_internal=False) -> DeserializingConsumer:
    if not use_internal:
        bootstrap_server_list = BOOTSTRAP_SERVERS_EXTERNAL.split(',')
    else:
        bootstrap_server_list = BOOTSTRAP_SERVERS_INTERNAL.split(',')

    client = SchemaRegistryManager._get_client(use_internal)
    avro_deserializer = AvroDeserializer(client)

    # TODO: dev 모드에서 자동 오프셋 커밋을 토글할 수 있어야함
    consumer_conf = {
        'bootstrap.servers': bootstrap_server_list[0],  # Kafka 브로커
        'auto.offset.reset': 'earliest',
        'group.id': group_id,
        'key.deserializer': StringDeserializer('utf-8'),  # 키는 문자열 가정
        'value.deserializer': avro_deserializer,
        'enable.auto.commit': False # in dev mode:
    }
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(topic_names)
    return consumer

def get_confluent_serializer_conf(topic: str, use_internal=False) -> Tuple[Union[AvroSerializer, StringSerializer], List[str]]:
    if not use_internal:
        bootstrap_server_list = BOOTSTRAP_SERVERS_EXTERNAL.split(',')
    else:
        bootstrap_server_list = BOOTSTRAP_SERVERS_INTERNAL.split(',')

    if topic is None or topic == '':
        # JSON 직렬화 함수 반환
        # Confluent 라이브러리의 규칙: AvroSerializer와 같은 Confluent의 공식 직렬화기(Serializer)들은 데이터를 직렬화할 때 항상 2개의 인자를 받도록 설계됨
        def json_serializer(data, ctx):
            if isinstance(data, dict):
                return json.dumps(data).encode('utf-8')
            else:
                return json.dumps(str(data)).encode('utf-8')
        serializer = json_serializer
    else:
        client = SchemaRegistryManager._get_client(use_internal)
        schema_obj = client.get_latest_version(topic).schema
        serializer = AvroSerializer(
            client,
            schema_obj,  # None으로 두면 subject 기반으로 fetch
            to_dict=lambda obj, ctx: obj,
            conf={
                'auto.register.schemas': False,
                'subject.name.strategy': lambda ctx, record_name: topic
                }
        )
    return serializer, bootstrap_server_list

def get_confluent_kafka_producer(bootstrap_server_list: List[str], serializer: Union[AvroSerializer, StringSerializer]) -> SerializingProducer:
    # TODO: Refactor `*BronzeProducer` to use an AVSC file for key schema instead of hard-coding in Python class.
    producer_conf = {
        'bootstrap.servers': ','.join(bootstrap_server_list),
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': serializer,
        
        # [정합성/안정성 설정]
        'acks': 'all',              # 데이터 유실 방지를 위해 모든 복제본(ISR)의 쓰기 확인
        'enable.idempotence': True, # 프로듀서 단의 멱등성 보장으로 중복 전송 방지
        'retries': 5,               # 일시적 네트워크 장애 시 자동 재시도 횟수 상향
        
        # [성능/비용 최적화 설정]
        'compression.type': 'zstd', # 높은 압축률로 네트워크 대역폭 비용 최적화
        'linger.ms': 10,            # 10ms 대기를 통해 메시지를 배치(Batch)로 묶어 Throughput 증대
        'batch.size': 65536,        # 배치의 최대 크기를 64KB로 설정하여 I/O 효율화
        
        # [연결 설정]
        'max.in.flight.requests.per.connection': 5 # 멱등성 유지와 순서 보장을 위한 동시 요청 제한
    }
    return SerializingProducer(producer_conf)

def delete_topics(admin_client: AdminClient, topic_names_to_delete: Iterable[str]):
    """
    Asynchronously deletes topics. The call returns a dict of futures.
    We wait for each future to finish to check for errors.
    """
    
    # delete_topics는 토픽 이름과 Future 객체를 담은 딕셔너리를 반환합니다.
    fs = admin_client.delete_topics(topic_names_to_delete)

    # 각 토픽의 Future 결과를 기다리며 성공/실패를 확인합니다.
    for topic, f in fs.items():
        try:
            # f.result()를 호출하여 작업이 완료될 때까지 기다립니다.
            f.result()
            print(f"Deleted topic: {topic}")
        except KafkaException as e:
            # f.result()에서 예외가 발생한 경우입니다.
            # 에러 코드를 확인하여 존재하지 않는 토픽인지 확인합니다.
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"Topic {topic} does not exist, skipping deletion.")
            else:
                # 그 외 다른 카프카 에러
                print(f"Error deleting topic {topic}: {e}")

def create_topics(admin_client: AdminClient, topics_names_to_create: Iterable[str], num_partitions:int = 1, replication_factor:int = 2):
    """
    Asynchronously creates topics. The call returns a dict of futures.
    We wait for each future to finish to check for errors.
    """
    new_topics = [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor) for topic in topics_names_to_create]
    
    # create_topics는 토픽 이름과 Future 객체를 담은 딕셔너리를 반환합니다.
    fs = admin_client.create_topics(new_topics)

    # 각 토픽의 Future 결과를 기다리며 성공/실패를 확인합니다.
    for topic, f in fs.items():
        try:
            # f.result()를 호출하면 작업이 완료될 때까지 기다립니다.
            # 성공하면 아무것도 반환하지 않습니다.
            f.result()
            print(f"Created topic: {topic}")
        except KafkaException as e:
            # f.result()에서 예외가 발생한 경우입니다.
            # 에러 코드를 확인하여 이미 존재하는 토픽인지 확인합니다.
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Topic {topic} already exists, skipping creation.")
            else:
                # 그 외 다른 카프카 에러
                print(f"Failed to create topic {topic}: {e}")

def wait_for_partition_assignment(consumer):
    max_attempts = 10
    for _ in range(max_attempts):
        if consumer.assignment():
            print('Consumer partition assignment loaded!')
            return consumer
        consumer.poll(1)
        time.sleep(10)
    raise TimeoutError("Consumer 파티션 할당 실패")

# from kafka import KafkaConsumer, KafkaProducer
# from kafka.admin import KafkaAdminClient

# def get_kafka_admin_client(bootstrp_servers: str) -> KafkaAdminClient:
#     bootstrp_server_list = bootstrp_servers.split(",")
#     return KafkaAdminClient(
#         bootstrap_servers=bootstrp_server_list
#     )

# def get_kafka_producer(bootstrp_servers: Iterable[str]) -> KafkaProducer:
#     bootstrap_server_list = bootstrp_servers.split(',')
#     return KafkaProducer(
#         bootstrap_servers=bootstrap_server_list,
#         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#         key_serializer=lambda k: str(k).encode('utf-8') if k else None,
#         acks='all',
#         retries=3,
#         max_in_flight_requests_per_connection=1
#     )

# def get_kafka_consumer(bootstrp_servers: Iterable[str], topic_name: Iterable[str]) -> KafkaConsumer:
#     consumer = KafkaConsumer(
#         bootstrap_servers=bootstrp_servers,
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         group_id=None,
#         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
#         key_deserializer=lambda k: k.decode('utf-8') if k else None
#     )

#     consumer.subscribe(topic_name)
#     wait_for_partition_assignment(consumer)
#     return consumer