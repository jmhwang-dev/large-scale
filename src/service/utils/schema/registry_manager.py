import os
import logging
import json
from typing import List
from functools import wraps
from dotenv import load_dotenv
from typing import Optional, Callable, Any
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

# 로거 인스턴스만 가져옵니다.
# 설정(basicConfig)은 이 파일을 import하는 실행 스크립트에서 담당합니다.
logger: logging.Logger = logging.getLogger(__name__)

def _handle_sr_errors(func: Callable) -> Callable:
    """SchemaRegistryError를 처리하고 클라이언트를 리셋하는 데코레이터."""
    @wraps(func)
    def wrapper(cls: 'SchemaRegistryManager', *args: Any, **kwargs: Any) -> Any:
        try:
            return func(cls, *args, **kwargs)
        except SchemaRegistryError as e:
            logger.error(f"Schema Registry 작업 실패 (함수: {func.__name__}): {e}")
            cls._reset_client()
            raise
    return wrapper

class SchemaRegistryManager:
    """Confluent Schema Registry와의 모든 상호작용을 관리하는 유틸리티 클래스."""

    load_dotenv('./configs/kafka/.env.kafka')
    SCHEMA_REGISTRY_URL: str = os.environ.get("SCHEMA_REGISTRY_INTERNAL_URL", "http://schema-registry:8081")
    _client: Optional[SchemaRegistryClient] = None

    @classmethod
    def _get_client(cls, use_internal=True) -> SchemaRegistryClient:
        """SchemaRegistryClient 인스턴스를 지연 초기화하여 반환합니다 (싱글턴 패턴)."""
        if not use_internal:
            cls.SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_EXTERNAL_URL", "http://192.168.45.190:8082")

        if cls._client is None:
            cls._client = SchemaRegistryClient({"url": cls.SCHEMA_REGISTRY_URL})
            logger.debug(f"SchemaRegistryClient 초기화 완료: {cls.SCHEMA_REGISTRY_URL}")
        return cls._client

    @classmethod
    def _reset_client(cls) -> None:
        """네트워크 문제 등 발생 시 클라이언트 연결을 리셋합니다."""
        cls._client = None
        logger.warning("SchemaRegistryClient가 리셋되었습니다. 다음 호출 시 재연결됩니다.")

    @classmethod
    @_handle_sr_errors
    def set_compatibility(cls, subject_name: str, level: str = "BACKWARD") -> None:
        """주제(Subject)의 호환성 레벨을 설정합니다."""
        client = cls._get_client(use_internal=False)
        client.set_compatibility(subject_name, level)
        logger.info(f"'{subject_name}' 주제의 호환성을 '{level}'로 설정했습니다.")

    @classmethod
    @_handle_sr_errors
    def register_schema(cls, subject_name: str, schema_str: str, schema_type: str = "AVRO") -> int:
        """
        새로운 스키마를 주제에 등록합니다.
        스키마가 이미 존재하면 기존 ID를 반환하고, 없으면 새로 등록합니다.
        """
        client = cls._get_client()
        schema = Schema(schema_str, schema_type=schema_type)

        try:
            registered_schema = client.lookup_schema(subject_name, schema)
            logger.info(
                f"스키마가 '{subject_name}'에 이미 존재합니다. "
                f"(ID: {registered_schema.schema_id}, Version: {registered_schema.version})"
            )
            return registered_schema.schema_id
        except SchemaRegistryError as e:
            if "not found" not in str(e).lower():
                raise

        schema_id = client.register_schema(subject_name=subject_name, schema=schema)
        logger.info(f"새로운 스키마를 '{subject_name}'에 등록했습니다. (ID: {schema_id})")
        return schema_id
    
    @classmethod
    @_handle_sr_errors
    def delete_schema_version(cls, subject_name: str, version: int) -> list[int]:
        """
        주제(Subject)의 특정 버전 스키마를 삭제합니다. (Soft Delete)
        
        :param subject_name: 삭제할 스키마가 속한 주제
        :param version: 삭제할 버전 번호
        :return: 삭제 후 남아있는 버전들의 리스트
        """
        client = cls._get_client()
        logger.warning(f"'{subject_name}' 주제의 '{version}' 버전을 삭제합니다.")
        # Confluent 라이브러리는 삭제된 버전 번호를 반환하지만, 여기서는 남아있는 버전을 조회하는 것이 더 명확할 수 있습니다.
        # 실제 delete_version 호출은 삭제된 버전 번호 하나만 반환합니다.
        deleted_version = client.delete_version(subject_name, version)
        logger.info(f"'{subject_name}' 주제에서 '{deleted_version}' 버전이 삭제되었습니다.")
        # 남아있는 버전을 확인하려면 별도 조회가 필요
        versions = client.get_versions(subject_name)
        return versions

    @classmethod
    def delete_subject(cls, subject_name_list: List[str], use_internal: bool) -> None:
        """
        주제(Subject)와 관련된 모든 버전의 스키마를 영구적으로 삭제합니다. (Hard Delete)
        !!주의!! 이 작업은 되돌릴 수 없으며 관련 데이터를 읽지 못하게 할 수 있습니다.
        
        :param subject_name_list: 삭제할 주제
        :use_internal: 클라이언트 네트워크 위치가 외부인지 내부인지
        """
        client = cls._get_client(use_internal)
        for subject_name in subject_name_list:
            try:
                deleted_versions = client.delete_subject(subject_name, permanent=False) # soft delete
                deleted_versions = client.delete_subject(subject_name, permanent=True)  # hard delete
                print(f'Deleted Schema version: {deleted_versions} {subject_name}')
            except SchemaRegistryError as e:
                print(e)
        return
    
# TODO: logger 정리
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
from config.confluent import *

def setup_logging():
    """스크립트의 로깅 설정을 구성합니다."""
    # 모든 로거의 기본 레벨을 DEBUG로 설정하여 모든 메시지를 핸들러로 전달
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # 1. 콘솔 핸들러: 간결한 포맷으로 INFO 레벨 이상의 로그만 출력
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)

    # 2. 파일 핸들러: 상세한 포맷으로 DEBUG 레벨 이상의 모든 로그를 파일에 기록
    Path("logs").mkdir(exist_ok=True)
    file_handler = logging.FileHandler("logs/schema_registration.log", mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # 설정된 핸들러들을 루트 로거에 추가
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

def process_schema_file(schema_path: Path):
    """단일 스키마 파일을 읽고 Schema Registry에 등록합니다."""
    logger.info(f"--- 처리 시작: {schema_path.relative_to(SCHEMA_DIR)} ---")
    try:
        schema_str = schema_path.read_text(encoding="utf-8")
        schema_json = json.loads(schema_str)
        subject_name = schema_json.get("name")

        # SchemaRegistryManager.delete_subject(subject_name)

        if not subject_name:
            logger.warning(f"'{schema_path.name}' 파일에 'name' 필드가 없어 건너뜁니다.")
            return

        logger.info(f"주제: '{subject_name}' 확인")
        SchemaRegistryManager.set_compatibility(subject_name)
        SchemaRegistryManager.register_schema(subject_name, schema_str)

    except FileNotFoundError:
        logger.error(f"파일을 찾을 수 없습니다: {schema_path}")
    except json.JSONDecodeError:
        logger.error(f"'{schema_path.name}' 파일의 JSON 형식이 올바르지 않습니다.")
    except Exception as e:
        logger.error(f"'{schema_path.name}' 처리 중 예상치 못한 오류 발생", exc_info=True)

def register_schema():
    """메인 실행 함수: 스키마 디렉토리를 순회하며 모든 .avsc 파일을 등록합니다."""
    setup_logging()  # 로깅 설정 실행

    logger.info("=" * 40)
    logger.info("Schema Registry 스크립트 시작")
    logger.info(f"대상 디렉토리: {SCHEMA_DIR.absolute()}")
    logger.info("=" * 40)

    if not SCHEMA_DIR.is_dir():
        logger.critical(f"스키마 디렉토리 '{SCHEMA_DIR}'를 찾을 수 없어 스크립트를 종료합니다.")
        return

    for tier_dir in sorted(SCHEMA_DIR.iterdir()):
        if not tier_dir.is_dir():
            continue

        logger.info(f"[{tier_dir.name.upper()} 계층 스키마 처리 시작]")
        schema_files = sorted(tier_dir.glob('*.avsc'))
        
        if not schema_files:
            logger.info(f"'{tier_dir.name}'에 등록할 스키마 파일(.avsc)이 없습니다.")
            continue
            
        for schema_path in schema_files:
            process_schema_file(schema_path)

    logger.info("=" * 40)
    logger.info("모든 스키마 처리가 완료되었습니다.")
    logger.info("=" * 40)