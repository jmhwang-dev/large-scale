from pathlib import Path

SCHEMA_DIR = Path('infra/confluent/schemas/')
SCHEMA_REGISTRY_INTERNAL_URL = "http://schema-registry:8081"
SCHEMA_REGISTRY_EXTERNAL_URL = "http://192.168.45.190:8082"
HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json"
}