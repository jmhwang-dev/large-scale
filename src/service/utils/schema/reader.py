import json
from service.utils.schema.registry_manager import SchemaRegistryManager
from confluent_kafka.schema_registry.error import SchemaRegistryError

class AvscReader:
    def __init__(self, schema_name, use_internal=False, is_stream:bool=False):
        self.subnamespace = 'batch' if not is_stream else 'stream'
        self.schema_name = schema_name
        self.client = SchemaRegistryManager._get_client(use_internal)

        # SchemaRegistryError could be raised if schema_name != topic_name
        self.schema_str = self.client.get_latest_version(schema_name).schema.schema_str
        self.schema_id = self.client.get_latest_version(schema_name).schema_id
        self.set_metadata(self.schema_str)

    def set_metadata(self, schema_str):
        """
        schema template
        {
            "type":"record",
            "namespace":"bronze",
            "name":"customer",
            "fields":[
                {"name":"customer_id","type":"string"},
                {"name":"zip_code","type":"int"}
            ]
        }
        """
        self.json_schema = json.loads(schema_str)
        self.namespace = self.json_schema.get('namespace')
        self.table_name = self.json_schema.get('name')

        self.dst_table_identifier = f"{self.namespace}.{self.subnamespace}.{self.table_name}"