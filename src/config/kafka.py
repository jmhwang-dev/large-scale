from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv('./configs/kafka/.env.kafka')
BOOTSTRAP_SERVERS_EXTERNAL = os.getenv("BOOTSTRAP_SERVERS_EXTERNAL", "192.168.45.190:19092,192.168.45.190:19094,192.168.45.190:19096")
BOOTSTRAP_SERVERS_INTERNAL = os.getenv("BOOTSTRAP_SERVERS_INTERNAL", "kafka1:9092,kafka2:9092,kafka3:9092")
DATASET_DIR = Path(os.getenv("DATASET_DIR", "./downloads/olist_redefined"))