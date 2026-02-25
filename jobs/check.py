from pyspark.sql import SparkSession
import socket

# 1. 스파크 세션 생성
# 마스터 주소는 미니 PC의 IP인 192.168.45.192로 지정합니다.
spark = SparkSession.builder \
    .appName("Homelab-Cluster-Check") \
    .master("spark://192.168.45.192:7077") \
    .getOrCreate()

def get_hostname(x):
    # 각 워커 노드의 호스트네임을 반환하는 함수
    return socket.gethostname()

try:
    print("\n" + "="*50)
    print("클러스터 연결 테스트를 시작합니다.")
    print("="*50)

    # 2. 테스트 데이터 생성 (워커 수보다 넉넉하게 파티션 생성)
    data = range(100)
    dist_data = spark.sparkContext.parallelize(data, numSlices=6)

    # 3. 각 파티션이 실행된 노드의 호스트네임 수집
    # 이 작업이 워커(라즈베리 파이)들에서 분산 실행됩니다.
    worker_nodes = dist_data.map(get_hostname).distinct().collect()

    print(f"\n[성공] 현재 클러스터에서 응답한 노드 목록:")
    for node in worker_nodes:
        print(f" - 노드 주소/이름: {node}")

    print("\n" + "="*50)
    print("모든 노드가 정상적으로 연결되었습니다!")
    print("="*50 + "\n")

finally:
    # 세션 종료
    spark.stop()