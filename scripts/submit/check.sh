# 1. 컨테이너 내부로 접속 (또는 exec로 바로 실행)
# docker exec -it spark-client bash
docker compose --env-file ./configs/spark/.env.desktop \
-f docker-compose.spark-driver.yml exec spark-client spark-submit \
  --master spark://192.168.45.192:7077 \
  /jobs/check.py