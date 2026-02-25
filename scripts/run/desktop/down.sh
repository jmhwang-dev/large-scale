docker compose \
    -f docker-compose.spark-driver.yml \
    --env-file ./configs/spark/.env.desktop \
    down -v