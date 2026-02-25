docker compose \
    -f docker-compose.spark-worker.yml \
    --env-file ./configs/spark/.env.pi2 \
    up -d --force-recreate