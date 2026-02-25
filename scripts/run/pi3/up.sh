docker compose \
    -f docker-compose.spark-worker.yml \
    --env-file ./configs/spark/.env.pi3 \
    up -d --force-recreate