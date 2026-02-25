docker buildx create --name multiarch --driver docker-container --bootstrap --use || docker buildx use multiarch
docker run --privileged --rm tonistiigi/binfmt --install all

# add token for ghcr.io
export CR_PAT= && echo $CR_PAT | docker login ghcr.io -u jmhwang-dev --password-stdin

DOCKER_BUILDKIT=1 docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --no-cache \
  -t ghcr.io/jmhwang-dev/warehouse:sp3.5.6-ice1.9.1 \
  --push \
  ./infra/spark