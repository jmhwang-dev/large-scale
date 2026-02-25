# 실행 중인 모든 컨테이너 중지
docker stop $(docker ps -aq)

# 모든 컨테이너 삭제
docker rm $(docker ps -aq)
docker rmi -f $(docker images -aq)
docker network prune -f
docker volume prune -f
