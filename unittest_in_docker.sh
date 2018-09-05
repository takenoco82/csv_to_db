docker-compose up -d
docker exec python bash -c "cd /using-pandas && nosetests -v"
