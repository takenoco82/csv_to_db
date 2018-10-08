docker-compose stop
docker-compose build
docker-compose up -d
docker exec python sh -c "cd /csv_to_db && nosetests -v"
