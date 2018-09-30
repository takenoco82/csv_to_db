export PYTHONPATH=$PYTHONPATH:`pwd`
mysql -h 127.0.0.1 -P 3306 -uroot -proot -D sandbox < setup/mysql/init/init_database.sql
python tests/csv_to_db.py
