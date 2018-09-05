import MySQLdb
import MySQLdb.cursors
import os


def get_connection():
    """ DB接続を取得する """
    return MySQLdb.connect(
        # ローカルから起動するときは 127.0.0.1 を使う
        host=os.getenv('MYSQL_HOST', '127.0.0.1'),
        user='root',
        password='root',
        port=3306,
        db='sandbox',
        charset="utf8mb4",
        cursorclass=MySQLdb.cursors.DictCursor
    )
