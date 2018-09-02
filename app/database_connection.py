import MySQLdb
import MySQLdb.cursors


def get_connection():
    """ DB接続を取得する """
    return MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        password='root',
        port=3306,
        db='sandbox',
        cursorclass=MySQLdb.cursors.DictCursor
    )
