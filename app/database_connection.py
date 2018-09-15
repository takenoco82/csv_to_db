import pymysql.cursors
import os
import unittest


def get_connection():
    """ DB接続を取得する """
    return pymysql.connect(
        # ローカルから起動するときは 127.0.0.1 を使う
        host=os.getenv('MYSQL_HOST', '127.0.0.1'),
        user='root',
        password='root',
        port=3306,
        db='sandbox',
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )


class TestGetConnection(unittest.TestCase):
    def test_get_connection(self):
        conn = get_connection()
        # 接続できているのでエラーにならない
        conn.ping(reconnect=False)

        # クローズしているのでエラーになる
        conn.close()
        with self.assertRaises(Exception):
            conn.ping(reconnect=False)


def main():
    unittest.main()


if __name__ == '__main__':
    main()
