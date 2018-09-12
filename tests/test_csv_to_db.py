from app import database_connection
from tests import csv_to_db
from datetime import datetime
import unittest
import _mysql_exceptions


class Test_csv_to_db(unittest.TestCase):
    '''
    tests.csv_to_db のテストです
    '''

    @classmethod
    def setUpClass(cls):
        cls.conn = database_connection.get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        try:
            # データ初期化: example1
            truncate_sql_example1 = "TRUNCATE TABLE example1"
            insert_sql_example1 = "INSERT INTO example1 VALUES (%s, %s, %s, %s, %s)"
            with self.conn.cursor() as cur:
                cur.execute(truncate_sql_example1)
                cur.execute(insert_sql_example1, (1, "aha", 1234567890, 123.456, "2018-08-01 12:34:56"))
                cur.execute(insert_sql_example1, (2, None, None, None, None))
                self.conn.commit()

        except Exception as e:
            self.conn.close()
            raise e

    def tearDown(self):
        pass

    # 1テーブルのデータの初期化
    def test_load_success(self):
        # 念のため、csvファイルをロードする前の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 2)
        # データの確認：1件目
        record = records[0]
        self.assertEqual(record['id'], 1)
        self.assertEqual(record['varchar_col'], 'aha')
        self.assertEqual(record['int_col'], 1234567890)
        self.assertEqual(record['double_col'], 123.456)
        self.assertEqual(record['datetime_col'], datetime(2018, 8, 1, 12, 34, 56))
        # データの確認：2件目
        record = records[1]
        self.assertEqual(record['id'], 2)
        self.assertEqual(record['varchar_col'], None)
        self.assertEqual(record['int_col'], None)
        self.assertEqual(record['double_col'], None)
        self.assertEqual(record['datetime_col'], None)

        # csvファイル内容で初期化
        csv_to_db.load(self.conn,
                       'example1',
                       'tests/data/example1_test_load_csv.csv')
        self.conn.commit()

        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 4)
        # データの確認：1件目
        record = records[0]
        self.assertEqual(record['id'], 11)
        self.assertEqual(record['varchar_col'], 'ihi')
        self.assertEqual(record['int_col'], 987654321)
        self.assertEqual(record['double_col'], 654.321)
        self.assertEqual(record['datetime_col'], datetime(2018, 8, 2, 11, 22, 33))
        # データの確認：2件目
        record = records[1]
        self.assertEqual(record['id'], 12)
        self.assertEqual(record['varchar_col'], 'う,ふ')
        self.assertEqual(record['int_col'], -1234567890)
        self.assertEqual(record['double_col'], -123.456)
        self.assertEqual(record['datetime_col'], datetime(2018, 8, 3, 0, 0, 0))
        # データの確認：3件目
        record = records[2]
        self.assertEqual(record['id'], 13)
        self.assertEqual(record['varchar_col'], None)
        self.assertEqual(record['int_col'], None)
        self.assertEqual(record['double_col'], None)
        self.assertEqual(record['datetime_col'], None)
        # データの確認：4件目
        record = records[3]
        self.assertEqual(record['id'], 14)
        self.assertEqual(record['varchar_col'], '')
        self.assertEqual(record['int_col'], 0)
        self.assertEqual(record['double_col'], 0)
        self.assertEqual(record['datetime_col'], None)

    # 複数テーブルのデータの初期化
    def test_load_success_multi(self):
        # データ初期化: example2
        truncate_sql_example2 = "TRUNCATE TABLE example2"
        insert_sql_example2 = "INSERT INTO example2 VALUES (%s, %s)"
        with self.conn.cursor() as cur:
            cur.execute(truncate_sql_example2)
            cur.execute(insert_sql_example2, (1, "ahaha"))
            self.conn.commit()

        # 念のため、csvファイルをロードする前の状態を確認
        sql = "SELECT * FROM example2 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 1)
        # データの確認：1件目
        record = records[0]
        self.assertEqual(record['id'], 1)
        self.assertEqual(record['col1'], 'ahaha')

        # csvファイル内容で初期化
        csv_to_db.load(self.conn,
                       'example1',
                       'tests/data/example1_test_load_csv.csv')
        csv_to_db.load(self.conn,
                       'example2',
                       'tests/data/example2_test_load_csv.csv')
        self.conn.commit()

        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 4)
        # データの確認
        record = records[0]
        self.assertEqual(record['id'], 11)
        record = records[1]
        self.assertEqual(record['id'], 12)
        record = records[2]
        self.assertEqual(record['id'], 13)
        record = records[3]
        self.assertEqual(record['id'], 14)

        sql = "SELECT * FROM example2 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 2)
        # データの確認
        record = records[0]
        self.assertEqual(record['id'], 101)
        self.assertEqual(record['col1'], 'ihihi')
        record = records[1]
        self.assertEqual(record['id'], 102)
        self.assertEqual(record['col1'], 'ufufu')

    # データの初期化をしない場合
    def test_load_success_no_truncate(self):
        # csvファイル内容を登録
        csv_to_db.load(self.conn,
                       'example1',
                       'tests/data/example1_test_load_csv.csv',
                       truncate=False)
        self.conn.commit()

        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認（2 + 4 -> 6）
        self.assertEqual(len(records), 6)
        # データの確認：1件目
        record = records[0]
        self.assertEqual(record['id'], 1)
        self.assertEqual(record['varchar_col'], 'aha')
        self.assertEqual(record['int_col'], 1234567890)
        self.assertEqual(record['double_col'], 123.456)
        self.assertEqual(record['datetime_col'], datetime(2018, 8, 1, 12, 34, 56))
        # データの確認：3件目
        record = records[2]
        self.assertEqual(record['id'], 11)
        self.assertEqual(record['varchar_col'], 'ihi')
        self.assertEqual(record['int_col'], 987654321)
        self.assertEqual(record['double_col'], 654.321)
        self.assertEqual(record['datetime_col'], datetime(2018, 8, 2, 11, 22, 33))
        # データの確認：5件目
        record = records[4]
        self.assertEqual(record['id'], 13)
        self.assertEqual(record['varchar_col'], None)
        self.assertEqual(record['int_col'], None)
        self.assertEqual(record['double_col'], None)
        self.assertEqual(record['datetime_col'], None)

    # csvファイルが見つからない場合
    def test_load_error_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            csv_to_db.load(self.conn,
                           'example1',
                           'path/to/example1_test_load_csv.csv')

        # テーブルが変更されてないことを確認
        sql = "SELECT * FROM example1 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 2)
        # データの確認
        record = records[0]
        self.assertEqual(record['id'], 1)
        record = records[1]
        self.assertEqual(record['id'], 2)

    # テーブルが存在しない場合
    def test_load_error_table_not_found(self):
        with self.assertRaises(_mysql_exceptions.ProgrammingError):
            csv_to_db.load(self.conn,
                           'example9',
                           'tests/data/example1_test_load_csv.csv')

    # csvファイルのデータとテーブル定義が一致しない場合
    def test_load_error_type(self):
        with self.assertRaises(_mysql_exceptions.OperationalError):
            csv_to_db.load(self.conn,
                           'example1',
                           'tests/data/example1_test_load_csv_error.csv')

    @csv_to_db.init({'example1': 'tests/data/example1_test_load_csv.csv'})
    def test_init_success_one(self):
        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 4)
        # データの確認
        record = records[0]
        self.assertEqual(record['id'], 11)
        record = records[1]
        self.assertEqual(record['id'], 12)
        record = records[2]
        self.assertEqual(record['id'], 13)
        record = records[3]
        self.assertEqual(record['id'], 14)

    @csv_to_db.init({
        'example1': 'tests/data/example1_test_load_csv.csv',
        'example2': 'tests/data/example2_test_load_csv.csv'})
    def test_init_success_multi(self):
        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 4)
        # データの確認
        record = records[0]
        self.assertEqual(record['id'], 11)
        record = records[1]
        self.assertEqual(record['id'], 12)
        record = records[2]
        self.assertEqual(record['id'], 13)
        record = records[3]
        self.assertEqual(record['id'], 14)

        sql = "SELECT * FROM example2 ORDER BY id"
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), 2)
        # データの確認
        record = records[0]
        self.assertEqual(record['id'], 101)
        record = records[1]
        self.assertEqual(record['id'], 102)


def main():
    unittest.main()


if __name__ == '__main__':
    main()
