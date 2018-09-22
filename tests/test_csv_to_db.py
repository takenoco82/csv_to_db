from app import database_connection
from tests import csv_to_db
from datetime import datetime
import unittest


TRUNCATE_SQL_EXAMPLE1 = "TRUNCATE TABLE example1"

INSERT_SQL_EXAMPLE1 = '''
        INSERT INTO example1 VALUES (
            %(id)s, %(varchar_col)s, %(int_col)s, %(double_col)s, %(datetime_col)s)
    '''

# example1の初期データ
INITIAL_DATA_EXAMPLE1 = [
    {
        'id': 1,
        'varchar_col': 'aha',
        'int_col': 1234567890,
        'double_col': 123.456,
        'datetime_col': datetime(2018, 8, 1, 12, 34, 56),
    },
    {
        'id': 2,
        'varchar_col': None,
        'int_col': None,
        'double_col': None,
        'datetime_col': None,
    },
]

# example1のテストで取り込むcsvファイル
TEST_FILE_EXAMPLE1 = "tests/data/example1_test_load_csv.csv"
# example1のテストデータ（tests/data/example1_test_load_csv.csvの内容）
TEST_DATA_EXAMPLE1 = [
    {
        'id': 11,
        'varchar_col': 'ihi',
        'int_col': 987654321,
        'double_col': 654.321,
        'datetime_col': datetime(2018, 8, 2, 11, 22, 33),
    },
    {
        'id': 12,
        'varchar_col': 'う,ふ',
        'int_col': -1234567890,
        'double_col': -123.456,
        'datetime_col': datetime(2018, 8, 3, 0, 0, 0),
    },
    {
        'id': 13,
        'varchar_col': None,
        'int_col': None,
        'double_col': None,
        'datetime_col': None,
    },
    {
        'id': 14,
        'varchar_col': '',
        'int_col': 0,
        'double_col': 0,
        'datetime_col': None,
    },
]

# example2のテストで取り込むcsvファイル
TEST_FILE_EXAMPLE2 = "tests/data/example2_test_load_csv.csv"
# example1のテストデータ（tests/data/example2_test_load_csv.csvの内容）
TEST_DATA_EXAMPLE2 = [
    {
        'id': 101,
        'col1': 'ihihi',
    },
    {
        'id': 102,
        'col1': 'ufufu',
    },
]


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
        # データ初期化: example1
        with self.conn.cursor() as cur:
            cur.execute(TRUNCATE_SQL_EXAMPLE1)
            for data in INITIAL_DATA_EXAMPLE1:
                cur.execute(INSERT_SQL_EXAMPLE1, data)
            self.conn.commit()

    def tearDown(self):
        pass

    # 1テーブルのデータの初期化
    def test_load_success(self):
        expected = TEST_DATA_EXAMPLE1

        # csvファイル内容で初期化
        csv_to_db.load(
            self.conn,
            example1=TEST_FILE_EXAMPLE1)
        self.conn.commit()

        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected))
        # データの確認
        for i, expected_row in enumerate(expected):
            with self.subTest(i=i):
                self.assertDictEqual(records[i], expected_row)

    # 複数テーブルのデータの初期化
    def test_load_success_multi(self):
        expected_example1 = TEST_DATA_EXAMPLE1
        expected_example2 = TEST_DATA_EXAMPLE2

        # csvファイル内容で初期化
        csv_to_db.load(
            self.conn,
            example1=TEST_FILE_EXAMPLE1,
            example2=TEST_FILE_EXAMPLE2)
        self.conn.commit()

        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected_example1))
        # データの確認
        for i, expected_row in enumerate(expected_example1):
            with self.subTest(i=i):
                self.assertDictEqual(records[i], expected_row)

        sql = "SELECT * FROM example2 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected_example2))
        # データの確認
        for i, expected in enumerate(expected_example2):
            with self.subTest(i=i):
                self.assertDictEqual(records[i], expected)

    # データの初期化をしない場合
    def test_load_success_no_truncate(self):
        expected = INITIAL_DATA_EXAMPLE1 + TEST_DATA_EXAMPLE1

        # csvファイル内容を登録（既存のデータは消さない）
        csv_to_db.load(
            self.conn,
            truncate=False,
            example1=TEST_FILE_EXAMPLE1)
        self.conn.commit()

        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認（2 + 4 -> 6）
        self.assertEqual(len(records), len(expected))
        # データの確認
        for i, expected_row in enumerate(expected):
            with self.subTest(i=i):
                self.assertDictEqual(records[i], expected_row)

    # csvファイルが見つからない場合
    def test_load_error_file_not_found(self):
        # TRUNCATEされてるので（rollbackしてもデータが戻らないため）0件になる
        expected = []

        # 例外の確認
        with self.assertRaises(FileNotFoundError):
            try:
                self.conn.begin()
                csv_to_db.load(
                    self.conn,
                    # 存在しないファイルを指定
                    example1='path/to/example1_test_load_csv.csv')
            except Exception as e:
                self.conn.rollback()
                raise e

        # 0件になっていることを確認
        sql = "SELECT * FROM example1 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected))

    # テーブルが存在しない場合
    def test_load_error_table_not_found(self):
        # TRUNCATEされないので戻る
        expected = INITIAL_DATA_EXAMPLE1

        # 例外の確認
        with self.assertRaises(Exception):
            try:
                self.conn.begin()
                csv_to_db.load(
                    self.conn,
                    # 存在しないテーブル名を指定
                    example9=TEST_FILE_EXAMPLE1)
            except Exception as e:
                self.conn.rollback()
                raise e

        # 戻っていることを確認
        sql = "SELECT * FROM example1 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected))

    # csvファイルのデータとテーブル定義が一致しない場合
    def test_load_error_type(self):
        # TRUNCATEされてるので（rollbackしてもデータが戻らないため）0件になる
        expected = []

        # 例外の確認
        with self.assertRaises(Exception):
            try:
                self.conn.begin()
                csv_to_db.load(
                    self.conn,
                    # int型の項目に文字列を指定
                    example1='tests/data/example1_test_load_csv_error.csv')
            except Exception as e:
                self.conn.rollback()
                raise e

        # 0件になっていることを確認
        sql = "SELECT * FROM example1 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected))

    @csv_to_db.setup_load(example1=TEST_FILE_EXAMPLE1)
    def test_setup_load_success(self):
        expected = TEST_DATA_EXAMPLE1

        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected))
        # データの確認
        for i, expected_row in enumerate(expected):
            with self.subTest(i=i):
                self.assertDictEqual(records[i], expected_row)

    @csv_to_db.setup_load(
        example1=TEST_FILE_EXAMPLE1,
        example2=TEST_FILE_EXAMPLE2)
    def test_setup_load_success_multi(self):
        expected_example1 = TEST_DATA_EXAMPLE1
        expected_example2 = TEST_DATA_EXAMPLE2

        # csvファイルをロードした後の状態を確認
        sql = "SELECT * FROM example1 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected_example1))
        # データの確認
        for i, expected_row in enumerate(expected_example1):
            with self.subTest(i=i):
                self.assertDictEqual(records[i], expected_row)

        sql = "SELECT * FROM example2 ORDER BY id"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            records = cur.fetchall()
        # 件数の確認
        self.assertEqual(len(records), len(expected_example2))
        # データの確認
        for i, expected in enumerate(expected_example2):
            with self.subTest(i=i):
                self.assertDictEqual(records[i], expected)


def main():
    unittest.main()


if __name__ == '__main__':
    main()
