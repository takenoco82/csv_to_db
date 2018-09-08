import unittest
from datetime import datetime
from app import database_connection
from app import example1
from tests import database_test_util


def count_example1(conn):
    sql = "select count(*) count from example1"
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchone()["count"]
        return result


class Test_select_example1_by_id(unittest.TestCase):
    '''
    app.example1.select_example1_by_id のテスト
    '''

    @classmethod
    def setUpClass(cls):
        cls.conn = database_connection.get_connection()
        # データ初期化
        database_test_util.load_csv(
            cls.conn,
            'example1',
            'tests/data/test_example1/example1_test_select_example1_by_id.csv')
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_select_example1_by_id_success(self):
        record = example1.select_example1_by_id(self.conn, 1)
        # データの確認：1件目
        self.assertEqual(record['id'], 1)
        self.assertEqual(record['varchar_col'], 'abcdefghij')
        self.assertEqual(record['int_col'], 1234567890)
        self.assertEqual(record['double_col'], 123.456)
        self.assertEqual(record['datetime_col'], datetime(2018, 8, 1, 12, 34, 56))

    def test_select_example1_by_id_success_none_value(self):
        record = example1.select_example1_by_id(self.conn, 2)
        # データの確認：1件目
        self.assertEqual(record['id'], 2)
        self.assertEqual(record['varchar_col'], None)
        self.assertEqual(record['int_col'], None)
        self.assertEqual(record['double_col'], None)
        self.assertEqual(record['datetime_col'], None)

    def test_select_example1_by_id_success_no_data(self):
        result = example1.select_example1_by_id(self.conn, 100)
        # 件数の確認
        self.assertIsNone(result)


class Test_select_example1(unittest.TestCase):
    '''
    app.example1.select_example1 のテスト
    '''

    @classmethod
    def setUpClass(cls):
        cls.conn = database_connection.get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        # データ初期化
        database_test_util.load_csv(
            self.conn,
            'example1',
            'tests/data/test_example1/example1_test_select_example1.csv')
        self.conn.commit()

    def tearDown(self):
        pass

    def test_select_example1_all(self):
        records = example1.select_example1(self.conn)
        # 件数の確認
        self.assertEqual(len(records), 8)
        # データの確認
        self.assertEqual(records[0]['id'], 11)
        self.assertEqual(records[1]['id'], 12)
        self.assertEqual(records[2]['id'], 13)
        self.assertEqual(records[3]['id'], 14)
        self.assertEqual(records[4]['id'], 15)
        self.assertEqual(records[5]['id'], 16)
        self.assertEqual(records[6]['id'], 17)
        self.assertEqual(records[7]['id'], 18)

    def test_select_example1_start(self):
        records = example1.select_example1(
            self.conn,
            start=datetime(2018, 1, 1, 0, 0, 0))
        # 件数の確認
        self.assertEqual(len(records), 6)
        # データの確認
        self.assertEqual(records[0]['id'], 12)
        self.assertEqual(records[1]['id'], 13)
        self.assertEqual(records[2]['id'], 14)
        self.assertEqual(records[3]['id'], 15)
        self.assertEqual(records[4]['id'], 16)
        self.assertEqual(records[5]['id'], 17)

    def test_select_example1_end(self):
        records = example1.select_example1(
            self.conn,
            end=datetime(2018, 1, 2, 0, 0, 0))
        # 件数の確認
        self.assertEqual(len(records), 6)
        # データの確認
        self.assertEqual(records[0]['id'], 11)
        self.assertEqual(records[1]['id'], 12)
        self.assertEqual(records[2]['id'], 13)
        self.assertEqual(records[3]['id'], 14)
        self.assertEqual(records[4]['id'], 15)
        self.assertEqual(records[5]['id'], 16)

    def test_select_example1_start_end(self):
        records = example1.select_example1(
            self.conn,
            start=datetime(2018, 1, 1, 0, 0, 0),
            end=datetime(2018, 1, 2, 0, 0, 0))
        # 件数の確認
        self.assertEqual(len(records), 5)
        # データの確認
        self.assertEqual(records[0]['id'], 12)
        self.assertEqual(records[1]['id'], 13)
        self.assertEqual(records[2]['id'], 14)
        self.assertEqual(records[3]['id'], 15)
        self.assertEqual(records[4]['id'], 16)

    def test_select_example1_start_equal_end(self):
        records = example1.select_example1(
            self.conn,
            start=datetime(2018, 1, 1, 0, 0, 0),
            end=datetime(2018, 1, 1, 0, 0, 0))
        # 件数の確認
        self.assertEqual(len(records), 1)
        # データの確認
        self.assertEqual(records[0]['id'], 12)

    def test_select_example1_no_data(self):
        records = example1.select_example1(
            self.conn,
            start=datetime(2017, 12, 31, 0, 0, 0),
            end=datetime(2017, 12, 31, 23, 59, 58))
        # 件数の確認
        self.assertEqual(len(records), 0)

    @database_test_util.setup_load_csv(
        'example1',
        'tests/data/test_example1/example1_test_select_example1_sort_by_id.csv')
    def test_select_example1_sort_by_id(self):
        records = example1.select_example1(self.conn, sort=example1.Sort.id)
        # 件数の確認
        self.assertEqual(len(records), 3)
        # データの確認
        self.assertEqual(records[0]['id'], 11)
        self.assertEqual(records[1]['id'], 12)
        self.assertEqual(records[2]['id'], 13)

    @database_test_util.setup_load_csv(
        'example1',
        'tests/data/test_example1/example1_test_select_example1_sort_by_datetime_col.csv')
    def test_select_example1_sort_by_datetime_col(self):
        records = example1.select_example1(self.conn, sort=example1.Sort.datetime_col)
        # 件数の確認
        self.assertEqual(len(records), 4)
        # データの確認
        self.assertEqual(records[0]['id'], 11)
        self.assertEqual(records[1]['id'], 13)
        self.assertEqual(records[2]['id'], 12)
        self.assertEqual(records[3]['id'], 14)

    @database_test_util.setup_load_csv(
        'example1',
        'tests/data/test_example1/example1_test_select_example1_sort_by_id.csv')
    def test_select_example1_order_by_asc(self):
        records = example1.select_example1(self.conn, order=example1.Order.asc)
        # 件数の確認
        self.assertEqual(len(records), 3)
        # データの確認
        self.assertEqual(records[0]['id'], 11)
        self.assertEqual(records[1]['id'], 12)
        self.assertEqual(records[2]['id'], 13)

    @database_test_util.setup_load_csv(
        'example1',
        'tests/data/test_example1/example1_test_select_example1_sort_by_datetime_col.csv')
    def test_select_example1_order_by_desc(self):
        records = example1.select_example1(self.conn,
                                           sort=example1.Sort.datetime_col,
                                           order=example1.Order.desc)
        # 件数の確認
        self.assertEqual(len(records), 4)
        # データの確認
        self.assertEqual(records[0]['id'], 14)
        self.assertEqual(records[1]['id'], 12)
        self.assertEqual(records[2]['id'], 13)
        self.assertEqual(records[3]['id'], 11)


class Test_insert_example1(unittest.TestCase):
    '''
    app.example1.insert_example1 のテスト
    '''

    @classmethod
    def setUpClass(cls):
        cls.conn = database_connection.get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        # データ初期化
        database_test_util.load_csv(
            self.conn,
            'example1',
            'tests/data/test_example1/example1_test_insert_example1.csv')
        self.conn.commit()

    def tearDown(self):
        pass

    def test_insert_example1_success(self):
        result = example1.insert_example1(
            self.conn,
            id=100,
            varchar_col='あいうえおかきくけこ',
            int_col=987654321,
            double_col=654.321,
            datetime_col=datetime(2001, 1, 2, 3, 4, 5))

        # 戻り値の確認
        self.assertEqual(result, 1)
        # 登録後の件数の確認（2 + 1 -> 3）
        self.assertEqual(count_example1(self.conn), 3)
        # 登録データの確認
        record = example1.select_example1_by_id(self.conn, 100)
        self.assertEqual(record['id'], 100)
        self.assertEqual(record['varchar_col'], 'あいうえおかきくけこ')
        self.assertEqual(record['int_col'], 987654321)
        self.assertEqual(record['double_col'], 654.321)
        self.assertEqual(record['datetime_col'], datetime(2001, 1, 2, 3, 4, 5))


class Test_delete_example1_by_id(unittest.TestCase):
    '''
    app.example1.delete_example1_by_id のテスト
    '''

    @classmethod
    def setUpClass(cls):
        cls.conn = database_connection.get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        # データ初期化
        database_test_util.load_csv(
            self.conn,
            'example1',
            'tests/data/test_example1/example1_test_delete_example1_by_id.csv')
        self.conn.commit()

    def tearDown(self):
        pass

    def test_delete_example1_by_id_success(self):
        result = example1.delete_example1_by_id(
            self.conn,
            id=2)

        # 戻り値の確認
        self.assertEqual(result, 1)
        # 削除後の件数の確認（3 - 1 -> 2）
        self.assertEqual(count_example1(self.conn), 2)
        # 削除したデータがないことを確認
        record = example1.select_example1_by_id(self.conn, 2)
        self.assertIsNone(record)

    def test_delete_example1_by_id_no_data(self):
        result = example1.delete_example1_by_id(
            self.conn,
            id=100)

        # 戻り値の確認
        self.assertEqual(result, 0)
        # 件数の確認（そのままなので 3）
        self.assertEqual(count_example1(self.conn), 3)


def main():
    unittest.main()


if __name__ == '__main__':
    main()
