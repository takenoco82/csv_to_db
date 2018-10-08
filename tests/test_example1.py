import unittest
from parameterized import parameterized, param
from datetime import datetime
from app import database_connection
from app import example1
from helper import csv_to_db
from collections import namedtuple


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
        csv_to_db.load(
            cls.conn,
            example1='tests/data/test_example1/example1_test_select_example1_by_id.csv')
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
        expected = {
            'id': 1,
            'varchar_col': 'abcdefghij',
            'int_col': 1234567890,
            'double_col': 123.456,
            'datetime_col': datetime(2018, 8, 1, 12, 34, 56)
        }
        self.assertDictEqual(record, expected)

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
        csv_to_db.load(
            self.conn,
            example1='tests/data/test_example1/example1_test_select_example1.csv')
        self.conn.commit()

    def tearDown(self):
        pass

    def test_select_example1_all(self):
        records = example1.select_example1(self.conn)
        expected_ids = [11, 12, 13, 14, 15, 16, 17, 18]
        # 件数の確認
        self.assertEqual(len(records), len(expected_ids))
        # データの確認
        for i, expected_id in enumerate(expected_ids):
            # subTestを使うとFAIL時に条件がわかる（unittestで実行していれば）
            with self.subTest(i=i):
                self.assertEqual(records[i]['id'], expected_id)

    # arg: start
    # arg: end
    # return: idのみのリスト
    Fixture1 = namedtuple('Fixture1', ('start', 'end', 'expected'))

    @parameterized.expand([
        # startのみ指定
        Fixture1(
            start=datetime(2018, 1, 1, 0, 0, 0),
            end=None,
            expected=[12, 13, 14, 15, 16, 17]),
        # endのみ指定
        Fixture1(
            start=None,
            end=datetime(2018, 1, 2, 0, 0, 0),
            expected=[11, 12, 13, 14, 15, 16]),
        # start, end両方指定
        Fixture1(
            start=datetime(2018, 1, 1, 0, 0, 0),
            end=datetime(2018, 1, 2, 0, 0, 0),
            expected=[12, 13, 14, 15, 16]),
        # start, endが同じ
        Fixture1(
            start=datetime(2018, 1, 1, 0, 0, 0),
            end=datetime(2018, 1, 1, 0, 0, 0),
            expected=[12]),
        # end < データの日付 -> 取得データなし
        Fixture1(
            start=datetime(2017, 12, 31, 0, 0, 0),
            end=datetime(2017, 12, 31, 23, 59, 58),
            expected=[]),
    ])
    def test_select_example1_parameterized_start_end(self, start, end, expected):
        records = example1.select_example1(self.conn, start=start, end=end)
        # 件数の確認
        self.assertEqual(
            len(records), len(expected),
            'failed with cond_start={},cond_end={}'.format(start, end))
        # データの確認
        actual = [record['id'] for record in records]
        self.assertListEqual(actual, expected)

    # param
    #   arg: start
    #   arg: end
    #   return: idのみのリスト
    @parameterized.expand([
        # startのみ指定
        param(
            start=datetime(2018, 1, 1, 0, 0, 0),
            end=None,
            expected=[12, 13, 14, 15, 16, 17]),
        # endのみ指定
        param(
            start=None,
            end=datetime(2018, 1, 2, 0, 0, 0),
            expected=[11, 12, 13, 14, 15, 16]),
        # start, end両方指定
        param(
            start=datetime(2018, 1, 1, 0, 0, 0),
            end=datetime(2018, 1, 2, 0, 0, 0),
            expected=[12, 13, 14, 15, 16]),
        # start, endが同じ
        param(
            start=datetime(2018, 1, 1, 0, 0, 0),
            end=datetime(2018, 1, 1, 0, 0, 0),
            expected=[12]),
        # end < データの日付 -> 取得データなし
        param(
            start=datetime(2017, 12, 31, 0, 0, 0),
            end=datetime(2017, 12, 31, 23, 59, 58),
            expected=[]),
    ])
    def test_select_example1_parameterized_start_end2(self, start, end, expected):
        records = example1.select_example1(self.conn, start=start, end=end)
        # 件数の確認
        self.assertEqual(
            len(records), len(expected),
            'failed with cond_start={},cond_end={}'.format(start, end))
        # データの確認
        actual = [record['id'] for record in records]
        self.assertListEqual(actual, expected)

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

    # test name
    # data: example1のデータ(csvファイル)
    # arg: sort
    # return: idのみのリスト
    Fixture2 = namedtuple('Fixture2', ('name', 'example1_file', 'sort', 'expected'))

    @parameterized.expand([
        # id（昇順）
        Fixture2(
            name="id",
            example1_file='tests/data/test_example1/example1_test_select_example1_sort_by_id.csv',
            sort=example1.Sort.id,
            expected=[11, 12, 13]),
        # datatime_col（昇順）
        Fixture2(
            name="datatime_col",
            example1_file='tests/data/test_example1/example1_test_select_example1_sort_by_datetime_col.csv',
            sort=example1.Sort.datetime_col,
            expected=[11, 13, 12, 14]),
    ])
    def test_select_example1_sort_parameterized(self, _, cond_file, cond_sort, expected_ids):
        # データ初期化
        csv_to_db.load(self.conn, example1=cond_file)
        self.conn.commit()

        records = example1.select_example1(self.conn, sort=cond_sort)
        # 件数の確認
        self.assertEqual(len(records), len(expected_ids))
        # データの確認
        for i, expected_id in enumerate(expected_ids):
            self.assertEqual(records[i]['id'], expected_id,
                             'failed with cond_file={},cond_sort={},i={}'.format(cond_file, cond_sort, i))

    def test_select_example1_sort_subtest(self):
        # test name
        # data: example1のデータ(csvファイル)
        # arg: sort
        # return: idのみのリスト
        Fixture2 = namedtuple('Fixture2', ('name', 'example1_file', 'sort', 'expected'))

        fixtures = [
            # id（昇順）
            Fixture2(
                name="id",
                example1_file='tests/data/test_example1/example1_test_select_example1_sort_by_id.csv',
                sort=example1.Sort.id,
                expected=[11, 12, 13]),
            # datatime_col（昇順）
            Fixture2(
                name="datatime_col",
                example1_file='tests/data/test_example1/example1_test_select_example1_sort_by_datetime_col.csv',
                sort=example1.Sort.datetime_col,
                expected=[11, 13, 12, 14]),
        ]

        for fixture in fixtures:
            with self.subTest(fixture=fixture):
                # データ初期化
                csv_to_db.load(self.conn, example1=fixture.example1_file)
                self.conn.commit()

                records = example1.select_example1(self.conn, sort=fixture.sort)
                # 件数の確認
                self.assertEqual(len(records), len(fixture.expected))
                # データの確認
                actual = [record['id'] for record in records]
                self.assertListEqual(actual, fixture.expected)

    @csv_to_db.setup_load(
        example1='tests/data/test_example1/example1_test_select_example1_sort_by_id.csv')
    def test_select_example1_sort_by_id(self):
        records = example1.select_example1(self.conn, sort=example1.Sort.id)
        # 件数の確認
        self.assertEqual(len(records), 3)
        # データの確認
        self.assertEqual(records[0]['id'], 11)
        self.assertEqual(records[1]['id'], 12)
        self.assertEqual(records[2]['id'], 13)

    @csv_to_db.setup_load(
        example1='tests/data/test_example1/example1_test_select_example1_sort_by_datetime_col.csv')
    def test_select_example1_sort_by_datetime_col(self):
        records = example1.select_example1(self.conn, sort=example1.Sort.datetime_col)
        # 件数の確認
        self.assertEqual(len(records), 4)
        # データの確認
        self.assertEqual(records[0]['id'], 11)
        self.assertEqual(records[1]['id'], 13)
        self.assertEqual(records[2]['id'], 12)
        self.assertEqual(records[3]['id'], 14)

    @csv_to_db.setup_load(
        example1='tests/data/test_example1/example1_test_select_example1_sort_by_id.csv')
    def test_select_example1_order_by_asc(self):
        records = example1.select_example1(self.conn, order=example1.Order.asc)
        # 件数の確認
        self.assertEqual(len(records), 3)
        # データの確認
        self.assertEqual(records[0]['id'], 11)
        self.assertEqual(records[1]['id'], 12)
        self.assertEqual(records[2]['id'], 13)

    @csv_to_db.setup_load(
        example1='tests/data/test_example1/example1_test_select_example1_sort_by_datetime_col.csv')
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
        csv_to_db.load(
            self.conn,
            example1='tests/data/test_example1/example1_test_insert_example1.csv')
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
        csv_to_db.load(
            self.conn,
            example1='tests/data/test_example1/example1_test_delete_example1_by_id.csv')
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
