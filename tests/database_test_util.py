from app import database_connection
from functools import wraps
import pandas


def setup_load_csv(table, filepath, truncate=True):
    def _setup_load_csv(func):
        # 関数名がデコレータで上書きされてしまうのを防ぐ
        @wraps(func)
        def wrapper(*args, **kwargs):
            conn = database_connection.get_connection()
            load_csv(conn, table, filepath, truncate=truncate)
            # 処理
            return func(*args, **kwargs)
        return wrapper
    return _setup_load_csv


def load_csv(conn, table, filepath, truncate=True):
    '''
    指定されたcsvファイルの内容をテーブルに登録します.

    csvファイルについて
        文字コードはUTF-8で作成してください
        改行コードはLFにしてください
        1行目はヘッダとし、テーブルの項目名と一致させてください

    特殊な値の登録について
        空文字列を登録するには「-」にします
        NULLを登録するには値なしにします
        例）
            id,name,created_at
            1,hoge,2018-01-01 12:34:56 ← 日付はyyyy-MM-dd hh:mm:ss形式。時間部分は省略可能
            2,-,2018-01-02             ← nameに空文字列で登録される
            3,,                        ← name, created_atにNULLで登録される

    外部キー制約について
        一時的に無効にし、最後に有効にしています

    Args:
        conn: DBコネクション
        table: 初期化するテーブル名
        filepath: csvファイルのパス
        truncate: もとのデータを削除してから登録するかどうか。Trueの場合、もとのデータを削除する。デフォルト: True

    Returns:
        なし
    '''
    dataframe = pandas.read_csv(filepath)
    # 「-」は空文字列に置き換える
    dataframe = dataframe.replace('-', '')
    # 欠損値（NaN）はNoneに置き換える
    dataframe = dataframe.where((pandas.notnull(dataframe)), None)

    try:
        _set_foreign_key_checks_disabled(conn)

        if truncate:
            _truncate(conn, table)

        header = dataframe.columns.values
        for row in dataframe.itertuples(index=False, name=None):
            _insert(conn, table, header, row)

    finally:
        _set_foreign_key_checks_enabled(conn)

    conn.commit()


def _set_foreign_key_checks_disabled(conn):
    sql = "SET FOREIGN_KEY_CHECKS = 0"
    with conn.cursor() as cur:
        cur.execute(sql)


def _set_foreign_key_checks_enabled(conn):
    sql = "SET FOREIGN_KEY_CHECKS = 1"
    with conn.cursor() as cur:
        cur.execute(sql)


def _truncate(conn, table):
    sql = "TRUNCATE TABLE {table}".format(table=table)
    with conn.cursor() as cur:
        cur.execute(sql)


def _insert(conn, table, columns, values):
    # TODO: csvファイル内では同じものなので、作成するのは1回だけにしたい
    sql = _create_insert_sql(table, columns)
    with conn.cursor() as cur:
        cur.execute(sql, values)


def _create_insert_sql(table, columns):
    '''
    戻り値のイメージ:
        INSERT INTO example (id, name, created_at) VALUES (%s, %s, %s)
    '''

    sql = "INSERT INTO {table} ({columns}) VALUES ({values})"
    sql = sql.format(
        table=table,
        columns=', '.join(columns),
        values=', '.join(('%s' for item in columns)))
    return sql


def main():
    conn = database_connection.get_connection()
    load_csv(conn,
             'example1',
             'tests/data/example1_test_load_csv_error.csv')


if __name__ == '__main__':
    main()
