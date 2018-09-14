from app import database_connection
from functools import wraps
import csv


def init(file_dict: dict, truncate: bool=True):
    """load()を実行するデコレータです.
    ※load()とは違いコミットします

    使用時の注意事項
        テストメソッドのトランザクションとデコレータのトランザクションが異なるため、
        デコレータ実行時にテストメソッドのトランザクションが開始されていると、
        metadata lockが発生し、DB更新処理で止まってしまい、
        終了しなくなる可能性があります。

        metadata lockの最も簡単な回避策は、
        setUp()でcommit()またはrollback()を行うことです。

    Args:
        file_dict (dict):
            key=テーブル名, value=ファイルパス
        truncate (bool, optional):
            Defaults to True.
            登録前に削除を行うかどうか。Trueの場合、削除を行う。
    """
    def _init(func):
        # 関数名がデコレータで上書きされてしまうのを防ぐ
        @wraps(func)
        def wrapper(*args, **kwargs):
            conn = database_connection.get_connection()
            for table, filepath in file_dict.items():
                load(conn, table, filepath, truncate=truncate)
            conn.commit()
            conn.close()
            return func(*args, **kwargs)
        return wrapper
    return _init


def load(conn, table: str, filepath: str, truncate: bool=True):
    '''
    指定されたcsvファイルの内容をテーブルに登録します.
    ※コミットはしません

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
        conn:
            DBコネクション
        table (str):
            初期化するテーブル名
        filepath (str):
            csvファイルのパス
        truncate (bool, optional):
            Defaults to True.
            登録前に削除を行うかどうか。Trueの場合、削除を行う。

    Returns:
        なし
    '''
    try:
        _set_foreign_key_checks_disabled(conn)

        conn.begin()
        if truncate:
            _delete(conn, table)

        with open(filepath, mode='r', encoding='utf_8') as f:
            csv_reader = csv.reader(f, delimiter=',', quotechar='"')
            header = next(csv_reader)
            # print('header={}'.format(header))

            for row in csv_reader:
                _insert(conn, table, header, row)

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        _set_foreign_key_checks_enabled(conn)


def _set_foreign_key_checks_disabled(conn):
    sql = "SET FOREIGN_KEY_CHECKS = 0"
    with conn.cursor() as cur:
        cur.execute(sql)


def _set_foreign_key_checks_enabled(conn):
    sql = "SET FOREIGN_KEY_CHECKS = 1"
    with conn.cursor() as cur:
        cur.execute(sql)


def _delete(conn, table):
    sql = "DELETE FROM {table}".format(table=table)
    with conn.cursor() as cur:
        cur.execute(sql)


def _insert(conn, table, columns, values):
    # TODO: csvファイル内では同じものなので、作成するのは1回だけにしたい
    sql = _create_insert_sql(table, columns)
    with conn.cursor() as cur:
        cur.execute(sql, [_convert_value(value) for value in values])


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


def _convert_value(value):
    '''DBに登録する値に変換する

    '': None,
    '-': '',
    上記以外: そのまま
    '''

    if value == '':
        return None
    if value == '-':
        return ''
    return value


def main():
    conn = database_connection.get_connection()
    load(conn,
         'example1',
         #   'tests/data/test_example1/example1_test_select_example1_sort_by_datetime_col.csv')
         'tests/data/example1_test_load_csv.csv')
    conn.commit()


if __name__ == '__main__':
    main()
