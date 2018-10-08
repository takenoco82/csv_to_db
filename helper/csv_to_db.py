import csv
import os
import re
from functools import wraps

from app import database_connection


def init_db(conn, csv_dir):
    targets = {}

    items = os.listdir(csv_dir)
    for item in items:
        filepath = os.path.join(csv_dir, item)
        if not os.path.isfile(filepath):
            continue

        extention = os.path.splitext(item)[1]
        if not re.match('\\.(csv|tsv)', extention, re.IGNORECASE):
            continue

        table = os.path.splitext(item)[0]
        targets[table] = filepath

    load(conn, **targets)


def setup_load(truncate: bool=True, **targets):
    """load()を実行するデコレータです.
    ※load()とは違いコミットします

    使用時の注意事項
        テストメソッドのトランザクションとデコレータのトランザクションが異なるため、
        デコレータ実行時にテストメソッドのトランザクションが開始されていると、
        metadata lockが発生し、DB更新処理で止まってしまい、
        終了しなくなる可能性があります。

        metadata lockの最も簡単な回避策は、
        setUp()でcommit()またはrollback()を行うことです。
    """
    def _setup_load(func):
        # 関数名がデコレータで上書きされてしまうのを防ぐ
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                conn = database_connection.get_connection()
                conn.begin()
                load(conn, truncate=truncate, **targets)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()
            return func(*args, **kwargs)
        return wrapper
    return _setup_load


def load(conn, truncate: bool=True, **targets):
    """
    指定されたcsvファイル（tsvファイル）の内容をテーブルに登録します.
    ※コミットはしません

    csvファイル（tsvファイル）について
        文字コードはUTF-8で作成してください
        改行コードはLFにしてください
        1行目はヘッダとし、テーブルの項目名と一致させてください
        拡張子はcsvまたはtsvにしてください
        区切り文字は以下の通りにしてください。
            csvファイルの場合、カンマ（2C）
            tsvファイルの場合、水平タブ（09）

    特殊な値の登録について
        空文字列を登録するには「-」にします
        NULLを登録するには値なしにします
        例）
            id,name,created_at
            1,hoge,2018-01-01 12:34:56 ← 日付はyyyy-MM-dd hh:mm:ss形式。時間部分は省略可能
            2,-,2018-01-02             ← nameに空文字列で登録される
            3,,                        ← name, created_atにNULLで登録される
            4,"hoge,fuga",             ← nameに「hoge,fuga」で登録される

    外部キー制約について
        一時的に無効にし、最後に有効にしています

    Args:
        conn:
            DBコネクション
        truncate (bool, optional):
            Defaults to True.
            登録前に削除を行うかどうか。Trueの場合、削除を行う。
        targets:
            テーブル名をキーワードとしてファイルパスを指定してください。複数指定可能
            例）
            load(conn, USER='path/to/USER.csv', COMPANY='path/to/COMPANY.csv')
    """
    try:
        _set_foreign_key_checks_disabled(conn)

        for table, filepath in targets.items():
            if not os.path.isfile(filepath):
                raise FileNotFoundError(filepath)

            extention = os.path.splitext(filepath)[1]
            if re.match('\\.csv', extention, re.IGNORECASE):
                delimiter = ','
            elif re.match('\\.tsv', extention, re.IGNORECASE):
                delimiter = '\t'
            else:
                raise ValueError("Unsupported extension: {}".format(filepath))

            _load(conn, truncate=truncate, table=table, filepath=filepath, delimiter=delimiter)

    finally:
        _set_foreign_key_checks_enabled(conn)


def _load(conn, truncate, table, filepath, delimiter=','):
    if truncate:
        _truncate(conn, table)

    with open(filepath, mode='r', encoding='utf_8') as f:
        csv_reader = csv.reader(f, delimiter=delimiter, quotechar='"')
        header = next(csv_reader)

        for row in csv_reader:
            _insert(conn, table, header, row)


def _set_foreign_key_checks_disabled(conn):
    sql = "SET FOREIGN_KEY_CHECKS = 0"
    with conn.cursor() as cur:
        cur.execute(sql)


def _set_foreign_key_checks_enabled(conn):
    sql = "SET FOREIGN_KEY_CHECKS = 1"
    with conn.cursor() as cur:
        cur.execute(sql)


def _truncate(conn, table):
    sql = "TRUNCATE TABLE {}".format(table)
    with conn.cursor() as cur:
        cur.execute(sql)


def _insert(conn, table, columns, values):
    # TODO: csvファイル内では同じものなので、作成するのは1回だけにしたい
    sql = _create_insert_sql(table, columns)
    print("LOAD DATA: {}, {}".format(table, values))
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
    conn.begin()
    init_db(conn, csv_dir="tests/data")
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
