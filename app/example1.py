from app import database_connection
from enum import Enum


class Sort(Enum):
    id = 1,
    datetime_col = 2


class Order(Enum):
    asc = 1,
    desc = 2


def select_example1_by_id(conn, id):
    sql = "select * from example1 where id = %s"
    params = (id, )
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def select_example1(conn, start=None, end=None, sort=Sort.id, order=Order.asc):
    sql = "select * from example1 where 1 = 1"
    params = {}
    if start:
        sql += " and datetime_col >= %(start)s"
        params["start"] = start
    if end:
        sql += " and datetime_col <= %(end)s"
        params["end"] = end
    sql += " order by {} {}".format(sort.name, order.name)
    print(sql)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def insert_example1(conn, id, varchar_col, int_col, double_col, datetime_col):
    sql = '''
    INSERT INTO example1(
        id,
        varchar_col,
        int_col,
        double_col,
        datetime_col
    )
    VALUES(%s, %s, %s, %s, %s)
    '''
    with conn.cursor() as cur:
        return cur.execute(
            sql, (id, varchar_col, int_col, double_col, datetime_col))


def delete_example1_by_id(conn, id):
    sql = 'delete from example1 where id = %s'
    with conn.cursor() as cur:
        return cur.execute(sql, (id, ))


def main():
    try:
        conn = database_connection.get_connection()
        result = select_example1_by_id(conn, 11)
        print(result)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
