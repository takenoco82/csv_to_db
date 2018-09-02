-- データベースの作成
DROP DATABASE IF EXISTS sandbox;
create database sandbox;

-- 使用するデータベースを選択
use sandbox;

-- テーブル作成 example1
DROP TABLE IF EXISTS example;
create table example1 (
  id int(10) not null auto_increment,
  varchar_col varchar(10),
  int_col int(10),
  double_col double(6,3),
  datetime_col datetime,
  primary key (id)
);

-- データ登録 example1
insert into example1 values(1, 'abcdefghij', 1234567890, 123.456, '2018-08-01 12:34:56');
insert into example1 values(2, NULL, NULL, NULL, NULL);


-- テーブル作成 example2
DROP TABLE IF EXISTS example2;
create table example2 (
  id int(10) not null auto_increment,
  col1 varchar(10),
  primary key (id)
);

-- データ登録 example2
insert into example2 values(1, 'abcdefghij');
insert into example2 values(2, NULL);
