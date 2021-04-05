# MySQL
## 启动
[root@bigdata-pro01 ~]# systemctl start docker
## 数据准备
```
mysql> create database storm;
Query OK, 1 row affected (0.00 sec)

mysql> use storm;
Database changed
mysql> create table if not exists user (user_id integer, user_name varchar(100), dept_name varchar(100), create_date date);
Query OK, 0 rows affected (0.04 sec)

mysql> create table if not exists department (dept_id integer, dept_name varchar(100));
Query OK, 0 rows affected (0.05 sec)

mysql> create table if not exists user_department (user_id integer, dept_id integer);
Query OK, 0 rows affected (0.05 sec)

mysql> insert into department values (1, 'R&D');
Query OK, 1 row affected (0.02 sec)

mysql> insert into department values (2, 'Finance');
Query OK, 1 row affected (0.01 sec)

mysql> insert into department values (3, 'HR');
Query OK, 1 row affected (0.02 sec)

mysql> insert into department values (4, 'Sales');
Query OK, 1 row affected (0.01 sec)

mysql> insert into user_department values (1, 1);
Query OK, 1 row affected (0.00 sec)

mysql> insert into user_department values (2, 2);
Query OK, 1 row affected (0.00 sec)

mysql> insert into user_department values (3, 3);
Query OK, 1 row affected (0.00 sec)

mysql> insert into user_department values (4, 4);
Query OK, 1 row affected (0.00 sec)

mysql> select dept_name from department, user_department where department.dept_id = user_department.dept_id and user_department.user_id = 1;
+-----------+
| dept_name |
+-----------+
| R&D       |
+-----------+
1 row in set (0.00 sec)

```
## 数据准备二
```
create table stormMysql_test(order_date varchar(10) NOT NULL, order_amt varchar(10) NOT NULL);
```
## Maven集成MySQL
```
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>storm-jdbc</artifactId>
    <version>1.2.1</version>
</dependency>

<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.31</version>
</dependency>
```
## 运行
### 数据生产
KafkaProducer
### 计算，存储到MySQL
