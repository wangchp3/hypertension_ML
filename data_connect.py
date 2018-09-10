# encoding utf-8
# from test
import pyodbc
import pandas as pd

class Db:
    pool = None

    def __init__(self):
        """数据库构造函数，从连接池中取出连接，并生成操作游标"""
        self.conn = Db.get_conn()
        self.has_transaction = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __del__(self):
        self.close()

    @staticmethod
    def get_conn():
        try:
            if Db.pool is None:
                configs = r'DRIVER={SQL Server Native Client 11.0};SERVER=192.168.0.16;DATABASE=DB_HYPERTENSION_ZONG;UID=wcp;PWD=Suvalue2016'
                # configs = r'DRIVER={SQL Server Native Client 11.0};SERVER=192.168.0.16;DATABASE=Db_hypertension;UID=wcp;PWD=Suvalue2016'
                pool = pyodbc.connect(configs)
            return pool
        except Exception as ex:
            raise Exception('数据库连接异常')

   # 获取元组数据
    def get_tuple_list(self, sql, *args):
        """
        以元组作为结果集的获取数据方法
        :param sql: 需要查询的sql语句
        :param args: 语句参数，如果是dict，则sql中用@(name)s作为占位符，如果是元组，则用%s作为占位符
        :return:
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, *args)
            data = cursor.fetchall()
            return data
        except Exception:
            raise
        finally:
            cursor.close()

    # 获取元组数据
    def get_single_tuple(self, sql, *args):
        """
        以单个元组作为结果的获取数据方法
        :param sql: 需要查询的sql语句
        :param args: 语句参数，如果是dict，则sql中用@(name)s作为占位符，如果是元组，则用%s作为占位符
        :return:
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, *args)
            data = cursor.fetchone()
            return data
        except Exception:
            raise
        finally:
            cursor.close()

    # 获取键值对数据
    def get_dict_list(self, sql, *args):
        """
        以键值对作为结果集的获取数据方法
        :param sql: 需要查询的sql语句
        :param args: 语句参数，如果是dict，则sql中用@(name)s作为占位符，如果是元组，则用%s作为占位符
        :return:
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, *args)
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame.from_records(dict(zip(columns, datarow)) for datarow in data)
            print(df.shape,df.MASTER_INDEX.unique(),df.describe())
            print(df.info())
            return df
        except Exception:
            raise
        finally:
            cursor.close()

    # 操作数据
    def exec_sql(self, sql, *args):
        """
        单条语句，单条数据处理
        :param sql:
        :param args:可选参数
        :return:作用的数据条数
        """
        cursor = self.conn.cursor()
        try:
            rowscount = cursor.execute(sql, *args)
            if not self.has_transaction:
                self.conn.commit()
            return rowscount
        except Exception:
            raise
        finally:
            cursor.close()

    # 操作数据
    def exec_many(self, sql, *args):
        """
        单条语句，可以多条数据处理
        :param sql:sql语句
        :param args:可选参数，
        :return:作用的数据条数
        """
        cursor = self.conn.cursor()
        try:
            rowscount = cursor.executemany(sql, *args)
            if not self.has_transaction:
                self.commit()
            return rowscount
        except Exception:
            raise
        finally:
            cursor.close()

    # 开启事务
    def begin_transaction(self):
        self.conn.begin()
        self.has_transaction = True

    # 提交
    def commit(self):
        try:
            self.conn.commit()
            self.has_transaction = False
        except Exception:
            pass

    # 回滚
    def rollback(self):
        try:
            self.conn.rollback()
            self.has_transaction = False
        except Exception:
            pass

    # 关闭连接(将当前连接放回连接池)
    def close(self):
        if not self.conn:
            self.conn.close()
            self.conn = None


Db.get_conn()

