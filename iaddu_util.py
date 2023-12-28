import pymysql
import configparser
import mysql.connector
from dbutils.pooled_db import PooledDB


class IadduUtil:
    def __init__(self):
        # 初始化方法，可以在这里进行一些初始化操作
        pass

        
    # 获取MySQL链接
    @staticmethod
    def get_mysql_conn(): 
        # 创建一个ConfigParser对象
        config = configparser.ConfigParser()
        # 读取INI文件
        config.read('./conf/db.ini')
        # 获取特定键的值
        word_host = config.get('word_conf', 'word_host')
        word_port = int(config.get('word_conf', 'word_port'))
        word_user = config.get('word_conf', 'word_user')
        word_pwd = config.get('word_conf', 'word_pwd')
        
        
        return mysql.connector.connect(host=word_host,
                                    port=word_port, 
                                    user=word_user,
                                    password = word_pwd,
                                    database='dataease'
                                    )

    # 获取MySQL连接池
    @staticmethod
    def get_mysql_conn_pool(max_conn):
        # 创建一个ConfigParser对象
        config = configparser.ConfigParser()
        # 读取INI文件
        config.read('./conf/db.ini')
        # 获取特定键的值
        word_host = config.get('word_conf', 'word_host')
        word_port = int(config.get('word_conf', 'word_port'))
        word_user = config.get('word_conf', 'word_user')
        word_pwd = config.get('word_conf', 'word_pwd')
        db_config = {
            "host": word_host,
            "port": word_port,
            "user": word_user,
            "password": word_pwd,
            "database": "dataease",
            "charset": "utf8mb4",
            "maxconnections": max_conn,  # 连接池允许的最大连接数
            "mincached": 5,       # 初始化时连接池中至少创建的空闲的连接，0表示不创建
            "maxcached": 5,      # 连接池中最多闲置的连接，0和None不限制
            "maxusage": 0,        # 每个连接最多被重复使用的次数，0或None表示无限制
            "blocking": True,     # 连接池中如果没有可用连接后，是否阻塞等待，True表示等待
        }
        conn_pool = PooledDB(pymysql, **db_config)
        return conn_pool

# 示例用法
if __name__ == "__main__":
    
    # 创建工具类实例
    my_utility = IadduUtil()
    conn_pool = my_utility.get_mysql_conn_pool(5)
    conn_pool.connection()
    print("hello")