import mysql.connector
import configparser

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

# 示例用法
if __name__ == "__main__":
    
    # 创建工具类实例
    my_utility = IadduUtil()
    print("hello")