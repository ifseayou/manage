#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2023/12/28 17:11:28
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，添加新的作者；
             2，根据作者名称爬取古诗文网站的作者生平等信息
             3，写入到 authors 表中
'''

import re
import requests
from bs4 import BeautifulSoup
from iaddu_util import IadduUtil


# 执行以下 sql 查找新增的作者 , 执行 insert 多条语句
'''
with tmp1 as (--
    select author
    from poems
    group by author
    )
select t1.author
from tmp1 t1
left join authors  t2
on t1.author = t2.name
where t2.name is null
;
'''

# 查找需要爬取的作者
'''
select name
from authors
where life is null 
or life = ''
'''

def get_new_author(conn):

    try:
        cursor = conn.cursor()
        select_query = """
            with tmp1 as (--
                select author
                from poems
                where author <> '佚名'
                group by author
                )
            select t1.author
            from tmp1 t1
            left join authors  t2
            on t1.author = t2.name
            where t2.name is null

            ;
        """
        cursor.execute(select_query)
        new_author_list = [row[0] for row in cursor.fetchall()]

    finally:
        cursor.close()
        # conn.close()
    return new_author_list

def insert_new_author(new_author_list,conn):
    try:
        cursor = conn.cursor()
        insert_query = "insert into authors (name,gender) values (%s)"
        data = [(author,1) for author in new_author_list]  # 注意：这里是一个元组的列表
        
        cursor.executemany(insert_query, data)
        conn.commit()

    finally:
        cursor.close()
        # conn.close()

def get_author_url(author_name):
    value = author_name
    valuej = author_name[0]
    url = f'https://so.gushiwen.cn/search.aspx?value={value}&valuej={valuej}'
    
    # 发起 GET 请求
    response = requests.get(url)
        
    if response.status_code == 200:
        # 使用 BeautifulSoup 解析网页内容，soup是整个网页
        soup = BeautifulSoup(response.text, 'html.parser')    
        # 找到第一个 <div class="contson">，并获取其中的所有内容，即当前的诗词的内容
        div_sonspic = soup.find('div', class_='sonspic')

        if div_sonspic == None:
            return "bad author"

        soup = BeautifulSoup(str(div_sonspic), 'html.parser')
        href_value = soup.find('a', {'target': '_blank'})['href']
        
    return f'https://so.gushiwen.cn/{href_value}' 

def get_author_life(author_name):
    author_url = get_author_url(author_name)
    
    # 如果没有当前作者
    if (author_url == 'bad author') :
        return author_url

    author_life = None
    response = requests.get(author_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')    
        div_contyishang_list =  soup.find_all(class_='contyishang')
        for div_contyishang in div_contyishang_list:
            h2_elements = div_contyishang.find_all('h2')
            for h2_ele in h2_elements:
                if '生平' in h2_ele.text or '人物生平' in h2_ele.text:
                        print(div_contyishang.prettify())  # 如果要打印整个元素，可以使用prettify()方法
    
    return author_life


# 将爬取到的内容，清洗之后，写入到 authors 表中


def get_author_by_ai(author_list):
    delimiter = "#"
    system_message = f"""
    你是一个中国古诗词专家，史学家 , 精通古代中国历史.\
    你需要根据输入的人物姓名，给出该人物的个人信息.\
    每个人物姓名按照{delimiter}分割\    
    你需要给出：人物姓名，昵称，性别，朝代，出生年份（只给数字），去世年份（只给数字），年龄（只给数字） ，家乡\
    
    
    严格按照以下示例产出:
    苏轼\t苏东坡\男\t北宋\t1037\t1101\t64\t四川眉山
    李清照\t易安居士\t女\t北宋\t1084\t1155\t71\t山东济南
    """

    batch_author = "#".join(author_list)       # 用"#"连接键

    user_message_for_model = f"""
        {batch_author}
    """
    messages =  [  
    {'role':'system', 'content': system_message},    
    {'role':'user', 'content': user_message_for_model},  
    ]


    author_info = IadduUtil().get_msg_by_ai(messages)

    # 将AI翻译的结果输出到日志中
    IadduUtil().log_res(author_info,'./conf/ai_author.txt')

    # 将文本按行分割，并去掉空行
    lines = [line.strip() for line in author_info.split('\n') if line.strip()]

    author_list = [(fields[0], fields[1], fields[2],fields[3],
                 fields[4], fields[5], fields[6],fields[7]
                ) for line in lines if len(fields := line.split('\t')) >= 8]
    
    return author_list

def save_to_db(author_list, conn):
    try:
        cursor = conn.cursor()
        
        insert_query = """
        insert into authors (name,nick_name,gender,dynasty,birth,death,age,hometown,type) 
        values (%s, %s, %s,%s, %s, %s,%s, %s,%s)
        """

        data = [(item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],1) 
                for item in author_list]
        cursor.executemany(insert_query, data)
        conn.commit()
    
    finally:
        cursor.close()
        

def main(option):
    try:
        conn_pool =  IadduUtil.get_mysql_conn_pool()
        if option == '2':
            new_author_list = get_new_author(conn_pool.connection())
        else:
            new_author_list = option.split(',') 
    
        author_list = get_author_by_ai(new_author_list)
        save_to_db(author_list,conn_pool.connection())
        
    finally:
        pass 
        # conn_pool.close()

if __name__ == '__main__':
    option = input("请输入作者名称(默认查询poem有author没有的作者)，按照英文逗号分割: ") or "2"
    main(option)
