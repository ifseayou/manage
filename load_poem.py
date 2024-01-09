#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2023/12/25 16:11:39
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，查询诗词内容为空的诗词；
             2，根据诗词名称爬取古诗文网站的诗词内容
             3，写入到 poems 表中
'''
import re
import requests
from bs4 import BeautifulSoup
from iaddu_util import IadduUtil


def get_poem_list(conn):
    cursor = conn.cursor()
    poem_list = None
    try:
        # 执行查询
        select_query ="""
        select id, name 
        from dataease.poems 
        where content is null 
        or content = ''
        """
        
        # select_query = "SELECT id, name FROM dataease.poems" # 初始化所有诗词

        cursor.execute(select_query)
        poem_list = cursor.fetchall()

        # 提交更改
        conn.commit()

    except Exception as e:
        # 发生异常时回滚更改
        print(f"Error: {e}")
        # connection.rollback()

    finally:
        # 关闭游标和连接
        cursor.close()
        conn.close()
    return poem_list

def get_poem_content(poem_name):
    value = poem_name
    valuej = poem_name[0] 
    url = f'https://so.gushiwen.cn/search.aspx?value={value}&valuej={valuej}'
    
    # 发起 GET 请求
    response = requests.get(url)
    poem_content = None
        
    if response.status_code == 200:
        # 使用 BeautifulSoup 解析网页内容，soup是整个网页
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 找到第一个 <div class="contson">，并获取其中的所有内容，即当前的诗词的内容
        contson_div = soup.find('div', class_='contson')

        # 替换HTML中的<br/>元素为换行
        poem_content = re.sub(r'<br/>', '\n', str(contson_div))
        contson_div = BeautifulSoup(poem_content, 'html.parser')

        if contson_div:
            has_p_tag = contson_div.find('p') is not None            
            if has_p_tag:   
                # 提取所有 <p> 标签的文本，并在每个 <p> 之间添加换行符
                paragraphs = [p.get_text(strip=True) for p in contson_div.find_all('p')]
                poem_content = '\n\n'.join(paragraphs)
            else:
                poem_content = contson_div.text.strip()
        else:
            print('未找到符合条件的数据')   
    
    return poem_content


def update_poem(conn,poem_list):
    cursor = conn.cursor()
    try:
        for poem_id , poem_name in poem_list:
            poem_content = get_poem_content(poem_name)

            update_query = f"""
            update dataease.poems
            set content = '{poem_content}' 
            where id = {poem_id}
            """
            print(update_query,"\n")
            cursor.execute(update_query)
            conn.commit()

    except Exception as e:
        # 发生异常时回滚更改
        print(f"Error: {e}")
        # connection.rollback()

    finally:
        # 关闭游标和连接
        cursor.close()
        conn.close()


if __name__ == '__main__':
    util_instance = IadduUtil()
    conn1 = util_instance.get_mysql_conn()
    conn2 = util_instance.get_mysql_conn()

    # 获取没有内容的诗词名称列表
    poem_list =  get_poem_list(conn1)
    
    # 为没有诗词内容的诗词增加内容
    update_poem(conn2,poem_list)