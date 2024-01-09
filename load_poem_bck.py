#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2024/01/02 20:27:13
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，查询诗词 write_date 为空的诗词名称列表；
             2，遍历诗词名称列表，根据诗词名称爬取古诗文网站的诗词的创作背景
             3，将爬取内容，喂给 ChatGPT，要求 ChatGPT 产出诗词创建 年月日
             4，将ChatGPT返回的内容，校验完毕后，写入到数据库
'''
 
import re
import openai
import requests
from bs4 import BeautifulSoup
from iaddu_util import IadduUtil

def get_poem_list(conn):
    try:
        cursor = conn.cursor()

        query_sql = """
        select name
        from poems
        where bck is null
        """
        cursor.execute(query_sql)
        poem_list = [row[0] for row in cursor.fetchall()]
        
    finally:
        cursor.close()
    return poem_list

def get_poem_bck(url):
    poem_bck = None
    if url == 'bad poem':
        return poem_bck
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')  
        sons_div_list = soup.find_all('div', class_='sons') # 获取所有的 sons div 
        for sons_div in sons_div_list:
            h2_tag = sons_div.find('h2')
            if h2_tag != None and h2_tag.text == "创作背景": # h2的内容为创作背景
                bck = sons_div.find('p')
                if bck != None:
                    poem_bck = bck.text.strip()
                else :
                    bck = sons_div.find('div')
                    poem_bck = bck.text.strip()

    return poem_bck

def get_poem_page(poem):
    value = poem
    valuej = poem[0]
    url = f'https://so.gushiwen.cn/search.aspx?value={value}&valuej={valuej}'
    
    # 发起 GET 请求
    response = requests.get(url)    
    if response.status_code == 200:
        # 使用 BeautifulSoup 解析网页内容，soup是整个网页
        soup = BeautifulSoup(response.text, 'html.parser')    
        contson_div = soup.find('div', class_='sons')

        if contson_div == None:
            return "bad poem"

        soup = BeautifulSoup(str(contson_div), 'html.parser')
        href_value = soup.find('a', {'target': '_blank'})['href']
        
    return f'https://so.gushiwen.cn/{href_value}'

def get_date_by_ai(poem,poem_bck):
    delimiter = "#"
    system_message = f"""
    You are a poem expert , master in poem and China history\
    you need output the {poem} 's write year and\
    the {poem} 's write backgroud  base on input text\

    Output example:
    示儿\t这首诗是陆游在公元1210年创作的，也是他的绝笔之作
    宿建德江\t这首诗是孟浩然在公元730年创作的，也是他在漫游吴越时写下的作品。孟浩然当时离开家乡赶赴洛阳，再漫游吴越，借以排遣仕途失意的郁闷
    """
    
    user_message_for_model = f"""
        {poem_bck}
    """

    messages =  [  
    {'role':'system', 'content': system_message},    
    {'role':'user', 'content': user_message_for_model},  
    ]

    poem_bck = IadduUtil().get_msg_by_ai(messages)
    # 将AI翻译的结果输出到日志中
    IadduUtil().log_ai_res(poem_bck,'./conf/ai_poem.txt')

    return poem_bck

def upd_poem_bck(poem_bck_list,conn):
    cursor = conn.cursor()
    try:
        for (poem,bck) in poem_bck_list:
            update_query = f"""
            update dataease.poems
            set bck = '{bck}'
            where name = '{poem}'
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

def main(option):
    conn_pool = IadduUtil.get_mysql_conn_pool(3)
    
    if option == "2":
        poem_list = get_poem_list(conn_pool.connection())
    else :
        poem_list = option.split(',')
    
    poem_bck_list = []

    for poem in poem_list:

        poem_url = get_poem_page(poem)
        poem_bck = get_poem_bck(poem_url)
        if poem_bck != None:

            # OpenAi接口不太稳定，直接使用爬虫数据
            # poem_date_bck = get_date_by_ai(poem,poem_bck)
            # poem_date_bck = poem_date_bck.split('\t')
            
            poem_bck_list.append((poem,poem_bck))
            
        # poem_cmt = get_poem_cmt(poem_url)

    upd_poem_bck(poem_bck_list,conn_pool.connection())
    conn_pool.close()

if __name__ == '__main__':
    option = input("请输入诗词名称(默认查询全表的数据)，按照英文逗号分割: ") or "2"
    main(option)