#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2024/01/09 13:45:16
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，爬虫，爬取 https://quotefancy.com/ 网站
             2，爬取 句子和对应的作者
             3，将爬取到的 句子和作者 写入到 sentences 表
'''
import re
import math
import requests
from bs4 import BeautifulSoup
from iaddu_util import IadduUtil
from concurrent.futures import ThreadPoolExecutor


def clean_quote_content(quote):
    # 使用正则表达式匹配并去除开头和结尾的内容
    return  re.sub(r'^\d+\.\s*“|”$', '', quote)

def clean_quote_author(quote_author):
    return re.sub(r'^—\s*', '', quote_author)

def load_into_sentence(quote_obj_list,conn):
    try:
        cursor = conn.cursor()
        insert_query = """
        insert into sentences (content,author,label,lan)
        values(%s,%s,%s,%s)
        """
        cursor.executemany(insert_query,quote_obj_list)
        conn.commit()
    finally:
        cursor.close()


def get_all_links(url):
    response = requests.get(url)
    links = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')    
        grid_div = soup.find('div', class_='post-grid post-min')    
        gridblock_titles = grid_div.find_all(class_='gridblock-title')
        links = [title.a['href'] for title in gridblock_titles]
    return links

def crawl_page(link):
    page_num =  get_page_num(link)
    quote_obj_list = []
    quote_tag = link.split('/')[-1].replace('-', ' ')

    for i in range(page_num):
        page_link = link + f"/page/{i + 1}"
        print(page_link)
        try:
            response = requests.get(page_link)
            # response = requests.get(link)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                quote_div_list = soup.find_all(class_ = 'q-container')
                # IadduUtil().log_res(quote_div_list,'./conf/quotefancy.html')
                for quote_div in quote_div_list:
                    quote_content = clean_quote_content(quote_div.find('p',class_ = 'quote-p').text.strip())
                    quote_author = clean_quote_author(quote_div.find('p',class_ = 'author-p').text.strip())
                    quote_obj_list.append((quote_content,quote_author,quote_tag,2))
                
        except Exception as e:
            print(f"Error crawling {link}: {str(e)}")

    return quote_obj_list

def get_page_num(url):
    response = requests.get(url)
    page_num = 0 
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')    
        head_div = soup.find('div', class_='header-content')
        page_num = math.ceil((int(head_div.text.strip().split(' ')[1]) + 1) / 50)
    return page_num

def exe_map(link,conn):

    quote_obj_list = crawl_page(link)
    load_into_sentence(quote_obj_list,conn)


def main():
    conn_pool = IadduUtil.get_mysql_conn_pool()

    url = 'https://quotefancy.com/'    
    links = get_all_links(url)

    for link in links:
        quote_obj_list = crawl_page(link)
        load_into_sentence(quote_obj_list,conn_pool.connection())
        print("insert into ----> ", link)

 
if __name__ == '__main__':
    pass
    # main()
    