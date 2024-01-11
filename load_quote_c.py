#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2024/01/10 15:50:48
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，爬虫，爬取 https://www.juzang.com/mingju/ 网站
             2，爬取 句子和对应的作者以及出处
             3，将爬取到的 句子和作者 写入到 sentences 表
'''

from operator import le
import re
import math
import requests
from bs4 import BeautifulSoup
from iaddu_util import IadduUtil

def clean_quote_author(quote_author):
    return re.sub(r'^—\s*', '', quote_author)

def get_author_link(url):

    author_obj_list = []
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')    
        author_divs = soup.find_all('div', class_='dicType2')
        for author_div in author_divs:
            author_img = author_div.find('div',class_= 'apic').find('img')['src']
            author_name = author_div.find('div',class_= 'aword').find('center').text.strip()
            
            author_obj_list.append(author_name)
    return author_obj_list

# 处理单个 page
def craw_single_page(page_url,option,author_name):
    print(page_url)

    quote_list = []
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    if (soup.find('div',class_ = 'main3').find('div',class_ = 'left') == None):
        return

    quote_divs = soup.find('div',class_ = 'main3').find('div',class_ = 'left').find_all('div', class_='sons')

    quote_content = ''
    quote_author = ''
    quote_frm = ''

    for quote_div in quote_divs:

        if quote_div.find('div',class_ = 'cont') != None:
            quote_content =  quote_div.find('div',class_ = 'cont').text.strip('\n')
            if('的句子' in quote_content or quote_content == ''):
                continue
        if quote_div.find('div',class_ = 'source')!= None:
            quote_author  =  clean_quote_author(quote_div.find('div',class_ = 'source').text.strip('\n'))
        if quote_div.find('a',class_ = 'laiyuan') != None:
            quote_frm = quote_div.find('a',class_ = 'laiyuan').text

        
        if ('', '', '') != (quote_content,quote_author,quote_frm):
            if(option == '1'):
                quote_list.append((quote_content,quote_author,quote_frm,None))
            else :
                quote_list.append((quote_content,quote_author,quote_frm,author_name))

    return quote_list
    


# 处理某个作者的所有 分页数据
def crawl_all_page(author_name,option):
    page = 1
    quote_all_list = []
    flag = True
    while flag:
        if (option == '1'):
            author_url = f'https://www.juzang.com/mingju/default.aspx?astr={author_name}&sstr=&page={page}'
        else:
            author_url = f'https://www.juzang.com/mingju/default.aspx?tstr={author_name}&sstr=&page={page}'

        quote_list = craw_single_page(author_url,option,author_name)
        
        if quote_list == None:
            return set(quote_all_list)

        if(bool(set(quote_list).intersection(set(quote_all_list)))): # 有交集
            quote_all_list += quote_list    
            flag = False    
            continue
        quote_all_list += quote_list
        page += 1                    

    return set(quote_all_list)

def load_into_sentence(quote_obj_list,conn):
    try:
        cursor = conn.cursor()
        insert_query = """
        insert into sentences (content,author,frm,label)
        values(%s,%s,%s,%s)
        """
        cursor.executemany(insert_query,quote_obj_list)
        conn.commit()
    finally:
        cursor.close()

def get_label_link(url):
    label_obj_list = []
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')    
        label_divs = soup.find_all('div', class_='dicType')
        for label_div in label_divs:
            label_name = label_div.find('div',class_= 'aword').find('center').text.strip()
            label_obj_list.append(label_name)
    
    return label_obj_list



def main(option):
    conn_pool = IadduUtil.get_mysql_conn_pool()

    url = 'https://www.juzang.com/mingju/'

    if option == '1':
        author_list = get_author_link(url)
    else :
        author_list = get_label_link(url)

    for author_name in author_list:
        quote_obj_list = crawl_all_page(author_name,option)
        load_into_sentence(quote_obj_list,conn_pool.connection())
        print("insert into ----> ", author_name)


if __name__ == '__main__':
    option = input("请输入类型，1：按照作者，2：按照标签: ")
    if(option in ('1','2')):
        main(option)