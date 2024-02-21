#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2024/01/30 11:10:12
@Author  :   benchen
@Contact :   benchen@yowant.com
@Desc    :   None
'''

import re
import requests
from bs4 import BeautifulSoup
from iaddu_util import IadduUtil

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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


def get_full_html(url):
    poem_cmt = None

    # 设置 Chrome 浏览器参数
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')  # 设置为无头模式，即后台运行

    driver = webdriver.Chrome(options=chrome_options)    
    driver.get(url)# 打开网页 

    cookies = [
        {'name':'sShiwen2017','value': '%2c19155%2c57601%2c48248%2c49394%2c20270%2c70838%2c71074%2c71076%2c47040%2c49386%2c49480%2c49436%2c49475%2c47921%2c70938%2c69101%2c71144%2c70878%2c52821%2c76098%2c71250%2c76092%2c76107%2c49119%2c71138%2c57913%2c50243%2c5637%2c5636%2c5653%2c75779%2c57678%2c72937%2c64945%2c49425%2c7501%2c12271%2c69108%2c57989%2c2025%2c47938%2c52794%2c49481%2c49423%2c47909%2c48269%2c71175%2c48343%2c18709%2c48358%2c28607%2c76106%2c76102%2c71206%2c325420%2c48401%2c48305%2c48310%2c48323%2c69134%2c70963%2c56022%2c49455%2c73277%2c48845%2c71084%2c49409%2c48789%2c47919%2c72824%2c49693%2c72666%2c112125%2c52793%2c52802%2c52817%2c52831%2c52809%2c70957%2c6443%2c69127%2c119351%2c69133%2c69125%2c137624%2c21750%2c71052%2c22498%2c22098%2c22492%2c23706%2c22550%2c70971%2c70973%2c10521%2c7722%2c47916%2c11187%2c48005%2c48003%2c48729%2c','domain':'.gushiwen.cn'},
        {'name':'gswPhone','value': '13126505729','domain':	'.gushiwen.cn'},
        {'name':'wxopenid','value': 'defoaltid',	'domain':'.gushiwen.cn'},
        {'name':'gsw2017user','value': '4500777%7cA1715177ACE25284DEF1934852D89C6B%7c2000%2f1%2f1%7c2000%2f1%2f1','domain':	'.gushiwen.cn'},
        {'name':'login','value': 'flase','domain':	'so.gushiwen.cn'},
        {'name':'gswZhanghao','value': '13126505729', 'domain':	'.gushiwen.cn'},
        {'name':'login','value': 'flase','domain':	'.gushiwen.cn'},
        {'name':'codeyzgswso','value': 'a5238165aef5a49a','domain':	'so.gushiwen.cn'},
        {'name':'ticketStr','value': '203344080%7cgQEO8TwAAAAAAAAAAS5odHRwOi8vd2VpeGluLnFxLmNvbS9xLzAyWTZKYVJ4bGVkN2kxTVNkdzFCMWsAAgQ2gLhlAwQAjScA','domain':'.gushiwen.cn'},
        {'name': 'ASP.NET_SessionId','value':'wbntltex54okwanfcwtirf2g',	'domain':'so.gushiwen.cn'},
    ]

    for cookie in cookies:
        driver.add_cookie(cookie)

    try:
        # 等待 "阅读原文" 链接可点击
        read_more_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//a[text()="展开阅读全文 ∨"]'))
        )
        read_more_link.click()

        # 等待页面加载完成
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )
              
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        sons_div_list = soup.find_all('div', class_='sons') # 获取所有的 sons div

        i = 1
        for sons_div in sons_div_list:
            h2_tag = sons_div.find('h2')
            if h2_tag != None and h2_tag.text == "译文及注释": # h2的内容为 译文及注释
                if (i ==2):
                    poem_cmt = process_cnt(sons_div)
                i += 1

            h2_tag = sons_div.find('h2')
            if h2_tag != None and h2_tag.text == "创作背景": # h2的内容为 译文及注释
                bck = sons_div.find('p')
                if bck != None:
                    poem_bck = bck.text.strip()
                else :
                    bck = sons_div.find('div')
                    poem_bck = bck.text.strip()
                
        return poem_cmt,poem_bck
    finally:
        # 关闭浏览器
        driver.quit()
        pass 

def process_cnt(cmt):
    poem_content = re.sub(r'<br/>', '\n', str(cmt))
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
    return  re.sub(r'译文|参考资料：完善|▲|注释', '', poem_content)
 
def upd_poem(poem_list,conn):
    cursor = conn.cursor()
    try:
        for (poem,cmt,bck) in poem_list:
            update_query = f"""
            update dataease.poems
            set cmt = '{cmt}',
                bck = '{bck}'
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
        print(poem , " is coming .....")
        poem_url = get_poem_page(poem)
        poem_cmt , poem_bck = get_full_html(poem_url)
        if poem_bck != None or poem_cmt != None:
            poem_bck_list.append((poem,poem_cmt,poem_bck))
            
        # poem_cmt = get_poem_cmt(poem_url)

    upd_poem(poem_bck_list,conn_pool.connection())
    conn_pool.close()

def get_poem_list(conn):
    try:
        cursor = conn.cursor()

        query_sql = """
        select name
        from poems
        where cmt is null
        """
        cursor.execute(query_sql)
        poem_list = [row[0] for row in cursor.fetchall()]
        
    finally:
        cursor.close()
    return poem_list

if __name__ == '__main__':
    # poem_url = get_poem_page('游山西村')
    # poem_url = get_poem_page('琵琶行')
    # poem_url = get_poem_page('满江红')
    # poem_cmt,poem_bck = get_full_html(poem_url)
    # print(poem_cmt)
    # print(poem_bck)
    # print(poem_cmt)

    option = input("请输入诗词名称(默认查询全表的数据，默认 2)，按照英文逗号分割: ") or "2"
    main(option)

