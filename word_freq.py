#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2023/12/25 22:18:43
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，从电子书中统计单词出现的频次，
             2，写入到表 word_freq 中
'''
import re
import os
from sqlite3 import paramstyle
import time
import epubs
import warnings
from iaddu_util import IadduUtil
from concurrent.futures import ThreadPoolExecutor

def get_db_books(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("select book_name from word_freq group by book_name")
        db_books = [row[0] for row in cursor.fetchall()]
    
    finally:
        cursor.close()
        conn.close()
    return db_books

def get_input_books(book_path):
    all_books = [filename for filename in os.listdir(book_path) if not filename.startswith('.')]
    return all_books

def scan_epub_book(book):
    illegal_words = ("b","c","e","de","k","g","n","v","s","x","ii","iv","vi","rl",'o','h','w','l','f',
                     't','r','m','j','d','p','u','y',"od", "op" ,"uc","xx","xv","sj","ho" ,"mc","nt",
                     "ee","el","sl","fi",'bc','bb','rb','gc','wu','ny','xl','rd','ha','dj','jc','uh',
                     'ds','bo','le','ng','pl','pa','ve','gs','yu','ji','il','da','jr','xix','xxi','xvi',
                     'xii','vii','iii','ixx','vy','nb','um','ig','ib','ry','ab','wh','ib','ig','vy','nb',
                     'um','ig','ib','ry','ab','wh','bu','ya','pf','au','es','jk','ex','oo','oi','hi','al',
                     'ow','ze','re','eh','st','em','ah','xi','fk','ps','ay','vp','co','dr','fs','tu','ia',
                     'pt','th','xi','bf','lc','yo','pp','ed','ix',
                     )
    book_path = f'./conf/book/{book}.epub'
    word_freq_dic = {}
    
    warnings.filterwarnings("ignore")
    text = epubs.to_text(book_path) 
    for paragraph in list(text):
        for sentence in paragraph:
            sentence = re.sub(r'[^A-Za-z -]','',sentence)
            for word in re.split(r'[ |-]',sentence):
                word = word.lower()
                if word not in illegal_words :
                    word_freq_dic[word] = word_freq_dic.get(word, 0) + 1

    return word_freq_dic

def scan_pdf_book(book):
    # todo 
    pass

def save_to_db(book_name, word_freq_dic, conn):
    try:
        cursor = conn.cursor()
        print("开始保存<---", book_name, "--->词频")
        insert_query = "insert into word_freq (book_name, word_name, word_cnt) values (%s, %s, %s)"
        data = [(book_name, word, count) for word, count in word_freq_dic.items()]
        cursor.executemany(insert_query, data)
        conn.commit()
    
    finally:
        
        cursor.close()
        conn.close()

def process_book(book,conn):
    word_freq_dic = scan_epub_book(book)
    save_to_db(book, word_freq_dic, conn)

def main():
    util_instance = IadduUtil()
    db_conn = util_instance.get_mysql_conn()
    global db_books
    db_books = get_db_books(db_conn)

    input_books = get_input_books('./conf/book')
    input_books =  [b_name.split('.')[0] for b_name in input_books]
    scan_books =  set(input_books) - set(db_books)
    
    parallelism = len(scan_books)
    
    print("\n 共计",parallelism,"本书籍\n")
    # for i in scan_books:
    #     print(i)


    # 使用线程池并发处理
    if parallelism < 1:
        return
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = [executor.submit(process_book, book,util_instance.get_mysql_conn()) for book in scan_books]


if __name__ == '__main__':
    start_time = time.time()
    # 词频统计逻辑
    main()
    end_time = time.time()
    elapsed_time = round((end_time - start_time)/60.0,2)

    print(f'\n\n词频更新已完成....\n消耗时长：{elapsed_time} 分钟')
