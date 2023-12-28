#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2023/12/27 17:29:18
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，找出 将 word_freq 中有，words 中没有的单词，
             2，将这些单词写入到表 word 表中
'''

from iaddu_util import IadduUtil

def get_new_word(conn,add_num):

    sql = f"""
        with tmp1 as ( --
            select word_name                 as word_name
                , count(distinct book_name) as book_cnt
                , sum(word_cnt)             as word_cnt
            from word_freq
            group by word_name
        )
        , tmp2 as (--

            select t1.word_name
            from tmp1       t1
            left join words t2
                    on t1.word_name = t2.word
            where t2.word is null

            order by book_cnt desc, word_cnt desc
            limit {add_num}
        )
        select word_name
        from tmp2
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        new_word_list = [row[0] for row in cursor.fetchall()]
    
    finally:
        cursor.close()
        conn.close()
    return new_word_list


def insert_new_word(new_word_list,conn):

    try:
        cursor = conn.cursor()
        insert_query = "insert into words (word) values (%s)"
        data = [(word,) for word in new_word_list]  # 注意：这里是一个元组的列表
        # for i in data:
        #     print(i)
        cursor.executemany(insert_query, data)
        conn.commit()

    finally:
        cursor.close()
        conn.close()

def main():
    conn1 = IadduUtil().get_mysql_conn()
    conn2 = IadduUtil().get_mysql_conn()

    add_num = 1000
    new_word_list = get_new_word(conn1,add_num)

    insert_new_word(new_word_list,conn2)

if __name__ == '__main__':
    main()
    