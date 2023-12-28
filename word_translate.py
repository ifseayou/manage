#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2023/12/26 14:57:06
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，根据 words 表中 pronunciation 为空或中文为空的单词，
             2，请求 OpenAI的接口，获取中文，或者发音
             3，更新 words 表的中文或者发音
'''
import time
import openai
from iaddu_util import IadduUtil
from concurrent.futures import ThreadPoolExecutor

def execute_select_sql(sql):
    conn = IadduUtil().get_mysql_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        word_list = [row[0] for row in cursor.fetchall()]
    
    finally:
        cursor.close()
        conn.close()
    return word_list
 
def get_no_chinese_words():
    sql = "select word from words where chinese is null or pronunciation = '';"
    return execute_select_sql(sql)

def get_no_pronunciation_words():
    sql = "select word from words where pronunciation is null or pronunciation = '' limit 500;"
    return execute_select_sql(sql)

def get_no_pc_words():
    sql = """
    select word 
    from words 
    where (pronunciation is null or pronunciation = '')
    and chinese is null or pronunciation = ''
    limit 1000
    ;
    """
    return execute_select_sql(sql)

def get_completion_from_messages(messages, 
                                 model="gpt-3.5-turbo", 
                                 temperature=0, 
                                 max_tokens=2000):
    response = openai.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return response.choices[0].message.content


def log_ai_trans(ai_trans_result):
    with open('./conf/ai_trans.txt', 'a') as file:  # 追加写入
        if ai_trans_result != None:
            file.write(f"{ai_trans_result}" + "\n") 



def ai_translator(word_list):
    delimiter = "#"
    system_message = f"""
    You are a translator , skilled in English and Chinese.\
    You need translate the English words into Chinese.\
    Every word will be delimited with {delimiter}\
    You need give the Chinese and the American pronunciation(美式音标)
    
    Output example:
    apple\t苹果\t/ˈæpəl/\nsmall\t小的\t/smɔːl/
    """

    batch_size = 50  # 指定每个分组的大小
    log_cnt = 0

    word_list_c_p = []

    print("开始请求OpenAI接口，相对耗时.....\n")

    for i in range(0, len(word_list), batch_size):
        batch = word_list[i : i + batch_size]  # 从列表中取出当前分组的键
        batch_word = "#".join(batch)       # 用"##"连接键
        # print(result)
        
        user_message_for_model = f"""
        {batch_word}
        """
        messages =  [  
        {'role':'system', 'content': system_message},    
        {'role':'user', 'content': user_message_for_model},  
        ]
        ai_trans_result = get_completion_from_messages(messages)    

        # 将AI翻译的结果输出到日志中
        log_ai_trans(ai_trans_result)

        # 将文本按行分割，并去掉空行
        lines = [line.strip() for line in ai_trans_result.split('\n') if line.strip()]

        # 解析每一行，封装为三元组列表 word_chinese_pronunciation
        # word_c_p = [(fields[0], fields[1], fields[2]) for line in lines if (fields := line.split('\t'))]
        word_c_p = [(fields[0], fields[1], fields[2]) for line in lines if len(fields := line.split('\t')) >= 3]


        word_list_c_p = word_list_c_p + word_c_p

        log_cnt += 1
        print("......完成第 ", log_cnt," 次")

    return word_list_c_p

def execute_update_sql(update_sql,conn):
    cursor = conn.cursor()
    cursor.execute(update_sql)
    conn.commit()
    cursor.close()
    conn.close()


def update_db(word_list_c_p,option):

    # conn_pool = IadduUtil().get_mysql_conn_pool(6)  # 设置最大连接数

    if option == 2: # 只更新发音
        update_sql_list = [
            f"update words set pronunciation = '{item[2]}' where word = '{item[0]}';"
            for item in word_list_c_p
        ]
    elif option == 1: # 只更新中文意思
        update_sql_list = [
            f"update words set chinese = '{item[1]}' where word = '{item[0]}';"
            for item in word_list_c_p
        ]
    else: # 更新中文意思和发音
        update_sql_list = [
            f"update words set chinese = '{item[1]}', pronunciation = '{item[2]}' where word = '{item[0]}';"
            for item in word_list_c_p
        ]

    # 并发执行更新sql
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(execute_update_sql, update_sql,IadduUtil().get_mysql_conn()) for update_sql in update_sql_list]

    # conn_pool.close()

def main():
    word_list = []

    option = input("\n请输入您的选择\n\n 1 : 获取中文;\n 2 : 获取发音 ; \n 3 : 获取中文和发音\n\n" +
    "-----默认为 2----- \n-----调用OpenAI接口，需开启VPN-----\n") or 2

    option = int(option)
    print("Your input is",option)
    print('start.....')

    if option == 1:
        word_list = get_no_chinese_words()
        
    elif option == 2:
        word_list = get_no_pronunciation_words()
    elif option == 3:
        word_list = get_no_pc_words()
    else:
        print("Error Input ... ")

    start_time = time.time()
    word_list_c_p = ai_translator(word_list)
    end_time = time.time()
    ai_elapsed_time = round((end_time - start_time)/60.0,2)
    print(f'\n\n 调用OpenAI接口消耗时长：{ai_elapsed_time} 分钟')

    update_db(word_list_c_p,option)

    print('end.....')

if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    elapsed_time = round((end_time - start_time)/60.0,2)

    print(f'\n\n消耗时长：{elapsed_time} 分钟')