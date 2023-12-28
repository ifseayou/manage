#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2023/12/26 14:57:06
@Author  :   i_add_u
@Contact :   xhl1024@gmail.com
@Desc    :   1，根据 word_freq 表中统计的单词频率，
             2，更新 words 表中 level 字段
'''

from iaddu_util import IadduUtil

def execute_sql(connection, sql):
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        connection.commit()
        print("SQL executed successfully.")
    except Exception as e:
        connection.rollback()
        print(f"Error executing SQL: {e}")
    finally:
        cursor.close()

def set_word_level(conn):
    # 执行 SQL 事务
    try:
        # 开启事务
        conn.start_transaction()

        # 执行 SQL 语句
        sql_statements = [
            "drop table if exists words_tmp;",

            """create table words_tmp
            (
                `id`            bigint       not null auto_increment,
                `word`          varchar(200) not null comment '单词，比如 hello，单词首字符小写为标准',
                `chinese`       varchar(200)      default null comment '单词对应的中文意思',
                `similar_word`  varchar(200)      default null comment '当前单词，相似/容易搞混/对照的词，多个按照,隔开',
                `eg_sentence`   text comment '对该单词的一个例句子',
                `pronunciation` varchar(200)      default null,
                `level`         int               default null comment '单词等级，每个单词，按照统计规律对应的频度',
                `tag`           varchar(200)      default null comment '单词对应的标签，home，fruit，animal,country',
                `created_at`    timestamp    null default current_timestamp,
                `updated_at`    timestamp    null default current_timestamp on update current_timestamp,
                primary key (`id`),
                unique key `uniq_word` (`word`)
            ) default charset = utf8mb4
            row_format = dynamic comment = '单词表'
            ;
            """,

            """
            insert into words_tmp(word, chinese, similar_word, eg_sentence, pronunciation, level, tag, created_at, updated_at)
            select t1.word          as word
                , t1.chinese       as chinese
                , t1.similar_word  as similar_word
                , t1.eg_sentence   as eg_sentence
                , t1.pronunciation as pronunciation
                , tx.rk            as level
                , t1.tag           as tag
                , t1.created_at    as created_at
                , t1.updated_at    as updated_at
            from words  t1
            left join (select word_name
                            , book_cnt
                            , word_cnt
                            , rk
                    from (select word_name                                                 as word_name
                                , book_cnt                                                  as book_cnt
                                , word_cnt                                                  as word_cnt
                                , row_number() over (order by book_cnt desc, word_cnt desc) as rk
                            from (select word_name                 as word_name
                                        , count(distinct book_name) as book_cnt
                                        , sum(word_cnt)             as word_cnt
                                from word_freq
                                group by word_name
                            ) t1
                    ) t2
                    ) tx
            on t1.word = tx.word_name
            ;
            """,
    
            "drop table if exists words_bk;",
            "alter table words rename to words_bk;",
            "alter table words_tmp rename to words;"
        ]

        for sql in sql_statements:
            execute_sql(conn, sql)

        # 提交事务
        conn.commit()
    except Exception as e:
        # 回滚事务
        conn.rollback()
        print(f"Transaction failed: {e}")
    finally:
        # 关闭数据库连接
        conn.close()

    pass 

if __name__ == '__main__':
    conn = IadduUtil().get_mysql_conn()
    set_word_level(conn)