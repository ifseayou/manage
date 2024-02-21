#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Time    :   2024/02/02 17:37:37
@Author  :   benchen
@Contact :   benchen@yowant.com
@Desc    :   None
'''

from ebooklib import epub

def extract_highlighted_text(epub_file):
    book = epub.read_epub(epub_file)
    highlighted_text = []

    for item in book.get_items():
        if item.get_type() == epub.ITEM_DOCUMENT:
            text = item.get_body_content().decode('utf-8')
            # 在这里添加您的代码以检查格式化文本
            # 例如，使用正则表达式搜索下划线或高亮文本
            # 如果找到，将其添加到 highlighted_text 列表中
            # 示例代码：
            if '<u>' in text or 'highlight' in text:
                highlighted_text.append(text)

    return highlighted_text

# 用法示例



def main():
    epub_file = 'your_epub_file.epub'
    highlighted_text = extract_highlighted_text(epub_file)
    print(highlighted_text)
    

def print_epub_contents(epub_file):
    book = epub.read_epub(epub_file)
    
    for item in book.get_items():
        # 输出每个项目的标识符和内容
        print("Item ID:", item.get_id())
        print("Item Content:")
        print(item.get_content())
        print("------------------------------")

# Example usage



if __name__ == '__main__':
    print_epub_contents('/Users/xhl/Desktop/lefe.epub')

    524346334818205696