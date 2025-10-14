#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV 行过滤脚本
根据指定列和值查找并打印匹配的行

使用方法：
    python python/csv_filter.py /NV1/ysh/dataset/buildings_1m/buildings_1m.csv id 106144990
    python csv_filter.py data.csv name "张三"
    python csv_filter.py data.csv age 25

或者使用列索引（从0开始）：
    python csv_filter.py data.csv 0 "张三"
"""

import csv
import sys
import argparse


def filter_csv_by_column(csv_file, column_identifier, target_value, 
                         encoding='utf-8', delimiter=','):
    """
    根据列名或列索引过滤 CSV 文件
    
    Args:
        csv_file: CSV 文件路径
        column_identifier: 列名（字符串）或列索引（整数）
        target_value: 要匹配的值
        encoding: 文件编码，默认 utf-8
        delimiter: 分隔符，默认逗号
    
    Returns:
        匹配的行列表
    """
    matched_rows = []
    
    try:
        with open(csv_file, 'r', encoding=encoding, newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            
            # 读取表头
            try:
                header = next(reader)
            except StopIteration:
                print("错误：CSV 文件为空")
                return matched_rows
            
            # 确定列索引
            if isinstance(column_identifier, int) or column_identifier.isdigit():
                # 使用列索引
                column_index = int(column_identifier)
                if column_index < 0 or column_index >= len(header):
                    print(f"错误：列索引 {column_index} 超出范围 (0-{len(header)-1})")
                    return matched_rows
                column_name = header[column_index]
            else:
                # 使用列名
                column_name = column_identifier
                if column_name not in header:
                    print(f"错误：找不到列名 '{column_name}'")
                    print(f"可用的列名: {', '.join(header)}")
                    return matched_rows
                column_index = header.index(column_name)
            
            print(f"正在搜索列 '{column_name}' (索引 {column_index}) 中值为 '{target_value}' 的行...\n")
            print("=" * 80)
            
            # 遍历所有行
            row_number = 1  # 数据行号（不含表头）
            total_rows = 0
            
            for row in reader:
                total_rows += 1
                if len(row) <= column_index:
                    continue
                
                # 检查是否匹配（支持字符串比较和数值比较）
                cell_value = row[column_index].strip()
                if cell_value == str(target_value).strip():
                    matched_rows.append((row_number, row))
                    
                    # 打印匹配的行
                    print(f"匹配行 #{row_number}:")
                    print("-" * 80)
                    for i, (col_name, col_value) in enumerate(zip(header, row)):
                        marker = " <<<" if i == column_index else ""
                        print(f"  {col_name:20s}: {col_value}{marker}")
                    print("=" * 80)
                    print()
                
                row_number += 1
            
            # 打印统计信息
            print(f"\n搜索完成！")
            print(f"总行数: {total_rows}")
            print(f"匹配行数: {len(matched_rows)}")
            
    except FileNotFoundError:
        print(f"错误：文件 '{csv_file}' 不存在")
    except Exception as e:
        print(f"错误：{str(e)}")
    
    return matched_rows


def main():
    parser = argparse.ArgumentParser(
        description='根据列名和值过滤 CSV 文件',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s data.csv name "张三"          # 查找 name 列值为 "张三" 的行
  %(prog)s data.csv 0 "张三"             # 查找第 0 列（第一列）值为 "张三" 的行
  %(prog)s data.csv age 25               # 查找 age 列值为 25 的行
  %(prog)s data.csv price 99.99 -d ";"   # 使用分号分隔的 CSV
  %(prog)s data.csv name "李四" -e gbk   # 使用 GBK 编码
        """
    )
    
    parser.add_argument('csv_file', help='CSV 文件路径')
    parser.add_argument('column', help='列名或列索引（从 0 开始）')
    parser.add_argument('value', help='要查找的值')
    parser.add_argument('-e', '--encoding', default='utf-8', 
                       help='文件编码 (默认: utf-8，常用: gbk, gb2312)')
    parser.add_argument('-d', '--delimiter', default=',', 
                       help='分隔符 (默认: 逗号)')
    parser.add_argument('-o', '--output', help='将结果保存到文件')
    
    args = parser.parse_args()
    
    # 执行过滤
    matched = filter_csv_by_column(
        args.csv_file, 
        args.column, 
        args.value,
        encoding=args.encoding,
        delimiter=args.delimiter
    )
    
    # 如果指定了输出文件，保存结果
    if args.output and matched:
        try:
            with open(args.output, 'w', encoding=args.encoding, newline='') as f:
                writer = csv.writer(f, delimiter=args.delimiter)
                
                # 重新读取表头
                with open(args.csv_file, 'r', encoding=args.encoding) as original:
                    header = next(csv.reader(original, delimiter=args.delimiter))
                    writer.writerow(header)
                
                # 写入匹配的行
                for _, row in matched:
                    writer.writerow(row)
                
                print(f"\n结果已保存到: {args.output}")
        except Exception as e:
            print(f"保存文件时出错: {str(e)}")


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("使用方法: python csv_filter.py <csv文件> <列名或索引> <值>")
        print("示例: python csv_filter.py data.csv name '张三'")
        print("更多帮助: python csv_filter.py -h")
        sys.exit(1)
    
    main()
