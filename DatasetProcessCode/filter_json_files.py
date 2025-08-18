#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON文件过滤脚本：基于processed_datasets.json中的ID列表过滤每个数据集的JSON文件
只保留在processed_datasets.json中有对应ID的内容
"""

import os
import json
import shutil
from pathlib import Path
from typing import Dict, List, Set
import argparse


def load_processed_datasets(processed_file_path: str) -> Dict[str, Set[str]]:
    """
    加载processed_datasets.json文件，返回数据集名称到ID集合的映射
    
    参数:
        processed_file_path (str): processed_datasets.json文件路径
        
    返回:
        dict: 数据集名称到ID集合的映射
    """
    try:
        with open(processed_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 转换为集合，提高查找效率
        return {dataset_name: set(ids) for dataset_name, ids in data.items()}
    
    except Exception as e:
        print(f"错误：无法加载 {processed_file_path}: {str(e)}")
        return {}


def find_json_files(directory_path: str) -> List[str]:
    """
    在指定目录中查找所有JSON文件
    
    参数:
        directory_path (str): 要搜索的目录路径
        
    返回:
        list: JSON文件路径列表
    """
    json_files = []
    
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    
    return json_files


def get_dataset_name_from_path(file_path: str, base_directory: str) -> str:
    """
    从文件路径中提取数据集名称
    
    参数:
        file_path (str): JSON文件路径
        base_directory (str): 基础目录路径
        
    返回:
        str: 数据集名称
    """
    # 获取相对于基础目录的路径
    rel_path = os.path.relpath(file_path, base_directory)
    
    # 分割路径，取第一个目录作为数据集名称
    path_parts = rel_path.split(os.sep)
    
    if len(path_parts) > 0:
        return path_parts[0]
    
    return "unknown"


def filter_json_file(
    json_file_path: str, 
    valid_ids: Set[str], 
    output_dir: str,
    dataset_name: str
) -> int:
    """
    过滤JSON文件，只保留有效的ID
    
    参数:
        json_file_path (str): 要过滤的JSON文件路径
        valid_ids (set): 有效的ID集合
        output_dir (str): 输出目录
        dataset_name (str): 数据集名称
        
    返回:
        int: 保留的记录数量
    """
    try:
        # 读取JSON文件
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 过滤数据，只保留有效的ID
        filtered_data = {}
        kept_count = 0
        
        for user_id, user_data in data.items():
            if user_id in valid_ids:
                filtered_data[user_id] = user_data
                kept_count += 1
        
        # 创建输出目录
        output_subdir = os.path.join(output_dir, dataset_name)
        os.makedirs(output_subdir, exist_ok=True)
        
        # 生成输出文件名
        original_filename = os.path.basename(json_file_path)
        output_filename = f"filtered_{original_filename}"
        output_path = os.path.join(output_subdir, output_filename)
        
        # 保存过滤后的数据
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, indent=2, ensure_ascii=False)
        
        print(f"  - {original_filename}: 保留 {kept_count}/{len(data)} 条记录")
        return kept_count
        
    except Exception as e:
        print(f"  - 处理文件 {json_file_path} 时出错: {str(e)}")
        return 0


def process_all_json_files(
    base_directory: str, 
    processed_datasets_path: str, 
    output_directory: str
) -> None:
    """
    处理所有JSON文件，根据processed_datasets.json进行过滤
    
    参数:
        base_directory (str): 包含JSON文件的基础目录
        processed_datasets_path (str): processed_datasets.json文件路径
        output_directory (str): 输出目录
    """
    print(f"开始处理目录: {base_directory}")
    
    # 加载处理后的数据集信息
    print("加载processed_datasets.json...")
    processed_datasets = load_processed_datasets(processed_datasets_path)
    
    if not processed_datasets:
        print("错误：无法加载processed_datasets.json")
        return
    
    print(f"成功加载 {len(processed_datasets)} 个数据集的信息")
    
    # 查找所有JSON文件
    print("查找JSON文件...")
    json_files = find_json_files(base_directory)
    
    if not json_files:
        print("未找到任何JSON文件")
        return
    
    print(f"找到 {len(json_files)} 个JSON文件")
    
    # 创建输出目录
    os.makedirs(output_directory, exist_ok=True)
    
    # 统计信息
    total_files = 0
    total_kept = 0
    dataset_stats = {}
    
    # 处理每个JSON文件
    print("\n开始过滤JSON文件...")
    
    for json_file in json_files:
        # 获取数据集名称
        dataset_name = get_dataset_name_from_path(json_file, base_directory)
        
        # 检查是否有对应的数据集信息
        if dataset_name in processed_datasets:
            valid_ids = processed_datasets[dataset_name]
            
            print(f"\n处理数据集: {dataset_name}")
            print(f"  文件: {os.path.basename(json_file)}")
            print(f"  有效ID数量: {len(valid_ids)}")
            
            # 过滤文件
            kept_count = filter_json_file(
                json_file, valid_ids, output_directory, dataset_name
            )
            
            # 更新统计信息
            total_files += 1
            total_kept += kept_count
            
            if dataset_name not in dataset_stats:
                dataset_stats[dataset_name] = {'files': 0, 'kept': 0}
            
            dataset_stats[dataset_name]['files'] += 1
            dataset_stats[dataset_name]['kept'] += kept_count
            
        else:
            print(f"\n警告：文件 {json_file} 对应的数据集 {dataset_name} 不在processed_datasets.json中")
    
    # 输出统计信息
    print("\n" + "=" * 60)
    print("过滤完成！统计信息:")
    print("=" * 60)
    print(f"总处理文件数: {total_files}")
    print(f"总保留记录数: {total_kept}")
    
    print("\n各数据集统计:")
    for dataset_name, stats in dataset_stats.items():
        print(f"  {dataset_name}: 处理 {stats['files']} 个文件，保留 {stats['kept']} 条记录")
    
    print(f"\n过滤后的文件已保存到: {output_directory}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='过滤JSON文件，只保留有效的ID')
    parser.add_argument(
        '--input', '-i', 
        default='/Users/boyuqiao/Downloads/dataset_processed',
        help='输入目录路径（包含JSON文件）'
    )
    parser.add_argument(
        '--processed', '-p',
        default='/Users/boyuqiao/Downloads/dataset_processed/processed_results/processed_datasets.json',
        help='processed_datasets.json文件路径'
    )
    parser.add_argument(
        '--output', '-o',
        default='/Users/boyuqiao/Downloads/dataset_processed/filtered_results',
        help='输出目录路径'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("JSON文件过滤工具")
    print("=" * 60)
    print(f"输入目录: {args.input}")
    print(f"处理文件: {args.processed}")
    print(f"输出目录: {args.output}")
    print("=" * 60)
    
    # 检查输入文件是否存在
    if not os.path.exists(args.processed):
        print(f"错误：processed_datasets.json文件不存在: {args.processed}")
        return
    
    if not os.path.exists(args.input):
        print(f"错误：输入目录不存在: {args.input}")
        return
    
    # 处理所有JSON文件
    process_all_json_files(args.input, args.processed, args.output)
    
    print("\n" + "=" * 60)
    print("处理完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
