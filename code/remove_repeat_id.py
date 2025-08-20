#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
去重脚本：自动处理指定目录中所有TSV文件，智能去除重复ID
保留年份较新的数据集中的重复ID
"""

import os
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict


def extract_year_from_filename(filename: str) -> int:
    """
    从文件名中提取年份信息
    
    参数:
        filename (str): 文件名
        
    返回:
        int: 提取的年份，如果无法提取则返回0
    """
    # 匹配常见的年份模式
    year_patterns = [
        r'(\d{4})',  # 4位数字年份
        r'20(\d{2})',  # 20xx格式
        r'(\d{2})',  # 2位数字年份（假设是20xx）
    ]
    
    for pattern in year_patterns:
        match = re.search(pattern, filename)
        if match:
            year = int(match.group(1))
            # 如果是2位数字，假设是20xx
            if year < 100:
                year += 2000
            return year
    
    return 0


def extract_ids_from_tsv(file_path: str) -> Tuple[List[str], List[str]]:
    """
    从TSV文件中提取bot和human对应的id
    
    参数:
        file_path (str): TSV文件路径
        
    返回:
        tuple: 包含两个列表的元组 (bot_ids, human_ids)
    """
    bot_ids = []
    human_ids = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            tsv_reader = csv.reader(file, delimiter='\t')
            
            for row in tsv_reader:
                if len(row) >= 2:
                    id_value = str(row[0]).strip()
                    role = row[1].strip().lower()
                    
                    if role == 'bot':
                        bot_ids.append(id_value)
                    elif role == 'human':
                        human_ids.append(id_value)
        
        print(f"  - 成功提取：{len(bot_ids)}个bot id，{len(human_ids)}个人类id")
        return bot_ids, human_ids
        
    except Exception as e:
        print(f"  - 处理文件时发生错误：{str(e)}")
        return [], []


def find_duplicate_ids_across_datasets(dataset_ids: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    找出跨数据集重复的ID
    
    参数:
        dataset_ids (dict): 数据集名称到ID列表的映射
        
    返回:
        dict: 重复ID到包含该ID的数据集列表的映射
    """
    id_to_datasets = defaultdict(list)
    
    for dataset_name, ids in dataset_ids.items():
        for id_value in ids:
            id_to_datasets[id_value].append(dataset_name)
    
    # 筛选出被多个数据集包含的ID
    duplicates = {
        id_value: datasets 
        for id_value, datasets in id_to_datasets.items() 
        if len(datasets) > 1
    }
    
    return duplicates


def resolve_duplicates_by_year(
    duplicates: Dict[str, List[str]], 
    dataset_years: Dict[str, int]
) -> Dict[str, str]:
    """
    根据年份解决重复ID，返回每个重复ID应该保留在哪个数据集中
    
    参数:
        duplicates (dict): 重复ID到数据集列表的映射
        dataset_years (dict): 数据集名称到年份的映射
        
    返回:
        dict: 重复ID到保留数据集的映射
    """
    resolution = {}
    
    for id_value, datasets in duplicates.items():
        # 找出包含该ID的数据集中年份最新的
        max_year = -1
        best_datasets = []
        
        # 首先找出最大年份
        for dataset in datasets:
            year = dataset_years.get(dataset, 0)
            if year > max_year:
                max_year = year
        
        # 然后找出所有具有最大年份的数据集
        for dataset in datasets:
            year = dataset_years.get(dataset, 0)
            if year == max_year:
                best_datasets.append(dataset)
        
        # 如果只有一个数据集具有最大年份，直接选择
        if len(best_datasets) == 1:
            resolution[id_value] = best_datasets[0]
        else:
            # 如果有多个数据集年份相同，使用启发式规则选择
            # 规则1: 优先选择数据集名称中包含更多信息的（如包含具体项目名称）
            # 规则2: 如果数据集名称长度相同，选择按字母顺序排列的第一个
            # 规则3: 如果都相同，选择第一个
            best_dataset = select_best_dataset_from_same_year(best_datasets)
            resolution[id_value] = best_dataset
            
            # 记录日志
            print(f"  - 重复ID {id_value} 在多个相同年份({max_year})的数据集中出现:")
            for dataset in best_datasets:
                print(f"    * {dataset}")
            print(f"    选择保留在: {best_dataset}")
    
    return resolution


def select_best_dataset_from_same_year(datasets: List[str]) -> str:
    """
    从相同年份的数据集中选择最佳的一个
    
    启发式规则:
    1. 优先选择名称更具体的数据集（名称长度更长）
    2. 如果长度相同，选择按字母顺序排列的第一个
    3. 如果都相同，选择第一个
    
    参数:
        datasets (list): 相同年份的数据集名称列表
        
    返回:
        str: 选择的最佳数据集名称
    """
    if len(datasets) == 1:
        return datasets[0]
    
    # 按名称长度排序（降序），然后按字母顺序排序
    sorted_datasets = sorted(datasets, key=lambda x: (-len(x), x))
    
    return sorted_datasets[0]


def remove_duplicates_and_redistribute(
    dataset_ids: Dict[str, List[str]], 
    duplicates: Dict[str, List[str]], 
    resolution: Dict[str, str]
) -> Dict[str, List[str]]:
    """
    去除重复ID并重新分配
    
    参数:
        dataset_ids (dict): 原始数据集ID映射
        duplicates (dict): 重复ID信息
        resolution (dict): 重复ID解决方案
        
    返回:
        dict: 处理后的数据集ID映射
    """
    # 创建副本避免修改原始数据
    processed_datasets = {name: ids.copy() for name, ids in dataset_ids.items()}
    
    # 从所有数据集中移除重复ID
    for id_value in duplicates.keys():
        for dataset_name in processed_datasets:
            if id_value in processed_datasets[dataset_name]:
                processed_datasets[dataset_name].remove(id_value)
    
    # 将重复ID添加到指定的数据集中
    for id_value, target_dataset in resolution.items():
        if target_dataset in processed_datasets:
            processed_datasets[target_dataset].append(id_value)
    
    return processed_datasets


def process_all_tsv_files(directory_path: str) -> None:
    """
    处理指定目录中所有TSV文件，去除重复ID
    
    参数:
        directory_path (str): 包含TSV文件的目录路径
    """
    print(f"开始处理目录: {directory_path}")
    
    # 检查目录是否存在
    if not os.path.exists(directory_path):
        print(f"错误：目录 '{directory_path}' 不存在")
        return
    
    # 查找所有TSV文件
    tsv_files = []
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.tsv'):
                tsv_files.append(os.path.join(root, file))
    
    if not tsv_files:
        print("未找到任何TSV文件")
        return
    
    print(f"找到 {len(tsv_files)} 个TSV文件")
    
    # 处理每个TSV文件
    dataset_ids = {}
    dataset_years = {}
    
    for file_path in tsv_files:
        # 从文件路径提取数据集名称
        dataset_name = os.path.basename(os.path.dirname(file_path))
        if dataset_name == os.path.basename(directory_path):
            dataset_name = os.path.basename(file_path).replace('.tsv', '')
        
        # 从文件名提取年份
        filename = os.path.basename(file_path)
        year = extract_year_from_filename(filename)
        
        print(f"\n处理文件: {filename}")
        print(f"  数据集名称: {dataset_name}")
        print(f"  提取年份: {year if year > 0 else '未知'}")
        
        # 提取ID
        bot_ids, human_ids = extract_ids_from_tsv(file_path)
        
        # 合并bot和human ID
        all_ids = bot_ids + human_ids
        
        if all_ids:
            dataset_ids[dataset_name] = all_ids
            dataset_years[dataset_name] = year if year > 0 else 0
        else:
            print(f"  - 警告：文件 {filename} 中没有提取到有效ID")
    
    if not dataset_ids:
        print("没有找到包含有效ID的数据集")
        return
    
    print(f"\n总共处理了 {len(dataset_ids)} 个数据集")
    
    # 统计原始ID数量
    total_original_ids = sum(len(ids) for ids in dataset_ids.values())
    print(f"原始总ID数量: {total_original_ids}")
    
    # 查找重复ID
    print("\n查找重复ID...")
    duplicates = find_duplicate_ids_across_datasets(dataset_ids)
    
    if duplicates:
        print(f"找到 {len(duplicates)} 个重复ID")
        
        # 根据年份解决重复
        print("根据年份解决重复...")
        resolution = resolve_duplicates_by_year(duplicates, dataset_years)
        
        # 去除重复并重新分配
        print("去除重复并重新分配...")
        processed_datasets = remove_duplicates_and_redistribute(
            dataset_ids, duplicates, resolution
        )
        
        # 统计处理后的ID数量
        total_processed_ids = sum(len(ids) for ids in processed_datasets.values())
        removed_count = total_original_ids - total_processed_ids
        
        print(f"\n去重完成！")
        print(f"移除的重复ID数量: {removed_count}")
        print(f"处理后总ID数量: {total_processed_ids}")
        
        # 保存结果
        output_dir = os.path.join(directory_path, "processed_results")
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存处理后的数据集
        processed_file = os.path.join(output_dir, "processed_datasets.json")
        with open(processed_file, 'w', encoding='utf-8') as f:
            json.dump(processed_datasets, f, indent=2, ensure_ascii=False)
        
        # 保存重复ID信息
        duplicates_file = os.path.join(output_dir, "duplicate_ids.json")
        with open(duplicates_file, 'w', encoding='utf-8') as f:
            json.dump(duplicates, f, indent=2, ensure_ascii=False)
        
        # 保存解决方案
        resolution_file = os.path.join(output_dir, "duplicate_resolution.json")
        with open(resolution_file, 'w', encoding='utf-8') as f:
            json.dump(resolution, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存到: {output_dir}")
        
        # 显示每个数据集的统计信息
        print("\n各数据集ID统计:")
        for dataset_name, ids in processed_datasets.items():
            year = dataset_years.get(dataset_name, 0)
            year_str = f"({year})" if year > 0 else "(未知年份)"
            print(f"  {dataset_name} {year_str}: {len(ids)} 个ID")
    
    else:
        print("没有找到重复ID")
        
        # 保存原始数据
        output_dir = os.path.join(directory_path, "processed_results")
        os.makedirs(output_dir, exist_ok=True)
        
        original_file = os.path.join(output_dir, "original_datasets.json")
        with open(original_file, 'w', encoding='utf-8') as f:
            json.dump(dataset_ids, f, indent=2, ensure_ascii=False)
        
        print(f"\n原始数据已保存到: {output_dir}")


def main():
    """主函数"""
    # 指定要处理的目录
    target_directory = "dataset_processed"
    
    print("=" * 60)
    print("智能TSV文件去重工具")
    print("=" * 60)
    
    # 处理所有TSV文件
    process_all_tsv_files(target_directory)
    
    print("\n" + "=" * 60)
    print("处理完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
