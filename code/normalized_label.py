#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签规范化脚本 (完整最终版)。

该脚本遍历指定目录下的所有数据集，
根据预设的、经过验证的规则为每一个数据集生成一个标准的 'label.tsv' 文件。
这个 'label.tsv' 文件包含两列：user_id 和 label (human/bot)。
"""
import os
import csv
import json
from pathlib import Path

def write_label_tsv(output_path: Path, records: list):
    """
    将记录写入一个标准的 label.tsv 文件。
    
    Args:
        output_path: 输出的 .tsv 文件路径。
        records: 一个包含 (user_id, label) 元组的列表。
    """
    try:
        # 确保输出目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            # 写入表头
            writer.writerow(['id', 'label'])
            # 写入所有记录
            writer.writerows(records)
        print(f"  ✅ 成功生成: {output_path.name} (共 {len(records)} 条记录)")
    except Exception as e:
        print(f"  ❌ 写入 {output_path.name} 时出错: {e}")

def normalize_from_csv(dataset_dir: Path):
    """
    规则一：从 'label.csv' 或特定的文件名转换。
    """
    print(f"  - [策略] 尝试从 CSV 标签文件进行规范化...")
    
    dataset_name = dataset_dir.name.lower()
    label_file = None

    if "twibot" in dataset_name:
        # 对于 Twibot，label.csv
        label_file = dataset_dir / "label.csv"
        print(f"  - 检测到 Twibot 数据集，查找: label.csv")
    elif "fox8" in dataset_name:
        # 对于 Fox8，fox8_23.csv
        label_file = dataset_dir / "fox8_23.csv"
        print(f"  - 检测到 Fox8 数据集，查找: fox8_23.csv")
    
    # 检查文件是否存在
    if not label_file or not label_file.is_file():
        print(f"  - 未找到预期的标签文件。")
        return

    # --- 后续的读取和写入逻辑保持不变 ---
    records = []
    with open(label_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        try: 
            next(reader, None) # 跳过表头
        except StopIteration: 
            return # 空文件
            
        for row in reader:
            if len(row) >= 2:
                user_id = row[0].strip().lstrip('u')
                label = row[1].strip().lower()
                if label in ['human', 'bot']:
                    records.append((user_id, label))

    if records:
        write_label_tsv(dataset_dir / "label.tsv", records)

def normalize_cresci15_from_folders(dataset_dir: Path):
    """
    规则二：为 Cresci-15 设计，根据子文件夹名称赋予准确标签。
    """
    print(f"  - [策略] 尝试为 Cresci-15 从文件夹结构进行规范化 (使用论文确认的标签)...")
    
    # 根据论文确认的最终标签映射表
    cresci15_label_map = {
        "E13.csv": "human",
        "TFP.csv": "human",
        "FSF.csv": "bot",
        "INT.csv": "bot",
        "TWT.csv": "bot",
    }
    
    records = []
    
    base_search_dir = dataset_dir / "Fake_project_dataset_csv"
    if not base_search_dir.is_dir():
        print(f"  - 警告: 未找到预期的子目录 {base_search_dir}")
        return

    for sub_dir in base_search_dir.iterdir():
        folder_name = sub_dir.name
        if not sub_dir.is_dir() or folder_name not in cresci15_label_map:
            continue
            
        label = cresci15_label_map[folder_name]
        
        try:
            json_file = next(sub_dir.glob("*_features.json"))
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for user_id in data.keys():
                    records.append((user_id, label))
        except StopIteration:
            print(f"  - 警告: 在文件夹 {sub_dir.name} 中未找到 *_features.json 文件。")
        except Exception as e:
            print(f"  - 警告: 处理 {sub_dir.name} 中的 JSON 文件时出错: {e}")
            
    if records:
        write_label_tsv(dataset_dir / "label.tsv", records)

def normalize_cresci17_from_folders(dataset_dir: Path):
    """
    规则三：专门为 Cresci-17 设计，根据子文件夹名称赋予标签。
    """
    print(f"  - [策略] 尝试为 Cresci-17 从文件夹结构进行规范化...")
    
    folder_to_label_map = {
        "fake_followers.csv": "bot",
        "social_spambots_1.csv": "bot", "social_spambots_2.csv": "bot", "social_spambots_3.csv": "bot",
        "traditional_spambots_1.csv": "bot", "traditional_spambots_2.csv": "bot",
        "traditional_spambots_3.csv": "bot", "traditional_spambots_4.csv": "bot",
        "genuine_accounts.csv": "human",
    }
    
    records = []
    
    base_search_dir = dataset_dir / "datasets_full.csv"
    if not base_search_dir.is_dir():
        print(f"  - 警告: 未找到预期的子目录 {base_search_dir}")
        return

    for sub_dir_name, label in folder_to_label_map.items():
        sub_dir = base_search_dir / f"{sub_dir_name}" # 文件夹名不带.csv
        if not sub_dir.is_dir():
            print(f"  - 提示: 未找到子目录 {sub_dir}, 跳过。")
            continue
        
        try:
            json_file = next(sub_dir.glob("*_features.json"))
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for user_id in data.keys():
                    records.append((user_id, label))
        except StopIteration:
            print(f"  - 警告: 在文件夹 {sub_dir.name} 中未找到 *_features.json 文件。")
        except Exception as e:
            print(f"  - 警告: 处理 {sub_dir.name} 中的 JSON 文件时出错: {e}")
            
    if records:
        write_label_tsv(dataset_dir / "label.tsv", records)

def main():
    """
    主函数，遍历所有数据集并应用规范化规则。
    """
    # 指定包含所有处理好数据集的根目录
    root_dir = Path("dataset_processed")

    print("="*50)
    print("开始进行标签文件规范化 (完整最终版)...")
    print(f"扫描根目录: {root_dir.resolve()}")
    print("="*50)

    if not root_dir.is_dir():
        print(f"错误：根目录 '{root_dir}' 不存在。")
        return

    # 遍历根目录下的每一个数据集文件夹
    for dataset_dir in root_dir.iterdir():
        if dataset_dir.is_dir():
            print(f"\n>>> 正在处理数据集: {dataset_dir.name}")
            
            # 检查是否已经存在 label.tsv，如果存在就跳过
            if (dataset_dir / "label.tsv").is_file():
                print("  - 已存在 label.tsv，跳过。")
                continue
            
            # 根据数据集名称应用不同的处理策略
            dataset_name = dataset_dir.name.lower()
            
            if "fox8" in dataset_name or "twibot" in dataset_name:
                normalize_from_csv(dataset_dir)
            elif "cresci-15" in dataset_name:
                normalize_cresci15_from_folders(dataset_dir)
            elif "cresci17" in dataset_name:
                normalize_cresci17_from_folders(dataset_dir)
            else:
                print(f"  - [跳过] 未找到 '{dataset_dir.name}' 的特定处理规则。")

    print("\n" + "="*50)
    print("所有数据集处理完毕！")
    print("="*50)


if __name__ == "__main__":
    main()

### 使用方法
    # python code/normalize_labels.py
    

