#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Twibot-20 推文融合。

该脚本调用通用的 process_tweets_to_json.py 脚本，
来处理规范化后的 Twibot-20 推文数据，并将其融合到用户画像中。
"""
from pathlib import Path
import sys

# --- 关键步骤：导入你通用的融合脚本中的核心函数 ---
try:
    # 假设这个启动器脚本和你想要调用的脚本都在同一个 'code' 文件夹下
    from process_tweets_to_json import process_csv
except ImportError:
    print("错误: 无法从 'process_tweets_to_json' 导入 process_csv 函数。")
    print("请确保 run_twibot20_merge.py 与 process_tweets_to_json.py 在同一个目录下。")
    sys.exit(1)


if __name__ == '__main__':
    # --- 配置区：在这里定义所有文件路径和参数 ---
    project_root = Path(r"E:\aashuoshi\LLM_Finetune_socialbot\LLM_Finetune_socialbot - csv")
    
    # 输入文件1: 规范化后的推文 CSV
    tweets_input_csv = project_root / "dataset_processed/Twibot-20/tweets_normalized.csv"
    
    # 输入文件2: 已经处理好的用户画像 JSON
    users_input_json = project_root / "dataset_processed/Twibot-20/users_profile_features.json"
    
    # 最终的输出文件
    final_output_json = project_root / "dataset_processed/Twibot-20/Twibot-20_processed.json"
    
    # 其他参数配置
    should_keep_text = True      # 等同于 --keep-text 参数
    csv_file_encoding = "utf-8"  # 等同于 --encoding 参数
    # --- 配置结束 ---

    # --- 检查与执行 ---
    print("--- 开始执行 Twibot-20 推文融合任务 ---")
    print(f"读取推文CSV: {tweets_input_csv.name}")
    print(f"读取用户JSON: {users_input_json.name}")
    print(f"写入最终输出: {final_output_json.name}")
    print("-" * 20)

    if not tweets_input_csv.is_file():
        print(f"错误: 找不到输入的推文CSV文件: {tweets_input_csv}")
    elif not users_input_json.is_file():
        print(f"错误: 找不到输入的用户JSON文件: {users_input_json}")
    else:
        # 确保输出目录存在
        final_output_json.parent.mkdir(parents=True, exist_ok=True)
        
        # 调用通用的融合函数
        process_csv(
            tweets_csv=str(tweets_input_csv),
            users_json=str(users_input_json),
            out_json=str(final_output_json),
            keep_text=should_keep_text,
            encoding=csv_file_encoding
        )
    
    print("\n--- Twibot-20 数据集完全处理完毕！ ---")