#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
处理Fox8数据集的JSON文件（流式处理版本）

使用流式处理避免内存错误，适用于大文件。

使用方法:
  python process_fox8_json_streaming.py --input <fox8_json_file> --output <output_json_file>
"""

import json
import argparse
from pathlib import Path
import sys
import os
import ijson  # 需要安装: pip install ijson

# 导入特征提取函数
sys.path.append(str(Path(__file__).parent))
from extract_profile_features import extract_profile_features

def process_fox8_json_streaming(input_path: Path, output_path: Path):
    """
    使用流式处理Fox8数据集的JSON文件
    """
    print(f"开始处理Fox8数据集: {input_path.name}")
    
    # 确保输出目录存在
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 处理结果字典
    output_dict = {}
    user_count = 0
    
    # 使用ijson进行流式解析
    with open(input_path, "rb") as f:
        # 解析用户数组
        users = ijson.items(f, "item")
        
        for user_entry in users:
            user_count += 1
            if user_count % 100 == 0:
                print(f"处理进度: {user_count} 个用户")
            
            user_id = str(user_entry.get("user_id"))
            if not user_id:
                continue
            
            # 从第一条推文中获取用户信息
            user_tweets = user_entry.get("user_tweets", [])
            if not user_tweets:
                print(f"警告: 用户 {user_id} 没有推文，跳过")
                continue
            
            # 获取用户信息（从第一条推文中）
            user_info = user_tweets[0].get("user", {})
            if not user_info:
                print(f"警告: 用户 {user_id} 的用户信息为空，跳过")
                continue
            
            # 提取用户特征
            try:
                profile_features, profile_features_missing = extract_profile_features(user_info)
            except Exception as e:
                print(f"错误: 提取用户 {user_id} 特征时出错: {e}")
                continue
            
            # 处理推文数据
            tweet_events = []
            for tweet in user_tweets:
                try:
                    # 提取推文特征
                    tweet_event = extract_tweet_features(tweet)
                    tweet_events.append(tweet_event)
                except Exception as e:
                    print(f"警告: 处理用户 {user_id} 的推文时出错: {e}")
                    continue
            
            # 构建输出结构
            output_dict[user_id] = {
                "profile_features": profile_features,
                "profile_features_missing": profile_features_missing,
                "tweet_events": tweet_events,
                "label": user_entry.get("label"),
                "dataset": user_entry.get("dataset")
            }
            
            # 每处理1000个用户就保存一次，避免内存占用过大
            if user_count % 1000 == 0:
                print(f"已处理 {user_count} 个用户，当前内存中用户数: {len(output_dict)}")
    
    # 写入输出文件
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_dict, f, ensure_ascii=False, indent=2)
    
    print(f"处理完成! 成功处理 {len(output_dict)} 个用户")
    print(f"输出文件: {output_path}")

def extract_tweet_features(tweet):
    """
    从推文中提取特征
    """
    # 清理和解析推文文本
    text = clean_text(tweet.get("text", ""))
    source = parse_source(tweet.get("source", ""))
    
    # 判断推文类型
    is_retweet = bool(tweet.get("retweeted_status_id") or tweet.get("retweeted_status"))
    is_reply = bool(tweet.get("in_reply_to_status_id") or tweet.get("in_reply_to_user_id"))
    
    # 提取URL和域名
    urls, domains = extract_url_domains(text)
    
    # 统计推文中的各种元素数量
    entities = tweet.get("entities", {})
    num_hashtags = len(entities.get("hashtags", []))
    num_urls = len(entities.get("urls", []))
    num_mentions = len(entities.get("user_mentions", []))
    
    # 获取推文的统计信息
    rt_count = tweet.get("retweet_count", 0)
    fav_count = tweet.get("favorite_count", 0)
    
    # 解析时间信息
    created_at = tweet.get("created_at")
    iso_ts, hour_of_day, day_of_week = parse_ts(created_at, None)
    
    # 构建推文事件对象
    event = {
        "tweet_id": str(tweet.get("id")),
        "created_at_iso": iso_ts,
        "hour_of_day": hour_of_day,
        "day_of_week": day_of_week,
        "client_source": source,
        "is_retweet": is_retweet,
        "is_reply": is_reply,
        "len_chars": len(text),
        "num_hashtags": num_hashtags,
        "num_urls": num_urls,
        "num_mentions": num_mentions,
        "retweet_count": rt_count,
        "favorite_count": fav_count,
        "urls": urls,
        "url_domains": domains,
        "text": text  # 保留推文文本
    }
    
    return event

def clean_text(s):
    """清理推文文本"""
    if s is None: 
        return ""
    import re
    import html
    s = html.unescape(s)             # 反转 &lt; &gt; &amp;
    s = s.replace("\u200b","")       # 移除零宽字符
    s = re.sub(r'\s+', ' ', s).strip()  # 规范化空白字符
    return s

def parse_source(s):
    """解析推文来源"""
    if not s:
        return "Unknown"
    import re
    # 移除HTML标签
    s = re.sub(r'<[^>]+>', '', s)
    return s.strip() if s.strip() else "Unknown"

def parse_ts(created_at, fallback_ts):
    """解析时间信息"""
    if not created_at:
        return None, None, None
    
    from datetime import datetime
    fmts = [
        "%a %b %d %H:%M:%S %z %Y",   # Sun Apr 21 10:43:18 +0000 2013
        "%Y-%m-%d %H:%M:%S",         # 2013-04-21 12:43:18
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
    ]
    
    for fmt in fmts:
        try:
            dt = datetime.strptime(created_at, fmt)
            iso = dt.isoformat()
            return iso, dt.hour, dt.strftime("%a")
        except Exception:
            continue
    
    return None, None, None

def extract_url_domains(text):
    """从文本中提取URL和域名"""
    import re
    from urllib.parse import urlparse
    
    url_pattern = re.compile(r'https?://\S+')
    urls = url_pattern.findall(text or "")
    domains = []
    
    for u in urls:
        try:
            host = urlparse(u).netloc.lower()
            if host: 
                domains.append(host)
        except Exception:
            continue
    
    return urls, domains

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="处理Fox8数据集的JSON文件（流式处理）")
    parser.add_argument("--input", required=True, help="输入的Fox8 JSON文件路径")
    parser.add_argument("--output", required=True, help="输出的JSON文件路径")
    args = parser.parse_args()
    
    input_file = Path(args.input)
    output_file = Path(args.output)
    
    if not input_file.exists():
        print(f"错误: 输入文件不存在: {input_file}")
        sys.exit(1)
    
    # 检查是否安装了ijson
    try:
        import ijson
    except ImportError:
        print("错误: 需要安装ijson库。请运行: pip install ijson")
        sys.exit(1)
    
    process_fox8_json_streaming(input_file, output_file)
