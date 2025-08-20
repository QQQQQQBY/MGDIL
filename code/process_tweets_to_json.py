#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_tweets_to_json.py
-------------------------
批量将推文CSV文件转换为每个用户的推文事件，并将它们合并到用户JSON文件中。

- 输入CSV列（忽略额外列）：
  "created_at","id","text","source","user_id","truncated",
  "in_reply_to_status_id","in_reply_to_user_id","in_reply_to_screen_name",
  "retweeted_status_id","geo","place","retweet_count","reply_count",
  "favorite_count","num_hashtags","num_urls","num_mentions","timestamp"

- 输入用户JSON结构（用户字典）：
  {
    "<user_id>": {
        ... (任何现有键，例如 "profile_text", "profile_numeric", ...)
    },
    ...
  }

此脚本将在每个用户对象下添加/扩展一个 "tweet_events" 列表。

使用方法
-----
python process_tweets_to_json.py \
  --tweets-csv example.csv \
  --users-json users_profile_features.json \
  --out-json users_profile_features_enriched.json

可选参数
--------------
  --keep-text         在每个事件中保留清理后的推文文本（默认：开启）
  --drop-text         不保留推文文本（隐私保护/文件更小）
  --encoding ENC      CSV编码（默认：utf-8）
"""
import argparse, csv, json, re, html
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from tqdm import tqdm

# 正则表达式模式定义
TAG_RE = re.compile(r"<[^>]+>")           # HTML标签匹配模式
URL_RE = re.compile(r'https?://\S+')       # URL匹配模式
MENTION_RE = re.compile(r'@[\w_]+' )       # 提及用户匹配模式
HASHTAG_RE = re.compile(r'#\w+')           # 话题标签匹配模式

def strip_tags(s: str) -> str:
    """
    移除HTML标签
    
    Args:
        s: 输入字符串
        
    Returns:
        str: 移除HTML标签后的字符串
    """
    return TAG_RE.sub("", s or "")

def clean_text(s: str) -> str:
    """
    清理推文文本
    包括HTML解码、移除零宽字符、规范化空白字符
    
    Args:
        s: 输入文本
        
    Returns:
        str: 清理后的文本
    """
    if s is None: return ""
    s = html.unescape(s)             # 反转 &lt; &gt; &amp;
    s = s.replace("\u200b","")       # 移除零宽字符
    s = re.sub(r'\s+', ' ', s).strip()  # 规范化空白字符
    return s

def parse_source(s: str) -> str:
    """
    解析推文来源
    "source"字段通常包含HTML锚点标签，提取可见的客户端名称
    
    Args:
        s: 来源字符串
        
    Returns:
        str: 清理后的客户端名称，如果为空则返回"Unknown"
    """
    pure = strip_tags(s or "")
    return pure if pure else "Unknown"

def parse_ts(created_at: str, fallback_ts: str):
    """
    尝试多种时间格式解析，返回ISO字符串和小时/星期几
    
    Args:
        created_at: 创建时间字符串
        fallback_ts: 备用时间戳字符串
        
    Returns:
        tuple: (ISO时间字符串, 小时, 星期几)，如果解析失败则返回(None, None, None)
    """
    fmts = [
        "%a %b %d %H:%M:%S %z %Y",   # Sun Apr 21 10:43:18 +0000 2013
        "%Y-%m-%d %H:%M:%S",         # 2013-04-21 12:43:18
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for s in (created_at, fallback_ts):
        if not s: 
            continue
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt)
                iso = dt.isoformat()
                return iso, dt.hour, dt.strftime("%a")
            except Exception:
                continue
    return None, None, None

def extract_url_domains(text: str):
    """
    从文本中提取URL和域名
    
    Args:
        text: 输入文本
        
    Returns:
        tuple: (URL列表, 域名列表)
    """
    urls = URL_RE.findall(text or "")
    domains = []
    for u in urls:
        try:
            host = urlparse(u).netloc.lower()
            if host: domains.append(host)
        except Exception:
            continue
    return urls, domains

def safe_int(x, default=0):
    """
    安全地将值转换为整数
    处理None值、空字符串和转换异常
    
    Args:
        x: 输入值
        default: 默认值，转换失败时返回
        
    Returns:
        int: 转换后的整数或默认值
    """
    try:
        if x is None: return default
        s = str(x).strip()
        if s in ("", "NULL", "None", "NaN"): return default
        return int(float(s))
    except Exception:
        return default

def load_users_json(path: str):
    """
    加载用户JSON文件
    
    Args:
        path: JSON文件路径
        
    Returns:
        dict: 用户数据字典，如果文件不存在则返回空字典
    """
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def ensure_user(users: dict, uid: str):
    """
    确保用户存在于用户字典中，并初始化tweet_events列表
    
    Args:
        users: 用户字典
        uid: 用户ID
    """
    if uid not in users:
        users[uid] = {}
    if "tweet_events" not in users[uid]:
        users[uid]["tweet_events"] = []

def process_csv(tweets_csv: str, users_json: str, out_json: str, keep_text: bool, encoding: str):
    """
    处理推文CSV文件并合并到用户JSON中
    
    Args:
        tweets_csv: 推文CSV文件路径
        users_json: 用户JSON文件路径
        out_json: 输出JSON文件路径
        keep_text: 是否保留推文文本
        encoding: CSV文件编码
    """
    # 加载现有用户数据
    users = load_users_json(users_json)

    count_rows = 0
    # 读取推文CSV文件
    with open(tweets_csv, "r", encoding=encoding, newline="") as f:
        # reader = csv.DictReader(f)
        # for row in reader:
        reader = csv.DictReader(f)
        # 使用 tqdm 包装 reader，它会自动显示进度
        for row in tqdm(reader, desc="融合推文中"):
            # 获取用户ID
            user_id = (row.get("user_id") or "").strip()
            if not user_id:
                continue

            # 清理和解析推文文本
            text = clean_text(row.get("text"))
            source = parse_source(row.get("source"))
            
            # 判断推文类型
            is_retweet = bool(row.get("retweeted_status_id") and row.get("retweeted_status_id").strip() and row.get("retweeted_status_id").strip().upper() != "NULL")
            is_reply = bool((row.get("in_reply_to_status_id") and row.get("in_reply_to_status_id").strip() not in ("", "0", "NULL")) or
                            (row.get("in_reply_to_user_id") and row.get("in_reply_to_user_id").strip() not in ("", "0", "NULL")))

            # 提取URL和域名
            urls, domains = extract_url_domains(text)

            # 统计推文中的各种元素数量
            num_hashtags = safe_int(row.get("num_hashtags"), default=len(HASHTAG_RE.findall(text)))
            num_urls = safe_int(row.get("num_urls"), default=len(urls))
            num_mentions = safe_int(row.get("num_mentions"), default=len(MENTION_RE.findall(text)))

            # 获取推文的统计信息
            rt_count = safe_int(row.get("retweet_count"))
            reply_count = safe_int(row.get("reply_count"))
            fav_count = safe_int(row.get("favorite_count"))

            # 解析时间信息
            iso_ts, hour_of_day, day_of_week = parse_ts(row.get("created_at"), row.get("timestamp"))

            # 构建推文事件对象
            event = {
                "tweet_id": row.get("id"),                    # 推文ID
                "created_at_iso": iso_ts,                     # ISO格式创建时间
                "hour_of_day": hour_of_day,                   # 一天中的小时
                "day_of_week": day_of_week,                   # 星期几
                "client_source": source,                       # 客户端来源
                "is_retweet": is_retweet,                     # 是否为转发
                "is_reply": is_reply,                         # 是否为回复
                "len_chars": len(text),                       # 字符长度
                "num_hashtags": num_hashtags,                 # 话题标签数量
                "num_urls": num_urls,                         # URL数量
                "num_mentions": num_mentions,                 # 提及数量
                "retweet_count": rt_count,                    # 转发次数
                "reply_count": reply_count,                   # 回复次数
                "favorite_count": fav_count,                  # 收藏次数
                "urls": urls,                                 # URL列表
                "url_domains": domains,                       # 域名列表
            }
            
            # 根据参数决定是否保留推文文本
            if keep_text:
                event["text"] = text

            # 确保用户存在并添加推文事件
            ensure_user(users, user_id)
            users[user_id]["tweet_events"].append(event)
            count_rows += 1

    # 将结果写入输出JSON文件
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

    print(f"[OK] 已处理推文行数: {count_rows}")
    print(f"[已写入] {out_json}")

def main():
    """
    主函数：解析命令行参数并执行推文处理
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--tweets-csv", required=True, help="输入推文CSV文件")
    ap.add_argument("--users-json", required=True, help="现有用户JSON文件（将被读取和合并）")
    ap.add_argument("--out-json", required=True, help="输出合并后的JSON文件")
    ap.add_argument("--encoding", default="utf-8", help="CSV编码（默认：utf-8）")
    
    # 互斥参数组：是否保留推文文本
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--keep-text", action="store_true", help="在事件中保留清理后的推文文本（默认）")
    g.add_argument("--drop-text", action="store_true", help="删除推文文本以保护隐私/减小文件大小")
    
    args = ap.parse_args()
   

    # 确定是否保留推文文本
    keep_text = True if args.keep_text or not args.drop_text else False

    # 执行CSV处理
    process_csv(args.tweets_csv, args.users_json, args.out_json, keep_text=keep_text, encoding=args.encoding)

if __name__ == "__main__":
    main()