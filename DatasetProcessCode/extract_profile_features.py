#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一的用户画像特征提取器 (Unified Profile Feature Extractor)

本脚本可以处理两种格式的用户数据文件：
1. JSON 文件: 包含一个用户对象列表 (list of user objects)。
2. CSV 文件: 每一行代表一个用户的扁平化数据。

它会读取输入文件，为每个用户提取一套丰富的个人资料特征，
并最终生成一个统一格式的 JSON 文件，以用户ID为键进行组织。

使用方法:
  python unified_feature_extractor.py --input <path_to_your_file.json_or.csv> --output <path_to_output.json>
"""

import json
import re
import csv
from pathlib import Path
import argparse
from urllib.parse import urlparse


# 正则表达式模式定义
EMOJI_RE = re.compile('['
    '\U0001F600-\U0001F64F'  # 表情符号
    '\U0001F300-\U0001F5FF'  # 杂项符号和象形文字
    '\U0001F680-\U0001F6FF'  # 交通和地图符号
    '\U0001F1E0-\U0001F1FF'  # 区域指示符号
    '\U00002702-\U000027B0'   # 装饰符号
    '\U000024C2-\U0001F251'   # 封闭字母数字符号
    ']+', flags=re.UNICODE)

EMAIL_RE = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
PHONE_RE = re.compile(r'(?:(?:\+?\d{1,3})?[-.\s]?)?(?:\(?\d{2,4}\)?[-.\s]?)?\d{3,4}[-.\s]?\d{4}')
GENERIC_LOCATIONS = {'earth', 'world', 'peaceful world', 'somewhere', 'everywhere'}


def count_emoji(s):
    """
    统计文本中的表情符号数量
    
    Args:
        s: 输入文本
        
    Returns:
        int: 表情符号的数量
    """
    if not s: return 0
    return len(EMOJI_RE.findall(s))

URL_RE = re.compile(r'https?://\S+')      # URL匹配模式
from urllib.parse import urlparse  # 如果还没引入，需新增

def _iter_urls_from_entity(url_entity):
    """
    从 url_entity 中抽取可判定的 URL 字符串。
    支持:
      - list[dict]（Twitter 的 entities.*.urls[*]）
      - dict（单个 url 对象）
      - str（已是 URL）
    优先 expanded_url，其次 url，最后 display_url（必要时补 http://）。
    """
    if not url_entity:
        return
    if isinstance(url_entity, list):
        for obj in url_entity:
            if isinstance(obj, dict):
                u = obj.get("expanded_url") or obj.get("url") or obj.get("display_url")
                if u:
                    if isinstance(u, str) and not urlparse(u).scheme:
                        u = "http://" + u
                    yield u
            elif isinstance(obj, str):
                yield obj
        return
    if isinstance(url_entity, dict):
        u = url_entity.get("expanded_url") or url_entity.get("url") or url_entity.get("display_url")
        if u:
            if not urlparse(u).scheme:
                u = "http://" + u
            yield u
        return
    if isinstance(url_entity, str):
        yield url_entity

from urllib.parse import urlparse

def _normalize_host(host: str) -> str:
    host = (host or "").strip().lower()
    # 去常见前缀
    for p in ("www.", "m.", "mobile.", "amp."):
        if host.startswith(p):
            host = host[len(p):]
    return host

def _host_is(host: str, domain: str) -> bool:
    """host 是否等于或为某 domain 的子域"""
    return host == domain or host.endswith("." + domain)

def url_domain_category(url_str: str) -> str:
    """
    根据 URL 的域名/路径进行更丰富的场景分类。
    返回值之一：
      social, messaging, forum/community, qa, video/streaming, music/audio,
      blog/writing, news, encyclopedia, education/research, gov/ngo, finance,
      ecommerce, job/career, image/hosting, file/cloud, dev/code, maps/nav,
      forms/surveys, app_store, sports, link_aggregator, short_link,
      personal/other, unavailable
    """
    if not url_str or str(url_str).strip() in {"", "nan", "none", "null"}:
        return "unavailable"

    # 如果没有协议，补 http:// 以便正确解析
    raw = str(url_str).strip()
    parsed = urlparse(raw if "://" in raw else ("http://" + raw))
    host = _normalize_host(parsed.netloc)
    path = (parsed.path or "").lower()

    if not host:
        return "unavailable"

    # ---- 规则库（按类别分组）----
    SOCIAL = [
        "twitter.com", "x.com", "facebook.com", "instagram.com", "tiktok.com",
        "youtube.com", "reddit.com", "linkedin.com", "weibo.com", "zhihu.com",
        "douban.com", "pinterest.com", "snapchat.com", "kakao.com", "vk.com",
        "xiaohongshu.com"
    ]
    MESSAGING = [
        "whatsapp.com", "wa.me", "t.me", "telegram.me", "telegram.org",
        "weixin.qq.com", "wechat.com", "line.me", "signal.org", "messenger.com",
        "discord.com", "discord.gg", "skype.com"
    ]
    FORUM_COMMUNITY = [
        "discord.com", "discord.gg", "reddit.com", "quora.com", "clubhouse.com"
    ]
    QA = ["stackoverflow.com", "stackexchange.com", "superuser.com", "serverfault.com", "askubuntu.com", "quora.com"]
    VIDEO = ["twitch.tv", "vimeo.com", "dailymotion.com", "bilibili.com", "youku.com", "douyin.com", "netflix.com"]
    MUSIC = ["spotify.com", "soundcloud.com", "bandcamp.com", "music.apple.com", "podcasts.apple.com", "anchor.fm"]
    BLOG = [
        "medium.com", "substack.com", "wordpress.com", "wordpress.org", "blogspot.com",
        "tumblr.com", "hashnode.com", "dev.to", "mirror.xyz", "ghost.org", "notion.site", "notion.so"
    ]
    NEWS = [
        "nytimes.com", "washingtonpost.com", "wsj.com", "theguardian.com", "apnews.com",
        "npr.org", "bbc.co.uk", "bbc.com", "aljazeera.com", "bloomberg.com", "reuters.com",
        "ft.com", "forbes.com", "time.com", "latimes.com", "foxnews.com", "cnbc.com",
        "nbcnews.com", "abcnews.go.com", "news.yahoo.com", "techcrunch.com", "wired.com", "theverge.com", "engadget.com"
    ]
    ENCYC = ["wikipedia.org", "wikidata.org", "baike.baidu.com", "britannica.com"]
    EDU_RESEARCH = ["arxiv.org", "acm.org", "ieee.org", "nature.com", "science.org", "springer.com",
                    "sciencedirect.com", "ssrn.com", "researchgate.net"]
    GOV_NGO = ["un.org", "who.int", "oecd.org", "worldbank.org", "imf.org", "europa.eu"]
    FINANCE = ["paypal.com", "paypal.me", "venmo.com", "cash.app", "stripe.com",
               "coinbase.com", "binance.com", "kraken.com", "opensea.io"]
    ECOM = [
        "amazon.", "ebay.", "aliexpress.", "taobao.", "tmall.", "shopify", "etsy.",
        "jd.com", "pinduoduo.com", "mercadolibre.com", "rakuten.co.jp", "shopee."
    ]
    JOB = ["linkedin.com", "indeed.com", "glassdoor.com", "lever.co", "greenhouse.io"]
    IMG_HOST = ["imgur.com", "flickr.com", "pixiv.net", "deviantart.com", "500px.com", "smugmug.com"]
    FILE_CLOUD = ["drive.google.com", "docs.google.com", "dropbox.com", "box.com", "mega.nz",
                  "onedrive.live.com", "mediafire.com", "icloud.com"]
    DEV_CODE = ["github.com", "gist.github.com", "gitlab.com", "bitbucket.org", "pypi.org",
                "npmjs.com", "crates.io", "huggingface.co", "kaggle.com", "colab.research.google.com"]
    MAPS = ["maps.google.com", "goo.gl/maps", "openstreetmap.org", "map.baidu.com", "waze.com"]
    FORMS = ["forms.gle", "docs.google.com", "typeform.com", "surveymonkey.com", "jinshuju.net", "wj.qq.com"]
    APP_STORE = ["apps.apple.com", "itunes.apple.com", "play.google.com", "appgallery.huawei.com", "apkpure.com"]
    SPORTS = ["espn.com", "nba.com", "fifa.com", "uefa.com", "mlb.com", "nhl.com"]
    LINK_AGG = ["linktr.ee", "bio.link", "about.me", "carrd.co", "beacons.ai", "tap.bio", "lnk.bio", "allmylinks.com", "solo.to"]
    SHORT = ["t.co", "bit.ly", "tinyurl.", "goo.gl", "ow.ly", "buff.ly", "bitly.com", "rebrand.ly"]

    # ---- 顶级域启发 ----
    if host.endswith(".gov") or _host_is(host, "gov.uk"):
        return "gov/ngo"
    if host.endswith(".edu"):
        return "education/research"

    # ---- 路径启发（优先早判：maps / forms / google docs）----
    if _host_is(host, "google.com"):
        if path.startswith("/maps"):
            return "maps/nav"
        if path.startswith("/forms") or path.startswith("/forms/") or "/forms/" in path:
            return "forms/surveys"
        if path.startswith("/drive") or path.startswith("/file") or path.startswith("/doc") or _host_is(host, "docs.google.com"):
            return "file/cloud"

    # ---- 按类别列表匹配 ----
    def match_any(host: str, doms: list) -> bool:
        for d in doms:
            # 支持列表里既有完整域（github.com）也有前缀（amazon. / shopee.）
            if d.endswith(".") and d in host:
                return True
            if _host_is(host, d.rstrip(".")):
                return True
        return False

    if match_any(host, SHORT):         return "short_link"
    if match_any(host, SOCIAL):        return "social"
    if match_any(host, MESSAGING):     return "messaging"
    if match_any(host, FORUM_COMMUNITY): return "forum/community"
    if match_any(host, QA):            return "qa"
    if match_any(host, VIDEO):         return "video/streaming"
    if match_any(host, MUSIC):         return "music/audio"
    if match_any(host, BLOG):          return "blog/writing"
    if match_any(host, NEWS):          return "news"
    if match_any(host, ENCYC):         return "encyclopedia"
    if match_any(host, EDU_RESEARCH):  return "education/research"
    if match_any(host, GOV_NGO):       return "gov/ngo"
    if match_any(host, FINANCE):       return "finance"
    if match_any(host, ECOM):          return "ecommerce"
    if match_any(host, JOB):           return "job/career"
    if match_any(host, IMG_HOST):      return "image/hosting"
    if match_any(host, FILE_CLOUD):    return "file/cloud"
    if match_any(host, DEV_CODE):      return "dev/code"
    if match_any(host, MAPS):          return "maps/nav"
    if match_any(host, FORMS):         return "forms/surveys"
    if match_any(host, APP_STORE):     return "app_store"
    if match_any(host, SPORTS):        return "sports"
    if match_any(host, LINK_AGG):      return "link_aggregator"

    return "personal/other"


def url_domain_categories(url_entity):
    """
    接受 url_entity（list[dict]/dict/str），返回多个类别 list（按出现次数降序，其次按优先级）：
      ['social','news','encyclopedia','blog/writing','ecommerce','personal/other','short_link','unavailable']
    规则：
      1) 多链接投票计数
      2) 若存在非 short_link 类别，忽略 short_link
      3) 同票时按优先级挑选顺序
    """
    urls = list(_iter_urls_from_entity(url_entity))
    if not urls:
        return ['unavailable']

    counts = {}
    for u in urls:
        try:
            host = urlparse(u).netloc
        except Exception:
            host = ''
        cat = url_domain_category(host)
        counts[cat] = counts.get(cat, 0) + 1

    if not counts:
        return ['unavailable']

    # 优先忽略短链
    non_short = {k: v for k, v in counts.items() if k != 'short_link'}
    use_counts = non_short if non_short else counts

    priority = ['social', 'news', 'encyclopedia', 'blog/writing', 'ecommerce', 'personal/other', 'short_link', 'unavailable']

    # 排序规则：先按出现次数降序，再按优先级顺序
    sorted_cats = sorted(use_counts.items(), key=lambda kv: (-kv[1], priority.index(kv[0]) if kv[0] in priority else len(priority)))
    return [k for k, _ in sorted_cats]

def str_similarity(a: str, b: str) -> float:
    # 简易相似度：共同字符的 Jaccard；可替换为编辑距离
    if not a or not b:
        return 0.0
    sa, sb = set(a.lower()), set(b.lower())
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0

def lang_hint_from_fields(profile_lang_field, description_text):
    """
    从个人资料语言字段和描述文本推断语言
    
    Args:
        profile_lang_field: 个人资料中的语言字段
        description_text: 个人简介文本
        
    Returns:
        str: 语言代码（zh表示中文，en表示英文）
    """
    if profile_lang_field and str(profile_lang_field).strip():
        return str(profile_lang_field).strip().lower()
    if re.search(r'[\u4e00-\u9fff]', description_text or ''):  # 检测中文字符
        return 'zh'
    return 'en'

def safe_int(x):
    """
    安全地将值转换为整数
    处理None值、空字符串和转换异常
    
    Args:
        x: 输入值
        
    Returns:
        int: 转换后的整数，如果转换失败则返回None
    """
    try:
        if x is None or str(x).strip() == '': return 0
        return int(float(x))
    except Exception:
        return 0

def safe_div(a, b):
    """
    安全的除法运算
    处理None值和除零错误
    
    Args:
        a: 被除数
        b: 除数
        
    Returns:
        float: 除法结果，如果出错则返回0.0
    """
    try:
        fa = 0.0 if a is None else float(a)
        fb = 0.0 if b is None else float(b)
        return fa / fb if fb > 0 else 0.0
    except Exception:
        return 0.0

def parse_bool(x):
    """
    解析布尔值字符串
    支持多种布尔值表示形式：1/true/yes/y/t 和 0/false/no/n/f
    
    Args:
        x: 输入值，可能是字符串、数字或None
        
    Returns:
        bool: 解析后的布尔值，如果无法解析则返回None
    """
    if x is None: return None
    s = str(x).strip().lower()
    if s in {'1','true','yes','y','t'}: return True
    if s in {'0','false','no','n','f'}: return False
    return None

def clean_text(s):
    """
    清理文本内容
    移除控制字符，规范化空白字符
    
    Args:
        s: 输入文本
        
    Returns:
        str: 清理后的文本
    """
    if s is None: 
        return ''
    s = re.sub(r'[\x00-\x1f\x7f]', ' ', str(s))  # 移除控制字符
    s = re.sub(r'\s+', ' ', s).strip()            # 规范化空白字符
    return s

def name_stats(s):
    """
    Return (name_length, digit_ratio, special_char_ratio) for a display name.
    - digit_ratio: digits / length
    - special_char_ratio: non-alnum and non-space / length
    """
    txt = clean_text(s or "")
    if not txt: return 0, 0.0, 0.0
    length = len(txt)
    digits = sum(ch.isdigit() for ch in txt)
    specials = sum((not ch.isalnum()) and (not ch.isspace()) for ch in txt)
    return length, digits / max(1, length), specials / max(1, length)

def screen_name_stats(s):
    """
    Return (screen_name_length, digit_ratio, underscore_ratio) for a screen name.
    - digit_ratio: digits / length
    - underscore_ratio: underscores / length
    """
    txt = clean_text(s or "")
    if not txt: return 0, 0.0, 0.0
    length = len(txt)
    digits = sum(ch.isdigit() for ch in txt)
    underscores = txt.count('_')
    return length, digits / max(1, length), underscores / max(1, length)

def compute_lang_timezone_mismatch(lang: str, time_zone: str, utc_offset):
    """
    Compute whether the language and timezone mismatch.
    """
    lang = (lang or '').lower().strip()
    tz = (time_zone or '').lower()
    # 这里给一个简单可解释的规则示例：en + 印度时区/新德里
    if lang == 'en':
        try:
            off = int(utc_offset) if utc_offset not in (None, '', 'nan', 'NaN') else None
        except Exception:
            off = None
        if ('delhi' in tz) or (off in {19800, 20700}):
            return True
    return False

def extract_profile_features(user):
    # 获取基础字段
    # 提取数值类型特征
    followers_count = safe_int(user.get("followers_count")) # 关注者数量
    friends_count = safe_int(user.get("friends_count")) # 朋友数量
    statuses_count = safe_int(user.get("statuses_count")) # 推文数量
    favourites_count = safe_int(user.get("favourites_count")) # 收藏数量
    listed_count = safe_int(user.get("listed_count")) # 列表数量
    ff_ratio = safe_div(followers_count, friends_count)

    # 提取布尔型特征
    verified = parse_bool(user.get("verified")) # 是否认证
    protected = parse_bool(user.get("protected")) # 是否保护
    default_profile_image = parse_bool(user.get("default_profile_image")) # 是否使用默认头像
    default_profile = parse_bool(user.get("default_profile")) # 是否使用默认个人资料
    geo_enabled = parse_bool(user.get("geo_enabled")) # 是否启用地理定位

    # 分析个人简介文本
    description = clean_text(user.get("description")) # 个人简介文本
    desc_length = len(description) if len(description) > 0 else 0 # 个人简介文本长度
    emoji_count = count_emoji(description) if len(description) > 0 else 0 # 个人简介文本中的表情符号数量
    has_mention_in_desc = bool(re.search(r'@\w+', description)) if len(description) > 0 else False
    url_entity_desc = user.get("entities", {}).get("description", {}).get("urls", []) # 个人简介文本中的URL
    has_url_in_desc = bool(url_entity_desc) if len(description) > 0 else False # 是否包含URL
    has_hashtag_in_desc = bool(re.search(r'#\w+', description)) if len(description) > 0 else False # 是否包含话题标签
    has_email_in_desc = bool(EMAIL_RE.search(description)) if len(description) > 0 else False # 是否包含邮箱
    has_phone_in_desc = bool(PHONE_RE.search(description)) if len(description) > 0 else False # 是否包含电话
    promo_keywords = ['business', 'advertising', 'promo', 'dm', '合作', '商务']
    has_promo_keyword_in_desc = any(k in description.lower() for k in promo_keywords) if len(description) > 0 else False # 是否包含推广关键词
    url_category_desc =  url_domain_categories(url_entity_desc) if url_entity_desc else [] # 简介文本中的URL分类

    # bio 中的URL  
    bio_urls = user.get("entities", {}).get("url", {}).get("urls", []) # bio 中的URL
    has_url_in_bio = bool(bio_urls) if len(bio_urls) > 0 else False # 是否包含URL
    url_category_bio =  url_domain_categories(bio_urls) if bio_urls else [] # bio 中的URL分类

    # 分析账户创建时间
    created_at = user.get("created_at")
    name = user.get("name")
    name_length, name_digit_ratio, name_special_char_ratio = name_stats(name)
    # screen_name 特征
    screen_name = user.get("screen_name") or ""
    sn_len, sn_digit_ratio, sn_underscore_ratio = screen_name_stats(screen_name)

    # name vs screen_name 相似度
    name_sn_similarity = str_similarity(name, screen_name) if (name and screen_name) else 0.0

    
    # 语言/时区/地点一致性与泛化地点
    lang_hint = lang_hint_from_fields(user.get("lang"), description)
    location_present = bool(user.get("location") and str(user.get("location")).strip() != '')
    profile_banner_url_present = bool(user.get("profile_banner_url") and str(user.get("profile_banner_url")).strip() != '')
    profile_use_background_image = bool(user.get("profile_use_background_image") and str(user.get("profile_use_background_image")).strip() != '')
    profile_background_tile = bool(user.get("profile_background_tile") and str(user.get("profile_background_tile")).strip() != '')

    time_zone_present = bool(user.get("time_zone") and str(user.get("time_zone")).strip() != '') # 是否存在时区
    utc_offset = user.get("utc_offset")  # 例如 19800=UTC+5:30
    utc_offset_present = not (utc_offset in (None, '', 'nan', 'NaN')) # 是否存在时区偏移量
    lang_timezone_mismatch = compute_lang_timezone_mismatch(user.get("lang"), user.get("time_zone"), user.get("utc_offset")) # 语言/时区/地点一致性与泛化地点

    location_generic_flag = (str(user.get('location') or '').strip().lower() in GENERIC_LOCATIONS) if location_present else False # 地点是否为通用/非地理名

    profile_features = {
        # ===== 原始特征（direct from user JSON）=====
        "followers_count": followers_count,                    # 原始
        "friends_count": friends_count,                        # 原始
        "statuses_count": statuses_count,                      # 原始
        "favourites_count": favourites_count,                  # 原始
        "listed_count": listed_count,                          # 原始
        "verified": verified,                                  # 原始
        "protected": protected,                                # 原始
        "default_profile_image": default_profile_image,        # 原始
        "default_profile": default_profile,                    # 原始
        "geo_enabled": geo_enabled,                            # 原始
        "lang_hint": lang_hint,                                # 原始（user.lang）
        "created_at": created_at,                              # 原始（user.created_at）
        "location_present": location_present,        # 原始（存在性）
        "profile_banner_url_present": profile_banner_url_present,   # 原始（存在性）
        "profile_use_background_image": profile_use_background_image,  # 原始
        "profile_background_tile": profile_background_tile, # 原始
        "time_zone_present": time_zone_present,      # 原始（存在性）
        "utc_offset_present": utc_offset_present,          # 原始（存在性）

        # ===== 衍生特征（computed/parsed/regex/组合）=====
        "ff_ratio": ff_ratio,                                  # 衍生 = followers_count / friends_count
        "desc_length": desc_length,                            # 衍生 = len(description)
        "emoji_count": emoji_count,                            # 衍生 = regex/范围统计
        "has_url_in_desc": has_url_in_desc,                      # 衍生 = bool(entities.description.urls)
        "has_mention_in_desc": has_mention_in_desc,              # 衍生 = 正则 @\w+
        "has_hashtag_in_desc": has_hashtag_in_desc,              # 衍生 = 正则 #\w+
        "has_email_in_desc": has_email_in_desc,                  # 衍生 = 正则 email
        "has_phone_in_desc": has_phone_in_desc,                  # 衍生 = 正则 phone
        "has_promo_keyword_in_desc": has_promo_keyword_in_desc,              # 衍生 = 关键词匹配（business/advertising/…）
        "url_category_desc": url_category_desc,# 衍生 = 按域名规则归类（可由 url_entity 得出）
        "has_url_in_bio": has_url_in_bio,                      # 衍生 = bool(entities.description.urls)
        "url_category_bio": url_category_bio,# 衍生 = 按域名规则归类（可由 url_entity 得出）
        "name_length": name_length,                            # 衍生 = len(name)
        "name_digit_ratio": name_digit_ratio,                  # 衍生 = 数字占比
        "name_special_char_ratio": name_special_char_ratio,    # 衍生 = 特殊字符占比

        "screen_name_length": sn_len,                          # 衍生 = len(screen_name)
        "screen_name_digit_ratio": sn_digit_ratio,             # 衍生 = 数字占比
        "screen_name_underscore_ratio": sn_underscore_ratio,   # 衍生 = 下划线占比
        "name_screen_name_similarity": name_sn_similarity,     # 衍生 = 相似度度量（Jaccard/编辑距离等）
        "lang_timezone_mismatch": lang_timezone_mismatch,      # 衍生 = 语言 vs 时区/偏移不一致
        "location_generic_flag": location_generic_flag         # 衍生 = 地点是否为通用/非地理名
    }


    # 生成 missing 标志
    # profile_features_missing = {
    #     f"{k}_missing": v is None for k, v in profile_features.items()
    # }
    profile_features_missing = {
                "followers_count_missing": followers_count == 0,
                "friends_count_missing": friends_count == 0,
                "statuses_count_missing": statuses_count == 0,
                "favourites_count_missing": favourites_count == 0,
                "listed_count_missing": listed_count == 0,
                "ff_ratio_missing": (followers_count == 0 or friends_count == 0),

                "verified_missing": verified is None,
                "protected_missing": protected is None,
                "default_profile_image_missing": default_profile_image is None,
                "default_profile_missing": default_profile is None,
                "geo_enabled_missing": geo_enabled is None,

                # desc 为空视为这些不可判定
                "desc_length_missing": desc_length == 0,
                "emoji_count_missing": emoji_count == 0,
                "has_url_in_desc_missing": has_url_in_desc == False,
                "has_mention_in_desc_missing": has_mention_in_desc == False,
                "has_hashtag_in_desc_missing": has_hashtag_in_desc == False,
                "has_email_in_desc_missing": has_email_in_desc == False,
                "has_phone_in_desc_missing": has_phone_in_desc == False,
                "has_promo_keyword_in_desc_missing": has_promo_keyword_in_desc == False,
                "url_category_desc_missing": len(url_category_desc) == 0,

                "has_url_in_bio_missing": has_url_in_bio == False,
                "url_category_bio_missing": len(url_category_bio) == 0,
                "lang_hint_missing": lang_hint is None,

                "created_at_missing": created_at is None,
                "name_length_missing": (name_length == 0),
                "name_digit_ratio_missing": (name_digit_ratio == 0),            # 若 name 为空，比例无意义
                "name_special_char_ratio_missing": (name_special_char_ratio == 0), # 若 name 为空，比例无意义
                "screen_name_length_missing": (sn_len == 0),
                "screen_name_digit_ratio_missing": (sn_digit_ratio == 0), # 若 screen_name 为空，比例无意义
                "screen_name_underscore_ratio_missing": (sn_underscore_ratio == 0), # 若 screen_name 为空，比例无意义
                "name_screen_name_similarity_missing": (sn_len == 0 or name_length == 0), # 若 screen_name 或 name 为空，相似度无意义

                # presence 源字段缺失才算 missing
                "location_present_missing": location_present == False,
                "profile_banner_url_present_missing": profile_banner_url_present == False,
                "profile_use_background_image_missing": profile_use_background_image == False,
                "profile_background_tile_missing": profile_background_tile == False,
                "time_zone_present_missing": time_zone_present == False,
                "utc_offset_present_missing": utc_offset_present == False,
                "lang_timezone_mismatch_missing": lang_timezone_mismatch == True,
                "location_generic_flag_missing": location_generic_flag == False,
            }

    return profile_features, profile_features_missing

def process_user_file(input_path: Path, output_path: Path):
    """
    根据输入文件的扩展名，读取用户数据并提取特征。
    """
    output_dict = {}
    input_suffix = input_path.suffix.lower()

    # --- 分支一：处理 JSON 文件 ---
    if input_suffix == '.json':
        print(f"检测到 JSON 文件，开始处理: {input_path.name}")
        with open(input_path, "r", encoding="utf-8") as f:
            data_list = json.load(f)

        for entry in data_list:
            user = entry.get("user", entry) # 兼容两种JSON结构
            user_id = str(user.get("id_str") or user.get("id"))
            if not user_id or user_id == 'None':
                continue
            
            # 直接调用核心函数
            pf, pf_missing = extract_profile_features(user)
            output_dict[user_id] = {
                "profile_features": pf,
                "profile_features_missing": pf_missing
            }

    # --- 分支二：处理 CSV 文件 (新增逻辑) ---
    elif input_suffix == '.csv':
        print(f"检测到 CSV 文件，开始处理: {input_path.name}")
        with open(input_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = str(row.get('id') or row.get('user_id') or row.get('uid'))
                if not user_id or user_id == 'None':
                    continue

                # 定义需要特殊处理的布尔字段列表
                bool_fields_to_fix = [
                    "verified", "protected", "default_profile_image",
                    "default_profile", "geo_enabled"
                ]

                for field in bool_fields_to_fix:
                    # 从 row 中获取原始值 (可能是 '1', '0', '' 或 None)
                    original_value = row.get(field)
                    
                    # 安全地转换为小写字符串进行判断
                    s = str(original_value or '').strip().lower()
                    
                    if s in {'1', 'true', 'yes', 'y', 't'}:
                        # 如果是明确的 "真"，将 row 中对应的值直接覆盖为 Python 的 True
                        row[field] = True
                    else:
                        # 对于CSV，其他所有情况 (包括'', '0', 'false', None) 都覆盖为 Python 的 False
                        row[field] = False

                # **创建适配层：将CSV行(row)转换为user_obj字典**
                user_obj = dict(row) # 复制所有扁平数据
                
                # 手动构建CSV所缺少的`entities`结构
                entities = {"description": {"urls": []}, "url": {"urls": []}}
                
                # 从文本中提取URL并填充到entities中
                desc_text = row.get('description', '')
                if desc_text:
                    desc_urls = re.findall(r'https?://\S+', desc_text)
                    entities['description']['urls'] = [{'expanded_url': u} for u in desc_urls]

                bio_url = row.get('url', '')
                if bio_url:
                    entities['url']['urls'] = [{'expanded_url': bio_url}]
                
                user_obj['entities'] = entities
                # **适配层结束**

                # 调用同一个核心函数
                pf, pf_missing = extract_profile_features(user_obj)
                output_dict[user_id] = {
                    "profile_features": pf,
                    "profile_features_missing": pf_missing
                }

    # --- 处理未知文件类型 ---
    else:
        print(f"[错误] 不支持的文件类型: '{input_suffix}'。只支持 .json 和 .csv 文件。")
        return

    # --- 统一的写出逻辑 ---
    output_path.parent.mkdir(parents=True, exist_ok=True) # 确保输出目录存在
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_dict, f, ensure_ascii=False, indent=2)
    print(f"[成功] 处理了 {len(output_dict)} 个用户，结果已保存到: {output_path.name}\n")


if __name__ == "__main__":
    """
    主程序入口，负责解析命令行参数并调用处理器。
    """
    parser = argparse.ArgumentParser(description="从JSON或CSV文件中提取用户画像特征。")
    parser.add_argument("--input", required=True, help="输入的 .json 或 .csv 文件路径")
    parser.add_argument("--output", required=True, help="输出的 .json 文件路径")
    args = parser.parse_args()

    input_file = Path(args.input)
    output_file = Path(args.output)

    if not input_file.exists():
        print(f"[错误] 输入文件不存在: {input_file}")
    else:
        process_user_file(input_file, output_file)