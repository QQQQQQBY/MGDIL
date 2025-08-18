# MGDIL

# 1 Process Original Dataset

## 1.1 Original Dataset Info

| Dataset                     | year | number   | profile | tweets | retweet | post | quote | Graph | annotation                                   | label reliablity | human  | bot    | Notes                                                                                                                                                                                                 |
|-----------------------------|------|----------|---------|--------|---------|------|-------|-------|----------------------------------------------|------------------|--------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Botometer-Feedback-2019     | 2019 | 529      | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | manually labeled                             | High             | 386    | 143    | 
| botwiki-2019                | 2019 | 704      | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | self-identified bot accounts                 | High             | 0      | 704    | 
| celebrity-2019              | 2019 | 5970     | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | CNets Team                                   | High             | 5970   | 0      | 
| cresci-rtbust-2019          | 2019 | 759      | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | manually labeled                             | High             | 368    | 391    | 
| cresci-stock-2018           | 2017 | 25987    | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | behavior pattern detection+manual verfication | High             | 18508  | 7479   | 
| gilani-2017                 | 2017 | 2652     | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | manually labeled                             | High             | 1130   | 1522   | 
| political-bots-2019         | 2019 | 62       | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | @josh emerson provided                       | High             | 0      | 62     | 
| vender-purchased-2019       | 2019 | 1088     | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | fake followers                               | High             | 0      | 1088   |
| verified-2019               | 2019 | 2000     | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | Verified human accounts                      | High             | 2000   | 0      | 
| midterm-2018                | 2018 | 50538    | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | Manual annotation + External validation      | High             | 8092   | 42446  | 
| Social Honeypot 2011        | 2011 | 41455    | ✅       | ✅      | ✅       | ✅    | ❌     | ❌     | honeypot twitter accounts                    | High             | 19276  | 22179  | 
| pronbots-2019               | 2019 | 21,964   | ✅       | ❌      | ❌       | ❌    | ❌     | ❌     | Automatically identified through its sharing behavior |                  | 0      | 21,964 | 
| Cresci-2015                 | 2015 | 5,301    | ✅       | ✅      | ✅       | ✅    | ❌     | ✅     | purchased                                    |                  | 1950   | 3351   | 
| Cresci-2017                 | 2017 | 10017    | ✅       | ✅      | ✅       | ✅    | ❌     | ❌     | Manual annotation + External validation      |                  | 3474   | 6543   | 
| Twibot-20                   | 2020 | 10,587   | ✅       | ✅      | ✅       | ✅    | ❌     | ✅     | Manual annotation                            |                  | 5,389  | 5,198  | 
| Fox8-2023                   | 2023 | 2280     | ✅       | ✅      | ✅       | ✅    | ❌     | ❌     | spontaneously exposed                        |                  | 1,140  | 1,140  | 

## 1.2 Processed Dataset Style

   Run `python DatasetProcessCode/extract_profile_features.py` to process **Profile Features**

   Run `DatasetProcessCode/process_tweets_to_json.py` to process **Text Features**

Processed Dataset will be stored into json format.

**Profile Json Format:**

```json
[
  "profile_features": {
    "followers_count": 790,
    "friends_count": 3218,
    "statuses_count": 6252,
    "favourites_count": 12898,
    "listed_count": 42,
    "verified": false,
    "protected": false,
    "default_profile_image": false,
    "default_profile": false,
    "geo_enabled": true,
    "lang_hint": "en",
    "created_at": "Thu Jun 07 22:16:27 +0000 2012",
    "location_present": true,
    "profile_banner_url_present": true,
    "profile_use_background_image": false,
    "profile_background_tile": false,
    "time_zone_present": true,
    "utc_offset_present": true,
    "ff_ratio": 0.24549409571162212,
    "desc_length": 151,
    "emoji_count": 2,
    "has_url_in_desc": false,
    "has_mention_in_desc": true,
    "has_hashtag_in_desc": true,
    "has_email_in_desc": false,
    "has_phone_in_desc": false,
    "has_promo_keyword_in_desc": false,
    "url_category_desc": [],
    "has_url_in_bio": true,
    "url_category_bio": [
      "personal/other"
    ],
    "name_length": 22,
    "name_digit_ratio": 0.0,
    "name_special_char_ratio": 0.5,
    "screen_name_length": 10,
    "screen_name_digit_ratio": 0.0,
    "screen_name_underscore_ratio": 0.0,
    "name_screen_name_similarity": 0.4,
    "lang_timezone_mismatch": false,
    "location_generic_flag": false
  }
]
```

**Text Info Json Format**

```json
{
        "tweet_id": "299538301315063808",
        "created_at_iso": "2013-02-07T15:21:10+00:00",
        "hour_of_day": 15,
        "day_of_week": "Thu",
        "client_source": "web",
        "is_retweet": false,
        "is_reply": true,
        "len_chars": 48,
        "num_hashtags": 0,
        "num_urls": 0,
        "num_mentions": 1,
        "retweet_count": 0,
        "reply_count": 0,
        "favorite_count": 0,
        "urls": [],
        "url_domains": [],
        "text": "@thulme cool drop me your email so we coordiante"
      }
```

## 1.3 Remove Repeated User IDs

Remove duplicate ids from older years and keep duplicates if they appear in the latest year

Run `python DatasetProcessCode/remove_repeat_id.py` to remove repeated ids

## 1.4 Filter JSON Files Across Different Datasets

Filter JSON files in different datasets, and the intention of this process is to prevent some repeated ids appearing in test set.

Run `python DatasetProcessCode/filter_json_files.py` to filter files.