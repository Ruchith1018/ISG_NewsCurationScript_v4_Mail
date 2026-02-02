# Utility Functions for News Scraping and Processing
# ===================================================

import json
import time
import uuid
import re
import numpy as np
import pandas as pd
import requests
import boto3
from botocore.config import Config
from sklearn.metrics.pairwise import cosine_similarity
from typing import List
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse
import os
from datetime import datetime, timedelta

from eventregistry import QueryArticlesIter, QueryItems, ReturnInfo, ArticleInfoFlags

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config import *
from constants import COMPANY_ALIASES, BLOG_PLATFORMS
from validation import clean_escapes
# =====================================================
# BEDROCK CLIENT
# =====================================================

def get_bedrock_client(region: str = BEDROCK_REGION):
    return boto3.client(
        "bedrock-runtime",
        region_name=region,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=Config(retries={"max_attempts": 5, "mode": "standard"})
    )

# =====================================================
# STRICT TOOL JSON VALIDATION
# =====================================================

def strict_tool_json(response_body, required_keys, tool_name):
    content = response_body.get("content", [])
    if not content or "input" not in content[0]:
        raise ValueError(f"{tool_name}: missing tool input")

    inputs = content[0]["input"]
    missing = [k for k in required_keys if k not in inputs]
    if missing:
        raise ValueError(f"{tool_name}: missing keys {missing}")

    return inputs


# -----------------------------
# Date Range Configuration
# -----------------------------
def get_date_range(days_back=3):
    to_dt = datetime.utcnow()
    from_dt = to_dt - timedelta(days=days_back)
    
    # Set from_dt to start of the day (00:00:00 UTC)
    from_dt = from_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Set to_dt to end of previous day
    to_dt_l = to_dt - timedelta(days=0) # days=0 if days_back==0 else 1
    to_dt = to_dt_l.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return from_dt, to_dt

def load_category_data(mode=None):
    """
    Load category data filtered by supplier_location mode.
    
    Args:
        mode (str): Either 'supplier' or 'location'. If None, loads all categories.
        
    Returns:
        tuple: (category_df, categories_enum) filtered by mode
    """
    category_df = run_query("""SELECT * FROM categories""")
    category_df = category_df[['category_name', 'risk_rating', 'supplier_location']]
    
    # Filter by mode if specified
    if mode:
        category_df = category_df[category_df['supplier_location'] == mode].copy()
        print(f"Loaded {len(category_df)} categories for mode: {mode}")
    
    categories_enum = category_df['category_name'].unique().tolist()
    return category_df, categories_enum

def load_locations_data():
    loc_df = run_query("""SELECT * FROM locations""")
    loc_enum = loc_df['location'].unique().tolist()
    return loc_df, loc_enum

# =====================================================
# FETCH NEWS
# =====================================================
def fetch_news_articles(er, from_dt, to_dt, companies, max_items, language, data_types, category, supplier_location):
    all_articles = []
    if supplier_location == 'supplier':

        for company in companies:
            print(f"\n🔍 Fetching news for {company}")

            company_uri = er.getConceptUri(company)
            category_uri = er.getCategoryUri(category)

            if company_uri:
                query = QueryArticlesIter(
                    conceptUri=company_uri,
                    # keywords=QueryItems.OR([company]),
                    # keywordsLoc="title",
                    categoryUri=category_uri,
                    dateStart=from_dt,
                    dateEnd=to_dt,
                    lang=language,
                    dataType=data_types,
                )
            else:
                query = QueryArticlesIter(
                    keywords=QueryItems.OR([company]),
                    # keywordsLoc="title",
                    categoryUri=category_uri,
                    dateStart=from_dt,
                    dateEnd=to_dt,
                    lang=language,
                    dataType=data_types,
                )

            return_info = ReturnInfo(
                articleInfo=ArticleInfoFlags(title=True, body=True, location=True)
            )

            for art in query.execQuery(er, sortBy="date", maxItems=max_items, returnInfo=return_info):
                all_articles.append({
                    "company": company,
                    "company_name": company,
                    "date": art.get("dateTimePub"),
                    "headline": art.get("title"),
                    "content": art.get("body"),
                    "url": art.get("url"),
                    "lang": art.get("lang"),
                    "cutoff_time": datetime.now(),
                })

            print(f"✅ Done for {company}")

    elif supplier_location == 'location':

        for company in companies:
            print(f"\n🌍 Fetching news for: {company}")

            location_uri = er.getLocationUri(company)

            q = QueryArticlesIter(
                locationUri=location_uri,
                keywords=QueryItems.OR(EVENT_KEYWORDS),
                lang="eng",
                dateStart=from_dt,
                dateEnd=to_dt
            )

            art_count = 0
            for art in q.execQuery(er, sortBy="date", maxItems=max_items):
                all_articles.append({
                    "company": company,
                    "company_name": company,
                    "date": art.get("dateTimePub", ""),
                    "headline": art.get("title", ""),
                    "content": art.get("body", ""),
                    "url": art.get("url", ""),
                    "lang": art.get("lang", ""),
                    "cutoff_time": datetime.now()
                })
                art_count += 1
            print(f"✅ Done for {company}:{art_count} articles")
        
        print(f"✅ {len(all_articles)} articles")

    return all_articles


# =====================================================
# BLOG LINK DETECTION
# =====================================================

def extract_blog_links(df, column_name="url", blog_platforms=None):
    if blog_platforms is None:
        blog_platforms = BLOG_PLATFORMS

    def is_blog(url):
        try:
            domain = urlparse(url).netloc.lower()
            return any(domain.endswith(p) for p in blog_platforms)
        except Exception:
            return False

    df = df.copy()
    df["blogpost links"] = df[column_name].apply(lambda x: x if is_blog(x) else "NO")
    return df

# =====================================================
# COMPANY VALIDATION
# =====================================================

def validate_company_in_headline(df, company_aliases=None):
    if company_aliases is None:
        company_aliases = COMPANY_ALIASES

    def check(row):
        headline = (row["headline"] or "").lower()
        company = (row["company"] or "").lower()
        # print("company: ------", company)

        if row["company"] in company_aliases:
            return "Yes" if any(a.lower() in headline for a in company_aliases[row["company"]]) else "No"
        return "Yes" if company in headline else "No"

    df = df.copy()
    df["valid"] = df.apply(check, axis=1)
    return df

# =====================================================
# SAVE TO EXCEL
# =====================================================

def save_to_excel(df, base_folder, filename):
    base_folder = Path(base_folder)
    base_folder.mkdir(parents=True, exist_ok=True)

    path = base_folder / filename
    df.to_excel(path, index=False)

    return str(path)

# =====================================================
# EMBEDDINGS
# =====================================================

def truncate_text(text: str, max_tokens: int = 7000) -> str:
    return text[: max_tokens * 4]

def titan_embed_texts(texts: List[str]) -> np.ndarray:
    client = get_bedrock_client()
    vectors = []

    for t in texts:
        payload = {"inputText": truncate_text(t)}
        r = client.invoke_model(
            modelId=TITAN_EMBED_MODEL_ID,
            body=json.dumps(payload),
            contentType="application/json"
        )
        body = json.loads(r["body"].read())
        vectors.append(body["embedding"])

    return np.array(vectors, dtype=np.float32)

# =====================================================
# DEDUPLICATION
# =====================================================

def deduplicate_by_cosine_bedrock(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    results = []

    for (company, date), grp in df.groupby(["company", "date"], sort=False):
        emb = titan_embed_texts(grp["headline"].fillna("").tolist())
        sim = cosine_similarity(emb)

        cluster = [-1] * len(grp)
        cid = 0

        for i in range(len(grp)):
            if cluster[i] != -1:
                continue
            idxs = np.where(sim[i] >= DUP_THRESHOLD)[0]
            for j in idxs:
                cluster[j] = cid
            cid += 1

        grp = grp.copy()
        grp["dup_group_id"] = [f"{company}_{date}_{c}" for c in cluster]
        grp["is_duplicate"] = grp.duplicated("dup_group_id", keep="first").map(
            {True: "Yes", False: "No"}
        )

        results.append(grp)

    return pd.concat(results).sort_index()
# =====================================================
# CLASSIFICATION (THREAD SAFE)
# =====================================================

def get_news_classifications(news_article, news_enum, bedrock_client, max_retries=MAX_RETRIES):
    """Call Bedrock API with retry logic for throttling errors."""
    tools = [
        {
            "name": "classify_news",
            "description": "Classify the news article only in one category and reason it and provide relevance score.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "classification": {
                        "type": "string",
                        "description": "Classify the given news article into one category from the provided enum list.",
                        "enum": news_enum,
                    },
                    "cat_reason": {
                        "type": "string",
                        "description": "Explain why the selected category best fits the article (max one sentences).",
                    },
                    "relevance":{
                        "type": "integer",
                        "description": "Provide a numeric relevance score between 0 and 1 with one decimal place indicating how strongly the article aligns with the chosen category (0 = not relevant, 1 = highly relevant).",
                    }
                },
                "required": ["classification", "cat_reason", "relevance"],
            },
        }
    ]

    prompt = """You are a precise and objective news classification assistant. 
Read the given article and classify it into one of the provided enum categories.
Return JSON strictly as:
{
  'classification': 'one category from enum list',
  'cat_reason': 'why this category best fits (max one sentences).',
  'relevance': 'Provide a numeric relevance score (example:0.6, minimum:0, maximum:1)'
}
"""

    messages = [
        {"role": "user", "content": f"The following is the news article: {news_article}."}
    ]

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": prompt,
        "messages": messages,
        "tools": tools,
        "tool_choice": {"type": "tool", "name": "classify_news"},
        "max_tokens": 1000,
        "temperature": 0,
        "top_p": 1,
    }

    for attempt in range(max_retries):
        try:
            response = bedrock_client.invoke_model(
                modelId=CLAUDE_MODEL_ID,
                body=json.dumps(body)
            )
            response_body = json.loads(response['body'].read())

            # print("Classification============", response_body)

            classification = response_body['content'][0]['input']['classification']
            cat_reason = response_body['content'][0]['input']['cat_reason']
            relevance = response_body['content'][0]['input']['relevance']

            return classification, cat_reason, relevance
        except Exception as e:
            err_msg = str(e)
            # If throttled, wait and retry
            if "ThrottlingException" in err_msg or "Too many tokens" in err_msg:
                wait_time = 2 ** attempt  # exponential backoff: 1s, 2s, 4s
                print(f"Throttled on attempt {attempt+1}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                print(f"Error processing article: {e}")
                break
    return None, None, None

def _classify_worker(idx, row, news_enum, cat_df, bedrock_client):
    try:
        article = f"{row['headline']} {row.get('content','')}"
        cls, cat_reason, relevance = get_news_classifications(article, news_enum, bedrock_client)

        rr = None
        if cls:
            r = cat_df.loc[cat_df["category_name"] == cls, "risk_rating"]
            rr = r.iloc[0] if not r.empty else None

        return idx, cls, rr, cat_reason, relevance
    except Exception:
        return idx, None, None, None, relevance

def classify_articles(df, news_enum, category_data_df, bedrock_client):
    df = df.copy()

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [
            ex.submit(_classify_worker, i, row, news_enum, category_data_df, bedrock_client)
            for i, row in df.iterrows()
        ]

        for f in tqdm(as_completed(futures), total=len(futures), desc="Classifying"):
            idx, cls, rr, cat_reason, relevance = f.result()
            df.loc[idx, ["classification", "risk_rating", "cat_reason", "relevance"]] = [cls, rr, cat_reason, float(relevance) if relevance is not None else None]

    return df

# =====================================================
# QC TAGGING
# =====================================================

def tag_news_article(news_article, qc_tags, bedrock_client, max_retries=MAX_RETRIES):
    """Call Bedrock model to tag the article with retry logic and throttling handling."""
    tools = [
        {
            "name": "tag_news",
            "description": "Identify all quality control (QC) issues from the given list that apply to this article and explain why.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "tags": {
                        "type": "string", 
                        "enum": qc_tags,
                        "description": "Comma separated list of all QC tags from the provided list that best describe problems in this article.",
                    },
                    "tag_reason": {
                        "type": "string",
                        "description": "Explain briefly why these QC tags were assigned in max one sentences.",
                    },
                },
                "required": ["tags", "tag_reason"],
            },
        }
    ]

    prompt = """You are a strict and objective QC tagging assistant.
Your job is to identify quality control issues in a news article.
Choose one or more tags ONLY from the given list and explain why briefly.

Response format:
{
  "tags": "zero or more comma separated tags from list",
  "tag_reason": "brief justification in max one sentences"
}
"""

    messages = [
        {"role": "user", "content": f"The following is the news article: {news_article}"}
    ]

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": prompt,
        "messages": messages,
        "tools": tools,
        "tool_choice": {"type": "tool", "name": "tag_news"},
        "max_tokens": 1000,
        "temperature": 0,
        "top_p": 1,
    }

    for attempt in range(max_retries):
        try:
            response = bedrock_client.invoke_model(
                modelId=CLAUDE_MODEL_ID,
                body=json.dumps(body)
            )
            response_body = json.loads(response["body"].read())

            # print("Tagging============", response_body)

            tags = response_body["content"][0]["input"]["tags"]
            tag_reason = response_body["content"][0]["input"]["tag_reason"]
            return tags, tag_reason
        except Exception as e:
            err_msg = str(e)
            if "ThrottlingException" in err_msg or "Too many tokens" in err_msg:
                wait_time = 2 ** attempt
                print(f"Throttled on attempt {attempt+1}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                print(f"Error tagging article: {e}")
                break
    return "", ""

def tag_articles(df, qc_tags, bedrock_client):
    df = df.copy()

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [
            ex.submit(tag_news_article,
                      f"{row['headline']} {row.get('content','')}",
                      qc_tags,
                      bedrock_client)
            for _, row in df.iterrows()
        ]

        for (idx, f) in zip(df.index, tqdm(futures, desc="QC Tagging")):
            tags, tag_reason = f.result()
            df.loc[idx, ["qc_tags", "tag_reason"]] = [tags, tag_reason]

    return df

# =====================================================
# ADS REMOVAL (LOCATIONS SAFE)
# =====================================================

def get_filtered_content(headline, content, bedrock_client, loc_enum, max_retries=3):
    content = content.replace('\n\n','')
    prompt = """You are a precise, objective content filtering and information extraction assistant.

Read the given news headline and article content.

TASKS:

1. filtered_content:
- Remove ONLY the following:
  - Ads or promotional material
  - "Read more" prompts, links, or buttons
  - Incomplete trailing text fragments
  - Page noise (navigation text, footers, cookie notices, page-level UI)
  - Subtitles or section headers not part of the main article body
- IMPORTANT: Remove all \\n (newline escape characters) from the text. Preserve the wording exactly otherwise.

2. filtered_content_html:
- A clean HTML version using ONLY semantic tags to the above filtered_content:
  - <p> for paragraphs
  - <ul><li> for lists
  - <blockquote> for quoted sections
  - <br> for line breaks inside a paragraph or quoted text
- IMPORTANT: Do NOT use \\n (newline escape characters) anywhere in the output.
- Do NOT add styling, CSS, or new content.

3. locations:
Extract ONLY the geographic locations where the ACTUAL EVENT happened.
- INCLUDE:
  - Cities, states, countries, or regions where the reported event occurred
  - Company locations
  - Company headquarters
If the event location is ambiguous or unclear, use your knowledge to identify and return the locations. 
Return the locations as a Python list of strings.

RETURN JSON STRICTLY IN THIS FORMAT:
{
  "filtered_content": "clean article text",
  "filtered_content_html": "<p>clean article text</p>",
  "removed": "yes or no",
  "removed_text": "exact snippets removed or empty string",
  "locations": "python list of event locations"
}

Always include ALL keys.
"""

    tools = [
        {
            "name": "filter_content",
            "description": "Clean article content, return HTML, and extract event locations only.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "filtered_content": {
                        "type": "string",
                        "description": "provide clean article text without ads and promotoinal things",
                    },
                    "filtered_content_html": {
                        "type": "string",
                        "description": "provide html tagged article text (add html tags to the filtered_content)",
                    },
                    "ads_removed_flag": {
                        "type": "string",
                        "enum": ["yes", "no"],
                        "description": "weather or not removed ads and promotional things",
                    },
                    "removed_text": {
                        "type": "string",
                        "description": "provide removed ads and promotional text",
                    },
                    "locations": {
                        "type": "array",
                        "items": {"type": "string","enum": loc_enum},
                        "description": "locations where the event occurred, even model inference is fine.",
                        },
                },
                "required": ["filtered_content","filtered_content_html","ads_removed_flag","removed_text","locations"]
            }
        }
    ]

    messages = [
        {
            "role": "user",
            "content": f"Headline: {headline}\n\nContent:\n{content}"
        }
    ]

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": prompt,
        "messages": messages,
        "tools": tools,
        "tool_choice": {"type": "tool", "name": "filter_content"},
        "max_tokens": 30000,
        "temperature": 0,
        "top_p": 1
    }

    for attempt in range(max_retries):
        try:
            response = bedrock_client.invoke_model(
                modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
                body=json.dumps(body)
            )

            response_body = json.loads(response["body"].read())

            inputs = response_body["content"][0]["input"]
            
            # print("input=============:", inputs)

            return {
                "filtered_content": clean_escapes(
                    inputs.get("filtered_content", content),
                    "filtered_content"
                ),

                "filtered_content_html": clean_escapes(
                    inputs.get(
                        "filtered_content_html",
                        f"<p>{inputs.get('filtered_content', content)}</p>"
                    ),
                    "filtered_content_html"
                ),

                "ads_removed_flag": inputs.get("ads_removed_flag", "no"),
                "removed_text": inputs.get("removed_text", ""),
                "locations": ", ".join(inputs.get("locations", ""))
            }

        except Exception as e:
            if "ThrottlingException" in str(e) or "Too many tokens" in str(e):
                wait_time = 2 ** attempt
                time.sleep(wait_time)
            else:
                print(f"Error filtering content: {e}")
                break

    # Safe fallback
    return {
        "filtered_content": content,
        "filtered_content_html": f"<p>{content}</p>",
        "ads_removed_flag": "no",
        "removed_text": "",
        "locations": ""
    }

def remove_ads(df, bedrock_client, loc_enum):
    df = df.copy()

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(get_filtered_content, row["headline"], row.get("content",""), bedrock_client, loc_enum): idx
            for idx, row in df.iterrows()
        }

        for f in tqdm(as_completed(futures), total=len(futures), desc="Ads Removal"):
            idx = futures[f]
            out = f.result()
            df.loc[idx, [
                "filtered_content",
                "filtered_content_html",
                "ads_removed_flag",
                "removed_text",
                "locations"
            ]] = [
                out["filtered_content"],
                out["filtered_content_html"],
                out["ads_removed_flag"],
                out["removed_text"],
                out["locations"]
            ]

    return df

# =====================================================
# URL CHECKING (ORDER SAFE)
# =====================================================

def check_url_status(url):
    try:
        r = requests.head(url, allow_redirects=True,
                          timeout=URL_CHECK_TIMEOUT, verify=False)
        return r.status_code
    except Exception:
        return None

def check_urls_threaded(df, url_col="url", max_workers=URL_CHECK_MAX_WORKERS):
    df = df.copy()
    results = [None] * len(df)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(check_url_status, url): i
            for i, url in enumerate(df[url_col])
        }

        for f in tqdm(as_completed(futures), total=len(futures), desc="Checking URLs"):
            results[futures[f]] = f.result()

    df["url_status_code"] = results
    df["url_status"] = df["url_status_code"].apply(
        lambda x: "Working" if x == 200 else f"Returned {x}" if x else "Failed"
    )
    return df

# =====================================================
# DATABASE
# =====================================================

def run_query(sql):
    engine = create_engine(
        f"postgresql://{ps_user}:{ps_password}@{ps_host}:{ps_port}/{ps_dbname}"
    )
    return pd.read_sql_query(text(sql), engine)

def generate_news_id():
    return f"{datetime.now():%Y%m%d}_{uuid.uuid4()}"

def insert_news_df(df: pd.DataFrame):
    engine = create_engine(
        f"postgresql+psycopg2://{ps_user}:{ps_password}@{ps_host}:{ps_port}/{ps_dbname}",
        pool_pre_ping=True
    )

    df_db = df.copy()

    # Normalize booleans
    BOOL_MAP = {
    "Yes": True, "No": False,
    "yes": True, "no": False,
    True: True, False: False
}
    
    for c in ["valid", "is_duplicate", "ads_removed_flag"]:
        if c in df_db.columns:
            df_db[c] = df_db[c].map(BOOL_MAP)

    # Dates
    for c in ["date", "ingestion_date"]:
        if c in df_db.columns:
            df_db[c] = pd.to_datetime(df_db[c], errors="coerce", utc=True)
            # df_db[c] = pd.to_datetime(df_db[c], errors="coerce").dt.date

    for c in ["cutoff_time", "created_at"]:
        if c in df_db.columns:
            df_db[c] = pd.to_datetime(df_db[c], errors="coerce")

    with engine.begin() as conn:
        df_db.to_sql(
            "news",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500
        )

    print(f"✅ Inserted {len(df_db)} rows into news table")


