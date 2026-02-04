import warnings
import pandas as pd
from datetime import datetime
from pathlib import Path
import traceback
from eventregistry import EventRegistry

from config import *
from utils import *
from pathlib import Path
from dotenv import load_dotenv         
from mailer import send_latest_finaloutput_gmail
from constants import INTERNAL_DEDUPE_COLS
warnings.filterwarnings("ignore")



# --------------------------------------------------
# RUN STATS
# --------------------------------------------------
def init_run_stats():
    return {
        "run_id": None,
        "start_time": None,
        "end_time": None,
        "status": "STARTED",
        "error_message": None,
        "total_articles_fetched": 0,
        "blogposts_marked": 0,
        "invalid_company_dropped": 0,
        "duplicates_marked": 0,
        "unique_articles_processed": 0,
        "classified_count": 0,
        "tagged_count": 0,
        "urls_checked": 0,
        "ads_cleaned": 0,
        "final_rows_saved": 0
    }


def save_run_stats_excel(stats):
    path = RUN_STATS_FILE  # from config.py
    df_new = pd.DataFrame([stats])

    if path.exists():
        df_old = pd.read_excel(path)
        df_new = pd.concat([df_old, df_new], ignore_index=True)

    df_new.to_excel(path, index=False)


def ensure_columns(df, cols):
    for col, default in cols.items():
        if col not in df.columns:
            df[col] = default
    return df


def process_articles(articles_list, mode, er, from_dt, to_dt, bedrock, loc_df, loc_enum):
    """
    Process articles for a given mode (supplier or location).
    
    Args:
        articles_list: List of companies or locations to fetch
        mode: 'supplier' or 'location'
        er: EventRegistry instance
        from_dt, to_dt: Date range
        bedrock: Bedrock client
        loc_df, loc_enum: Location data
        
    Returns:
        DataFrame with processed articles
    """
    if not articles_list:
        print(f"Skipping {mode} - empty list")
        return pd.DataFrame()
    
    print(f"\n{'='*60}")
    print(f"Processing {mode.upper()}: {len(articles_list)} items")
    print(f"{'='*60}\n")
    
    # --------------------------------------------------
    # LOAD MODE-SPECIFIC CATEGORIES
    # --------------------------------------------------
    category_df, news_enum = load_category_data(mode)
    print(f'Number of categories for {mode}: {len(news_enum)}')
    
    # --------------------------------------------------
    # FETCH
    # --------------------------------------------------
    articles = fetch_news_articles(
        er, from_dt, to_dt,
        articles_list, MAX_ITEMS, LANGUAGE, DATA_TYPES, CATEGORY, mode
    )

    df = pd.DataFrame(articles)
    if df.empty:
        print(f"No articles fetched for {mode}")
        return df
    
    print(f'Articles fetched for {mode}: {df.shape[0]}')

    # Merge to bring back company_name
    table, field = get_table_config(mode)
    source_df = run_query(f"SELECT {field} FROM {table} ORDER BY company_name ASC;")
    
    df = df.drop(columns=['company_name']).merge(
            source_df[['full_name', 'company_name']],
            left_on='company',
            right_on='full_name',
            how='left'
        )
    df = df.drop(columns=['full_name'])
    
    # Apply cutoff time filter
    mask = pd.to_datetime(df["date"], utc=True).dt.tz_convert("Asia/Kolkata") > cutoff_time
    df = df[mask]
    print(f'Articles after cutoff filter: {df.shape[0]}')

    before = len(df)
    df = df[df["er_event_uri"].isna()]
    print(f"Articles skipped due to eventUri (clustered events): {before - len(df)}")

    if df.empty:
        return df

    # --------------------------------------------------
    # ENSURE BASE COLUMNS
    # --------------------------------------------------
    df = ensure_columns(df, {
        "url": None,
        "company": None,
        "content": None
    })

    # --------------------------------------------------
    # BLOG DETECTION
    # --------------------------------------------------
    df = extract_blog_links(df)

    # --------------------------------------------------
    # COMPANY VALIDATION
    # --------------------------------------------------
    df = validate_company_in_headline(df)
    before = len(df)
    df = df[df["valid"] == "Yes"].reset_index(drop=True)
    print(f"Invalid company/location dropped: {before - len(df)}")

    if df.empty:
        return df

    # --------------------------------------------------
    # DEDUPLICATION (EventRegistry native)
    # --------------------------------------------------
    df = ensure_columns(df, {
        "is_duplicate": "No",
        "dup_group_id": None,
        "er_is_duplicate": False,
        "er_original_uri": None,
        "er_uri": None
    })

    # Ensure is_duplicate is consistent with er_is_duplicate
    df["is_duplicate"] = df["er_is_duplicate"].fillna(False).apply(
        lambda x: "Yes" if bool(x) else "No"
    )

    # Ensure group id exists (use original if present else self)
    df["dup_group_id"] = (
        df["dup_group_id"]
        .fillna(df["er_original_uri"])
        .fillna(df["er_uri"])
    )

    print(f"Duplicates marked (ER): {(df['is_duplicate'] == 'Yes').sum()}")


    df_no = df[df["is_duplicate"] == "No"].copy()

    if not df_no.empty:
        deduped = deduplicate_by_cosine_bedrock(df_no)

        # update only those rows (keeps ER duplicates untouched)
        df.loc[deduped.index, "is_duplicate"] = deduped["is_duplicate"]

        # optional: keep ER dup_group_id if already present, else set cosine group id
        df.loc[deduped.index, "dup_group_id"] = df.loc[deduped.index, "dup_group_id"].fillna(deduped["dup_group_id"])

        print(f"Additional duplicates marked (Cosine): {(deduped['is_duplicate'] == 'Yes').sum()}")

    print(f"Total duplicates after both layers: {(df['is_duplicate'] == 'Yes').sum()}")

    # --------------------------------------------------
    # PROCESS UNIQUE
    # --------------------------------------------------
    df_process = df[df["is_duplicate"] == "No"].copy()
    print(f"Unique articles to process: {len(df_process)}")

    if df_process.empty:
        return df

    # --------------------------------------------------
    # ENSURE DOWNSTREAM COLUMNS
    # --------------------------------------------------
    df = ensure_columns(df, {
        "classification": None,
        "risk_rating": None,
        "cat_reason": None,
        "relevance":pd.NA,
        "qc_tags": None,
        "tag_reason": None,
        "url_status": None,
        "url_status_code": None,
        "filtered_content": None,
        "ads_removed_flag": None,
        "removed_text": None,
        "filtered_content_html": None,
        "locations": None,
        "supplier_location": mode
    })

    df["relevance"] = pd.to_numeric(df["relevance"], errors="coerce")

    # --------------------------------------------------
    # CLASSIFICATION
    # --------------------------------------------------
    classified = classify_articles(df_process, news_enum, category_df, bedrock)
    df_process.loc[
        classified.index,
        ["classification", "risk_rating", "cat_reason", "relevance"]
    ] = classified[["classification", "risk_rating", "cat_reason", "relevance"]]

    # --------------------------------------------------
    # QC TAGGING
    # --------------------------------------------------
    tagged = tag_articles(df_process, QC_TAGS, bedrock)
    df_process.loc[
        tagged.index,
        ["qc_tags", "tag_reason"]
    ] = tagged[["qc_tags", "tag_reason"]]

    # --------------------------------------------------
    # URL CHECK
    # --------------------------------------------------
    checked = check_urls_threaded(df_process)
    df_process.loc[
        checked.index,
        ["url_status", "url_status_code"]
    ] = checked[["url_status", "url_status_code"]]

    # --------------------------------------------------
    # ADS REMOVAL
    # --------------------------------------------------
    cleaned = remove_ads(df_process, bedrock, loc_enum)
    df_process.loc[
        cleaned.index,
        ["filtered_content", "ads_removed_flag", "removed_text",
         "filtered_content_html", "locations"]
    ] = cleaned[
        ["filtered_content", "ads_removed_flag", "removed_text",
         "filtered_content_html", "locations"]
    ]

    # --------------------------------------------------
    # MERGE BACK
    # --------------------------------------------------
    df.update(df_process)
    
    # --------------------------------------------------
    # FINAL ENRICHMENT
    # --------------------------------------------------
    df = df.rename(columns={"blogpost links": "blogpost_links"})
    df = df.drop(columns=["risk_rating", "company"], errors="ignore")

    categories = run_query("SELECT category_id, category_name, risk_rating, risk_level FROM categories")
    
    table, field = get_id_field_config(mode)
    companies = run_query(f"SELECT {field} FROM {table} ORDER BY company_name ASC;")

    df = df.merge(categories,left_on="classification",right_on="category_name",how="left").drop(columns=["classification"])
    df = df.merge(companies,on="company_name",how="left")
    
    # --------------------------------------------------
    # FINAL FIELDS
    # --------------------------------------------------
    df["news_id"] = [generate_news_id() for _ in range(len(df))]
    now = datetime.now()

    for col in [
        "analyst_dup", "analyst_cat", "analyst_tag",
        "analyst_approval", "processed",
        "analyst_risk_level", "analyst_remark",
        "analyst_cat_id", "source"
    ]:
        df[col] = None

    df["ingestion_date"] = now
    df["created_at"] = now
    df['supplier_location'] = mode

    return df


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    run_stats = init_run_stats()
    run_stats["run_id"] = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_stats["start_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # --------------------------------------------------
        # INIT
        # --------------------------------------------------
        er = EventRegistry(apiKey=NEWS_API_KEY)
        print("days_back:", days_back)
        from_dt, to_dt = get_date_range(days_back=days_back)

        print("from_dt: ", from_dt)
        print("to_dt: ", to_dt)

        bedrock = get_bedrock_client()
        loc_df, loc_enum = load_locations_data()

        # --------------------------------------------------
        # PROCESS COMPANIES AND LOCATIONS
        # --------------------------------------------------
        all_dataframes = []
        
        # Process companies if configured
        if COMPANIES_LIST:
            df_companies = process_articles(
                COMPANIES_LIST, "supplier", er, from_dt, to_dt,
                bedrock, loc_df, loc_enum
            )
            if not df_companies.empty:
                all_dataframes.append(df_companies)
                
                # Save separate file for companies
                now = datetime.now()
                df_companies_out = df_companies.drop(columns=INTERNAL_DEDUPE_COLS, errors="ignore")

                companies_path = save_to_excel(
                    df_companies_out,
                    BASE_FOLDER,
                    f"Companies_Output_{now:%Y%m%d_%H%M%S}.xlsx"
                )

                print(f"✅ Companies output saved: {companies_path}")
        
        # Process locations if configured
        if LOCATIONS_LIST:
            df_locations = process_articles(
                LOCATIONS_LIST, "location", er, from_dt, to_dt,
                bedrock, loc_df, loc_enum
            )
            if not df_locations.empty:
                all_dataframes.append(df_locations)
                
                # Save separate file for locations
                now = datetime.now()
                df_locations_out = df_locations.drop(columns=INTERNAL_DEDUPE_COLS, errors="ignore")
                locations_path = save_to_excel(
                    df_locations_out,
                    BASE_FOLDER,
                    f"Locations_Output_{now:%Y%m%d_%H%M%S}.xlsx"
                )
                print(f"✅ Locations output saved: {locations_path}")
        
        # --------------------------------------------------
        # COMBINE AND SAVE
        # --------------------------------------------------
        if not all_dataframes:
            print("⚠️ No articles processed")
            run_stats["status"] = "SUCCESS"
            return
        
        # Combine all dataframes
        df_combined = pd.concat(all_dataframes, ignore_index=True)
        
        run_stats["total_articles_fetched"] = len(df_combined)
        run_stats["blogposts_marked"] = (df_combined["blogpost_links"] != "NO").sum()
        run_stats["duplicates_marked"] = (df_combined["is_duplicate"] == "Yes").sum()
        run_stats["unique_articles_processed"] = (df_combined["is_duplicate"] == "No").sum()
        
        # Save combined output
        df_for_excel = df_combined.copy()

        # drop internal dedupe columns
        df_for_excel = df_for_excel.drop(columns=INTERNAL_DEDUPE_COLS, errors="ignore")

        df_for_excel["analyst_headline"] = ""
        df_for_excel["analyst_content"] = ""

        now = datetime.now()
        final_path = save_to_excel(
            df_for_excel,
            BASE_FOLDER,
            f"FinalOutput_{now:%Y%m%d_%H%M%S}.xlsx"
        )

        run_stats["final_rows_saved"] = len(df_combined)
        
        # Insert into database
        insert_news_df(df_combined[:1])

        run_stats["status"] = "SUCCESS"
        print("✅ Pipeline completed successfully")
        
        

        load_dotenv(Path(__file__).resolve().parent / ".env")
        send_latest_finaloutput_gmail(Path(__file__).resolve().parent)


    except Exception as e:
        run_stats["status"] = "FAILED"
        run_stats["error_message"] = str(e)
        traceback.print_exc()

    finally:
        run_stats["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_run_stats_excel(run_stats)


if __name__ == "__main__":
    main()
