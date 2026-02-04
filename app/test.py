import pandas as pd
from pathlib import Path
from utils import insert_news_df   # make sure path/import is correct


def insert_from_output_excel(excel_path: str):
    excel_path = Path(excel_path)

    if not excel_path.exists():
        raise FileNotFoundError(f"File not found: {excel_path}")

    print(f"📄 Reading output file: {excel_path}")

    df = pd.read_excel(excel_path)

    if df.empty:
        print("⚠️ Excel is empty. Nothing to insert.")
        return

    print(f"📊 Rows found in Excel: {len(df)}")

    # 🔒 Safety: drop internal pipeline columns (DB doesn't have these)
    INTERNAL_COLS = [
        "er_uri", "er_is_duplicate", "er_original_uri",
        "dup_group_id", "er_event_uri",'analyst_headline', 'analyst_content',
    ]
    df = df.drop(columns=INTERNAL_COLS, errors="ignore")

    insert_news_df(df)


def main():
    # 🔽 PUT YOUR GENERATED FILE PATH HERE
    excel_file = r"app\Outputs\20260204_NewsAPI_extraction\FinalOutput_20260204_135937.xlsx"

    insert_from_output_excel(excel_file)
    print("✅ Output file inserted into DB successfully")


if __name__ == "__main__":
    main()
