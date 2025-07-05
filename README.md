# Facebook Ad Library Data Pipeline

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Don't forget to install playwright

```bash
playwright install
```

## Running the pipeline

`python main.py`

This script will:

- Save raw data to `data/raw/ads_raw_data_<timestamp>.json`
- Save cleaned data to `data/transformed/transformed_ads_<timestamp>.parquet`
- Save a report with top 10 ads to `data/reports/top_10_ads_<timestamp>.csv`

# Report Output

The report will include fields like:

- `ad_id`
- `ad_link`
- `is_active`
- `start_date`, `end_date`
- `hours_passed` (how long the ad has been live)
- `media_mix` (video, image, both, none)
- `ad_text`
- `ad_lang_code` (language detection)

Saved to:
`data/reports/report\_<timestamp>.csv`

# Rank performance logic

The top 10 ads in the report are selected based on how long they’ve been actively running (hours_passed).

Longer-running ads are likely more effective if an ad stays live for a long time, it’s often a sign that it’s performing well or generating engagement.

It’s a reliable proxy the Facebook Ad Library doesn’t expose deeper performance metrics like impressions or clicks for this ad type, so runtime is the most consistent and comparable signal available.

Focusing on active ads adds relevance by filtering only ads that are currently live, the report highlights the most recent and potentially impactful creatives being used right now.

This approach ensures the report prioritizes ads that are not only sustained over time, but also still in use, giving you insights into what strategies are currently working.
