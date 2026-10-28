"""Load the Simplify new grad listings JSON into BigQuery (raw.simplify_listings).

Flow: download -> add ingestion metadata -> write NDJSON temp file -> BigQuery load job.
Auth uses gcloud application default credentials (`gcloud auth application-default login`).
"""
import json
import os
import tempfile
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# Same source as the JobTracker project.
LISTINGS_URL = "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/dev/.github/scripts/listings.json"
DATASET = "raw"
TABLE = "simplify_listings"


def main():
    # Step 6: read the project ID from .env (kept out of git) rather than hardcoding it.
    load_dotenv()
    project_id = os.environ["GCP_PROJECT_ID"]

    # Step 1: download the listings JSON (a top-level array of listing objects).
    resp = requests.get(LISTINGS_URL, timeout=60)
    resp.raise_for_status()
    listings = resp.json()

    # Step 2: keep every field as is; add one UTC timestamp and its date.
    # The date is the partition key, so compute both from the same instant.
    now = datetime.now(timezone.utc)
    ingested_at = now.isoformat()
    ingestion_date = now.date().isoformat()
    for row in listings:
        row["ingested_at"] = ingested_at
        row["ingestion_date"] = ingestion_date

    # Step 3: write newline delimited JSON (one object per line), the format load jobs expect.
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False, encoding="utf-8") as tmp:
        for row in listings:
            tmp.write(json.dumps(row) + "\n")
        ndjson_path = tmp.name

    try:
        # Authentication comes from application default credentials; no key files.
        client = bigquery.Client(project=project_id)

        # Step 4: load job config -- NDJSON input, schema autodetect, and a table
        # partitioned by the ingestion_date column.
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingestion_date",
            ),
            # Step 5: WRITE_TRUNCATE on a partition decorator ($YYYYMMDD) replaces only
            # today's partition, so same-day reruns don't duplicate and older days are kept.
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        partition_suffix = now.strftime("%Y%m%d")
        table_id = f"{project_id}.{DATASET}.{TABLE}${partition_suffix}"

        # Use a batch load job (not streaming, not INSERT) and wait for it to finish.
        with open(ndjson_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {job.output_rows} rows into {table_id}")
    finally:
        # Clean up the temp file whether or not the load succeeded.
        os.remove(ndjson_path)


if __name__ == "__main__":
    main()
