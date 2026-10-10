"""
=====================================================================================
API -> Snowflake pipeline (annotated teaching version)
=====================================================================================

WHAT THIS DOES
    1. EXTRACT:   Pull records from a REST API, page by page, with retries.
    2. TRANSFORM: Flatten the JSON into a table, clean column names, dedupe.
    3. LOAD:      Push the table into Snowflake and MERGE (upsert) it into the target.

This is the classic "EL(T)" shape. We do light cleanup in Python and leave the heavy
business logic to dbt inside Snowflake. That split is deliberate and is how most
modern data teams work: Python moves data, SQL/dbt models it.

SKILLS THIS FILE DEMONSTRATES (what hiring managers actually look for)
    - Talking to APIs reliably: auth, pagination, rate limits, retries, timeouts.
    - Incremental loading: only pull what changed, not the whole dataset every run.
    - Idempotency: running the job twice should NOT create duplicate rows.
    - Secure credentials: no passwords in code, key pair auth, secrets from env vars.
    - Warehouse fluency: staging tables, MERGE statements, schema handling.
    - Production habits: logging, clean failure exit codes, config via environment.
    - Containerization: this file runs inside Docker (see Dockerfile), scheduled by
      an orchestrator (Orchestra, Airflow, Dagster, Prefect, etc.).

None of these are exotic. What separates a junior from a solid data engineer is
doing ALL of them by default, every time, without being told.
=====================================================================================
"""

# -----------------------------------------------------------------------------------
# IMPORTS
# Standard library first, third-party second. Small thing, but reviewers notice.
# -----------------------------------------------------------------------------------
import logging   # Real logging, not print(). Logs get shipped to monitoring tools.
import os        # Read config and secrets from environment variables.
import sys       # Exit codes. 0 = success, non-zero = failure. Schedulers rely on this.
import time      # Small sleeps between API calls so we don't hammer the vendor.
from datetime import datetime, timedelta, timezone  # Always use timezone-aware UTC.

import pandas as pd   # In-memory table. Fine for thousands to low millions of rows.
import requests       # The de facto HTTP library in Python.

# cryptography: used to load the RSA private key for Snowflake key pair auth.
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# HTTPAdapter + Retry: built-in automatic retries for flaky APIs. Knowing these
# exist (instead of hand-rolling a while loop with try/except) is a good signal.
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Snowflake's official Python connector, plus its pandas helper for bulk loads.
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas


# -----------------------------------------------------------------------------------
# LOGGING SETUP
# Timestamps + level on every line. When this runs at 3am in a container and fails,
# the logs are the ONLY thing you'll have to debug it. Make them useful.
# -----------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")


# -----------------------------------------------------------------------------------
# CONFIGURATION
# Everything comes from environment variables. Why?
#   - The SAME Docker image runs in dev, test, and prod. Only the env vars change.
#   - Secrets never get committed to git.
#   - Ops teams can change behavior without a code deploy.
#
# os.environ["X"]   -> REQUIRED. Crashes immediately if missing (good: fail fast).
# os.getenv("X", d) -> OPTIONAL. Falls back to a sensible default.
# -----------------------------------------------------------------------------------
API_BASE = os.environ["API_BASE_URL"]       # e.g. https://api.vendor.com/v1
API_KEY = os.environ["API_KEY"]             # Secret. Injected at runtime, never hardcoded.
ENDPOINT = os.getenv("API_ENDPOINT", "records")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "500"))  # Rows per API call. Check vendor max.

# LOOKBACK_DAYS: how far back to pull on each run.
# We intentionally OVERLAP runs (e.g. run daily, pull 3 days). If yesterday's run
# failed, or the vendor backfilled late data, today's run catches it. The overlap
# creates duplicates, but the MERGE at the end handles that. This is the core idea
# behind "idempotent incremental loads."
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))

# Where the data lands in Snowflake. Database.Schema.Table.
SF_DB = os.getenv("SF_DATABASE", "RAW_DB")
SF_SCHEMA = os.getenv("SF_SCHEMA", "VENDOR_API")
SF_TABLE = os.getenv("SF_TABLE", "VENDOR_RECORDS")

# MERGE_KEYS: the column(s) that uniquely identify a row (the "natural key").
# Getting this right is critical. Wrong key = silent duplicates or overwritten data.
# Always confirm with the API docs or by profiling the data.
MERGE_KEYS = ["ID"]


# ===================================================================================
# STEP 1: EXTRACT
# ===================================================================================

def http_session() -> requests.Session:
    """
    Build a reusable HTTP session with automatic retries.

    Why a Session instead of plain requests.get()?
      - Reuses the TCP connection across pages (faster).
      - Headers (auth) are set once instead of on every call.
      - Retry behavior is attached once and applies everywhere.
    """
    s = requests.Session()

    # Retry policy. This single block handles most real-world API flakiness.
    retry = Retry(
        total=5,               # Give up after 5 attempts.
        backoff_factor=2,      # Exponential backoff: waits ~2s, 4s, 8s, 16s...
                               # Backing off instead of retrying instantly is polite
                               # AND more likely to succeed once the API recovers.
        status_forcelist=[     # Which HTTP errors are worth retrying:
            429,               #   Too Many Requests (rate limited). Very common.
            500, 502, 503, 504 #   Server-side hiccups. Usually transient.
        ],                     # NOT retried: 400/401/403/404. Those are OUR bugs
                               # (bad params, bad key). Retrying won't fix them.
        respect_retry_after_header=True,  # If the API says "wait 30s", we wait 30s.
        allowed_methods=["GET"],          # Only retry safe, read-only calls.
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))

    # Auth header. Bearer tokens are the most common pattern. Some vendors use
    # API key headers (X-API-Key), OAuth2 client credentials, or query params.
    # Being comfortable with all of them is part of the job.
    s.headers.update({
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json",
    })
    return s


def extract(since: datetime) -> list[dict]:
    """
    Pull every record updated since `since`, walking through all pages.

    PAGINATION: APIs rarely return everything in one call. Common styles:
      - Offset/limit  (used here): ?limit=500&offset=0, then offset=500, etc.
      - Cursor:       response includes a "next_cursor" token you pass back.
      - Next link:    response includes a full URL for the next page.
      - Page number:  ?page=1, ?page=2...
    Cursor-based is the most robust for large/changing datasets. Read the vendor
    docs and adapt this loop. The structure stays the same.
    """
    s = http_session()
    rows: list[dict] = []
    offset = 0

    while True:
        r = s.get(
            f"{API_BASE}/{ENDPOINT}",
            params={
                # INCREMENTAL FILTER: ask the API only for changed records.
                # Pulling the full dataset every run is the #1 rookie mistake.
                # It's slow, expensive, and hits rate limits.
                "updated_since": since.isoformat(),
                "limit": PAGE_SIZE,
                "offset": offset,
            },
            # ALWAYS set a timeout. Without one, a hung connection can block the
            # job forever and nobody gets alerted. Easy interview talking point.
            timeout=60,
        )

        # Turn 4xx/5xx responses into Python exceptions. Without this, a 401 error
        # page would get silently parsed as "zero rows" and the job would "succeed".
        r.raise_for_status()

        # Most APIs wrap results in an envelope like {"data": [...], "meta": {...}}.
        # Adjust the key to match your vendor.
        batch = r.json().get("data", [])
        rows.extend(batch)

        # Log progress per page. Helps answer "how far did it get before it died?"
        log.info("page offset=%s rows=%s total=%s", offset, len(batch), len(rows))

        # Stop condition: a short (or empty) page means we've reached the end.
        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.25)  # Light throttle. Being a good API citizen avoids bans.

    return rows


# ===================================================================================
# STEP 2: TRANSFORM
# Keep this LIGHT. Only do what's needed to land the data cleanly. Business logic
# (joins, calculations, metrics) belongs in dbt/SQL where it's versioned, tested,
# and visible to analysts. Burying business rules in Python is a common anti-pattern.
# ===================================================================================

def transform(rows: list[dict]) -> pd.DataFrame:
    """Flatten raw JSON into a clean, load-ready DataFrame."""

    # json_normalize flattens nested JSON:
    #   {"id": 1, "well": {"name": "A-1", "field": "X"}}
    # becomes columns: id, well_name, well_field
    df = pd.json_normalize(rows, sep="_")

    if df.empty:
        return df

    # Snowflake treats unquoted identifiers as UPPERCASE. If you load lowercase
    # column names, you end up having to write "quoted_names" in every query
    # forever. Uppercasing up front saves analysts real pain.
    df.columns = [c.upper() for c in df.columns]

    # Dedupe WITHIN this batch. Pagination can return the same record twice if data
    # changes mid-pull. MERGE will fail ("duplicate row detected") if the source has
    # two rows with the same key, so we must clean this before loading.
    df = df.drop_duplicates(subset=MERGE_KEYS, keep="last")

    # Audit column: when did this row land? Invaluable for debugging ("when did this
    # bad value show up?") and for downstream incremental dbt models.
    df["_LOADED_AT"] = pd.Timestamp.now(tz="UTC")

    # Any leftover nested lists/dicts (e.g. an array of tags) can't go into a normal
    # column cleanly. Simplest approach: stringify them. A more advanced approach is
    # loading them into a Snowflake VARIANT column and parsing with SQL later.
    for c in df.columns:
        if df[c].map(lambda v: isinstance(v, (list, dict))).any():
            df[c] = df[c].astype(str)

    return df


# ===================================================================================
# STEP 3: LOAD
# ===================================================================================

def sf_connect():
    """
    Connect to Snowflake using RSA KEY PAIR authentication.

    Why not username/password?
      - Snowflake is enforcing MFA on password logins. Service accounts (bots like
        this pipeline) can't tap "approve" on a phone. Key pair auth is the
        standard answer for automated jobs.
      - A private key file is far harder to leak or guess than a password.
      - Keys can be rotated without downtime (Snowflake supports two active keys).

    The private key lives in a secret store (Azure Key Vault, AWS Secrets Manager,
    etc.) and gets mounted into the container as a file at runtime. It is NEVER
    baked into the Docker image or committed to git.
    """
    # Read the PEM-encoded private key file.
    with open(os.environ["SF_PRIVATE_KEY_PATH"], "rb") as f:
        pkey = serialization.load_pem_private_key(
            f.read(),
            # Encrypted keys need a passphrase. Empty string -> None (unencrypted key).
            password=os.getenv("SF_PRIVATE_KEY_PASSPHRASE", "").encode() or None,
            backend=default_backend(),
        )

    # The Snowflake connector wants the key in DER/PKCS8 bytes, not PEM text.
    # This conversion trips up a lot of people the first time.
    pkb = pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return connect(
        account=os.environ["SF_ACCOUNT"],      # e.g. myorg-myaccount
        user=os.environ["SF_USER"],            # Dedicated service user, not a person.
        private_key=pkb,
        # ROLE: use a least-privilege role that can ONLY write to this schema.
        # Never run pipelines as ACCOUNTADMIN or SYSADMIN. Security teams check this.
        role=os.environ["SF_ROLE"],
        # WAREHOUSE: the compute that runs queries. Use a small, auto-suspending one
        # for loads. Snowflake bills by warehouse-seconds, so this is a cost lever.
        warehouse=os.environ["SF_WAREHOUSE"],
        database=SF_DB,
        schema=SF_SCHEMA,
    )


def load(df: pd.DataFrame) -> None:
    """
    Load the DataFrame into Snowflake using the STAGE -> MERGE pattern.

    Why not just INSERT straight into the target table?
      - Plain INSERT on overlapping incremental pulls = duplicate rows.
      - Plain TRUNCATE + INSERT = target is empty/partial while loading, and you
        lose history if the API only returns recent data.

    STAGE -> MERGE gives us:
      1. Bulk-load new rows into a throwaway staging table (fast).
      2. MERGE staging into target: update rows that exist, insert ones that don't.
    Result: the job is IDEMPOTENT. Run it once or ten times, same final table.
    This one concept comes up in almost every data engineering interview.
    """
    stage_tbl = f"{SF_TABLE}_STG"
    cols = list(df.columns)

    # `with` blocks guarantee the connection and cursor close even if something
    # throws. Leaked connections are a quiet source of production problems.
    with sf_connect() as conn, conn.cursor() as cur:

        # write_pandas: bulk upload a DataFrame. Under the hood it writes Parquet
        # files, PUTs them to an internal Snowflake stage, and runs COPY INTO.
        # That is WAY faster than row-by-row INSERTs (orders of magnitude).
        ok, _, nrows, _ = write_pandas(
            conn,
            df,
            stage_tbl,
            auto_create_table=True,     # Create the staging table from df's shape.
            table_type="temporary",     # TEMP table: auto-dropped when the session
                                        # ends. No cleanup code, no leftover junk.
            use_logical_type=True,      # Keep timestamps as real timestamps, not ints.
            quote_identifiers=False,    # Keep names unquoted (we uppercased earlier).
        )
        if not ok:
            raise RuntimeError("write_pandas failed")
        log.info("staged %s rows", nrows)

        # First run only: create the permanent target table using the staging
        # table's structure. In a mature setup you'd define the target with explicit
        # DDL (or manage it via dbt/Terraform/schemachange) for tighter control
        # over types. This shortcut is fine for a demo.
        cur.execute(f"CREATE TABLE IF NOT EXISTS {SF_TABLE} LIKE {stage_tbl}")

        # Build the MERGE statement dynamically from the column list, so we don't
        # have to hand-maintain a giant SQL string every time the API adds a field.
        #
        # NOTE: Building SQL with f-strings is only safe here because column names
        # come from our own code/config, not from user input. Never do this with
        # untrusted values. That's how SQL injection happens.
        on = " AND ".join(f"t.{k} = s.{k}" for k in MERGE_KEYS)                 # match condition
        upd = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in MERGE_KEYS)  # columns to update
        ins_cols = ", ".join(cols)                                              # columns to insert
        ins_vals = ", ".join(f"s.{c}" for c in cols)                            # values to insert

        # The MERGE itself. Reads as plain English:
        #   For each staging row, find the matching target row by key.
        #   Found it?   -> update it with the fresh values.
        #   Not found?  -> insert it as a new row.
        cur.execute(f"""
            MERGE INTO {SF_TABLE} t
            USING {stage_tbl} s
                ON {on}
            WHEN MATCHED THEN
                UPDATE SET {upd}
            WHEN NOT MATCHED THEN
                INSERT ({ins_cols}) VALUES ({ins_vals})
        """)

        # Snowflake returns (rows inserted, rows updated). Log it. Over time this
        # gives you a cheap data-volume monitor: a sudden drop to 0 is a red flag.
        log.info("merge result (inserted, updated): %s", cur.fetchone())

        # Note: Snowflake autocommits by default, so no explicit COMMIT needed.


# ===================================================================================
# MAIN: wire the three steps together
# ===================================================================================

def main() -> int:
    # Compute the incremental window in UTC. Mixing local time and UTC is one of
    # the most common sources of "missing data" bugs in pipelines. Use UTC everywhere.
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    rows = extract(since)

    # No new data is a NORMAL outcome, not an error. Exit cleanly and skip the
    # Snowflake connection entirely (saves warehouse spin-up cost).
    if not rows:
        log.info("no new rows since %s", since)
        return 0

    df = transform(rows)
    load(df)
    log.info("done: %s rows", len(df))
    return 0


# -----------------------------------------------------------------------------------
# ENTRY POINT
# This guard means the code below only runs when the file is executed directly
# (python pipeline.py), not when it's imported (e.g. by unit tests). Writing code
# that's importable and testable is another quiet signal of experience.
# -----------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        # log.exception records the full stack trace, not just the message.
        log.exception("pipeline failed")
        # Non-zero exit code tells the container platform and the scheduler that
        # the run FAILED, which triggers retries/alerts. A pipeline that swallows
        # errors and exits 0 is worse than one that crashes: it fails silently
        # and someone finds out weeks later from a wrong dashboard.
        sys.exit(1)


# ===================================================================================
# WHERE TO GO FROM HERE (stuff that would come up in a senior-level conversation)
# ===================================================================================
#   - Testing: unit tests for transform() with sample JSON (pytest), plus data
#     tests in dbt (unique, not_null, freshness) on the landed table.
#   - Schema drift: what happens when the vendor adds/renames a field? Options:
#     land raw JSON in a VARIANT column, or detect and ALTER TABLE automatically.
#   - Scale: for millions of rows, stream pages to files in cloud storage and use
#     COPY INTO / Snowpipe instead of holding everything in pandas memory.
#   - State: store the last successful high-water mark in a control table instead
#     of a fixed lookback window.
#   - Observability: alerting on failures, row-count anomalies, and run duration.
#   - CI/CD: lint + tests on every PR, auto-build and push the Docker image.
# ===================================================================================
