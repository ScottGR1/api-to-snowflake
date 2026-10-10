# api-to-snowflake

An annotated, end-to-end example of a production-style data pipeline. It pulls from a REST API, does a light cleanup, and upserts the result into Snowflake.

The comments in `pipeline.py` explain both what the code does and why each choice was made. Read it top to bottom.

## Flow

1. **Extract**: paginated API pull with retries, backoff, timeouts, and an incremental `updated_since` filter
2. **Transform**: flatten the JSON, uppercase the columns, dedupe on the key, add a load timestamp
3. **Load**: bulk load into a temp staging table with `write_pandas`, then `MERGE` into the target

## Concepts covered

- Incremental and idempotent loads, using an overlapping window plus MERGE
- Snowflake RSA key pair auth, with no passwords
- Config and secrets supplied through environment variables
- Logging and non-zero exit codes on failure
- Running in Docker, scheduled by an orchestrator

## Run

```bash
docker build -t api-to-snowflake .
docker run --env-file .env -v /path/to/key.p8:/mnt/secrets/sf-private-key:ro api-to-snowflake
```

## Required env vars

| Var | Notes |
|---|---|
| `API_BASE_URL` | e.g. `https://api.vendor.com/v1` |
| `API_KEY` | Bearer token |
| `SF_ACCOUNT` | e.g. `myorg-myaccount` |
| `SF_USER` | Service user |
| `SF_ROLE` | Least-privilege role |
| `SF_WAREHOUSE` | Small, auto-suspending warehouse |
| `SF_PRIVATE_KEY_PATH` | Defaults to `/mnt/secrets/sf-private-key` |

Optional variables: `API_ENDPOINT`, `PAGE_SIZE`, `LOOKBACK_DAYS`, `SF_DATABASE`, `SF_SCHEMA`, `SF_TABLE`, `SF_PRIVATE_KEY_PASSPHRASE`.
