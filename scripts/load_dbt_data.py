"""Load CSV into an empty ClickHouse raw table using the HTTP interface.

Uses only the Python standard library. Refuses a nonempty table instead of
duplicating data. A single INSERT is atomic for this single-partition-independent
MergeTree input block pipeline only to the extent guaranteed by ClickHouse;
after interruption inspect row counts before retrying.
"""
import argparse
import base64
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, ProxyHandler, build_opener


def request(query, body=None):
    url = os.getenv("CLICKHOUSE_URL", "http://localhost:18123")
    credentials = (
        os.getenv("DBT_USER", "dbt") + ":" +
        os.getenv("DBT_PASSWORD", "local_dbt_password")
    )
    headers = {"Authorization": "Basic " + base64.b64encode(credentials.encode()).decode()}
    if body is not None and hasattr(body, 'fileno'):
        headers['Content-Length'] = str(os.fstat(body.fileno()).st_size)
    req = Request(url + "/?" + urlencode({"query": query}), data=body, headers=headers)
    try:
        # Local database traffic must not pass through a corporate/VPN proxy.
        opener = build_opener(ProxyHandler({})) if urlparse(url).hostname in ('localhost', '127.0.0.1') else build_opener()
        with opener.open(req, timeout=600) as response:
            return response.read().decode()
    except HTTPError as error:
        raise RuntimeError(error.read().decode()) from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=Path("data/train.csv"))
    parser.add_argument('--bootstrap', action='store_true', help='Create the raw database and table from repository DDL')
    args = parser.parse_args()
    if not args.csv.is_file():
        parser.error(f"CSV not found: {args.csv}")
    if args.bootstrap:
        ddl = Path(__file__).resolve().parents[1] / 'dbt/bootstrap/01_transactions.sql'
        for statement in ddl.read_text(encoding='utf-8').split(';'):
            if statement.strip():
                request(statement.strip())
    count = int(request("SELECT count() FROM transactions_db.transactions").strip())
    if count:
        print(f"Raw already contains {count} rows; no data appended.")
        return
    with args.csv.open("rb") as stream:
        print(request(
            "INSERT INTO transactions_db.transactions SETTINGS date_time_input_format='best_effort' FORMAT CSVWithNames", stream
        ))
    print(request("SELECT count(), min(transaction_time), max(transaction_time) FROM transactions_db.transactions"))


if __name__ == "__main__":
    main()
