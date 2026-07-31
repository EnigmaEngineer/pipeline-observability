"""The pipeline the monitors watch.

Two stages, one partition at a time.

    raw JSONL under dt=YYYY-MM-DD/  ->  raw_orders  ->  daily_orders

Both stages overwrite their partition instead of appending. Re-running a date is the
normal case, not the exception, and an append-only load turns a retry into a doubled
day. A doubled day is also exactly the shape of failure a volume monitor is supposed to
catch, so getting it wrong here would let the pipeline manufacture its own alerts.

The column list is declared rather than inferred. read_json will happily guess, and its
guess changes when the data changes, which would mean a null-heavy day silently retypes
a column. This project is partly about detecting schema drift, so the load has to be the
one place that does not drift on its own.
"""

from datetime import date, datetime, timezone
from pathlib import Path


def now_utc():
    """Naive UTC. `obs.model` has the same three lines and that is deliberate.

    The pipeline must not import the thing watching it, so a shared helper module would
    buy tidiness at the cost of a dependency pointing the wrong way. Duplicating three
    lines is cheaper than that.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)

RAW_COLUMNS = {
    "order_id": "VARCHAR",
    "customer_id": "VARCHAR",
    "ordered_at": "TIMESTAMP",
    "channel": "VARCHAR",
    "country": "VARCHAR",
    "item_count": "INTEGER",
    "order_amount_usd": "DOUBLE",
    "coupon_code": "VARCHAR",
    "status": "VARCHAR",
}

DDL = [
    """
    CREATE TABLE IF NOT EXISTS raw_orders (
        order_id         VARCHAR NOT NULL,
        customer_id      VARCHAR NOT NULL,
        ordered_at       TIMESTAMP NOT NULL,
        channel          VARCHAR,
        country          VARCHAR,
        item_count       INTEGER,
        order_amount_usd DOUBLE,
        coupon_code      VARCHAR,
        status           VARCHAR,
        dt               DATE NOT NULL,
        loaded_at        TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_orders (
        dt                 DATE PRIMARY KEY,
        orders             BIGINT NOT NULL,
        gross_usd          DOUBLE NOT NULL,
        items              BIGINT NOT NULL,
        distinct_customers BIGINT NOT NULL,
        coupon_orders      BIGINT NOT NULL,
        cancelled          BIGINT NOT NULL,
        built_at           TIMESTAMP NOT NULL
    )
    """,
]


def create_tables(con):
    for statement in DDL:
        con.execute(statement)


def partition_path(raw_root, dt: date):
    return Path(raw_root) / f"dt={dt.isoformat()}" / "orders.jsonl"


def load_raw(con, dt: date, raw_root, now=None):
    """Overwrite one partition of raw_orders from its JSONL file."""
    src = partition_path(raw_root, dt)
    if not src.exists():
        raise FileNotFoundError(f"no partition file at {src}")
    now = now or now_utc()

    # the delete and the insert are one unit. without the transaction a malformed file
    # leaves the partition deleted and not replaced, which is a worse outcome than the
    # failed load, and the volume monitor would report it as a day of zero orders.
    columns = ", ".join(f"'{k}': '{v}'" for k, v in RAW_COLUMNS.items())
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM raw_orders WHERE dt = ?", [dt])
        con.execute(
            f"""
            INSERT INTO raw_orders
            SELECT {', '.join(RAW_COLUMNS)}, ? AS dt, ? AS loaded_at
              FROM read_json(?, format='newline_delimited', columns={{{columns}}})
            """,
            [dt, now, str(src)],
        )
    except Exception:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT")
    return con.execute(
        "SELECT count(*) FROM raw_orders WHERE dt = ?", [dt]
    ).fetchone()[0]


def build_daily(con, dt: date, now=None):
    """Rebuild one row of daily_orders from raw_orders.

    Grouping is on dt, the partition the file arrived in, not on ordered_at. So an event
    that happened on the 3rd but landed in the 4th's file is counted on the 4th. The
    generator never does that today, which means this pipeline is correct only because
    its source is well behaved. Late arrival goes into the day-6 injected failures.
    """
    now = now or now_utc()
    con.execute("DELETE FROM daily_orders WHERE dt = ?", [dt])
    con.execute(
        """
        INSERT INTO daily_orders
        SELECT dt,
               count(*),
               round(sum(order_amount_usd), 2),
               sum(item_count),
               count(DISTINCT customer_id),
               count(coupon_code),
               count(*) FILTER (WHERE status = 'cancelled'),
               ?
          FROM raw_orders
         WHERE dt = ?
         GROUP BY dt
        """,
        [now, dt],
    )
    row = con.execute(
        "SELECT orders, gross_usd FROM daily_orders WHERE dt = ?", [dt]
    ).fetchone()
    return row
