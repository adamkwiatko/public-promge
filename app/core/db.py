"""
create_engine_util.py

Utility to create a SQLAlchemy Engine for reuse across functions.

Provides:
    - make_engine(...) -> sqlalchemy.engine.Engine

Features:
    - Accepts either a full connection string or discrete connection components.
    - Exposes common pooling and connection options with sensible defaults.
    - Returns a ready-to-use SQLAlchemy Engine (call .dispose() when done).
"""

from typing import Optional, Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL


def make_engine(
    conn_str: Optional[str] = None,
    *,
    drivername: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
    pool_pre_ping: bool = True,
    connect_args: Optional[Dict[str, Any]] = None,
    future: bool = True,
    **kwargs: Any,
) -> Engine:
    """
    Create and return a SQLAlchemy Engine.

    Parameters
    - conn_str: Full SQLAlchemy connection string (e.g. "postgresql+psycopg2://user:pass@host/db").
                If provided, other connection components (drivername/username/...) are ignored.
    - drivername, username, password, host, port, database:
                Discrete connection components used to build a connection URL when conn_str is None.
                drivername examples: "postgresql+psycopg2", "mysql+pymysql", "sqlite"
                For sqlite, pass database="path/to/db.sqlite" and leave host/port empty.
    - pool_size, max_overflow: Connection pool sizing (useful for production).
    - echo: If True, SQLAlchemy will log SQL statements (helpful for debugging).
    - pool_pre_ping: If True, enable pool "pre-ping" to avoid stale connections.
    - connect_args: Optional DBAPI-specific arguments (e.g. {"sslmode": "require"}).
    - future: Use SQLAlchemy 2.0 style execution when supported (default True).
    - kwargs: Additional keyword args passed through to sqlalchemy.create_engine.

    Returns:
    - sqlalchemy.engine.Engine

    Usage examples:
        # Using a full connection string:
        engine = make_engine("postgresql+psycopg2://user:pass@db.example.com:5432/mydb")

        # Using discrete components:
        engine = make_engine(
            drivername="postgresql+psycopg2",
            username="user",
            password="pass",
            host="db.example.com",
            port=5432,
            database="mydb",
            pool_size=10,
            max_overflow=20,
        )

        # When done with the engine:
        engine.dispose()
    """
    if conn_str:
        url = conn_str
    else:
        if not drivername:
            raise ValueError("drivername is required when conn_str is not provided")
        url = URL.create(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )

    # Ensure connect_args is a dict if None
    connect_args = connect_args or {}

    engine = create_engine(
        url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        echo=echo,
        pool_pre_ping=pool_pre_ping,
        connect_args=connect_args,
        future=future,
        **kwargs,
    )
    return engine

"""
Save a pandas DataFrame into a SQLite table using SQLAlchemy, with upsert support.

Fixes the issue where an extra empty "index" column is created when calling the function
with index=True by ensuring the DataFrame used for INSERT/UPSERT includes the index
column (reset and renamed to the same name used when creating the table).
"""
from typing import Optional, List, Union, Sequence, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, text, inspect, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine


def save_df_to_sqlite_upsert(
    df: pd.DataFrame,
    db_path: str,
    table_name: str,
    *,
    index: bool = False,
    index_label: Optional[str] = None,
    chunksize: Optional[int] = None,
    conflict_columns: Optional[List[str]] = None,
    auto_detect_conflict: bool = True,
    create_index_if_missing: bool = False,
    index_name: Optional[str] = None,
    update_columns: Optional[List[str]] = None,
) -> None:
    """
    Save DataFrame to SQLite via SQLAlchemy and optionally perform upserts.

    Key fix: when index=True ensure the DataFrame used for insertion includes the index
    as a column named exactly as pandas.to_sql would have created it (index_label or index.name or 'index').
    """
    if chunksize is not None and chunksize <= 0:
        raise ValueError("chunksize must be None or a positive integer")

    engine = create_engine(f"sqlite:///{db_path}", future=True)
    inspector = inspect(engine)
    metadata = MetaData()

    # Ensure the table exists; if not, create it from df (zero rows)
    if table_name not in inspector.get_table_names():
        # create table schema using pandas (it will create the index column if index=True)
        df.iloc[0:0].to_sql(
            name=table_name,
            con=engine,
            if_exists="fail",
            index=index,
            index_label=index_label,
        )

    # reflect table
    metadata.reflect(bind=engine, only=[table_name])
    table: Table = metadata.tables[table_name]
    table_cols = [c.name for c in table.columns]

    def _validate_cols_exist(cols: List[str], where: str = "provided"):
        missing = [c for c in cols if c not in table_cols]
        if missing:
            raise ValueError(f"{where} columns not found in target table '{table_name}': {missing}")

    # Determine conflict columns (explicit or auto-detected)
    detected_conflict_cols: Optional[List[str]] = None
    if conflict_columns:
        _validate_cols_exist(conflict_columns, "conflict_columns")
        detected_conflict_cols = conflict_columns[:]
    else:
        if auto_detect_conflict:
            pk = inspector.get_pk_constraint(table_name).get("constrained_columns") or []
            if pk:
                detected_conflict_cols = pk
            else:
                uniqs = inspector.get_unique_constraints(table_name) or []
                for u in uniqs:
                    cols = u.get("column_names") or []
                    if cols:
                        detected_conflict_cols = cols
                        break
                if detected_conflict_cols is None:
                    indexes = inspector.get_indexes(table_name) or []
                    for idx in indexes:
                        if idx.get("unique"):
                            cols = idx.get("column_names") or []
                            if cols:
                                detected_conflict_cols = cols
                                break

    # If no conflict target found -> plain append
    if detected_conflict_cols is None:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            method="multi",
        )
        engine.dispose()
        return

    _validate_cols_exist(detected_conflict_cols, "conflict")
    if update_columns is None:
        update_columns = [c for c in table_cols if c not in detected_conflict_cols]
    else:
        _validate_cols_exist(update_columns, "update_columns")

    # Ensure a UNIQUE index exists on detected_conflict_cols if requested
    if create_index_if_missing:
        matching = False
        uniqs = inspector.get_unique_constraints(table_name) or []
        for u in uniqs:
            cols = u.get("column_names") or []
            if cols == detected_conflict_cols:
                matching = True
                break
        if not matching:
            indexes = inspector.get_indexes(table_name) or []
            for idx in indexes:
                if idx.get("unique") and idx.get("column_names") == detected_conflict_cols:
                    matching = True
                    break
        if not matching:
            if not index_name:
                cols_part = "_".join(detected_conflict_cols)
                index_name = f"uq_{table_name}_{cols_part}"
            cols_sql = ", ".join([f'"{c}"' for c in detected_conflict_cols])
            create_idx_sql = f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ({cols_sql})'
            with engine.begin() as conn:
                conn.execute(text(create_idx_sql))
            inspector = inspect(engine)

    # Build DataFrame used for insertion: include index column when index=True
    if index:
        # Determine the column name pandas.to_sql used for the index
        idx_col_name = index_label or (df.index.name if df.index.name else "index")
        df_for_insert = df.reset_index()
        # reset_index uses index.name if set, otherwise 'index' — ensure it matches idx_col_name
        if df_for_insert.columns[0] != idx_col_name:
            df_for_insert = df_for_insert.rename(columns={df_for_insert.columns[0]: idx_col_name})
    else:
        df_for_insert = df

    # Validate that columns in df_for_insert exist in the target table
    missing_cols = [c for c in df_for_insert.columns if c not in table_cols]
    if missing_cols:
        raise ValueError(f"columns in DataFrame not present in target table '{table_name}': {missing_cols}")

    records = df_for_insert.to_dict(orient="records")
    if not records:
        engine.dispose()
        return

    batch_size = chunksize or len(records)

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            chunk = records[i : i + batch_size]
            ins = sqlite_insert(table).values(chunk)
            if update_columns:
                set_dict = {col: ins.excluded[col] for col in update_columns}
                stmt = ins.on_conflict_do_update(index_elements=detected_conflict_cols, set_=set_dict)
            else:
                stmt = ins.on_conflict_do_nothing(index_elements=detected_conflict_cols)
            conn.execute(stmt)

    engine.dispose()

"""
read_sqlalchemy.py

Utility to read data from a SQL database using SQLAlchemy and return a pandas DataFrame.

Function:
    read_table_to_df(engine_or_conn_str, table_name, selected_columns='*', where_condition=None, schema=None)

Arguments:
    engine_or_conn_str: SQLAlchemy Engine instance OR a database connection string (e.g. "postgresql+psycopg2://user:pass@host/db")
    table_name: str, table name in the database
    selected_columns: list[str] or tuple[str] or '*' (default) — columns to select
    where_condition: None, dict, or str
        - dict: { 'col1': value1, 'col2': value2 } will be parameterized safely and combined with AND
        - str: raw SQL text used in WHERE (use carefully; prone to SQL injection if untrusted input)
    schema: optional schema name (for databases that support schemas)
"""

def read_table_to_df(
    engine_or_conn_str: Union[Engine, str],
    table_name: str,
    selected_columns: Union[Sequence[str], str] = '*',
    where_condition: Optional[Union[Dict[str, Any], str]] = None,
    schema: Optional[str] = None,
) -> pd.DataFrame:
    """
    Read rows from a SQL table into a pandas DataFrame using SQLAlchemy.

    Returns:
        pandas.DataFrame with the selected rows/columns.

    Usage examples:
        # Using connection string
        df = read_table_to_df("sqlite:///my.db", "users", ["id", "name"], {"active": True})

        # Using existing engine
        engine = create_engine("postgresql+psycopg2://...")
        df = read_table_to_df(engine, "orders", "*", "created_at >= '2025-01-01'")

    Notes:
        - Prefer passing where_condition as a dict for safe parameterization.
        - If where_condition is a string, it will be used as raw SQL in the WHERE clause.
    """
    # Create or reuse engine
    created_engine = False
    if isinstance(engine_or_conn_str, Engine):
        engine = engine_or_conn_str
    else:
        engine = create_engine(engine_or_conn_str)
        created_engine = True

    metadata = MetaData()
    try:
        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
    except Exception as e:
        # Provide a clearer error if reflection fails
        raise RuntimeError(f"Failed to reflect table '{table_name}': {e}") from e

    # Determine columns to select
    if selected_columns == '*' or selected_columns is None:
        stmt = select(table)
    else:
        if isinstance(selected_columns, str):
            # single column name provided as string
            selected_columns = [selected_columns]
        try:
            cols = [table.c[col] for col in selected_columns]
        except KeyError as e:
            raise ValueError(f"Selected column not found in table {table_name}: {e}") from e
        stmt = select(*cols)

    # Build where clause
    if where_condition is None:
        pass
    elif isinstance(where_condition, dict):
        # Combine equality conditions with AND. SQLAlchemy will parameterize values.
        for col, val in where_condition.items():
            if col not in table.c:
                raise ValueError(f"Column for where condition not found in table {table_name}: {col}")
            stmt = stmt.where(table.c[col] == val)
    elif isinstance(where_condition, str):
        # Raw SQL text WHERE clause (use carefully)
        stmt = stmt.where(text(where_condition))
    else:
        raise TypeError("where_condition must be None, a dict, or a SQL string.")

    # Execute and return DataFrame
    try:
        with engine.connect() as conn:
            result = conn.execute(stmt)
            rows = result.fetchall()
            cols = result.keys()
            df = pd.DataFrame(rows, columns=cols)
            return df
    finally:
        if created_engine:
            # If we created the engine here, dispose it to free resources
            engine.dispose()