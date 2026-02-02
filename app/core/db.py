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
