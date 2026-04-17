"""ClickHouse helper utilities for dlt pipeline protection and cleanup.

NOTE: This module reads ClickHouse credentials from environment variables that were
set by vault_env.build_clickhouse_env() in the manifest_runner.py context.

The credentials are injected into os.environ via temporary_environ() context manager
before calling these functions, so we safely read from os.getenv().
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List

from dltaf.services.execution.redaction import safe_exception_message

logger = logging.getLogger(__name__)


def get_clickhouse_client():
    """Get ClickHouse client from environment credentials.
    
    NOTE: Credentials must be in os.environ (set by vault_env.build_clickhouse_env).
    This is guaranteed when called from manifest_runner.py context.

    """
    try:
        import clickhouse_connect
    except ImportError:
        logger.warning("clickhouse-connect not available, cleanup/protection features disabled")
        return None
    
    # These env vars are set by vault_env.build_clickhouse_env() from Vault secret
    host = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST")
    if not host:
        logger.warning("ClickHouse host not configured, cleanup/protection features disabled")
        return None
    
    port = int(os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT", "8123"))
    username = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME", "default")
    password = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD", "")
    database = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE", "default")
    secure = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE", "0") == "1"
    
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            secure=secure,
            database=database,
        )
        return client
    except Exception as e:
        logger.warning("Could not create ClickHouse client: %s", safe_exception_message(e))
        return None


def check_tables_exist(
    dataset_name: str,
    table_names: List[str],
    dataset_separator: str = "__",
) -> Dict[str, bool]:
    """Check which tables exist in ClickHouse dataset"""
    client = get_clickhouse_client()
    if not client:
        # Assume tables exist if we can't check
        return {table: True for table in table_names}
    
    try:
        # Get all tables in the dataset
        query = f"SELECT name FROM system.tables WHERE database = '{dataset_name}'"
        result = client.query(query)
        existing_tables = set(row[0] for row in result.result_rows)
        
        # Check various naming patterns
        table_status = {}
        for table_name in table_names:
            # Try different naming patterns that dlt might use
            possible_names = [
                f"{dataset_name}{dataset_separator}{table_name}",  # dataset__table
                table_name,  # plain table name
            ]
            table_status[table_name] = any(name in existing_tables for name in possible_names)
        
        return table_status
    
    except Exception as e:
        logger.warning("Could not check table existence: %s", safe_exception_message(e))
        # Assume tables exist on error (safer for replace)
        return {table: True for table in table_names}


def cleanup_staging_tables(dataset_name: str) -> int:
    """Drop orphaned staging tables in ClickHouse dataset.
    
    Staging tables are temporary tables created by dlt that should be removed
    after successful loads. Sometimes they remain after failed runs.
    
    """
    client = get_clickhouse_client()
    if not client:
        return 0
    
    try:
        # Get all tables in the dataset
        query = f"SELECT name FROM system.tables WHERE database = '{dataset_name}'"
        result = client.query(query)
        all_tables = [row[0] for row in result.result_rows]
        
        # Find staging tables (various patterns)
        staging_tables = [
            t for t in all_tables 
            if "staging" in t.lower()
        ]
        
        dropped_count = 0
        for staging_table in staging_tables:
            # Try to find corresponding main table
            main_table_candidates = []
            
            # Pattern 1: staging_{name} -> {name}
            if staging_table.startswith("staging_"):
                main_table_candidates.append(staging_table.replace("staging_", "", 1))
            
            # Pattern 2: {dataset}_staging___{table} -> {dataset}___{table}
            if f"{dataset_name}_staging___" in staging_table:
                main_table_candidates.append(
                    staging_table.replace(f"{dataset_name}_staging___", f"{dataset_name}___", 1)
                )
            
            # Pattern 3: {dataset}_staging__{table} -> {dataset}__{table}
            if f"{dataset_name}_staging__" in staging_table:
                main_table_candidates.append(
                    staging_table.replace(f"{dataset_name}_staging__", f"{dataset_name}__", 1)
                )
            
            # Pattern 4: {name}_staging -> {name}
            if staging_table.endswith("_staging"):
                main_table_candidates.append(staging_table.replace("_staging", "", 1))
            
            # Check if main table exists
            main_table_exists = any(candidate in all_tables for candidate in main_table_candidates)
            
            if not main_table_exists:
                # Main table doesn't exist, drop staging table
                try:
                    client.command(f"DROP TABLE IF EXISTS `{dataset_name}`.`{staging_table}`")
                    logger.info("Dropped orphaned staging table: %s", staging_table)
                    dropped_count += 1
                except Exception as e:
                    logger.warning(
                        "Could not drop staging table %s: %s",
                        staging_table,
                        safe_exception_message(e),
                    )
        
        if dropped_count > 0:
            logger.info("Cleaned up %d orphaned staging table(s)", dropped_count)
        
        return dropped_count
    
    except Exception as e:
        logger.warning("Could not cleanup staging tables: %s", safe_exception_message(e))
        return 0


def drop_pending_packages(pipeline) -> bool:
    """Drop pending packages from dlt pipeline state.
    
    Args:
        pipeline: dlt.Pipeline instance
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Try to use dlt's drop_pending_packages method if available
        if hasattr(pipeline, 'drop_pending_packages'):
            pipeline.drop_pending_packages()
            logger.info("Dropped pending packages successfully")
            return True
        
        # Try alternative methods
        if hasattr(pipeline, '_pipeline'):
            inner_pipeline = pipeline._pipeline
            if hasattr(inner_pipeline, 'drop_pending_packages'):
                inner_pipeline.drop_pending_packages()
                logger.info("Dropped pending packages successfully")
                return True
        
        logger.warning("drop_pending_packages method not available in this dlt version")
        return False
    
    except Exception as e:
        logger.warning("Could not drop pending packages: %s", safe_exception_message(e))
        return False


def get_expected_table_names(manifest: Dict) -> List[str]:
    """Extract expected table names from manifest.
    
    Args:
        manifest: loaded manifest dict
    
    Returns:
        List of expected table names
    """
    source_cfg = manifest.get("source") or {}
    kind = source_cfg.get("kind")
    
    tables = []
    
    if kind == "sqldb":
        mode = str(source_cfg.get("mode") or "").strip().lower()
        if mode == "catalog":
            catalog = source_cfg.get("catalog") or {}
            schemas = catalog.get("schemas")
            if schemas:
                for schema_name, scfg in schemas.items():
                    schema_tables = (scfg or {}).get("tables") or []
                    for table in schema_tables:
                        tables.append(f"{schema_name}__{table}")
            else:
                schema_tables = catalog.get("tables") or []
                table_name_transform = catalog.get("table_name_transform") or {}
                drop_prefix = table_name_transform.get("drop_prefix")
                for table in schema_tables:
                    table_name = table
                    if drop_prefix and str(table_name).startswith(str(drop_prefix)):
                        table_name = str(table_name)[len(str(drop_prefix)) :]
                    tables.append(str(table_name))
        elif mode == "query":
            queries = ((source_cfg.get("query") or {}).get("queries") or [])
            for q in queries:
                table_name = q.get("table_name") or q.get("name")
                if table_name:
                    tables.append(str(table_name))

    elif kind == "oracle_custom_sql":
        queries = source_cfg.get("queries") or []
        for q in queries:
            table_name = q.get("table_name") or q.get("name")
            if table_name:
                tables.append(str(table_name))

    elif kind in {"sql_database", "oracle"}:
        if kind == "oracle":
            queries = source_cfg.get("queries") or []
            for q in queries:
                table_name = q.get("table_name") or q.get("name")
                if table_name:
                    tables.append(str(table_name))
            return tables

        # Multi-schema mode
        schemas = source_cfg.get("schemas")
        if schemas:
            for schema_name, scfg in schemas.items():
                schema_tables = scfg.get("tables") or []
                for table in schema_tables:
                    # In multi-schema mode, dlt creates tables like: {schema}__{table}
                    tables.append(f"{schema_name}__{table}")
        else:
            # Single schema mode
            schema_tables = source_cfg.get("tables") or []
            
            # Apply table_name_transform if present
            table_name_transform = source_cfg.get("table_name_transform") or {}
            drop_prefix = table_name_transform.get("drop_prefix")
            
            for table in schema_tables:
                table_name = table
                if drop_prefix and table_name.startswith(drop_prefix):
                    table_name = table_name[len(drop_prefix):]
                tables.append(table_name)
    
    elif kind == "mongodb":
        collections = source_cfg.get("collection_names") or []
        tables.extend([str(c) for c in collections])

    return tables


def fetch_first_column_values(query: str) -> List[str]:
    """Execute a ClickHouse query and return the first column as a list of strings.

    This helper is useful for resolving reference lists (eg, BINs) from ClickHouse
    without hardcoding large arrays in YAML manifests.

    Notes:
        - Uses `clickhouse_connect` (usually present when `dlt[clickhouse]` is installed).
        - Connection settings are taken from `DESTINATION__CLICKHOUSE__CREDENTIALS__*` env vars.

    Args:
        query: SQL query that returns at least one column.

    Returns:
        List of values from the first column.

    Raises:
        RuntimeError: if ClickHouse client cannot be created.
    """

    client = get_clickhouse_client()
    if client is None:
        raise RuntimeError(
            "ClickHouse client is not available. "
            "Ensure clickhouse-connect (or dlt[clickhouse]) is installed and DESTINATION__CLICKHOUSE__CREDENTIALS__* env vars are set."
        )

    result = client.query(query)
    rows = result.result_rows or []
    return [str(r[0]) for r in rows if r and len(r) > 0]
