from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence

import yaml
from dltaf.plugins import (
    PluginRegistry,
    RegisteredSourcePlugin,
    SourcePlugin,
    SourcePluginValidationError,
    build_source_registry,
    env_plugin_modules,
    env_plugin_paths,
)

from dlt_utils.naming import validate_pipeline_name
from dlt_utils.oracle_serialization import serialize_oracle_value
from dlt_utils.vault_env import (
    apply_overrides,
    build_clickhouse_env,
    build_keycloak_env,
    build_mongodb_env,
    build_sql_database_env,
    get_secret_from_vault,
    parse_vault_ref,
    temporary_environ,
)
from dlt_utils.clickhouse_helpers import (
    check_tables_exist,
    cleanup_staging_tables,
    drop_pending_packages,
    get_expected_table_names,
)
from lineage.manifest_dependency_resolver import _extract_depends_on_simple, InvalidDependencyError

logger = logging.getLogger(__name__)


_ENV_PATTERN = re.compile(r"^\$\{ENV:([A-Z0-9_]+)(?:\|([^}]*))?\}$")


def _today_str() -> str:
    return datetime.now().strftime("%Y/%m/%d")


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _airflow_variable_lookup_enabled() -> bool:
    return any(
        key == "AIRFLOW_HOME" or key.startswith(("AIRFLOW__", "AIRFLOW_CTX_"))
        for key in os.environ
    )


def _get_env_or_airflow_var(name: str, default: str = "") -> str:
    """Resolve a manifest variable with precedence ENV -> Airflow Variable -> default."""

    raw = os.getenv(name)
    if not _is_blank(raw):
        return str(raw)

    raw = os.getenv(f"AIRFLOW_VAR_{name}")
    if not _is_blank(raw):
        return str(raw)

    if _airflow_variable_lookup_enabled():
        try:
            from airflow.models import Variable

            raw = Variable.get(name, default_var=None)
            if not _is_blank(raw):
                return str(raw)
        except Exception:
            pass

    return default


def _resolve_str(value: str) -> Any:
    """Resolve supported templating in strings.

    Supported:
      - TODAY_STR
      - ${ENV:VAR|default} with ENV -> Airflow Variable -> default precedence
    """
    if value == "TODAY_STR":
        return _today_str()

    m = _ENV_PATTERN.match(value)
    if not m:
        return value

    env_key = m.group(1)
    default = m.group(2) if m.group(2) is not None else ""
    raw = _get_env_or_airflow_var(env_key, default)
    if raw == "TODAY_STR":
        return _today_str()
    return raw


def resolve_manifest(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: resolve_manifest(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_manifest(v) for v in obj]
    if isinstance(obj, str):
        return _resolve_str(obj)
    return obj


def load_manifest(path: str | Path) -> Dict[str, Any]:
    p = Path(path).expanduser().resolve()
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Manifest must be a YAML mapping, got: {type(data)}")
    data = resolve_manifest(data)
    data["__manifest_path__"] = str(p)
    return data


def _read_sql_file(sql_path: Path) -> str:
    sql = sql_path.read_text(encoding="utf-8")
    if "{" in sql or "}" in sql:
        # prevents accidental .format() placeholders in SQL
        raise ValueError(
            f"SQL file contains '{{' or '}}' which is forbidden for safety: {sql_path}"
        )
    return sql


def _extract_bind_params(sql: str) -> set[str]:
    # very light heuristic: :param
    return set(re.findall(r":([A-Za-z_][A-Za-z0-9_]*)", sql))


def _validate_oracle_custom_sql_source(
    source: Mapping[str, Any],
    manifest: Mapping[str, Any],
    *,
    base_dir: Path,
) -> None:
    queries = source.get("queries")
    if not isinstance(queries, list) or not queries:
        raise ValueError("source.queries must be a non-empty list")
    for q in queries:
        if not isinstance(q, Mapping):
            raise ValueError(f"query must be a mapping, got: {type(q)}")
        qname = str(q.get("name") or "").strip()
        if not qname:
            raise ValueError("query.name is required")
        sql_file = str(q.get("sql_file") or "").strip()
        if not sql_file:
            raise ValueError(f"query '{qname}': sql_file is required")

        sql_path = (base_dir / sql_file).resolve()
        if not sql_path.exists():
            raise ValueError(f"query '{qname}': sql_file does not exist: {sql_path}")
        sql = _read_sql_file(sql_path)

        write_disposition = str(
            q.get("write_disposition") or manifest.get("run", {}).get("write_disposition") or ""
        ).strip().lower()
        primary_key = q.get("primary_key")

        if write_disposition == "merge":
            if not isinstance(primary_key, list) or not primary_key:
                raise ValueError(
                    f"query '{qname}': primary_key is required for write_disposition=merge"
                )

        params = q.get("params") or {}
        if params is None:
            params = {}
        if not isinstance(params, Mapping):
            raise ValueError(f"query '{qname}': params must be a mapping")

        bind_params = _extract_bind_params(sql)
        missing = bind_params.difference(set(params.keys()))
        if missing:
            raise ValueError(
                f"query '{qname}': missing params for SQL bind placeholders: {sorted(missing)}"
            )


def _validate_sql_database_source(source: Mapping[str, Any]) -> None:
    schemas = source.get("schemas")
    schema = source.get("schema")
    tables = source.get("tables")
    if schemas is None:
        if not schema or not tables:
            raise ValueError("sql_database source requires either 'schemas' or ('schema' and 'tables')")
    else:
        if not isinstance(schemas, Mapping) or not schemas:
            raise ValueError("source.schemas must be a non-empty mapping")
        for sname, scfg in schemas.items():
            if not isinstance(scfg, Mapping) or not scfg.get("tables"):
                raise ValueError(f"schema '{sname}' must define tables")


def _validate_mongodb_source(source: Mapping[str, Any]) -> None:
    cols = source.get("collection_names")
    if cols is not None and not isinstance(cols, list):
        raise ValueError("source.collection_names must be a list")


def _manifest_source_config(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    source = manifest.get("source") or {}
    if not isinstance(source, Mapping):
        raise ValueError("source section must be a mapping")
    return source


def _manifest_base_dir(manifest: Mapping[str, Any]) -> Path:
    manifest_path = str(manifest.get("__manifest_path__") or "").strip()
    if manifest_path:
        return Path(manifest_path).expanduser().resolve().parent
    return Path.cwd()


def _validate_oracle_custom_sql_manifest(manifest: Mapping[str, Any]) -> None:
    _validate_oracle_custom_sql_source(
        _manifest_source_config(manifest),
        manifest,
        base_dir=_manifest_base_dir(manifest),
    )


def _validate_sql_database_manifest(manifest: Mapping[str, Any]) -> None:
    _validate_sql_database_source(_manifest_source_config(manifest))


def _validate_mongodb_manifest(manifest: Mapping[str, Any]) -> None:
    _validate_mongodb_source(_manifest_source_config(manifest))


def _default_private_plugin_catalog_paths() -> list[Path]:
    repo_root = Path(__file__).resolve().parents[3]
    default_path = repo_root / "internal" / "dltaf_plugins"
    if default_path.exists():
        return [default_path]
    return []


def _builtin_source_plugins() -> tuple[SourcePlugin, ...]:
    return (
        SourcePlugin(
            kind="oracle_custom_sql",
            validate=_validate_oracle_custom_sql_manifest,
            run=_run_oracle_custom_sql,
            display_name="Oracle custom SQL",
            docs_url="https://paulkov.github.io/dltaf/examples/",
            required_packages=("oracledb>=2.0.0", "sqlalchemy>=2.0.25"),
            healthcheck_hint="Validate SQL files, Oracle credentials, and merge primary keys.",
        ),
        SourcePlugin(
            kind="sql_database",
            validate=_validate_sql_database_manifest,
            run=_run_sql_database,
            display_name="SQL database",
            docs_url="https://paulkov.github.io/dltaf/examples/",
            required_packages=("dlt[sql_database]>=1.18.2,<2",),
            healthcheck_hint="Check schema/table selection and source database credentials.",
        ),
        SourcePlugin(
            kind="mongodb",
            validate=_validate_mongodb_manifest,
            run=_run_mongodb,
            display_name="MongoDB",
            docs_url="https://paulkov.github.io/dltaf/examples/",
            required_packages=("pymongo>=4.6.0",),
            healthcheck_hint="Confirm Mongo connection URL and collection selection.",
        ),
    )


@lru_cache(maxsize=None)
def _cached_source_registry(
    plugin_paths_signature: tuple[str, ...],
    plugin_modules_signature: tuple[str, ...],
) -> PluginRegistry:
    plugin_paths = [Path(item) for item in plugin_paths_signature]
    return build_source_registry(
        builtins=_builtin_source_plugins(),
        plugin_paths=plugin_paths,
        plugin_modules=plugin_modules_signature,
    )


def get_source_registry() -> PluginRegistry:
    default_paths = _default_private_plugin_catalog_paths()
    plugin_paths_signature = tuple(
        str(path) for path in env_plugin_paths(default_paths=default_paths)
    )
    plugin_modules_signature = tuple(env_plugin_modules())
    return _cached_source_registry(plugin_paths_signature, plugin_modules_signature)


def clear_source_registry_cache() -> None:
    _cached_source_registry.cache_clear()


def _resolve_source_plugin(kind: str) -> RegisteredSourcePlugin:
    return get_source_registry().resolve(kind)


def _validate_with_source_plugin(
    plugin: RegisteredSourcePlugin,
    manifest: Mapping[str, Any],
) -> None:
    if plugin.plugin.validate is None:
        return
    try:
        plugin.plugin.validate(manifest)
    except SourcePluginValidationError:
        raise
    except Exception as exc:
        raise SourcePluginValidationError(plugin=plugin, cause=exc) from exc


def _build_runtime_env_from_plugin(
    plugin: RegisteredSourcePlugin,
    manifest: Mapping[str, Any],
) -> dict[str, str]:
    if plugin.plugin.build_runtime_env is None:
        return {}

    try:
        runtime_env = plugin.plugin.build_runtime_env(manifest) or {}
    except Exception as exc:
        raise RuntimeError(
            f"Failed to build runtime env for source.kind {plugin.requested_kind!r} "
            f"(resolved to {plugin.kind!r} from {plugin.origin}): {exc}"
        ) from exc

    if not isinstance(runtime_env, Mapping):
        raise TypeError(
            f"Plugin runtime env must be a mapping, got {type(runtime_env).__name__}"
        )

    return {str(key): str(value) for key, value in runtime_env.items()}


def validate_manifest(manifest: Mapping[str, Any]) -> None:
    version = manifest.get("version")
    if version != 1:
        raise ValueError(f"Unsupported manifest version: {version!r} (expected 1)")

    pipeline = manifest.get("pipeline") or {}
    if not isinstance(pipeline, Mapping):
        raise ValueError("pipeline section must be a mapping")

    name = str(pipeline.get("name") or "").strip()
    if not name:
        raise ValueError("pipeline.name is required")
    validate_pipeline_name(name)
    
    # Validate depends_on field (if present)
    manifest_path = Path(str(manifest.get("__manifest_path__") or ""))
    try:
        depends_on = _extract_depends_on_simple(manifest, manifest_path)
        # Basic validation: ensure no self-dependency
        if name in depends_on:
            raise ValueError(
                f"Pipeline '{name}' cannot depend on itself. "
                f"Self-referencing dependency detected in depends_on."
            )
        logger.debug(f"Pipeline '{name}' depends_on: {depends_on}")
    except InvalidDependencyError as e:
        raise ValueError(f"Invalid depends_on in manifest: {e}") from e

    # Company convention: the manifest file name (without extension) is the pipeline identifier.
    # This enables:
    #   * stable naming across dlt pipeline_name + airflow dag_id
    #   * manual runs "by name" without looking inside YAML
    manifest_path = Path(str(manifest.get("__manifest_path__") or ""))
    if manifest_path.name:
        expected_name = manifest_path.stem
        if expected_name != name:
            raise ValueError(
                "pipeline.name must match manifest filename. "
                f"Expected '{expected_name}', got '{name}'."
            )

    airflow_cfg = manifest.get("airflow") or {}
    if airflow_cfg and isinstance(airflow_cfg, Mapping):
        dag_id = airflow_cfg.get("dag_id")
        if dag_id and str(dag_id).strip() != name:
            raise ValueError(
                "airflow.dag_id must match pipeline.name. "
                f"Expected '{name}', got '{dag_id}'."
            )

    dest = str(pipeline.get("destination") or "").strip()
    if not dest:
        raise ValueError("pipeline.destination is required")

    dataset = str(pipeline.get("dataset") or "").strip()
    if not dataset:
        raise ValueError("pipeline.dataset is required")

    source = manifest.get("source") or {}
    if not isinstance(source, Mapping):
        raise ValueError("source section must be a mapping")

    kind = str(source.get("kind") or "").strip()
    if not kind:
        raise ValueError("source.kind is required")

    plugin = _resolve_source_plugin(kind)
    _validate_with_source_plugin(plugin, manifest)


# ---------------------------------------------------------------------------
# Pipeline builders
# ---------------------------------------------------------------------------
# Generic replace protection wrapper
# ---------------------------------------------------------------------------


def _run_with_replace_protection(
    pipeline: Any,
    source_factory: Any,
    manifest: Mapping[str, Any],
    write_disposition: str,
) -> Any:
    """Generic wrapper for replace mode protection.
    
    Handles:
    - Cleanup (pending packages, staging tables)
    - Table existence check
    - Auto-create tables with append if missing
    - Automatic retry on errors
    
    """
    pipeline_cfg = manifest.get("pipeline") or {}
    dataset_name = pipeline_cfg.get("dataset", "")
    
    # Protection for replace mode
    if write_disposition == "replace":
        logger.info("Running cleanup before replace operation...")
        drop_pending_packages(pipeline)
        cleanup_staging_tables(dataset_name)
        
        # Check if tables exist
        expected_tables = get_expected_table_names(manifest)
        
        if expected_tables:
            dataset_separator = os.getenv(
                "DESTINATION__CLICKHOUSE__CREDENTIALS__DATASET_TABLE_SEPARATOR", "__"
            )
            table_status = check_tables_exist(dataset_name, expected_tables, dataset_separator)
            missing_tables = [table for table, exists in table_status.items() if not exists]
            
            if missing_tables:
                logger.warning("Tables do not exist yet: %s", missing_tables)
                logger.info("Creating tables first with append mode...")
                try:
                    # First run with append to create tables
                    source_append = source_factory()
                    pipeline.run(source_append, write_disposition="append")
                    logger.info("Tables created successfully, now proceeding with replace...")
                except Exception as e:
                    logger.error("Failed to create tables with append: %s", e)
                    raise
    
    # Run the pipeline
    try:
        source = source_factory()
        load_info = pipeline.run(source, write_disposition=write_disposition)
        logger.info("dlt load finished: %s", load_info)
        return load_info
    
    except Exception as e:
        error_msg = str(e)
        
        # Check if error is related to non-existent tables or pending packages
        is_table_error = "does not exist" in error_msg.lower()
        is_staging_error = "staging" in error_msg.lower() and "does not exist" in error_msg.lower()
        is_pending_error = "pending packages" in error_msg.lower()
        
        if (is_table_error or is_staging_error or is_pending_error) and write_disposition == "replace":
            logger.warning("Detected table/staging/pending error, attempting automatic recovery...")
            
            # Cleanup
            drop_pending_packages(pipeline)
            cleanup_staging_tables(dataset_name)
            
            # Retry: create tables with append, then replace
            logger.info("Step 1: Creating tables with append mode...")
            try:
                source_retry = source_factory()
                pipeline.run(source_retry, write_disposition="append")
                logger.info("Tables created successfully")
                
                logger.info("Step 2: Running with replace mode...")
                source_retry2 = source_factory()
                load_info = pipeline.run(source_retry2, write_disposition="replace")
                logger.info("dlt load finished after recovery: %s", load_info)
                return load_info
            except Exception as retry_error:
                logger.error("Retry failed: %s", retry_error)
                raise retry_error from e
        
        # Re-raise original exception if we couldn't handle it
        raise


# ---------------------------------------------------------------------------
# Oracle custom SQL runner
# ---------------------------------------------------------------------------


def _run_oracle_custom_sql(manifest: Mapping[str, Any]) -> Any:
    """Run an oracle_custom_sql pipeline defined in YAML."""
    import dlt
    from sqlalchemy import text
    from sqlalchemy.engine import Engine
    from sqlalchemy import create_engine

    try:
        import oracledb
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "oracledb is required for oracle_custom_sql. Install 'oracledb>=2.0.0'."
        ) from e

    thick_mode_enabled = False
    try:
        oracledb.init_oracle_client()
        thick_mode_enabled = True
        print("✓ Oracle thick mode enabled successfully")
    except Exception as thick_err:
        print(f"✗ Warning: Could not enable thick mode: {thick_err}")
        print("  Help: https://python-oracledb.readthedocs.io/en/latest/user_guide/troubleshooting.html#dpi-1047")
        print("  Continuing with thin mode (may not work with Oracle < 12.1)")

    base_dir = Path(str(manifest.get("__manifest_path__"))).parent
    source_cfg = manifest.get("source") or {}
    pipeline_cfg = manifest.get("pipeline") or {}
    run_cfg = manifest.get("run") or {}

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_cfg["name"],
        destination=pipeline_cfg.get("destination", "clickhouse"),
        dataset_name=pipeline_cfg.get("dataset"),
        progress=pipeline_cfg.get("progress", "log"),
        dev_mode=bool(pipeline_cfg.get("dev_mode", False)),
    )

    # --- Credentials from ENV (already injected from vault)
    host = os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__HOST")
    port = int(os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__PORT", "1521"))
    username = os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME")
    password = os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD")
    database = os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE")
    dsn_str = os.getenv("SOURCES__SQL_DATABASE__CREDENTIALS__DSN", "").strip()
    
    print("Oracle connection details:")
    print(f"  Host: {host}:{port}")
    print(f"  Database/SID: {database}")
    print(f"  Username: {username}")
    print(f"  DSN: {dsn_str if dsn_str else 'not set'}")
    print(f"  Thick mode: {'enabled' if thick_mode_enabled else 'disabled (thin mode)'}")

    if not host or not username or not password:
        raise ValueError("Oracle credentials are not configured in ENV")

    def make_engine() -> Engine:
        """Create SQLAlchemy engine for Oracle connection using oracledb (thick mode if available)"""
        # Prefer DSN from secret (same approach as Airflow OracleHook)
        if dsn_str:
            def _connect():
                try:
                    print(f"Attempting Oracle connection with DSN: {dsn_str}")
                    conn = oracledb.connect(user=username, password=password, dsn=dsn_str)
                    # Try to get Oracle version for debugging
                    try:
                        cursor = conn.cursor()
                        cursor.execute("SELECT BANNER FROM v$version WHERE ROWNUM = 1")
                        version = cursor.fetchone()
                        print(f"✓ Connected to Oracle: {version[0] if version else 'version unknown'}")
                        cursor.close()
                    except Exception:
                        print("✓ Connected to Oracle (version check failed)")
                    return conn
                except oracledb.DatabaseError as e:
                    error_obj, = e.args
                    print("✗ Oracle connection failed:")
                    print(f"  Error code: {error_obj.code}")
                    print(f"  Message: {error_obj.message}")
                    if error_obj.code == 1017:
                        print(f"  Hint: Invalid username/password for user '{username}' on database '{database}'")
                        print("  Action: Check credentials in Vault or verify user account status")
                    raise
                except oracledb.NotSupportedError as e:
                    print("✗ Oracle version not supported in thin mode:")
                    print(f"  Error: {e}")
                    print("  Solution: Install Oracle Instant Client and restart Airflow")
                    print("  See: ORACLE_INSTANT_CLIENT_INSTALL.md")
                    raise
            return create_engine(
                "oracle+oracledb://",
                creator=_connect,
                pool_pre_ping=True,
            )

        if database:
            dsn = oracledb.makedsn(host, port, sid=database)
        else:
            dsn = oracledb.makedsn(host, port)

        def _connect():
            try:
                print(f"Attempting Oracle connection: {username}@{host}:{port}/{database}")
                conn = oracledb.connect(user=username, password=password, dsn=dsn)
                # Try to get Oracle version for debugging
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT BANNER FROM v$version WHERE ROWNUM = 1")
                    version = cursor.fetchone()
                    print(f"✓ Connected to Oracle: {version[0] if version else 'version unknown'}")
                    cursor.close()
                except Exception:
                    print("✓ Connected to Oracle (version check failed)")
                return conn
            except oracledb.DatabaseError as e:
                error_obj, = e.args
                print("✗ Oracle connection failed:")
                print(f"  Error code: {error_obj.code}")
                print(f"  Message: {error_obj.message}")
                if error_obj.code == 1017:
                    print(f"  Hint: Invalid username/password for user '{username}' on database '{database}'")
                    print("  Action: Check credentials in Vault or verify user account status")
                raise
            except oracledb.NotSupportedError as e:
                print("✗ Oracle version not supported in thin mode:")
                print(f"  Error: {e}")
                print("  Solution: Install Oracle Instant Client and restart Airflow")
                print("  See: ORACLE_INSTANT_CLIENT_INSTALL.md")
                raise

        return create_engine(
            "oracle+oracledb://",
            creator=_connect,
            pool_pre_ping=True,
        )

    engine = make_engine()

    init_sql = source_cfg.get("init_sql")
    fetch_batch_size = int(source_cfg.get("fetch_batch_size") or 2000)

    queries = source_cfg.get("queries") or []

    @dlt.source(name=str(source_cfg.get("name") or "oracle_custom_sql"))
    def oracle_custom_sql_source() -> dlt.Source:
        for q in queries:
            qname = q["name"]
            table_name = q.get("table_name") or qname
            sql_path = (base_dir / str(q["sql_file"])).resolve()
            sql = _read_sql_file(sql_path)
            params = dict(q.get("params") or {})

            write_disposition = str(q.get("write_disposition") or run_cfg.get("write_disposition") or "merge")
            primary_key = q.get("primary_key")

            @dlt.resource(
                name=str(table_name),
                write_disposition=write_disposition,
                primary_key=primary_key,
            )
            def _resource(sql=sql, params=params, qname=qname) -> Iterator[Dict[str, Any]]:
                rows = 0
                with engine.connect() as conn:
                    if init_sql:
                        conn.execute(text(str(init_sql)))
                        conn.commit()

                    result = conn.execute(text(sql), params)
                    while True:
                        batch = result.fetchmany(fetch_batch_size)
                        if not batch:
                            break
                        for row in batch:
                            rows += 1
                            # Serialize Oracle-specific types (timedelta from INTERVAL columns)
                            yield serialize_oracle_value(dict(row._mapping))

                logger.info("oracle_custom_sql query '%s' finished: %s rows", qname, rows)

            yield _resource

    write_disposition = str(run_cfg.get("write_disposition") or "merge")
    
    # Use generic protection wrapper
    return _run_with_replace_protection(
        pipeline=pipeline,
        source_factory=oracle_custom_sql_source,
        manifest=manifest,
        write_disposition=write_disposition,
    )


def _run_sql_database(manifest: Mapping[str, Any]) -> Any:
    import dlt
    from dlt.sources.sql_database import sql_database

    source_cfg = manifest.get("source") or {}
    pipeline_cfg = manifest.get("pipeline") or {}
    run_cfg = manifest.get("run") or {}

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_cfg["name"],
        destination=pipeline_cfg.get("destination", "clickhouse"),
        dataset_name=pipeline_cfg.get("dataset"),
        progress=pipeline_cfg.get("progress", "log"),
        dev_mode=bool(pipeline_cfg.get("dev_mode", False)),
    )

    # Support both reflection_level (new) and detect_precision_hints (deprecated)
    reflection_level = source_cfg.get("reflection_level")
    detect_precision_hints = source_cfg.get("detect_precision_hints")
    
    # Determine which parameter to use
    if reflection_level is not None:
        # Use new parameter
        use_reflection_level = True
        reflection_level_value = str(reflection_level)
    elif detect_precision_hints is not None:
        # Fallback to deprecated parameter for backward compatibility
        use_reflection_level = False
        detect_precision_hints_value = bool(detect_precision_hints)
    else:
        # Default: use reflection_level="full"
        use_reflection_level = True
        reflection_level_value = "full"
    
    table_name_transform = source_cfg.get("table_name_transform") or {}

    def transform_table_name(schema: str, table: str) -> str:
        name = table
        drop_prefix = table_name_transform.get("drop_prefix")
        if drop_prefix and name.startswith(drop_prefix):
            name = name[len(drop_prefix) :]
        # for multi-schema mode we keep schema prefix
        return name

    schemas = source_cfg.get("schemas")

    @dlt.source(name=str(source_cfg.get("name") or "sql_database"))
    def sql_database_source() -> dlt.Source:
        if schemas:
            for schema_name, scfg in schemas.items():
                tables = list(scfg.get("tables") or [])
                
                # Create sql_database source with appropriate parameter
                if use_reflection_level:
                    schema_source = sql_database(
                        schema=schema_name,
                        table_names=tables,
                        reflection_level=reflection_level_value,
                    )
                else:
                    schema_source = sql_database(
                        schema=schema_name,
                        table_names=tables,
                        detect_precision_hints=detect_precision_hints_value,
                    )
                
                for resource in schema_source.resources.values():
                    logical_name = f"{schema_name}__{resource.name}"
                    resource.apply_hints(table_name=logical_name)
                    yield resource.with_name(logical_name)
        else:
            schema = str(source_cfg.get("schema"))
            tables = list(source_cfg.get("tables") or [])
            
            # Create sql_database source with appropriate parameter
            if use_reflection_level:
                schema_source = sql_database(
                    schema=schema,
                    table_names=tables,
                    reflection_level=reflection_level_value,
                )
            else:
                schema_source = sql_database(
                    schema=schema,
                    table_names=tables,
                    detect_precision_hints=detect_precision_hints_value,
                )
            
            for resource in schema_source.resources.values():
                logical = transform_table_name(schema, resource.name)
                resource.apply_hints(table_name=logical)
                yield resource.with_name(logical)

    write_disposition = str(run_cfg.get("write_disposition") or "merge")
    
    # Use generic protection wrapper
    return _run_with_replace_protection(
        pipeline=pipeline,
        source_factory=sql_database_source,
        manifest=manifest,
        write_disposition=write_disposition,
    )


def _run_mongodb(manifest: Mapping[str, Any]) -> Any:
    import dlt

    source_cfg = manifest.get("source") or {}
    pipeline_cfg = manifest.get("pipeline") or {}
    run_cfg = manifest.get("run") or {}

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_cfg["name"],
        destination=pipeline_cfg.get("destination", "clickhouse"),
        dataset_name=pipeline_cfg.get("dataset"),
        progress=pipeline_cfg.get("progress", "log"),
        dev_mode=bool(pipeline_cfg.get("dev_mode", False)),
    )

    from dlt_pipelines.mongodb_runtime.mongodb import mongodb

    connection_url = os.getenv("SOURCES__MONGODB__CONNECTION_URL")
    if not connection_url:
        raise ValueError("Mongo credentials are not configured in ENV")

    database = source_cfg.get("database")
    collection_names = source_cfg.get("collection_names")
    max_table_nesting = source_cfg.get("max_table_nesting")

    def mongodb_source_factory():
        """Factory to create fresh mongodb source."""
        source = mongodb(
            connection_url=connection_url,
            database=database,
            collection_names=collection_names,
            write_disposition=str(run_cfg.get("write_disposition") or "replace"),
        )
        if max_table_nesting is not None:
            try:
                source.max_table_nesting = int(max_table_nesting)
            except Exception:
                pass
        return source

    write_disposition = str(run_cfg.get("write_disposition") or "replace")
    
    # Use generic protection wrapper
    return _run_with_replace_protection(
        pipeline=pipeline,
        source_factory=mongodb_source_factory,
        manifest=manifest,
        write_disposition=write_disposition,
    )


def _build_env_from_connections(connections: Mapping[str, Any]) -> Dict[str, str]:
    env: Dict[str, str] = {}

    # source
    if connections.get("source"):
        src = connections["source"]
        src_kind = str(src.get("kind") or "").lower()
        src_vault = src.get("vault")
        src_overrides = src.get("overrides") or {}
        if src_vault:
            secret = get_secret_from_vault(parse_vault_ref(src_vault))
            secret = apply_overrides(secret, src_overrides)
            if src_kind in {"oracle", "postgres", "sql"}:
                env.update(build_sql_database_env(secret))
            elif src_kind in {"mongodb", "mongo"}:
                env.update(build_mongodb_env(secret))
            elif src_kind in {"keycloak"}:
                env.update(build_keycloak_env(secret))
            else:
                raise ValueError(f"Unsupported connections.source.kind: {src_kind}")

    # destination
    if connections.get("destination"):
        dst = connections["destination"]
        dst_kind = str(dst.get("kind") or "").lower()
        dst_vault = dst.get("vault")
        dst_overrides = dst.get("overrides") or {}
        if dst_vault:
            secret = get_secret_from_vault(parse_vault_ref(dst_vault))
            secret = apply_overrides(secret, dst_overrides)
            if dst_kind in {"clickhouse"}:
                database = dst_overrides.get("database") or secret.get("database")
                sep = dst_overrides.get("dataset_table_separator") or secret.get("dataset_table_separator") or "__"
                db_override = str(database) if database not in (None, "") else None
                env.update(build_clickhouse_env(secret, database=db_override, dataset_table_separator=str(sep)))
            else:
                raise ValueError(f"Unsupported connections.destination.kind: {dst_kind}")

    return env


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_manifest(
    manifest_path: str | Path,
    *,
    write_disposition: Optional[str] = None,
    extra_env: Optional[Mapping[str, str]] = None,
    validate_only: bool = False,
    configure_logging: bool = True,
    log_level: str = "INFO",
) -> Any:
    """Load + validate + run a pipeline manifest.

    Designed to be used from:
      - Airflow PythonOperator
      - CLI: python -m dlt_utils.manifest_runner --manifest ...

    """
    if configure_logging:
        logging.basicConfig(
            level=getattr(logging, log_level.upper(), logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        )

    manifest = load_manifest(manifest_path)
    validate_manifest(manifest)

    if validate_only:
        logger.info("Manifest validated: %s", manifest_path)
        return None

    # apply runtime overrides
    if write_disposition:
        manifest.setdefault("run", {})
        manifest["run"]["write_disposition"] = write_disposition

    env_from_vault = _build_env_from_connections(manifest.get("connections") or {})
    if extra_env:
        env_from_vault.update({k: str(v) for k, v in extra_env.items()})

    source_kind = str((manifest.get("source") or {}).get("kind") or "").strip()
    plugin = _resolve_source_plugin(source_kind)
    env_from_vault.update(_build_runtime_env_from_plugin(plugin, manifest))

    with temporary_environ(env_from_vault):
        return plugin.plugin.run(manifest)


def main(argv: Optional[Sequence[str]] = None) -> None:  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Run dlt pipeline from YAML manifest")
    parser.add_argument("--manifest", required=False, help="Path to manifest YAML")
    parser.add_argument(
        "--validate-all",
        action="store_true",
        help="Validate all manifests in a directory and exit",
    )
    parser.add_argument(
        "--manifests-dir",
        default="dlt_pipelines/manifests",
        help="Directory with manifests (used with --validate-all)",
    )
    parser.add_argument("--write-disposition", dest="write_disposition", default=None)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args(argv)

    if args.validate_all:
        from pathlib import Path

        base = Path(args.manifests_dir).expanduser().resolve()
        manifests = sorted(base.glob("*.yaml"))
        if not manifests:
            raise SystemExit(f"No manifests found in: {base}")
        failed = 0
        for m in manifests:
            try:
                manifest = load_manifest(m)
                validate_manifest(manifest)
                print(f"OK  {m.name}")
            except Exception as e:
                failed += 1
                print(f"FAIL {m.name}: {e}")
        if failed:
            raise SystemExit(1)
        raise SystemExit(0)

    if not args.manifest:
        raise SystemExit("--manifest is required unless --validate-all is specified")

    run_manifest(
        args.manifest,
        write_disposition=args.write_disposition,
        validate_only=args.validate_only,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
