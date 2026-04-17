from __future__ import annotations

import logging
from typing import Any

from dltaf.integrations.sqldb.config import ResolvedSqlDbConfig, SqlDbOracleConnectionConfig


class OracleDialect:
    name = "oracle"

    def validate(self, config: ResolvedSqlDbConfig) -> None:
        if config.mode not in {"query", "catalog"}:
            raise ValueError(f"Unsupported Oracle sqldb mode: {config.mode}")

    def make_engine(self, connection: SqlDbOracleConnectionConfig, *, logger_: logging.Logger) -> Any:
        from sqlalchemy import create_engine
        from sqlalchemy.engine import Engine

        try:
            import oracledb
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "oracledb is required for sqldb dialect=oracle. Install 'oracledb>=2.0.0'."
            ) from exc

        def _connect_with_dsn():
            try:
                conn = oracledb.connect(
                    user=connection.username,
                    password=connection.password,
                    dsn=connection.dsn,
                )
                self._log_oracle_version(conn, logger_)
                return conn
            except oracledb.DatabaseError as exc:
                self._log_database_error(exc, connection, logger_)
                raise
            except oracledb.NotSupportedError as exc:
                self._log_not_supported(exc, logger_)
                raise

        def _connect_with_host():
            dsn = (
                oracledb.makedsn(connection.host, connection.port, sid=connection.database)
                if connection.database
                else oracledb.makedsn(connection.host, connection.port)
            )
            try:
                conn = oracledb.connect(
                    user=connection.username,
                    password=connection.password,
                    dsn=dsn,
                )
                self._log_oracle_version(conn, logger_)
                return conn
            except oracledb.DatabaseError as exc:
                self._log_database_error(exc, connection, logger_)
                raise
            except oracledb.NotSupportedError as exc:
                self._log_not_supported(exc, logger_)
                raise

        creator = _connect_with_dsn if connection.dsn else _connect_with_host
        engine: Engine = create_engine(
            "oracle+oracledb://",
            creator=creator,
            pool_pre_ping=True,
        )
        return engine

    def try_enable_thick_mode(self, *, enabled: bool | None, logger_: logging.Logger) -> bool:
        if enabled is False:
            logger_.info("Oracle thick mode explicitly disabled; continuing with thin mode")
            return False
        try:
            import oracledb
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "oracledb is required for sqldb dialect=oracle. Install 'oracledb>=2.0.0'."
            ) from exc

        try:
            oracledb.init_oracle_client()
            logger_.info("Oracle thick mode enabled successfully")
            return True
        except Exception as thick_err:
            logger_.warning("Could not enable Oracle thick mode: %s", thick_err)
            logger_.warning(
                "Continuing with thin mode (may not work with Oracle < 12.1). See python-oracledb DPI-1047 docs."
            )
            return False

    def _log_oracle_version(self, connection: Any, logger_: logging.Logger) -> None:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT BANNER FROM v$version WHERE ROWNUM = 1")
            version = cursor.fetchone()
            logger_.info(
                "Connected to Oracle: %s",
                version[0] if version else "version unknown",
            )
            cursor.close()
        except Exception:
            logger_.info("Connected to Oracle (version check failed)")

    def _log_database_error(self, exc: Exception, connection: SqlDbOracleConnectionConfig, logger_: logging.Logger) -> None:
        try:
            (error_obj,) = exc.args
            code = getattr(error_obj, "code", "unknown")
            message = getattr(error_obj, "message", str(exc))
        except Exception:
            code = "unknown"
            message = str(exc)
        logger_.error("Oracle connection failed: code=%s message=%s", code, message)
        if str(code) == "1017":
            logger_.error(
                "Hint: invalid username/password for user '%s' on database '%s'",
                connection.username,
                connection.database,
            )

    def _log_not_supported(self, exc: Exception, logger_: logging.Logger) -> None:
        logger_.error("Oracle version not supported in thin mode: %s", exc)
        logger_.error(
            "Solution: install Oracle Instant Client and restart the runtime. See ORACLE_INSTANT_CLIENT_INSTALL.md"
        )
