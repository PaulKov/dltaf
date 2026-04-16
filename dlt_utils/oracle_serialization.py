from __future__ import annotations

from datetime import timedelta
from typing import Any


def serialize_oracle_value(value: Any) -> Any:
    """Convert Oracle-specific non-JSON-serializable types to JSON-compatible values.
    
    Oracle INTERVAL types are returned as timedelta objects by oracledb driver,
    which are not JSON serializable. This function converts them to total seconds (float).
    
    """
    if isinstance(value, timedelta):
        return value.total_seconds()
    elif isinstance(value, dict):
        return {k: serialize_oracle_value(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [serialize_oracle_value(item) for item in value]
    elif isinstance(value, tuple):
        return tuple(serialize_oracle_value(item) for item in value)
    
    return value


def serialize_oracle_row(row_dict: dict[str, Any]) -> dict[str, Any]:
    """Convert Oracle-specific types in a row dictionary.
    
    Convenience function specifically for processing database row dictionaries.
    Handles timedelta objects from Oracle INTERVAL types and recursively processes
    nested structures.
    
    Args:
        row_dict: Dictionary representing a database row
        
    Returns:
        Dictionary with all Oracle-specific types converted
        
    Example:
        >>> from datetime import timedelta
        >>> row = {
        ...     'id': 1,
        ...     'name': 'Test',
        ...     'duration': timedelta(hours=2, minutes=30)
        ... }
        >>> serialize_oracle_row(row)
        {'id': 1, 'name': 'Test', 'duration': 9000.0}
    """
    return serialize_oracle_value(row_dict)