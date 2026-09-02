"""
Data Quality Enums and Type Normalization
"""

from enum import Enum
import re


class AllowedOperator(Enum):
    """Supported SQL comparison operators for data quality checks."""

    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="

    @classmethod
    def is_valid(cls, operator: str) -> bool:
        """Helper to validate if an operator string is supported."""
        return any(operator == item.value for item in cls)


class DataTypeAlias(Enum):
    """Normalized Spark SQL data types and their aliases/common typos."""

    # Integers
    INT = "int"
    INTEGER = "int"
    BIGINT = "long"
    LONG = "long"
    SMALLINT = "short"
    SHORT = "short"
    TINYINT = "byte"
    BYTE = "byte"

    # Floats & Decimals
    DOUBLE = "double"
    DOBULE = "double"  # Common typo handling
    FLOAT = "float"
    DECIMAL = "decimal"
    NUMERIC = "decimal"

    # Strings
    STRING = "string"
    STR = "string"
    VARCHAR = "string"
    TEXT = "string"

    # Dates & Timestamps
    DATE = "date"
    TIMESTAMP = "timestamp"
    DATETIME = "timestamp"
    TIME = "timestamp"

    # Booleans
    BOOL = "boolean"
    BOOLEAN = "boolean"

    @classmethod
    def normalize(cls, type_str: str) -> str:
        """
        Normalizes input string to canonical Spark SQL data type.
        Supports base types and parameterized types like decimal(10,2) or array<string>.
        """
        if not type_str:
            return "unknown"

        cleaned = str(type_str).strip().lower()

        # Extract base type and parameter suffix (e.g., decimal(10,2) or array<string>)
        match = re.match(r"^([a-z0-9_]+)([\(\<].*)?$", cleaned)
        if not match:
            return cleaned

        base_type, param_suffix = match.group(1), match.group(2) or ""

        upper_key = base_type.upper()
        if upper_key in cls.__members__:
            canonical_base = cls[upper_key].value
            return f"{canonical_base}{param_suffix}"

        return cleaned