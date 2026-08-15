from enum import Enum


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
        return operator in cls._value2member_map_


from enum import Enum
import re


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
        Supports base types and parameterized types like decimal(10,2).
        """
        if not type_str:
            return "unknown"

        cleaned = str(type_str).strip().lower()

        # Handle parameterized types (e.g., decimal(10, 2) -> decimal)
        base_type = re.split(r"[\(\<]", cleaned)[0].strip()

        # Check against upper-case enum members
        upper_key = base_type.upper()
        if upper_key in cls.__members__:
            canonical_base = cls[upper_key].value
            # Reattach parameters if original had them (e.g., decimal(10,2))
            if "(" in cleaned:
                param_suffix = cleaned[cleaned.find("("):]
                return f"{canonical_base}{param_suffix}"
            return canonical_base

        return cleaned