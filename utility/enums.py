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
        Falls back to raw lowercase string if not found in alias enum.
        """
        cleaned = str(type_str).strip().lower()
        if cleaned in cls.__members__:
            return cls[cleaned].value
        return cleaned