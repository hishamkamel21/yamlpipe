# yamlpipe/enums.py
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
    BIGINT = "bigint"
    SMALLINT = "smallint"
    TINYINT = "tinyint"
    
    # Floats & Decimals
    DOUBLE = "double"
    DOBULE = "double"  # Handles typo
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
        Falls back to raw lowercase string if not found in alias map.
        """
        cleaned = type_str.strip().lower()
        if cleaned in cls.__members__:
            return cls[cleaned].value
        return cleaned