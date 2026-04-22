"""Response translator for converting RDS Data API responses to MySQL protocol format.

This module provides the ResponseTranslator class that converts RDS Data API
query results into MySQL-compatible format, including type mapping, NULL value
handling, and error translation.
"""

from dataclasses import dataclass
from typing import Any, Optional
import logging

from botocore.exceptions import ClientError

from .rds_client import QueryResult, ColumnMetadata


logger = logging.getLogger(__name__)


# MySQL type codes (from MySQL protocol specification)
MYSQL_TYPE_DECIMAL = 0
MYSQL_TYPE_TINY = 1
MYSQL_TYPE_SHORT = 2
MYSQL_TYPE_LONG = 3
MYSQL_TYPE_FLOAT = 4
MYSQL_TYPE_DOUBLE = 5
MYSQL_TYPE_NULL = 6
MYSQL_TYPE_TIMESTAMP = 7
MYSQL_TYPE_LONGLONG = 8
MYSQL_TYPE_INT24 = 9
MYSQL_TYPE_DATE = 10
MYSQL_TYPE_TIME = 11
MYSQL_TYPE_DATETIME = 12
MYSQL_TYPE_YEAR = 13
MYSQL_TYPE_NEWDATE = 14
MYSQL_TYPE_VARCHAR = 15
MYSQL_TYPE_BIT = 16
MYSQL_TYPE_NEWDECIMAL = 246
MYSQL_TYPE_ENUM = 247
MYSQL_TYPE_SET = 248
MYSQL_TYPE_TINY_BLOB = 249
MYSQL_TYPE_MEDIUM_BLOB = 250
MYSQL_TYPE_LONG_BLOB = 251
MYSQL_TYPE_BLOB = 252
MYSQL_TYPE_VAR_STRING = 253
MYSQL_TYPE_STRING = 254
MYSQL_TYPE_GEOMETRY = 255


@dataclass
class ColumnDef:
    """MySQL column definition.
    
    Attributes:
        name: Column name
        type_code: MySQL type code (e.g., MYSQL_TYPE_VAR_STRING)
        flags: Column flags (e.g., NOT_NULL, PRIMARY_KEY)
        max_length: Maximum length of column values
        decimals: Number of decimal places (for numeric types)
    """
    name: str
    type_code: int
    flags: int = 0
    max_length: int = 0
    decimals: int = 0


class ResponseTranslator:
    """Translator for converting RDS Data API responses to MySQL protocol format.
    
    This class handles:
    - Type mapping from Data API types to MySQL types
    - Result set translation (columns and rows)
    - NULL value handling
    - Affected row count preservation
    - AWS error to MySQL error translation
    """
    
    # Type mapping from Data API type names to MySQL type codes
    TYPE_MAPPING = {
        'VARCHAR': MYSQL_TYPE_VAR_STRING,
        'CHAR': MYSQL_TYPE_STRING,
        'TEXT': MYSQL_TYPE_VAR_STRING,
        'TINYTEXT': MYSQL_TYPE_VAR_STRING,
        'MEDIUMTEXT': MYSQL_TYPE_VAR_STRING,
        'LONGTEXT': MYSQL_TYPE_VAR_STRING,
        'INTEGER': MYSQL_TYPE_LONG,
        'INT': MYSQL_TYPE_LONG,
        'TINYINT': MYSQL_TYPE_TINY,
        'SMALLINT': MYSQL_TYPE_SHORT,
        'MEDIUMINT': MYSQL_TYPE_INT24,
        'BIGINT': MYSQL_TYPE_LONGLONG,
        'DECIMAL': MYSQL_TYPE_NEWDECIMAL,
        'NUMERIC': MYSQL_TYPE_NEWDECIMAL,
        'FLOAT': MYSQL_TYPE_FLOAT,
        'DOUBLE': MYSQL_TYPE_DOUBLE,
        'REAL': MYSQL_TYPE_DOUBLE,
        'TIMESTAMP': MYSQL_TYPE_TIMESTAMP,
        'DATETIME': MYSQL_TYPE_DATETIME,
        'DATE': MYSQL_TYPE_DATE,
        'TIME': MYSQL_TYPE_TIME,
        'YEAR': MYSQL_TYPE_YEAR,
        'BLOB': MYSQL_TYPE_BLOB,
        'TINYBLOB': MYSQL_TYPE_TINY_BLOB,
        'MEDIUMBLOB': MYSQL_TYPE_MEDIUM_BLOB,
        'LONGBLOB': MYSQL_TYPE_LONG_BLOB,
        'BINARY': MYSQL_TYPE_STRING,
        'VARBINARY': MYSQL_TYPE_VAR_STRING,
        'BIT': MYSQL_TYPE_BIT,
        'ENUM': MYSQL_TYPE_ENUM,
        'SET': MYSQL_TYPE_SET,
        'GEOMETRY': MYSQL_TYPE_GEOMETRY,
        'POINT': MYSQL_TYPE_GEOMETRY,
        'LINESTRING': MYSQL_TYPE_GEOMETRY,
        'POLYGON': MYSQL_TYPE_GEOMETRY,
    }
    
    # AWS error code to MySQL error code mapping
    ERROR_MAPPING = {
        'BadRequestException': (1064, '42000'),  # Syntax error
        'StatementTimeoutException': (1205, 'HY000'),  # Lock wait timeout
        'ForbiddenException': (1045, '28000'),  # Access denied
        'ServiceUnavailableException': (2013, 'HY000'),  # Lost connection
        'ThrottlingException': (1040, 'HY000'),  # Too many connections
    }
    
    def translate_result_set(
        self, result: QueryResult
    ) -> tuple[list[ColumnDef], list[list[Any]]]:
        """Convert Data API result to MySQL format.
        
        Translates column metadata and row data from RDS Data API format
        to MySQL protocol format. Handles NULL values and type conversions.
        
        Args:
            result: QueryResult from RDS Data API execution
            
        Returns:
            Tuple of (column_definitions, rows) in MySQL format
        """
        logger.debug(
            f"Translating result set: {len(result.columns)} columns, "
            f"{len(result.rows)} rows"
        )
        
        # Translate column metadata
        column_defs = []
        for col_meta in result.columns:
            col_def = self._translate_column(col_meta)
            column_defs.append(col_def)
        
        # Handle duplicate column names (e.g., from SELECT * with JOINs)
        # MySQL clients expect unique column names, so we append a suffix
        column_defs = self._ensure_unique_column_names(column_defs)
        
        # Translate row data (values are already extracted by RDSClient)
        # We just need to ensure NULL values are properly represented
        translated_rows = []
        for row in result.rows:
            translated_row = [self._convert_value(val) for val in row]
            translated_rows.append(translated_row)
        
        logger.debug(
            f"Result set translated: {len(column_defs)} columns, "
            f"{len(translated_rows)} rows"
        )
        
        return column_defs, translated_rows
    
    def _translate_column(self, col_meta: ColumnMetadata) -> ColumnDef:
        """Translate column metadata from Data API to MySQL format.
        
        Args:
            col_meta: ColumnMetadata from Data API
            
        Returns:
            ColumnDef for MySQL protocol
        """
        # Map Data API type to MySQL type code
        type_code = self._map_data_type(col_meta.type_name)
        
        # Set flags based on column properties
        flags = 0
        if not col_meta.nullable:
            flags |= 0x0001  # NOT_NULL flag
        
        # Calculate max_length based on type
        max_length = self._calculate_max_length(col_meta)
        
        # Set decimals for numeric types
        decimals = col_meta.scale if col_meta.scale is not None else 0
        
        # Use label if available, otherwise use name
        column_name = col_meta.label if col_meta.label else col_meta.name
        
        return ColumnDef(
            name=column_name,
            type_code=type_code,
            flags=flags,
            max_length=max_length,
            decimals=decimals
        )
    
    def _ensure_unique_column_names(self, column_defs: list[ColumnDef]) -> list[ColumnDef]:
        """Ensure all column names are unique by appending suffixes to duplicates.
        
        When SELECT * is used with JOINs, multiple columns can have the same name.
        This method makes them unique by appending _1, _2, etc. to duplicates.
        
        Args:
            column_defs: List of column definitions
            
        Returns:
            List of column definitions with unique names
        """
        name_counts = {}
        result = []
        
        for col_def in column_defs:
            original_name = col_def.name
            
            # Track how many times we've seen this name
            if original_name in name_counts:
                name_counts[original_name] += 1
                # Append suffix to make it unique
                unique_name = f"{original_name}_{name_counts[original_name]}"
                logger.debug(f"Duplicate column name '{original_name}' renamed to '{unique_name}'")
            else:
                name_counts[original_name] = 0
                unique_name = original_name
            
            # Create new ColumnDef with unique name
            result.append(ColumnDef(
                name=unique_name,
                type_code=col_def.type_code,
                flags=col_def.flags,
                max_length=col_def.max_length,
                decimals=col_def.decimals
            ))
        
        return result
    
    def _map_data_type(self, data_api_type: str) -> int:
        """Map Data API type to MySQL type code.
        
        Args:
            data_api_type: Type name from Data API (e.g., "VARCHAR", "INTEGER")
            
        Returns:
            MySQL type code (e.g., MYSQL_TYPE_VAR_STRING)
        """
        # Normalize type name to uppercase
        normalized_type = data_api_type.upper()
        
        # Look up in mapping, default to VARCHAR if unknown
        type_code = self.TYPE_MAPPING.get(normalized_type, MYSQL_TYPE_VAR_STRING)
        
        if normalized_type not in self.TYPE_MAPPING:
            logger.warning(
                f"Unknown Data API type '{data_api_type}', "
                f"defaulting to VARCHAR"
            )
        
        return type_code
    
    def _calculate_max_length(self, col_meta: ColumnMetadata) -> int:
        """Calculate maximum length for a column.
        
        Args:
            col_meta: ColumnMetadata from Data API
            
        Returns:
            Maximum length in bytes
        """
        # Use precision if available
        if col_meta.precision is not None:
            return col_meta.precision
        
        # Default lengths based on type
        type_name = col_meta.type_name.upper()
        
        if type_name in ('TINYINT',):
            return 4
        elif type_name in ('SMALLINT',):
            return 6
        elif type_name in ('MEDIUMINT',):
            return 9
        elif type_name in ('INT', 'INTEGER'):
            return 11
        elif type_name in ('BIGINT',):
            return 20
        elif type_name in ('FLOAT',):
            return 12
        elif type_name in ('DOUBLE', 'REAL'):
            return 22
        elif type_name in ('DATE',):
            return 10
        elif type_name in ('TIME',):
            return 10
        elif type_name in ('DATETIME', 'TIMESTAMP'):
            return 19
        elif type_name in ('YEAR',):
            return 4
        else:
            # Default for string types
            return 255
    
    def _convert_value(self, value: Any) -> Any:
        """Convert Data API value to MySQL-compatible format.
        
        Args:
            value: Value from Data API (already extracted by RDSClient)
            
        Returns:
            MySQL-compatible value (None for NULL)
        """
        # NULL values are already represented as None
        if value is None:
            return None
        
        # Convert bytes to string for display (if needed)
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                # Keep as bytes if not valid UTF-8
                return value
        
        # All other values can be used as-is
        return value
    
    def translate_error(
        self, error: Exception
    ) -> tuple[int, str, str]:
        """Convert AWS error to MySQL error code, SQL state, and message.
        
        Args:
            error: Exception from AWS API or other source
            
        Returns:
            Tuple of (error_code, sql_state, error_message)
        """
        logger.debug(f"Translating error: {type(error).__name__}: {error}")
        
        # Handle AWS ClientError
        if isinstance(error, ClientError):
            error_code_str = error.response.get('Error', {}).get('Code', 'Unknown')
            error_message = error.response.get('Error', {}).get('Message', str(error))
            
            # Look up MySQL error code and SQL state
            if error_code_str in self.ERROR_MAPPING:
                mysql_code, sql_state = self.ERROR_MAPPING[error_code_str]
            else:
                # Generic error
                mysql_code = 1105  # ER_UNKNOWN_ERROR
                sql_state = 'HY000'  # General error
            
            logger.debug(
                f"AWS error '{error_code_str}' mapped to MySQL error "
                f"{mysql_code} ({sql_state})"
            )
            
            return mysql_code, sql_state, error_message
        
        # Handle other exceptions
        else:
            # Generic error for non-AWS exceptions
            mysql_code = 1105  # ER_UNKNOWN_ERROR
            sql_state = 'HY000'  # General error
            error_message = str(error)
            
            logger.debug(
                f"Generic error mapped to MySQL error {mysql_code} ({sql_state})"
            )
            
            return mysql_code, sql_state, error_message
    
    def get_affected_rows(self, result: QueryResult) -> int:
        """Get affected row count from query result.
        
        For DML statements (INSERT, UPDATE, DELETE), returns the number
        of rows affected. For SELECT statements, returns 0.
        
        Args:
            result: QueryResult from RDS Data API execution
            
        Returns:
            Number of affected rows
        """
        return result.affected_rows
    
    def get_last_insert_id(self, result: QueryResult) -> int:
        """Get last insert ID from query result.
        
        Args:
            result: QueryResult from RDS Data API execution
            
        Returns:
            Last insert ID (0 if not available)
        """
        return result.last_insert_id
