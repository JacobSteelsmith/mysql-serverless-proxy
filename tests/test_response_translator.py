"""Unit tests for ResponseTranslator.

These tests verify specific examples and edge cases for response translation.
"""

import pytest
from botocore.exceptions import ClientError

from mysql_rds_proxy.response_translator import (
    ResponseTranslator,
    ColumnDef,
    MYSQL_TYPE_VAR_STRING,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_NEWDECIMAL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_BLOB,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_STRING,
)
from mysql_rds_proxy.rds_client import QueryResult, ColumnMetadata


class TestResponseTranslator:
    """Unit tests for ResponseTranslator."""
    
    def test_empty_result_set(self):
        """Test translation of empty result set (no columns, no rows)."""
        translator = ResponseTranslator()
        
        result = QueryResult(
            columns=[],
            rows=[],
            affected_rows=0,
            last_insert_id=0,
            error=None
        )
        
        column_defs, rows = translator.translate_result_set(result)
        
        assert column_defs == []
        assert rows == []
    
    def test_result_set_with_columns_but_no_rows(self):
        """Test translation of result set with columns but no rows."""
        translator = ResponseTranslator()
        
        result = QueryResult(
            columns=[
                ColumnMetadata(name='id', type_name='INTEGER', nullable=False),
                ColumnMetadata(name='name', type_name='VARCHAR', nullable=True),
            ],
            rows=[],
            affected_rows=0,
            last_insert_id=0,
            error=None
        )
        
        column_defs, rows = translator.translate_result_set(result)
        
        assert len(column_defs) == 2
        assert column_defs[0].name == 'id'
        assert column_defs[0].type_code == MYSQL_TYPE_LONG
        assert column_defs[1].name == 'name'
        assert column_defs[1].type_code == MYSQL_TYPE_VAR_STRING
        assert rows == []
    
    def test_null_values_in_various_column_types(self):
        """Test NULL value handling in different column types."""
        translator = ResponseTranslator()
        
        result = QueryResult(
            columns=[
                ColumnMetadata(name='int_col', type_name='INTEGER', nullable=True),
                ColumnMetadata(name='str_col', type_name='VARCHAR', nullable=True),
                ColumnMetadata(name='float_col', type_name='FLOAT', nullable=True),
                ColumnMetadata(name='blob_col', type_name='BLOB', nullable=True),
            ],
            rows=[
                [None, None, None, None],
                [42, 'test', 3.14, b'data'],
                [None, 'mixed', None, b'blob'],
            ],
            affected_rows=0,
            last_insert_id=0,
            error=None
        )
        
        column_defs, rows = translator.translate_result_set(result)
        
        # Verify NULL values are preserved
        assert rows[0] == [None, None, None, None]
        assert rows[1][0] == 42
        assert rows[1][1] == 'test'
        assert rows[1][2] == 3.14
        assert rows[1][3] == 'data'  # bytes converted to string
        assert rows[2][0] is None
        assert rows[2][1] == 'mixed'
        assert rows[2][2] is None
    
    def test_large_result_set(self):
        """Test translation of large result set."""
        translator = ResponseTranslator()
        
        # Create a result set with many rows
        num_rows = 1000
        result = QueryResult(
            columns=[
                ColumnMetadata(name='id', type_name='INTEGER', nullable=False),
                ColumnMetadata(name='value', type_name='VARCHAR', nullable=True),
            ],
            rows=[[i, f'value_{i}'] for i in range(num_rows)],
            affected_rows=0,
            last_insert_id=0,
            error=None
        )
        
        column_defs, rows = translator.translate_result_set(result)
        
        assert len(rows) == num_rows
        assert rows[0] == [0, 'value_0']
        assert rows[999] == [999, 'value_999']
    
    def test_all_supported_mysql_type_codes(self):
        """Test mapping of all supported Data API types to MySQL type codes."""
        translator = ResponseTranslator()
        
        type_mappings = [
            ('VARCHAR', MYSQL_TYPE_VAR_STRING),
            ('CHAR', MYSQL_TYPE_STRING),
            ('TEXT', MYSQL_TYPE_VAR_STRING),
            ('INTEGER', MYSQL_TYPE_LONG),
            ('INT', MYSQL_TYPE_LONG),
            ('TINYINT', MYSQL_TYPE_TINY),
            ('SMALLINT', MYSQL_TYPE_SHORT),
            ('MEDIUMINT', MYSQL_TYPE_INT24),
            ('BIGINT', MYSQL_TYPE_LONGLONG),
            ('DECIMAL', MYSQL_TYPE_NEWDECIMAL),
            ('FLOAT', MYSQL_TYPE_FLOAT),
            ('DOUBLE', MYSQL_TYPE_DOUBLE),
            ('TIMESTAMP', MYSQL_TYPE_TIMESTAMP),
            ('DATETIME', MYSQL_TYPE_DATETIME),
            ('DATE', MYSQL_TYPE_DATE),
            ('TIME', MYSQL_TYPE_TIME),
            ('BLOB', MYSQL_TYPE_BLOB),
        ]
        
        for data_api_type, expected_mysql_type in type_mappings:
            mysql_type = translator._map_data_type(data_api_type)
            assert mysql_type == expected_mysql_type, \
                f"Type {data_api_type} should map to {expected_mysql_type}, got {mysql_type}"
    
    def test_unknown_type_defaults_to_varchar(self):
        """Test that unknown types default to VARCHAR."""
        translator = ResponseTranslator()
        
        unknown_types = ['UNKNOWN_TYPE', 'CUSTOM_TYPE', 'WEIRD_TYPE']
        
        for unknown_type in unknown_types:
            mysql_type = translator._map_data_type(unknown_type)
            assert mysql_type == MYSQL_TYPE_VAR_STRING
    
    def test_case_insensitive_type_mapping(self):
        """Test that type mapping is case-insensitive."""
        translator = ResponseTranslator()
        
        # Test various case combinations
        assert translator._map_data_type('VARCHAR') == MYSQL_TYPE_VAR_STRING
        assert translator._map_data_type('varchar') == MYSQL_TYPE_VAR_STRING
        assert translator._map_data_type('VarChar') == MYSQL_TYPE_VAR_STRING
        assert translator._map_data_type('INTEGER') == MYSQL_TYPE_LONG
        assert translator._map_data_type('integer') == MYSQL_TYPE_LONG
        assert translator._map_data_type('Integer') == MYSQL_TYPE_LONG
    
    def test_column_with_label(self):
        """Test that column label is used when available."""
        translator = ResponseTranslator()
        
        col_meta = ColumnMetadata(
            name='original_name',
            type_name='VARCHAR',
            nullable=True,
            label='alias_name'
        )
        
        col_def = translator._translate_column(col_meta)
        
        # Should use label instead of name
        assert col_def.name == 'alias_name'
    
    def test_column_without_label(self):
        """Test that column name is used when label is not available."""
        translator = ResponseTranslator()
        
        col_meta = ColumnMetadata(
            name='column_name',
            type_name='VARCHAR',
            nullable=True,
            label=None
        )
        
        col_def = translator._translate_column(col_meta)
        
        # Should use name
        assert col_def.name == 'column_name'
    
    def test_not_null_flag_set_correctly(self):
        """Test that NOT NULL flag is set correctly."""
        translator = ResponseTranslator()
        
        # Non-nullable column
        col_meta_not_null = ColumnMetadata(
            name='id',
            type_name='INTEGER',
            nullable=False
        )
        col_def_not_null = translator._translate_column(col_meta_not_null)
        assert col_def_not_null.flags & 0x0001 == 0x0001  # NOT_NULL flag set
        
        # Nullable column
        col_meta_null = ColumnMetadata(
            name='name',
            type_name='VARCHAR',
            nullable=True
        )
        col_def_null = translator._translate_column(col_meta_null)
        assert col_def_null.flags & 0x0001 == 0  # NOT_NULL flag not set
    
    def test_precision_and_scale_handling(self):
        """Test that precision and scale are handled correctly."""
        translator = ResponseTranslator()
        
        col_meta = ColumnMetadata(
            name='price',
            type_name='DECIMAL',
            nullable=True,
            precision=10,
            scale=2
        )
        
        col_def = translator._translate_column(col_meta)
        
        assert col_def.max_length == 10
        assert col_def.decimals == 2
    
    def test_max_length_calculation_for_various_types(self):
        """Test max_length calculation for different types."""
        translator = ResponseTranslator()
        
        test_cases = [
            ('TINYINT', None, 4),
            ('SMALLINT', None, 6),
            ('MEDIUMINT', None, 9),
            ('INT', None, 11),
            ('BIGINT', None, 20),
            ('FLOAT', None, 12),
            ('DOUBLE', None, 22),
            ('DATE', None, 10),
            ('TIME', None, 10),
            ('DATETIME', None, 19),
            ('TIMESTAMP', None, 19),
            ('YEAR', None, 4),
            ('VARCHAR', 100, 100),  # Uses precision if available
        ]
        
        for type_name, precision, expected_length in test_cases:
            col_meta = ColumnMetadata(
                name='test',
                type_name=type_name,
                nullable=True,
                precision=precision
            )
            col_def = translator._translate_column(col_meta)
            assert col_def.max_length == expected_length, \
                f"Type {type_name} should have max_length {expected_length}, got {col_def.max_length}"
    
    def test_bytes_to_string_conversion(self):
        """Test that bytes are converted to strings when possible."""
        translator = ResponseTranslator()
        
        # Valid UTF-8 bytes
        utf8_bytes = b'Hello, World!'
        converted = translator._convert_value(utf8_bytes)
        assert converted == 'Hello, World!'
        
        # Invalid UTF-8 bytes (should remain as bytes)
        invalid_bytes = b'\xff\xfe\xfd'
        converted = translator._convert_value(invalid_bytes)
        assert converted == invalid_bytes
    
    def test_affected_rows_for_dml_statements(self):
        """Test affected row count for DML statements."""
        translator = ResponseTranslator()
        
        # INSERT statement
        result_insert = QueryResult(
            columns=[],
            rows=[],
            affected_rows=5,
            last_insert_id=100,
            error=None
        )
        assert translator.get_affected_rows(result_insert) == 5
        assert translator.get_last_insert_id(result_insert) == 100
        
        # UPDATE statement
        result_update = QueryResult(
            columns=[],
            rows=[],
            affected_rows=10,
            last_insert_id=0,
            error=None
        )
        assert translator.get_affected_rows(result_update) == 10
        
        # DELETE statement
        result_delete = QueryResult(
            columns=[],
            rows=[],
            affected_rows=3,
            last_insert_id=0,
            error=None
        )
        assert translator.get_affected_rows(result_delete) == 3
    
    def test_aws_error_translation_bad_request(self):
        """Test translation of BadRequestException."""
        translator = ResponseTranslator()
        
        error_response = {
            'Error': {
                'Code': 'BadRequestException',
                'Message': 'Syntax error in SQL statement'
            }
        }
        error = ClientError(error_response, 'execute_statement')
        
        mysql_code, sql_state, message = translator.translate_error(error)
        
        assert mysql_code == 1064  # Syntax error
        assert sql_state == '42000'
        assert message == 'Syntax error in SQL statement'
    
    def test_aws_error_translation_timeout(self):
        """Test translation of StatementTimeoutException."""
        translator = ResponseTranslator()
        
        error_response = {
            'Error': {
                'Code': 'StatementTimeoutException',
                'Message': 'Statement execution timeout'
            }
        }
        error = ClientError(error_response, 'execute_statement')
        
        mysql_code, sql_state, message = translator.translate_error(error)
        
        assert mysql_code == 1205  # Lock wait timeout
        assert sql_state == 'HY000'
        assert message == 'Statement execution timeout'
    
    def test_aws_error_translation_forbidden(self):
        """Test translation of ForbiddenException."""
        translator = ResponseTranslator()
        
        error_response = {
            'Error': {
                'Code': 'ForbiddenException',
                'Message': 'Access denied'
            }
        }
        error = ClientError(error_response, 'execute_statement')
        
        mysql_code, sql_state, message = translator.translate_error(error)
        
        assert mysql_code == 1045  # Access denied
        assert sql_state == '28000'
        assert message == 'Access denied'
    
    def test_aws_error_translation_service_unavailable(self):
        """Test translation of ServiceUnavailableException."""
        translator = ResponseTranslator()
        
        error_response = {
            'Error': {
                'Code': 'ServiceUnavailableException',
                'Message': 'Service temporarily unavailable'
            }
        }
        error = ClientError(error_response, 'execute_statement')
        
        mysql_code, sql_state, message = translator.translate_error(error)
        
        assert mysql_code == 2013  # Lost connection
        assert sql_state == 'HY000'
        assert message == 'Service temporarily unavailable'
    
    def test_aws_error_translation_throttling(self):
        """Test translation of ThrottlingException."""
        translator = ResponseTranslator()
        
        error_response = {
            'Error': {
                'Code': 'ThrottlingException',
                'Message': 'Rate limit exceeded'
            }
        }
        error = ClientError(error_response, 'execute_statement')
        
        mysql_code, sql_state, message = translator.translate_error(error)
        
        assert mysql_code == 1040  # Too many connections
        assert sql_state == 'HY000'
        assert message == 'Rate limit exceeded'
    
    def test_aws_error_translation_unknown_error(self):
        """Test translation of unknown AWS error."""
        translator = ResponseTranslator()
        
        error_response = {
            'Error': {
                'Code': 'UnknownException',
                'Message': 'Something went wrong'
            }
        }
        error = ClientError(error_response, 'execute_statement')
        
        mysql_code, sql_state, message = translator.translate_error(error)
        
        assert mysql_code == 1105  # Generic error
        assert sql_state == 'HY000'
        assert message == 'Something went wrong'
    
    def test_generic_exception_translation(self):
        """Test translation of generic Python exceptions."""
        translator = ResponseTranslator()
        
        # ValueError
        error = ValueError('Invalid value')
        mysql_code, sql_state, message = translator.translate_error(error)
        assert mysql_code == 1105
        assert sql_state == 'HY000'
        assert message == 'Invalid value'
        
        # TypeError
        error = TypeError('Type mismatch')
        mysql_code, sql_state, message = translator.translate_error(error)
        assert mysql_code == 1105
        assert sql_state == 'HY000'
        assert message == 'Type mismatch'
        
        # RuntimeError
        error = RuntimeError('Runtime error occurred')
        mysql_code, sql_state, message = translator.translate_error(error)
        assert mysql_code == 1105
        assert sql_state == 'HY000'
        assert message == 'Runtime error occurred'
