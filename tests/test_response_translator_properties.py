"""Property-based tests for ResponseTranslator.

These tests verify universal properties that should hold for all inputs
using Hypothesis for property-based testing.
"""

import pytest
from hypothesis import given, strategies as st, settings

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
)
from mysql_rds_proxy.rds_client import QueryResult, ColumnMetadata
from botocore.exceptions import ClientError


# Strategy for generating Data API type names
data_api_types = st.sampled_from([
    'VARCHAR', 'CHAR', 'TEXT', 'INTEGER', 'INT', 'BIGINT', 'DECIMAL',
    'FLOAT', 'DOUBLE', 'TIMESTAMP', 'DATETIME', 'DATE', 'TIME',
    'BLOB', 'TINYINT', 'SMALLINT', 'MEDIUMINT', 'YEAR',
    'TINYTEXT', 'MEDIUMTEXT', 'LONGTEXT',
    'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB',
    'BINARY', 'VARBINARY', 'BIT', 'ENUM', 'SET',
    'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON',
])


# Strategy for generating column metadata
@st.composite
def column_metadata_strategy(draw):
    """Generate ColumnMetadata instances."""
    name = draw(st.text(min_size=1, max_size=64, alphabet=st.characters(
        whitelist_categories=('Lu', 'Ll', 'Nd'), whitelist_characters='_'
    )))
    type_name = draw(data_api_types)
    nullable = draw(st.booleans())
    precision = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=65535)))
    scale = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=30)))
    label = draw(st.one_of(st.none(), st.text(min_size=1, max_size=64)))
    
    return ColumnMetadata(
        name=name,
        type_name=type_name,
        nullable=nullable,
        precision=precision,
        scale=scale,
        label=label
    )


# Strategy for generating query results
@st.composite
def query_result_strategy(draw):
    """Generate QueryResult instances."""
    num_columns = draw(st.integers(min_value=0, max_value=20))
    columns = [draw(column_metadata_strategy()) for _ in range(num_columns)]
    
    num_rows = draw(st.integers(min_value=0, max_value=50))
    rows = []
    for _ in range(num_rows):
        row = []
        for _ in range(num_columns):
            # Generate various value types including NULL
            value = draw(st.one_of(
                st.none(),
                st.text(max_size=100),
                st.integers(),
                st.floats(allow_nan=False, allow_infinity=False),
                st.booleans(),
                st.binary(max_size=100),
            ))
            row.append(value)
        rows.append(row)
    
    affected_rows = draw(st.integers(min_value=0, max_value=1000000))
    last_insert_id = draw(st.integers(min_value=0, max_value=1000000))
    
    return QueryResult(
        columns=columns,
        rows=rows,
        affected_rows=affected_rows,
        last_insert_id=last_insert_id,
        error=None
    )


class TestResponseTranslatorProperties:
    """Property-based tests for ResponseTranslator."""
    
    @given(data_api_type=data_api_types)
    @settings(max_examples=100)
    def test_property_14_data_type_mapping(self, data_api_type):
        """Property 14: Data Type Mapping
        
        **Validates: Requirements 5.2**
        
        For any RDS Data API column type, the response translator should map it
        to a valid MySQL column type code.
        """
        translator = ResponseTranslator()
        
        # Map the type
        mysql_type = translator._map_data_type(data_api_type)
        
        # Verify it's a valid MySQL type code (integer)
        assert isinstance(mysql_type, int)
        assert mysql_type >= 0
        assert mysql_type <= 255  # MySQL type codes are in range 0-255
        
        # Verify the mapping is consistent (same input -> same output)
        mysql_type2 = translator._map_data_type(data_api_type)
        assert mysql_type == mysql_type2
        
        # Verify case-insensitive mapping
        mysql_type_lower = translator._map_data_type(data_api_type.lower())
        mysql_type_upper = translator._map_data_type(data_api_type.upper())
        assert mysql_type_lower == mysql_type_upper
    
    @given(result=query_result_strategy())
    @settings(max_examples=100)
    def test_property_15_result_set_translation(self, result):
        """Property 15: Result Set Translation
        
        **Validates: Requirements 5.1**
        
        For any RDS Data API result set, translating it should produce
        MySQL-compatible column definitions and row data.
        """
        translator = ResponseTranslator()
        
        # Translate the result set
        column_defs, rows = translator.translate_result_set(result)
        
        # Verify column definitions
        assert isinstance(column_defs, list)
        assert len(column_defs) == len(result.columns)
        
        for col_def in column_defs:
            assert isinstance(col_def, ColumnDef)
            assert isinstance(col_def.name, str)
            assert isinstance(col_def.type_code, int)
            assert col_def.type_code >= 0
            assert col_def.type_code <= 255
            assert isinstance(col_def.flags, int)
            assert isinstance(col_def.max_length, int)
            assert isinstance(col_def.decimals, int)
        
        # Verify row data
        assert isinstance(rows, list)
        assert len(rows) == len(result.rows)
        
        for row in rows:
            assert isinstance(row, list)
            assert len(row) == len(result.columns)
        
        # Verify NULL values are preserved
        for i, result_row in enumerate(result.rows):
            for j, value in enumerate(result_row):
                if value is None:
                    assert rows[i][j] is None
    
    @given(result=query_result_strategy())
    @settings(max_examples=100)
    def test_property_16_affected_row_count_preservation(self, result):
        """Property 16: Affected Row Count Preservation
        
        **Validates: Requirements 5.4**
        
        For any INSERT/UPDATE/DELETE query result, the affected row count from
        the Data API should be preserved in the MySQL response.
        """
        translator = ResponseTranslator()
        
        # Get affected rows
        affected_rows = translator.get_affected_rows(result)
        
        # Verify it matches the original
        assert affected_rows == result.affected_rows
        assert isinstance(affected_rows, int)
        assert affected_rows >= 0
        
        # Verify last insert ID is also preserved
        last_insert_id = translator.get_last_insert_id(result)
        assert last_insert_id == result.last_insert_id
        assert isinstance(last_insert_id, int)
        assert last_insert_id >= 0
    
    @given(
        error_code=st.sampled_from([
            'BadRequestException',
            'StatementTimeoutException',
            'ForbiddenException',
            'ServiceUnavailableException',
            'ThrottlingException',
            'UnknownException',
        ]),
        error_message=st.text(min_size=1, max_size=200)
    )
    @settings(max_examples=100)
    def test_property_17_error_translation(self, error_code, error_message):
        """Property 17: Error Translation
        
        **Validates: Requirements 5.5**
        
        For any AWS API error, translating it should produce a valid MySQL
        error code, SQL state, and error message.
        """
        translator = ResponseTranslator()
        
        # Create a mock ClientError
        error_response = {
            'Error': {
                'Code': error_code,
                'Message': error_message
            }
        }
        error = ClientError(error_response, 'execute_statement')
        
        # Translate the error
        mysql_code, sql_state, message = translator.translate_error(error)
        
        # Verify MySQL error code is valid
        assert isinstance(mysql_code, int)
        assert mysql_code > 0
        assert mysql_code < 10000  # MySQL error codes are typically < 10000
        
        # Verify SQL state is valid (5 characters)
        assert isinstance(sql_state, str)
        assert len(sql_state) == 5
        
        # Verify message is preserved
        assert isinstance(message, str)
        assert len(message) > 0
        assert message == error_message
        
        # Verify consistency (same error -> same translation)
        mysql_code2, sql_state2, message2 = translator.translate_error(error)
        assert mysql_code == mysql_code2
        assert sql_state == sql_state2
        assert message == message2
    
    @given(
        exception_type=st.sampled_from([
            ValueError,
            TypeError,
            RuntimeError,
            KeyError,
        ]),
        error_message=st.text(min_size=1, max_size=200)
    )
    @settings(max_examples=100)
    def test_property_17_generic_error_translation(self, exception_type, error_message):
        """Property 17: Error Translation (Generic Errors)
        
        **Validates: Requirements 5.5**
        
        For any generic Python exception, translating it should produce a valid
        MySQL error code, SQL state, and error message.
        """
        translator = ResponseTranslator()
        
        # Create a generic exception
        error = exception_type(error_message)
        
        # Translate the error
        mysql_code, sql_state, message = translator.translate_error(error)
        
        # Verify MySQL error code is valid
        assert isinstance(mysql_code, int)
        assert mysql_code > 0
        
        # Verify SQL state is valid (5 characters)
        assert isinstance(sql_state, str)
        assert len(sql_state) == 5
        
        # Verify message is present
        assert isinstance(message, str)
        assert len(message) > 0
    
    @given(col_meta=column_metadata_strategy())
    @settings(max_examples=100)
    def test_column_translation_preserves_name(self, col_meta):
        """Verify that column translation preserves the column name or label."""
        translator = ResponseTranslator()
        
        col_def = translator._translate_column(col_meta)
        
        # Should use label if available, otherwise name
        expected_name = col_meta.label if col_meta.label else col_meta.name
        assert col_def.name == expected_name
    
    @given(col_meta=column_metadata_strategy())
    @settings(max_examples=100)
    def test_column_translation_sets_not_null_flag(self, col_meta):
        """Verify that NOT NULL flag is set correctly."""
        translator = ResponseTranslator()
        
        col_def = translator._translate_column(col_meta)
        
        # NOT_NULL flag (0x0001) should be set if column is not nullable
        if not col_meta.nullable:
            assert col_def.flags & 0x0001 == 0x0001
        else:
            # Flag may or may not be set for nullable columns
            pass
    
    @given(
        value=st.one_of(
            st.none(),
            st.text(),
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.booleans(),
            st.binary(),
        )
    )
    @settings(max_examples=100)
    def test_value_conversion_preserves_none(self, value):
        """Verify that NULL values (None) are preserved during conversion."""
        translator = ResponseTranslator()
        
        converted = translator._convert_value(value)
        
        if value is None:
            assert converted is None
        else:
            # Non-None values should remain non-None
            assert converted is not None
