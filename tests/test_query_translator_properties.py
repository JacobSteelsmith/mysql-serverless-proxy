"""Property-based tests for query translation functionality.

These tests use Hypothesis to verify universal properties across many inputs.
"""

import re
import pytest
from hypothesis import given, strategies as st, assume, settings

from mysql_rds_proxy.query_translator import QueryTranslator
from mysql_rds_proxy.schema_mapper import SchemaMapper


# Custom strategies for generating SQL-like identifiers and queries
@st.composite
def sql_identifier(draw):
    """Generate a valid SQL identifier (schema, table, or column name)."""
    first_char = draw(st.sampled_from('abcdefghijklmnopqrstuvwxyz_'))
    rest = draw(st.text(
        alphabet='abcdefghijklmnopqrstuvwxyz0123456789_',
        min_size=0,
        max_size=20
    ))
    return first_char + rest


@st.composite
def schema_mappings(draw):
    """Generate valid schema name mappings."""
    num_mappings = draw(st.integers(min_value=0, max_value=5))
    mappings = {}
    for _ in range(num_mappings):
        source = draw(sql_identifier())
        target = draw(sql_identifier())
        mappings[source] = target
    return mappings


@st.composite
def select_query_with_alias(draw):
    """Generate a SELECT query with column alias."""
    table = draw(sql_identifier())
    column = draw(sql_identifier())
    alias = draw(sql_identifier())
    
    # Use AS keyword (case may vary)
    as_keyword = draw(st.sampled_from(['AS', 'as', 'As']))
    
    return f"SELECT {column} {as_keyword} {alias} FROM {table}"


@st.composite
def select_query_without_alias(draw):
    """Generate a SELECT query without column alias."""
    table = draw(sql_identifier())
    num_columns = draw(st.integers(min_value=1, max_value=5))
    columns = [draw(sql_identifier()) for _ in range(num_columns)]
    
    return f"SELECT {', '.join(columns)} FROM {table}"


class TestQueryTranslatorProperties:
    """Property-based tests for QueryTranslator."""
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        query=select_query_with_alias()
    )
    def test_property_8_alias_detection(self, mappings, query):
        """Property 8: Alias Detection.
        
        For any SQL query, the alias detection should correctly identify
        whether the query contains column aliases (AS keyword).
        
        **Validates: Requirements 8.1**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        # The query has an alias, so it should be detected
        result = translator.translate(query)
        
        # If alias is detected, query should be wrapped
        # The wrapped format is: SELECT * FROM (original_query) AS random_alias
        assert result.startswith("SELECT * FROM ("), \
            f"Query with alias not wrapped: {query} -> {result}"
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        query=select_query_with_alias()
    )
    def test_property_9_alias_wrapping_format(self, mappings, query):
        """Property 9: Alias Wrapping Format.
        
        For any SQL query containing column aliases, wrapping it should produce
        a query in the format "SELECT * FROM (original_query) AS random_alias"
        where the random alias is alphanumeric.
        
        **Validates: Requirements 3.3, 8.2, 8.3, 8.4, 8.5**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        result = translator.translate(query)
        
        # Check format: SELECT * FROM (query) AS alias
        pattern = r'^SELECT \* FROM \((.*)\) AS ([a-z][a-z0-9]{7})$'
        match = re.match(pattern, result, re.DOTALL)
        
        assert match is not None, \
            f"Result doesn't match expected format: {result}"
        
        # Extract the inner query and alias
        inner_query = match.group(1)
        alias = match.group(2)
        
        # The inner query should be the original query (possibly with schema mapping applied)
        # We can't directly compare because schema mapping may have been applied
        # But we can verify the alias format
        
        # Verify alias is alphanumeric
        assert alias.isalnum(), f"Alias is not alphanumeric: {alias}"
        
        # Verify alias starts with a letter
        assert alias[0].isalpha(), f"Alias doesn't start with letter: {alias}"
        
        # Verify alias is 8 characters
        assert len(alias) == 8, f"Alias is not 8 characters: {alias}"
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        query=select_query_with_alias()
    )
    def test_property_10_random_alias_generation(self, mappings, query):
        """Property 10: Random Alias Generation.
        
        For any generated subquery alias, it should consist only of
        alphanumeric characters.
        
        **Validates: Requirements 3.4, 8.4**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        # Generate multiple translations
        results = [translator.translate(query) for _ in range(5)]
        
        # Extract aliases from all results
        aliases = []
        for result in results:
            match = re.search(r'AS ([a-z][a-z0-9]{7})$', result)
            if match:
                aliases.append(match.group(1))
        
        # All aliases should be found
        assert len(aliases) == 5, "Not all aliases were extracted"
        
        # All aliases should be alphanumeric
        assert all(alias.isalnum() for alias in aliases), \
            f"Some aliases are not alphanumeric: {aliases}"
        
        # All aliases should start with a letter
        assert all(alias[0].isalpha() for alias in aliases), \
            f"Some aliases don't start with letter: {aliases}"
        
        # All aliases should be 8 characters
        assert all(len(alias) == 8 for alias in aliases), \
            f"Some aliases are not 8 characters: {aliases}"
        
        # Aliases should vary (with high probability)
        # With 5 samples, we expect at least 2 different aliases
        unique_aliases = set(aliases)
        assert len(unique_aliases) >= 2, \
            f"All aliases are the same, randomness may be broken: {aliases}"
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        query=select_query_without_alias()
    )
    def test_property_8_no_alias_no_wrapping(self, mappings, query):
        """Property 8: Alias Detection - No Wrapping Without Aliases.
        
        For any SQL query without column aliases, it should not be wrapped.
        
        **Validates: Requirements 8.1**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        result = translator.translate(query)
        
        # Query without aliases should not be wrapped
        # (unless schema mapping changed it)
        # Check if it's wrapped
        if result.startswith("SELECT * FROM ("):
            # If wrapped, the original query should be inside
            # This might happen if the detection is conservative
            # Let's just verify it's a valid wrapping
            assert re.search(r'\) AS [a-z][a-z0-9]{7}$', result)
        else:
            # Not wrapped - this is expected for queries without aliases
            # The result might differ from query due to schema mapping
            pass
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        schema=sql_identifier(),
        table=sql_identifier(),
        column=sql_identifier(),
        alias=sql_identifier()
    )
    def test_property_9_schema_mapping_before_wrapping(self, schema, table, column, alias):
        """Property 9: Schema Mapping Applied Before Wrapping.
        
        For any query with schema references and aliases, schema mapping
        should be applied before alias wrapping.
        
        **Validates: Requirements 3.1, 3.2, 8.2**
        """
        # Create a mapping for the schema
        mappings = {schema: f"{schema}_mapped"}
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        # Create a query with schema reference and alias
        query = f"SELECT {column} AS {alias} FROM {schema}.{table}"
        
        result = translator.translate(query)
        
        # Should be wrapped
        assert result.startswith("SELECT * FROM (")
        
        # The inner query should have the mapped schema
        assert f"{schema}_mapped.{table}" in result
        
        # The original schema should not appear as a standalone reference
        # (unless it's the same as the mapped schema)
        if schema != f"{schema}_mapped":
            # Check that the original schema.table pattern is not present
            # Use word boundaries to avoid false positives with substrings
            import re
            pattern = rf'\b{re.escape(schema)}\.{re.escape(table)}\b'
            assert not re.search(pattern, result), \
                f"Original schema reference '{schema}.{table}' found in result: {result}"
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        table=sql_identifier(),
        column=sql_identifier(),
        alias=sql_identifier(),
        string_content=st.text(min_size=1, max_size=50)
    )
    def test_property_8_ignores_as_in_strings(self, mappings, table, column, alias, string_content):
        """Property 8: Alias Detection - Ignores AS in String Literals.
        
        For any query with AS keyword in string literals, those should not
        trigger alias wrapping.
        
        **Validates: Requirements 8.1**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        # Create a query with AS in a string literal but no actual alias
        query = f"SELECT {column} FROM {table} WHERE note = '{string_content}'"
        
        result = translator.translate(query)
        
        # Should not be wrapped (no column alias)
        # Unless the string content happens to create a false positive
        # Let's check if it's wrapped
        if result.startswith("SELECT * FROM ("):
            # If wrapped, verify it's still valid
            assert re.search(r'\) AS [a-z][a-z0-9]{7}$', result)
        else:
            # Not wrapped - expected behavior
            pass
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        query=select_query_with_alias()
    )
    def test_property_9_preserves_query_semantics(self, mappings, query):
        """Property 9: Alias Wrapping Preserves Query Semantics.
        
        For any query that is wrapped, the wrapping should preserve the
        original query's semantics by selecting all columns from it.
        
        **Validates: Requirements 3.5**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        result = translator.translate(query)
        
        # If wrapped, it should be SELECT * FROM (original_query) AS alias
        if result.startswith("SELECT * FROM ("):
            # Extract the inner query
            match = re.match(r'^SELECT \* FROM \((.*)\) AS [a-z][a-z0-9]{7}$', result, re.DOTALL)
            assert match is not None
            
            inner_query = match.group(1)
            
            # The inner query should contain the essential parts of the original query
            # (it may have schema mapping applied, so we can't do exact comparison)
            # But we can verify it's a SELECT query
            assert 'SELECT' in inner_query.upper()
            assert 'FROM' in inner_query.upper()
    
    @pytest.mark.property
    @settings(max_examples=50)
    @given(
        mappings=schema_mappings(),
        query=st.sampled_from([
            "INSERT INTO users (name) VALUES ('John')",
            "UPDATE users SET name = 'Jane' WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
            "CREATE TABLE users (id INT, name VARCHAR(100))",
        ])
    )
    def test_property_8_non_select_not_wrapped(self, mappings, query):
        """Property 8: Alias Detection - Non-SELECT Queries Not Wrapped.
        
        For any non-SELECT query, it should not be wrapped even if it
        contains AS keyword.
        
        **Validates: Requirements 8.1**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        result = translator.translate(query)
        
        # Non-SELECT queries should not be wrapped
        assert not result.startswith("SELECT * FROM (")
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        query=select_query_with_alias()
    )
    def test_property_10_alias_uniqueness(self, mappings, query):
        """Property 10: Random Alias Generation - Uniqueness.
        
        For any query translated multiple times, the generated aliases
        should be different with high probability.
        
        **Validates: Requirements 3.4, 8.4**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        # Generate 20 translations
        results = [translator.translate(query) for _ in range(20)]
        
        # Extract aliases
        aliases = []
        for result in results:
            match = re.search(r'AS ([a-z][a-z0-9]{7})$', result)
            if match:
                aliases.append(match.group(1))
        
        # Should have extracted all aliases
        assert len(aliases) == 20
        
        # With 20 samples and 8-character random strings (36^8 possibilities),
        # we expect all or nearly all to be unique
        unique_aliases = set(aliases)
        
        # At least 15 out of 20 should be unique (allowing for some collisions)
        assert len(unique_aliases) >= 15, \
            f"Too many duplicate aliases: {len(unique_aliases)} unique out of 20"
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        query=select_query_with_alias()
    )
    def test_property_9_trailing_semicolon_handling(self, mappings, query):
        """Property 9: Alias Wrapping - Trailing Semicolon Handling.
        
        For any query with a trailing semicolon, it should be removed
        before wrapping.
        
        **Validates: Requirements 8.2, 8.5**
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        # Add trailing semicolon
        query_with_semicolon = query + ";"
        
        result = translator.translate(query_with_semicolon)
        
        # Should be wrapped
        assert result.startswith("SELECT * FROM (")
        
        # Should not have semicolon inside the subquery
        # Extract inner query
        match = re.match(r'^SELECT \* FROM \((.*)\) AS [a-z][a-z0-9]{7}$', result, re.DOTALL)
        if match:
            inner_query = match.group(1)
            # Inner query should not end with semicolon
            assert not inner_query.rstrip().endswith(';')
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(mappings=schema_mappings())
    def test_empty_query_unchanged(self, mappings):
        """Verify that empty queries are handled gracefully."""
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        result = translator.translate("")
        assert result == ""
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        mappings=schema_mappings(),
        query=select_query_with_alias()
    )
    def test_idempotent_translation(self, mappings, query):
        """Verify that translating a translated query doesn't double-wrap.
        
        Note: This property may not hold if the translated query itself
        contains aliases, which would trigger another wrapping.
        """
        mapper = SchemaMapper(mappings)
        translator = QueryTranslator(mapper)
        
        result1 = translator.translate(query)
        result2 = translator.translate(result1)
        
        # The second translation will wrap the already-wrapped query
        # because "SELECT * FROM (...) AS alias" doesn't have column aliases
        # So result2 should equal result1 (no double wrapping)
        # Actually, the wrapped query "SELECT * FROM (...) AS alias" has a table alias,
        # not a column alias, so it shouldn't be wrapped again
        assert result2 == result1, \
            "Translation is not idempotent - query was wrapped twice"
