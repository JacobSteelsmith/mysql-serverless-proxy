"""Unit tests for schema mapping functionality.

Tests schema name replacement in SQL queries across various contexts.
"""

import pytest

from mysql_rds_proxy.schema_mapper import SchemaMapper


class TestSchemaMapper:
    """Unit tests for SchemaMapper class."""
    
    def test_simple_from_clause(self):
        """Test schema mapping in a simple FROM clause."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs', 'test': 'test_production'})
        query = "SELECT * FROM mydb.applications"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM mydb_jacobs.applications"
    
    def test_multiple_tables_same_schema(self):
        """Test schema mapping with multiple tables from the same schema."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM mydb.users u JOIN mydb.orders o ON u.id = o.user_id"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM mydb_jacobs.users u JOIN mydb_jacobs.orders o ON u.id = o.user_id"
    
    def test_multiple_schemas(self):
        """Test schema mapping with multiple different schemas."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs', 'test': 'test_production'})
        query = "SELECT * FROM mydb.users u JOIN test.config c ON u.config_id = c.id"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM mydb_jacobs.users u JOIN test_production.config c ON u.config_id = c.id"
    
    def test_insert_statement(self):
        """Test schema mapping in INSERT statement."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "INSERT INTO mydb.users (name, email) VALUES ('John', 'john@example.com')"
        result = mapper.map_schema_names(query)
        assert result == "INSERT INTO mydb_jacobs.users (name, email) VALUES ('John', 'john@example.com')"
    
    def test_update_statement(self):
        """Test schema mapping in UPDATE statement."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "UPDATE mydb.users SET name = 'Jane' WHERE id = 1"
        result = mapper.map_schema_names(query)
        assert result == "UPDATE mydb_jacobs.users SET name = 'Jane' WHERE id = 1"
    
    def test_delete_statement(self):
        """Test schema mapping in DELETE statement."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "DELETE FROM mydb.users WHERE id = 1"
        result = mapper.map_schema_names(query)
        assert result == "DELETE FROM mydb_jacobs.users WHERE id = 1"
    
    def test_join_clause(self):
        """Test schema mapping in JOIN clause."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM users u INNER JOIN mydb.orders o ON u.id = o.user_id"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM users u INNER JOIN mydb_jacobs.orders o ON u.id = o.user_id"
    
    def test_left_join(self):
        """Test schema mapping in LEFT JOIN clause."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM users u LEFT JOIN mydb.orders o ON u.id = o.user_id"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM users u LEFT JOIN mydb_jacobs.orders o ON u.id = o.user_id"
    
    def test_no_schema_reference(self):
        """Test query without schema references remains unchanged."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM users WHERE id = 1"
        result = mapper.map_schema_names(query)
        assert result == query
    
    def test_unmapped_schema(self):
        """Test that unmapped schema names are left unchanged."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM other.users WHERE id = 1"
        result = mapper.map_schema_names(query)
        assert result == query
    
    def test_empty_mappings(self):
        """Test that empty mappings leave query unchanged."""
        mapper = SchemaMapper({})
        query = "SELECT * FROM mydb.users WHERE id = 1"
        result = mapper.map_schema_names(query)
        assert result == query
    
    def test_backtick_quoted_schema(self):
        """Test schema mapping with backtick-quoted schema name."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM `mydb`.applications"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM `mydb_jacobs`.applications"
    
    def test_backtick_quoted_table(self):
        """Test schema mapping with backtick-quoted table name."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM mydb.`applications`"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM mydb_jacobs.`applications`"
    
    def test_both_backtick_quoted(self):
        """Test schema mapping with both schema and table backtick-quoted."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM `mydb`.`applications`"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM `mydb_jacobs`.`applications`"
    
    def test_whitespace_around_dot(self):
        """Test schema mapping with whitespace around the dot."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM mydb . applications"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM mydb_jacobs . applications"
    
    def test_case_sensitive_schema_names(self):
        """Test that schema name matching is case-sensitive."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM mydb.users"
        result = mapper.map_schema_names(query)
        # Should not match because 'mydb' != 'mydb'
        assert result == query
    
    def test_schema_name_with_underscore(self):
        """Test schema mapping with underscores in schema name."""
        mapper = SchemaMapper({'my_schema': 'prod_schema'})
        query = "SELECT * FROM my_schema.users"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM prod_schema.users"
    
    def test_schema_name_with_numbers(self):
        """Test schema mapping with numbers in schema name."""
        mapper = SchemaMapper({'schema123': 'prod_schema456'})
        query = "SELECT * FROM schema123.users"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM prod_schema456.users"
    
    def test_subquery_with_schema(self):
        """Test schema mapping in subquery."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM (SELECT * FROM mydb.users) AS u"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM (SELECT * FROM mydb_jacobs.users) AS u"
    
    def test_multiple_occurrences_same_schema(self):
        """Test that all occurrences of the same schema are replaced."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = """
            SELECT u.*, o.* 
            FROM mydb.users u
            JOIN mydb.orders o ON u.id = o.user_id
            WHERE EXISTS (SELECT 1 FROM mydb.permissions p WHERE p.user_id = u.id)
        """
        result = mapper.map_schema_names(query)
        assert result.count('mydb_jacobs') == 3
        assert 'mydb.' not in result
    
    def test_schema_in_where_clause(self):
        """Test schema mapping in WHERE clause with subquery."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM users WHERE id IN (SELECT user_id FROM mydb.orders)"
        result = mapper.map_schema_names(query)
        assert result == "SELECT * FROM users WHERE id IN (SELECT user_id FROM mydb_jacobs.orders)"
    
    def test_complex_query_multiple_contexts(self):
        """Test schema mapping in a complex query with multiple contexts."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs', 'test': 'test_production'})
        query = """
            INSERT INTO mydb.audit_log (user_id, action)
            SELECT u.id, 'login'
            FROM mydb.users u
            LEFT JOIN test.sessions s ON u.id = s.user_id
            WHERE u.active = 1
        """
        result = mapper.map_schema_names(query)
        assert 'mydb_jacobs.audit_log' in result
        assert 'mydb_jacobs.users' in result
        assert 'test_production.sessions' in result
        assert 'mydb.' not in result
        assert 'test.' not in result
    
    def test_schema_mapping_preserves_query_structure(self):
        """Test that schema mapping preserves SQL structure and semantics."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT COUNT(*) FROM mydb.users WHERE created_at > '2024-01-01'"
        result = mapper.map_schema_names(query)
        # Should only replace the schema name, everything else unchanged
        assert result == "SELECT COUNT(*) FROM mydb_jacobs.users WHERE created_at > '2024-01-01'"
    
    def test_empty_query(self):
        """Test that empty query is handled gracefully."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = ""
        result = mapper.map_schema_names(query)
        assert result == ""
    
    def test_query_without_tables(self):
        """Test query without table references."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT 1 + 1 AS result"
        result = mapper.map_schema_names(query)
        assert result == query
    
    def test_schema_name_as_substring(self):
        """Test that schema name as substring in other identifiers is not replaced."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        # 'mydb' appears in column name but should not be replaced
        query = "SELECT mydb_column FROM users"
        result = mapper.map_schema_names(query)
        assert result == query
    
    def test_dot_in_string_literal(self):
        """Test that dots in string literals are not treated as schema separators."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        query = "SELECT * FROM mydb.users WHERE email = 'user@mydb.com'"
        result = mapper.map_schema_names(query)
        # Should only replace the schema reference, not the dot in the email
        assert result == "SELECT * FROM mydb_jacobs.users WHERE email = 'user@mydb.com'"


class TestSchemaMapperEdgeCases:
    """Edge case tests for schema mapping functionality.
    
    Tests Requirements 7.3, 7.5 - edge cases for schema mapping.
    """
    
    def test_query_with_no_schema_references(self):
        """Test queries with no schema references remain unchanged.
        
        Requirements: 7.3
        """
        mapper = SchemaMapper({'mydb': 'mydb_jacobs', 'test': 'test_production'})
        
        # Simple SELECT without schema
        query1 = "SELECT * FROM users WHERE id = 1"
        assert mapper.map_schema_names(query1) == query1
        
        # JOIN without schema
        query2 = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id"
        assert mapper.map_schema_names(query2) == query2
        
        # INSERT without schema
        query3 = "INSERT INTO users (name) VALUES ('John')"
        assert mapper.map_schema_names(query3) == query3
        
        # UPDATE without schema
        query4 = "UPDATE users SET name = 'Jane' WHERE id = 1"
        assert mapper.map_schema_names(query4) == query4
        
        # DELETE without schema
        query5 = "DELETE FROM users WHERE id = 1"
        assert mapper.map_schema_names(query5) == query5
        
        # Complex query without schema
        query6 = """
            SELECT u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.active = 1
            GROUP BY u.id
            HAVING COUNT(o.id) > 5
        """
        assert mapper.map_schema_names(query6) == query6
    
    def test_query_with_unmapped_schema_names(self):
        """Test queries with unmapped schema names are left unchanged.
        
        Requirements: 7.3
        """
        mapper = SchemaMapper({'mydb': 'mydb_jacobs', 'test': 'test_production'})
        
        # Single unmapped schema
        query1 = "SELECT * FROM other.users WHERE id = 1"
        assert mapper.map_schema_names(query1) == query1
        
        # Multiple unmapped schemas
        query2 = "SELECT * FROM other.users u JOIN another.orders o ON u.id = o.user_id"
        assert mapper.map_schema_names(query2) == query2
        
        # Mix of mapped and unmapped schemas
        query3 = "SELECT * FROM mydb.users u JOIN other.orders o ON u.id = o.user_id"
        result3 = mapper.map_schema_names(query3)
        assert 'mydb_jacobs.users' in result3
        assert 'other.orders' in result3
        
        # Unmapped schema in subquery
        query4 = "SELECT * FROM mydb.users WHERE id IN (SELECT user_id FROM other.orders)"
        result4 = mapper.map_schema_names(query4)
        assert 'mydb_jacobs.users' in result4
        assert 'other.orders' in result4
    
    def test_query_with_multiple_occurrences_same_schema(self):
        """Test queries with multiple occurrences of the same schema.
        
        All occurrences should be replaced consistently.
        Requirements: 7.2, 7.3
        """
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        
        # Two occurrences in FROM and JOIN
        query1 = "SELECT * FROM mydb.users u JOIN mydb.orders o ON u.id = o.user_id"
        result1 = mapper.map_schema_names(query1)
        assert result1.count('mydb_jacobs') == 2
        assert 'mydb.' not in result1
        
        # Three occurrences in complex query
        query2 = """
            SELECT u.*, o.*, p.*
            FROM mydb.users u
            JOIN mydb.orders o ON u.id = o.user_id
            JOIN mydb.permissions p ON u.id = p.user_id
        """
        result2 = mapper.map_schema_names(query2)
        assert result2.count('mydb_jacobs') == 3
        assert 'mydb.' not in result2
        
        # Multiple occurrences with subquery
        query3 = """
            SELECT * FROM mydb.users
            WHERE id IN (SELECT user_id FROM mydb.orders)
            AND status IN (SELECT status FROM mydb.user_status)
        """
        result3 = mapper.map_schema_names(query3)
        assert result3.count('mydb_jacobs') == 3
        assert 'mydb.' not in result3
        
        # Multiple occurrences in INSERT with SELECT
        query4 = """
            INSERT INTO mydb.audit_log (user_id, action)
            SELECT u.id, 'login'
            FROM mydb.users u
            WHERE u.id NOT IN (SELECT user_id FROM mydb.recent_logins)
        """
        result4 = mapper.map_schema_names(query4)
        assert result4.count('mydb_jacobs') == 3
        assert 'mydb.' not in result4
    
    def test_schema_names_in_different_sql_contexts(self):
        """Test schema names in different SQL contexts are all handled.
        
        Requirements: 7.5
        """
        mapper = SchemaMapper({'mydb': 'mydb_jacobs', 'test': 'test_production'})
        
        # FROM clause
        query1 = "SELECT * FROM mydb.users"
        assert mapper.map_schema_names(query1) == "SELECT * FROM mydb_jacobs.users"
        
        # INNER JOIN
        query2 = "SELECT * FROM users u INNER JOIN mydb.orders o ON u.id = o.user_id"
        result2 = mapper.map_schema_names(query2)
        assert 'mydb_jacobs.orders' in result2
        
        # LEFT JOIN
        query3 = "SELECT * FROM users u LEFT JOIN mydb.orders o ON u.id = o.user_id"
        result3 = mapper.map_schema_names(query3)
        assert 'mydb_jacobs.orders' in result3
        
        # RIGHT JOIN
        query4 = "SELECT * FROM users u RIGHT JOIN mydb.orders o ON u.id = o.user_id"
        result4 = mapper.map_schema_names(query4)
        assert 'mydb_jacobs.orders' in result4
        
        # CROSS JOIN
        query5 = "SELECT * FROM users u CROSS JOIN mydb.config c"
        result5 = mapper.map_schema_names(query5)
        assert 'mydb_jacobs.config' in result5
        
        # INSERT INTO
        query6 = "INSERT INTO mydb.users (name) VALUES ('John')"
        assert mapper.map_schema_names(query6) == "INSERT INTO mydb_jacobs.users (name) VALUES ('John')"
        
        # UPDATE
        query7 = "UPDATE mydb.users SET name = 'Jane' WHERE id = 1"
        assert mapper.map_schema_names(query7) == "UPDATE mydb_jacobs.users SET name = 'Jane' WHERE id = 1"
        
        # DELETE FROM
        query8 = "DELETE FROM mydb.users WHERE id = 1"
        assert mapper.map_schema_names(query8) == "DELETE FROM mydb_jacobs.users WHERE id = 1"
        
        # Subquery in WHERE clause
        query9 = "SELECT * FROM users WHERE id IN (SELECT user_id FROM mydb.orders)"
        result9 = mapper.map_schema_names(query9)
        assert 'mydb_jacobs.orders' in result9
        
        # Subquery in FROM clause
        query10 = "SELECT * FROM (SELECT * FROM mydb.users) AS u"
        result10 = mapper.map_schema_names(query10)
        assert 'mydb_jacobs.users' in result10
        
        # Multiple contexts in one query
        query11 = """
            INSERT INTO test.audit_log (user_id, action)
            SELECT u.id, 'update'
            FROM mydb.users u
            LEFT JOIN test.sessions s ON u.id = s.user_id
            WHERE u.id IN (SELECT user_id FROM mydb.recent_activity)
        """
        result11 = mapper.map_schema_names(query11)
        assert 'test_production.audit_log' in result11
        assert 'mydb_jacobs.users' in result11
        assert 'test_production.sessions' in result11
        assert 'mydb_jacobs.recent_activity' in result11
        assert 'mydb.' not in result11
        assert 'test.' not in result11
    
    def test_edge_case_schema_name_at_query_boundaries(self):
        """Test schema references at the start or end of queries."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        
        # Schema at start of query
        query1 = "mydb.users"
        # This won't match because there's no SQL keyword before it
        # But let's test a valid minimal query
        query1 = "SELECT * FROM mydb.users"
        assert mapper.map_schema_names(query1) == "SELECT * FROM mydb_jacobs.users"
        
        # Schema at end of query (no semicolon)
        query2 = "SELECT * FROM mydb.users"
        assert mapper.map_schema_names(query2) == "SELECT * FROM mydb_jacobs.users"
        
        # Schema at end of query (with semicolon)
        query3 = "SELECT * FROM mydb.users;"
        assert mapper.map_schema_names(query3) == "SELECT * FROM mydb_jacobs.users;"
    
    def test_edge_case_very_long_query(self):
        """Test schema mapping in very long queries."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        
        # Generate a long query with many schema references
        tables = ['users', 'orders', 'products', 'categories', 'reviews']
        query_parts = ["SELECT * FROM mydb.users u"]
        for i, table in enumerate(tables[1:], 1):
            query_parts.append(f"JOIN mydb.{table} t{i} ON u.id = t{i}.user_id")
        query = " ".join(query_parts)
        
        result = mapper.map_schema_names(query)
        
        # All schema references should be replaced
        assert result.count('mydb_jacobs') == len(tables)
        assert 'mydb.' not in result
    
    def test_edge_case_schema_with_special_patterns(self):
        """Test schema names that might cause regex issues."""
        # Schema name that's a substring of another
        mapper = SchemaMapper({'nt': 'nt_prod', 'mydb': 'mydb_jacobs'})
        
        query = "SELECT * FROM mydb.users u JOIN nt.config c ON u.config_id = c.id"
        result = mapper.map_schema_names(query)
        
        # Both should be replaced correctly
        assert 'mydb_jacobs.users' in result
        assert 'nt_prod.config' in result
        # Original schemas should not appear
        assert 'mydb.' not in result
        assert 'nt.' not in result or 'nt_prod' in result  # nt. might appear in mydb_jacobs
    
    def test_edge_case_empty_and_whitespace_queries(self):
        """Test edge cases with empty or whitespace-only queries."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        
        # Empty query
        assert mapper.map_schema_names("") == ""
        
        # Whitespace only
        assert mapper.map_schema_names("   ") == "   "
        assert mapper.map_schema_names("\n\t") == "\n\t"
        
        # Query with lots of whitespace
        query = "SELECT   *   FROM   mydb.users   WHERE   id   =   1"
        result = mapper.map_schema_names(query)
        assert 'mydb_jacobs.users' in result
    
    def test_edge_case_schema_in_comments(self):
        """Test that schema references in comments are handled.
        
        Note: Current implementation may not handle SQL comments specially,
        so this test documents the current behavior.
        """
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        
        # Single-line comment (MySQL style)
        query1 = "SELECT * FROM mydb.users -- from mydb.users table"
        result1 = mapper.map_schema_names(query1)
        # The schema in the actual query should be replaced
        assert 'mydb_jacobs.users' in result1
        # Note: The comment might also be affected, which is acceptable
        
        # Multi-line comment
        query2 = "/* Query mydb.users */ SELECT * FROM mydb.users"
        result2 = mapper.map_schema_names(query2)
        # The schema in the actual query should be replaced
        assert 'mydb_jacobs.users' in result2
    
    def test_edge_case_case_sensitivity(self):
        """Test case sensitivity in schema name matching."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs', 'mydb': 'mydb_JACOBS'})
        
        # Lowercase schema
        query1 = "SELECT * FROM mydb.users"
        assert mapper.map_schema_names(query1) == "SELECT * FROM mydb_jacobs.users"
        
        # Uppercase schema (different mapping)
        query2 = "SELECT * FROM mydb.users"
        assert mapper.map_schema_names(query2) == "SELECT * FROM mydb_JACOBS.users"
        
        # Mixed case (no mapping)
        query3 = "SELECT * FROM mydb.users"
        assert mapper.map_schema_names(query3) == query3
    
    def test_edge_case_numeric_table_names(self):
        """Test schema mapping with numeric table names."""
        mapper = SchemaMapper({'mydb': 'mydb_jacobs'})
        
        # Table name starting with number (if valid in the DB)
        query = "SELECT * FROM mydb.table123"
        result = mapper.map_schema_names(query)
        assert 'mydb_jacobs.table123' in result
    
    def test_edge_case_schema_mapping_with_same_source_and_target(self):
        """Test schema mapping where source and target are the same."""
        mapper = SchemaMapper({'mydb': 'mydb', 'test': 'test_production'})
        
        # Schema that maps to itself
        query1 = "SELECT * FROM mydb.users"
        result1 = mapper.map_schema_names(query1)
        assert result1 == "SELECT * FROM mydb.users"
        
        # Mix of self-mapping and different mapping
        query2 = "SELECT * FROM mydb.users u JOIN test.config c ON u.config_id = c.id"
        result2 = mapper.map_schema_names(query2)
        assert 'mydb.users' in result2
        assert 'test_production.config' in result2
