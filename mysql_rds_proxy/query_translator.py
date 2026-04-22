"""Query translation for RDS Data API compatibility.

This module provides functionality to transform MySQL queries for compatibility
with AWS RDS Data API, including alias wrapping and schema name mapping.
"""

import random
import re
import string
from typing import Optional

from mysql_rds_proxy.schema_mapper import SchemaMapper


class QueryTranslator:
    """Translates MySQL queries for RDS Data API compatibility.
    
    The QueryTranslator applies transformations to MySQL queries to work around
    RDS Data API limitations. The main transformation is wrapping queries that
    contain column aliases in a subquery, as the Data API has issues with aliases.
    
    The translator also integrates schema name mapping to replace schema names
    according to configuration.
    
    Attributes:
        schema_mapper: SchemaMapper instance for schema name replacement
    """
    
    def __init__(self, schema_mapper: SchemaMapper):
        """Initialize query translator.
        
        Args:
            schema_mapper: SchemaMapper instance for schema name replacement
        """
        self.schema_mapper = schema_mapper
    
    def translate(self, query: str) -> str:
        """Apply all transformations to the query.
        
        This method applies the following transformations in order:
        1. Schema name mapping (replace schema names according to configuration)
        2. Alias wrapping (wrap queries with column aliases in a subquery)
        
        Args:
            query: Original SQL query string
            
        Returns:
            Transformed query string ready for RDS Data API execution
        """
        # First, apply schema name mapping
        query = self.schema_mapper.map_schema_names(query)
        
        # Then, check if alias wrapping is needed
        if self._needs_alias_wrapping(query):
            query = self._wrap_with_subquery(query)
        
        return query
    
    def _needs_alias_wrapping(self, query: str) -> bool:
        """Detect if query contains column aliases.
        
        A query needs alias wrapping if it contains the AS keyword used for
        column aliasing in the SELECT clause. This method attempts to distinguish
        between column aliases (SELECT col AS alias) and table aliases
        (FROM table AS alias).
        
        The detection is conservative: it looks for AS keyword patterns that
        are likely to be column aliases. This may have false positives (detecting
        table aliases as column aliases), but wrapping a query unnecessarily is
        safe, while missing a column alias would cause issues with RDS Data API.
        
        Args:
            query: SQL query string to check
            
        Returns:
            True if the query appears to contain column aliases, False otherwise
        """
        # Remove string literals to avoid false matches
        query_without_strings = self._remove_string_literals(query)
        
        # First, check if this is a SELECT query
        if not re.search(r'\bSELECT\b', query_without_strings, re.IGNORECASE):
            return False
        
        # Calculate parenthesis depth at each position to ignore AS inside subqueries
        paren_depth = self._calculate_paren_depth(query_without_strings)
        
        # Look for AS keyword followed by an identifier or quoted identifier
        # This pattern matches: AS identifier or AS `identifier` or AS "identifier"
        # Note: We don't use \b at the end for quoted identifiers since quotes aren't word chars
        as_pattern = r'\bAS\s+(?:`[^`]+`|"[^"]+"|[a-zA-Z_][a-zA-Z0-9_]*(?:\b|(?=[^a-zA-Z0-9_])))'
        
        matches = re.finditer(as_pattern, query_without_strings, re.IGNORECASE)
        
        for match in matches:
            # Skip AS keywords inside subqueries (parenthesis depth > 0)
            if paren_depth[match.start()] > 0:
                continue
            
            # Check what comes before AS to distinguish column aliases from table aliases
            before_as = query_without_strings[:match.start()].rstrip()
            
            # Check if AS appears after FROM, JOIN, etc. (table alias context)
            # Look backwards for the nearest SQL keyword
            # Extract a reasonable chunk before AS to check context
            context_before = before_as[-100:] if len(before_as) > 100 else before_as
            
            # If the immediate context suggests a table alias, skip it
            # Table aliases typically appear after: FROM table AS alias, JOIN table AS alias
            # Look for pattern: (FROM|JOIN) followed by table name (possibly with schema)
            # followed by optional whitespace before AS
            if re.search(r'\b(FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|CROSS\s+JOIN)\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?\s*$', 
                        context_before, re.IGNORECASE):
                continue
            
            # Check for subquery pattern: ) AS alias
            # But we need to distinguish between:
            # - (SELECT ...) AS alias (table alias - skip)
            # - COUNT(*) AS alias (column alias - detect)
            # - CASE ... END AS alias (column alias - detect)
            if before_as.endswith(')'):
                # Look for keywords that indicate this is a subquery table alias
                # If we see FROM or JOIN before the opening paren, it's likely a table alias
                # Find the matching opening paren
                paren_count = 1
                i = len(before_as) - 2  # Start before the closing paren
                while i >= 0 and paren_count > 0:
                    if before_as[i] == ')':
                        paren_count += 1
                    elif before_as[i] == '(':
                        paren_count -= 1
                    i -= 1
                
                # i is now at the position before the opening paren
                if i >= 0:
                    before_paren = before_as[:i+1].rstrip()
                    # Check if there's FROM or JOIN right before the opening paren
                    if re.search(r'\b(FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|CROSS\s+JOIN)\s*$', 
                                before_paren, re.IGNORECASE):
                        # This is a subquery table alias, skip it
                        continue
            
            # Otherwise, it's likely a column alias
            return True
        
        return False
    
    def _calculate_paren_depth(self, query: str) -> list[int]:
        """Calculate parenthesis depth at each position in the query.
        
        Returns a list where each element is the parenthesis depth at that
        position in the query. Depth 0 means not inside any parentheses.
        
        Args:
            query: SQL query string
            
        Returns:
            List of integers representing parenthesis depth at each position
        """
        depth = []
        current_depth = 0
        
        for char in query:
            depth.append(current_depth)
            if char == '(':
                current_depth += 1
            elif char == ')':
                current_depth = max(0, current_depth - 1)
        
        return depth
    
    def _wrap_with_subquery(self, query: str) -> str:
        """Wrap query in a subquery with random alias.
        
        This method wraps the original query in a subquery to work around
        RDS Data API limitations with column aliases. The format is:
        SELECT * FROM (original_query) AS random_alias
        
        Args:
            query: Original SQL query string
            
        Returns:
            Query wrapped in a subquery with a random alias
        """
        # Generate a random alias
        alias = self._generate_random_alias()
        
        # Strip trailing semicolon if present
        query = query.rstrip()
        if query.endswith(';'):
            query = query[:-1].rstrip()
        
        # Wrap the query
        wrapped = f"SELECT * FROM ({query}) AS {alias}"
        
        return wrapped
    
    def _generate_random_alias(self) -> str:
        """Generate a random alphanumeric alias.
        
        Generates a random string of lowercase letters and digits to use as
        a subquery alias. The alias is 8 characters long to minimize collision
        probability while keeping it reasonably short.
        
        Returns:
            Random alphanumeric string (e.g., 'x5rhy2k9')
        """
        # Use lowercase letters and digits
        chars = string.ascii_lowercase + string.digits
        
        # Generate 8 random characters
        # Start with a letter to ensure it's a valid SQL identifier
        first_char = random.choice(string.ascii_lowercase)
        rest_chars = ''.join(random.choice(chars) for _ in range(7))
        
        return first_char + rest_chars
    
    def _remove_string_literals(self, query: str) -> str:
        """Remove string literals from query to avoid false matches.
        
        Replaces all string literals (single-quoted only) with empty strings
        to prevent matching patterns inside string literals. Double quotes are
        not removed as they can be used for identifiers in SQL.
        
        Args:
            query: SQL query string
            
        Returns:
            Query with string literals removed
        """
        result = []
        in_string = False
        i = 0
        
        while i < len(query):
            char = query[i]
            
            if not in_string:
                if char == "'":
                    in_string = True
                    # Keep the quote character but skip the content
                    result.append(char)
                else:
                    result.append(char)
            else:
                # Inside string literal
                if char == "'":
                    # Check if it's escaped (doubled quote)
                    if i + 1 < len(query) and query[i + 1] == "'":
                        # Escaped quote, skip both
                        i += 1
                    else:
                        # End of string
                        result.append(char)
                        in_string = False
                # Skip content inside string literals
            
            i += 1
        
        return ''.join(result)
