"""Schema name mapping for SQL queries.

This module provides functionality to replace schema names in SQL queries
according to configured mappings, handling various SQL contexts and identifier formats.
"""

import re
from typing import Dict, List, Tuple


class SchemaMapper:
    """Maps schema names in SQL queries according to configuration.
    
    The SchemaMapper replaces schema names that appear in qualified table references
    (e.g., schema.table) throughout SQL queries. It handles various SQL contexts
    including FROM, JOIN, INSERT, UPDATE, and DELETE clauses, and supports both
    quoted and unquoted identifiers.
    
    Attributes:
        mappings: Dictionary mapping source schema names to target schema names
    """
    
    def __init__(self, mappings: Dict[str, str]):
        """Initialize with schema name mappings.
        
        Args:
            mappings: Dictionary mapping source schema names to target schema names.
                     Keys are the schema names to replace, values are the replacements.
        """
        self.mappings = mappings
    
    def map_schema_names(self, query: str) -> str:
        """Replace schema names in the query according to configured mappings.
        
        This method finds all schema references in the query (in the form schema.table)
        and replaces them with their mapped values if a mapping exists. Schema names
        that don't have a mapping are left unchanged.
        
        The method handles:
        - Unquoted identifiers: schema.table
        - Backtick-quoted identifiers: `schema`.`table`
        - Mixed quoting: schema.`table` or `schema`.table
        - Schema names in various SQL contexts (FROM, JOIN, INSERT, UPDATE, DELETE)
        
        Args:
            query: SQL query string to process
            
        Returns:
            Query string with schema names replaced according to mappings
        """
        if not self.mappings:
            return query
        
        # Find all schema references and their positions
        references = self._find_schema_references(query)
        
        # Sort by position in reverse order so we can replace from end to start
        # This prevents position shifts from affecting subsequent replacements
        references.sort(key=lambda x: x[0], reverse=True)
        
        # Replace each schema reference
        result = query
        for start_pos, end_pos, schema_name, whitespace in references:
            # Check if this schema has a mapping
            if schema_name in self.mappings:
                # Extract the original text (may include backticks)
                original_text = query[start_pos:end_pos]
                
                # Determine if the schema name was quoted
                if original_text.startswith('`') and '`' in original_text[1:]:
                    # Quoted schema: `schema` followed by whitespace and dot
                    replacement = f"`{self.mappings[schema_name]}`{whitespace}"
                else:
                    # Unquoted schema: schema followed by whitespace and dot
                    replacement = f"{self.mappings[schema_name]}{whitespace}"
                
                # Replace in result
                result = result[:start_pos] + replacement + result[end_pos:]
        
        return result
    
    def _find_schema_references(self, query: str) -> List[Tuple[int, int, str, str]]:
        """Find all schema references with their positions in the query.
        
        A schema reference is a qualified identifier in the form:
        - schema.table (unquoted)
        - `schema`.table (quoted schema)
        - schema.`table` (quoted table)
        - `schema`.`table` (both quoted)
        
        This method avoids matching schema-like patterns inside string literals.
        
        Args:
            query: SQL query string to search
            
        Returns:
            List of tuples (start_pos, end_pos, schema_name, whitespace) where:
            - start_pos: Starting position of the schema reference
            - end_pos: Position after the dot (start of table name)
            - schema_name: The schema name (without quotes)
            - whitespace: The whitespace and dot between schema and table
        """
        references = []
        
        # First, identify string literal positions to exclude them
        string_ranges = self._find_string_literals(query)
        
        # Pattern to match schema.table references
        # Matches:
        # - `schema`.table or `schema`.`table` (backtick-quoted schema)
        # - schema.table or schema.`table` (unquoted schema)
        # 
        # Pattern breakdown:
        # (?:`([a-zA-Z0-9_]+)`|([a-zA-Z0-9_]+)) - schema name (quoted or unquoted)
        # (\s*\.\s*) - dot with optional whitespace (captured to preserve it)
        # (?=`?[a-zA-Z0-9_]+`?) - lookahead for table name (not captured)
        pattern = r'(?:`([a-zA-Z0-9_]+)`|([a-zA-Z0-9_]+))(\s*\.\s*)(?=`?[a-zA-Z0-9_]+`?)'
        
        for match in re.finditer(pattern, query):
            # Check if this match is inside a string literal
            match_pos = match.start()
            if self._is_in_string_literal(match_pos, string_ranges):
                continue
            
            # Extract schema name (from either quoted or unquoted group)
            schema_name = match.group(1) if match.group(1) else match.group(2)
            
            # Extract the whitespace and dot
            whitespace = match.group(3)
            
            # Get positions
            start_pos = match.start()
            # End position is after the whitespace and dot
            end_pos = match.start() + len(match.group(0)) - len(whitespace)
            end_pos = match.end() - len(whitespace)
            
            # Actually, we want to include the whitespace in what we're replacing
            # So end_pos should be after the dot
            end_pos = match.end()
            
            references.append((start_pos, end_pos, schema_name, whitespace))
        
        return references
    
    def _find_string_literals(self, query: str) -> List[Tuple[int, int]]:
        """Find all string literal positions in the query.
        
        Args:
            query: SQL query string
            
        Returns:
            List of tuples (start, end) representing string literal ranges
        """
        string_ranges = []
        in_string = False
        string_start = 0
        quote_char = None
        i = 0
        
        while i < len(query):
            char = query[i]
            
            if not in_string:
                if char in ("'", '"'):
                    in_string = True
                    string_start = i
                    quote_char = char
            else:
                # Check for escaped quote or end of string
                if char == quote_char:
                    # Check if it's escaped (doubled quote)
                    if i + 1 < len(query) and query[i + 1] == quote_char:
                        # Escaped quote, skip both
                        i += 1
                    else:
                        # End of string
                        string_ranges.append((string_start, i + 1))
                        in_string = False
                        quote_char = None
            
            i += 1
        
        return string_ranges
    
    def _is_in_string_literal(self, pos: int, string_ranges: List[Tuple[int, int]]) -> bool:
        """Check if a position is inside a string literal.
        
        Args:
            pos: Position to check
            string_ranges: List of string literal ranges
            
        Returns:
            True if position is inside a string literal
        """
        for start, end in string_ranges:
            if start <= pos < end:
                return True
        return False
