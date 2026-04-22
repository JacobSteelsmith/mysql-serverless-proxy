"""MySQL session implementation using mysql-mimic for RDS Data API proxy.

This module implements the Session interface from mysql-mimic to handle
MySQL client connections and route queries to AWS RDS Data API.
"""

import asyncio
import logging
from typing import Any, Optional

from mysql_mimic import Session

from .config import ConfigurationManager
from .query_translator import QueryTranslator
from .schema_mapper import SchemaMapper
from .rds_client import RDSClient
from .response_translator import ResponseTranslator


logger = logging.getLogger(__name__)


class RDSProxySession(Session):
    """MySQL session that proxies queries to AWS RDS Data API.
    
    This class implements the mysql-mimic Session interface to handle
    MySQL protocol connections. It translates queries and routes them
    to AWS RDS Data API, then translates responses back to MySQL format.
    """
    
    # Class-level shared components (initialized once)
    _config: Optional[ConfigurationManager] = None
    _query_translator: Optional[QueryTranslator] = None
    _rds_client: Optional[RDSClient] = None
    _response_translator: Optional[ResponseTranslator] = None
    
    @classmethod
    def initialize(cls, config: ConfigurationManager):
        """Initialize shared components for all sessions.
        
        Args:
            config: Configuration manager instance
        """
        cls._config = config
        schema_mapper = SchemaMapper(config.get_schema_mappings())
        cls._query_translator = QueryTranslator(schema_mapper)
        cls._rds_client = RDSClient(config)
        cls._response_translator = ResponseTranslator()
        logger.info("RDSProxySession initialized with shared components")
    
    def __init__(self, *args, **kwargs):
        """Initialize session instance.
        
        Args:
            *args: Positional arguments passed to parent Session
            **kwargs: Keyword arguments passed to parent Session
        """
        super().__init__(*args, **kwargs)
        
        if self._config is None:
            raise RuntimeError(
                "RDSProxySession.initialize() must be called before creating sessions"
            )
        
        logger.debug(f"Created new session for user: {self.username}")
    
    async def handle_query(self, sql: str, attrs: dict) -> tuple[list[tuple], list[str]]:
        """Override handle_query to bypass sqlglot parsing and pass all queries to RDS.
        
        mysql-mimic's default handle_query tries to parse SQL with sqlglot and handle
        some queries locally (like SHOW DATABASES). We bypass this entirely and send
        all queries directly to RDS Data API.
        
        Args:
            sql: SQL query string
            attrs: Query attributes
            
        Returns:
            Tuple of (rows, column_names)
        """
        # Skip sqlglot parsing entirely - pass all queries directly to RDS
        logger.debug(f"Bypassing sqlglot, sending query directly to RDS: {sql[:100]}...")
        return await self.query(None, sql, attrs)
    
    async def query(
        self, 
        expression: Any, 
        sql: str, 
        attrs: dict
    ) -> tuple[list[tuple], list[str]]:
        """Handle query execution.
        
        This method is called by mysql-mimic when a client sends a query.
        It translates the query, executes it via RDS Data API, and returns
        the results in the format expected by mysql-mimic.
        
        Args:
            expression: Parsed SQL expression (from sqlglot) - may be None
            sql: Original SQL string
            attrs: Query attributes
            
        Returns:
            Tuple of (rows, column_names) where:
            - rows: List of tuples, each tuple is a row of data
            - column_names: List of column name strings
        """
        logger.info(
            f"[{self.username}@{self.database or 'none'}] Executing query: {sql[:100]}..."
        )
        logger.debug(f"Expression type: {type(expression)}, Expression: {expression}")
        
        try:
            # Translate query (schema mapping, alias wrapping)
            translated_sql = self._query_translator.translate(sql)
            logger.debug(f"Translated query: {translated_sql[:100]}...")
            
            # Execute via RDS Data API (run in thread pool since it's sync)
            result = await asyncio.to_thread(
                self._rds_client.execute_query,
                translated_sql,
                database=self.database
            )
            
            # Check for errors
            if result.error:
                logger.error(f"Query execution failed: {result.error}")
                raise result.error
            
            # Translate response
            if result.columns:
                # SELECT query - return result set
                columns, rows = self._response_translator.translate_result_set(result)
                
                # Convert to format expected by mysql-mimic
                # rows: list of tuples
                # column_names: list of strings
                column_names = [col.name for col in columns]
                row_tuples = [tuple(row) for row in rows]
                
                logger.info(
                    f"Query completed: {len(row_tuples)} rows returned"
                )
                
                return row_tuples, column_names
            else:
                # DML query (INSERT/UPDATE/DELETE) - return empty result
                affected_rows = self._response_translator.get_affected_rows(result)
                logger.info(
                    f"Query completed: {affected_rows} rows affected"
                )
                
                # For DML queries, mysql-mimic handles affected rows automatically
                # We just return empty result
                return [], []
                
        except Exception as e:
            logger.error(
                f"Error executing query: {e}",
                exc_info=True
            )
            # Re-raise to let mysql-mimic handle error response
            raise
    
    async def schema(self) -> dict[str, dict[str, str]]:
        """Provide database schema information.
        
        Return empty dict to disable mysql-mimic's default schema handling.
        This should force SHOW queries to be passed through to our query() method.
        
        Returns:
            Empty dictionary
        """
        # Return empty dict - no local schema, all queries go to RDS
        return {}
