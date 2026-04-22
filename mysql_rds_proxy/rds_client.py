"""RDS Data API client for executing queries against AWS RDS serverless clusters.

This module provides the RDSClient class that uses boto3 to execute SQL queries
via the AWS RDS Data API and retrieve results.
"""

from dataclasses import dataclass
from typing import Any, Optional
import logging

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from .config import ConfigurationManager


logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    """Column metadata from Data API.
    
    Attributes:
        name: Column name
        type_name: Data API type name (e.g., "VARCHAR", "INTEGER")
        nullable: Whether the column can contain NULL values
        precision: Numeric precision (for numeric types)
        scale: Numeric scale (for numeric types)
        label: Column label (for aliased columns)
    """
    name: str
    type_name: str
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None
    label: Optional[str] = None


@dataclass
class QueryResult:
    """Result from RDS Data API execution.
    
    Attributes:
        columns: List of column metadata
        rows: List of rows, where each row is a list of values
        affected_rows: Number of rows affected by DML statements
        last_insert_id: Last insert ID for INSERT statements
        error: Exception if query execution failed, None otherwise
    """
    columns: list[ColumnMetadata]
    rows: list[list[Any]]
    affected_rows: int = 0
    last_insert_id: int = 0
    error: Optional[Exception] = None


class RDSClient:
    """Client for executing queries against AWS RDS Data API.
    
    This class uses boto3 to communicate with the RDS Data API, executing
    SQL statements and retrieving results. It handles AWS credential chain
    authentication and error capture.
    """
    
    def __init__(self, config: ConfigurationManager):
        """Initialize RDS client with AWS configuration.
        
        Args:
            config: ConfigurationManager instance with AWS settings
        """
        self._config = config
        self._client: Optional[Any] = None
        self._cluster_arn = config.get_cluster_arn()
        self._secret_arn = config.get_secret_arn()
        self._region = config.get_aws_region()
        
        logger.info(
            f"RDS client initialized for region {self._region}, "
            f"cluster {self._cluster_arn}"
        )
    
    def _get_boto3_client(self) -> Any:
        """Get or create boto3 RDS Data API client.
        
        Creates a boto3 client on first call and reuses it for subsequent calls.
        Uses the AWS credential chain for authentication.
        
        Returns:
            boto3 RDS Data Service client
        """
        if self._client is None:
            logger.debug(f"Creating boto3 RDS Data client for region {self._region}")
            self._client = boto3.client(
                'rds-data',
                region_name=self._region
            )
        return self._client
    
    def execute_query(self, sql: str, database: Optional[str] = None) -> QueryResult:
        """Execute SQL query via Data API.
        
        Executes the provided SQL statement using the RDS Data API execute_statement
        operation. Retrieves all result rows and column metadata.
        
        Args:
            sql: SQL statement to execute
            database: Optional database name to use for the query
            
        Returns:
            QueryResult containing columns, rows, and execution metadata
        """
        logger.debug(f"Executing query: {sql[:100]}...")
        
        try:
            client = self._get_boto3_client()
            
            # Prepare execute_statement parameters
            params = {
                'resourceArn': self._cluster_arn,
                'secretArn': self._secret_arn,
                'sql': sql,
                'includeResultMetadata': True
            }
            
            # Add database parameter if provided
            if database:
                params['database'] = database
            
            # Execute the statement
            response = client.execute_statement(**params)
            
            logger.debug(f"Query executed successfully, processing results")
            
            # Extract result metadata
            affected_rows = response.get('numberOfRecordsUpdated', 0)
            
            # Extract column metadata if present
            columns = []
            column_metadata = response.get('columnMetadata', [])
            for col_meta in column_metadata:
                column = ColumnMetadata(
                    name=col_meta.get('name', ''),
                    type_name=col_meta.get('typeName', 'VARCHAR'),
                    nullable=col_meta.get('nullable', 1) == 1,
                    precision=col_meta.get('precision'),
                    scale=col_meta.get('scale'),
                    label=col_meta.get('label')
                )
                columns.append(column)
            
            # Extract rows if present
            rows = []
            records = response.get('records', [])
            for record in records:
                row = []
                for field in record:
                    # Extract value from field based on type
                    value = self._extract_field_value(field)
                    row.append(value)
                rows.append(row)
            
            logger.info(
                f"Query completed: {len(rows)} rows returned, "
                f"{affected_rows} rows affected"
            )
            
            return QueryResult(
                columns=columns,
                rows=rows,
                affected_rows=affected_rows,
                last_insert_id=0  # Data API doesn't provide this directly
            )
            
        except ClientError as e:
            # AWS API error
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            logger.error(
                f"AWS API error executing query: {error_code} - {error_message}"
            )
            
            return QueryResult(
                columns=[],
                rows=[],
                error=e
            )
            
        except BotoCoreError as e:
            # boto3 core error (network, credentials, etc.)
            logger.error(f"boto3 error executing query: {e}")
            
            return QueryResult(
                columns=[],
                rows=[],
                error=e
            )
            
        except Exception as e:
            # Unexpected error
            logger.error(f"Unexpected error executing query: {e}", exc_info=True)
            
            return QueryResult(
                columns=[],
                rows=[],
                error=e
            )
    
    def _extract_field_value(self, field: dict) -> Any:
        """Extract value from a Data API field.
        
        The Data API returns field values in a dictionary with type-specific keys
        (e.g., 'stringValue', 'longValue', 'isNull'). This method extracts the
        actual value.
        
        Args:
            field: Field dictionary from Data API response
            
        Returns:
            Extracted value (str, int, float, bytes, bool, or None)
        """
        # Check for NULL
        if field.get('isNull', False):
            return None
        
        # Extract value based on type
        if 'stringValue' in field:
            return field['stringValue']
        elif 'longValue' in field:
            return field['longValue']
        elif 'doubleValue' in field:
            return field['doubleValue']
        elif 'booleanValue' in field:
            return field['booleanValue']
        elif 'blobValue' in field:
            return field['blobValue']
        elif 'arrayValue' in field:
            # Handle array values (recursive)
            array_values = field['arrayValue'].get('values', [])
            return [self._extract_field_value(v) for v in array_values]
        else:
            # Unknown field type, return None
            logger.warning(f"Unknown field type in Data API response: {field}")
            return None
