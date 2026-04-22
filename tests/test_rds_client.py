"""Unit tests for RDS Data API client.

Tests the RDSClient class with mocked boto3 responses to verify query execution,
result retrieval, and error handling.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError, BotoCoreError

from mysql_rds_proxy.config import ConfigurationManager
from mysql_rds_proxy.rds_client import RDSClient, QueryResult, ColumnMetadata


@pytest.fixture
def mock_config():
    """Create a mock configuration manager."""
    config = Mock(spec=ConfigurationManager)
    config.get_cluster_arn.return_value = "arn:aws:rds:us-west-2:123456789012:cluster:test-cluster"
    config.get_secret_arn.return_value = "arn:aws:secretsmanager:us-west-2:123456789012:secret:test-secret"
    config.get_aws_region.return_value = "us-west-2"
    return config


@pytest.fixture
def rds_client(mock_config):
    """Create an RDS client with mock configuration."""
    return RDSClient(mock_config)


class TestRDSClientInitialization:
    """Tests for RDS client initialization."""
    
    def test_initialization_with_config(self, mock_config):
        """Test that RDS client initializes with configuration."""
        client = RDSClient(mock_config)
        
        assert client._cluster_arn == "arn:aws:rds:us-west-2:123456789012:cluster:test-cluster"
        assert client._secret_arn == "arn:aws:secretsmanager:us-west-2:123456789012:secret:test-secret"
        assert client._region == "us-west-2"
        assert client._client is None  # Not created until first use


class TestQueryExecution:
    """Tests for query execution."""
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_successful_select_query(self, mock_boto3_client, rds_client):
        """Test successful execution of a SELECT query."""
        # Mock boto3 client response
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [
                {
                    'name': 'id',
                    'typeName': 'INTEGER',
                    'nullable': 0,
                    'precision': 10,
                    'scale': 0
                },
                {
                    'name': 'name',
                    'typeName': 'VARCHAR',
                    'nullable': 1,
                    'label': 'name'
                }
            ],
            'records': [
                [
                    {'longValue': 1},
                    {'stringValue': 'Alice'}
                ],
                [
                    {'longValue': 2},
                    {'stringValue': 'Bob'}
                ]
            ]
        }
        
        # Execute query
        result = rds_client.execute_query("SELECT id, name FROM users")
        
        # Verify boto3 client was called correctly
        mock_boto3_client.assert_called_once_with('rds-data', region_name='us-west-2')
        mock_client.execute_statement.assert_called_once_with(
            resourceArn="arn:aws:rds:us-west-2:123456789012:cluster:test-cluster",
            secretArn="arn:aws:secretsmanager:us-west-2:123456789012:secret:test-secret",
            sql="SELECT id, name FROM users",
            includeResultMetadata=True
        )
        
        # Verify result
        assert result.error is None
        assert len(result.columns) == 2
        assert result.columns[0].name == 'id'
        assert result.columns[0].type_name == 'INTEGER'
        assert result.columns[0].nullable is False
        assert result.columns[1].name == 'name'
        assert result.columns[1].type_name == 'VARCHAR'
        assert result.columns[1].nullable is True
        
        assert len(result.rows) == 2
        assert result.rows[0] == [1, 'Alice']
        assert result.rows[1] == [2, 'Bob']
        assert result.affected_rows == 0
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_successful_insert_query(self, mock_boto3_client, rds_client):
        """Test successful execution of an INSERT query."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 1,
            'columnMetadata': [],
            'records': []
        }
        
        result = rds_client.execute_query("INSERT INTO users (name) VALUES ('Charlie')")
        
        assert result.error is None
        assert len(result.columns) == 0
        assert len(result.rows) == 0
        assert result.affected_rows == 1
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_query_with_database_parameter(self, mock_boto3_client, rds_client):
        """Test query execution with database parameter."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [],
            'records': []
        }
        
        result = rds_client.execute_query("SELECT 1", database="test_db")
        
        # Verify database parameter was included
        mock_client.execute_statement.assert_called_once()
        call_args = mock_client.execute_statement.call_args[1]
        assert call_args['database'] == 'test_db'
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_query_with_null_values(self, mock_boto3_client, rds_client):
        """Test query execution with NULL values in results."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [
                {'name': 'id', 'typeName': 'INTEGER', 'nullable': 0},
                {'name': 'email', 'typeName': 'VARCHAR', 'nullable': 1}
            ],
            'records': [
                [
                    {'longValue': 1},
                    {'isNull': True}
                ],
                [
                    {'longValue': 2},
                    {'stringValue': 'test@example.com'}
                ]
            ]
        }
        
        result = rds_client.execute_query("SELECT id, email FROM users")
        
        assert result.error is None
        assert len(result.rows) == 2
        assert result.rows[0] == [1, None]
        assert result.rows[1] == [2, 'test@example.com']
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_query_with_various_data_types(self, mock_boto3_client, rds_client):
        """Test query execution with various data types."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [
                {'name': 'str_col', 'typeName': 'VARCHAR', 'nullable': 1},
                {'name': 'int_col', 'typeName': 'INTEGER', 'nullable': 1},
                {'name': 'float_col', 'typeName': 'DOUBLE', 'nullable': 1},
                {'name': 'bool_col', 'typeName': 'BOOLEAN', 'nullable': 1},
                {'name': 'blob_col', 'typeName': 'BLOB', 'nullable': 1}
            ],
            'records': [
                [
                    {'stringValue': 'test'},
                    {'longValue': 42},
                    {'doubleValue': 3.14},
                    {'booleanValue': True},
                    {'blobValue': b'binary data'}
                ]
            ]
        }
        
        result = rds_client.execute_query("SELECT * FROM test_table")
        
        assert result.error is None
        assert len(result.rows) == 1
        assert result.rows[0] == ['test', 42, 3.14, True, b'binary data']
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_empty_result_set(self, mock_boto3_client, rds_client):
        """Test query execution with empty result set."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [
                {'name': 'id', 'typeName': 'INTEGER', 'nullable': 0}
            ],
            'records': []
        }
        
        result = rds_client.execute_query("SELECT id FROM users WHERE id = 999")
        
        assert result.error is None
        assert len(result.columns) == 1
        assert len(result.rows) == 0


class TestErrorHandling:
    """Tests for error handling."""
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_aws_authentication_error(self, mock_boto3_client, rds_client):
        """Test handling of AWS authentication errors."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        # Simulate authentication error
        error_response = {
            'Error': {
                'Code': 'InvalidSignatureException',
                'Message': 'The request signature we calculated does not match'
            }
        }
        mock_client.execute_statement.side_effect = ClientError(
            error_response, 'ExecuteStatement'
        )
        
        result = rds_client.execute_query("SELECT 1")
        
        assert result.error is not None
        assert isinstance(result.error, ClientError)
        assert len(result.columns) == 0
        assert len(result.rows) == 0
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_aws_throttling_error(self, mock_boto3_client, rds_client):
        """Test handling of AWS throttling errors."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        error_response = {
            'Error': {
                'Code': 'ThrottlingException',
                'Message': 'Rate exceeded'
            }
        }
        mock_client.execute_statement.side_effect = ClientError(
            error_response, 'ExecuteStatement'
        )
        
        result = rds_client.execute_query("SELECT 1")
        
        assert result.error is not None
        assert isinstance(result.error, ClientError)
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_aws_service_unavailable_error(self, mock_boto3_client, rds_client):
        """Test handling of AWS service unavailable errors."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        error_response = {
            'Error': {
                'Code': 'ServiceUnavailableException',
                'Message': 'Service is temporarily unavailable'
            }
        }
        mock_client.execute_statement.side_effect = ClientError(
            error_response, 'ExecuteStatement'
        )
        
        result = rds_client.execute_query("SELECT 1")
        
        assert result.error is not None
        assert isinstance(result.error, ClientError)
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_botocore_error(self, mock_boto3_client, rds_client):
        """Test handling of botocore errors."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.side_effect = BotoCoreError()
        
        result = rds_client.execute_query("SELECT 1")
        
        assert result.error is not None
        assert isinstance(result.error, BotoCoreError)
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_unexpected_error(self, mock_boto3_client, rds_client):
        """Test handling of unexpected errors."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.side_effect = RuntimeError("Unexpected error")
        
        result = rds_client.execute_query("SELECT 1")
        
        assert result.error is not None
        assert isinstance(result.error, RuntimeError)


class TestBoto3ClientReuse:
    """Tests for boto3 client reuse."""
    
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_client_reuse_across_queries(self, mock_boto3_client, rds_client):
        """Test that boto3 client is reused across multiple queries."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [],
            'records': []
        }
        
        # Execute multiple queries
        rds_client.execute_query("SELECT 1")
        rds_client.execute_query("SELECT 2")
        rds_client.execute_query("SELECT 3")
        
        # Verify boto3.client was only called once
        assert mock_boto3_client.call_count == 1
        
        # Verify execute_statement was called three times
        assert mock_client.execute_statement.call_count == 3


class TestFieldValueExtraction:
    """Tests for field value extraction."""
    
    def test_extract_string_value(self, rds_client):
        """Test extraction of string values."""
        field = {'stringValue': 'test'}
        value = rds_client._extract_field_value(field)
        assert value == 'test'
    
    def test_extract_long_value(self, rds_client):
        """Test extraction of long values."""
        field = {'longValue': 42}
        value = rds_client._extract_field_value(field)
        assert value == 42
    
    def test_extract_double_value(self, rds_client):
        """Test extraction of double values."""
        field = {'doubleValue': 3.14}
        value = rds_client._extract_field_value(field)
        assert value == 3.14
    
    def test_extract_boolean_value(self, rds_client):
        """Test extraction of boolean values."""
        field = {'booleanValue': True}
        value = rds_client._extract_field_value(field)
        assert value is True
    
    def test_extract_blob_value(self, rds_client):
        """Test extraction of blob values."""
        field = {'blobValue': b'binary'}
        value = rds_client._extract_field_value(field)
        assert value == b'binary'
    
    def test_extract_null_value(self, rds_client):
        """Test extraction of NULL values."""
        field = {'isNull': True}
        value = rds_client._extract_field_value(field)
        assert value is None
    
    def test_extract_array_value(self, rds_client):
        """Test extraction of array values."""
        field = {
            'arrayValue': {
                'values': [
                    {'stringValue': 'a'},
                    {'stringValue': 'b'},
                    {'stringValue': 'c'}
                ]
            }
        }
        value = rds_client._extract_field_value(field)
        assert value == ['a', 'b', 'c']
    
    def test_extract_unknown_field_type(self, rds_client):
        """Test extraction of unknown field types."""
        field = {'unknownType': 'value'}
        value = rds_client._extract_field_value(field)
        assert value is None
