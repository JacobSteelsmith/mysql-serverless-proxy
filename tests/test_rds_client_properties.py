"""Property-based tests for RDS Data API client.

These tests use Hypothesis to verify universal properties of the RDS client
across a wide range of inputs.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from hypothesis import given, strategies as st, settings

from mysql_rds_proxy.config import ConfigurationManager
from mysql_rds_proxy.rds_client import RDSClient, QueryResult, ColumnMetadata


# Strategy for generating valid AWS ARNs
@st.composite
def aws_arns(draw, service='rds', resource_type='cluster'):
    """Generate valid AWS ARN strings."""
    region = draw(st.sampled_from(['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1']))
    account_id = draw(st.integers(min_value=100000000000, max_value=999999999999))
    resource_name = draw(st.text(
        alphabet=st.characters(whitelist_categories=('Ll', 'Lu', 'Nd'), whitelist_characters='-'),
        min_size=1,
        max_size=20
    ).filter(lambda x: x and not x.startswith('-') and not x.endswith('-')))
    
    if service == 'rds':
        return f"arn:aws:rds:{region}:{account_id}:{resource_type}:{resource_name}"
    elif service == 'secretsmanager':
        secret_id = draw(st.text(
            alphabet=st.characters(whitelist_categories=('Ll', 'Lu', 'Nd'), whitelist_characters='-!'),
            min_size=1,
            max_size=20
        ))
        return f"arn:aws:secretsmanager:{region}:{account_id}:secret:{secret_id}"
    return f"arn:aws:{service}:{region}:{account_id}:{resource_type}:{resource_name}"


# Strategy for generating SQL queries
sql_queries = st.text(min_size=1, max_size=500).filter(lambda x: x.strip())


class TestRDSClientConfigurationUsage:
    """Property 11: RDS Client Configuration Usage
    
    **Validates: Requirements 4.1, 4.5, 12.4**
    
    For any query execution, the RDS client should use the configured cluster ARN,
    secret ARN, and AWS region when calling the Data API.
    """
    
    @settings(max_examples=100)
    @given(
        cluster_arn=aws_arns(service='rds', resource_type='cluster'),
        secret_arn=aws_arns(service='secretsmanager'),
        region=st.sampled_from(['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1']),
        sql=sql_queries
    )
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_rds_client_uses_configured_values(
        self, mock_boto3_client, cluster_arn, secret_arn, region, sql
    ):
        """For any configuration, RDS client should use configured ARNs and region."""
        # Create mock configuration
        mock_config = Mock(spec=ConfigurationManager)
        mock_config.get_cluster_arn.return_value = cluster_arn
        mock_config.get_secret_arn.return_value = secret_arn
        mock_config.get_aws_region.return_value = region
        
        # Create mock boto3 client
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [],
            'records': []
        }
        
        # Create RDS client and execute query
        rds_client = RDSClient(mock_config)
        result = rds_client.execute_query(sql)
        
        # Verify boto3 client was created with correct region
        mock_boto3_client.assert_called_once_with('rds-data', region_name=region)
        
        # Verify execute_statement was called with correct ARNs
        mock_client.execute_statement.assert_called_once()
        call_args = mock_client.execute_statement.call_args[1]
        
        assert call_args['resourceArn'] == cluster_arn
        assert call_args['secretArn'] == secret_arn
        assert call_args['sql'] == sql
        assert call_args['includeResultMetadata'] is True
    
    @settings(max_examples=50)
    @given(
        cluster_arn=aws_arns(service='rds', resource_type='cluster'),
        secret_arn=aws_arns(service='secretsmanager'),
        region=st.sampled_from(['us-east-1', 'us-west-2', 'eu-west-1']),
        database=st.one_of(
            st.none(),
            st.text(min_size=1, max_size=50).filter(lambda x: x.strip())
        )
    )
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_rds_client_uses_database_parameter_when_provided(
        self, mock_boto3_client, cluster_arn, secret_arn, region, database
    ):
        """For any database parameter, RDS client should include it when provided."""
        mock_config = Mock(spec=ConfigurationManager)
        mock_config.get_cluster_arn.return_value = cluster_arn
        mock_config.get_secret_arn.return_value = secret_arn
        mock_config.get_aws_region.return_value = region
        
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [],
            'records': []
        }
        
        rds_client = RDSClient(mock_config)
        result = rds_client.execute_query("SELECT 1", database=database)
        
        call_args = mock_client.execute_statement.call_args[1]
        
        if database:
            assert 'database' in call_args
            assert call_args['database'] == database
        else:
            # When database is None, it should not be included
            assert 'database' not in call_args or call_args.get('database') is None


class TestResultRowRetrieval:
    """Property 12: Result Row Retrieval
    
    **Validates: Requirements 4.3**
    
    For any Data API response containing result rows, all rows should be retrieved
    and included in the query result.
    """
    
    @settings(max_examples=100)
    @given(
        num_rows=st.integers(min_value=0, max_value=100),
        num_cols=st.integers(min_value=1, max_value=10)
    )
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_all_rows_retrieved_from_data_api_response(
        self, mock_boto3_client, num_rows, num_cols
    ):
        """For any number of rows in Data API response, all should be retrieved."""
        # Create mock configuration
        mock_config = Mock(spec=ConfigurationManager)
        mock_config.get_cluster_arn.return_value = "arn:aws:rds:us-west-2:123:cluster:test"
        mock_config.get_secret_arn.return_value = "arn:aws:secretsmanager:us-west-2:123:secret:test"
        mock_config.get_aws_region.return_value = "us-west-2"
        
        # Generate column metadata
        column_metadata = [
            {
                'name': f'col{i}',
                'typeName': 'VARCHAR',
                'nullable': 1
            }
            for i in range(num_cols)
        ]
        
        # Generate records
        records = [
            [{'stringValue': f'row{r}_col{c}'} for c in range(num_cols)]
            for r in range(num_rows)
        ]
        
        # Mock boto3 client
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': column_metadata,
            'records': records
        }
        
        # Execute query
        rds_client = RDSClient(mock_config)
        result = rds_client.execute_query("SELECT * FROM test")
        
        # Verify all rows were retrieved
        assert len(result.rows) == num_rows
        
        # Verify each row has correct number of columns
        for row in result.rows:
            assert len(row) == num_cols
        
        # Verify row content matches
        for r in range(num_rows):
            for c in range(num_cols):
                assert result.rows[r][c] == f'row{r}_col{c}'
    
    @settings(max_examples=50)
    @given(
        row_data=st.lists(
            st.lists(
                st.one_of(
                    st.integers(),
                    st.text(max_size=50),
                    st.floats(allow_nan=False, allow_infinity=False),
                    st.booleans(),
                    st.none()
                ),
                min_size=1,
                max_size=5
            ),
            min_size=0,
            max_size=20
        )
    )
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_all_row_values_preserved(self, mock_boto3_client, row_data):
        """For any row values, all should be preserved in the result."""
        if not row_data:
            return  # Skip empty data
        
        num_cols = len(row_data[0])
        
        # Ensure all rows have same number of columns
        if not all(len(row) == num_cols for row in row_data):
            return
        
        mock_config = Mock(spec=ConfigurationManager)
        mock_config.get_cluster_arn.return_value = "arn:aws:rds:us-west-2:123:cluster:test"
        mock_config.get_secret_arn.return_value = "arn:aws:secretsmanager:us-west-2:123:secret:test"
        mock_config.get_aws_region.return_value = "us-west-2"
        
        # Convert row data to Data API format
        def value_to_field(value):
            if value is None:
                return {'isNull': True}
            elif isinstance(value, bool):
                return {'booleanValue': value}
            elif isinstance(value, int):
                return {'longValue': value}
            elif isinstance(value, float):
                return {'doubleValue': value}
            elif isinstance(value, str):
                return {'stringValue': value}
            else:
                return {'stringValue': str(value)}
        
        records = [
            [value_to_field(val) for val in row]
            for row in row_data
        ]
        
        column_metadata = [
            {'name': f'col{i}', 'typeName': 'VARCHAR', 'nullable': 1}
            for i in range(num_cols)
        ]
        
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': column_metadata,
            'records': records
        }
        
        rds_client = RDSClient(mock_config)
        result = rds_client.execute_query("SELECT * FROM test")
        
        # Verify all values are preserved
        assert len(result.rows) == len(row_data)
        for i, row in enumerate(result.rows):
            assert len(row) == len(row_data[i])
            for j, val in enumerate(row):
                assert val == row_data[i][j]


class TestAWSErrorCapture:
    """Property 13: AWS Error Capture
    
    **Validates: Requirements 4.4**
    
    For any AWS API error, the RDS client should capture the error details
    and make them available for translation.
    """
    
    @settings(max_examples=50)
    @given(
        error_code=st.sampled_from([
            'BadRequestException',
            'StatementTimeoutException',
            'ForbiddenException',
            'ServiceUnavailableException',
            'ThrottlingException',
            'InternalServerErrorException'
        ]),
        error_message=st.text(min_size=1, max_size=200)
    )
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_aws_errors_captured_in_result(
        self, mock_boto3_client, error_code, error_message
    ):
        """For any AWS API error, it should be captured in the QueryResult."""
        from botocore.exceptions import ClientError
        
        mock_config = Mock(spec=ConfigurationManager)
        mock_config.get_cluster_arn.return_value = "arn:aws:rds:us-west-2:123:cluster:test"
        mock_config.get_secret_arn.return_value = "arn:aws:secretsmanager:us-west-2:123:secret:test"
        mock_config.get_aws_region.return_value = "us-west-2"
        
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        # Simulate AWS error
        error_response = {
            'Error': {
                'Code': error_code,
                'Message': error_message
            }
        }
        mock_client.execute_statement.side_effect = ClientError(
            error_response, 'ExecuteStatement'
        )
        
        rds_client = RDSClient(mock_config)
        result = rds_client.execute_query("SELECT 1")
        
        # Verify error was captured
        assert result.error is not None
        assert isinstance(result.error, ClientError)
        
        # Verify result has no data when error occurs
        assert len(result.columns) == 0
        assert len(result.rows) == 0
    
    @settings(max_examples=30)
    @given(
        exception_type=st.sampled_from([
            'BotoCoreError',
            'RuntimeError',
            'ValueError'
        ])
    )
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_non_aws_errors_captured_in_result(
        self, mock_boto3_client, exception_type
    ):
        """For any exception type, it should be captured in the QueryResult."""
        from botocore.exceptions import BotoCoreError
        
        mock_config = Mock(spec=ConfigurationManager)
        mock_config.get_cluster_arn.return_value = "arn:aws:rds:us-west-2:123:cluster:test"
        mock_config.get_secret_arn.return_value = "arn:aws:secretsmanager:us-west-2:123:secret:test"
        mock_config.get_aws_region.return_value = "us-west-2"
        
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        # Simulate different exception types
        if exception_type == 'BotoCoreError':
            mock_client.execute_statement.side_effect = BotoCoreError()
        elif exception_type == 'RuntimeError':
            mock_client.execute_statement.side_effect = RuntimeError("Test error")
        elif exception_type == 'ValueError':
            mock_client.execute_statement.side_effect = ValueError("Test error")
        
        rds_client = RDSClient(mock_config)
        result = rds_client.execute_query("SELECT 1")
        
        # Verify error was captured
        assert result.error is not None
        assert len(result.columns) == 0
        assert len(result.rows) == 0


class TestSecretsManagerIntegration:
    """Property 23: Secrets Manager Integration
    
    **Validates: Requirements 12.2**
    
    For any query execution, the RDS client should use the configured secret ARN
    when accessing database credentials via Secrets Manager.
    """
    
    @settings(max_examples=50)
    @given(
        secret_arn=aws_arns(service='secretsmanager'),
        sql=sql_queries
    )
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_secret_arn_used_in_all_queries(
        self, mock_boto3_client, secret_arn, sql
    ):
        """For any query, the configured secret ARN should be used."""
        mock_config = Mock(spec=ConfigurationManager)
        mock_config.get_cluster_arn.return_value = "arn:aws:rds:us-west-2:123:cluster:test"
        mock_config.get_secret_arn.return_value = secret_arn
        mock_config.get_aws_region.return_value = "us-west-2"
        
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [],
            'records': []
        }
        
        rds_client = RDSClient(mock_config)
        result = rds_client.execute_query(sql)
        
        # Verify secret ARN was used
        call_args = mock_client.execute_statement.call_args[1]
        assert call_args['secretArn'] == secret_arn
    
    @settings(max_examples=30)
    @given(
        secret_arn=aws_arns(service='secretsmanager'),
        num_queries=st.integers(min_value=1, max_value=10)
    )
    @patch('mysql_rds_proxy.rds_client.boto3.client')
    def test_secret_arn_consistent_across_multiple_queries(
        self, mock_boto3_client, secret_arn, num_queries
    ):
        """For multiple queries, the same secret ARN should be used consistently."""
        mock_config = Mock(spec=ConfigurationManager)
        mock_config.get_cluster_arn.return_value = "arn:aws:rds:us-west-2:123:cluster:test"
        mock_config.get_secret_arn.return_value = secret_arn
        mock_config.get_aws_region.return_value = "us-west-2"
        
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.execute_statement.return_value = {
            'numberOfRecordsUpdated': 0,
            'columnMetadata': [],
            'records': []
        }
        
        rds_client = RDSClient(mock_config)
        
        # Execute multiple queries
        for i in range(num_queries):
            result = rds_client.execute_query(f"SELECT {i}")
        
        # Verify secret ARN was used in all calls
        assert mock_client.execute_statement.call_count == num_queries
        
        for call in mock_client.execute_statement.call_args_list:
            call_args = call[1]
            assert call_args['secretArn'] == secret_arn
