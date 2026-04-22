"""Tests for USE database command support."""

import pytest
from unittest.mock import Mock

from mysql_rds_proxy.connection_manager import ConnectionManager, ConnectionContext
from mysql_rds_proxy.protocol_handler import MySQLProtocolHandler
from mysql_rds_proxy.config import ConfigurationManager


def test_use_database_command(tmp_path):
    """Test that USE database command sets the current database."""
    # Create test config
    config_content = """
proxy:
  listen_host: "127.0.0.1"
  listen_port: 13306

aws:
  region: "us-west-2"
  cluster_arn: "arn:aws:rds:us-west-2:123456789012:cluster:test-cluster"
  secret_arn: "arn:aws:secretsmanager:us-west-2:123456789012:secret:test-secret"

schema_mappings:
  test: "test_mapped"

logging:
  level: "INFO"
"""
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    
    # Create config and connection manager
    config = ConfigurationManager(str(config_file))
    manager = ConnectionManager(config)
    
    # Create mock socket
    mock_socket = Mock()
    
    # Create connection context
    context = manager.create_connection(mock_socket)
    
    # Initially, no database should be set
    assert context.current_database is None
    
    # Create protocol handler
    protocol = MySQLProtocolHandler(mock_socket, 1)
    
    # Simulate USE database command
    use_db_query = "__USE_DATABASE__:mydb_jacobs"
    
    # Route the query
    manager._route_query(context, protocol, use_db_query)
    
    # Verify database was set
    assert context.current_database == "mydb_jacobs"


def test_parse_use_database_packet():
    """Test that COM_INIT_DB packets are parsed correctly."""
    mock_socket = Mock()
    protocol = MySQLProtocolHandler(mock_socket, 1)
    
    # Create a COM_INIT_DB packet (command byte 0x02 + database name)
    database_name = "mydb_jacobs"
    packet = bytes([0x02]) + database_name.encode('utf-8')
    
    # Parse the packet
    result = protocol.parse_query_packet(packet)
    
    # Should return the special USE database marker
    assert result == f"__USE_DATABASE__:{database_name}"


def test_database_used_in_query_execution(tmp_path, monkeypatch):
    """Test that the current database is passed to RDS client."""
    from unittest.mock import patch, MagicMock
    
    # Create test config
    config_content = """
proxy:
  listen_host: "127.0.0.1"
  listen_port: 13306

aws:
  region: "us-west-2"
  cluster_arn: "arn:aws:rds:us-west-2:123456789012:cluster:test-cluster"
  secret_arn: "arn:aws:secretsmanager:us-west-2:123456789012:secret:test-secret"

schema_mappings:
  test: "test_mapped"

logging:
  level: "INFO"
"""
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    
    # Create config and connection manager
    config = ConfigurationManager(str(config_file))
    
    # Mock RDSClient
    with patch('mysql_rds_proxy.connection_manager.RDSClient') as mock_rds_class:
        mock_rds_instance = MagicMock()
        mock_rds_class.return_value = mock_rds_instance
        
        # Mock execute_query to return a simple result
        from mysql_rds_proxy.rds_client import QueryResult
        mock_rds_instance.execute_query.return_value = QueryResult(
            columns=[],
            rows=[],
            affected_rows=1,
            last_insert_id=0,
            error=None
        )
        
        manager = ConnectionManager(config)
        
        # Create mock socket
        mock_socket = Mock()
        
        # Create connection context
        context = manager.create_connection(mock_socket)
        
        # Set current database
        context.current_database = "mydb_jacobs"
        
        # Create protocol handler
        protocol = MySQLProtocolHandler(mock_socket, 1)
        
        # Execute a query
        manager._route_query(context, protocol, "SELECT * FROM users")
        
        # Verify execute_query was called with the database parameter
        mock_rds_instance.execute_query.assert_called_once()
        call_args = mock_rds_instance.execute_query.call_args
        
        # Check that database parameter was passed
        assert call_args[1]['database'] == "mydb_jacobs"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
