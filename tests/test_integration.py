"""Integration tests for MySQL-to-RDS Data API proxy.

These tests verify end-to-end functionality of the proxy server.
"""

import pytest
import socket
import struct
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from mysql_rds_proxy.config import ConfigurationManager
from mysql_rds_proxy.proxy_server import ProxyServer
from mysql_rds_proxy.rds_client import QueryResult, ColumnMetadata


@pytest.fixture
def test_config_file(tmp_path, request):
    """Create a test configuration file."""
    # Use different port for each test to avoid conflicts
    port = 13306 + hash(request.node.name) % 1000
    
    config_content = f"""
proxy:
  listen_host: "127.0.0.1"
  listen_port: {port}

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
    return str(config_file)


@pytest.fixture
def mock_rds_client():
    """Mock RDS client for testing."""
    with patch('mysql_rds_proxy.connection_manager.RDSClient') as mock:
        # Create a mock instance
        instance = Mock()
        
        # Mock execute_query to return a simple result
        instance.execute_query.return_value = QueryResult(
            columns=[
                ColumnMetadata(
                    name='id',
                    type_name='INTEGER',
                    nullable=False,
                    precision=11,
                    scale=0,
                    label='id'
                ),
                ColumnMetadata(
                    name='name',
                    type_name='VARCHAR',
                    nullable=True,
                    precision=255,
                    scale=None,
                    label='name'
                )
            ],
            rows=[
                [1, 'Test'],
                [2, 'Example']
            ],
            affected_rows=0,
            last_insert_id=0,
            error=None
        )
        
        mock.return_value = instance
        yield instance


def test_proxy_server_starts_and_stops(test_config_file, mock_rds_client):
    """Test that proxy server can start and stop."""
    config = ConfigurationManager(test_config_file)
    server = ProxyServer(config)
    
    # Start server in a thread
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    
    # Wait for server to start
    time.sleep(0.5)
    
    # Verify server is listening
    assert server.running
    
    # Stop server
    server.stop()
    
    # Wait for shutdown
    time.sleep(0.5)
    
    assert not server.running


def test_proxy_accepts_connection(test_config_file, mock_rds_client):
    """Test that proxy accepts MySQL client connections."""
    config = ConfigurationManager(test_config_file)
    server = ProxyServer(config)
    port = config.get_listen_port()
    
    # Start server in a thread
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    
    # Wait for server to start
    time.sleep(0.5)
    
    try:
        # Connect to proxy
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(2.0)
        client_socket.connect(('127.0.0.1', port))
        
        # Read handshake packet
        header = client_socket.recv(4)
        assert len(header) == 4
        
        # Parse packet length
        payload_length = struct.unpack('<I', header[:3] + b'\x00')[0]
        
        # Read payload
        payload = client_socket.recv(payload_length)
        assert len(payload) > 0
        
        # Verify protocol version
        assert payload[0] == 10  # Protocol version 10
        
        client_socket.close()
        
    finally:
        server.stop()
        time.sleep(0.2)


def test_proxy_handles_query(test_config_file, mock_rds_client):
    """Test that proxy handles a simple query."""
    config = ConfigurationManager(test_config_file)
    server = ProxyServer(config)
    port = config.get_listen_port()
    
    # Start server in a thread
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    
    # Wait for server to start
    time.sleep(0.5)
    
    try:
        # Connect to proxy
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(5.0)
        client_socket.connect(('127.0.0.1', port))
        
        # Read handshake packet
        header = client_socket.recv(4)
        payload_length = struct.unpack('<I', header[:3] + b'\x00')[0]
        handshake = client_socket.recv(payload_length)
        
        # Send minimal auth response (simplified)
        auth_response = bytearray()
        auth_response.extend(struct.pack('<I', 0x00000200))  # CLIENT_PROTOCOL_41
        auth_response.extend(struct.pack('<I', 16777216))  # max packet size
        auth_response.append(33)  # charset
        auth_response.extend(bytes(23))  # reserved
        auth_response.extend(b'root\x00')  # username
        auth_response.append(0)  # auth response length
        
        # Send auth packet
        auth_header = struct.pack('<I', len(auth_response))[:3] + bytes([1])
        client_socket.sendall(auth_header + auth_response)
        
        # Read OK packet
        ok_header = client_socket.recv(4)
        ok_length = struct.unpack('<I', ok_header[:3] + b'\x00')[0]
        ok_packet = client_socket.recv(ok_length)
        
        # Verify OK packet (starts with 0x00)
        assert ok_packet[0] == 0x00
        
        # Send query packet
        query = b'SELECT * FROM test.users'
        query_packet = bytes([0x03]) + query  # COM_QUERY + query text
        query_header = struct.pack('<I', len(query_packet))[:3] + bytes([0])
        client_socket.sendall(query_header + query_packet)
        
        # Read response (column count packet)
        response_header = client_socket.recv(4)
        response_length = struct.unpack('<I', response_header[:3] + b'\x00')[0]
        response = client_socket.recv(response_length)
        
        # Should receive column count (2 columns in our mock)
        assert len(response) > 0
        
        # Verify RDS client was called with translated query
        mock_rds_client.execute_query.assert_called_once()
        called_query = mock_rds_client.execute_query.call_args[0][0]
        
        # Verify schema mapping was applied
        assert 'test_mapped' in called_query
        
        client_socket.close()
        
    finally:
        server.stop()
        time.sleep(0.2)


def test_configuration_loading(test_config_file):
    """Test that configuration is loaded correctly."""
    config = ConfigurationManager(test_config_file)
    
    assert config.get_listen_host() == "127.0.0.1"
    assert config.get_listen_port() >= 13306  # Port varies by test
    assert config.get_aws_region() == "us-west-2"
    assert config.get_cluster_arn() == "arn:aws:rds:us-west-2:123456789012:cluster:test-cluster"
    assert config.get_secret_arn() == "arn:aws:secretsmanager:us-west-2:123456789012:secret:test-secret"
    assert config.get_schema_mappings() == {"test": "test_mapped"}


def test_query_translation_with_schema_mapping(test_config_file, mock_rds_client):
    """Test that queries are translated with schema mapping."""
    from mysql_rds_proxy.schema_mapper import SchemaMapper
    from mysql_rds_proxy.query_translator import QueryTranslator
    
    config = ConfigurationManager(test_config_file)
    schema_mapper = SchemaMapper(config.get_schema_mappings())
    translator = QueryTranslator(schema_mapper)
    
    # Test schema mapping
    query = "SELECT * FROM test.users"
    translated = translator.translate(query)
    
    assert 'test_mapped' in translated
    assert 'test.users' not in translated or 'test_mapped.users' in translated


def test_query_translation_with_alias_wrapping(test_config_file):
    """Test that queries with aliases are wrapped."""
    from mysql_rds_proxy.schema_mapper import SchemaMapper
    from mysql_rds_proxy.query_translator import QueryTranslator
    
    config = ConfigurationManager(test_config_file)
    schema_mapper = SchemaMapper(config.get_schema_mappings())
    translator = QueryTranslator(schema_mapper)
    
    # Test alias wrapping
    query = "SELECT id AS user_id FROM test.users"
    translated = translator.translate(query)
    
    # Should be wrapped in subquery
    assert 'SELECT * FROM (' in translated
    assert ') AS ' in translated
    assert 'test_mapped' in translated


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
