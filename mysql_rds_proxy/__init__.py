"""MySQL-to-RDS Data API Translation Proxy.

A local proxy server that translates MySQL protocol connections into AWS RDS Data API calls,
enabling standard MySQL clients to connect to serverless RDS clusters.
"""

from mysql_rds_proxy.config import ConfigurationManager, ProxyConfig, ConfigurationError
from mysql_rds_proxy.schema_mapper import SchemaMapper
from mysql_rds_proxy.query_translator import QueryTranslator
from mysql_rds_proxy.rds_client import RDSClient, QueryResult, ColumnMetadata
from mysql_rds_proxy.response_translator import ResponseTranslator, ColumnDef
from mysql_rds_proxy.protocol_handler import MySQLProtocolHandler
from mysql_rds_proxy.connection_manager import ConnectionManager, ConnectionContext
from mysql_rds_proxy.proxy_server import ProxyServer

__version__ = "0.1.0"

__all__ = [
    'ConfigurationManager',
    'ProxyConfig',
    'ConfigurationError',
    'SchemaMapper',
    'QueryTranslator',
    'RDSClient',
    'QueryResult',
    'ColumnMetadata',
    'ResponseTranslator',
    'ColumnDef',
    'MySQLProtocolHandler',
    'ConnectionManager',
    'ConnectionContext',
    'ProxyServer',
]
