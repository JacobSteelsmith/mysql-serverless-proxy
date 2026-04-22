# Design Document: MySQL-to-RDS Data API Translation Proxy

## Overview

The MySQL-to-RDS Data API translation proxy is a Python-based local server that bridges the gap between standard MySQL protocol clients and AWS RDS serverless clusters that only expose the Data API. The proxy accepts MySQL wire protocol connections on a local port, translates incoming queries to be compatible with the RDS Data API, executes them via boto3, and translates responses back to MySQL protocol format.

This design enables developers to use familiar tools like MySQL Workbench and applications like Lucee to interact with serverless RDS clusters without requiring VPC access or traditional MySQL endpoint connectivity.

### Key Design Principles

1. **Protocol Fidelity**: Implement enough of the MySQL wire protocol to support common client tools
2. **Transparent Translation**: Query transformations should be invisible to the client
3. **Configuration-Driven**: All environment-specific settings externalized to configuration
4. **Error Preservation**: Maintain meaningful error messages through the translation layers
5. **Concurrent Support**: Handle multiple simultaneous client connections independently

## Architecture

The system follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                     MySQL Clients                            │
│            (Workbench, Lucee, etc.)                         │
└─────────────────────┬───────────────────────────────────────┘
                      │ MySQL Protocol
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                  Proxy Server                                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Connection Manager                            │  │
│  │  (handles multiple concurrent connections)            │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                        │
│  ┌──────────────────▼───────────────────────────────────┐  │
│  │      MySQL Protocol Handler                           │  │
│  │  (handshake, packet parsing, result formatting)       │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                        │
│  ┌──────────────────▼───────────────────────────────────┐  │
│  │         Query Translator                              │  │
│  │  ┌────────────────────────────────────────────────┐  │  │
│  │  │  Schema Mapper                                  │  │  │
│  │  │  (replaces schema names)                        │  │  │
│  │  └────────────────────────────────────────────────┘  │  │
│  │  ┌────────────────────────────────────────────────┐  │  │
│  │  │  Alias Wrapper                                  │  │  │
│  │  │  (wraps queries with aliases)                   │  │  │
│  │  └────────────────────────────────────────────────┘  │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                        │
│  ┌──────────────────▼───────────────────────────────────┐  │
│  │         RDS Client                                    │  │
│  │  (boto3 Data API calls)                              │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     │                                        │
│  ┌──────────────────▼───────────────────────────────────┐  │
│  │      Response Translator                              │  │
│  │  (Data API → MySQL protocol)                         │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │      Configuration Manager                            │  │
│  │  (loads settings from config file)                    │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                      │ HTTPS (boto3)
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                  AWS Services                                │
│  ┌────────────────────────┐  ┌──────────────────────────┐  │
│  │  RDS Data API          │  │  Secrets Manager         │  │
│  └────────────────────────┘  └──────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### Component Interaction Flow

1. **Connection Establishment**: Client connects → Connection Manager creates session → MySQL Protocol Handler performs handshake
2. **Query Execution**: Client sends query → Protocol Handler parses → Query Translator transforms → RDS Client executes → Response Translator converts → Protocol Handler sends results
3. **Error Handling**: Error at any layer → Translated to MySQL error → Sent to client

## Components and Interfaces

### 1. Configuration Manager

**Responsibility**: Load and provide access to configuration settings.

**Configuration File Format** (YAML):
```yaml
proxy:
  listen_port: 3306
  listen_host: "127.0.0.1"

aws:
  region: "us-east-1"
  cluster_arn: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster"
  secret_arn: "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret"

schema_mappings:
  mydb: "mydb_jacobs"
  test: "test_production"

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

**Interface**:
```python
class ConfigurationManager:
    def __init__(self, config_path: str):
        """Load configuration from file."""
        pass
    
    def get_listen_port(self) -> int:
        """Return the port to listen on."""
        pass
    
    def get_listen_host(self) -> str:
        """Return the host address to bind to."""
        pass
    
    def get_aws_region(self) -> str:
        """Return the AWS region."""
        pass
    
    def get_cluster_arn(self) -> str:
        """Return the RDS cluster ARN."""
        pass
    
    def get_secret_arn(self) -> str:
        """Return the Secrets Manager secret ARN."""
        pass
    
    def get_schema_mappings(self) -> dict[str, str]:
        """Return schema name mappings."""
        pass
```

### 2. Proxy Server

**Responsibility**: Main entry point, manages server lifecycle and accepts connections.

**Interface**:
```python
class ProxyServer:
    def __init__(self, config: ConfigurationManager):
        """Initialize proxy server with configuration."""
        pass
    
    def start(self) -> None:
        """Start listening for connections."""
        pass
    
    def stop(self) -> None:
        """Gracefully shut down the server."""
        pass
    
    def _accept_connection(self, client_socket: socket.socket) -> None:
        """Handle a new client connection."""
        pass
```

### 3. Connection Manager

**Responsibility**: Manage individual client connections and their lifecycle.

**Interface**:
```python
class ConnectionContext:
    """Holds state for a single client connection."""
    connection_id: str
    client_socket: socket.socket
    authenticated: bool
    current_database: str | None

class ConnectionManager:
    def __init__(self, config: ConfigurationManager):
        """Initialize connection manager."""
        pass
    
    def create_connection(self, client_socket: socket.socket) -> ConnectionContext:
        """Create a new connection context."""
        pass
    
    def handle_connection(self, context: ConnectionContext) -> None:
        """Main loop for handling a client connection."""
        pass
    
    def close_connection(self, context: ConnectionContext) -> None:
        """Clean up connection resources."""
        pass
```

### 4. MySQL Protocol Handler

**Responsibility**: Implement MySQL wire protocol for handshake, packet parsing, and result formatting.

**Key Protocol Elements**:
- Handshake: Server greeting, authentication challenge/response
- Command packets: COM_QUERY (0x03) for SQL queries
- Result packets: Column definitions, row data, EOF markers
- Error packets: Error code, SQL state, error message

**Interface**:
```python
class MySQLProtocolHandler:
    def __init__(self, context: ConnectionContext):
        """Initialize protocol handler for a connection."""
        pass
    
    def perform_handshake(self) -> bool:
        """Execute MySQL handshake sequence. Returns True if successful."""
        pass
    
    def read_packet(self) -> bytes:
        """Read a MySQL packet from the client."""
        pass
    
    def parse_query_packet(self, packet: bytes) -> str:
        """Extract SQL query from COM_QUERY packet."""
        pass
    
    def send_result_set(self, columns: list[ColumnDef], rows: list[list[Any]]) -> None:
        """Send a result set to the client."""
        pass
    
    def send_ok_packet(self, affected_rows: int, last_insert_id: int = 0) -> None:
        """Send an OK packet for successful non-SELECT queries."""
        pass
    
    def send_error_packet(self, error_code: int, sql_state: str, message: str) -> None:
        """Send an error packet to the client."""
        pass

class ColumnDef:
    """MySQL column definition."""
    name: str
    type_code: int
    flags: int
```

**MySQL Protocol Subset**:
We implement a minimal subset sufficient for query execution:
- Handshake v10 protocol
- Authentication (simplified - accept any credentials)
- COM_QUERY command
- Result set protocol (column defs + rows)
- OK and Error packets

### 5. Query Translator

**Responsibility**: Transform MySQL queries for RDS Data API compatibility.

**Interface**:
```python
class QueryTranslator:
    def __init__(self, schema_mapper: SchemaMapper):
        """Initialize query translator."""
        pass
    
    def translate(self, query: str) -> str:
        """Apply all transformations to the query."""
        pass
    
    def _needs_alias_wrapping(self, query: str) -> bool:
        """Detect if query contains column aliases."""
        pass
    
    def _wrap_with_subquery(self, query: str) -> str:
        """Wrap query in a subquery with random alias."""
        pass
    
    def _generate_random_alias(self) -> str:
        """Generate a random alphanumeric alias (e.g., 'x5rhy')."""
        pass
```

### 6. Schema Mapper

**Responsibility**: Replace schema names in queries according to configuration.

**Interface**:
```python
class SchemaMapper:
    def __init__(self, mappings: dict[str, str]):
        """Initialize with schema name mappings."""
        pass
    
    def map_schema_names(self, query: str) -> str:
        """Replace schema names in the query."""
        pass
    
    def _find_schema_references(self, query: str) -> list[tuple[int, int, str]]:
        """Find all schema references with their positions."""
        pass
```

**Implementation Approach**:
Use regular expressions to find schema references in common SQL contexts:
- `FROM schema.table`
- `JOIN schema.table`
- `INSERT INTO schema.table`
- `UPDATE schema.table`
- `DELETE FROM schema.table`

Pattern: `\b(schema_name)\s*\.\s*(\w+)`

### 7. RDS Client

**Responsibility**: Execute queries against AWS RDS Data API using boto3.

**Interface**:
```python
class RDSClient:
    def __init__(self, config: ConfigurationManager):
        """Initialize RDS client with AWS configuration."""
        pass
    
    def execute_query(self, sql: str) -> QueryResult:
        """Execute SQL query via Data API."""
        pass
    
    def _get_boto3_client(self) -> Any:
        """Get or create boto3 RDS Data API client."""
        pass

class QueryResult:
    """Result from RDS Data API execution."""
    columns: list[ColumnMetadata]
    rows: list[list[Any]]
    affected_rows: int
    error: Exception | None

class ColumnMetadata:
    """Column metadata from Data API."""
    name: str
    type_name: str
    nullable: bool
```

**boto3 Data API Usage**:
```python
response = client.execute_statement(
    resourceArn=cluster_arn,
    secretArn=secret_arn,
    sql=translated_query,
    database=database_name,
    includeResultMetadata=True
)
```

### 8. Response Translator

**Responsibility**: Convert RDS Data API responses to MySQL protocol format.

**Interface**:
```python
class ResponseTranslator:
    def translate_result_set(self, result: QueryResult) -> tuple[list[ColumnDef], list[list[Any]]]:
        """Convert Data API result to MySQL format."""
        pass
    
    def _map_data_type(self, data_api_type: str) -> int:
        """Map Data API type to MySQL type code."""
        pass
    
    def _convert_value(self, value: Any, data_api_type: str) -> Any:
        """Convert Data API value to MySQL-compatible format."""
        pass
    
    def translate_error(self, error: Exception) -> tuple[int, str, str]:
        """Convert AWS error to MySQL error code, SQL state, and message."""
        pass
```

**Type Mapping** (Data API → MySQL):
- `VARCHAR` → `MYSQL_TYPE_VAR_STRING` (253)
- `INTEGER` → `MYSQL_TYPE_LONG` (3)
- `BIGINT` → `MYSQL_TYPE_LONGLONG` (8)
- `DECIMAL` → `MYSQL_TYPE_DECIMAL` (0)
- `TIMESTAMP` → `MYSQL_TYPE_TIMESTAMP` (7)
- `BLOB` → `MYSQL_TYPE_BLOB` (252)
- `NULL` → Special NULL marker in MySQL protocol

## Data Models

### Configuration Data

```python
@dataclass
class ProxyConfig:
    """Complete proxy configuration."""
    listen_host: str
    listen_port: int
    aws_region: str
    cluster_arn: str
    secret_arn: str
    schema_mappings: dict[str, str]
    log_level: str
```

### Connection State

```python
@dataclass
class ConnectionContext:
    """State for a single client connection."""
    connection_id: str
    client_socket: socket.socket
    authenticated: bool
    current_database: str | None
    sequence_id: int  # MySQL packet sequence number
```

### Query Execution Pipeline

```python
@dataclass
class QueryRequest:
    """Query to be executed."""
    original_sql: str
    translated_sql: str
    connection_id: str

@dataclass
class QueryResult:
    """Result from query execution."""
    columns: list[ColumnMetadata]
    rows: list[list[Any]]
    affected_rows: int
    last_insert_id: int
    error: Exception | None
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*


### Property 1: Server Port Binding

*For any* valid port number, when the proxy server starts with that port configuration, attempting to connect to that port should succeed.

**Validates: Requirements 1.1**

### Property 2: MySQL Handshake Completion

*For any* client connection attempt, the MySQL protocol handshake sequence should complete successfully and establish a session.

**Validates: Requirements 1.2, 2.1**

### Property 3: Connection Isolation

*For any* set of concurrent client connections, queries sent on one connection should not affect or interfere with queries on other connections.

**Validates: Requirements 1.3, 10.5**

### Property 4: Query Packet Round-Trip

*For any* SQL query string, encoding it as a MySQL COM_QUERY packet and then parsing it should produce the original query string.

**Validates: Requirements 2.2**

### Property 5: Result Set Packet Validity

*For any* result set (columns and rows), formatting it as MySQL result set packets should produce valid packets that can be parsed by MySQL clients.

**Validates: Requirements 2.3**

### Property 6: Error Packet Formatting

*For any* error (code, SQL state, message), formatting it as a MySQL error packet should produce a valid error packet with all components preserved.

**Validates: Requirements 2.4, 9.5**

### Property 7: Schema Name Replacement

*For any* SQL query containing schema names that match configured mappings, all occurrences of those schema names should be replaced with their mapped values, regardless of SQL context (FROM, JOIN, INSERT, UPDATE, DELETE).

**Validates: Requirements 3.2, 7.1, 7.2, 7.3, 7.5**

### Property 8: Alias Detection

*For any* SQL query, the alias detection should correctly identify whether the query contains column aliases (AS keyword).

**Validates: Requirements 8.1**

### Property 9: Alias Wrapping Format

*For any* SQL query containing column aliases, wrapping it should produce a query in the format "SELECT * FROM (original_query) AS random_alias" where the random alias is alphanumeric.

**Validates: Requirements 3.3, 8.2, 8.3, 8.4, 8.5**

### Property 10: Random Alias Generation

*For any* generated subquery alias, it should consist only of alphanumeric characters.

**Validates: Requirements 3.4, 8.4**

### Property 11: RDS Client Configuration Usage

*For any* query execution, the RDS client should use the configured cluster ARN, secret ARN, and AWS region when calling the Data API.

**Validates: Requirements 4.1, 4.5, 12.4**

### Property 12: Result Row Retrieval

*For any* Data API response containing result rows, all rows should be retrieved and included in the query result.

**Validates: Requirements 4.3**

### Property 13: AWS Error Capture

*For any* AWS API error, the RDS client should capture the error details and make them available for translation.

**Validates: Requirements 4.4**

### Property 14: Data Type Mapping

*For any* RDS Data API column type, the response translator should map it to a valid MySQL column type code.

**Validates: Requirements 5.2**

### Property 15: Result Set Translation

*For any* RDS Data API result set, translating it should produce MySQL-compatible column definitions and row data.

**Validates: Requirements 5.1**

### Property 16: Affected Row Count Preservation

*For any* INSERT/UPDATE/DELETE query result, the affected row count from the Data API should be preserved in the MySQL response.

**Validates: Requirements 5.4**

### Property 17: Error Translation

*For any* AWS API error, translating it should produce a valid MySQL error code, SQL state, and error message.

**Validates: Requirements 5.5**

### Property 18: Configuration Loading

*For any* valid configuration file, loading it should correctly populate all configuration fields (cluster ARN, secret ARN, region, port, schema mappings).

**Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6**

### Property 19: Invalid Configuration Handling

*For any* invalid or malformed configuration file, the configuration manager should raise a clear error indicating what is wrong.

**Validates: Requirements 6.7**

### Property 20: SQL Statement Type Processing

*For any* SQL statement type (SELECT, INSERT, UPDATE, DELETE, DDL), the query translator should process it and produce appropriate output (result set for SELECT, affected rows for DML, success for DDL).

**Validates: Requirements 11.1, 11.2, 11.3, 11.4, 11.5**

### Property 21: Connection Context Creation

*For any* new client connection, the connection manager should create a unique connection context with a unique connection ID.

**Validates: Requirements 10.1**

### Property 22: Query Routing

*For any* query received on a connection, the connection manager should route it to the query translator and RDS client for execution.

**Validates: Requirements 10.3**

### Property 23: Secrets Manager Integration

*For any* query execution, the RDS client should use the configured secret ARN when accessing database credentials via Secrets Manager.

**Validates: Requirements 12.2**

## Error Handling

### Error Categories

1. **MySQL Protocol Errors**
   - Invalid packet format
   - Unsupported commands
   - Authentication failures
   - Response: MySQL error packet with appropriate error code

2. **Query Translation Errors**
   - Malformed SQL
   - Unsupported SQL features
   - Response: MySQL error packet with syntax error code (1064)

3. **AWS API Errors**
   - Authentication failures (credentials invalid/expired)
   - Authorization failures (insufficient permissions)
   - Throttling (rate limit exceeded)
   - Service errors (RDS Data API unavailable)
   - Response: Translated to MySQL error packets with descriptive messages

4. **Configuration Errors**
   - Missing configuration file
   - Invalid YAML syntax
   - Missing required fields
   - Invalid ARN formats
   - Response: Fail fast at startup with clear error message

5. **Network Errors**
   - Client disconnection during query
   - AWS API timeout
   - Response: Log error, clean up connection resources

### Error Translation Strategy

AWS errors are mapped to MySQL error codes:

| AWS Error Type | MySQL Error Code | SQL State |
|----------------|------------------|-----------|
| BadRequestException | 1064 | 42000 |
| StatementTimeoutException | 1205 | HY000 |
| ForbiddenException | 1045 | 28000 |
| ServiceUnavailableException | 2013 | HY000 |
| ThrottlingException | 1040 | HY000 |
| Generic errors | 1105 | HY000 |

### Error Context

All errors include:
- Connection ID
- Original query (if applicable)
- Timestamp
- Stack trace (in logs, not sent to client)

## Testing Strategy

### Dual Testing Approach

This feature requires both unit tests and property-based tests for comprehensive coverage:

- **Unit tests**: Verify specific examples, edge cases, and error conditions
- **Property tests**: Verify universal properties across all inputs

Both approaches are complementary and necessary. Unit tests catch concrete bugs in specific scenarios, while property tests verify general correctness across a wide range of inputs.

### Property-Based Testing

We will use **Hypothesis** (Python's property-based testing library) to implement the correctness properties defined above.

**Configuration**:
- Minimum 100 iterations per property test (due to randomization)
- Each property test must reference its design document property
- Tag format: `# Feature: mysql-rds-data-api-proxy, Property {number}: {property_text}`

**Example Property Test Structure**:
```python
from hypothesis import given, strategies as st

# Feature: mysql-rds-data-api-proxy, Property 7: Schema Name Replacement
@given(
    query=st.text(min_size=10),
    schema_mappings=st.dictionaries(
        keys=st.text(min_size=1, max_size=20),
        values=st.text(min_size=1, max_size=20)
    )
)
def test_schema_name_replacement(query, schema_mappings):
    """For any SQL query containing schema names that match configured mappings,
    all occurrences should be replaced with their mapped values."""
    mapper = SchemaMapper(schema_mappings)
    result = mapper.map_schema_names(query)
    
    # Verify all mapped schema names are replaced
    for old_schema, new_schema in schema_mappings.items():
        if old_schema in query:
            assert new_schema in result
            assert old_schema not in result or old_schema == new_schema
```

### Unit Testing Focus

Unit tests should focus on:

1. **Specific Examples**
   - Known query patterns (e.g., "SELECT * FROM mydb.applications")
   - Common SQL statement types
   - Typical configuration files

2. **Edge Cases**
   - Empty result sets
   - NULL values in results
   - Very long queries
   - Special characters in schema names
   - Queries without schema references

3. **Error Conditions**
   - Invalid configuration files
   - AWS authentication failures
   - Network timeouts
   - Malformed MySQL packets

4. **Integration Points**
   - boto3 mocking for RDS Data API calls
   - Socket communication for MySQL protocol
   - File I/O for configuration loading

### Test Coverage Goals

- Core translation logic: 100% coverage
- Protocol handlers: 90%+ coverage
- Error handling paths: 100% coverage
- Configuration loading: 100% coverage

### Testing Challenges

1. **MySQL Protocol Complexity**: Use existing MySQL client libraries for validation
2. **AWS API Mocking**: Use moto or boto3 stubber for consistent mocking
3. **Concurrency Testing**: Use threading to simulate multiple connections
4. **Random Alias Generation**: Verify uniqueness over many iterations

## Implementation Notes

### MySQL Protocol Library Options

Rather than implementing the MySQL wire protocol from scratch, consider using:

1. **mysql-connector-python**: Can be used to understand packet formats
2. **PyMySQL**: Pure Python implementation, easier to study
3. **Custom implementation**: Minimal subset for proxy use case

Recommendation: Implement a minimal custom protocol handler focusing only on:
- Handshake v10
- COM_QUERY command
- Result set protocol
- OK and Error packets

This reduces complexity and dependencies while meeting requirements.

### Query Parsing Strategy

For schema name detection and alias detection:

1. **Use sqlparse library**: Python SQL parser that handles various SQL dialects
2. **Regex patterns**: For simple cases, regex may suffice
3. **AST-based approach**: Parse SQL into AST, transform, regenerate

Recommendation: Start with sqlparse for robust SQL parsing, fall back to regex for simple patterns.

### Concurrency Model

Use Python's `threading` module:
- Main thread: Accept connections
- Worker threads: One per client connection
- Thread-safe: Each connection has isolated state

Alternative: `asyncio` for async I/O, but threading is simpler for this use case.

### Configuration File Location

Default search paths:
1. `./mysql-rds-proxy.yaml` (current directory)
2. `~/.mysql-rds-proxy.yaml` (home directory)
3. `/etc/mysql-rds-proxy.yaml` (system-wide)
4. Path specified via `--config` command-line argument

### Logging Strategy

Use Python's `logging` module:
- INFO: Connection events, query execution
- DEBUG: Packet details, translation steps
- WARNING: Recoverable errors
- ERROR: Fatal errors, AWS API failures

Log to both console and file (`mysql-rds-proxy.log`).

### Performance Considerations

1. **Connection Pooling**: Reuse boto3 clients across queries
2. **Query Caching**: Consider caching translated queries (optional optimization)
3. **Batch Operations**: RDS Data API supports batch operations (future enhancement)
4. **Timeout Configuration**: Set reasonable timeouts for AWS API calls

### Security Considerations

1. **Credential Storage**: Never log AWS credentials or database passwords
2. **SQL Injection**: Proxy doesn't add injection risk (passes queries through)
3. **Network Security**: Bind to localhost by default, require explicit configuration for external access
4. **TLS**: MySQL protocol TLS support (optional future enhancement)

### Deployment

Package as:
1. **Python package**: `pip install mysql-rds-proxy`
2. **Docker container**: Pre-configured container image
3. **Standalone executable**: PyInstaller for single-file distribution

Include:
- Sample configuration file
- README with setup instructions
- Troubleshooting guide
