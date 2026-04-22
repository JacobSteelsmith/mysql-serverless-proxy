# MySQL-to-RDS Data API Translation Proxy

A local proxy server that translates MySQL protocol connections into AWS RDS Data API calls, enabling standard MySQL clients (like MySQL Workbench) and applications (like Lucee) to connect to serverless RDS clusters that only expose the Data API interface.

## Features

- **MySQL Protocol Support**: Accept standard MySQL client connections
- **Transparent Translation**: Automatically translate queries for RDS Data API compatibility
- **Schema Mapping**: Map local schema names to remote schema names
- **Alias Wrapping**: Handle column aliases that are incompatible with RDS Data API
- **Database Selection**: Support for `USE database` command
- **Configuration-Driven**: Easy YAML-based configuration
- **Concurrent Connections**: Handle multiple simultaneous client connections

## Installation

### From Source

```bash
# Clone the repository
git clone <repository-url>
cd mysql-rds-proxy

sudo apt install pipx

# Install with pip
pipx install -e .
```


### Reinstall after changes
```bash
pipx reinstall mysql-rds-proxy --force
```

### For Development

```bash
# Install with development dependencies
pipx install -e ".[dev]"
```

## Quick Start

1. **Copy the example configuration file:**

```bash
cp mysql-rds-proxy.yaml.example mysql-rds-proxy.yaml
```

2. **Edit the configuration with your AWS RDS cluster details:**

```yaml
proxy:
  listen_port: 3306
  listen_host: "127.0.0.1"

aws:
  region: "us-west-2"
  cluster_arn: "arn:aws:rds:us-west-2:123456789012:cluster:my-cluster"
  secret_arn: "arn:aws:secretsmanager:us-west-2:123456789012:secret:my-secret"

schema_mappings:
  local_schema: "remote_schema"

logging:
  level: "INFO"
```

3. **Start the proxy server:**

```bash
mysql-rds-proxy --config mysql-rds-proxy.yaml
```

4. **Connect with any MySQL client:**

```bash
# Using mysql command-line client
mysql -h 127.0.0.1 -P 3306 -u username -p

# Using MySQL Workbench
# Create a new connection with:
# - Hostname: 127.0.0.1
# - Port: 3306
# - Username: (any - authentication is handled by AWS)
```

## Configuration

### Configuration File Format

The proxy uses a YAML configuration file with the following structure:

```yaml
proxy:
  # Local port to listen on for MySQL connections
  listen_port: 3306
  
  # Host address to bind to
  # - "127.0.0.1" for localhost only (recommended for security)
  # - "0.0.0.0" for all interfaces
  listen_host: "127.0.0.1"

aws:
  # AWS region where your RDS cluster is located
  region: "us-west-2"
  
  # RDS cluster ARN (Amazon Resource Name)
  # Find this in the AWS RDS console
  cluster_arn: "arn:aws:rds:us-west-2:123456789012:cluster:my-cluster"
  
  # AWS Secrets Manager secret ARN for database credentials
  # The proxy uses this to authenticate with the RDS cluster
  secret_arn: "arn:aws:secretsmanager:us-west-2:123456789012:secret:my-secret"

# Schema name mappings: local_name -> remote_name
# Queries using local_name will be automatically translated to use remote_name
schema_mappings:
  mydb: "mydb_jacobs"
  test: "test_jacobs"
  # Add more mappings as needed

logging:
  # Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
  level: "INFO"
  
  # Log format (Python logging format string)
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  
  # Optional: Log file path (if not specified, logs to console only)
  # file: "mysql-rds-proxy.log"
```

### Configuration File Locations

The proxy searches for configuration files in the following order:

1. Path specified with `--config` flag
2. `./mysql-rds-proxy.yaml` (current directory)
3. `~/.mysql-rds-proxy.yaml` (home directory)
4. `/etc/mysql-rds-proxy.yaml` (system-wide)

## Usage Examples

### MySQL Workbench

1. Open MySQL Workbench
2. Create a new connection:
   - Connection Name: "RDS Data API Proxy"
   - Hostname: 127.0.0.1
   - Port: 3306
   - Username: (any value - authentication is handled by AWS)
3. Test the connection and connect

### Command-Line MySQL Client

```bash
# Connect to the proxy
mysql -h 127.0.0.1 -P 3306 -u myuser -p

# Select a database
mysql> USE mydb_jacobs;
Database changed

# Run queries - schema names are automatically mapped
mysql> SELECT * FROM mydb.applications;
# This is translated to: SELECT * FROM mydb_jacobs.applications

# Or use the database you selected with USE
mysql> SELECT * FROM applications;
# This uses the database set by USE mydb_jacobs

# Queries with aliases are automatically wrapped
mysql> SELECT id AS user_id FROM mydb.users;
# This is translated to: SELECT * FROM (SELECT id AS user_id FROM mydb_jacobs.users) AS x5rhy
```

### Lucee (CFML)

Add a datasource in Lucee Administrator:

```cfml
<cfset datasource = {
    class: "com.mysql.cj.jdbc.Driver",
    connectionString: "jdbc:mysql://127.0.0.1:3306/",
    username: "myuser",
    password: "mypassword"
}>

<cfquery datasource="#datasource#">
    SELECT * FROM mydb.applications
</cfquery>
```

### Python with mysql-connector

```python
import mysql.connector

# Connect to the proxy
conn = mysql.connector.connect(
    host='127.0.0.1',
    port=3306,
    user='myuser',
    password='mypassword'
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM mydb.applications")
results = cursor.fetchall()

conn.close()
```

## How It Works

The proxy acts as a translation layer between MySQL clients and AWS RDS Data API:

1. **Client Connection**: MySQL client connects to the proxy using standard MySQL protocol
2. **Query Translation**: 
   - Schema names are mapped according to configuration
   - Queries with column aliases are wrapped in subqueries (RDS Data API requirement)
3. **RDS Execution**: Translated query is executed via AWS RDS Data API using boto3
4. **Response Translation**: RDS Data API response is converted back to MySQL protocol format
5. **Client Response**: Results are sent to the client in MySQL format

### Query Translation Examples

**Schema Mapping:**
```sql
-- Original query
SELECT * FROM mydb.applications

-- Translated query (with mapping mydb -> mydb_jacobs)
SELECT * FROM mydb_jacobs.applications
```

**Alias Wrapping:**
```sql
-- Original query
SELECT id AS user_id, name AS user_name FROM users

-- Translated query (wrapped for RDS Data API compatibility)
SELECT * FROM (SELECT id AS user_id, name AS user_name FROM users) AS x5rhy
```

**Combined:**
```sql
-- Original query
SELECT id AS user_id FROM mydb.users

-- Translated query
SELECT * FROM (SELECT id AS user_id FROM mydb_jacobs.users) AS x5rhy
```

## Requirements

- **Python**: 3.10 or higher
- **AWS Credentials**: Configured via one of:
  - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  - AWS credentials file (`~/.aws/credentials`)
  - IAM role (if running on EC2/ECS)
- **RDS Cluster**: Aurora Serverless v2 or v1 with Data API enabled
- **Secrets Manager**: Secret containing database credentials
- **IAM Permissions**: The AWS credentials must have permissions for:
  - `rds-data:ExecuteStatement`
  - `secretsmanager:GetSecretValue`

## Troubleshooting

### Connection Refused

**Problem**: Client cannot connect to proxy

**Solutions**:
- Verify proxy is running: `ps aux | grep mysql-rds-proxy`
- Check port is not in use: `lsof -i :3306`
- Verify `listen_host` in config (use `0.0.0.0` to accept external connections)

### AWS Authentication Errors

**Problem**: "Access denied" or "Invalid credentials"

**Solutions**:
- Verify AWS credentials are configured: `aws sts get-caller-identity`
- Check IAM permissions for RDS Data API and Secrets Manager
- Verify cluster ARN and secret ARN are correct

### Query Execution Errors

**Problem**: Queries fail with syntax errors

**Solutions**:
- Check schema mappings in configuration
- Verify schema names exist in RDS cluster
- Enable DEBUG logging to see translated queries: `level: "DEBUG"`

### Performance Issues

**Problem**: Queries are slow

**Solutions**:
- RDS Data API has higher latency than direct MySQL connections
- Consider connection pooling in your application
- Use batch operations where possible
- Monitor AWS CloudWatch metrics for RDS Data API

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=mysql_rds_proxy --cov-report=html

# Run specific test file
pytest tests/test_integration.py -v

# Run property-based tests only
pytest -m property
```

### Project Structure

```
mysql-rds-proxy/
├── mysql_rds_proxy/          # Main package
│   ├── __init__.py
│   ├── cli.py                # Command-line interface
│   ├── config.py             # Configuration management
│   ├── connection_manager.py # Connection lifecycle
│   ├── protocol_handler.py   # MySQL wire protocol
│   ├── proxy_server.py       # Main server
│   ├── query_translator.py   # Query translation
│   ├── rds_client.py         # AWS RDS Data API client
│   ├── response_translator.py # Response translation
│   └── schema_mapper.py      # Schema name mapping
├── tests/                    # Test suite
│   ├── test_*.py            # Unit tests
│   ├── test_*_properties.py # Property-based tests
│   └── test_integration.py  # Integration tests
├── mysql-rds-proxy.yaml.example # Example configuration
├── README.md
├── setup.py
└── requirements.txt
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass: `pytest`
6. Submit a pull request

## License

MIT License

## Support

For issues, questions, or contributions, please open an issue on GitHub.
