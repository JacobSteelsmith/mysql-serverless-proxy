# MySQL-Mimic Integration Plan

## Overview

Replace our custom MySQL protocol implementation with **mysql-mimic**, a mature pure-Python library that implements the MySQL server wire protocol correctly.

**Repository**: https://github.com/kelsin/mysql-mimic (now maintained at https://github.com/barakalon/mysql-mimic)
**PyPI**: https://pypi.org/project/mysql-mimic/
**License**: MIT

## Why mysql-mimic?

1. **Battle-tested**: Handles all MySQL protocol complexities correctly
2. **Async-first**: Built on asyncio for better performance
3. **Complete**: Supports authentication, metadata queries, variables, etc.
4. **Maintained**: Active development and bug fixes
5. **Clean API**: Simple Session interface for implementing query handlers

## Architecture Changes

### Current Architecture
```
Client → ProxyServer → ConnectionManager → MySQLProtocolHandler → QueryTranslator → RDSClient
```

### New Architecture with mysql-mimic
```
Client → MysqlServer (mysql-mimic) → CustomSession → QueryTranslator → RDSClient
```

## Implementation Steps

### 1. Add Dependency
```bash
pip install mysql-mimic
```

Update `requirements.txt` and `setup.py`.

### 2. Create Custom Session Class

```python
from mysql_mimic import Session

class RDSProxySession(Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_translator = QueryTranslator(schema_mapper)
        self.rds_client = RDSClient(config)
        self.response_translator = ResponseTranslator()
    
    async def query(self, expression, sql, attrs):
        """Handle query execution."""
        # Translate query
        translated_sql = self.query_translator.translate(sql)
        
        # Execute via RDS Data API
        result = await asyncio.to_thread(
            self.rds_client.execute_query,
            translated_sql,
            database=self.database
        )
        
        # Translate response
        if result.columns:
            columns, rows = self.response_translator.translate_result_set(result)
            return rows, [col.name for col in columns]
        else:
            # DML query - return affected rows
            return [], []
    
    async def schema(self):
        """Optionally provide database schema for INFORMATION_SCHEMA queries."""
        # Could query RDS Data API for schema information
        return {}
```

### 3. Update Server Entry Point

```python
import asyncio
from mysql_mimic import MysqlServer
from .session import RDSProxySession

def main():
    config = ConfigurationManager(config_path)
    
    # Create mysql-mimic server
    server = MysqlServer(
        host=config.get_listen_host(),
        port=config.get_listen_port(),
        session_factory=lambda: RDSProxySession(config)
    )
    
    # Run server
    asyncio.run(server.serve_forever())
```

### 4. Remove Custom Protocol Code

Delete or deprecate:
- `mysql_rds_proxy/protocol_handler.py` (replaced by mysql-mimic)
- `mysql_rds_proxy/connection_manager.py` (replaced by mysql-mimic)
- `mysql_rds_proxy/proxy_server.py` (simplified)

Keep:
- `mysql_rds_proxy/config.py`
- `mysql_rds_proxy/query_translator.py`
- `mysql_rds_proxy/schema_mapper.py`
- `mysql_rds_proxy/rds_client.py`
- `mysql_rds_proxy/response_translator.py`

### 5. Update Tests

- Remove protocol handler tests
- Remove connection manager tests
- Keep query translator, RDS client, response translator tests
- Add integration tests with mysql-mimic

## Benefits

1. **Fixes protocol issues**: mysql-mimic handles all protocol edge cases correctly
2. **Reduces code**: ~500 lines of complex protocol code removed
3. **Better performance**: Async I/O for concurrent connections
4. **More features**: Built-in support for metadata queries, variables, etc.
5. **Maintainability**: Protocol updates handled by mysql-mimic maintainers

## Risks & Mitigation

### Risk 1: Async/Sync Mismatch
- **Issue**: mysql-mimic is async, our RDS client is sync
- **Mitigation**: Use `asyncio.to_thread()` to run sync code in thread pool

### Risk 2: API Differences
- **Issue**: mysql-mimic API may not match our needs exactly
- **Mitigation**: Wrapper layer to adapt between mysql-mimic and our code

### Risk 3: Dependency Management
- **Issue**: Adding external dependency
- **Mitigation**: mysql-mimic is pure Python, MIT licensed, actively maintained

## Timeline

- **Phase 1** (2 hours): Install mysql-mimic, create basic session class
- **Phase 2** (2 hours): Integrate query translator and RDS client
- **Phase 3** (2 hours): Update tests and verify functionality
- **Phase 4** (1 hour): Documentation and cleanup

**Total**: ~7 hours of development time

## Success Criteria

1. MySQL client connects without errors
2. Queries execute and return correct results
3. No "Lost connection" errors
4. All existing tests pass (with protocol tests removed/updated)
5. Performance is equal or better than current implementation

## Rollback Plan

If integration fails:
1. Keep custom protocol implementation in separate branch
2. Can revert to custom implementation if needed
3. Document issues encountered for future attempts

## Next Steps

1. Create new branch: `feature/mysql-mimic-integration`
2. Install mysql-mimic dependency
3. Implement RDSProxySession class
4. Test with MySQL client
5. Update tests
6. Merge if successful

## References

- mysql-mimic GitHub: https://github.com/barakalon/mysql-mimic
- mysql-mimic Examples: https://github.com/barakalon/mysql-mimic/tree/main/examples
- MySQL Protocol Docs: https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html
