"""Main entry point for running the proxy as a module.

Allows running the proxy with: python -m mysql_rds_proxy
"""

from mysql_rds_proxy.cli import main

if __name__ == '__main__':
    main()
