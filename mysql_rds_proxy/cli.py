"""Command-line interface for MySQL-to-RDS Data API proxy.

This module provides the CLI for starting and configuring the proxy server.
"""

import argparse
import logging
import sys
from pathlib import Path

from .config import ConfigurationManager, ConfigurationError
from .proxy_server import ProxyServer


def setup_logging(log_level: str, log_format: str, log_file: str = None):
    """Configure logging for the proxy.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Python logging format string
        log_file: Optional path to log file
    """
    # Convert log level string to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure handlers
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    # Configure logging
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        handlers=handlers
    )


def find_config_file(config_path: str = None) -> str:
    """Find configuration file.
    
    Searches in default locations if no path specified.
    
    Args:
        config_path: Explicit config file path (optional)
        
    Returns:
        Path to configuration file
        
    Raises:
        FileNotFoundError: If no configuration file found
    """
    if config_path:
        if Path(config_path).exists():
            return config_path
        else:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Search default locations
    default_locations = [
        Path('./mysql-rds-proxy.yaml'),
        Path.home() / '.mysql-rds-proxy.yaml',
        Path('/etc/mysql-rds-proxy.yaml'),
    ]
    
    for location in default_locations:
        if location.exists():
            return str(location)
    
    raise FileNotFoundError(
        "No configuration file found. Searched:\n" +
        "\n".join(f"  - {loc}" for loc in default_locations) +
        "\n\nCreate a configuration file or specify path with --config"
    )


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description='MySQL-to-RDS Data API Translation Proxy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start with default config file
  mysql-rds-proxy

  # Start with specific config file
  mysql-rds-proxy --config /path/to/config.yaml

  # Show version
  mysql-rds-proxy --version

Configuration file locations (searched in order):
  1. ./mysql-rds-proxy.yaml
  2. ~/.mysql-rds-proxy.yaml
  3. /etc/mysql-rds-proxy.yaml
        """
    )
    
    parser.add_argument(
        '--config',
        '-c',
        metavar='PATH',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--version',
        '-v',
        action='version',
        version='mysql-rds-proxy 0.1.0'
    )
    
    args = parser.parse_args()
    
    try:
        # Find configuration file
        config_path = find_config_file(args.config)
        print(f"Using configuration file: {config_path}")
        
        # Load configuration
        config = ConfigurationManager(config_path)
        
        # Setup logging
        setup_logging(
            config.get_log_level(),
            config.get_log_format(),
            config.get_log_file()
        )
        
        # Create and start proxy server
        server = ProxyServer(config)
        server.start()
        
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
        sys.exit(0)
    
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
