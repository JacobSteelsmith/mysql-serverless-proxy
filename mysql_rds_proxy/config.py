"""Configuration management for MySQL-to-RDS Data API proxy.

This module provides configuration loading and validation from YAML files.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class ProxyConfig:
    """Complete proxy configuration.
    
    Attributes:
        listen_host: Host address to bind to (e.g., "127.0.0.1")
        listen_port: TCP port to listen on (e.g., 3306)
        aws_region: AWS region for RDS cluster (e.g., "us-west-2")
        cluster_arn: RDS cluster ARN
        secret_arn: AWS Secrets Manager secret ARN for database credentials
        schema_mappings: Dictionary mapping local schema names to remote schema names
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Python logging format string
        log_file: Optional path to log file
    """
    listen_host: str
    listen_port: int
    aws_region: str
    cluster_arn: str
    secret_arn: str
    schema_mappings: dict[str, str]
    log_level: str
    log_format: str
    log_file: Optional[str] = None


class ConfigurationError(Exception):
    """Raised when configuration is invalid or cannot be loaded."""
    pass


class ConfigurationManager:
    """Manages loading and accessing proxy configuration from YAML files.
    
    The configuration manager loads settings from a YAML file and provides
    type-safe access to configuration values with validation.
    """
    
    def __init__(self, config_path: str):
        """Load configuration from file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Raises:
            ConfigurationError: If configuration file is missing, invalid, or incomplete
        """
        self._config_path = config_path
        self._config: ProxyConfig = self._load_config()
    
    def _load_config(self) -> ProxyConfig:
        """Load and validate configuration from YAML file.
        
        Returns:
            ProxyConfig instance with validated configuration
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        # Check if file exists
        if not os.path.exists(self._config_path):
            raise ConfigurationError(
                f"Configuration file not found: {self._config_path}"
            )
        
        # Load YAML
        try:
            with open(self._config_path, 'r') as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Invalid YAML syntax in configuration file: {e}"
            )
        except Exception as e:
            raise ConfigurationError(
                f"Failed to read configuration file: {e}"
            )
        
        if data is None:
            raise ConfigurationError("Configuration file is empty")
        
        # Validate and extract required fields
        try:
            proxy_config = data.get('proxy', {})
            aws_config = data.get('aws', {})
            schema_mappings = data.get('schema_mappings', {})
            logging_config = data.get('logging', {})
            
            # Validate required fields
            self._validate_required_field(proxy_config, 'listen_host', 'proxy.listen_host')
            self._validate_required_field(proxy_config, 'listen_port', 'proxy.listen_port')
            self._validate_required_field(aws_config, 'region', 'aws.region')
            self._validate_required_field(aws_config, 'cluster_arn', 'aws.cluster_arn')
            self._validate_required_field(aws_config, 'secret_arn', 'aws.secret_arn')
            
            # Validate types
            listen_port = proxy_config['listen_port']
            if not isinstance(listen_port, int):
                raise ConfigurationError(
                    f"proxy.listen_port must be an integer, got {type(listen_port).__name__}"
                )
            
            if not (1 <= listen_port <= 65535):
                raise ConfigurationError(
                    f"proxy.listen_port must be between 1 and 65535, got {listen_port}"
                )
            
            if not isinstance(schema_mappings, dict):
                raise ConfigurationError(
                    f"schema_mappings must be a dictionary, got {type(schema_mappings).__name__}"
                )
            
            # Validate ARN formats
            cluster_arn = aws_config['cluster_arn']
            secret_arn = aws_config['secret_arn']
            
            if not cluster_arn.startswith('arn:aws:rds:'):
                raise ConfigurationError(
                    f"Invalid cluster ARN format: {cluster_arn}"
                )
            
            if not secret_arn.startswith('arn:aws:secretsmanager:'):
                raise ConfigurationError(
                    f"Invalid secret ARN format: {secret_arn}"
                )
            
            # Create ProxyConfig with defaults for optional fields
            return ProxyConfig(
                listen_host=proxy_config['listen_host'],
                listen_port=listen_port,
                aws_region=aws_config['region'],
                cluster_arn=cluster_arn,
                secret_arn=secret_arn,
                schema_mappings=schema_mappings,
                log_level=logging_config.get('level', 'INFO'),
                log_format=logging_config.get(
                    'format',
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                ),
                log_file=logging_config.get('file')
            )
            
        except ConfigurationError:
            raise
        except KeyError as e:
            raise ConfigurationError(f"Missing required configuration field: {e}")
        except Exception as e:
            raise ConfigurationError(f"Configuration validation failed: {e}")
    
    def _validate_required_field(self, config_dict: dict, field: str, full_path: str):
        """Validate that a required field exists in configuration.
        
        Args:
            config_dict: Dictionary to check
            field: Field name to validate
            full_path: Full path for error message (e.g., "aws.region")
            
        Raises:
            ConfigurationError: If field is missing or empty
        """
        if field not in config_dict:
            raise ConfigurationError(f"Missing required configuration field: {full_path}")
        
        value = config_dict[field]
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ConfigurationError(f"Configuration field cannot be empty: {full_path}")
    
    def get_listen_port(self) -> int:
        """Return the port to listen on."""
        return self._config.listen_port
    
    def get_listen_host(self) -> str:
        """Return the host address to bind to."""
        return self._config.listen_host
    
    def get_aws_region(self) -> str:
        """Return the AWS region."""
        return self._config.aws_region
    
    def get_cluster_arn(self) -> str:
        """Return the RDS cluster ARN."""
        return self._config.cluster_arn
    
    def get_secret_arn(self) -> str:
        """Return the Secrets Manager secret ARN."""
        return self._config.secret_arn
    
    def get_schema_mappings(self) -> dict[str, str]:
        """Return schema name mappings."""
        return self._config.schema_mappings.copy()
    
    def get_log_level(self) -> str:
        """Return the logging level."""
        return self._config.log_level
    
    def get_log_format(self) -> str:
        """Return the logging format string."""
        return self._config.log_format
    
    def get_log_file(self) -> Optional[str]:
        """Return the log file path, if configured."""
        return self._config.log_file
    
    @property
    def config(self) -> ProxyConfig:
        """Get the complete configuration object."""
        return self._config
