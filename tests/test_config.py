"""Unit tests for configuration management.

Tests configuration loading, validation, and error handling.
"""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from mysql_rds_proxy.config import ConfigurationManager, ConfigurationError, ProxyConfig


class TestConfigurationManager:
    """Unit tests for ConfigurationManager class."""
    
    def test_load_valid_configuration(self, tmp_path):
        """Test loading a valid configuration file."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test-cluster',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test-secret',
            },
            'schema_mappings': {
                'local': 'remote',
                'test': 'production',
            },
            'logging': {
                'level': 'DEBUG',
                'format': '%(message)s',
                'file': '/var/log/proxy.log',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        manager = ConfigurationManager(str(config_file))
        
        assert manager.get_listen_host() == '127.0.0.1'
        assert manager.get_listen_port() == 3306
        assert manager.get_aws_region() == 'us-west-2'
        assert manager.get_cluster_arn() == 'arn:aws:rds:us-west-2:123456789012:cluster:test-cluster'
        assert manager.get_secret_arn() == 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test-secret'
        assert manager.get_schema_mappings() == {'local': 'remote', 'test': 'production'}
        assert manager.get_log_level() == 'DEBUG'
        assert manager.get_log_format() == '%(message)s'
        assert manager.get_log_file() == '/var/log/proxy.log'
    
    def test_load_configuration_with_defaults(self, tmp_path):
        """Test loading configuration with optional fields using defaults."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '0.0.0.0',
                'listen_port': 3307,
            },
            'aws': {
                'region': 'us-east-1',
                'cluster_arn': 'arn:aws:rds:us-east-1:123456789012:cluster:my-cluster',
                'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        manager = ConfigurationManager(str(config_file))
        
        # Check defaults
        assert manager.get_schema_mappings() == {}
        assert manager.get_log_level() == 'INFO'
        assert manager.get_log_format() == '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        assert manager.get_log_file() is None
    
    def test_missing_configuration_file(self):
        """Test error when configuration file doesn't exist."""
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager('/nonexistent/config.yaml')
        
        assert 'Configuration file not found' in str(exc_info.value)
    
    def test_invalid_yaml_syntax(self, tmp_path):
        """Test error when YAML syntax is invalid."""
        config_file = tmp_path / "config.yaml"
        
        with open(config_file, 'w') as f:
            f.write("invalid: yaml: syntax: [unclosed")
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'Invalid YAML syntax' in str(exc_info.value)
    
    def test_empty_configuration_file(self, tmp_path):
        """Test error when configuration file is empty."""
        config_file = tmp_path / "config.yaml"
        config_file.touch()
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'Configuration file is empty' in str(exc_info.value)
    
    def test_missing_required_field_listen_host(self, tmp_path):
        """Test error when proxy.listen_host is missing."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'proxy.listen_host' in str(exc_info.value)
    
    def test_missing_required_field_listen_port(self, tmp_path):
        """Test error when proxy.listen_port is missing."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'proxy.listen_port' in str(exc_info.value)
    
    def test_missing_required_field_aws_region(self, tmp_path):
        """Test error when aws.region is missing."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'aws.region' in str(exc_info.value)
    
    def test_missing_required_field_cluster_arn(self, tmp_path):
        """Test error when aws.cluster_arn is missing."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'aws.cluster_arn' in str(exc_info.value)
    
    def test_missing_required_field_secret_arn(self, tmp_path):
        """Test error when aws.secret_arn is missing."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'aws.secret_arn' in str(exc_info.value)
    
    def test_invalid_port_type(self, tmp_path):
        """Test error when listen_port is not an integer."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 'not-a-number',
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'must be an integer' in str(exc_info.value)
    
    def test_invalid_port_range_too_low(self, tmp_path):
        """Test error when listen_port is below valid range."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 0,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'must be between 1 and 65535' in str(exc_info.value)
    
    def test_invalid_port_range_too_high(self, tmp_path):
        """Test error when listen_port is above valid range."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 70000,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'must be between 1 and 65535' in str(exc_info.value)
    
    def test_invalid_cluster_arn_format(self, tmp_path):
        """Test error when cluster ARN format is invalid."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'invalid-arn-format',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'Invalid cluster ARN format' in str(exc_info.value)
    
    def test_invalid_secret_arn_format(self, tmp_path):
        """Test error when secret ARN format is invalid."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'invalid-secret-arn',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'Invalid secret ARN format' in str(exc_info.value)
    
    def test_empty_required_field(self, tmp_path):
        """Test error when required field is empty string."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'cannot be empty' in str(exc_info.value)
    
    def test_schema_mappings_not_dict(self, tmp_path):
        """Test error when schema_mappings is not a dictionary."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
            'schema_mappings': ['not', 'a', 'dict'],
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigurationManager(str(config_file))
        
        assert 'must be a dictionary' in str(exc_info.value)
    
    def test_get_schema_mappings_returns_copy(self, tmp_path):
        """Test that get_schema_mappings returns a copy, not the original."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
            'schema_mappings': {
                'local': 'remote',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        manager = ConfigurationManager(str(config_file))
        mappings1 = manager.get_schema_mappings()
        mappings2 = manager.get_schema_mappings()
        
        # Modify one copy
        mappings1['new'] = 'value'
        
        # Other copy should be unchanged
        assert 'new' not in mappings2
        assert mappings2 == {'local': 'remote'}
    
    def test_config_property(self, tmp_path):
        """Test accessing the complete config via property."""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        manager = ConfigurationManager(str(config_file))
        config = manager.config
        
        assert isinstance(config, ProxyConfig)
        assert config.listen_host == '127.0.0.1'
        assert config.listen_port == 3306
