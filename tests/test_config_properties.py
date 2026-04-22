"""Property-based tests for configuration management.

These tests use Hypothesis to verify universal properties across many inputs.
"""

import os
import tempfile
from pathlib import Path

import pytest
import yaml
from hypothesis import given, strategies as st, assume, settings, HealthCheck

from mysql_rds_proxy.config import ConfigurationManager, ConfigurationError


# Custom strategies for generating valid configuration data
@st.composite
def valid_port(draw):
    """Generate a valid port number (1-65535)."""
    return draw(st.integers(min_value=1, max_value=65535))


@st.composite
def valid_host(draw):
    """Generate a valid host address."""
    return draw(st.sampled_from(['127.0.0.1', '0.0.0.0', 'localhost', '192.168.1.1']))


@st.composite
def valid_aws_region(draw):
    """Generate a valid AWS region."""
    return draw(st.sampled_from([
        'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
        'eu-west-1', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1'
    ]))


@st.composite
def valid_cluster_arn(draw):
    """Generate a valid RDS cluster ARN."""
    region = draw(valid_aws_region())
    account_id = draw(st.integers(min_value=100000000000, max_value=999999999999))
    cluster_name = draw(st.text(min_size=1, max_size=63, alphabet=st.characters(
        whitelist_categories=('Ll', 'Lu', 'Nd'), whitelist_characters='-'
    )))
    return f"arn:aws:rds:{region}:{account_id}:cluster:{cluster_name}"


@st.composite
def valid_secret_arn(draw):
    """Generate a valid Secrets Manager secret ARN."""
    region = draw(valid_aws_region())
    account_id = draw(st.integers(min_value=100000000000, max_value=999999999999))
    secret_name = draw(st.text(min_size=1, max_size=512, alphabet=st.characters(
        whitelist_categories=('Ll', 'Lu', 'Nd'), whitelist_characters='-_'
    )))
    return f"arn:aws:secretsmanager:{region}:{account_id}:secret:{secret_name}"


@st.composite
def valid_schema_mappings(draw):
    """Generate valid schema mappings."""
    return draw(st.dictionaries(
        keys=st.text(min_size=1, max_size=64, alphabet=st.characters(
            whitelist_categories=('Ll', 'Lu', 'Nd'), whitelist_characters='_'
        )),
        values=st.text(min_size=1, max_size=64, alphabet=st.characters(
            whitelist_categories=('Ll', 'Lu', 'Nd'), whitelist_characters='_'
        )),
        max_size=10
    ))


@st.composite
def valid_log_level(draw):
    """Generate a valid log level."""
    return draw(st.sampled_from(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))


@st.composite
def valid_config_data(draw):
    """Generate a complete valid configuration."""
    return {
        'proxy': {
            'listen_host': draw(valid_host()),
            'listen_port': draw(valid_port()),
        },
        'aws': {
            'region': draw(valid_aws_region()),
            'cluster_arn': draw(valid_cluster_arn()),
            'secret_arn': draw(valid_secret_arn()),
        },
        'schema_mappings': draw(valid_schema_mappings()),
        'logging': {
            'level': draw(valid_log_level()),
            'format': draw(st.text(min_size=1, max_size=200)),
        },
    }


class TestConfigurationProperties:
    """Property-based tests for ConfigurationManager."""
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(config_data=valid_config_data())
    def test_property_18_configuration_loading(self, config_data):
        """Property 18: Configuration Loading.
        
        For any valid configuration file, loading it should correctly populate
        all configuration fields (cluster ARN, secret ARN, region, port, schema mappings).
        
        **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6**
        """
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            # Load configuration
            manager = ConfigurationManager(config_file)
            
            # Verify all fields are correctly loaded
            assert manager.get_listen_host() == config_data['proxy']['listen_host']
            assert manager.get_listen_port() == config_data['proxy']['listen_port']
            assert manager.get_aws_region() == config_data['aws']['region']
            assert manager.get_cluster_arn() == config_data['aws']['cluster_arn']
            assert manager.get_secret_arn() == config_data['aws']['secret_arn']
            assert manager.get_schema_mappings() == config_data['schema_mappings']
            assert manager.get_log_level() == config_data['logging']['level']
            assert manager.get_log_format() == config_data['logging']['format']
        finally:
            # Clean up
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @pytest.mark.property
    @settings(max_examples=50)
    @given(
        field_to_remove=st.sampled_from([
            'proxy.listen_host',
            'proxy.listen_port',
            'aws.region',
            'aws.cluster_arn',
            'aws.secret_arn',
        ])
    )
    def test_property_19_invalid_configuration_handling(self, field_to_remove):
        """Property 19: Invalid Configuration Handling.
        
        For any invalid or malformed configuration file, the configuration manager
        should raise a clear error indicating what is wrong.
        
        **Validates: Requirements 6.7**
        """
        # Create a valid base configuration
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
        
        # Remove the specified field
        section, field = field_to_remove.split('.')
        del config_data[section][field]
        
        # Write config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            # Verify that loading raises ConfigurationError with clear message
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigurationManager(config_file)
            
            # Error message should mention the missing field
            error_message = str(exc_info.value).lower()
            assert field.lower() in error_message or field_to_remove.lower() in error_message
        finally:
            # Clean up
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @pytest.mark.property
    @settings(max_examples=50)
    @given(
        invalid_port=st.one_of(
            st.integers(max_value=0),
            st.integers(min_value=65536),
        )
    )
    def test_property_19_invalid_port_range(self, invalid_port):
        """Property 19: Invalid Configuration Handling - Port Range.
        
        For any port number outside the valid range (1-65535), the configuration
        manager should raise a clear error.
        
        **Validates: Requirements 6.7**
        """
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': invalid_port,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigurationManager(config_file)
            
            error_message = str(exc_info.value).lower()
            assert 'port' in error_message or 'between' in error_message
        finally:
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @pytest.mark.property
    @settings(max_examples=50)
    @given(
        invalid_arn=st.text(min_size=1, max_size=100).filter(
            lambda x: not x.startswith('arn:aws:rds:') and not x.startswith('arn:aws:secretsmanager:')
        )
    )
    def test_property_19_invalid_cluster_arn_format(self, invalid_arn):
        """Property 19: Invalid Configuration Handling - Cluster ARN Format.
        
        For any invalid cluster ARN format, the configuration manager should raise
        a clear error indicating the ARN is invalid.
        
        **Validates: Requirements 6.7**
        """
        # Test with invalid cluster ARN
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': invalid_arn,
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigurationManager(config_file)
            
            error_message = str(exc_info.value).lower()
            assert 'arn' in error_message or 'invalid' in error_message
        finally:
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @pytest.mark.property
    @settings(max_examples=50)
    @given(
        invalid_arn=st.text(min_size=1, max_size=100).filter(
            lambda x: not x.startswith('arn:aws:secretsmanager:')
        )
    )
    def test_property_19_invalid_secret_arn_format(self, invalid_arn):
        """Property 19: Invalid Configuration Handling - Secret ARN Format.
        
        For any invalid secret ARN format, the configuration manager should raise
        a clear error indicating the ARN is invalid.
        
        **Validates: Requirements 6.7**
        """
        # Test with invalid secret ARN
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': 3306,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': invalid_arn,
            },
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigurationManager(config_file)
            
            error_message = str(exc_info.value).lower()
            assert 'arn' in error_message or 'invalid' in error_message or 'secret' in error_message
        finally:
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @pytest.mark.property
    def test_property_19_invalid_yaml_syntax(self):
        """Property 19: Invalid Configuration Handling - YAML Syntax.
        
        For any malformed YAML content, the configuration manager should raise
        a clear error indicating the YAML syntax is invalid.
        
        **Validates: Requirements 6.7**
        """
        # Create file with invalid YAML that will definitely fail parsing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            # Write intentionally malformed YAML
            f.write("invalid: yaml: syntax: [unclosed\n")
            f.write("  - item1\n")
            f.write("  - item2\n")
            f.write("  [another unclosed\n")
            config_file = f.name
        
        try:
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigurationManager(config_file)
            
            error_message = str(exc_info.value).lower()
            # The error should mention YAML or syntax issues
            assert 'yaml' in error_message or 'syntax' in error_message or 'invalid' in error_message or 'failed' in error_message
        finally:
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @pytest.mark.property
    @settings(max_examples=50)
    @given(
        invalid_port_type=st.one_of(
            st.text(min_size=1, max_size=10),
            st.floats(allow_nan=False, allow_infinity=False),
            st.lists(st.integers()),
        )
    )
    def test_property_19_invalid_port_type(self, invalid_port_type):
        """Property 19: Invalid Configuration Handling - Port Type.
        
        For any non-integer port value, the configuration manager should raise
        a clear error indicating the port must be an integer.
        
        **Validates: Requirements 6.7**
        """
        # Skip if the value happens to be an integer (edge case with floats like 3306.0)
        if isinstance(invalid_port_type, (int, bool)):
            assume(False)
        
        config_data = {
            'proxy': {
                'listen_host': '127.0.0.1',
                'listen_port': invalid_port_type,
            },
            'aws': {
                'region': 'us-west-2',
                'cluster_arn': 'arn:aws:rds:us-west-2:123456789012:cluster:test',
                'secret_arn': 'arn:aws:secretsmanager:us-west-2:123456789012:secret:test',
            },
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigurationManager(config_file)
            
            error_message = str(exc_info.value).lower()
            assert 'port' in error_message or 'integer' in error_message or 'type' in error_message
        finally:
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @pytest.mark.property
    @settings(max_examples=50)
    @given(
        invalid_mappings=st.one_of(
            st.lists(st.text()),
            st.text(),
            st.integers(),
        )
    )
    def test_property_19_invalid_schema_mappings_type(self, invalid_mappings):
        """Property 19: Invalid Configuration Handling - Schema Mappings Type.
        
        For any non-dictionary schema_mappings value, the configuration manager
        should raise a clear error indicating it must be a dictionary.
        
        **Validates: Requirements 6.7**
        """
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
            'schema_mappings': invalid_mappings,
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigurationManager(config_file)
            
            error_message = str(exc_info.value).lower()
            assert 'schema' in error_message or 'dictionary' in error_message or 'dict' in error_message
        finally:
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @pytest.mark.property
    @settings(max_examples=50)
    @given(config_data=valid_config_data())
    def test_schema_mappings_isolation(self, config_data):
        """Verify that modifying returned schema mappings doesn't affect internal state.
        
        For any valid configuration, get_schema_mappings should return a copy
        that can be modified without affecting the configuration manager's internal state.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            manager = ConfigurationManager(config_file)
            
            # Get mappings twice
            mappings1 = manager.get_schema_mappings()
            mappings2 = manager.get_schema_mappings()
            
            # Modify first copy
            mappings1['new_key'] = 'new_value'
            mappings1.clear()
            
            # Second copy should be unchanged
            assert mappings2 == config_data['schema_mappings']
            
            # Getting mappings again should return original
            mappings3 = manager.get_schema_mappings()
            assert mappings3 == config_data['schema_mappings']
        finally:
            if os.path.exists(config_file):
                os.unlink(config_file)
