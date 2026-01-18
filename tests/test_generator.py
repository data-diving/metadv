"""Tests for the MetaDV generator."""

import tempfile
from pathlib import Path

import pytest
import yaml

from metadv import MetaDVGenerator, SUPPORTED_DV_PACKAGES


@pytest.fixture
def sample_project(tmp_path):
    """Create a sample dbt project with metadv configuration."""
    # Create dbt_project.yml
    dbt_project = {
        'name': 'test_project',
        'version': '1.0.0',
        'config-version': 2,
        'profile': 'test',
    }
    dbt_project_path = tmp_path / 'dbt_project.yml'
    with open(dbt_project_path, 'w') as f:
        yaml.dump(dbt_project, f)

    # Create packages.yml with automate_dv
    packages = {
        'packages': [
            {'package': 'datavault-uk/automate_dv', 'version': '0.10.0'}
        ]
    }
    packages_path = tmp_path / 'packages.yml'
    with open(packages_path, 'w') as f:
        yaml.dump(packages, f)

    # Create metadv directory and config
    metadv_dir = tmp_path / 'models' / 'metadv'
    metadv_dir.mkdir(parents=True)

    metadv_config = {
        'metadv': {
            'targets': [
                {
                    'name': 'customer',
                    'type': 'entity',
                    'description': 'Customer entity'
                },
                {
                    'name': 'product',
                    'type': 'entity',
                    'description': 'Product entity'
                },
            ],
            'sources': [
                {
                    'name': 'stg_customers',
                    'columns': [
                        {
                            'name': 'customer_id',
                            'target': [{'target_name': 'customer'}]
                        },
                        {
                            'name': 'customer_name',
                            'target': [{'attribute_of': 'customer'}]
                        },
                    ]
                },
                {
                    'name': 'stg_products',
                    'columns': [
                        {
                            'name': 'product_id',
                            'target': [{'target_name': 'product'}]
                        },
                        {
                            'name': 'product_name',
                            'target': [{'attribute_of': 'product'}]
                        },
                    ]
                },
            ]
        }
    }
    metadv_yml_path = metadv_dir / 'metadv.yml'
    with open(metadv_yml_path, 'w') as f:
        yaml.dump(metadv_config, f)

    return tmp_path


class TestMetaDVGenerator:
    """Test cases for MetaDVGenerator."""

    def test_generator_init(self, sample_project):
        """Test generator initialization."""
        generator = MetaDVGenerator(str(sample_project), 'datavault-uk/automate_dv')
        assert generator.project_path == sample_project
        assert generator.package_prefix == 'automate_dv'

    def test_generator_exists(self, sample_project):
        """Test exists() method."""
        generator = MetaDVGenerator(str(sample_project), 'datavault-uk/automate_dv')
        assert generator.exists() is True

    def test_generator_read(self, sample_project):
        """Test read() method."""
        generator = MetaDVGenerator(str(sample_project), 'datavault-uk/automate_dv')
        success, error, data = generator.read()

        assert success is True
        assert error is None
        assert data is not None
        assert len(data.targets) == 2
        assert len(data.source_columns) == 4

    def test_generator_validate(self, sample_project):
        """Test validate() method."""
        generator = MetaDVGenerator(str(sample_project), 'datavault-uk/automate_dv')
        result = generator.validate()

        assert result.success is True
        assert len(result.errors) == 0

    def test_generator_generate(self, sample_project):
        """Test generate() method."""
        generator = MetaDVGenerator(str(sample_project), 'datavault-uk/automate_dv')
        success, error, files = generator.generate()

        assert success is True
        assert error is None
        assert len(files) > 0

        # Check that expected files were generated
        metadv_dir = sample_project / 'models' / 'metadv'
        assert (metadv_dir / 'stage').exists()
        assert (metadv_dir / 'hub').exists()
        assert (metadv_dir / 'sat').exists()

    def test_generator_with_datavault4dbt(self, sample_project):
        """Test generator with datavault4dbt package."""
        # Update packages.yml to use datavault4dbt
        packages = {
            'packages': [
                {'package': 'scalefreecom/datavault4dbt', 'version': '1.0.0'}
            ]
        }
        packages_path = sample_project / 'packages.yml'
        with open(packages_path, 'w') as f:
            yaml.dump(packages, f)

        generator = MetaDVGenerator(str(sample_project), 'scalefreecom/datavault4dbt')
        assert generator.package_prefix == 'datavault4dbt'

        success, error, files = generator.generate()
        assert success is True


class TestMultiactiveSatellite:
    """Test cases for multiactive satellite generation."""

    def test_multiactive_satellite(self, tmp_path):
        """Test multiactive satellite with multiactive_key columns."""
        # Create minimal project structure
        dbt_project = {'name': 'test', 'version': '1.0.0', 'config-version': 2}
        with open(tmp_path / 'dbt_project.yml', 'w') as f:
            yaml.dump(dbt_project, f)

        metadv_dir = tmp_path / 'models' / 'metadv'
        metadv_dir.mkdir(parents=True)

        metadv_config = {
            'metadv': {
                'targets': [
                    {'name': 'customer', 'type': 'entity'}
                ],
                'sources': [
                    {
                        'name': 'stg_phones',
                        'columns': [
                            {'name': 'customer_id', 'target': [{'target_name': 'customer'}]},
                            {'name': 'phone_type', 'target': [{'attribute_of': 'customer', 'multiactive_key': True}]},
                            {'name': 'phone_number', 'target': [{'attribute_of': 'customer'}]},
                        ]
                    }
                ]
            }
        }
        with open(metadv_dir / 'metadv.yml', 'w') as f:
            yaml.dump(metadv_config, f)

        generator = MetaDVGenerator(str(tmp_path), 'datavault-uk/automate_dv')
        success, error, files = generator.generate()

        assert success is True

        # Check that ma_sat file was generated
        sat_files = [f for f in files if 'ma_sat_' in f]
        assert len(sat_files) > 0


class TestValidation:
    """Test cases for validation."""

    def test_validation_entity_no_source(self, tmp_path):
        """Test warning for entity without source connection."""
        dbt_project = {'name': 'test', 'version': '1.0.0', 'config-version': 2}
        with open(tmp_path / 'dbt_project.yml', 'w') as f:
            yaml.dump(dbt_project, f)

        metadv_dir = tmp_path / 'models' / 'metadv'
        metadv_dir.mkdir(parents=True)

        # Entity with no source columns connecting to it
        metadv_config = {
            'metadv': {
                'targets': [
                    {'name': 'customer', 'type': 'entity'}
                ],
                'sources': []
            }
        }
        with open(metadv_dir / 'metadv.yml', 'w') as f:
            yaml.dump(metadv_config, f)

        generator = MetaDVGenerator(str(tmp_path), 'datavault-uk/automate_dv')
        result = generator.validate()

        assert result.success is True
        # Should have a warning about entity without source
        warning_codes = [w.code for w in result.warnings]
        assert 'entity_no_source' in warning_codes
