"""Common pytest fixtures and configuration."""

import pytest
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch
import boto3
from moto import mock_s3
import pandas as pd

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
    env_vars = {
        'AWS_ACCESS_KEY': 'test-access-key',
        'AWS_SECRET_KEY': 'test-secret-key',
        'REGION': 'us-east-1',
        'BUCKET_NAME': 'test-audiophile-bucket',
        'REDSHIFT_HOST': 'test-cluster.redshift.amazonaws.com',
        'REDSHIFT_PORT': '5439',
        'REDSHIFT_DATABASE': 'test_db',
        'REDSHIFT_USERNAME': 'test_user',
        'REDSHIFT_PASSWORD': 'test_password',
        'REDSHIFT_IAM_ROLE': 'arn:aws:iam::123456789012:role/test-role'
    }
    
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    return env_vars


@pytest.fixture
def sample_headphones_data():
    """Sample headphones data for testing."""
    return {
        'model': 'Sennheiser HD800S',
        'price_msrp': 1699.0,
        'rank': 'S',
        'tone_grade': 'A+',
        'technical_grade': 'S',
        'preference_score': 95.5,
        'driver_type': 'Dynamic',
        'cup_type': 'Open',
        'comments': 'Excellent soundstage'
    }


@pytest.fixture
def sample_iems_data():
    """Sample IEMs data for testing."""
    return {
        'model': 'Moondrop Blessing 2',
        'price_msrp': 320.0,
        'rank': 'A+',
        'value_rating': '★★★',
        'setup': 'Stock',
        'signature': 'Neutral',
        'comments': 'Great value for money'
    }


@pytest.fixture
def sample_headphones_df():
    """Sample headphones DataFrame for testing."""
    data = [
        {
            'model': 'Sennheiser HD800S',
            'price (MSRP)': '$1,699',
            'rank': 'S',
            'tone grade': 'A+',
            'technical grade': 'S',
            'preference score': '95.5',
            'driver type': 'Dynamic',
            'cup type': 'Open',
            'comments': 'Excellent soundstage'
        },
        {
            'model': 'Focal Clear',
            'price (MSRP)': '$1,490',
            'rank': 'A+',
            'tone grade': 'A',
            'technical grade': 'A+',
            'preference score': '92.0',
            'driver type': 'Dynamic',
            'cup type': 'Open',
            'comments': 'Neutral and detailed'
        }
    ]
    return pd.DataFrame(data)


@pytest.fixture
def sample_iems_df():
    """Sample IEMs DataFrame for testing."""
    data = [
        {
            'model': 'Moondrop Blessing 2',
            'price (MSRP)': '$320',
            'rank': 'A+',
            'value rating': '★★★',
            'setup': 'Stock',
            'signature': 'Neutral',
            'comments': 'Great value'
        },
        {
            'model': 'ThieAudio Monarch',
            'price (MSRP)': '$730',
            'rank': 'S-',
            'value rating': '★★',
            'setup': 'Stock',
            'signature': 'U-shaped',
            'comments': 'Technical marvel'
        }
    ]
    return pd.DataFrame(data)


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for testing."""
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        # Create test bucket
        s3.create_bucket(Bucket='test-audiophile-bucket')
        yield s3


@pytest.fixture
def mock_requests_get():
    """Mock requests.get for web scraping tests."""
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '''
        <html>
            <body>
                <table>
                    <thead>
                        <tr>
                            <th>Model</th>
                            <th>Price (MSRP)</th>
                            <th>Rank</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Test Model</td>
                            <td>$500</td>
                            <td>A</td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
        '''
        mock_get.return_value = mock_response
        yield mock_get


@pytest.fixture
def mock_psycopg2_connect():
    """Mock psycopg2 connection for database tests."""
    with patch('psycopg2.connect') as mock_connect:
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_connect