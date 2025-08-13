"""
Local settings for development
This file allows developers to test extensions locally
"""

# Extensions for local development testing
LOCAL_EXTENSIONS = [
    # Add any additional extensions for local testing here
    # Example: 'extensions.risk_analytics'
]

# Extension-specific settings for development
RISK_ANALYTICS_DEBUG = True
RISK_ANALYTICS_LICENSE_KEY = 'dev_key_12345'
GITHUB_USER = 'developer'

# Logging configuration for extensions
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
