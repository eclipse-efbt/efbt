# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ReadmeGenerator:
    """
    Generates comprehensive README.md files for BIRD extensions.
    Creates documentation with installation, configuration, and usage instructions.
    """
    
    def __init__(self):
        pass
    
    def generate(
        self,
        output_path: Path,
        extension_name: str,
        repo_name: str,
        username: str,
        platform: str,
        version: str = '1.0.0',
        description: str = None,
        dependency_analysis: Dict = None,
        **kwargs
    ) -> Path:
        """
        Generate comprehensive README.md file.
        
        Args:
            output_path: Directory where to create README.md
            extension_name: Name of the extension
            repo_name: Repository name
            username: GitHub/GitLab username or organization
            platform: 'github' or 'gitlab'
            version: Extension version
            description: Extension description
            **kwargs: Additional options
            
        Returns:
            Path to the generated README.md file
        """
        readme_path = output_path / 'README.md'
        
        display_name = extension_name.replace('_', ' ').title()
        repo_url = self._get_repo_url(platform, username, repo_name)
        
        with open(readme_path, 'w', encoding='utf-8') as f:
            # Write header
            self._write_header(f, display_name, description, repo_url, platform, version)
            
            # Write table of contents
            self._write_table_of_contents(f)
            
            # Write overview section
            self._write_overview(f, extension_name, display_name, description)
            
            # Write features section
            self._write_features(f, extension_name, kwargs)
            
            # Write requirements section
            self._write_requirements(f)
            
            # Write installation section
            self._write_installation(f, extension_name, repo_url, dependency_analysis)
            
            # Write configuration section
            self._write_configuration(f, extension_name)
            
            # Write usage section
            self._write_usage(f, extension_name, display_name)
            
            # Write API documentation section
            self._write_api_documentation(f, extension_name)
            
            # Write development section
            self._write_development(f, extension_name, repo_url)
            
            # Write troubleshooting section
            self._write_troubleshooting(f, extension_name)
            
            # Write contributing section
            self._write_contributing(f, repo_url)
            
            # Write changelog section
            self._write_changelog(f)
            
            # Write license section
            self._write_license(f)
            
            # Write footer
            self._write_footer(f, display_name, username, platform)
        
        logger.info(f"Generated README.md for {extension_name}")
        return readme_path
    
    def _get_repo_url(self, platform: str, username: str, repo_name: str) -> str:
        """Get the repository URL based on platform."""
        if platform == 'github':
            return f"https://github.com/{username}/{repo_name}"
        elif platform == 'gitlab':
            return f"https://gitlab.com/{username}/{repo_name}"
        else:
            return f"https://github.com/{username}/{repo_name}"
    
    def _write_header(self, f, display_name: str, description: str, repo_url: str, platform: str, version: str):
        """Write the README header with badges and basic info."""
        f.write(f"# {display_name}\n\n")
        
        # Add badges
        if platform == 'github':
            f.write(f"[![License: EPL-2.0](https://img.shields.io/badge/License-EPL%202.0-red.svg)]({repo_url}/blob/main/LICENSE)\n")
            f.write(f"[![Version](https://img.shields.io/badge/version-{version}-blue.svg)]({repo_url}/releases)\n")
            f.write(f"[![BIRD Compatible](https://img.shields.io/badge/BIRD-5.1%2B-green.svg)](https://github.com/eclipse-efbt/efbt)\n")
            f.write(f"[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org)\n")
            f.write(f"[![Django](https://img.shields.io/badge/django-5.1%2B-green.svg)](https://djangoproject.com)\n\n")
        
        # Description
        if description:
            f.write(f"{description}\n\n")
        
        f.write(f"A powerful extension for the BIRD (Banks' Integrated Reporting Dictionary) framework, ")
        f.write(f"providing enhanced functionality for regulatory reporting and data processing.\n\n")
    
    def _write_table_of_contents(self, f):
        """Write table of contents."""
        f.write("## 📋 Table of Contents\n\n")
        f.write("- [Overview](#-overview)\n")
        f.write("- [Features](#-features)\n") 
        f.write("- [Requirements](#-requirements)\n")
        f.write("- [Installation](#-installation)\n")
        f.write("- [Configuration](#-configuration)\n")
        f.write("- [Usage](#-usage)\n")
        f.write("- [API Documentation](#-api-documentation)\n")
        f.write("- [Development](#-development)\n")
        f.write("- [Troubleshooting](#-troubleshooting)\n")
        f.write("- [Contributing](#-contributing)\n")
        f.write("- [Changelog](#-changelog)\n")
        f.write("- [License](#-license)\n\n")
    
    def _write_overview(self, f, extension_name: str, display_name: str, description: str):
        """Write the overview section."""
        f.write("## 🔍 Overview\n\n")
        f.write(f"{display_name} is a specialized extension for the BIRD Bench platform that enhances ")
        f.write(f"the core functionality with additional features for regulatory reporting and data analysis.\n\n")
        
        f.write("### What is BIRD?\n\n")
        f.write("BIRD (Banks' Integrated Reporting Dictionary) is a project aimed at providing a common ")
        f.write("language for reporting and data models in the banking industry. It helps banks standardize ")
        f.write("their regulatory reporting processes.\n\n")
        
        f.write("### Extension Purpose\n\n")
        f.write(f"This extension provides specialized functionality that extends the core BIRD capabilities ")
        f.write(f"with domain-specific features, enhanced user interfaces, and additional data processing tools.\n\n")
    
    def _write_features(self, f, extension_name: str, kwargs: Dict):
        """Write the features section."""
        f.write("## ✨ Features\n\n")
        
        # Standard features
        features = [
            "🎯 **Seamless Integration**: Fully integrated with BIRD Bench core platform",
            "🔧 **Easy Installation**: Automated installation and configuration",
            "📊 **Enhanced UI**: Modern, responsive user interface components",
            "🔌 **Plugin Architecture**: Modular design for easy customization",
            "🛡️ **Secure**: Built with security best practices",
            "📱 **Responsive Design**: Works on desktop, tablet, and mobile devices",
            "🔄 **Real-time Updates**: Live data processing and updates",
            "📈 **Performance Optimized**: Efficient data handling and processing"
        ]
        
        # Add custom features if provided
        custom_features = kwargs.get('features', [])
        if custom_features:
            features.extend([f"✅ **{feature}**" for feature in custom_features])
        
        for feature in features:
            f.write(f"- {feature}\n")
        
        f.write("\n")
        
        # Feature details
        f.write("### Core Functionality\n\n")
        f.write("- **Data Processing**: Advanced algorithms for regulatory data transformation\n")
        f.write("- **Report Generation**: Automated generation of compliance reports\n")
        f.write("- **Validation Engine**: Built-in data validation and quality checks\n")
        f.write("- **Export Capabilities**: Multiple export formats (CSV, Excel, PDF, JSON)\n")
        f.write("- **API Integration**: RESTful API for external system integration\n")
        f.write("- **Audit Trail**: Comprehensive logging and audit capabilities\n\n")
    
    def _write_requirements(self, f):
        """Write the requirements section."""
        f.write("## 📋 Requirements\n\n")
        
        f.write("### System Requirements\n\n")
        f.write("- **Operating System**: Linux, macOS, or Windows\n")
        f.write("- **Python**: 3.8 or higher\n")
        f.write("- **Memory**: Minimum 4GB RAM (8GB recommended)\n")
        f.write("- **Disk Space**: At least 1GB free space\n")
        f.write("- **Network**: Internet connection for installation\n\n")
        
        f.write("### Software Dependencies\n\n")
        f.write("- **Django**: 5.1.0 or higher\n")
        f.write("- **BIRD Bench**: Latest version\n")
        f.write("- **PyEcore**: 0.15.1 or higher\n")
        f.write("- **Database**: SQLite (default) or PostgreSQL\n\n")
        
        f.write("### Compatible BIRD Versions\n\n")
        f.write("| BIRD Version | Extension Version | Status |\n")
        f.write("|--------------|------------------|--------|\n")
        f.write("| 5.1.x        | 1.0.x           | ✅ Supported |\n")
        f.write("| 5.2.x        | 1.0.x           | 🧪 Beta |\n\n")
    
    def _write_installation(self, f, extension_name: str, repo_url: str, dependency_analysis: Dict = None):
        """Write the installation section with dependency-aware instructions."""
        f.write("## 🚀 Installation\n\n")
        
        # Write dependency overview if available
        if dependency_analysis:
            runtime_deps = dependency_analysis.get('runtime_dependencies', {})
            dev_deps = dependency_analysis.get('development_dependencies', {})
            
            f.write("### Prerequisites\n\n")
            f.write("This extension requires the following dependencies beyond the base BIRD framework:\n\n")
            
            if runtime_deps:
                core_deps = {'django', 'pyecore'}
                extension_deps = {k: v for k, v in runtime_deps.items() if k not in core_deps}
                
                if extension_deps:
                    f.write("**Additional Runtime Dependencies:**\n")
                    for pkg, version in sorted(extension_deps.items()):
                        f.write(f"- `{pkg}{version}`\n")
                    f.write("\n")
            
            if dev_deps:
                f.write("**Development Dependencies (optional):**\n")
                for pkg, version in sorted(list(dev_deps.items())[:5]):  # Show top 5
                    f.write(f"- `{pkg}{version}`\n")
                if len(dev_deps) > 5:
                    f.write(f"- ... and {len(dev_deps) - 5} more (see requirements-dev.txt)\n")
                f.write("\n")
        
        f.write("### Quick Install\n\n")
        f.write("1. **Clone the repository**:\n")
        f.write("   ```bash\n")
        f.write(f"   git clone {repo_url}.git\n")
        f.write(f"   cd {repo_url.split('/')[-1]}\n")
        f.write("   ```\n\n")
        
        f.write("2. **Run the automated installer**:\n")
        f.write("   ```bash\n")
        f.write("   python install.py --bird-path /path/to/your/bird/project\n")
        f.write("   ```\n\n")
        
        f.write("3. **Restart your BIRD server**:\n")
        f.write("   ```bash\n")
        f.write("   cd /path/to/your/bird/project\n")
        f.write("   python manage.py runserver\n")
        f.write("   ```\n\n")
        
        f.write("### Manual Installation\n\n")
        f.write("If you prefer manual installation:\n\n")
        
        f.write("1. **Install runtime dependencies**:\n")
        f.write("   ```bash\n")
        if dependency_analysis:
            f.write("   # Using pip\n")
            f.write("   pip install -r requirements.txt\n\n")
            f.write("   # Using uv (recommended)\n")
            f.write("   uv add -r requirements.txt\n")
        else:
            f.write("   pip install -r requirements.txt\n")
        f.write("   ```\n\n")
        
        f.write("2. **Install development dependencies** (optional):\n")
        f.write("   ```bash\n")
        if dependency_analysis:
            f.write("   # Using pip\n")
            f.write("   pip install -r requirements-dev.txt\n\n")
            f.write("   # Using uv\n")
            f.write("   uv add -r requirements-dev.txt --group dev\n")
        else:
            f.write("   pip install -r requirements-dev.txt\n")
        f.write("   ```\n\n")
        
        f.write("3. **Copy extension files**:\n")
        f.write("   ```bash\n")
        f.write(f"   cp -r {extension_name} /path/to/bird/extensions/\n")
        f.write("   ```\n\n")
        
        f.write("4. **Run migrations** (if the extension has models):\n")
        f.write("   ```bash\n")
        f.write("   cd /path/to/bird/project\n")
        f.write(f"   python manage.py makemigrations {extension_name}\n")
        f.write("   python manage.py migrate\n")
        f.write("   ```\n\n")
        
        f.write("5. **Collect static files**:\n")
        f.write("   ```bash\n")
        f.write("   python manage.py collectstatic\n")
        f.write("   ```\n\n")
        
        f.write("### Modern Python Installation (pyproject.toml)\n\n")
        f.write("This extension includes a modern `pyproject.toml` configuration:\n\n")
        f.write("```bash\n")
        f.write("# Install in editable mode for development\n")
        f.write("pip install -e .\n\n")
        f.write("# Install with optional development dependencies\n")
        f.write("pip install -e .[dev]\n\n")
        f.write("# Using uv (fastest)\n")
        f.write("uv pip install -e .\n")
        f.write("```\n\n")
        
        f.write("### Standalone Installation\n\n")
        f.write("To install as a standalone Django project:\n\n")
        f.write("```bash\n")
        f.write("python install.py --standalone\n")
        f.write("python manage.py runserver\n")
        f.write("```\n\n")
        
        f.write("### Dependency Management\n\n")
        f.write("This extension uses modern Python packaging standards:\n\n")
        f.write("- **`requirements.txt`**: Runtime dependencies\n")
        f.write("- **`requirements-dev.txt`**: Development dependencies\n")
        f.write("- **`pyproject.toml`**: Modern packaging configuration\n")
        f.write("- **`DEPENDENCIES.md`**: Detailed dependency analysis\n\n")
        
        if dependency_analysis and dependency_analysis.get('analysis_errors'):
            f.write("⚠️ **Note**: Some dependencies may have been detected automatically. ")
            f.write("Please review `DEPENDENCIES.md` for any manual adjustments needed.\n\n")
        
        f.write("### Docker Installation\n\n")
        f.write("Coming soon! Docker support will be added in future releases.\n\n")
    
    def _write_configuration(self, f, extension_name: str):
        """Write the configuration section."""
        f.write("## ⚙️ Configuration\n\n")
        
        f.write("### Basic Configuration\n\n")
        f.write("The extension uses Django settings for configuration. Key settings include:\n\n")
        f.write("```python\n")
        f.write("# settings.py or local_settings.py\n\n")
        f.write("# Extension is auto-discovered, but you can manually add it:\n")
        f.write("INSTALLED_APPS = [\n")
        f.write("    # ... other apps\n")
        f.write(f"    'extensions.{extension_name}',\n")
        f.write("]\n\n")
        f.write("# Extension-specific settings\n")
        f.write(f"{extension_name.upper()}_SETTINGS = {{\n")
        f.write("    'FEATURE_ENABLED': True,\n")
        f.write("    'DEFAULT_PAGE_SIZE': 25,\n")
        f.write("    'CACHE_TIMEOUT': 3600,\n")
        f.write("}\n")
        f.write("```\n\n")
        
        f.write("### Database Configuration\n\n")
        f.write("The extension works with SQLite by default but supports PostgreSQL for production:\n\n")
        f.write("```python\n")
        f.write("# For production with PostgreSQL\n")
        f.write("DATABASES = {\n")
        f.write("    'default': {\n")
        f.write("        'ENGINE': 'django.db.backends.postgresql',\n")
        f.write(f"        'NAME': '{extension_name}_db',\n")
        f.write("        'USER': 'your_user',\n")
        f.write("        'PASSWORD': 'your_password',\n")
        f.write("        'HOST': 'localhost',\n")
        f.write("        'PORT': '5432',\n")
        f.write("    }\n")
        f.write("}\n")
        f.write("```\n\n")
        
        f.write("### Environment Variables\n\n")
        f.write("You can use environment variables for configuration:\n\n")
        f.write("```bash\n")
        f.write("# .env file\n")
        f.write("DEBUG=False\n")
        f.write("SECRET_KEY=your-secret-key-here\n")
        f.write(f"{extension_name.upper()}_API_KEY=your-api-key\n")
        f.write("```\n\n")
    
    def _write_usage(self, f, extension_name: str, display_name: str):
        """Write the usage section."""
        f.write("## 📖 Usage\n\n")
        
        f.write(f"### Accessing {display_name}\n\n")
        f.write("Once installed, you can access the extension at:\n\n")
        f.write(f"```\nhttp://localhost:8000/extensions/{extension_name}/\n```\n\n")
        
        f.write("### Main Features\n\n")
        f.write("#### Dashboard\n")
        f.write("The main dashboard provides an overview of all available features and quick access to common tasks.\n\n")
        
        f.write("#### Data Processing\n")
        f.write("1. **Upload Data**: Use the upload interface to import your data files\n")
        f.write("2. **Configure Processing**: Set up processing parameters\n")
        f.write("3. **Run Analysis**: Execute the data analysis workflow\n")
        f.write("4. **Review Results**: View and validate the processing results\n")
        f.write("5. **Export Data**: Download results in your preferred format\n\n")
        
        f.write("#### Report Generation\n")
        f.write("```python\n")
        f.write("# Example: Generating reports programmatically\n")
        f.write(f"from extensions.{extension_name}.services import ReportGenerator\n\n")
        f.write("generator = ReportGenerator()\n")
        f.write("report = generator.create_report(\n")
        f.write("    data_source='your_data.csv',\n")
        f.write("    report_type='compliance',\n")
        f.write("    format='pdf'\n")
        f.write(")\n")
        f.write("```\n\n")
        
        f.write("### Command Line Interface\n\n")
        f.write("The extension provides management commands:\n\n")
        f.write("```bash\n")
        f.write("# Process data from command line\n")
        f.write(f"python manage.py {extension_name}_process --input data.csv --output results.json\n\n")
        f.write("# Generate reports\n")
        f.write(f"python manage.py {extension_name}_report --type monthly --format pdf\n\n")
        f.write("# Import configuration\n")
        f.write(f"python manage.py {extension_name}_import --config config.yaml\n")
        f.write("```\n\n")
    
    def _write_api_documentation(self, f, extension_name: str):
        """Write API documentation section."""
        f.write("## 📡 API Documentation\n\n")
        
        f.write("The extension provides a RESTful API for integration with external systems.\n\n")
        
        f.write("### Authentication\n\n")
        f.write("API endpoints require authentication using Django's built-in authentication system.\n\n")
        f.write("### Endpoints\n\n")
        
        f.write("#### Data Processing\n\n")
        f.write("**POST** `/extensions/{extension_name}/api/process/`\n\n")
        f.write("Process data through the extension's workflow.\n\n")
        f.write("```json\n")
        f.write("{\n")
        f.write('  "data": "base64_encoded_data",\n')
        f.write('  "format": "csv",\n')
        f.write('  "options": {\n')
        f.write('    "validate": true,\n')
        f.write('    "output_format": "json"\n')
        f.write("  }\n")
        f.write("}\n")
        f.write("```\n\n")
        
        f.write("#### Report Generation\n\n")
        f.write("**GET** `/extensions/{extension_name}/api/reports/`\n\n")
        f.write("List available reports.\n\n")
        f.write("**POST** `/extensions/{extension_name}/api/reports/generate/`\n\n")
        f.write("Generate a new report.\n\n")
        
        f.write("#### Status Monitoring\n\n")
        f.write("**GET** `/extensions/{extension_name}/api/status/`\n\n")
        f.write("Get extension status and health information.\n\n")
        
        f.write("### Response Formats\n\n")
        f.write("All API responses follow a consistent format:\n\n")
        f.write("```json\n")
        f.write("{\n")
        f.write('  "success": true,\n')
        f.write('  "data": { /* response data */ },\n')
        f.write('  "message": "Operation completed successfully",\n')
        f.write('  "timestamp": "2024-01-01T12:00:00Z"\n')
        f.write("}\n")
        f.write("```\n\n")
    
    def _write_development(self, f, extension_name: str, repo_url: str):
        """Write development section."""
        f.write("## 🛠️ Development\n\n")
        
        f.write("### Setting up Development Environment\n\n")
        f.write("1. **Fork and clone the repository**:\n")
        f.write("   ```bash\n")
        f.write(f"   git clone {repo_url}.git\n")
        f.write(f"   cd {repo_url.split('/')[-1]}\n")
        f.write("   ```\n\n")
        
        f.write("2. **Create virtual environment**:\n")
        f.write("   ```bash\n")
        f.write("   python -m venv venv\n")
        f.write("   source venv/bin/activate  # On Windows: venv\\Scripts\\activate\n")
        f.write("   ```\n\n")
        
        f.write("3. **Install development dependencies**:\n")
        f.write("   ```bash\n")
        f.write("   pip install -r requirements.txt\n")
        f.write("   pip install pytest pytest-django ruff black\n")
        f.write("   ```\n\n")
        
        f.write("### Running Tests\n\n")
        f.write("```bash\n")
        f.write("# Run all tests\n")
        f.write("pytest\n\n")
        f.write("# Run tests with coverage\n")
        f.write("pytest --cov=extensions/{extension_name}\n\n")
        f.write("# Run specific test file\n")
        f.write("pytest tests/test_views.py\n")
        f.write("```\n\n")
        
        f.write("### Code Quality\n\n")
        f.write("```bash\n")
        f.write("# Lint code\n")
        f.write("ruff check .\n\n")
        f.write("# Format code\n")
        f.write("ruff format .\n\n")
        f.write("# Type checking (if using mypy)\n")
        f.write("mypy extensions/{extension_name}\n")
        f.write("```\n\n")
        
        f.write("### Project Structure\n\n")
        f.write("```\n")
        f.write(f"{extension_name}/\n")
        f.write("├── __init__.py              # Package initialization\n")
        f.write("├── views.py                 # Django views\n")
        f.write("├── urls.py                  # URL configuration\n")
        f.write("├── models.py                # Django models (optional)\n")
        f.write("├── forms.py                 # Django forms (optional)\n")
        f.write("├── templates/               # HTML templates\n")
        f.write("│   └── {extension_name}/\n")
        f.write("│       └── dashboard.html\n")
        f.write("├── static/                  # Static files\n")
        f.write("│   └── {extension_name}/\n")
        f.write("│       ├── css/\n")
        f.write("│       ├── js/\n")
        f.write("│       └── images/\n")
        f.write("├── process_steps/           # Business logic\n")
        f.write("├── management/              # Django commands\n")
        f.write("│   └── commands/\n")
        f.write("├── migrations/              # Database migrations\n")
        f.write("└── tests/                   # Test files\n")
        f.write("```\n\n")
    
    def _write_troubleshooting(self, f, extension_name: str):
        """Write troubleshooting section."""
        f.write("## 🔧 Troubleshooting\n\n")
        
        f.write("### Common Issues\n\n")
        
        f.write("#### Installation Problems\n\n")
        f.write("**Issue**: Extension not found after installation\n")
        f.write("```bash\n")
        f.write("# Solution: Check if extension is in the correct location\n")
        f.write("ls /path/to/bird/extensions/\n")
        f.write("# Restart Django server\n")
        f.write("python manage.py runserver\n")
        f.write("```\n\n")
        
        f.write("**Issue**: Permission denied errors\n")
        f.write("```bash\n")
        f.write("# Solution: Fix file permissions\n")
        f.write("chmod +x install.py\n")
        f.write("chmod -R 755 extensions/{extension_name}\n")
        f.write("```\n\n")
        
        f.write("#### Runtime Errors\n\n")
        f.write("**Issue**: Extension URLs not working\n")
        f.write("- Check that the extension is listed in Django settings\n")
        f.write("- Verify URL patterns are correctly configured\n")
        f.write("- Restart the Django development server\n\n")
        
        f.write("**Issue**: Database migration errors\n")
        f.write("```bash\n")
        f.write("# Reset migrations\n")
        f.write(f"python manage.py migrate {extension_name} zero\n")
        f.write(f"rm extensions/{extension_name}/migrations/0001_initial.py\n")
        f.write(f"python manage.py makemigrations {extension_name}\n")
        f.write("python manage.py migrate\n")
        f.write("```\n\n")
        
        f.write("#### Performance Issues\n\n")
        f.write("**Issue**: Slow page loading\n")
        f.write("- Enable Django's caching framework\n")
        f.write("- Optimize database queries\n")
        f.write("- Use Django Debug Toolbar for profiling\n\n")
        
        f.write("### Getting Help\n\n")
        f.write("1. **Check the logs**: Look at Django logs for error messages\n")
        f.write("2. **Enable debug mode**: Set `DEBUG = True` in settings\n")
        f.write("3. **Use Django shell**: Test components interactively\n")
        f.write("   ```bash\n")
        f.write("   python manage.py shell\n")
        f.write("   ```\n")
        f.write("4. **Contact support**: Create an issue in the repository\n\n")
    
    def _write_contributing(self, f, repo_url: str):
        """Write contributing section."""
        f.write("## 🤝 Contributing\n\n")
        
        f.write("We welcome contributions from the community! Here's how you can help:\n\n")
        
        f.write("### Types of Contributions\n\n")
        f.write("- 🐛 **Bug Reports**: Help us identify and fix issues\n")
        f.write("- ✨ **Feature Requests**: Suggest new functionality\n")
        f.write("- 📝 **Documentation**: Improve or translate documentation\n")
        f.write("- 💻 **Code**: Submit pull requests with improvements\n")
        f.write("- 🧪 **Testing**: Help test new features and releases\n\n")
        
        f.write("### Development Process\n\n")
        f.write("1. **Fork the repository**\n")
        f.write("2. **Create a feature branch**: `git checkout -b feature/amazing-feature`\n")
        f.write("3. **Make your changes**\n")
        f.write("4. **Add tests**: Ensure your changes are tested\n")
        f.write("5. **Update documentation**: Document any new features\n")
        f.write("6. **Commit your changes**: `git commit -m 'Add amazing feature'`\n")
        f.write("7. **Push to branch**: `git push origin feature/amazing-feature`\n")
        f.write("8. **Create Pull Request**\n\n")
        
        f.write("### Coding Standards\n\n")
        f.write("- Follow PEP 8 style guide\n")
        f.write("- Use meaningful variable and function names\n")
        f.write("- Write docstrings for all functions and classes\n")
        f.write("- Add type hints where appropriate\n")
        f.write("- Ensure all tests pass before submitting\n\n")
        
        f.write("### Pull Request Guidelines\n\n")
        f.write("- **Clear description**: Explain what your PR does and why\n")
        f.write("- **Link issues**: Reference related issue numbers\n")
        f.write("- **Small changes**: Keep PRs focused and manageable\n")
        f.write("- **Tests included**: Add tests for new functionality\n")
        f.write("- **Documentation updated**: Update relevant documentation\n\n")
        
        f.write(f"### Reporting Issues\n\n")
        f.write(f"Found a bug? Please create an issue at [{repo_url}/issues]({repo_url}/issues) with:\n\n")
        f.write("- Clear description of the problem\n")
        f.write("- Steps to reproduce the issue\n")
        f.write("- Expected vs actual behavior\n")
        f.write("- Environment details (OS, Python version, etc.)\n")
        f.write("- Relevant error messages or screenshots\n\n")
    
    def _write_changelog(self, f):
        """Write changelog section."""
        f.write("## 📅 Changelog\n\n")
        f.write("All notable changes to this project will be documented in this section.\n\n")
        f.write("### [1.0.0] - 2024-01-01\n\n")
        f.write("#### Added\n")
        f.write("- Initial release of the extension\n")
        f.write("- Core functionality implementation\n")
        f.write("- Integration with BIRD framework\n")
        f.write("- Comprehensive documentation\n")
        f.write("- Automated testing suite\n")
        f.write("- Installation scripts\n\n")
        f.write("#### Changed\n")
        f.write("- N/A (initial release)\n\n")
        f.write("#### Fixed\n")
        f.write("- N/A (initial release)\n\n")
        f.write("For a complete changelog, see [CHANGELOG.md](CHANGELOG.md).\n\n")
    
    def _write_license(self, f):
        """Write license section."""
        f.write("## 📄 License\n\n")
        f.write("This project is licensed under the Eclipse Public License 2.0 - see the [LICENSE](LICENSE) file for details.\n\n")
        f.write("### Eclipse Public License 2.0\n\n")
        f.write("The Eclipse Public License 2.0 (EPL-2.0) is a copyleft license that allows you to:\n\n")
        f.write("- ✅ **Use** the software commercially\n")
        f.write("- ✅ **Modify** the software\n")
        f.write("- ✅ **Distribute** the software\n")
        f.write("- ✅ **Patent use** the software\n")
        f.write("- ✅ **Private use** the software\n\n")
        f.write("**Conditions**:\n")
        f.write("- 📋 **License and copyright notice** must be included\n")
        f.write("- 📋 **Disclose source** of modifications\n")
        f.write("- 📋 **Same license** for modifications\n\n")
        f.write("For more information, visit: https://www.eclipse.org/legal/epl-2.0/\n\n")
    
    def _write_footer(self, f, display_name: str, username: str, platform: str):
        """Write footer section."""
        f.write("---\n\n")
        f.write(f"## 🙏 Acknowledgments\n\n")
        f.write("- **Eclipse Foundation** for the BIRD framework\n")
        f.write("- **Django Community** for the excellent web framework\n")
        f.write("- **Contributors** who help improve this extension\n")
        f.write("- **Users** who provide valuable feedback\n\n")
        
        f.write("## 📞 Support\n\n")
        f.write("- 📖 **Documentation**: Check this README and inline documentation\n")
        f.write("- 🐛 **Issues**: Report bugs via the issue tracker\n")
        f.write("- 💬 **Discussions**: Join community discussions\n")
        f.write("- 📧 **Email**: Contact maintainers for urgent matters\n\n")
        
        f.write("---\n\n")
        f.write(f"<div align=\"center\">\n")
        f.write(f"  <strong>Made with ❤️ for the BIRD Community</strong><br>\n")
        f.write(f"  <sub>Built by {username} • Powered by Django & BIRD</sub>\n")
        f.write(f"</div>\n")
    
    def generate_contributing_guide(self, output_path: Path, repo_url: str) -> Path:
        """Generate a separate CONTRIBUTING.md file."""
        contributing_path = output_path / 'CONTRIBUTING.md'
        
        with open(contributing_path, 'w', encoding='utf-8') as f:
            f.write("# Contributing Guide\n\n")
            f.write("Thank you for your interest in contributing to this BIRD extension!\n\n")
            
            f.write("## Code of Conduct\n\n")
            f.write("This project adheres to the Eclipse Foundation Code of Conduct. ")
            f.write("By participating, you are expected to uphold this code.\n\n")
            
            f.write("## How to Contribute\n\n")
            f.write("### Reporting Bugs\n\n")
            f.write("Before creating bug reports, please check existing issues to avoid duplicates.\n\n")
            f.write("When creating a bug report, please include:\n")
            f.write("- A clear and descriptive title\n")
            f.write("- Detailed steps to reproduce the issue\n")
            f.write("- Expected behavior vs actual behavior\n")
            f.write("- Environment information\n")
            f.write("- Screenshots if applicable\n\n")
            
            f.write("### Suggesting Features\n\n")
            f.write("Feature suggestions are welcome! Please:\n")
            f.write("- Use a clear and descriptive title\n")
            f.write("- Provide a detailed description of the feature\n")
            f.write("- Explain why this feature would be useful\n")
            f.write("- Consider the scope and complexity\n\n")
            
            f.write("### Development Workflow\n\n")
            f.write("1. Fork the repository\n")
            f.write("2. Create your feature branch (`git checkout -b feature/AmazingFeature`)\n")
            f.write("3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)\n")
            f.write("4. Push to the branch (`git push origin feature/AmazingFeature`)\n")
            f.write("5. Open a Pull Request\n\n")
            
            f.write("### Pull Request Process\n\n")
            f.write("1. Ensure your code follows the project's coding standards\n")
            f.write("2. Add tests for new functionality\n")
            f.write("3. Update documentation as needed\n")
            f.write("4. Ensure all tests pass\n")
            f.write("5. Update the CHANGELOG.md with details of changes\n\n")
            
            f.write("### Coding Standards\n\n")
            f.write("- Follow PEP 8 for Python code\n")
            f.write("- Use meaningful names for variables and functions\n")
            f.write("- Write docstrings for all public functions and classes\n")
            f.write("- Add type hints where appropriate\n")
            f.write("- Keep functions small and focused\n")
            f.write("- Write tests for new functionality\n\n")
            
            f.write("### Testing\n\n")
            f.write("Run the test suite before submitting your changes:\n\n")
            f.write("```bash\n")
            f.write("pytest\n")
            f.write("```\n\n")
            
            f.write("Add tests for new features:\n\n")
            f.write("```bash\n")
            f.write("pytest tests/test_your_feature.py\n")
            f.write("```\n\n")
            
            f.write("### Documentation\n\n")
            f.write("- Update docstrings for any changed functions\n")
            f.write("- Update README.md if you change functionality\n")
            f.write("- Add inline comments for complex logic\n")
            f.write("- Update API documentation if applicable\n\n")
            
            f.write("## Questions?\n\n")
            f.write(f"If you have questions, please create an issue at {repo_url}/issues\n")
        
        return contributing_path