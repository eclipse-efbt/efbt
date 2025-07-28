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

from django import forms
from django.core.exceptions import ValidationError
import re


class AutomodeConfigurationSessionForm(forms.Form):
    """Form for configuring automode data sources and settings (session-based)."""

    DATA_MODEL_CHOICES = [
        ('ELDM', 'ELDM (Logical Data Model)'),
        ('EIL', 'EIL (Input Layer)'),
    ]

    TECHNICAL_EXPORT_SOURCE_CHOICES = [
        ('BIRD_WEBSITE', 'BIRD Website'),
        ('GITHUB', 'GitHub Repository'),
        ('MANUAL_UPLOAD', 'Manual Upload'),
    ]

    CONFIG_FILES_SOURCE_CHOICES = [
        ('MANUAL', 'Manual Upload'),
        ('GITHUB', 'GitHub Repository'),
    ]

    WHEN_TO_STOP_CHOICES = [
        ('RESOURCE_DOWNLOAD', 'Stop after resource download and move to step by step mode'),
        ('DATABASE_CREATION', 'Stop after database creation'),
        ('SMCUBES_RULES', 'Stop after creation of SMCubes generation rules for custom configuration before python generation'),
        ('PYTHON_CODE', 'Use previous customisation and stop after generating Python code'),
        ('FULL_EXECUTION', 'Do everything including creating Python code and running the test suite'),
    ]

    data_model_type = forms.ChoiceField(
        choices=DATA_MODEL_CHOICES,
        initial='ELDM',
        widget=forms.RadioSelect(),
        help_text='Select whether to use ELDM or EIL data model'
    )

    technical_export_source = forms.ChoiceField(
        choices=TECHNICAL_EXPORT_SOURCE_CHOICES,
        initial='BIRD_WEBSITE',
        widget=forms.RadioSelect(),
        help_text='Source for technical export files'
    )

    technical_export_github_url = forms.URLField(
        required=False,
        initial='https://github.com/regcommunity/FreeBIRD',
        help_text='GitHub repository URL for technical export files (when GitHub source is selected)'
    )

    config_files_source = forms.ChoiceField(
        choices=CONFIG_FILES_SOURCE_CHOICES,
        initial='MANUAL',
        widget=forms.RadioSelect(),
        help_text='Source for configuration files (joins, extra variables)'
    )

    config_files_github_url = forms.URLField(
        required=False,
        initial='https://github.com/regcommunity/FreeBIRD',
        help_text='GitHub repository URL for configuration files (when GitHub source is selected)'
    )

    when_to_stop = forms.ChoiceField(
        choices=WHEN_TO_STOP_CHOICES,
        initial='RESOURCE_DOWNLOAD',
        widget=forms.RadioSelect(),
        help_text='Defines how far to take automode processing before stopping'
    )

    enable_lineage_tracking = forms.BooleanField(
        initial=True,
        required=False,
        widget=forms.CheckboxInput(),
        help_text='Enable lineage tracking for transformation and filter generation'
    )

    # Add github_token as a non-model field for security (not stored in database)
    github_token = forms.CharField(
        max_length=255,
        required=False,
        widget=forms.PasswordInput(attrs={
            'class': 'form-input',
            'placeholder': 'ghp_xxxxxxxxxxxxxxxxxxxx (optional for private repos)',
            'style': 'display: none;'
        }),
        help_text='GitHub personal access token for private repositories (not stored in database)'
    )

    def clean_technical_export_github_url(self):
        """Validate technical export GitHub URL."""
        url = self.cleaned_data.get('technical_export_github_url')
        source = self.cleaned_data.get('technical_export_source')

        if source == 'GITHUB':
            if not url:
                raise ValidationError('GitHub URL is required when GitHub is selected as source.')

            if not self._is_valid_github_url(url):
                raise ValidationError('Please enter a valid GitHub repository URL.')

        return url

    def clean_config_files_github_url(self):
        """Validate configuration files GitHub URL."""
        url = self.cleaned_data.get('config_files_github_url')
        source = self.cleaned_data.get('config_files_source')

        if source == 'GITHUB':
            if not url:
                raise ValidationError('GitHub URL is required when GitHub is selected as source.')

            if not self._is_valid_github_url(url):
                raise ValidationError('Please enter a valid GitHub repository URL.')

        return url

    def _is_valid_github_url(self, url):
        """Check if URL is a valid GitHub repository URL."""
        # Allow both with and without .git suffix
        github_pattern = re.compile(
            r'^https://github\.com/[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+(\.git)?/?$'
        )
        return bool(github_pattern.match(url))


class AutomodeConfigurationForm(forms.ModelForm):
    """Form for configuring automode data sources and settings (model-based)."""

    # Add github_token as a non-model field for security (not stored in database)
    github_token = forms.CharField(
        max_length=255,
        required=False,
        widget=forms.PasswordInput(attrs={
            'class': 'form-input',
            'placeholder': 'ghp_xxxxxxxxxxxxxxxxxxxx (optional for private repos)',
            'style': 'display: none;'
        }),
        help_text='GitHub personal access token for private repositories (not stored in database)'
    )

    class Meta:
        from .models.workflow_model import AutomodeConfiguration
        model = AutomodeConfiguration
        fields = [
            'data_model_type',
            'technical_export_source',
            'technical_export_github_url',
            'config_files_source',
            'config_files_github_url',
            'when_to_stop'
        ]
        widgets = {
            'data_model_type': forms.RadioSelect(attrs={
                'class': 'form-radio',
                'onchange': 'updateFormVisibility()'
            }),
            'technical_export_source': forms.RadioSelect(attrs={
                'class': 'form-radio',
                'onchange': 'updateFormVisibility()'
            }),
            'technical_export_github_url': forms.URLInput(attrs={
                'class': 'form-input',
                'placeholder': 'https://github.com/username/repository',
                'style': 'display: none;'
            }),
            'config_files_source': forms.RadioSelect(attrs={
                'class': 'form-radio',
                'onchange': 'updateFormVisibility()'
            }),
            'config_files_github_url': forms.URLInput(attrs={
                'class': 'form-input',
                'placeholder': 'https://github.com/username/repository',
                'style': 'display: none;'
            }),
            'when_to_stop': forms.RadioSelect(attrs={
                'class': 'form-radio'
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Set default GitHub URLs if not provided
        if not self.instance.pk:
            self.fields['technical_export_github_url'].initial = 'https://github.com/regcommunity/FreeBIRD'
            self.fields['config_files_github_url'].initial = 'https://github.com/regcommunity/FreeBIRD'

        # Add CSS classes and help text
        for field_name, field in self.fields.items():
            if hasattr(field.widget, 'attrs'):
                field.widget.attrs.update({'class': 'form-control'})

        # Custom labels and help text
        self.fields['data_model_type'].label = 'Data Model Type'
        self.fields['data_model_type'].help_text = 'Choose whether to use ELDM (Logical Data Model) or EIL (Input Layer)'

        self.fields['technical_export_source'].label = 'Technical Export Source'
        self.fields['technical_export_source'].help_text = 'Choose where to fetch technical export files from'

        self.fields['technical_export_github_url'].label = 'Technical Export GitHub URL'
        self.fields['technical_export_github_url'].help_text = 'GitHub repository URL for technical export files'

        self.fields['config_files_source'].label = 'Configuration Files Source'
        self.fields['config_files_source'].help_text = 'Choose where to fetch configuration files (joins, extra variables) from'

        self.fields['config_files_github_url'].label = 'Configuration Files GitHub URL'
        self.fields['config_files_github_url'].help_text = 'GitHub repository URL for configuration files'

        self.fields['github_token'].label = 'GitHub Personal Access Token'
        self.fields['github_token'].help_text = 'Optional token for accessing private repositories. Create at github.com/settings/tokens'

        self.fields['when_to_stop'].label = 'When to Stop Processing'
        self.fields['when_to_stop'].help_text = 'Choose how far to take automode processing before stopping'

    def clean_technical_export_github_url(self):
        """Validate technical export GitHub URL."""
        url = self.cleaned_data.get('technical_export_github_url')
        source = self.cleaned_data.get('technical_export_source')

        if source == 'GITHUB':
            if not url:
                raise ValidationError('GitHub URL is required when GitHub is selected as source.')

            if not self._is_valid_github_url(url):
                raise ValidationError('Please enter a valid GitHub repository URL.')

        return url

    def clean_config_files_github_url(self):
        """Validate configuration files GitHub URL."""
        url = self.cleaned_data.get('config_files_github_url')
        source = self.cleaned_data.get('config_files_source')

        if source == 'GITHUB':
            if not url:
                raise ValidationError('GitHub URL is required when GitHub is selected as source.')

            if not self._is_valid_github_url(url):
                raise ValidationError('Please enter a valid GitHub repository URL.')

        return url

    def _is_valid_github_url(self, url):
        """Check if URL is a valid GitHub repository URL."""
        # Allow both with and without .git suffix
        github_pattern = re.compile(
            r'^https://github\.com/[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+(\.git)?/?$'
        )
        return bool(github_pattern.match(url))

    def clean(self):
        """Cross-field validation."""
        cleaned_data = super().clean()

        # Additional validation can be added here
        technical_source = cleaned_data.get('technical_export_source')
        config_source = cleaned_data.get('config_files_source')

        # Warn if both sources are GitHub but URLs are different
        if (technical_source == 'GITHUB' and config_source == 'GITHUB' and
            cleaned_data.get('technical_export_github_url') != cleaned_data.get('config_files_github_url')):
            # This is allowed but we could add a warning
            pass

        return cleaned_data


class ResourceDownloadForm(forms.Form):
    """Form for Task 1: Resource Download configuration."""

    DATA_MODEL_CHOICES = [
        ('ELDM', 'ELDM (Logical Data Model)'),
        ('EIL', 'EIL (Input Layer)'),
    ]

    TECHNICAL_EXPORT_SOURCE_CHOICES = [
        ('BIRD_WEBSITE', 'BIRD Website'),
        ('GITHUB', 'GitHub Repository'),
        ('MANUAL', 'Manual Upload'),
    ]

    CONFIGURATION_SOURCE_CHOICES = [
        ('MANUAL', 'Manual Upload'),
        ('GITHUB', 'GitHub Repository'),
    ]

    eldm_or_eil = forms.ChoiceField(
        choices=DATA_MODEL_CHOICES,
        initial='ELDM',
        widget=forms.RadioSelect(),
        label='Data Model Type',
        help_text='Select whether to use ELDM or EIL data model'
    )

    technical_export_source = forms.ChoiceField(
        choices=TECHNICAL_EXPORT_SOURCE_CHOICES,
        initial='BIRD_WEBSITE',
        widget=forms.RadioSelect(),
        label='Technical Export Source',
        help_text='Choose where to fetch technical export files from'
    )

    technical_export_file = forms.FileField(
        required=False,
        widget=forms.ClearableFileInput(attrs={'class': 'form-control'}),
        label='Technical Export File',
        help_text='Upload technical export file (required when Manual Upload is selected)'
    )

    configuration_source = forms.ChoiceField(
        choices=CONFIGURATION_SOURCE_CHOICES,
        initial='MANUAL',
        widget=forms.RadioSelect(),
        label='Configuration Files Source',
        help_text='Choose where to fetch configuration files from'
    )

    configuration_github_url = forms.URLField(
        required=False,
        initial='https://github.com/regcommunity/FreeBIRD',
        widget=forms.URLInput(attrs={'class': 'form-control'}),
        label='Configuration GitHub URL',
        help_text='GitHub repository URL for configuration files'
    )

    configuration_file = forms.FileField(
        required=False,
        widget=forms.ClearableFileInput(attrs={'class': 'form-control'}),
        label='Configuration File',
        help_text='Upload configuration file (required when Manual Upload is selected)'
    )

    github_token = forms.CharField(
        max_length=255,
        required=False,
        widget=forms.PasswordInput(attrs={'class': 'form-control'}),
        label='GitHub Personal Access Token',
        help_text='Optional: For accessing private repositories'
    )

    def clean_technical_export_file(self):
        """Validate technical export file when manual upload is selected."""
        technical_export_file = self.cleaned_data.get('technical_export_file')
        technical_export_source = self.cleaned_data.get('technical_export_source')

        if technical_export_source == 'MANUAL' and not technical_export_file:
            raise ValidationError('Technical export file is required when Manual Upload is selected.')

        return technical_export_file

    def clean_configuration_file(self):
        """Validate configuration file when manual upload is selected."""
        configuration_file = self.cleaned_data.get('configuration_file')
        configuration_source = self.cleaned_data.get('configuration_source')

        if configuration_source == 'MANUAL' and not configuration_file:
            raise ValidationError('Configuration file is required when Manual Upload is selected.')

        return configuration_file

    def clean_configuration_github_url(self):
        """Validate configuration GitHub URL when GitHub source is selected."""
        url = self.cleaned_data.get('configuration_github_url')
        source = self.cleaned_data.get('configuration_source')

        if source == 'GITHUB':
            if not url:
                raise ValidationError('GitHub URL is required when GitHub is selected as source.')

            # Use the same validation as in AutomodeConfigurationForm
            github_pattern = re.compile(
                r'^https://github\.com/[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+(\.git)?/?$'
            )
            if not github_pattern.match(url):
                raise ValidationError('Please enter a valid GitHub repository URL.')

        return url


class SMCubesCoreForm(forms.Form):
    """Form for Task 3: SMCubes Core Creation configuration."""

    delete_database = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Delete Existing Database Before Creation',
        help_text='Check this to clear the database before creating new structures'
    )

    import_input_model = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Create Cube Structures',
        help_text='Generate multidimensional cube structures for reporting'
    )

    generate_templates = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Generate Report Templates',
        help_text='Create report template structures'
    )

    import_hierarchy_analysis = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Import LDM/EIL Hierarchies',
        help_text='Convert hierarchical structures from the imported data model'
    )

    process_semantic = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Process Semantic Integrations',
        help_text='Import and process semantic integration definitions'
    )

    def clean(self):
        """Ensure at least one option is selected."""
        cleaned_data = super().clean()

        # Check if at least one option (excluding delete_database) is selected
        execution_options = [
            cleaned_data.get('import_input_model'),
            cleaned_data.get('generate_templates'),
            cleaned_data.get('import_hierarchy_analysis'),
            cleaned_data.get('process_semantic'),
        ]

        if not any(execution_options):
            raise ValidationError('At least one execution option must be selected.')

        return cleaned_data


class SMCubesRulesForm(forms.Form):
    """Form for Task 4: SMCubes Transformation Rules Creation configuration."""

    # Filter Generation Options
    generate_all_filters = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Generate all available filters',
        help_text='Create filter rules for all available business logic'
    )

    validate_filters = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Validate filter logic',
        help_text='Perform validation checks on generated filters'
    )

    # Join Configuration Options
    auto_detect_joins = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Auto-detect join relationships',
        help_text='Automatically identify and create data relationship joins'
    )

    optimize_joins = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Optimize join execution order',
        help_text='Optimize the order of join operations for better performance'
    )

    # Performance Options
    parallel_processing = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Enable parallel processing',
        help_text='Use parallel processing to improve generation speed'
    )

    generate_indexes = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Generate performance indexes',
        help_text='Create database indexes to improve query performance'
    )

    def clean(self):
        """Ensure at least one generation option is selected."""
        cleaned_data = super().clean()

        # Check if at least one core generation option is selected
        core_options = [
            cleaned_data.get('generate_all_filters'),
            cleaned_data.get('auto_detect_joins'),
        ]

        if not any(core_options):
            raise ValidationError('At least one of the core generation options (filters or joins) must be selected.')

        return cleaned_data


class PythonRulesForm(forms.Form):
    """Form for Task 5: Python Transformation Rules Creation configuration."""

    CODE_STYLE_CHOICES = [
        ('pep8', 'PEP 8 Standard'),
        ('black', 'Black Formatter'),
        ('google', 'Google Style'),
    ]

    # Code Style Options
    code_style = forms.ChoiceField(
        choices=CODE_STYLE_CHOICES,
        initial='pep8',
        widget=forms.Select(attrs={'class': 'form-control'}),
        label='Python Code Style',
        help_text='Choose the coding style standard for generated Python code'
    )

    add_type_hints = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Add type hints (Python 3.6+)',
        help_text='Include type annotations in generated code'
    )

    add_docstrings = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Generate comprehensive docstrings',
        help_text='Add detailed documentation strings to generated functions and classes'
    )

    # Performance Options
    use_pandas = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Use pandas for data operations',
        help_text='Leverage pandas library for efficient data manipulation'
    )

    enable_caching = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Enable result caching',
        help_text='Add caching mechanisms to improve performance'
    )

    parallel_execution = forms.BooleanField(
        required=False,
        initial=False,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Generate parallel execution support',
        help_text='Add support for parallel processing in generated code'
    )

    # Testing Options
    generate_tests = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Generate unit tests',
        help_text='Create unit test files for generated transformation code'
    )

    generate_fixtures = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Generate test fixtures',
        help_text='Create test data fixtures for unit tests'
    )


class FullExecutionForm(forms.Form):
    """Form for Task 6: Full Execution with Test Suite configuration."""

    # Datapoint Selection
    datapoint_id = forms.CharField(
        max_length=255,
        initial='default_datapoint',
        widget=forms.TextInput(attrs={'class': 'form-control'}),
        label='Datapoint ID',
        help_text='Enter the specific datapoint ID to execute'
    )

    validate_results = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Validate execution results',
        help_text='Perform validation checks on execution results'
    )

    # Execution Options
    generate_reports = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Generate execution reports',
        help_text='Create detailed reports of the execution process'
    )

    save_intermediate = forms.BooleanField(
        required=False,
        initial=False,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label='Save intermediate results',
        help_text='Save intermediate processing results for debugging and analysis'
    )

    def clean_datapoint_id(self):
        """Validate datapoint ID format."""
        datapoint_id = self.cleaned_data.get('datapoint_id')

        if not datapoint_id or not datapoint_id.strip():
            raise ValidationError('Datapoint ID is required.')

        # Basic validation - alphanumeric and underscores only
        if not re.match(r'^[a-zA-Z0-9_]+$', datapoint_id.strip()):
            raise ValidationError('Datapoint ID can only contain letters, numbers, and underscores.')

        return datapoint_id.strip()


class AutomodeExecutionForm(forms.Form):
    """Form for executing automode setup with the current configuration."""

    confirm_execution = forms.BooleanField(
        required=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-checkbox'}),
        label='I confirm that I want to execute the automode setup with the current configuration'
    )

    force_refresh = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={'class': 'form-checkbox'}),
        label='Force refresh of all data (re-download files even if they already exist)',
        help_text='Check this to force re-downloading of all files, even if they already exist in the target directories'
    )

    def clean_confirm_execution(self):
        """Ensure user has confirmed execution."""
        confirmed = self.cleaned_data.get('confirm_execution')
        if not confirmed:
            raise ValidationError('You must confirm execution to proceed.')
        return confirmed
