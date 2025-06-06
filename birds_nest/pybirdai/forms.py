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
from .bird_meta_data_model import AutomodeConfiguration


class AutomodeConfigurationForm(forms.ModelForm):
    """Form for configuring automode data sources and settings."""
    
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