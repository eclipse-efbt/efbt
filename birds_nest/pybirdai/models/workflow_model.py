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
from django.db import models
from django.utils import timezone

# Workflow Models for 6-Task UI

class AutomodeConfiguration(models.Model):
    DATA_MODEL_CHOICES = [
        ('EIL', 'EIL (Input Layer)'),
        ('ELDM', 'ELDM (Logical Data Model)'),
        
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

    TEST_SUITE_SOURCE_CHOICES = [
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

    FRAMEWORK_CHOICES = [
        # Core Reporting Frameworks
        ('FINREP', 'FINREP (Financial Reporting)'),
        ('COREP', 'COREP (Common Reporting)'),
        ('AE', 'AE (Asset Encumbrance)'),
        ('FP', 'FP (Funding Plans)'),
        ('SBP', 'SBP (Supervisory Benchmarking)'),
        # Regulatory Frameworks
        ('REM', 'REM (Remuneration)'),
        ('RES', 'RES (Resolution)'),
        ('PAY', 'PAY (Payments)'),
        ('GSII', 'GSII (Global Systemic Institutions)'),
        ('MREL', 'MREL (MREL and TLAC)'),
        ('IMPRAC', 'IMPRAC (Impracticability of Bail-in)'),
        # Specialized Frameworks
        ('COVID19', 'COVID19 (COVID-19 Moratoria)'),
        ('IF', 'IF (Investment Firms)'),
        ('ESG', 'ESG (Environmental, Social & Governance)'),
        ('IPU', 'IPU (Intermediate Parent Undertaking)'),
        ('PILLAR3', 'PILLAR3 (Pillar 3 Disclosures)'),
        ('IRRBB', 'IRRBB (Interest Rate Risk in Banking Book)'),
        ('DORA', 'DORA (Digital Operational Resilience)'),
        ('FC', 'FC (FICO)'),
        ('MICA', 'MICA (Markets in Crypto-Assets)'),
        # Non-DPM Framework
        ('ANCRDT', 'ANCRDT (Analytical Credit Datasets)'),
    ]

    framework = models.CharField(
        max_length=20,
        choices=FRAMEWORK_CHOICES,
        default='FINREP',
        help_text='Select the framework to process (FINREP, COREP, or ANCRDT)'
    )

    data_model_type = models.CharField(
        max_length=10,
        choices=DATA_MODEL_CHOICES,
        default='EIL',
        help_text='Select whether to use ELDM or EIL data model'
    )
    
    technical_export_source = models.CharField(
        max_length=20,
        choices=TECHNICAL_EXPORT_SOURCE_CHOICES,
        default='BIRD_WEBSITE',
        help_text='Source for technical export files'
    )
    
    technical_export_github_url = models.URLField(
        blank=True,
        null=True,
        help_text='GitHub repository URL for technical export files (when GitHub source is selected)'
    )
    
    config_files_source = models.CharField(
        max_length=20,
        choices=CONFIG_FILES_SOURCE_CHOICES,
        default='GITHUB',
        help_text='Source for configuration files (joins, extra variables) - always uses BIRD Content Repository'
    )
    
    config_files_github_url = models.URLField(
        blank=True,
        null=True,
        help_text='GitHub repository URL for configuration files (when GitHub source is selected)'
    )

    test_suite_source = models.CharField(
        max_length=20,
        choices=TEST_SUITE_SOURCE_CHOICES,
        default='GITHUB',
        help_text='Source for test suite files - always uses GitHub repository'
    )

    test_suite_github_url = models.URLField(
        blank=True,
        null=True,
        help_text='GitHub repository URL for test suite files (when GitHub source is selected)'
    )

    bird_content_branch = models.CharField(
        max_length=100,
        blank=True,
        default='main',
        help_text='Branch name for BIRD content repository (default: main)'
    )

    test_suite_branch = models.CharField(
        max_length=100,
        blank=True,
        default='main',
        help_text='Branch name for test suite repository (default: main)'
    )

    # Per-pipeline GitHub URLs for framework-specific imports
    pipeline_url_main = models.URLField(
        blank=True,
        default='https://github.com/regcommunity/FreeBIRD_IL_66',
        help_text='GitHub URL for Main/FINREP workflow (default: FreeBIRD_IL_66)'
    )

    pipeline_url_ancrdt = models.URLField(
        blank=True,
        default='https://github.com/regcommunity/FreeBIRD_ANCRDT',
        help_text='GitHub URL for AnaCredit/ANCRDT workflow'
    )

    pipeline_url_dpm = models.URLField(
        blank=True,
        default='https://github.com/regcommunity/FreeBIRD_COREP',
        help_text='GitHub URL for DPM/COREP workflow'
    )

    # Per-pipeline Test Suite URLs
    test_suite_url_main = models.URLField(
        blank=True,
        default='',
        help_text='GitHub URL for Main/FINREP test suite'
    )

    test_suite_url_ancrdt = models.URLField(
        blank=True,
        default='https://github.com/benjamin-arfa/bird-ancrdt-test-suite',
        help_text='GitHub URL for AnaCredit test suite'
    )

    test_suite_url_dpm = models.URLField(
        blank=True,
        default='',
        help_text='GitHub URL for DPM/COREP test suite'
    )

    when_to_stop = models.CharField(
        max_length=20,
        choices=WHEN_TO_STOP_CHOICES,
        default='RESOURCE_DOWNLOAD',
        help_text='Defines how far to take automode processing before stopping'
    )
    
    # Note: github_token is intentionally NOT stored in database for security reasons
    # The token should be provided at runtime and handled in memory only
    
    is_active = models.BooleanField(
        default=True,
        help_text='Whether this configuration is currently active'
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Automode Configuration"
        verbose_name_plural = "Automode Configurations"
        ordering = ['-updated_at']
    
    def __str__(self):
        return f"Automode Config ({self.data_model_type}) - {self.created_at.strftime('%Y-%m-%d %H:%M')}"
    
    def clean(self):
        from django.core.exceptions import ValidationError
        
        # Validate GitHub URLs are provided when GitHub source is selected
        if self.technical_export_source == 'GITHUB' and not self.technical_export_github_url:
            raise ValidationError({
                'technical_export_github_url': 'GitHub URL is required when GitHub is selected as technical export source.'
            })
        
        if self.config_files_source == 'GITHUB' and not self.config_files_github_url:
            raise ValidationError({
                'config_files_github_url': 'GitHub URL is required when GitHub is selected as config files source.'
            })

        if self.test_suite_source == 'GITHUB' and not self.test_suite_github_url:
            raise ValidationError({
                'test_suite_github_url': 'GitHub URL is required when GitHub is selected as test suite source.'
            })
    
    @classmethod
    def get_active_configuration(cls):
        """Get the currently active configuration, or create a default one if none exists."""
        try:
            return cls.objects.filter(is_active=True).first()
        except cls.DoesNotExist:
            return cls.objects.create()
    
    def save(self, *args, **kwargs):
        # Ensure only one configuration is active at a time
        if self.is_active:
            AutomodeConfiguration.objects.filter(is_active=True).update(is_active=False)
        super().save(*args, **kwargs)

class WorkflowTaskExecution(models.Model):
    """Track execution state of workflow tasks"""
    
    TASK_CHOICES = [
        (1, 'SMCubes Core Creation'),
        (2, 'SMCubes Transformation Rules Creation'),
        (3, 'Python Transformation Rules Creation'),
        (4, 'Full Execution with Test Suite'),
    ]
    
    OPERATION_CHOICES = [
        ('do', 'Do'),
        ('review', 'Review'),
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('invalidated', 'Invalidated'),
        ('paused', 'Paused'),
    ]
    
    task_number = models.IntegerField(choices=TASK_CHOICES)
    subtask_name = models.CharField(max_length=100, blank=True, null=True)
    operation_type = models.CharField(max_length=20, choices=OPERATION_CHOICES)
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(blank=True, null=True)
    completed_at = models.DateTimeField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    execution_data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    # Framework association - auto-detected as FINREP for main workflow
    framework_id = models.CharField(
        max_length=100,
        default='FINREP',
        help_text="Framework this execution belongs to (auto-detected: FINREP for main workflow)",
        verbose_name="Framework"
    )

    # New fields for enhanced workflow functionality
    substep_results = models.JSONField(
        default=dict, 
        blank=True,
        help_text="Detailed execution results for each substep",
        verbose_name="Substep Results"
    )
    
    validation_messages = models.TextField(
        blank=True, 
        null=True,
        help_text="Validation results and warnings from the execution",
        verbose_name="Validation Messages"
    )
    
    comparison_data = models.JSONField(
        default=dict, 
        blank=True,
        help_text="Data comparing this execution with previous versions",
        verbose_name="Comparison Data"
    )
    
    REVIEW_STATUS_CHOICES = [
        ('not_reviewed', 'Not Reviewed'),
        ('in_review', 'In Review'),
        ('approved', 'Approved'),
        ('rejected', 'Rejected'),
        ('needs_revision', 'Needs Revision'),
    ]
    
    review_status = models.CharField(
        max_length=20, 
        choices=REVIEW_STATUS_CHOICES, 
        default='not_reviewed',
        help_text="Current review status of this execution",
        verbose_name="Review Status"
    )
    
    review_comments = models.TextField(
        blank=True, 
        null=True,
        help_text="Comments from the review process",
        verbose_name="Review Comments"
    )
    
    progress_percentage = models.IntegerField(
        default=0,
        help_text="Progress percentage (0-100) of the current execution",
        verbose_name="Progress Percentage"
    )
    
    error_details = models.TextField(
        blank=True, 
        null=True,
        help_text="Detailed error information including stack traces",
        verbose_name="Error Details"
    )
    
    RECOVERY_ACTION_CHOICES = [
        ('none', 'No Recovery Action'),
        ('retry', 'Retry Operation'),
        ('skip', 'Skip and Continue'),
        ('manual', 'Manual Intervention Required'),
        ('rollback', 'Rollback Changes'),
    ]
    
    recovery_action = models.CharField(
        max_length=20, 
        choices=RECOVERY_ACTION_CHOICES, 
        default='none',
        help_text="Recommended recovery action if execution failed",
        verbose_name="Recovery Action"
    )
    
    class Meta:
        verbose_name = "WorkflowTaskExecution"
        verbose_name_plural = "WorkflowTaskExecutions"
        ordering = ['task_number', 'created_at']
        unique_together = [['task_number', 'operation_type']]
    
    def __str__(self):
        return f"Task {self.task_number} - {self.get_operation_type_display()}: {self.status}"
    
    def invalidate_downstream_tasks(self):
        """Invalidate all tasks that depend on this one"""
        WorkflowTaskExecution.objects.filter(
            task_number__gt=self.task_number
        ).update(status='invalidated')
    
    def can_execute(self):
        """Check if this task can be executed based on dependencies"""
        if self.task_number == 1:
            return True
        
        # Check if previous task is completed
        previous_tasks = WorkflowTaskExecution.objects.filter(
            task_number__lt=self.task_number,
            operation_type='do'
        )
        
        for task in previous_tasks:
            if task.status != 'completed':
                return False
        
        return True
    
    @classmethod
    def get_latest_execution(cls, task_number, operation_type='do'):
        """Get the latest execution for a specific task and operation"""
        try:
            return cls.objects.filter(
                task_number=task_number,
                operation_type=operation_type
            ).latest('created_at')
        except cls.DoesNotExist:
            return None
    
    def mark_as_reviewed(self, review_status, comments=None):
        """Mark this execution as reviewed with given status and comments"""
        self.review_status = review_status
        if comments:
            self.review_comments = comments
        self.save(update_fields=['review_status', 'review_comments'])
    
    def update_progress(self, percentage, substep_name=None, substep_data=None):
        """Update the progress percentage and optionally add substep data"""
        self.progress_percentage = max(0, min(100, percentage))
        
        if substep_name and substep_data is not None:
            if not self.substep_results:
                self.substep_results = {}
            self.substep_results[substep_name] = {
                'data': substep_data,
                'timestamp': timezone.now().isoformat(),
                'progress': percentage
            }
        
        self.save(update_fields=['progress_percentage', 'substep_results'])
    
    def add_validation_message(self, message_type, message, details=None):
        """Add a validation message to the execution"""
        timestamp = timezone.now().isoformat()
        new_message = f"[{timestamp}] [{message_type.upper()}] {message}"
        if details:
            new_message += f"\nDetails: {details}"
        
        if self.validation_messages:
            self.validation_messages += f"\n\n{new_message}"
        else:
            self.validation_messages = new_message
        
        self.save(update_fields=['validation_messages'])
    
    def set_comparison_data(self, comparison_type, data):
        """Set comparison data for this execution"""
        if not self.comparison_data:
            self.comparison_data = {}
        
        self.comparison_data[comparison_type] = {
            'data': data,
            'timestamp': timezone.now().isoformat()
        }
        
        self.save(update_fields=['comparison_data'])
    
    def handle_error(self, error_message, error_details=None, recovery_action='none'):
        """Handle execution error with detailed information and recovery action"""
        self.status = 'failed'
        self.error_message = error_message
        self.error_details = error_details
        self.recovery_action = recovery_action
        self.completed_at = timezone.now()
        
        self.save(update_fields=[
            'status', 'error_message', 'error_details', 
            'recovery_action', 'completed_at'
        ])
    
    def start_execution(self):
        """Mark execution as started"""
        self.status = 'running'
        self.started_at = timezone.now()
        self.progress_percentage = 0
        
        self.save(update_fields=['status', 'started_at', 'progress_percentage'])
    
    def complete_execution(self, final_data=None):
        """Mark execution as completed"""
        self.status = 'completed'
        self.completed_at = timezone.now()
        self.progress_percentage = 100
        
        if final_data:
            self.execution_data.update(final_data)
        
        self.save(update_fields=[
            'status', 'completed_at', 'progress_percentage', 'execution_data'
        ])


class WorkflowTaskDependency(models.Model):
    """Define dependencies between workflow tasks"""
    
    DEPENDENCY_TYPES = [
        ('sequential', 'Sequential'),
        ('optional', 'Optional'),
        ('conditional', 'Conditional'),
    ]
    
    task_number = models.IntegerField()
    depends_on_task = models.IntegerField()
    dependency_type = models.CharField(max_length=50, choices=DEPENDENCY_TYPES, default='sequential')
    
    class Meta:
        verbose_name = "WorkflowTaskDependency"
        verbose_name_plural = "WorkflowTaskDependencies"
        unique_together = [['task_number', 'depends_on_task']]
    
    def __str__(self):
        return f"Task {self.task_number} depends on Task {self.depends_on_task}"


class WorkflowSession(models.Model):
    """Track overall workflow session state"""
    
    session_id = models.CharField(max_length=100, unique=True)
    configuration = models.JSONField(default=dict)
    current_task = models.IntegerField(default=1)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    # Framework association - determines which framework this workflow session operates on
    framework_id = models.CharField(
        max_length=100,
        default='FINREP',
        help_text="Framework this session belongs to (FINREP, COREP, or ANCRDT)",
        verbose_name="Framework"
    )

    class Meta:
        verbose_name = "WorkflowSession"
        verbose_name_plural = "WorkflowSessions"
    
    def get_task_status_grid(self):
        """Get a 4x3 grid of task statuses"""
        grid = []
        for task_num in range(1, 5):
            task_row = {
                'task_number': task_num,
                'task_name': dict(WorkflowTaskExecution.TASK_CHOICES)[task_num],
                'operations': {}
            }
            
            for op_type in ['do', 'review']:
                try:
                    execution = WorkflowTaskExecution.objects.get(
                        task_number=task_num,
                        operation_type=op_type
                    )
                    task_row['operations'][op_type] = {
                        'status': execution.status,
                        'started_at': execution.started_at,
                        'completed_at': execution.completed_at,
                        'error_message': execution.error_message
                    }
                except WorkflowTaskExecution.DoesNotExist:
                    task_row['operations'][op_type] = {
                        'status': 'pending',
                        'started_at': None,
                        'completed_at': None,
                        'error_message': None
                    }
            
            grid.append(task_row)
        
        return grid
    
    def get_progress_percentage(self):
        """Calculate overall progress percentage based on completed 'do' operations only"""
        total_tasks = 4  # 4 tasks total
        completed_do_operations = WorkflowTaskExecution.objects.filter(
            operation_type='do',
            status='completed'
        ).count()

        return int((completed_do_operations / total_tasks) * 100)


class DPMProcessExecution(models.Model):
    """Track DPM process execution status"""

    # EBA Source: 6 steps (download from EBA website)
    EBA_STEP_CHOICES = [
        (1, 'Extract DPM Metadata'),
        (2, 'Process & Import Selected Tables'),
        (3, 'Create Output Layers'),
        (4, 'Create Transformation Rules'),
        (5, 'Generate Python Code'),
        (6, 'Execute DPM Tests'),
    ]

    # GitHub Source: 4 steps (import from regcommunity packages)
    GITHUB_STEP_CHOICES = [
        (1, 'Import Data'),
        (2, 'Generate Structure Links'),
        (3, 'Generate Executable Code'),
        (4, 'Run Tests'),
    ]

    # Keep STEP_CHOICES for backwards compatibility (defaults to EBA steps)
    STEP_CHOICES = EBA_STEP_CHOICES

    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    SOURCE_TYPE_CHOICES = [
        ('eba', 'EBA Website'),
        ('github', 'GitHub Package'),
    ]

    session = models.ForeignKey(WorkflowSession, on_delete=models.CASCADE, related_name='dpm_executions')
    step_number = models.IntegerField(choices=STEP_CHOICES)
    step_name = models.CharField(max_length=255)
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(blank=True, null=True)
    completed_at = models.DateTimeField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    execution_data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    # Framework association - user-selected framework(s) for DPM workflow
    # Primary framework for this execution (used for output layer creation and tests)
    framework_id = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        help_text="Primary framework for this execution (FINREP, COREP, etc.)",
        verbose_name="Framework"
    )

    # List of frameworks selected for import steps (allows multiple)
    # Example: ['FINREP', 'COREP'] for importing both frameworks
    selected_frameworks = models.JSONField(
        default=list,
        blank=True,
        help_text="List of frameworks selected for import (e.g., ['FINREP', 'COREP'])",
        verbose_name="Selected Frameworks"
    )

    # Table selection for DPM import (Step 1)
    # List of table_ids selected for import (e.g., ['EBA_FINREP_F_01_01_2_8', 'EBA_COREP_C_01_00_2_8'])
    selected_tables = models.JSONField(
        default=list,
        blank=True,
        help_text="List of table IDs selected for import (filters ordinate explosion)",
        verbose_name="Selected Tables"
    )

    # Saved presets for table selections
    # Example: {'balance_sheet': ['EBA_FINREP_F_01_01_2_8', ...], 'income': [...]}
    table_selection_presets = models.JSONField(
        default=dict,
        blank=True,
        help_text="Saved presets for table selections (name -> list of table_ids)",
        verbose_name="Table Selection Presets"
    )

    # Fields for tracking substeps and progress
    substep_results = models.JSONField(
        default=dict,
        blank=True,
        help_text="Detailed execution results for each substep",
        verbose_name="Substep Results"
    )

    progress_percentage = models.IntegerField(
        default=0,
        help_text="Progress percentage (0-100) of the current execution",
        verbose_name="Progress Percentage"
    )

    # Source type: 'eba' (EBA website) or 'github' (GitHub package)
    source_type = models.CharField(
        max_length=20,
        choices=SOURCE_TYPE_CHOICES,
        default='eba',
        help_text="Data source: 'eba' for EBA website, 'github' for GitHub package",
        verbose_name="Source Type"
    )

    # GitHub package URL (when source_type is 'github')
    github_package_url = models.URLField(
        blank=True,
        null=True,
        help_text="GitHub repository URL for the DPM package (e.g., https://github.com/regcommunity/FreeBIRD_IL_66_C07)",
        verbose_name="GitHub Package URL"
    )

    # GitHub branch to use
    github_branch = models.CharField(
        max_length=100,
        blank=True,
        default='main',
        help_text="Branch name for GitHub repository (default: main)",
        verbose_name="GitHub Branch"
    )

    # GitHub step statuses for the 4-step flow (when source_type is 'github')
    github_step_statuses = models.JSONField(
        default=dict,
        blank=True,
        help_text="Status tracking for GitHub-based 4-step workflow",
        verbose_name="GitHub Step Statuses"
    )

    class Meta:
        verbose_name = "DPM Process Execution"
        verbose_name_plural = "DPM Process Executions"
        ordering = ['step_number', 'created_at']
        unique_together = [['session', 'step_number', 'source_type']]

    def __str__(self):
        return f"DPM Step {self.step_number} - {self.step_name}: {self.status}"

    def start_execution(self):
        """Mark execution as started"""
        self.status = 'running'
        self.started_at = timezone.now()
        self.completed_at = None  # Clear previous completion time
        self.error_message = None  # Clear previous errors
        self.save(update_fields=['status', 'started_at', 'completed_at', 'error_message'])

    def complete_execution(self, final_data=None):
        """Mark execution as completed"""
        self.status = 'completed'
        self.completed_at = timezone.now()

        if final_data:
            self.execution_data.update(final_data)

        self.save(update_fields=['status', 'completed_at', 'execution_data'])

    def handle_error(self, error_message):
        """Handle execution error"""
        self.status = 'failed'
        self.error_message = error_message
        self.completed_at = timezone.now()

        self.save(update_fields=['status', 'error_message', 'completed_at'])

    def get_step_choices(self):
        """Get step choices based on source type"""
        if self.source_type == 'github':
            return self.GITHUB_STEP_CHOICES
        return self.EBA_STEP_CHOICES

    def get_step_name_for_source(self, step_number):
        """Get step name for given step number based on source type"""
        choices = self.get_step_choices()
        for num, name in choices:
            if num == step_number:
                return name
        return f'Step {step_number}'

    def get_total_steps(self):
        """Get total number of steps based on source type"""
        if self.source_type == 'github':
            return 4
        return 6

    @classmethod
    def get_or_create_for_github(cls, session, step_number, github_url=None, branch='main'):
        """Get or create a DPM execution record for GitHub source"""
        execution, created = cls.objects.get_or_create(
            session=session,
            step_number=step_number,
            source_type='github',
            defaults={
                'step_name': dict(cls.GITHUB_STEP_CHOICES).get(step_number, f'Step {step_number}'),
                'status': 'pending',
                'github_package_url': github_url,
                'github_branch': branch,
            }
        )
        return execution, created


class AnaCreditProcessExecution(models.Model):
    """Track AnaCredit process execution status"""

    STEP_CHOICES = [
        (1, 'Import Metadata'),
        (2, 'Create Joins Metadata'),
        (3, 'Create Executable Joins'),
        (4, 'Run Test Suite'),
    ]

    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    session = models.ForeignKey(WorkflowSession, on_delete=models.CASCADE, related_name='anacredit_executions')
    step_number = models.IntegerField(choices=STEP_CHOICES)
    step_name = models.CharField(max_length=255)
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(blank=True, null=True)
    completed_at = models.DateTimeField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    execution_data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    # Framework association - auto-detected as ANCRDT for AnaCredit workflow
    framework_id = models.CharField(
        max_length=100,
        default='ANCRDT',
        help_text="Framework this execution belongs to (auto-detected: ANCRDT for AnaCredit workflow)",
        verbose_name="Framework"
    )

    # Fields for tracking substeps and progress (matching DPM workflow)
    substep_results = models.JSONField(
        default=dict,
        blank=True,
        help_text="Detailed execution results for each substep",
        verbose_name="Substep Results"
    )

    progress_percentage = models.IntegerField(
        default=0,
        help_text="Progress percentage (0-100) of the current execution",
        verbose_name="Progress Percentage"
    )

    class Meta:
        verbose_name = "AnaCredit Process Execution"
        verbose_name_plural = "AnaCredit Process Executions"
        ordering = ['step_number', 'created_at']
        unique_together = [['session', 'step_number']]

    def __str__(self):
        return f"AnaCredit Step {self.step_number} - {self.step_name}: {self.status}"

    def start_execution(self):
        """Mark execution as started"""
        self.status = 'running'
        self.started_at = timezone.now()
        self.completed_at = None  # Clear previous completion time
        self.error_message = None  # Clear previous errors
        self.save(update_fields=['status', 'started_at', 'completed_at', 'error_message'])

    def complete_execution(self, final_data=None):
        """Mark execution as completed"""
        self.status = 'completed'
        self.completed_at = timezone.now()

        if final_data:
            self.execution_data.update(final_data)

        self.save(update_fields=['status', 'completed_at', 'execution_data'])

    def handle_error(self, error_message):
        """Handle execution error"""
        self.status = 'failed'
        self.error_message = error_message
        self.completed_at = timezone.now()

        self.save(update_fields=['status', 'error_message', 'completed_at'])

    def update_progress(self, percentage, substep_name=None, substep_data=None):
        """Update the progress percentage and optionally add substep data"""
        self.progress_percentage = max(0, min(100, percentage))

        if substep_name and substep_data is not None:
            if not self.substep_results:
                self.substep_results = {}
            self.substep_results[substep_name] = {
                'data': substep_data,
                'timestamp': timezone.now().isoformat(),
                'progress': percentage
            }

        self.save(update_fields=['progress_percentage', 'substep_results'])


class FrameworkTestSuite(models.Model):
    """Track framework-specific test suites.

    Each framework (FINREP, COREP, ANCRDT) has its own test suite with
    specific configuration and test data. This model manages the test
    configurations for each framework.
    """

    framework_id = models.CharField(
        max_length=100,
        unique=True,
        help_text="Framework identifier (FINREP, COREP, or ANCRDT)",
        verbose_name="Framework"
    )

    test_suite_name = models.CharField(
        max_length=255,
        help_text="Descriptive name for the test suite",
        verbose_name="Test Suite Name"
    )

    test_config_path = models.CharField(
        max_length=500,
        help_text="Path to the test configuration JSON file (relative to project root)",
        verbose_name="Test Configuration Path"
    )

    is_active = models.BooleanField(
        default=True,
        help_text="Whether this test suite is currently active and should be executed",
        verbose_name="Is Active"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    description = models.TextField(
        blank=True,
        null=True,
        help_text="Detailed description of what this test suite covers",
        verbose_name="Description"
    )

    last_execution_date = models.DateTimeField(
        blank=True,
        null=True,
        help_text="When this test suite was last executed",
        verbose_name="Last Execution Date"
    )

    last_execution_status = models.CharField(
        max_length=50,
        choices=[
            ('not_run', 'Not Run'),
            ('passed', 'Passed'),
            ('failed', 'Failed'),
            ('partial', 'Partially Passed'),
        ],
        default='not_run',
        help_text="Status of the last test execution",
        verbose_name="Last Execution Status"
    )

    test_count = models.IntegerField(
        default=0,
        help_text="Number of tests in this suite",
        verbose_name="Test Count"
    )

    class Meta:
        verbose_name = "Framework Test Suite"
        verbose_name_plural = "Framework Test Suites"
        ordering = ['framework_id']

    def __str__(self):
        return f"{self.framework_id} Test Suite - {self.test_suite_name}"

    def mark_executed(self, status, test_count=None):
        """Mark the test suite as executed with given status"""
        self.last_execution_date = timezone.now()
        self.last_execution_status = status
        if test_count is not None:
            self.test_count = test_count
        self.save(update_fields=['last_execution_date', 'last_execution_status', 'test_count'])

    @classmethod
    def get_test_suite_for_framework(cls, framework_id):
        """Get the active test suite for a specific framework"""
        try:
            return cls.objects.get(framework_id=framework_id, is_active=True)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_default_test_suites(cls):
        """Create default test suites for all frameworks using auto-discovery.

        This method scans the tests/ directory for test suites and creates
        database entries for each discovered suite. Test suites are matched
        by the test_type field in their configuration_file_tests.json.
        """
        from pybirdai.utils.test_discovery import discover_all_test_suites

        # Framework descriptions for UI display
        framework_descriptions = {
            'BIRD_EIL': 'Test suite for BIRD EIL (Input Layer) data model',
            'BIRD_ELDM': 'Test suite for BIRD ELDM (Logical Data Model)',
            'FINREP': 'Test suite for FINREP (Financial Reporting) framework',
            'COREP': 'Test suite for COREP (Common Reporting) framework / DPM process',
            'ANCRDT': 'Test suite for AnaCredit (Analytical Credit Datasets) framework',
        }

        # Framework display names
        framework_names = {
            'BIRD_EIL': 'BIRD EIL Test Suite',
            'BIRD_ELDM': 'BIRD ELDM Test Suite',
            'FINREP': 'FINREP Test Suite',
            'COREP': 'COREP Test Suite',
            'ANCRDT': 'AnaCredit Test Suite',
        }

        # Discover all test suites from the tests/ directory
        discovered_suites = discover_all_test_suites()

        for framework_id, suite_info in discovered_suites.items():
            if suite_info:  # Only create entry if suite was discovered
                cls.objects.get_or_create(
                    framework_id=framework_id,
                    defaults={
                        'framework_id': framework_id,
                        'test_suite_name': framework_names.get(framework_id, f'{framework_id} Test Suite'),
                        'test_config_path': suite_info['config_path'],
                        'description': framework_descriptions.get(framework_id, f'Test suite for {framework_id}')
                    }
                )