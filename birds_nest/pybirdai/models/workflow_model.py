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
    
    data_model_type = models.CharField(
        max_length=10,
        choices=DATA_MODEL_CHOICES,
        default='ELDM',
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
        default='MANUAL',
        help_text='Source for configuration files (joins, extra variables)'
    )
    
    config_files_github_url = models.URLField(
        blank=True,
        null=True,
        help_text='GitHub repository URL for configuration files (when GitHub source is selected)'
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
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(blank=True, null=True)
    completed_at = models.DateTimeField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    execution_data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    
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