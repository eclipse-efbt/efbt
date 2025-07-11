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

class WorkflowTaskExecution(models.Model):
    """Track execution state of workflow tasks"""

    TASK_CHOICES = [
        (1, 'Resource Download'),
        (2, 'Database Creation'),
        (3, 'SMCubes Core Creation'),
        (4, 'SMCubes Transformation Rules Creation'),
        (5, 'Python Transformation Rules Creation'),
        (6, 'Full Execution with Test Suite'),
    ]

    OPERATION_CHOICES = [
        ('do', 'Do'),
        ('review', 'Review'),
        ('compare', 'Compare'),
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
        """Get a 6x3 grid of task statuses"""
        grid = []
        for task_num in range(1, 7):
            task_row = {
                'task_number': task_num,
                'task_name': dict(WorkflowTaskExecution.TASK_CHOICES)[task_num],
                'operations': {}
            }

            for op_type in ['do', 'review', 'compare']:
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
        total_tasks = 6  # 6 tasks total
        completed_do_operations = WorkflowTaskExecution.objects.filter(
            operation_type='do',
            status='completed'
        ).count()

        return int((completed_do_operations / total_tasks) * 100)
