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