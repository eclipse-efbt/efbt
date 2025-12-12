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
#
# Extracted from workflow_views.py

import logging

from pybirdai.models.workflow_model import (
    WorkflowTaskExecution, WorkflowSession, 
    DPMProcessExecution, AnaCreditProcessExecution
)

logger = logging.getLogger(__name__)

def get_dpm_task_grid(session):
    """Get DPM process status grid"""
    if not session:
        return []

    dpm_steps = [
        (1, 'Extract DPM Metadata'),
        (2, 'Process & Import Selected Tables'),
        (3, 'Create Output Layers'),
        (4, 'Create Transformation Rules'),
        (5, 'Generate Python Code'),
        (6, 'Execute DPM Tests'),
    ]

    grid = []
    for step_number, step_name in dpm_steps:
        try:
            execution = DPMProcessExecution.objects.get(
                session=session,
                step_number=step_number
            )
            status = execution.status
        except DPMProcessExecution.DoesNotExist:
            # Create default pending entry
            execution = DPMProcessExecution.objects.create(
                session=session,
                step_number=step_number,
                step_name=step_name,
                status='pending'
            )
            status = 'pending'

        grid.append({
            'step_number': step_number,
            'step_name': step_name,
            'status': status,
            'started_at': execution.started_at if execution else None,
            'completed_at': execution.completed_at if execution else None,
        })

    return grid


def get_ancrdt_task_grid(session):
    """Get AnaCredit process status grid"""
    if not session:
        return []

    ancrdt_steps = [
        (0, 'Fetch Metadata CSV'),
        (1, 'Import Metadata'),
        (2, 'Create Joins Metadata'),
        (3, 'Create Executable Joins'),
        (4, 'Full Execution with Test Suite'),
        (5, 'Execute Tables'),
    ]

    grid = []
    for step_number, step_name in ancrdt_steps:
        try:
            execution = AnaCreditProcessExecution.objects.get(
                session=session,
                step_number=step_number
            )
            status = execution.status
        except AnaCreditProcessExecution.DoesNotExist:
            # Create default pending entry
            execution = AnaCreditProcessExecution.objects.create(
                session=session,
                step_number=step_number,
                step_name=step_name,
                status='pending'
            )
            status = 'pending'

        grid.append({
            'step_number': step_number,
            'step_name': step_name,
            'status': status,
            'started_at': execution.started_at if execution else None,
            'completed_at': execution.completed_at if execution else None,
        })

    return grid


def get_workflow_progress_summary(session):
    """Get progress summary for all workflows"""
    if not session:
        return {
            'main': {'completed': 0, 'total': 4, 'active': False, 'current': 0},
            'dpm': {'completed': 0, 'total': 3, 'active': False, 'current': 0},
            'ancrdt': {'completed': 0, 'total': 5, 'active': False, 'current': 0},
        }

    # Main workflow progress
    main_completed = WorkflowTaskExecution.objects.filter(
        operation_type='do',
        status='completed'
    ).count()
    main_active = WorkflowTaskExecution.objects.filter(
        operation_type='do',
        status='running'
    ).exists()
    main_current = WorkflowTaskExecution.objects.filter(
        operation_type='do',
        status='running'
    ).first()
    main_current_num = main_current.task_number if main_current else 0

    # DPM workflow progress
    dpm_completed = DPMProcessExecution.objects.filter(
        session=session,
        status='completed'
    ).count()
    dpm_active = DPMProcessExecution.objects.filter(
        session=session,
        status='running'
    ).exists()
    dpm_current = DPMProcessExecution.objects.filter(
        session=session,
        status='running'
    ).first()
    dpm_current_num = dpm_current.step_number if dpm_current else 0

    # AnaCredit workflow progress
    ancrdt_completed = AnaCreditProcessExecution.objects.filter(
        session=session,
        status='completed'
    ).count()
    ancrdt_active = AnaCreditProcessExecution.objects.filter(
        session=session,
        status='running'
    ).exists()
    ancrdt_current = AnaCreditProcessExecution.objects.filter(
        session=session,
        status='running'
    ).first()
    ancrdt_current_num = ancrdt_current.step_number if ancrdt_current else 0

    return {
        'main': {
            'completed': main_completed,
            'total': 4,
            'active': main_active,
            'current': main_current_num,
        },
        'dpm': {
            'completed': dpm_completed,
            'total': 3,
            'active': dpm_active,
            'current': dpm_current_num,
        },
        'ancrdt': {
            'completed': ancrdt_completed,
            'total': 4,
            'active': ancrdt_active,
            'current': ancrdt_current_num,
        },
    }
