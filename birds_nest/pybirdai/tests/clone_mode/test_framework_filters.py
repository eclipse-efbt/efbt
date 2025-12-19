# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Unit tests for FrameworkSubgraphFetcher and framework filtering utilities.

These tests verify that framework filtering correctly isolates data by framework
(FINREP, COREP, ANCRDT) using the CUBE traversal strategy.

Usage:
    python -m pytest pybirdai/tests/clone_mode/test_framework_filters.py -v
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Add the birds_nest directory to the path
BIRDS_NEST_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(BIRDS_NEST_DIR))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

import django
django.setup()


class TestFrameworkSubgraphFetcher:
    """Tests for FrameworkSubgraphFetcher utility class."""

    @patch('pybirdai.views.core.framework_filters.CUBE')
    def test_get_cubes_for_framework(self, mock_cube):
        """Test getting CUBEs filtered by framework."""
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        # Create mock queryset
        mock_queryset = MagicMock()
        mock_cube.objects.filter.return_value = mock_queryset

        result = FrameworkSubgraphFetcher.get_cubes_for_framework('FINREP')

        mock_cube.objects.filter.assert_called_once_with(framework_id='FINREP')
        assert result == mock_queryset

    @patch('pybirdai.views.core.framework_filters.CUBE')
    @patch('pybirdai.views.core.framework_filters.CUBE_STRUCTURE')
    def test_get_cube_structures_for_framework(self, mock_structure, mock_cube):
        """Test getting CUBE_STRUCTUREs for a framework."""
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        # Setup mock chain
        mock_cubes_qs = MagicMock()
        mock_cubes_qs.values_list.return_value.distinct.return_value = ['struct1', 'struct2']
        mock_cube.objects.filter.return_value = mock_cubes_qs

        mock_structures_qs = MagicMock()
        mock_structure.objects.filter.return_value = mock_structures_qs

        result = FrameworkSubgraphFetcher.get_cube_structures_for_framework('FINREP')

        mock_cube.objects.filter.assert_called_once_with(framework_id='FINREP')
        mock_structure.objects.filter.assert_called_once()
        assert result == mock_structures_qs

    @patch('pybirdai.views.core.framework_filters.CUBE')
    @patch('pybirdai.views.core.framework_filters.CUBE_STRUCTURE_ITEM')
    def test_get_cube_structure_items_for_framework(self, mock_item, mock_cube):
        """Test getting CUBE_STRUCTURE_ITEMs for a framework."""
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        # Setup mock chain
        mock_cubes_qs = MagicMock()
        mock_cubes_qs.values_list.return_value.distinct.return_value = ['struct1']
        mock_cube.objects.filter.return_value = mock_cubes_qs

        mock_items_qs = MagicMock()
        mock_item.objects.filter.return_value = mock_items_qs

        result = FrameworkSubgraphFetcher.get_cube_structure_items_for_framework('COREP')

        mock_cube.objects.filter.assert_called_once_with(framework_id='COREP')
        assert result == mock_items_qs


class TestFrameworkFilteringIntegration:
    """Integration tests for framework filtering (requires Django models)."""

    @pytest.fixture
    def setup_test_data(self):
        """Set up test data for integration tests."""
        from pybirdai.models.bird_meta_data_model import (
            FRAMEWORK, DOMAIN, VARIABLE, CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM
        )

        # Note: These would need actual database setup
        # This is a placeholder showing the intended test structure
        yield

        # Cleanup would go here

    @pytest.mark.skip(reason="Requires database setup - use for integration testing")
    def test_framework_isolation_finrep(self, setup_test_data):
        """Test that FINREP filtering returns only FINREP data."""
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        cubes = FrameworkSubgraphFetcher.get_cubes_for_framework('FINREP')

        # All cubes should have framework_id = 'FINREP'
        for cube in cubes:
            assert cube.framework_id == 'FINREP'

    @pytest.mark.skip(reason="Requires database setup - use for integration testing")
    def test_framework_isolation_no_cross_contamination(self, setup_test_data):
        """Test that framework filtering doesn't leak data from other frameworks."""
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        finrep_cubes = set(FrameworkSubgraphFetcher.get_cubes_for_framework('FINREP').values_list('cube_id', flat=True))
        corep_cubes = set(FrameworkSubgraphFetcher.get_cubes_for_framework('COREP').values_list('cube_id', flat=True))

        # There should be no overlap
        assert finrep_cubes.isdisjoint(corep_cubes), "FINREP and COREP cubes should not overlap"


class TestFrameworkFilterEdgeCases:
    """Tests for edge cases in framework filtering."""

    @patch('pybirdai.views.core.framework_filters.CUBE')
    def test_get_cubes_for_nonexistent_framework(self, mock_cube):
        """Test filtering for a framework that doesn't exist."""
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        mock_empty_qs = MagicMock()
        mock_empty_qs.count.return_value = 0
        mock_cube.objects.filter.return_value = mock_empty_qs

        result = FrameworkSubgraphFetcher.get_cubes_for_framework('NONEXISTENT')

        assert result.count() == 0

    @patch('pybirdai.views.core.framework_filters.CUBE')
    def test_get_cubes_for_empty_framework_id(self, mock_cube):
        """Test filtering with empty framework ID."""
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        mock_empty_qs = MagicMock()
        mock_cube.objects.filter.return_value = mock_empty_qs

        result = FrameworkSubgraphFetcher.get_cubes_for_framework('')

        mock_cube.objects.filter.assert_called_once_with(framework_id='')
        assert result == mock_empty_qs


class TestFrameworkFilterPerformance:
    """Tests related to framework filter performance considerations."""

    @patch('pybirdai.views.core.framework_filters.CUBE')
    @patch('pybirdai.views.core.framework_filters.CUBE_STRUCTURE')
    def test_cube_structure_filter_uses_distinct(self, mock_structure, mock_cube):
        """Test that CUBE_STRUCTURE filtering uses distinct() to avoid duplicates."""
        from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

        mock_cubes_qs = MagicMock()
        mock_values_list = MagicMock()
        mock_cubes_qs.values_list.return_value = mock_values_list
        mock_values_list.distinct.return_value = ['struct1']
        mock_cube.objects.filter.return_value = mock_cubes_qs

        FrameworkSubgraphFetcher.get_cube_structures_for_framework('FINREP')

        # Verify distinct() was called
        mock_values_list.distinct.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
