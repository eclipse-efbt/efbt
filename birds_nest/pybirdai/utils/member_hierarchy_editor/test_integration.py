import unittest
from django.test import TestCase
from django.test.utils import override_settings
from ...bird_meta_data_model import MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE, DOMAIN
from .django_hierarchy_integration import DjangoMemberHierarchyIntegration
from .django_model_converter import DjangoModelConverter


class HierarchyIntegrationTestCase(TestCase):
    """Test cases for the member hierarchy editor integration"""
    
    def setUp(self):
        """Set up test data"""
        # Create test domain
        self.domain = DOMAIN.objects.create(
            domain_id="TEST_DOMAIN",
            name="Test Domain",
            description="Test domain for hierarchy testing"
        )
        
        # Create test hierarchy
        self.hierarchy = MEMBER_HIERARCHY.objects.create(
            member_hierarchy_id="TEST_HIERARCHY",
            name="Test Hierarchy",
            description="Test hierarchy for testing",
            domain_id=self.domain
        )
        
        # Create test members
        self.member1 = MEMBER.objects.create(
            member_id="MEMBER_001",
            name="Root Member",
            description="Root level member",
            domain_id=self.domain
        )
        
        self.member2 = MEMBER.objects.create(
            member_id="MEMBER_002",
            name="Child Member 1",
            description="First child member",
            domain_id=self.domain
        )
        
        self.member3 = MEMBER.objects.create(
            member_id="MEMBER_003",
            name="Child Member 2",
            description="Second child member",
            domain_id=self.domain
        )
        
        # Create hierarchy nodes
        self.root_node = MEMBER_HIERARCHY_NODE.objects.create(
            member_hierarchy_id=self.hierarchy,
            member_id=self.member1,
            level=1,
            comparator="=",
            operator=""
        )
        
        self.child_node1 = MEMBER_HIERARCHY_NODE.objects.create(
            member_hierarchy_id=self.hierarchy,
            member_id=self.member2,
            parent_member_id=self.member1,
            level=2,
            comparator="",
            operator="+"
        )
        
        self.child_node2 = MEMBER_HIERARCHY_NODE.objects.create(
            member_hierarchy_id=self.hierarchy,
            member_id=self.member3,
            parent_member_id=self.member1,
            level=2,
            comparator="",
            operator="+"
        )
        
        self.integration = DjangoMemberHierarchyIntegration()
        self.converter = DjangoModelConverter()
    
    def test_get_hierarchy_by_id(self):
        """Test converting Django models to visualization format"""
        result = self.integration.get_hierarchy_by_id("TEST_HIERARCHY")
        
        self.assertIn('boxes', result)
        self.assertIn('arrows', result)
        self.assertIn('hierarchy_info', result)
        
        # Should have 3 boxes
        self.assertEqual(len(result['boxes']), 3)
        
        # Should have 2 arrows (child1->root, child2->root)
        self.assertEqual(len(result['arrows']), 2)
        
        # Check hierarchy info
        hierarchy_info = result['hierarchy_info']
        self.assertEqual(hierarchy_info['id'], 'TEST_HIERARCHY')
        self.assertEqual(hierarchy_info['name'], 'Test Hierarchy')
        self.assertEqual(hierarchy_info['domain'], 'TEST_DOMAIN')
    
    def test_save_hierarchy_from_visualization(self):
        """Test saving visualization data back to Django models"""
        visualization_data = {
            'boxes': [
                {
                    'id': 'MEMBER_001',
                    'x': 100,
                    'y': 100,
                    'width': 300,
                    'height': 120,
                    'name': 'Root Member',
                    'text': 'Updated description'
                },
                {
                    'id': 'MEMBER_002',
                    'x': 200,
                    'y': 300,
                    'width': 300,
                    'height': 120,
                    'name': 'Child Member 1',
                    'text': 'Child description'
                }
            ],
            'arrows': [
                {
                    'from': 'MEMBER_002',
                    'to': 'MEMBER_001'
                }
            ]
        }
        
        success = self.integration.save_hierarchy_from_visualization(
            "TEST_HIERARCHY", 
            visualization_data
        )
        
        self.assertTrue(success)
        
        # Verify nodes were recreated correctly
        nodes = MEMBER_HIERARCHY_NODE.objects.filter(
            member_hierarchy_id=self.hierarchy
        ).order_by('level', 'member_id__member_id')
        
        self.assertEqual(nodes.count(), 2)
        
        # Check root node
        root = nodes.filter(level=1).first()
        self.assertEqual(root.member_id.member_id, 'MEMBER_001')
        self.assertEqual(root.comparator, '=')
        self.assertEqual(root.operator, '')
        self.assertIsNone(root.parent_member_id)
        
        # Check child node
        child = nodes.filter(level=2).first()
        self.assertEqual(child.member_id.member_id, 'MEMBER_002')
        self.assertEqual(child.comparator, '')
        self.assertEqual(child.operator, '+')
        self.assertEqual(child.parent_member_id.member_id, 'MEMBER_001')
    
    def test_validate_hierarchy_structure(self):
        """Test hierarchy structure validation"""
        # Valid structure
        valid_data = {
            'boxes': [
                {'id': 'MEMBER_001', 'x': 0, 'y': 0, 'width': 300, 'height': 120, 'name': 'Root', 'text': ''},
                {'id': 'MEMBER_002', 'x': 0, 'y': 200, 'width': 300, 'height': 120, 'name': 'Child', 'text': ''}
            ],
            'arrows': [
                {'from': 'MEMBER_002', 'to': 'MEMBER_001'}
            ]
        }
        
        is_valid, errors = self.converter.validate_hierarchy_structure(valid_data)
        self.assertTrue(is_valid)
        self.assertEqual(len([e for e in errors if e.startswith("Error:")]), 0)
        
        # Invalid structure - circular reference
        circular_data = {
            'boxes': [
                {'id': 'MEMBER_001', 'x': 0, 'y': 0, 'width': 300, 'height': 120, 'name': 'A', 'text': ''},
                {'id': 'MEMBER_002', 'x': 0, 'y': 200, 'width': 300, 'height': 120, 'name': 'B', 'text': ''}
            ],
            'arrows': [
                {'from': 'MEMBER_001', 'to': 'MEMBER_002'},
                {'from': 'MEMBER_002', 'to': 'MEMBER_001'}
            ]
        }
        
        is_valid, errors = self.converter.validate_hierarchy_structure(circular_data)
        self.assertFalse(is_valid)
        self.assertTrue(any('circular' in error.lower() for error in errors))
    
    def test_get_available_hierarchies(self):
        """Test getting list of available hierarchies"""
        hierarchies = self.integration.get_available_hierarchies()
        
        self.assertEqual(len(hierarchies), 1)
        hierarchy = hierarchies[0]
        
        self.assertEqual(hierarchy['member_hierarchy_id'], 'TEST_HIERARCHY')
        self.assertEqual(hierarchy['name'], 'Test Hierarchy')
        self.assertEqual(hierarchy['domain_id'], 'TEST_DOMAIN')
    
    def test_get_domain_members(self):
        """Test getting members for a domain"""
        members = self.integration.get_domain_members('TEST_DOMAIN')
        
        self.assertEqual(len(members), 3)
        member_ids = [m['member_id'] for m in members]
        self.assertIn('MEMBER_001', member_ids)
        self.assertIn('MEMBER_002', member_ids)
        self.assertIn('MEMBER_003', member_ids)
    
    def test_get_hierarchy_statistics(self):
        """Test getting hierarchy statistics"""
        stats = self.converter.get_hierarchy_statistics('TEST_HIERARCHY')
        
        self.assertEqual(stats['total_nodes'], 3)
        self.assertEqual(stats['max_level'], 2)
        self.assertEqual(stats['root_nodes'], 1)
        self.assertEqual(stats['leaf_nodes'], 2)
        self.assertEqual(stats['intermediate_nodes'], 0)
        self.assertEqual(stats['hierarchy_name'], 'Test Hierarchy')
        self.assertEqual(stats['domain'], 'TEST_DOMAIN')


if __name__ == '__main__':
    unittest.main()