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

import os
import csv
from datetime import datetime
from django.apps import apps
from django.db import models
from difflib import get_close_matches

class ConvertLDMToSDDHierarchies:
    """Class for converting LDM hierarchies to SDD hierarchies."""

    def find_closest_member(self, member_id):
        """
        Find the closest matching existing member name.
        
        Args:
            member_id: The member ID to find matches for
            
        Returns:
            tuple: (name, member_id) of closest match, or (None, None) if no matches
        """
        MEMBER = apps.get_model('pybirdai', 'MEMBER')
        
        # Check for exact match with underscores
        member_with_underscores = member_id.replace(' ', '_')
        member = MEMBER.objects.filter(name=member_with_underscores).first()
        if member:
            return (member.name, member.member_id)
            
        # If no underscore match, find closest match
        existing_members = list(MEMBER.objects.values_list('name', flat=True))
        matches = get_close_matches(member_id, existing_members, n=1, cutoff=0.6)
        if matches:
            member = MEMBER.objects.filter(name=matches[0]).first()
            return (member.name, member.member_id)
        return (None, None)

    def check_member_exists(self, member_id):
        """
        Check if a member exists in the MEMBER table.
        
        Args:
            member_id: The member ID to check (with spaces)
            
        Returns:
            tuple: (exists, member_id) where exists is bool and member_id is the matched ID or None
        """
        MEMBER = apps.get_model('pybirdai', 'MEMBER')
        # Check with spaces
        member = MEMBER.objects.filter(name=member_id).first()
        if member:
            return (True, member.member_id)
        # Check with underscores
        member = MEMBER.objects.filter(name=member_id.replace(' ', '_')).first()
        if member:
            return (True, member.member_id)
        return (False, None)

    def get_all_subclasses_and_delegates(self, cls, processed=None, parent=None, level=1):
        """
        Recursively get all subclasses and delegate relationships of a class.
        
        Args:
            cls: The class to get subclasses and delegates for
            processed: Set of already processed classes to avoid cycles
            parent: Parent class for tracking hierarchy
            level: Current level in hierarchy
            
        Returns:
            list: List of tuples (class, parent_class, is_delegate, level)
        """
        if processed is None:
            processed = set()
            
        if cls in processed:
            return []
            
        processed.add(cls)
        result = []
        
        # Get direct subclasses
        for subclass in cls.__subclasses__():
            result.append((subclass, cls, False, level))
            result.extend(self.get_all_subclasses_and_delegates(subclass, processed, cls, level + 1))
            
        # Get delegate relationships
        for field in cls._meta.get_fields():
            if isinstance(field, models.ForeignKey) and field.name.endswith('_delegate'):
                delegate_class = field.related_model
                if delegate_class not in processed:
                    result.append((delegate_class, cls, True, level))
                    result.extend(self.get_all_subclasses_and_delegates(delegate_class, processed, cls, level + 1))
                    
        return result

    def convert_hierarchies(self, context, sdd_context):
        """
        Convert LDM hierarchies to SDD hierarchies.
        
        Args:
            context: The general context containing file paths and settings
            sdd_context: The SDD-specific context containing SDD-related settings
        """
        # Constants for the hierarchy
        MAINTENANCE_AGENCY_ID = "BIRD"
        HIERARCHY_ID = "INSTRMNT_HIERARCHY"
        DOMAIN_ID = "INSTRMNT_DOMAIN"
        VALID_FROM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        VALID_TO = "9999-12-31"

        # Get the INSTRMNT class
        INSTRMNT = apps.get_model('pybirdai', 'INSTRMNT')
        
        # Get all subclasses and delegates recursively
        class_relationships = self.get_all_subclasses_and_delegates(INSTRMNT)
        
        # Track missing members and their suggestions
        missing_members = {}
        
        # Create output directory if it doesn't exist
        output_dir = os.path.join(context.output_directory, 'ldm_to_sdd_hierarchies')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create member_hierarchy.csv
        hierarchy_file = os.path.join(output_dir, 'member_hierarchy.csv')
        with open(hierarchy_file, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow([
                'MAINTENANCE_AGENCY_ID', 
                'MEMBER_HIERARCHY_ID', 
                'CODE', 
                'DOMAIN_ID', 
                'NAME', 
                'DESCRIPTION', 
                'IS_MAIN_HIERARCHY'
            ])
            # Write data
            writer.writerow([
                MAINTENANCE_AGENCY_ID,
                HIERARCHY_ID,
                '1',
                DOMAIN_ID,
                'Instrument type hierarchy',
                'Hierarchical structure of instrument types and delegates',
                'true'
            ])

        # Create member_hierarchy_node.csv
        nodes_file = os.path.join(output_dir, 'member_hierarchy_node.csv')
        with open(nodes_file, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow([
                'MEMBER_HIERARCHY_ID',
                'MEMBER_ID',
                'LEVEL',
                'PARENT_MEMBER_ID',
                'COMPARATOR',
                'OPERATOR',
                'VALID_FROM',
                'VALID_TO'
            ])
            
            # Check root node
            root_member_id = INSTRMNT._meta.verbose_name.replace('_', ' ')
            exists, matched_id = self.check_member_exists(root_member_id)
            if not exists:
                suggestion = self.find_closest_member(root_member_id)
                missing_members[root_member_id] = suggestion
            
            # Write root node
            writer.writerow([
                HIERARCHY_ID,
                matched_id if matched_id else root_member_id,
                1,
                '',
                '=',
                '',
                VALID_FROM,
                VALID_TO
            ])
            
            # Write all nodes (both inheritance and delegate relationships)
            for cls, parent_cls, is_delegate, level in class_relationships:
                member_name = cls._meta.verbose_name.replace('_', ' ')
                parent_member_name = parent_cls._meta.verbose_name.replace('_', ' ')
                
                # Check if members exist and get their IDs
                exists, member_matched_id = self.check_member_exists(member_name)
                if not exists:
                    suggestion = self.find_closest_member(member_name)
                    missing_members[member_name] = suggestion
                
                exists, parent_matched_id = self.check_member_exists(parent_member_name)
                if not exists:
                    suggestion = self.find_closest_member(parent_member_name)
                    missing_members[parent_member_name] = suggestion
                
                writer.writerow([
                    HIERARCHY_ID,
                    member_matched_id if member_matched_id else member_name,
                    level + 1,  # Add 1 since root is level 1
                    parent_matched_id if parent_matched_id else parent_member_name,
                    '=' if not is_delegate else 'D',  # Use 'D' comparator for delegate relationships
                    '',
                    VALID_FROM,
                    VALID_TO
                ])

        # Save missing members information to CSV
        if missing_members:
            missing_members_file = os.path.join(output_dir, 'missing_members.csv')
            with open(missing_members_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Missing Member', 'Match Type', 'Matched Name', 'Matched Member ID'])
                for member, suggestion in sorted(missing_members.items()):
                    if suggestion:
                        name, member_id = suggestion
                        match_type = 'Underscore Match' if name == member.replace(' ', '_') else 'Closest Match'
                        writer.writerow([member, match_type, name, member_id])
                    else:
                        writer.writerow([member, 'No Match', '', ''])

        return f"Created hierarchy files in {output_dir} including both inheritance and delegate relationships" 