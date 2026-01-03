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

"""
Management command to test the COREP member_id fix in combination_items.

Usage:
    python manage.py test_corep_member_fix --check          # Check current state
    python manage.py test_corep_member_fix --test-lookup    # Test member lookup logic
    python manage.py test_corep_member_fix --regenerate     # Regenerate combinations for a sample table
    python manage.py test_corep_member_fix --all            # Run all tests
"""

from django.core.management.base import BaseCommand
from pybirdai.models.bird_meta_data_model import (
    CUBE, CUBE_TO_COMBINATION, COMBINATION, COMBINATION_ITEM,
    CUBE_STRUCTURE_ITEM, TABLE_CELL, CELL_POSITION, ORDINATE_ITEM,
    VARIABLE_MAPPING_ITEM, MEMBER_MAPPING_ITEM, TABLE
)


class Command(BaseCommand):
    help = 'Test the COREP member_id fix in combination_items'

    def add_arguments(self, parser):
        parser.add_argument(
            '--check',
            action='store_true',
            help='Check current state of COREP combination_items'
        )
        parser.add_argument(
            '--test-lookup',
            action='store_true',
            help='Test the member lookup logic for a sample cell'
        )
        parser.add_argument(
            '--regenerate',
            action='store_true',
            help='Regenerate combinations for a sample COREP table'
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Run all tests'
        )

    def handle(self, *args, **options):
        if options['all']:
            options['check'] = True
            options['test_lookup'] = True

        if options['check']:
            self.check_current_state()

        if options['test_lookup']:
            self.test_member_lookup()

        if options['regenerate']:
            self.regenerate_sample_combinations()

    def check_current_state(self):
        """Check current state of COREP combination_items."""
        self.stdout.write(self.style.HTTP_INFO("\n" + "=" * 60))
        self.stdout.write(self.style.HTTP_INFO("CHECKING CURRENT STATE OF COREP COMBINATION_ITEMS"))
        self.stdout.write(self.style.HTTP_INFO("=" * 60))

        # 1. Check COREP cubes
        corep_cubes = CUBE.objects.filter(cube_id__contains='COREP')
        self.stdout.write(f"\nCOREP Cubes: {corep_cubes.count()}")

        # 2. Check combinations linked to COREP cubes
        total_combinations = 0
        total_combo_items = 0
        items_with_member = 0
        items_without_member = 0

        for cube in corep_cubes[:5]:  # Sample first 5 cubes
            cube_to_combos = CUBE_TO_COMBINATION.objects.filter(cube_id=cube)
            combo_count = cube_to_combos.count()
            total_combinations += combo_count

            for ctc in cube_to_combos:
                combo = ctc.combination_id
                items = COMBINATION_ITEM.objects.filter(combination_id=combo)
                total_combo_items += items.count()

                for item in items:
                    if item.member_id:
                        items_with_member += 1
                    else:
                        items_without_member += 1

        self.stdout.write(f"\nSample of first 5 COREP cubes:")
        self.stdout.write(f"  Total combinations: {total_combinations}")
        self.stdout.write(f"  Total combination_items: {total_combo_items}")
        self.stdout.write(f"  Items WITH member_id: {items_with_member}")
        self.stdout.write(f"  Items WITHOUT member_id: {items_without_member}")

        if items_without_member > 0 and items_with_member == 0:
            self.stdout.write(self.style.ERROR(
                f"\n  ISSUE CONFIRMED: All {items_without_member} combination_items have NULL member_id"
            ))
        elif items_with_member > 0:
            self.stdout.write(self.style.SUCCESS(
                f"\n  FIX WORKING: {items_with_member} combination_items have member_id populated"
            ))

        # 3. Check ORDINATE_ITEM members for comparison
        self.stdout.write(self.style.HTTP_INFO("\n" + "-" * 40))
        self.stdout.write("Comparing with ORDINATE_ITEM data:")

        corep_oi_with_member = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__axis_ordinate_id__contains='COREP',
            member_id__isnull=False
        ).count()

        corep_oi_without_member = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__axis_ordinate_id__contains='COREP',
            member_id__isnull=True
        ).count()

        self.stdout.write(f"  ORDINATE_ITEM with member_id: {corep_oi_with_member}")
        self.stdout.write(f"  ORDINATE_ITEM without member_id: {corep_oi_without_member}")

        # 4. Check member mappings
        self.stdout.write(self.style.HTTP_INFO("\n" + "-" * 40))
        self.stdout.write("Member mapping data:")

        total_mmi = MEMBER_MAPPING_ITEM.objects.count()
        self.stdout.write(f"  Total MEMBER_MAPPING_ITEMs: {total_mmi}")

        # Sample some mappings
        sample_mmis = MEMBER_MAPPING_ITEM.objects.select_related(
            'variable_id', 'member_id'
        )[:5]

        self.stdout.write("\n  Sample member mappings:")
        for mmi in sample_mmis:
            var = mmi.variable_id.variable_id if mmi.variable_id else "None"
            mem = mmi.member_id.member_id if mmi.member_id else "None"
            is_src = mmi.is_source
            self.stdout.write(f"    Variable: {var}, Member: {mem}, is_source: {is_src}")

    def test_member_lookup(self):
        """Test the member lookup logic for a sample cell."""
        self.stdout.write(self.style.HTTP_INFO("\n" + "=" * 60))
        self.stdout.write(self.style.HTTP_INFO("TESTING MEMBER LOOKUP LOGIC"))
        self.stdout.write(self.style.HTTP_INFO("=" * 60))

        # Find a sample COREP cell with ordinate_items that have members
        sample_cell = None

        # Get a COREP cube
        corep_cube = CUBE.objects.filter(cube_id__contains='COREP').first()
        if not corep_cube:
            self.stdout.write(self.style.WARNING("No COREP cubes found"))
            return

        self.stdout.write(f"\nUsing cube: {corep_cube.cube_id}")

        # Get a combination linked to this cube
        ctc = CUBE_TO_COMBINATION.objects.filter(cube_id=corep_cube).first()
        if not ctc:
            self.stdout.write(self.style.WARNING("No combinations linked to cube"))
            return

        combo = ctc.combination_id
        self.stdout.write(f"Using combination: {combo.combination_id}")

        # Try to find a cell that references this combination
        # First, find any cell for a COREP table
        corep_table = TABLE.objects.filter(table_id__contains='COREP').first()
        if not corep_table:
            self.stdout.write(self.style.WARNING("No COREP tables found"))
            return

        sample_cell = TABLE_CELL.objects.filter(table_id=corep_table).first()
        if not sample_cell:
            self.stdout.write(self.style.WARNING("No cells found for COREP table"))
            return

        self.stdout.write(f"Using cell: {sample_cell.cell_id}")

        # Test the member lookup
        self.stdout.write(self.style.HTTP_INFO("\n" + "-" * 40))
        self.stdout.write("Testing _build_member_lookup_from_cell():")

        from pybirdai.process_steps.output_layer_mapping_workflow.lib.combination_creator import CombinationCreator

        creator = CombinationCreator("TEST", "1.0")
        ldm_var_to_member = creator._build_member_lookup_from_cell(sample_cell)

        if ldm_var_to_member:
            self.stdout.write(self.style.SUCCESS(
                f"\n  SUCCESS: Built lookup map with {len(ldm_var_to_member)} entries"
            ))
            for ldm_var, member in ldm_var_to_member.items():
                self.stdout.write(f"    LDM Variable: {ldm_var} -> Member: {member.member_id}")
        else:
            self.stdout.write(self.style.WARNING(
                "\n  No member mappings found for this cell"
            ))
            self.stdout.write("  This could mean:")
            self.stdout.write("    1. Cell has no ordinate_items with members")
            self.stdout.write("    2. No VARIABLE_MAPPING exists for DPM variables")
            self.stdout.write("    3. No MEMBER_MAPPING exists for DPM members")

        # Debug: Show what ordinate_items exist for this cell
        self.stdout.write(self.style.HTTP_INFO("\n" + "-" * 40))
        self.stdout.write("Cell's ordinate_items:")

        cell_positions = CELL_POSITION.objects.filter(cell_id=sample_cell)
        axis_ordinate_ids = [cp.axis_ordinate_id_id for cp in cell_positions]

        ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=axis_ordinate_ids
        ).select_related('variable_id', 'member_id')[:10]

        for oi in ordinate_items:
            var = oi.variable_id.variable_id if oi.variable_id else "None"
            mem = oi.member_id.member_id if oi.member_id else "None"
            self.stdout.write(f"  Variable: {var}, Member: {mem}")

        # Check if variable mappings exist for these DPM variables
        self.stdout.write(self.style.HTTP_INFO("\n" + "-" * 40))
        self.stdout.write("Checking VARIABLE_MAPPING_ITEMs for DPM variables:")

        dpm_var_ids = [oi.variable_id.variable_id for oi in ordinate_items if oi.variable_id]
        for dpm_var_id in dpm_var_ids[:5]:
            vmi_count = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_id__variable_id=dpm_var_id
            ).count()
            self.stdout.write(f"  {dpm_var_id}: {vmi_count} mapping(s)")

    def regenerate_sample_combinations(self):
        """Regenerate combinations for a sample COREP table."""
        self.stdout.write(self.style.HTTP_INFO("\n" + "=" * 60))
        self.stdout.write(self.style.HTTP_INFO("REGENERATING SAMPLE COREP COMBINATIONS"))
        self.stdout.write(self.style.HTTP_INFO("=" * 60))

        self.stdout.write(self.style.WARNING(
            "\nThis will delete and recreate combination_items for a sample COREP cube."
        ))

        # Get a COREP cube
        corep_cube = CUBE.objects.filter(cube_id__contains='COREP').first()
        if not corep_cube:
            self.stdout.write(self.style.ERROR("No COREP cubes found"))
            return

        self.stdout.write(f"\nTarget cube: {corep_cube.cube_id}")

        # Get combinations for this cube
        cube_to_combos = CUBE_TO_COMBINATION.objects.filter(cube_id=corep_cube)
        combo_count = cube_to_combos.count()

        if combo_count == 0:
            self.stdout.write(self.style.WARNING("No combinations to regenerate"))
            return

        self.stdout.write(f"Found {combo_count} combinations")

        # Count current state
        before_with_member = 0
        before_without_member = 0

        for ctc in cube_to_combos:
            combo = ctc.combination_id
            items = COMBINATION_ITEM.objects.filter(combination_id=combo)
            for item in items:
                if item.member_id:
                    before_with_member += 1
                else:
                    before_without_member += 1

        self.stdout.write(f"\nBefore regeneration:")
        self.stdout.write(f"  Items with member_id: {before_with_member}")
        self.stdout.write(f"  Items without member_id: {before_without_member}")

        # Ask for confirmation
        confirm = input("\nProceed with regeneration? (yes/no): ")
        if confirm.lower() != 'yes':
            self.stdout.write("Aborted.")
            return

        # Delete existing combination_items and regenerate
        from pybirdai.process_steps.output_layer_mapping_workflow.lib.combination_creator import CombinationCreator
        import datetime

        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        regenerated_count = 0
        for ctc in cube_to_combos[:5]:  # Only process first 5 for safety
            combo = ctc.combination_id

            # Find associated cell
            cell = TABLE_CELL.objects.filter(
                table_cell_combination_id=combo.combination_id
            ).first()

            if not cell:
                self.stdout.write(f"  No cell found for combination {combo.combination_id}")
                continue

            # Delete existing items
            old_items = COMBINATION_ITEM.objects.filter(combination_id=combo)
            old_count = old_items.count()
            old_items.delete()

            # Recreate using the fixed method
            creator = CombinationCreator("REGEN", "1.0")
            creator._create_default_combination_items(combo, corep_cube, cell)

            # Count new items
            new_items = COMBINATION_ITEM.objects.filter(combination_id=combo)
            new_with_member = new_items.filter(member_id__isnull=False).count()
            new_without_member = new_items.filter(member_id__isnull=True).count()

            self.stdout.write(
                f"  Regenerated {combo.combination_id}: "
                f"{old_count} -> {new_items.count()} items "
                f"(with member: {new_with_member}, without: {new_without_member})"
            )
            regenerated_count += 1

        self.stdout.write(self.style.SUCCESS(f"\nRegenerated {regenerated_count} combinations"))

        # Final state
        after_with_member = 0
        after_without_member = 0

        for ctc in cube_to_combos[:5]:
            combo = ctc.combination_id
            items = COMBINATION_ITEM.objects.filter(combination_id=combo)
            for item in items:
                if item.member_id:
                    after_with_member += 1
                else:
                    after_without_member += 1

        self.stdout.write(f"\nAfter regeneration (first 5 combinations):")
        self.stdout.write(f"  Items with member_id: {after_with_member}")
        self.stdout.write(f"  Items without member_id: {after_without_member}")

        if after_with_member > before_with_member:
            self.stdout.write(self.style.SUCCESS(
                f"\n  FIX WORKING: member_id population increased by {after_with_member - before_with_member}"
            ))
        elif after_with_member == 0:
            self.stdout.write(self.style.WARNING(
                "\n  Still no member_ids - check member_mapping data"
            ))
