from django.test import SimpleTestCase

from pybirdai.services.table_rendering_service import TableRenderingService


class TableRenderingServiceTests(SimpleTestCase):
    def test_header_builders_preserve_ordinate_items(self):
        root_items = [{
            'variable_id': 'ROOT_VAR',
            'variable_name': 'Root variable',
            'member_id': 'ROOT_MEMBER',
            'member_name': 'Root member',
        }]
        leaf_items = [{
            'variable_id': 'LEAF_VAR',
            'variable_name': 'Leaf variable',
            'member_id': None,
            'member_name': None,
        }]
        tree = [{
            'ordinate_id': 'ORD_ROOT',
            'name': 'Root ordinate',
            'is_abstract_header': True,
            'ordinate_items': root_items,
            'leaf_count': 1,
            'children': [{
                'ordinate_id': 'ORD_LEAF',
                'name': 'Leaf ordinate',
                'is_abstract_header': False,
                'ordinate_items': leaf_items,
                'leaf_count': 1,
                'children': [],
            }],
        }]

        column_headers = TableRenderingService._build_column_headers(tree)
        row_headers = TableRenderingService._build_row_headers(tree)

        self.assertEqual(column_headers['levels'][0][0]['ordinate_items'], root_items)
        self.assertEqual(column_headers['levels'][1][0]['ordinate_items'], leaf_items)
        self.assertEqual(row_headers[0]['ordinate_items'], root_items)
        self.assertEqual(row_headers[1]['ordinate_items'], leaf_items)
