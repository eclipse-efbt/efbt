import tempfile
from pathlib import Path

from django.test import SimpleTestCase

from pybirdai.process_steps.pybird.create_executable_filters import CreateExecutableFilters


class FakeDomain:
    def __init__(self, domain_id):
        self.domain_id = domain_id


class FakeMember:
    def __init__(self, member_id, domain_id=None):
        self.member_id = member_id
        self.domain_id = domain_id


class FakeSddContext:
    def __init__(self):
        self.domain_to_hierarchy_dictionary = {}
        self.member_plus_hierarchy_to_child_literals = {}
        self.members_that_are_nodes = set()


class CreateExecutableFiltersTests(SimpleTestCase):
    def test_member_without_domain_returns_no_leaf_members(self):
        filters = CreateExecutableFilters()
        context = FakeSddContext()
        member = FakeMember("NO_DOMAIN")

        result = filters.get_member_list_considering_hierarchies(context, member, None)

        self.assertEqual(result, [])

    def test_null_domain_hierarchy_entry_is_ignored(self):
        filters = CreateExecutableFilters()
        context = FakeSddContext()
        member = FakeMember("MEMBER", FakeDomain("DOMAIN"))
        context.members_that_are_nodes.add(member)
        context.domain_to_hierarchy_dictionary[None] = []

        result = filters.get_member_list_considering_hierarchies(context, member, None)

        self.assertEqual(result, [])

    def test_generated_python_cleanup_removes_cache_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            generated_dir = base_dir / "results" / "generated_python_filters"
            generated_dir.mkdir(parents=True)
            (generated_dir / "report_cells.py").write_text("# generated\n", encoding="utf-8")
            pycache_dir = generated_dir / "__pycache__"
            pycache_dir.mkdir()
            (pycache_dir / "report_cells.pyc").write_bytes(b"cache")

            with self.settings(BASE_DIR=str(base_dir)):
                CreateExecutableFilters().delete_generated_python_filter_files(None)

            self.assertEqual(list(generated_dir.iterdir()), [])
