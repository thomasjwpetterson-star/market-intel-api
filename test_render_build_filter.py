import importlib.util
from pathlib import Path
import tempfile

spec = importlib.util.spec_from_file_location('render_filter', Path(__file__).parent / 'scripts/render_build_filter.py')
filters = importlib.util.module_from_spec(spec)
spec.loader.exec_module(filters)


def test_reviewed_ask_changes_skip_main_but_shared_release_and_mapping_changes_deploy():
    payload, shared = filters.prepare()
    assert shared == ['ask_mimir_beta/platform_description_mapping.py', 'ask_mimir_beta/platform_manifest.py']
    for path in ['ask_mimir_beta/lab_api.py', 'ask_mimir_beta/company_context.py',
                 'ask_mimir_beta/tests/test_company_cold_focus.py', 'ask_mimir_beta/requirements.txt']:
        assert not filters.triggers_main([path], payload)
    for path in [*shared, 'main.py', 'dashboard_projection.py', 'requirements.txt',
                 'private_platform_reference.py', 'platform_reference/new_reader.py',
                 'ask_mimir_beta/new_unreviewed_module.py', 'new_shared_package/main.py']:
        assert filters.triggers_main([path], payload), path
    assert filters.triggers_main(['ask_mimir_beta/lab_api.py', 'main.py'], payload)
    assert not filters.triggers_main([], payload)


def test_transitive_imports_are_retained_and_new_shared_dependency_invalidates_old_filter():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root/'ask_mimir_beta').mkdir()
        (root/'main.py').write_text('from ask_mimir_beta.manifest import resolve\n')
        (root/'ask_mimir_beta/manifest.py').write_text('from .shared import resolve\n')
        (root/'ask_mimir_beta/shared.py').write_text('def resolve(): pass\n')
        assert filters.shared_ask_dependencies(root) == [
            'ask_mimir_beta/manifest.py', 'ask_mimir_beta/shared.py']


def test_committed_snapshot_matches_current_main_import_closure():
    import json
    payload, _ = filters.prepare()
    assert json.loads(filters.DESTINATION.read_text()) == payload
