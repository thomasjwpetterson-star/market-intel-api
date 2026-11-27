"""Prepare Main auto-deploy exclusions. No network access or service mutations."""
from __future__ import annotations
import ast
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / 'infrastructure/render/main-build-filter.json'


def shared_ask_dependencies(root=ROOT):
    """Follow local Python imports from Main; retain shared Ask modules transitively."""
    pending, seen = [root / 'main.py'], set()
    while pending:
        path = pending.pop()
        if path in seen or not path.is_file():
            continue
        seen.add(path)
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            modules = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                prefix = node.module or ''
                if node.level:
                    parent = path.parent
                    for _ in range(node.level - 1):
                        parent = parent.parent
                    prefix = '.'.join((*parent.relative_to(root).parts, *prefix.split('.'))).strip('.')
                modules = [prefix, *[prefix + '.' + alias.name for alias in node.names]]
            for module in modules:
                target = root.joinpath(*module.split('.'))
                pending.extend([target.with_suffix('.py'), target / '__init__.py'])
    return sorted(str(path.relative_to(root)) for path in seen
                  if path.is_relative_to(root / 'ask_mimir_beta'))


def prepare(root=ROOT):
    shared = shared_ask_dependencies(root)
    tracked = subprocess.check_output(['git', 'ls-files', 'ask_mimir_beta/'], cwd=root, text=True).splitlines()
    # Ignore reviewed existing entries only. New top-level files/directories fail
    # open and still deploy Main until reviewed. Never ignore the entire Ask tree.
    entries = sorted({path.split('/')[1] for path in tracked})
    ignored = []
    for entry in entries:
        path = 'ask_mimir_beta/' + entry
        if any(item == path or item.startswith(path + '/') for item in shared):
            continue
        ignored.append(path + '/**' if (root / path).is_dir() else path)
    return {'buildFilter': {'paths': [], 'ignoredPaths': ignored}}, shared


def triggers_main(paths, payload):
    """Match the exact-file and directory/** subset emitted by prepare()."""
    ignored = payload['buildFilter']['ignoredPaths']
    return any(not any(path == pattern or pattern.endswith('/**') and
                       path.startswith(pattern[:-2]) for pattern in ignored) for path in paths)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true', help='Write the local reviewed JSON payload only')
    parser.add_argument('--check', action='store_true', help='Check local JSON still matches the dependency audit')
    args = parser.parse_args()
    payload, shared = prepare()
    if args.check:
        if json.loads(DESTINATION.read_text()) != payload:
            raise SystemExit('Filter snapshot is stale: review shared imports and regenerate before applying.')
    elif args.write:
        DESTINATION.parent.mkdir(parents=True, exist_ok=True)
        DESTINATION.write_text(json.dumps(payload, indent=2) + '\n')
    else:
        print(json.dumps({'proposed_payload': payload, 'shared_ask_files_retained': shared}, indent=2))
