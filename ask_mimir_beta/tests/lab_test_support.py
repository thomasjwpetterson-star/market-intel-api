"""Load real API definitions without booting a multi-GB production snapshot.

Only application construction, global runtime construction and route decoration
are skipped. Tests execute the unmodified function/class bodies with fixtures.
"""
import ast
import sys
import types
from pathlib import Path


def load_lab():
    path = Path(__file__).resolve().parents[1] / "lab_api.py"
    module = types.ModuleType("mimir_api_test_definitions")
    module.__file__ = str(path)
    sys.modules[module.__name__] = module
    nodes = []
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.ClassDef)):
            nodes.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.decorator_list = []
            nodes.append(node)
        elif isinstance(node, ast.Assign) and all(
            isinstance(target, ast.Name) and target.id.isupper() for target in node.targets
        ):
            nodes.append(node)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), module.__dict__)
    return module
