"""Structural regression: examples must not train or generate at import time.

This supplements (does not replace) runtime approved-module tests.
Run with python -m unittest discover -s ml/tests.
"""
import ast
from pathlib import Path
import unittest


class ExampleImportSafety(unittest.TestCase):
    def test_generation_is_guarded(self):
        root = Path(__file__).resolve().parents[1] / "examples"
        for path in root.glob("*predictor.py"):
            with self.subTest(example=path.name):
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                generators = [
                    node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == "generate_models"
                ]
                self.assertEqual(len(generators), 1)
                for node in tree.body:
                    if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom)):
                        continue
                    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                        continue  # module docstring
                    if isinstance(node, ast.If):
                        self.assertEqual(ast.unparse(node.test), "__name__ == '__main__'")
                        continue
                    # Constants are allowed, executable model construction is not.
                    if isinstance(node, ast.Assign):
                        self.assertFalse(any(isinstance(n, ast.Call) for n in ast.walk(node)))
                        continue
                    self.fail(f"Unexpected executable module-level statement: {ast.unparse(node)}")


if __name__ == "__main__":
    unittest.main()
