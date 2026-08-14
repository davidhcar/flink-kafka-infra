import unittest
import os
import sys
import tempfile
from pathlib import Path

scripts_dir = str(Path(__file__).resolve().parents[4] / "scripts")
if scripts_dir not in sys.path:
    sys.path.insert(0, scripts_dir)

from render_pipeline import analyze_python_ast, render_template, process_modular_pipeline


class TestRenderPipeline(unittest.TestCase):

    def test_render_template_env_substitution(self):
        """Verifies environment variable substitution with defaults."""
        os.environ["TEST_BOOTSTRAP_HOST"] = "custom-kafka:9092"
        raw = "server: ${TEST_BOOTSTRAP_HOST:-localhost:9092}, other: ${UNSET_VAR:-default_val}"
        rendered = render_template(raw)
        self.assertEqual(rendered, "server: custom-kafka:9092, other: default_val")

    def test_analyze_python_ast_detects_entrypoint(self):
        """Verifies that AST finds entrypoint functions dynamically without hardcoding."""
        code = '''
def helper():
    pass

def custom_transformation_logic(input_tick):
    return input_tick.upper()

if __name__ == '__main__':
    print(helper())
'''
        entrypoint, cleaned = analyze_python_ast(code)
        self.assertEqual(entrypoint, "custom_transformation_logic")
        self.assertNotIn("if __name__ ==", cleaned)

    def test_analyze_python_ast_preserves_existing_return(self):
        """Verifies that scripts with existing top-level returns are not modified."""
        raw_script = "x = input.upper()\nreturn x\n"
        entrypoint, cleaned = analyze_python_ast(raw_script)
        self.assertIsNone(entrypoint)
        self.assertEqual(cleaned, raw_script)

    def test_process_modular_pipeline_missing_file_raises_error(self):
        """Verifies that missing file references immediately fail with explicit error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yaml_path = os.path.join(tmpdir, "pipeline.yaml")
            with open(yaml_path, "w") as f:
                f.write("steps:\n  - type: process\n    language: python\n    file: missing_script.py\n")

            with self.assertRaises(FileNotFoundError):
                process_modular_pipeline(yaml_path)


if __name__ == "__main__":
    unittest.main()
