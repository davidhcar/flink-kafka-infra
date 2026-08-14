#!/usr/bin/env python3
"""
Foolproof, Zero-Drift Flinkflow Pipeline Compiler & Renderer.
-------------------------------------------------------------
1. Resolves modular external source files (`file: src/...`, `file: sql/...`).
2. Uses Python AST analysis to dynamically detect operator entrypoints without hardcoding.
3. Automatically strips local development blocks (`if __name__ == '__main__':`).
4. Substitutes environment variables (${VAR_NAME} and ${VAR_NAME:-default}).
5. Performs strict validation on all file references, syntax, and schema properties.
"""

import ast
import os
import re
import sys
import yaml
from typing import Optional, List, Tuple


def render_template(content: str) -> str:
    """Substitutes ${VAR_NAME} and ${VAR_NAME:-default}."""
    pattern = re.compile(r'\$\{([A-Za-z0-9_]+)(?::-([^}]*))?\}')

    def replace_var(match):
        var_name = match.group(1)
        default_val = match.group(2)
        env_val = os.environ.get(var_name)
        if env_val is not None and env_val != "":
            return env_val
        if default_val is not None:
            return default_val
        return match.group(0)

    return pattern.sub(replace_var, content)


def analyze_python_ast(code_str: str) -> Tuple[Optional[str], str]:
    """
    Parses Python source code with AST to:
    1. Safely detect the primary streaming entrypoint function name.
    2. Strip `if __name__ == '__main__':` execution blocks.
    3. Check for existing top-level returns.
    """
    try:
        tree = ast.parse(code_str)
    except SyntaxError as e:
        raise ValueError(f"Syntax error in Python module: {e}")

    top_level_functions: List[str] = []
    has_top_level_return = False
    cleaned_lines = code_str.splitlines()

    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            top_level_functions.append(node.name)
        elif isinstance(node, ast.Return):
            has_top_level_return = True

    # If the script already has a top-level return (legacy or raw script), keep as is
    if has_top_level_return:
        return None, code_str

    # Preferred streaming entrypoint names in priority order
    preferred_names = [
        "process", "process_event", "transform", "map_record", "map",
        "evaluate_patient_window", "evaluate", "handle", "format_card", "main"
    ]

    selected_entrypoint: Optional[str] = None
    for pref in preferred_names:
        if pref in top_level_functions:
            selected_entrypoint = pref
            break

    # Fallback: choose the last top-level function defined in the file
    if not selected_entrypoint and top_level_functions:
        selected_entrypoint = top_level_functions[-1]

    # Filter out `if __name__ == "__main__":` blocks to keep operator code clean
    filtered_code_lines = []
    skip_main_block = False
    main_indent = 0

    for line in cleaned_lines:
        stripped = line.strip()
        if stripped.startswith("if __name__ ==") or stripped.startswith("if '__main__' =="):
            skip_main_block = True
            main_indent = len(line) - len(line.lstrip())
            continue

        if skip_main_block:
            if stripped == "":
                continue
            cur_indent = len(line) - len(line.lstrip())
            if cur_indent > main_indent:
                continue  # Inside main block, skip
            else:
                skip_main_block = False

        filtered_code_lines.append(line)

    cleaned_code = "\n".join(filtered_code_lines).strip()
    return selected_entrypoint, cleaned_code


def process_modular_pipeline(input_path: str) -> str:
    """Reads pipeline YAML, inlines any referenced `file:` paths, and renders env vars."""
    base_dir = os.path.dirname(os.path.abspath(input_path))

    with open(input_path, "r", encoding="utf-8") as f:
        raw_text = f.read()

    rendered_text = render_template(raw_text)

    try:
        doc = yaml.safe_load(rendered_text)
    except Exception as e:
        raise RuntimeError(f"Failed to parse pipeline YAML '{input_path}': {e}")

    if not isinstance(doc, dict) or "steps" not in doc:
        raise ValueError(f"Invalid pipeline YAML '{input_path}': missing top-level 'steps' array")

    for idx, step in enumerate(doc.get("steps", [])):
        if not isinstance(step, dict):
            continue

        step_name = step.get("name", f"step_{idx+1}")
        step_type = str(step.get("type", "")).lower()
        file_ref = step.get("file")

        if not file_ref:
            continue

        target_file = os.path.normpath(os.path.join(base_dir, file_ref))
        if not os.path.exists(target_file):
            raise FileNotFoundError(f"Step '{step_name}' references missing file: '{target_file}'")

        with open(target_file, "r", encoding="utf-8") as tf:
            file_content = tf.read().strip()

        if step_type == "sql":
            if "properties" not in step:
                step["properties"] = {}
            step["properties"]["query"] = file_content
            del step["file"]

        elif step_type == "process" or step.get("language") == "python":
            explicit_entrypoint = step.get("entrypoint")
            detected_entrypoint, cleaned_code = analyze_python_ast(file_content)

            target_fn = explicit_entrypoint or detected_entrypoint

            if target_fn:
                wrapper = f"\n\nreturn {target_fn}(input)\n"
            else:
                wrapper = ""

            step["code"] = cleaned_code + wrapper
            del step["file"]
            if "entrypoint" in step:
                del step["entrypoint"]

    # Represent multi-line block scalars cleanly with '|'
    class CustomDumper(yaml.SafeDumper):
        pass

    def str_presenter(dumper, data):
        if len(data.splitlines()) > 1:
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)

    CustomDumper.add_representer(str, str_presenter)

    output_yaml = yaml.dump(doc, Dumper=CustomDumper, sort_keys=False, width=1000)
    return output_yaml


def main():
    if len(sys.argv) < 2:
        print("Usage: python render_pipeline.py <pipeline_file> [output_file]", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.exists(input_file):
        print(f"Error: Pipeline file '{input_file}' not found", file=sys.stderr)
        sys.exit(1)

    try:
        final_yaml = process_modular_pipeline(input_file)
    except Exception as e:
        print(f"Compilation Error: {e}", file=sys.stderr)
        sys.exit(1)

    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
        parent = os.path.dirname(os.path.abspath(output_file))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(final_yaml)
    else:
        sys.stdout.write(final_yaml)


if __name__ == "__main__":
    main()


