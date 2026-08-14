#!/usr/bin/env python3
"""
Render Flinkflow pipeline YAML by substituting environment variables.
Supports ${VAR_NAME} and ${VAR_NAME:-default_value} syntax.
Environment variables are sourced from env.local (loaded into environment by mise).
"""

import os
import re
import sys

def render_template(content: str) -> str:
    # Match ${VAR_NAME} or ${VAR_NAME:-default}
    # Pattern strictly matches uppercase/alphanumeric variable names, avoiding Python f-strings like ${foo['bar']}
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

def main():
    if len(sys.argv) < 2:
        print("Usage: python render_pipeline.py <pipeline_file> [output_file]", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.exists(input_file):
        print(f"Error: Pipeline file '{input_file}' not found", file=sys.stderr)
        sys.exit(1)

    with open(input_file, "r", encoding="utf-8") as f:
        content = f.read()

    rendered = render_template(content)

    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
        parent = os.path.dirname(os.path.abspath(output_file))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(rendered)
    else:
        sys.stdout.write(rendered)

if __name__ == "__main__":
    main()
