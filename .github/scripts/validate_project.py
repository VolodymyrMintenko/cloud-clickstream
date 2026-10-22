#!/usr/bin/env python3
"""Checks that the DataSQRL project files are present and consistent with each other."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SHARED_PACKAGE = "web_clickstream-shared-package.json"
ENVIRONMENTS = ("test", "local", "prod")

errors = []


def load_package(name):
    path = ROOT / name
    if not path.is_file():
        errors.append(f"missing package file: {name}")
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as error:
        errors.append(f"{name} is not valid JSON: {error}")
        return None


shared = load_package(SHARED_PACKAGE)

for environment in ENVIRONMENTS:
    overlay = load_package(f"web_clickstream-{environment}-package.json")
    if overlay is None:
        continue
    declared = overlay.get("script", {}).get("config", {}).get("environment")
    if declared != environment:
        errors.append(
            f"web_clickstream-{environment}-package.json declares environment "
            f"{declared!r}, expected {environment!r}"
        )
    connector = ROOT / "connectors" / f"pageview-{environment}.sqrl"
    if not connector.is_file():
        errors.append(f"missing connector for the {environment} environment: {connector.name}")

if shared is not None:
    main_script = shared.get("script", {}).get("main")
    if not main_script:
        errors.append(f"{SHARED_PACKAGE} does not declare script.main")
    elif not (ROOT / main_script).is_file():
        errors.append(f"script.main points at a missing file: {main_script}")

    engines = shared.get("enabled-engines")
    if not engines:
        errors.append(f"{SHARED_PACKAGE} does not declare enabled-engines")

test_runner = ROOT / "run-tests.sh"
if not test_runner.is_file():
    errors.append("missing run-tests.sh")

if errors:
    for error in errors:
        print(f"error: {error}")
    sys.exit(1)

print("project layout is valid")
