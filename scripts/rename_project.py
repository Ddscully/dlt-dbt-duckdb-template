"""Rename this project from the template's placeholder to yours.

    uv run python -m scripts.rename_project acme_metrics

Replaces `my_warehouse` and `my-warehouse` in every file git tracks — the
distribution name, the dbt project and profile, the dlt pipeline name, the
Dagster code location, the Evidence package and the lockfiles that repeat them.

Four things it deliberately does not touch:

* **the package**, `src/modern_data_stack/`. `[tool.uv.build-backend]
  module-name` in pyproject.toml decouples it from the project name, so every
  `from modern_data_stack...` import survives a rename.
* **`lakehouse`**, the DuckLake ATTACH alias. dbt writes it into the stored SQL
  of every staging view, and `_sources.yml` and `profiles.yml` must agree with
  it.
* **`warehouse.duckdb`**, the file's name, which dbt also writes into those
  fully-qualified view definitions.
* **`repo-local`**, the Claude Code marketplace id in `.claude/settings.json`.
  It names a role, not this project, exactly so a rename never has to reach it;
  making it project-derived would leave every adopter carrying a stale id.

It skips itself, so this file keeps naming the placeholder and a second run
reports that there is nothing left to rename.

Run it on a clean tree, so `git diff` shows exactly what moved. Then
`uv sync` (the distribution name changed) and `just test`.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

from modern_data_stack.paths import project_root

SNAKE = "my_warehouse"
KEBAB = "my-warehouse"

# The name becomes a Python identifier (the dlt pipeline and dbt profile keys),
# a directory in `~/.dlt`, and a distribution name. Lowercase snake_case is the
# intersection that is valid as all three.
VALID = re.compile(r"^[a-z][a-z0-9_]*$")


def tracked_text_files(root: Path) -> list[Path]:
    """Every tracked file, minus the ones that are not text."""
    listed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split("\0")
    out = []
    this_file = Path(__file__).resolve()
    for name in filter(None, listed):
        path = root / name
        if not path.is_file() or path.is_symlink():
            continue
        # Itself: the placeholder is what this file documents, and a tool that
        # rewrites its own instructions mid-run reads as a bug either way.
        if path.resolve() == this_file:
            continue
        try:
            path.read_text()
        except UnicodeDecodeError:
            continue
        out.append(path)
    return out


def rename(new_name: str, root: Path | None = None) -> dict[str, int]:
    """Rewrite both spellings of the placeholder. Returns {path: replacements}."""
    if not VALID.match(new_name):
        raise ValueError(
            f"{new_name!r} is not a valid project name: lowercase letters, digits "
            "and underscores, starting with a letter (it becomes a Python "
            "identifier, a dbt profile key and a distribution name)"
        )
    root = root or project_root()
    kebab = new_name.replace("_", "-")
    changed: dict[str, int] = {}
    for path in tracked_text_files(root):
        text = path.read_text()
        hits = text.count(SNAKE) + text.count(KEBAB)
        if not hits:
            continue
        path.write_text(text.replace(SNAKE, new_name).replace(KEBAB, kebab))
        changed[str(path.relative_to(root))] = hits
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("name", help="the new project name, in snake_case")
    args = parser.parse_args()

    try:
        changed = rename(args.name)
    except ValueError as exc:
        sys.exit(str(exc))

    if not changed:
        sys.exit(
            f"nothing to rename: no tracked file mentions {SNAKE!r} or {KEBAB!r}. "
            "This project has already been renamed."
        )
    for path, hits in sorted(changed.items()):
        print(f"  {path:<40} {hits:>3} replacements")
    print(
        f"\nrenamed to {args.name} in {len(changed)} files. Next:\n"
        "  uv sync --group dev --group orchestration   # the distribution name changed\n"
        "  just test\n"
        "Then edit `authors` in pyproject.toml, the owner in dbt/models/_groups.yml\n"
        "and dbt/models/_exposures.yml, and LICENSE. Delete the 'Renaming'\n"
        "sections of README.md and AGENTS.md: this run rewrote the placeholder\n"
        "inside them, so they now describe a rename that has already happened.\n"
        "Wrong name? `git checkout .` and run it again — the placeholder is gone\n"
        "from the tree, so a second run has nothing to find."
    )


if __name__ == "__main__":
    main()
