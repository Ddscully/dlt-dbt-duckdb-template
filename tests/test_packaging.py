"""The three places `src/modern_data_stack/` is spelled, and what checks them.

The directory is on disk, `[tool.uv.build-backend] module-name` is read by
uv_build at install time, and `[project.scripts]`'s entry point is resolved only
when someone runs `mds`. Nothing makes the three agree.

This matters more here than in most repos: `scripts/rename_project.py` exists so
that renaming the project does *not* rename the package, and `module-name` is
the whole of what makes that true. Its docstring lists this key as one of the
four names a rename deliberately leaves alone; these tests are why that list can
be believed.
"""

from __future__ import annotations

import tomllib

import modern_data_stack
from modern_data_stack.paths import project_root

PACKAGE = "modern_data_stack"


def _pyproject() -> dict:
    with (project_root() / "pyproject.toml").open("rb") as fh:
        return tomllib.load(fh)


def test_the_package_name_is_set_rather_than_derived():
    """Deleting `module-name` is the regression here, and it is silent.

    uv_build then derives the module from `[project] name`, which still agrees
    with the directory until someone renames the project — which, in a template,
    is the first thing anyone does. The error they would get names `src/` and
    mentions neither this key nor the rename script that sent them there.
    """
    config = _pyproject()["tool"]["uv"].get("build-backend", {})
    assert config.get("module-name") == PACKAGE
    assert (project_root() / "src" / PACKAGE / "__init__.py").is_file()


def test_the_console_script_names_a_callable_in_that_package():
    """`mds = "modern_data_stack:main"` is resolved when someone runs `mds`, not
    when the package is installed, so a rename that reached it by mistake would
    break for a user and for no one else."""
    module, _, attribute = _pyproject()["project"]["scripts"]["mds"].partition(":")
    assert module == PACKAGE
    assert callable(getattr(modern_data_stack, attribute))
