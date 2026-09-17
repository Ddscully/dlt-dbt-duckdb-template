"""The workflows, and the hand-maintained lists that have to agree with the tree.

A workflow cannot be run locally, so everything here is read as text: which test
files CI re-runs once the dbt manifest exists, that every workflow takes its
environment from the one composite action, that Dependabot can see the pins that
action holds, and that every `just` recipe which writes says which warehouse it
is writing to.

A project that deploys the site or publishes the warehouse adds a workflow for
it — and, if that workflow carries a hand-maintained path filter or asset list,
a check here that holds the list to the tree.
"""

from __future__ import annotations

import re
from fnmatch import fnmatchcase
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS_DIR = REPO_ROOT / ".github/workflows"
CI_WORKFLOW = WORKFLOWS_DIR / "ci.yml"


def _matches(path: str, pattern: str) -> bool:
    """fnmatch, with `**` meaning "and everything under it".

    `fnmatchcase` treats `*` as matching separators too, so `docs/**` and
    `docs/*` behave alike here; the pattern is written the way the YAML writes
    it so the two can be compared by eye.
    """
    return fnmatchcase(path, pattern) or fnmatchcase(path, pattern.rstrip("*").rstrip("/") + "/*")


# --------------------------------------------------------------------------- #
# ci.yml re-runs the manifest-gated tests, and that list is hand-maintained too
# --------------------------------------------------------------------------- #


# Anchored at column 0, so it matches a real module-level `pytestmark` and not a
# file that merely mentions one — this module writes both strings itself.
_GATED = re.compile(
    r"^pytestmark\s*=\s*pytest\.mark\.skipif\((?:.|\n)*?manifest_path", re.MULTILINE
)


def manifest_gated() -> set[str]:
    """Test files that skip themselves when `dbt/target/manifest.json` is absent."""
    return {
        path.name
        for path in (REPO_ROOT / "tests").glob("test_*.py")
        if _GATED.search(path.read_text())
    }


def re_run_after_parse() -> set[str]:
    """The files `ci.yml` names once the manifest exists.

    The unit-test step is a bare `uv run pytest` with nothing after it, so it
    contributes nothing here — which is the point: that is the run where these
    files skip.
    """
    named = re.findall(r"uv run pytest ([^\n]+)", CI_WORKFLOW.read_text())
    return {Path(arg).name for line in named for arg in line.split()}


def test_every_manifest_gated_test_file_is_re_run_after_dbt_parse():
    """A file that skips itself in CI's first step and is not named in its second
    runs **nowhere** in CI, and nothing says so.

    The skips show in every build log and read as normal; the silence is in the
    step that should pick the files back up.

    Compared as a set both ways, so adding a gated file without adding it to
    the workflow fails, and removing one from the workflow while it still skips
    fails too.
    """
    gated, re_run = manifest_gated(), re_run_after_parse()
    assert gated == re_run, (
        "ci.yml's post-parse pytest step and the manifest-gated test files disagree.\n"
        f"  gated but never re-run (they run nowhere in CI): {sorted(gated - re_run)}\n"
        f"  re-run but not gated (harmless, but the list is now wrong): {sorted(re_run - gated)}"
    )


# --------------------------------------------------------------------------- #
# The pipeline environment is defined once, in the setup action
# --------------------------------------------------------------------------- #


SETUP_ACTION = REPO_ROOT / ".github/actions/setup/action.yml"

# The three paths every layer resolves itself from. Absolute, and the first one
# is load-bearing rather than tidy — see the action's own comment.
PIPELINE_PATHS = ("WAREHOUSE_PATH", "LAKEHOUSE_DIR", "DAGSTER_HOME")


def _uncommented(text: str) -> str:
    """Workflow text with whole-line comments dropped.

    A workflow may name `DAGSTER_HOME` in prose, and a scan that cannot tell a
    mention from an assignment would either fail on that sentence or be widened
    until it stopped seeing assignments too.
    """
    return "\n".join(line for line in text.splitlines() if not line.strip().startswith("#"))


def _assigns(text: str, name: str) -> bool:
    """`NAME: value` as a YAML key, or `NAME=value` in a run block."""
    return re.search(rf"^\s*{name}\s*[:=]", _uncommented(text), re.MULTILINE) is not None


def test_the_setup_action_defines_every_pipeline_path():
    """Vacuity guard, and it comes first: the two tests below assert an *absence*.

    If the action stopped exporting these the workflows would be clean of them,
    both of the assertions below would pass, and every job would run against
    `profiles.yml`'s relative default — which resolves to the same file until the
    day something runs from another directory.
    """
    action = SETUP_ACTION.read_text()
    missing = [name for name in PIPELINE_PATHS if f"{name}=$GITHUB_WORKSPACE" not in action]
    assert not missing, (
        f"{SETUP_ACTION.name} no longer exports {missing}; the absence tests below "
        "would then pass while nothing sets them at all"
    )


def test_no_workflow_defines_a_pipeline_path_itself():
    """One definition, in the setup action, so a new path reaches every workflow.

    Four copies drift: a new variable lands in some and not others. For
    `LAKEHOUSE_DIR` that is loud but misplaced — DuckLake compares `data_path`
    as a *string*, so dlt (from the repo root) and dbt (from `dbt/`) spelling
    one directory two ways is refused inside `dbt build`, a layer downstream of
    the cause. A workflow setting one of these again is not wrong on its own; it
    is a second definition, which is how the first one drifts.
    """
    offenders = {
        path.name: [name for name in PIPELINE_PATHS if _assigns(path.read_text(), name)]
        for path in sorted(WORKFLOWS_DIR.glob("*.yml"))
    }
    offenders = {name: found for name, found in offenders.items() if found}
    assert not offenders, (
        f"workflows defining a pipeline path themselves: {offenders}\n"
        "These come from .github/actions/setup, which is the one place they are stated."
    )


def test_every_workflow_that_runs_the_pipeline_uses_the_setup_action():
    """The other direction, and the one an absence test cannot reach.

    Dropping the `uses:` line makes a workflow inherit nothing: no `just` on
    PATH, no paths exported, and `profiles.yml`'s relative default quietly
    standing in for the two that matter. Nothing above notices, because a
    workflow that defines none of them is exactly what those tests want to see.
    """
    runners = {
        path.name
        for path in sorted(WORKFLOWS_DIR.glob("*.yml"))
        if "just materialize" in path.read_text()
    }
    assert runners == {"ci.yml", "nightly.yml"}, (
        f"the scan for pipeline-running workflows found {sorted(runners)}; both run it"
    )
    missing = sorted(
        name
        for name in runners
        if "./.github/actions/setup" not in (WORKFLOWS_DIR / name).read_text()
    )
    assert not missing, f"workflows running the pipeline without the setup action: {missing}"


# --------------------------------------------------------------------------- #
# Dependabot watches the composite action, not just the workflows
# --------------------------------------------------------------------------- #


DEPENDABOT = REPO_ROOT / ".github/dependabot.yml"
ACTIONS_DIR = REPO_ROOT / ".github/actions"


def composite_actions_with_dependencies() -> set[str]:
    """`{"/.github/actions/setup"}` — local actions that pin a third-party one.

    An action with no `uses:` of its own has nothing for Dependabot to bump and
    is not this test's business.
    """
    return {
        f"/.github/actions/{path.parent.name}"
        for path in ACTIONS_DIR.glob("*/action.yml")
        if re.search(r"^\s*(-\s*)?uses:", path.read_text(), re.MULTILINE)
    }


def dependabot_action_directories() -> list[str]:
    """The `directory`/`directories` values on the `github-actions` entry.

    Scanned rather than parsed: PyYAML is not a runtime dependency of this
    project, and the tests that need it declare their own import.
    """
    lines = DEPENDABOT.read_text().splitlines()
    found: list[str] = []
    in_entry = in_list = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- package-ecosystem:"):
            in_entry = "github-actions" in stripped
            in_list = False
            continue
        if not in_entry or stripped.startswith("#"):
            continue
        if stripped.startswith("directory:"):
            found.append(stripped.split(":", 1)[1].strip().strip("\"'"))
        elif stripped.startswith("directories:"):
            in_list = True
        elif in_list and stripped.startswith("- "):
            found.append(stripped[2:].strip().strip("\"'"))
        elif in_list:
            in_list = False
    return found


def test_the_dependabot_scan_reads_the_entry_it_thinks_it_does():
    """Vacuity guard: the test below asserts a *covering*, and an empty read
    covers nothing while also finding nothing to cover."""
    assert "/" in dependabot_action_directories(), (
        "the github-actions entry in dependabot.yml no longer yields `/`; the scan "
        "has stopped matching the file rather than the file having changed"
    )
    assert composite_actions_with_dependencies(), (
        "no composite action with a `uses:` was found; either they are gone or the "
        "scan is looking in the wrong place"
    )


def test_dependabot_watches_every_composite_action_that_pins_one():
    """A bare `directory: /` scans `.github/workflows/` and nothing else.

    So a composite action's exactly-pinned `uses:` — `astral-sh/setup-uv` in
    `.github/actions/setup` — is unwatched unless its directory is listed:
    still pinned, frozen, and nothing goes red.

    `setup-uv` is the one that makes it matter: it stopped publishing moving
    major tags at v8, so a Dependabot PR is the only thing that can bump it
    (see the `dependency-versions` skill).
    """
    watched = dependabot_action_directories()
    unwatched = sorted(
        directory
        for directory in composite_actions_with_dependencies()
        if not any(_matches(directory.lstrip("/"), pattern.lstrip("/")) for pattern in watched)
    )
    assert not unwatched, (
        f"composite actions Dependabot cannot see: {unwatched}\n"
        "Add the directory (or a glob covering it) to the github-actions entry's "
        "`directories:` list in .github/dependabot.yml."
    )


JUSTFILE = REPO_ROOT / "justfile"

# Running any of these writes to the warehouse or the landing zone. The set is
# derived from what a recipe *does*, not from a list of recipe names, so a new
# writing recipe is covered without anyone remembering to add it.
WRITING_COMMANDS = (
    "python -m ingest.pipeline",
    "python -m transform.",
    "dbt build",
    "dagster job execute",
    "dagster asset materialize",
)

# `name:`, `name dep:` or `name arg='': dep` at column 0. The lookahead keeps
# `set dotenv-load := true` and `export LAKEHOUSE_DIR := …` out: those are
# assignments, and `:=` is the only thing separating them from a recipe header.
_RECIPE_HEADER = re.compile(r"^(?P<name>[a-z][a-z0-9-]*)(?P<params>[^:\n]*):(?!=)(?P<deps>.*)$")


def just_recipes() -> dict[str, tuple[str, str]]:
    """recipe name -> (its dependency list, its body)."""
    recipes: dict[str, tuple[str, str]] = {}
    name: str | None = None
    deps = ""
    body: list[str] = []
    for line in JUSTFILE.read_text().splitlines():
        header = _RECIPE_HEADER.match(line)
        if header:
            if name is not None:
                recipes[name] = (deps, "\n".join(body))
            name, deps, body = header["name"], header["deps"], []
        elif name is not None and (line.startswith((" ", "\t")) or not line.strip()):
            body.append(line)
        elif line.startswith("#"):
            continue
        else:
            if name is not None:
                recipes[name] = (deps, "\n".join(body))
            name, deps, body = None, "", []
    if name is not None:
        recipes[name] = (deps, "\n".join(body))
    return recipes


def writing_recipes() -> dict[str, tuple[str, str]]:
    return {
        name: parts
        for name, parts in just_recipes().items()
        if any(command in parts[1] for command in WRITING_COMMANDS)
    }


def test_the_justfile_scan_finds_the_recipes_it_thinks_it_does():
    """Vacuity guard, and it comes first.

    The test below iterates over whatever this scan returns, so a regex that
    matched nothing — or that swallowed `export LAKEHOUSE_DIR := …` as a recipe
    — would pass it by having nothing to check.
    """
    recipes = just_recipes()
    assert "where" in recipes, "the announcing recipe itself is missing"
    assert "export" not in recipes, "`:=` assignments are being read as recipes"
    writing = writing_recipes()
    assert {"ingest", "dbt-build", "materialize", "test-pipeline"} <= set(writing), (
        f"the writing-recipe scan found only {sorted(writing)}"
    )


def test_every_recipe_that_writes_says_where_it_is_writing():
    """dbt's only "where am I" line names the *target*, and there is one target
    here on purpose (`dbt/profiles.yml` argues it), so that line is a constant.
    What varies is the file, and no dbt output prints it.

    So a recipe that writes either depends on `where`, or exports its own
    `WAREHOUSE_PATH` and announces that itself — `test-pipeline` does the
    latter, and taking `where` as a dependency there would print the *outer*
    value, which is worse than printing nothing.
    """
    silent = []
    for name, (deps, body) in sorted(writing_recipes().items()):
        if "where" in deps.split():
            continue
        if "export WAREHOUSE_PATH" in body:
            continue
        silent.append(name)
    assert not silent, (
        f"these recipes write to the warehouse or the landing zone without saying "
        f"where: {silent}. Add `where` as their first dependency, or export "
        f"WAREHOUSE_PATH in the body and echo it as `test-pipeline` does."
    )


def test_both_ways_of_announcing_are_actually_in_use():
    """The test above passes a recipe on either of two conditions, so a tree
    where every writing recipe took the same route would leave the other branch
    unexercised and free to rot."""
    by_dependency, by_own_export = [], []
    for name, (deps, body) in writing_recipes().items():
        (by_dependency if "where" in deps.split() else by_own_export).append(name)
    assert by_dependency, "no recipe depends on `where`"
    assert by_own_export, "no recipe exports its own WAREHOUSE_PATH any more"


def test_every_recipe_that_writes_refuses_a_dbt_dotenv():
    """dbt 1.12 loads `dbt/.env` for any variable a recipe leaves unset, so a
    writing recipe must reach `_no-dbt-dotenv` — through `where`, which depends
    on it, or directly. The recipes that export their own WAREHOUSE_PATH skip
    `where` and repeat the dependency by hand, and the announcing test above
    passes a new one without it."""
    guard = "_no-dbt-dotenv"
    assert guard in just_recipes()["where"][0].split(), (
        f"`where` no longer depends on `{guard}`, so no recipe that depends on "
        f"`where` is guarded against dbt/.env"
    )
    unguarded = sorted(
        name
        for name, (deps, _) in writing_recipes().items()
        if not {"where", guard} & set(deps.split())
    )
    assert not unguarded, (
        f"these recipes write without refusing a dbt/.env: {unguarded}. Add "
        f"`where` as their first dependency or, if they export WAREHOUSE_PATH, "
        f"`{guard}`."
    )


def _recipe(name: str) -> str:
    """The body of one `just` recipe, from its header to the next blank-line gap.

    Read as text rather than by running `just --evaluate`: the assertions here
    are about what the recipe *says*, and a recipe that has stopped exporting
    something evaluates perfectly well.
    """
    lines = (REPO_ROOT / "justfile").read_text().splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith(f"{name}:"))
    body: list[str] = []
    for line in lines[start + 1 :]:
        if line and not line.startswith((" ", "\t", "#")):
            break
        body.append(line)
    return "\n".join(body)


def test_the_fixture_pipeline_isolates_every_piece_of_state_it_touches():
    """`just test-pipeline` must not leave anything behind for the next real run.

    Four overrides, each for a leak that was observed:

    * `WAREHOUSE_PATH` — without it a fixture run overwrites the real warehouse
      with the recorded slice.
    * `LAKEHOUSE_DIR` — dlt *lands* in the lakehouse, so without it the slice
      merges into the only copy of every landing table there is.
    * `DBT_RUN_RESULTS_PATH` (with `--target-path`) — dbt writes its artifacts
      to `dbt/target/` wherever the build pointed, and `analytics.pipeline_runs`
      records whatever that file last held, so the next `just pipeline-status`
      would file the fixture timings in the real warehouse's build history.
    * `DBT_MANIFEST_PATH` — the same directory, so the test inventory reads the
      fixture build's manifest rather than a real one alongside it.

    And a fifth, set only when the landing zone's Parquet is in a bucket:
    `LAKEHOUSE_DATA_PATH` outranks `LAKEHOUSE_DIR`, so without its own override
    the slice's Parquet lands under the real prefix — the second leak again, in
    a bucket.

    Asserted as a set rather than by reading the recipe's behaviour, because
    each is invisible when missing: the fixture run still passes, and what
    breaks is the *next* command against real data.
    """
    recipe = _recipe("test-pipeline")
    for variable in (
        "WAREHOUSE_PATH",
        "LAKEHOUSE_DIR",
        "DBT_RUN_RESULTS_PATH",
        "DBT_MANIFEST_PATH",
        "LAKEHOUSE_DATA_PATH",
    ):
        assert f"export {variable}=" in recipe, (
            f"`just test-pipeline` no longer overrides {variable}, so a fixture run "
            f"leaks that state into the next command that reads it"
        )
    # The env var alone is not enough: dbt has to be told to *write* there too,
    # or the override points at a file the build never creates.
    assert '--target-path "$DBT_TARGET_PATH"' in recipe
