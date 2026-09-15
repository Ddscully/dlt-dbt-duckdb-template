"""The project skills, checked against the repo they cite.

`.agents/skills/*/SKILL.md` quotes file paths and `just` recipes out of the rest
of the tree. None of that is executable, so it rots the way an exposure does:
the skill still reads correctly, and the path it sends an agent to was renamed
six commits ago. A skill that points at a missing file is worse than no skill,
because the reader assumes they are the one who is wrong.

The same argument as `tests/test_exposures.py` and `tests/test_report.py`: an
assertion about the *outside* of a system needs a test on the outside of it.

No warehouse and no dbt manifest here — `just test` has neither, so everything
below reads markdown and the justfile as text.
"""

from __future__ import annotations

import re
import subprocess

import pytest

from modern_data_stack.paths import project_root

JUSTFILE = project_root() / "justfile"

# Globbed, not listed: from a hand-maintained list a skill could be omitted
# without any error, and nothing would check it.
SKILLS_DIR = project_root() / ".agents" / "skills"

# Top-level directories a skill may cite. `data/` is deliberately absent: it is
# gitignored and built, so a path under it is correct even on a fresh clone
# where it does not exist yet.
CITABLE_ROOTS = (
    "dbt",
    "docs",
    "ingest",
    "lake",
    "orchestration",
    "publish",
    "reports",
    "scripts",
    "src",
    "tests",
    "transform",
)

# A backticked path, e.g. `dbt/models/marts/gold/fct_gold_price_month.sql`.
# Anchored on the citable roots so prose like `month_start` cannot match.
_CITED_PATH = re.compile(r"`((?:" + "|".join(CITABLE_ROOTS) + r")/[A-Za-z0-9_./*-]+)`")

# `just dbt-parse`. Searched only inside code — a fenced block or an inline span
# — because "just" is also an English word and the prose is full of it. Comments
# inside a fenced block are prose too, and are stripped for the same reason.
_CITED_RECIPE = re.compile(r"\bjust ([a-z][a-z0-9-]*)")
_FENCED = re.compile(r"^```[^\n]*\n(.*?)^```", re.MULTILINE | re.DOTALL)
_INLINE_CODE = re.compile(r"`([^`\n]+)`")
_SHELL_COMMENT = re.compile(r"#[^\n]*")

# A recipe definition in the justfile: name, then optional args which may carry
# a default (`sql mode="read"`), then the colon. The default matters — without
# it that recipe reads as undefined and every citation of it fails.
_RECIPE_DEF = re.compile(r"^([a-z][a-z0-9-]*)(?: [a-z_]+(?:=[^\s:]*)?)*:", re.MULTILINE)


def skills() -> list:
    return sorted(SKILLS_DIR.glob("*/SKILL.md"))


def recipes() -> set[str]:
    """Every recipe name the justfile defines."""
    return set(_RECIPE_DEF.findall(JUSTFILE.read_text()))


def ignored(paths: set[str]) -> set[str]:
    """Which of `paths` git ignores.

    A gitignored path is a build artifact: correct to cite, and absent on a
    fresh clone. Asking git avoids a second list that could drift from the
    .gitignore.

    Every path is asked about twice, with and without a trailing slash, because
    a directory-only pattern (`dbt/target/`) matches only a path the filesystem
    says is a directory. Without the slashed probe, a cited `dbt/target` would
    be exempt on a machine that had built and nowhere else — green locally, red
    on a fresh checkout.
    """
    if not paths:
        return set()
    probes = {form for c in paths for form in (c.rstrip("/"), c.rstrip("/") + "/")}
    done = subprocess.run(
        ["git", "check-ignore", "--stdin"],
        cwd=project_root(),
        input="\n".join(sorted(probes)),
        capture_output=True,
        text=True,
        check=False,  # exit 1 simply means nothing matched
    )
    hits = {line.strip() for line in done.stdout.splitlines() if line.strip()}
    return {c for c in paths if c.rstrip("/") in hits or c.rstrip("/") + "/" in hits}


def code_spans(text: str) -> list[str]:
    """Every fenced block (comments stripped) and inline code span."""
    return [_SHELL_COMMENT.sub("", block) for block in _FENCED.findall(text)] + (
        _INLINE_CODE.findall(_FENCED.sub("", text))
    )


def test_there_are_skills_to_check():
    """The parametrised tests below pass by not running if the glob stops matching."""
    assert skills(), "no .agents/skills/*/SKILL.md found — the skills directory moved"


@pytest.mark.parametrize("skill", skills(), ids=[s.parent.name for s in skills()])
def test_every_path_a_skill_cites_exists(skill):
    """A renamed model must not leave a skill pointing at a dead path."""
    # A glob is a description of a set, not a path to open.
    cited = {c for c in _CITED_PATH.findall(skill.read_text()) if "*" not in c}
    missing = sorted(c for c in cited - ignored(cited) if not (project_root() / c).exists())
    assert not missing, f"{skill.parent.name} cites paths that no longer exist: {missing}"


@pytest.mark.parametrize("skill", skills(), ids=[s.parent.name for s in skills()])
def test_every_just_recipe_a_skill_cites_exists(skill):
    """`just dbt-parse` in a command an agent will run must be a recipe."""
    defined = recipes()
    missing = sorted(
        {
            cited
            for span in code_spans(skill.read_text())
            for cited in _CITED_RECIPE.findall(span)
            if cited not in defined
        }
    )
    assert not missing, f"{skill.parent.name} cites justfile recipes that don't exist: {missing}"


def test_the_citation_scans_still_find_citations():
    """Both tests above pass by not looking if their patterns stop matching.

    A scanner whose pattern drifts reports no findings, which is
    indistinguishable from a clean tree. Non-emptiness and nothing else: a floor
    on how much the skills cite would go red on a legitimate deletion.
    """
    text = "\n".join(skill.read_text() for skill in skills())
    assert _CITED_PATH.findall(text), (
        "no skill cites a path under any of CITABLE_ROOTS — either the skills "
        "stopped quoting the tree, or _CITED_PATH has stopped matching"
    )
    assert [r for span in code_spans(text) for r in _CITED_RECIPE.findall(span)], (
        "no skill cites a `just` recipe — either the skills stopped naming them, "
        "or the code-span scan has stopped matching"
    )
