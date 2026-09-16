<!--
Everything shared with other agents is in AGENTS.md, pulled in by the import
below. This file holds only what Claude Code alone reads: its plugin and
marketplace declarations. Claude Code strips block-level HTML comments before
the file reaches the model, so this note costs no context.
-->

@AGENTS.md

## Claude Code plugins

The shared instructions are [`AGENTS.md`](AGENTS.md), which every agent reads
and the `@AGENTS.md` line imports; this section is what only Claude Code does.
Claude Code loads the import as a file of its own, after this one, not spliced
in at that line — the model sees the line itself, then this section, then
`AGENTS.md` — so nothing here may say the shared text is "above".

Vendor skills are declared in [`.claude/settings.json`](.claude/settings.json),
so Claude Code offers to install them when you trust this repo. They carry the
tool-level knowledge; `AGENTS.md` and the project skills carry the repo-level
knowledge.

| Plugin | Covers |
|--------|--------|
| `dbt@dbt-agent-marketplace` | [dbt Labs' skills](https://github.com/dbt-labs/dbt-agent-skills) — models, tests, docs, debugging |
| `ty-lsp@repo-local` | this repo's own ty language server, below |

**Retire a plugin by deleting its entry, never with `false`.** A `false` entry
reads as a declaration and does nothing.

**Two plugins that both declare a language server for `.py` is a silent
conflict** — the first loaded wins, and the other's is shadowed with a `[WARN]`
line in `~/.claude/debug/`. That is the trap in enabling a vendor plugin
alongside `ty-lsp`: the vendor's usually runs `uvx ty@latest`, the newest ty on
every launch, while `just typecheck` runs the one in `uv.lock`, so the editor
would show findings the recipe cannot reproduce.

`.claude/marketplace/` is a repo-local marketplace holding `ty-lsp`, which runs
the dev group's ty as a language server — there is no published ty plugin, and
an LSP server is a ten-line `.lsp.json`. Its command is `uv run ty server`, so
it runs `uv.lock`'s ty and must be launched from the project root. A `directory`
marketplace resolves from a **relative** path (`./.claude/marketplace`) and is
read live from the repo, so editing it needs no reinstall.
`claude plugin marketplace add` writes an *absolute* path into user settings, so
declare it in `.claude/settings.json` by hand.

**`.claude/skills` is a symlink to `.agents/skills`**, and Claude Code follows
it. Replacing it with a real directory gives two copies of every skill that
drift; `tests/test_agent_instructions.py` fails on that, and on this file losing
its `@AGENTS.md` line, which would leave Claude Code this section and nothing
else, with no error.
