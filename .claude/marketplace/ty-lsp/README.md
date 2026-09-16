# ty-lsp

Runs [ty](https://docs.astral.sh/ty/) as the Python language server for this
project, so Claude Code (and any editor pointed at the same config) gets real
type diagnostics and go-to-definition instead of grep.

## Why this is a repo-local plugin

There is no published ty plugin, and a language server is a ten-line
`.lsp.json`. `pyright-lsp@claude-plugins-official` is the obvious alternative
and was rejected on one row: it installs as `npm install -g pyright`, an
unpinned global binary no lockfile in this repo can see, where ty comes from the
dev group and is pinned in `uv.lock`. A checker whose version the project cannot
state is a checker that will one day disagree with CI.

## Why the command is `uv run ty`, not `ty`

Same reason. A bare `ty` on `PATH` would be a second copy free to drift from the
one in `uv.lock`; `uv run` resolves the locked version. The cost is that the
server has to be launched with the project root as its working directory, which
is what `uv run` walks up from to find `pyproject.toml`.

## It has to outrank any other plugin claiming `.py`

Astral publishes its own plugin, and it ships a ty language server too —
`uvx ty@latest server`, the newest published ty on every launch. This one is
`uv run ty server`, the version in `uv.lock`.

Both would claim `.py`; **the first loaded wins and the loser is a `[WARN]`
nobody sees**. The order is the order of `enabledPlugins` in
`.claude/settings.json`, so enabling such a plugin means putting it *below*
`ty-lsp` — and alphabetical tidying of that block would quietly swap them. The
symptom is subtle: the editor showing findings `just typecheck` cannot
reproduce. Check with `claude --debug -p ok` and
`grep 'already handled by' ~/.claude/debug/latest`; no output is the passing
state.

## Install

Declared in `.claude/settings.json`, so Claude Code offers it when you trust the
repo. By hand:

```bash
claude plugin marketplace add ./.claude/marketplace
claude plugin install ty-lsp@repo-local
```

`just typecheck` is the same checker without the editor.
