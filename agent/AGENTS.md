# Scoring-Optimized Solver Instructions

Your output is scored by **positional line-level exact matching** against a reference solution.
Each changed line (added or removed) is compared position-by-position in the unified diff.
Score = matched_lines / max(your_lines, reference_lines).
Extra, missing, or misordered lines reduce your score.

## Workflow

1. Read the task carefully. Identify which files need modification.
2. Read each file you will edit **in full** before making any changes.
3. Make the **minimum necessary edits** to accomplish the task.
4. Stop immediately after editing. Do not summarize, explain, or verify.

## Rules

- **Minimal diff.** Change only what the task requires. Every extra changed line hurts your score. Do not touch formatting, imports, comments, or anything the task does not explicitly ask for.
- **Exact style match.** Use the same indentation (tabs vs spaces, width), quote style, semicolons, trailing commas, naming conventions, and spacing as the surrounding code. Match existing code character-for-character.
- **No cosmetic changes.** Do not add or modify comments, docstrings, type annotations, error handling, logging, blank lines, or whitespace unless the task explicitly requires it. Do not reformat, reorder imports, rename variables, or fix unrelated issues.
- **Direct implementation.** Use the simplest, most straightforward approach. Follow patterns already present in the codebase. Do not introduce abstractions, helpers, or generalization beyond what the task specifies.
- **File order.** When editing multiple files, process them in alphabetical path order. Within each file, edit from top to bottom.
- **Targeted reads.** Only read files that the task references or that clearly need modification. Do not explore project structure, read documentation, or read test files unless the task modifies them.
- **No verification.** Do not run tests, builds, linters, or type checkers. Do not re-read files after editing.
- **No commits.** The evaluation framework captures your diff automatically.
- **When unsure, don't.** If a change seems ambiguous or unnecessary, leave the code as-is. A smaller correct patch always beats a larger one with side effects.
- **No new files** unless the task explicitly requires creating one. Prefer editing existing files.
- **Preserve surrounding context.** When using edit tools, use enough context lines to anchor your edit precisely. Misplaced edits shift diff positions and reduce score.
