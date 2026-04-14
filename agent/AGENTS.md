# Scoring-Optimized Solver Instructions

Your diff is scored by **positional line-level exact matching** against a reference solution.

```
score = matched_lines / max(your_changed_lines, reference_changed_lines)
```

Every line you add or remove is compared position-by-position to the reference diff.
Extra lines, missing lines, and misordered lines all reduce your score directly.
The only way to maximize score is to produce the **same changes** as the reference, **in the same order**.

## Workflow

1. **Parse the task.** Extract the exact symbol names (functions, methods, classes, variables, types) that must change.
2. **Locate files instantly** using targeted search — do not browse directory trees:
   ```bash
   grep -r "symbol_name" . --include="*.ts" -l 2>/dev/null | head -5
   grep -r "def symbol_name" . --include="*.py" -l 2>/dev/null | head -5
   grep -r "func symbol_name" . --include="*.go" -l 2>/dev/null | head -5
   ```
3. **Read each file you will edit in full** — never edit a file you haven't completely read.
4. **Make the minimum necessary edits.** Change only what the task explicitly requires.
5. **Stop immediately** after editing. Do not verify, summarize, explain, or re-read.

## Rules

- **Minimal diff.** Every extra changed line hurts your score. Do not touch anything the task does not explicitly require.
- **Exact style match.** Copy indentation (tabs vs spaces, width), quote style, semicolons, trailing commas, spacing, and naming conventions character-for-character from the surrounding code.
- **No cosmetic changes.** Never add or modify comments, blank lines, docstrings, type annotations, error handling, logging, or imports unless the task explicitly requires it. Never reformat, reorder imports, rename variables, or fix unrelated issues.
- **No refactoring.** Do not introduce abstractions, helpers, or generalization beyond what the task specifies.
- **File order.** Edit files in alphabetical path order. Within each file, edit top-to-bottom.
- **Targeted reads only.** Read only files the task references or that contain the symbols you must change. Do not read test files, documentation, or configuration unless the task requires modifying them.
- **No verification.** Do not run tests, builds, linters, or type checkers. Do not re-read files after editing.
- **No commits.** The evaluation framework captures your diff automatically via `git diff`.
- **No new files** unless the task explicitly requires creating one. Prefer editing existing files.
- **When unsure, don't.** A smaller correct patch always beats a larger patch with side effects.
- **Preserve context.** When using edit tools, include enough surrounding lines to anchor the edit precisely. Misplaced edits shift line positions and reduce score.

## Fast Symbol Search

When the task names a specific function, class, or variable, find its file immediately:

```bash
# Find by function/method name
grep -rn "functionName" . --include="*.ts" -l 2>/dev/null
grep -rn "def method_name" . --include="*.py" -l 2>/dev/null

# Find by class or type
grep -rn "class ClassName" . --include="*.go" -l 2>/dev/null
grep -rn "interface TypeName" . --include="*.ts" -l 2>/dev/null

# Find by string literal or error message mentioned in the task
grep -rn "exact error string" . -l 2>/dev/null
```

One grep call finds the right file in under one second. Never traverse directories manually.

## What the Score Measures

The scorer builds a sequence of changed lines from your diff and from the reference diff, then counts positional matches. This means:

- Changing the **right lines** in the **right files** is essential — wrong files score zero.
- Changing **more lines than needed** inflates `your_changed_lines` and lowers the ratio.
- The **content** of each changed line must match exactly, including whitespace.
- **Order matters** — the same lines in a different sequence reduce the match count.

## Anti-Patterns That Kill Score

| Action | Why it hurts |
|--------|-------------|
| Reformatting surrounding code | Adds unmatched changed lines |
| Adding/removing blank lines | Shifts line positions, breaks matches |
| Changing import order | Adds unmatched diff hunks |
| Adding helpful comments | Every comment line is an unmatched changed line |
| Fixing unrelated bugs | More unmatched lines |
| Running tests and reverting failures | Wastes time, may leave unwanted changes |
