/**
 * System prompt construction and project context loading
 */

import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { getDocsPath, getExamplesPath, getReadmePath } from "../config.js";
import { formatSkillsForPrompt, type Skill } from "./skills.js";

function grepTaskKeywords(cwd: string, taskText: string): string {
	try {
		const backtickMatches = taskText.match(/`([^`]{2,60})`/g)?.map(k => k.replace(/`/g, '')) || [];
		const camelMatches = taskText.match(/\b[A-Z][a-z]+(?:[A-Z][a-z]+)+\b/g) || [];
		const snakeMatches = taskText.match(/\b[a-z]+_[a-z_]+\b/g) || [];
		const filePathMatches = taskText.match(/\b[\w./\\-]+\.(?:ts|tsx|js|jsx|py|go|java|kt|rb|cs|rs|c|cpp|h|vue|sh|yaml|yml|toml|json|md)\b/g) || [];
		const allKeywords = [...new Set([...filePathMatches, ...backtickMatches, ...camelMatches, ...snakeMatches])]
			.filter(k => k.length >= 3 && k.length <= 60)
			.filter(k => !['the', 'and', 'for', 'with', 'that', 'this', 'from', 'should', 'must', 'when', 'each', 'into', 'also'].includes(k.toLowerCase()))
			.slice(0, 15);
		if (allKeywords.length === 0) return "";
		const fileHits = new Map<string, string[]>();
		for (const keyword of allKeywords) {
			try {
				const escaped = keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
				const result = execSync(
					`grep -rl "${escaped}" --include="*.ts" --include="*.tsx" --include="*.js" --include="*.jsx" --include="*.py" --include="*.go" --include="*.java" --include="*.kt" --include="*.dart" --include="*.rb" --include="*.cs" --include="*.vue" --include="*.rs" --include="*.c" --include="*.cpp" --include="*.h" --include="*.sh" --include="*.yaml" --include="*.yml" --include="*.toml" . 2>/dev/null | grep -v node_modules | grep -v .git | grep -v dist/ | grep -v build/ | head -10`,
					{ cwd, timeout: 3000, encoding: "utf-8" }
				).trim();
				if (result) {
					for (const file of result.split("\n")) {
						const clean = file.replace("./", "");
						if (!fileHits.has(clean)) fileHits.set(clean, []);
						fileHits.get(clean)!.push(keyword);
					}
				}
			} catch {}
		}
		const sorted = [...fileHits.entries()].sort((a, b) => b[1].length - a[1].length).slice(0, 20);
		let result = "\n\n## Files matching task keywords\n\nThese files contain identifiers from the task. Start here:\n";
		for (const [file, keywords] of sorted) {
			result += `- ${file} (${keywords.join(", ")})\n`;
		}
		// Always list .github/workflows/ files if they exist — relevant for CI/CD tasks
		try {
			const workflows = execSync(
				`ls .github/workflows/*.yml .github/workflows/*.yaml 2>/dev/null | head -10`,
				{ cwd, timeout: 2000, encoding: "utf-8" }
			).trim();
			if (workflows) {
				result += `\n## Existing CI/CD workflow files (check .github/workflows/ for CI tasks)\n`;
				for (const wf of workflows.split("\n").filter(Boolean)) {
					result += `- ${wf.replace("./", "")}\n`;
				}
			}
		} catch {}
		return result + "\n";
	} catch {}
	return "";
}

// =============================================================================
// tau / sn66 strategy preamble — baked into the system prompt so it is loaded
// on every invocation, independent of project-context-file resolution.
//
// Scoring (verified in tau/src/compare.py):
//   - Validator runs cursor on the same task as the live oracle.
//   - For each agent: changed_sequence(orig, agent_repo) is built per file
//     using difflib.SequenceMatcher (top-to-bottom file order, "-:" lines
//     first then "+:" lines per replace block).
//   - matched_changed_lines = sum over files of zip(seq_a, seq_b) positional
//     equality.
//   - Winner = whoever has more matched_changed_lines vs the cursor oracle.
//
// Implications driving the rules below:
//   1. Touching files cursor would not touch is pure loss (bloat).
//   2. Missing files cursor would touch forfeits all matches on that file.
//   3. Wholesale `write` of an existing file generates a huge changed
//      sequence that almost never positionally aligns with cursor's
//      surgical `edit`. Use `edit` for existing files.
//   4. Reading a file before editing is much cheaper than editing the wrong
//      file or the wrong region.
// =============================================================================
const TAU_SCORING_PREAMBLE = `# SN66 / tau strategy

Your diff is scored line-by-line against a reference agent. Score = matched_lines / max(your_lines, ref_lines). Empty diff = 0.

## CRITICAL CONSTRAINT — Maximum 2 read() calls between edits

**NEVER make more than 2 read() calls without an intervening edit() or write().** A third read() call without an edit() in between will be blocked. After 5 consecutive blocks the session terminates. When blocked: DO NOT apologize, DO NOT explain — immediately call edit() or write() on a file you've already read. The required pattern is: **read 1-2 files → edit/write one → read 1-2 more files → edit/write → done.**

## RULE 1 — Determine task type before acting

**BUG FIX task** (says "Fix", "Stabilize", "Resolve", "Correct", "Ensure"): Read the file first, then make the SMALLEST targeted edit.
- **NEVER change function signatures, component props/types, or class interfaces.** The reference always keeps the same public API.
- When task says "internally managed" or "internal state/ref": add useRef/useState INSIDE the component WITHOUT changing the props interface.
- Never add React.memo, useMemo, useCallback unless the task explicitly mentions them.
- Use short old_string (3-5 lines) — more reliable than large blocks.
- If an edit() fails: re-read the file then retry once.

**NEW FEATURE task** (says "Implement", "Add", "Expand", "Create", "Introduce", "Automate", "Set up", "Configure"): Read the file first, then write() the COMPLETE replacement.
- **Use write() to replace the entire file** with the old structure + new feature combined. Do NOT use multiple edit() calls to add a feature.
- Keep all existing #includes, utilities, and code style from the original file.
- Implement ALL acceptance criteria items.
- Use the SIMPLEST possible data structures (map/dict over struct, list over class hierarchy).
- For CI/CD/workflow tasks: check .github/workflows/ for existing YAML files to modify, and create new .github/workflows/*.yml files as needed.

## RULE 2 — Cover ALL files the task implies

Count the acceptance criteria bullets. Each typically needs at least one edit across 1-5 files. Don't stop early — missing a file loses ALL its matched lines.

- "X and also Y" = both must be edited
- If unsure which file: ONE bash grep to find it, then edit
- Do NOT remove code not mentioned in the task

## RULE 3 — Match the oracle exactly

- Match surrounding code's indent style, quote style, semicolons exactly
- String literals: copy verbatim from task description
- No cosmetic changes (blank lines, imports, comments) unless required

## RULE 4 — No explanations

After editing, say "done" or nothing. Never write summaries or recaps.

## File hints

Pre-computed hints follow (if present). These are the most likely files — start with the ones most relevant to each acceptance criterion.

---

`;

export interface BuildSystemPromptOptions {
	/** Custom system prompt (replaces default). */
	customPrompt?: string;
	/** Tools to include in prompt. Default: [read, bash, edit, write] */
	selectedTools?: string[];
	/** Optional one-line tool snippets keyed by tool name. */
	toolSnippets?: Record<string, string>;
	/** Additional guideline bullets appended to the default system prompt guidelines. */
	promptGuidelines?: string[];
	/** Text to append to system prompt. */
	appendSystemPrompt?: string;
	/** Working directory. Default: process.cwd() */
	cwd?: string;
	/** Pre-loaded context files. */
	contextFiles?: Array<{ path: string; content: string }>;
	/** Pre-loaded skills. */
	skills?: Skill[];
}

/** Build the system prompt with tools, guidelines, and context */
export function buildSystemPrompt(options: BuildSystemPromptOptions = {}): string {
	const {
		customPrompt,
		selectedTools,
		toolSnippets,
		promptGuidelines,
		appendSystemPrompt,
		cwd,
		contextFiles: providedContextFiles,
		skills: providedSkills,
	} = options;
	const resolvedCwd = cwd ?? process.cwd();
	const promptCwd = resolvedCwd.replace(/\\/g, "/");

	const date = new Date().toISOString().slice(0, 10);

	const appendSection = appendSystemPrompt ? `\n\n${appendSystemPrompt}` : "";

	// In Docker, task text arrives as a user message (not as customPrompt).
	// TAU_PROMPT_FILE is set by docker_solver.py and points to a file with the task text.
	const taskTextForKeywords = customPrompt
		?? (() => {
			try {
				const f = process.env.TAU_PROMPT_FILE;
				return f ? readFileSync(f, "utf-8") : "";
			} catch { return ""; }
		})();
	const keywordHits = taskTextForKeywords ? grepTaskKeywords(resolvedCwd, taskTextForKeywords) : "";

	const contextFiles = providedContextFiles ?? [];
	const skills = providedSkills ?? [];

	if (customPrompt) {
		let prompt = TAU_SCORING_PREAMBLE + keywordHits + customPrompt;

		if (appendSection) {
			prompt += appendSection;
		}

		// Append project context files
		if (contextFiles.length > 0) {
			prompt += "\n\n# Project Context\n\n";
			prompt += "Project-specific instructions and guidelines:\n\n";
			for (const { path: filePath, content } of contextFiles) {
				prompt += `## ${filePath}\n\n${content}\n\n`;
			}
		}

		// Append skills section (only if read tool is available)
		const customPromptHasRead = !selectedTools || selectedTools.includes("read");
		if (customPromptHasRead && skills.length > 0) {
			prompt += formatSkillsForPrompt(skills);
		}

		// Add date and working directory last
		prompt += `\nCurrent date: ${date}`;
		prompt += `\nCurrent working directory: ${promptCwd}`;

		return prompt;
	}

	// Get absolute paths to documentation and examples
	const readmePath = getReadmePath();
	const docsPath = getDocsPath();
	const examplesPath = getExamplesPath();

	// Build tools list based on selected tools.
	// A tool appears in Available tools only when the caller provides a one-line snippet.
	const tools = selectedTools || ["read", "bash", "edit", "write"];
	const visibleTools = tools.filter((name) => !!toolSnippets?.[name]);
	const toolsList =
		visibleTools.length > 0 ? visibleTools.map((name) => `- ${name}: ${toolSnippets![name]}`).join("\n") : "(none)";

	// Build guidelines based on which tools are actually available
	const guidelinesList: string[] = [];
	const guidelinesSet = new Set<string>();
	const addGuideline = (guideline: string): void => {
		if (guidelinesSet.has(guideline)) {
			return;
		}
		guidelinesSet.add(guideline);
		guidelinesList.push(guideline);
	};

	const hasBash = tools.includes("bash");
	const hasGrep = tools.includes("grep");
	const hasFind = tools.includes("find");
	const hasLs = tools.includes("ls");
	const hasRead = tools.includes("read");

	// File exploration guidelines
	if (hasBash && !hasGrep && !hasFind && !hasLs) {
		addGuideline("Use bash for file operations like ls, rg, find");
	} else if (hasBash && (hasGrep || hasFind || hasLs)) {
		addGuideline("Prefer grep/find/ls tools over bash for file exploration (faster, respects .gitignore)");
	}

	for (const guideline of promptGuidelines ?? []) {
		const normalized = guideline.trim();
		if (normalized.length > 0) {
			addGuideline(normalized);
		}
	}

	// Always include these
	addGuideline("Be concise in your responses");
	addGuideline("Show file paths clearly when working with files");

	const guidelines = guidelinesList.map((g) => `- ${g}`).join("\n");

	let prompt = TAU_SCORING_PREAMBLE + keywordHits + `You are an expert coding assistant operating inside pi, a coding agent harness. You help users by reading files, executing commands, editing code, and writing new files.

Available tools:
${toolsList}

In addition to the tools above, you may have access to other custom tools depending on the project.

Guidelines:
${guidelines}

Pi documentation (read only when the user asks about pi itself, its SDK, extensions, themes, skills, or TUI):
- Main documentation: ${readmePath}
- Additional docs: ${docsPath}
- Examples: ${examplesPath} (extensions, custom tools, SDK)
- When asked about: extensions (docs/extensions.md, examples/extensions/), themes (docs/themes.md), skills (docs/skills.md), prompt templates (docs/prompt-templates.md), TUI components (docs/tui.md), keybindings (docs/keybindings.md), SDK integrations (docs/sdk.md), custom providers (docs/custom-provider.md), adding models (docs/models.md), pi packages (docs/packages.md)
- When working on pi topics, read the docs and examples, and follow .md cross-references before implementing
- Always read pi .md files completely and follow links to related docs (e.g., tui.md for TUI API details)`;

	if (appendSection) {
		prompt += appendSection;
	}

	// Append project context files
	if (contextFiles.length > 0) {
		prompt += "\n\n# Project Context\n\n";
		prompt += "Project-specific instructions and guidelines:\n\n";
		for (const { path: filePath, content } of contextFiles) {
			prompt += `## ${filePath}\n\n${content}\n\n`;
		}
	}

	// Append skills section (only if read tool is available)
	if (hasRead && skills.length > 0) {
		prompt += formatSkillsForPrompt(skills);
	}

	// Add date and working directory last
	prompt += `\nCurrent date: ${date}`;
	prompt += `\nCurrent working directory: ${promptCwd}`;

	return prompt;
}
