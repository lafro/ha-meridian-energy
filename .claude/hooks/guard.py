"""
PreToolUse guard for agent sessions in this repository.

Entry point: ``guard.sh`` in the same folder, registered for the Bash tool in
``.claude/settings.json``. Input is Claude Code's PreToolUse JSON on stdin
(fields used: ``tool_name``, ``tool_input.command``, ``cwd``). Exit 0 allows the
call; exit 2 blocks it, with the reason on stderr, which the agent sees.

Why: the maintainer merges every pull request, and releases come only from the
Release workflow, whose tags are immutable. Deny rules in settings.json match a
command only as written; this hook also catches other spellings, such as
``git -C . push origin HEAD:main``, ``gh api -X PUT .../pulls/38/merge`` or
``git push origin v1.2.3``.

Blocked, in any segment of a compound command, ``sh -c '...'``, ``eval`` or
``$(...)``:

1. ``git push`` that lands on ``main``: a refspec naming main, a wildcard,
   ``--all``/``--branches``/``--mirror``, or a push with no refspec while main is
   the current branch or its push destination. If the destination cannot be
   worked out, the push is blocked.
2. Force pushes and pushes that delete remote refs, on any branch.
3. Tag pushes: ``--tags``, ``--follow-tags``, ``refs/tags/...``, a refspec that
   names a local tag or looks like a version tag (``v1.2.3``).
4. ``git tag`` that creates, moves or deletes a tag (listing is allowed).
5. ``gh pr merge``; ``gh release create|edit|upload|delete|delete-asset``;
   ``gh repo delete``; ``gh repo edit --default-branch``; ``gh workflow run``
   for the Release workflow.
6. GitHub API writes (``gh api``, or curl/wget/httpie to api.github.com) that
   merge, create/move/delete refs or tag objects, create/change/delete
   releases, dispatch the Release workflow, change rulesets or branch
   protection, write contents on main, or change or delete the repository;
   and GraphQL mutations that do the same.
7. Any Home Assistant MCP tool (``mcp__<server>__ha_*``), if this script is ever
   registered for non-Bash tools. settings.json blocks those with its own hook.

Known limits: a push run from inside a script file or a git alias is not seen;
GitHub tools that are not Bash (for example a GitHub MCP server) are not checked
here.

Python 3.9 or newer, standard library only, so it runs wherever an agent does.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

PROTECTED = "main"
WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
HA_TOOL = re.compile(r"^mcp__.+__ha_")
VERSION_LIKE = re.compile(r"^v\d")
RELEASE_WORKFLOW = re.compile(r"^release(\.ya?ml)?$", re.IGNORECASE)
TAG_PUSH = "pushes a tag; tags are created only by the Release workflow"
RELEASE_WRITES = frozenset({"create", "edit", "upload", "delete", "delete-asset"})
UNKNOWN_DEST = (
    "could not work out which branch this push goes to (not a git repo, "
    "detached HEAD, or git unavailable). Name the branch explicitly, for "
    "example `git push origin fix/<slug>`"
)
GRAPHQL_BAD = re.compile(
    r"\b(mergePullRequest|enablePullRequestAutoMerge|mergeBranch|createRef|"
    r"updateRefs?|deleteRef|createCommitOnBranch|createRelease|updateRelease|"
    r"deleteRelease)\b"
)
SHELLS = frozenset({"sh", "bash", "zsh", "dash", "ksh", "fish"})
HTTP_CLIENTS = frozenset({"curl", "wget", "http", "https", "xh"})
HTTPIE = frozenset({"http", "https", "xh"})
# Leading words that run the next word as the command, with the options of each
# wrapper that take a value.
WRAPPERS: dict[str, frozenset[str]] = {
    "env": frozenset({"-u", "--unset", "-C", "--chdir", "-S", "--split-string"}),
    "command": frozenset(),
    "builtin": frozenset(),
    "exec": frozenset({"-a"}),
    "nohup": frozenset(),
    "nice": frozenset({"-n", "--adjustment"}),
    "time": frozenset(),
    "timeout": frozenset({"-s", "--signal", "-k", "--kill-after"}),
    "stdbuf": frozenset({"-i", "-o", "-e"}),
    "sudo": frozenset({"-u", "-g", "-h", "-p", "-C", "-D", "-r", "-t", "-U"}),
    "doas": frozenset({"-u", "-C"}),
    "noglob": frozenset(),
    "nocorrect": frozenset(),
    "setsid": frozenset(),
    "caffeinate": frozenset({"-t", "-w"}),
    "xargs": frozenset({"-n", "-I", "-L", "-P", "-s", "-d", "-E", "-a"}),
    "!": frozenset(),
    "{": frozenset(),
    "}": frozenset(),
    "if": frozenset(),
    "then": frozenset(),
    "else": frozenset(),
    "elif": frozenset(),
    "do": frozenset(),
    "while": frozenset(),
    "until": frozenset(),
}
ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
GIT_VALUE_OPTIONS = frozenset(
    {
        "-c",
        "--git-dir",
        "--work-tree",
        "--namespace",
        "--exec-path",
        "--super-prefix",
        "--config-env",
        "--attr-source",
    }
)
PUSH_VALUE_OPTIONS = frozenset(
    {"-o", "--push-option", "--repo", "--receive-pack", "--exec"}
)
TAG_WRITE_LONG = frozenset(
    {
        "--annotate",
        "--sign",
        "--local-user",
        "--force",
        "--delete",
        "--message",
        "--file",
        "--edit",
        "--cleanup",
        "--create-reflog",
        "--trailer",
    }
)
TAG_LIST_LONG = frozenset(
    {
        "--list",
        "--verify",
        "--contains",
        "--no-contains",
        "--merged",
        "--no-merged",
        "--points-at",
    }
)
TAG_VALUE_LONG = frozenset({"--sort", "--format", "--color", "--column"})
GH_API_VALUE_OPTIONS = frozenset(
    {
        "-H",
        "--header",
        "-q",
        "--jq",
        "-t",
        "--template",
        "--cache",
        "--hostname",
        "-p",
        "--preview",
    }
)
HTTP_DATA_OPTIONS = frozenset(
    {
        "-d",
        "--data",
        "--data-raw",
        "--data-binary",
        "--data-urlencode",
        "--json",
        "-F",
        "--form",
        "-T",
        "--upload-file",
        "--post-data",
        "--post-file",
        "--body-data",
        "--body-file",
    }
)


# --------------------------------------------------------------------- helpers


def git(cwd: str, *args: str) -> str | None:
    """Run a read-only git query; return its stdout, or None on failure."""
    try:
        out = subprocess.run(
            ["git", "-C", cwd, *args],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


def join(cwd: str, path: str) -> str:
    """Resolve ``path`` against ``cwd`` the way a shell ``cd`` would."""
    return os.path.normpath(Path(cwd) / Path(path).expanduser())


def is_main(ref: str | None) -> bool:
    """Return whether a ref name is the protected branch."""
    ref = (ref or "").strip()
    for prefix in ("refs/heads/", "heads/"):
        if ref.startswith(prefix):
            ref = ref[len(prefix) :]
            break
    return ref == PROTECTED


def current_branch(cwd: str) -> str | None:
    """Return the checked-out branch, or None when detached or not a repo."""
    return git(cwd, "symbolic-ref", "--quiet", "--short", "HEAD")


def is_local_tag(name: str, cwd: str) -> bool:
    """Return whether ``name`` is a tag in the local repository."""
    if not name or name.startswith("refs/"):
        return False
    return git(cwd, "show-ref", "--verify", "--quiet", f"refs/tags/{name}") is not None


def split_segments(cmd: str) -> list[str]:
    """Split a shell command on ; && || | & newlines ( ) and ` outside quotes."""
    segs: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    i, n = 0, len(cmd)

    def flush() -> None:
        text = "".join(buf).strip()
        if text:
            segs.append(text)
        buf.clear()

    while i < n:
        c = cmd[i]
        if quote:
            buf.append(c)
            if c == "\\" and quote == '"' and i + 1 < n:
                buf.append(cmd[i + 1])
                i += 2
                continue
            if c == quote:
                quote = None
            i += 1
            continue
        if c == "\\" and i + 1 < n:
            buf.extend((c, cmd[i + 1]))
            i += 2
            continue
        if c in "'\"":
            quote = c
            buf.append(c)
            i += 1
            continue
        if c == "#" and (not buf or buf[-1].isspace()):
            while i < n and cmd[i] != "\n":
                i += 1
            continue
        if cmd[i : i + 2] in ("&&", "||", "$(", "|&"):
            flush()
            i += 2
            continue
        if c == "&" and ((i and cmd[i - 1] in "<>") or cmd[i + 1 : i + 2] == ">"):
            buf.append(c)
            i += 1
            continue
        if c in ";\n|&`()":
            flush()
            i += 1
            continue
        buf.append(c)
        i += 1
    flush()
    return segs


def tokenise(segment: str) -> list[str]:
    """Split one command segment into words."""
    try:
        return shlex.split(segment)
    except ValueError:  # unbalanced quotes: fall back to a quote-blind split
        return segment.replace('"', " ").replace("'", " ").split()


def strip_wrappers(tokens: list[str]) -> list[str]:
    """Drop variable assignments and wrappers such as env, nohup or timeout."""
    while tokens:
        head = tokens[0]
        if ASSIGNMENT.match(head):
            tokens = tokens[1:]
            continue
        if head not in WRAPPERS:
            break
        valued, tokens = WRAPPERS[head], tokens[1:]
        while tokens and (
            tokens[0].startswith("-") or (head == "env" and ASSIGNMENT.match(tokens[0]))
        ):
            tokens = tokens[2:] if tokens[0] in valued else tokens[1:]
        if head == "timeout" and tokens:  # the duration
            tokens = tokens[1:]
    return tokens


# -------------------------------------------------------------------- git push


def check_refspec(spec: str, cwd: str) -> str | None:
    """Return why one push refspec is blocked, or None."""
    if "$" in spec or "`" in spec:
        return (
            "the destination is a shell expansion the guard cannot resolve; "
            "name the branch literally"
        )
    if spec.startswith("+"):
        return "force push (a +refspec)"
    if ":" in spec:
        src, dst = spec.split(":", 1)
        if not src:
            return "deletes a remote branch or tag (:ref)"
    else:
        src = dst = spec
    if src.startswith(("refs/tags/", "tags/")) or dst.startswith("refs/tags/"):
        return TAG_PUSH
    if VERSION_LIKE.match(src) or VERSION_LIKE.match(dst):
        return TAG_PUSH + " (a version-like ref name)"
    if is_local_tag(src, cwd):
        return TAG_PUSH + " (the source names a local tag)"
    if "*" in dst:
        return "a wildcard refspec can include main"
    if dst in ("HEAD", "@"):
        if src not in ("HEAD", "@"):
            return "pushes to the remote HEAD, which is main"
        dst = current_branch(cwd) or ""
        if not dst:
            return UNKNOWN_DEST
    if is_main(dst):
        return "pushes to main"
    return None


def check_implicit_push(cwd: str) -> str | None:
    """Return why a push without a refspec is blocked, or None."""
    branch = current_branch(cwd)
    if branch is None:
        return UNKNOWN_DEST
    if is_main(branch):
        return "the current branch is main, so a push without a refspec goes to main"
    if git(cwd, "config", "--get", "push.default") == "matching":
        return "push.default is matching, so this push would update main as well"
    if git(cwd, "config", "--bool", "--get", "push.followTags") == "true":
        return TAG_PUSH + " (push.followTags is set)"
    dest = git(cwd, "rev-parse", "--symbolic-full-name", "@{push}")
    if dest:
        for remote in (git(cwd, "remote") or "").split():
            if dest == f"refs/remotes/{remote}/{PROTECTED}":
                return "this branch's push destination (@{push}) is main"
    configured = git(cwd, "config", "--get-regexp", r"^remote\..*\.push$") or ""
    for line in configured.splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2 and check_refspec(parts[1], cwd):
            return (
                "a configured remote.<name>.push refspec forces, tags or targets main"
            )
    return None


def check_push(args: list[str], cwd: str) -> str | None:
    """Return why a ``git push`` is blocked, or None."""
    positional: list[str] = []
    j = 0
    while j < len(args):
        a = args[j]
        if a == "--":
            positional += args[j + 1 :]
            break
        if a in PUSH_VALUE_OPTIONS:
            j += 2
            continue
        if a.startswith("--"):
            name = a.split("=", 1)[0]
            if name in ("--force", "--force-with-lease", "--force-if-includes"):
                return "force push"
            if name in ("--delete", "--prune"):
                return "deletes remote branches or tags"
            if name == "--mirror":
                return "mirror push (forces and deletes, includes main)"
            if name in ("--all", "--branches"):
                return "pushes every branch, including main"
            if name in ("--tags", "--follow-tags"):
                return TAG_PUSH
        elif a.startswith("-") and len(a) > 1:
            if "f" in a[1:]:
                return "force push"
            if "d" in a[1:]:
                return "deletes remote branches or tags"
        else:
            positional.append(a)
        j += 1
    refspecs = positional[1:]
    if not refspecs:
        return check_implicit_push(cwd)
    for spec in refspecs:
        reason = check_refspec(spec, cwd)
        if reason:
            return reason
    return None


def check_tag(args: list[str]) -> str | None:
    """Return why a ``git tag`` is blocked (it writes a tag), or None."""
    listing = False
    positional: list[str] = []
    i = 0
    while i < len(args):
        a = args[i]
        name = a.split("=", 1)[0]
        if a == "--":
            positional += args[i + 1 :]
            break
        if a.startswith("--"):
            if name in TAG_WRITE_LONG:
                return "creates, moves or deletes a tag"
            if name in TAG_LIST_LONG:
                listing = True
            elif name in TAG_VALUE_LONG and "=" not in a:
                i += 1
        elif a.startswith("-") and len(a) > 1:
            flags = a[1:]
            if any(flag in "asfdmFeu" for flag in flags):
                return "creates, moves or deletes a tag"
            if any(flag in "lvn" for flag in flags):
                listing = True
        else:
            positional.append(a)
        i += 1
    if positional and not listing:
        return "creates a tag"
    return None


def check_git(tokens: list[str], cwd: str) -> str | None:
    """Return why a git command is blocked, or None."""
    i, here = 1, cwd
    while i < len(tokens) and tokens[i].startswith("-"):
        opt = tokens[i]
        if opt == "-C" and i + 1 < len(tokens):
            here = join(here, tokens[i + 1])
            i += 2
        elif opt in GIT_VALUE_OPTIONS:
            i += 2
        else:
            i += 1
    if i >= len(tokens):
        return None
    if tokens[i] == "push":
        return check_push(tokens[i + 1 :], here)
    if tokens[i] == "tag":
        return check_tag(tokens[i + 1 :])
    return None


# ------------------------------------------------------------------ GitHub API


def check_api_write(
    method: str | None, endpoint: str, fields: dict[str, str], *, opaque: bool
) -> str | None:
    """Return why a GitHub REST write is blocked, or None."""
    method = (method or "GET").upper()
    if method not in WRITE_METHODS:
        return None
    path = re.sub(r"^(https?://)?api\.github\.com", "", endpoint)
    path = path.split("?", 1)[0].strip("/")
    reason = None
    if re.search(r"(^|/)pulls/[^/]+/merge$", path):
        reason = "merges a pull request through the API; the maintainer merges"
    elif re.search(r"(^|/)merges$", path):
        reason = "merges branches through the API"
    elif re.search(r"(^|/)git/refs(/|$)", path):
        reason = (
            "creates, moves or deletes a branch or tag through the API; push "
            "branches with git, and tags come only from the Release workflow"
        )
    elif re.search(r"(^|/)git/tags$", path):
        reason = "creates a tag object; tags come only from the Release workflow"
    elif re.search(r"(^|/)releases(/|$)", path):
        reason = "creates, changes or deletes a release outside the Release workflow"
    elif match := re.search(r"(^|/)actions/workflows/([^/]+)/dispatches$", path):
        if RELEASE_WORKFLOW.match(match.group(2)):
            reason = "dispatches the Release workflow; the maintainer releases"
    elif re.search(r"(^|/)(rulesets|branches/[^/]+/protection)(/|$)", path):
        reason = "changes rulesets or branch protection"
    elif re.search(r"(^|/)contents(/|$)", path):
        branch = fields.get("branch")
        if branch is None:
            reason = "writes repository contents without naming a branch, so on main"
        elif is_main(branch):
            reason = "writes repository contents on main"
    elif re.search(r"(^|/)branches/" + PROTECTED + "(/|$)", path):
        reason = "changes main's branch settings"
    elif re.fullmatch(r"repos/[^/]+/[^/]+", path):
        if method == "DELETE":
            reason = "deletes the repository"
        elif "default_branch" in fields or (opaque and not fields):
            reason = "may change the default branch"
    return reason


def read_input_file(name: str, cwd: str) -> str:
    """Read a ``gh api --input`` body so GraphQL mutations in it are seen."""
    if not name or name == "-":
        return ""
    try:
        return (Path(cwd) / name).read_text(errors="replace")[:200_000]
    except OSError:
        return ""


def check_gh_api(api: list[str], cwd: str) -> str | None:
    """Return why a ``gh api`` call is blocked, or None."""
    method: str | None = None
    fields: dict[str, str] = {}
    body: list[str] = []
    opaque = False
    endpoint: str | None = None

    def add_field(pair: str) -> None:
        key, _, value = pair.partition("=")
        fields[key] = value
        body.append(value)

    i = 0
    while i < len(api):
        a = api[i]
        nxt = api[i + 1] if i + 1 < len(api) else ""
        if a in ("-X", "--method"):
            method, i = nxt, i + 2
        elif a.startswith("--method="):
            method, i = a.split("=", 1)[1], i + 1
        elif a.startswith("-X") and len(a) > 2:
            method, i = a[2:], i + 1
        elif a in ("-f", "-F", "--field", "--raw-field"):
            add_field(nxt)
            i += 2
        elif a.startswith(("--field=", "--raw-field=")):
            add_field(a.split("=", 1)[1])
            i += 1
        elif a[:2] in ("-f", "-F") and len(a) > 2 and not a.startswith("--"):
            add_field(a[2:])
            i += 1
        elif a == "--input" or a.startswith("--input="):
            name = nxt if a == "--input" else a.split("=", 1)[1]
            opaque = True
            body.append(read_input_file(name, cwd))
            i += 2 if a == "--input" else 1
        elif a in GH_API_VALUE_OPTIONS:
            i += 2
        elif a.startswith("-"):
            i += 1
        else:
            endpoint = endpoint or a
            i += 1
    if endpoint is None:
        return None
    if method is None:
        method = "POST" if (fields or opaque) else "GET"
    if endpoint.strip("/") == "graphql":
        if GRAPHQL_BAD.search(" ".join(body)):
            return "a GraphQL mutation that merges, moves refs or changes releases"
        return None
    return check_api_write(method, endpoint, fields, opaque=opaque)


def check_gh(tokens: list[str], cwd: str) -> str | None:
    """Return why a gh command is blocked, or None."""
    args = tokens[1:]
    words: list[str] = []
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("-R", "--repo"):
            i += 2
            continue
        if not a.startswith("-"):
            words.append(a)
        i += 1
    head = words[:2]
    if head == ["pr", "merge"]:
        return "gh pr merge: the maintainer merges every pull request"
    if head[:1] == ["release"] and len(words) > 1 and words[1] in RELEASE_WRITES:
        return "creates, changes or deletes a release outside the Release workflow"
    if head == ["repo", "delete"]:
        return "deletes the repository"
    if head == ["repo", "edit"] and any(a.startswith("--default-branch") for a in args):
        return "changes the default branch"
    if head == ["workflow", "run"] and len(words) > 2:
        if RELEASE_WORKFLOW.match(Path(words[2]).name):
            return "dispatches the Release workflow; the maintainer releases"
        return None
    if not words or words[0] != "api":
        return None
    return check_gh_api(args[args.index("api") + 1 :], cwd)


def check_http(tokens: list[str]) -> str | None:
    """Return why a curl/wget/httpie call to the GitHub API is blocked, or None."""
    url = next((a for a in tokens[1:] if "api.github.com" in a), None)
    if url is None:
        return None
    prog = Path(tokens[0]).name
    method: str | None = None
    data: list[str] = []
    if (
        prog in HTTPIE
        and len(tokens) > 1
        and tokens[1].upper() in WRITE_METHODS | {"GET"}
    ):
        method = tokens[1]
    for i, a in enumerate(tokens[1:], start=1):
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        if a in ("-X", "--request", "--method"):
            method = nxt
        elif a.startswith(("--request=", "--method=")):
            method = a.split("=", 1)[1]
        elif a.startswith("-X") and len(a) > 2 and not a.startswith("--"):
            method = a[2:]
        elif a in HTTP_DATA_OPTIONS:
            data.append(nxt)
        elif a.startswith(
            ("--data=", "--data-raw=", "--json=", "--post-data=", "--body-data=")
        ):
            data.append(a.split("=", 1)[1])
        elif a.startswith("-d") and len(a) > 2:
            data.append(a[2:])
        elif prog in HTTPIE and re.match(r"^[\w\[\]]+:?=", a):
            key, _, value = a.partition("=")
            data.append(json.dumps({key.rstrip(":"): value}))
    if method is None:
        method = "POST" if data else "GET"
    fields: dict[str, str] = {}
    opaque = False
    for item in data:
        try:
            parsed = json.loads(item)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            fields.update(
                {
                    str(k): str(v).lower() if isinstance(v, bool) else str(v)
                    for k, v in parsed.items()
                }
            )
        else:
            opaque = True
    if url.split("?", 1)[0].rstrip("/").endswith("/graphql"):
        if method.upper() in WRITE_METHODS and GRAPHQL_BAD.search(" ".join(data)):
            return "a GraphQL mutation that merges, moves refs or changes releases"
        return None
    return check_api_write(method, url, fields, opaque=opaque)


# ---------------------------------------------------------------------- driver


def analyse(command: str, cwd: str, depth: int = 0) -> str | None:
    """Return why a shell command is blocked, or None."""
    if depth > 4:
        return None
    # Command substitutions run even inside double quotes; check them on their own.
    for inner in re.findall(r"\$\(([^()]*)\)|`([^`]*)`", command):
        reason = analyse(inner[0] or inner[1], cwd, depth + 1)
        if reason:
            return reason
    here = cwd
    for segment in split_segments(command):
        tokens = strip_wrappers(tokenise(segment))
        if not tokens:
            continue
        prog = Path(tokens[0]).name
        if prog in ("cd", "pushd") and len(tokens) > 1:
            here = join(here, tokens[1])
            continue
        reason = None
        if prog == "git":
            reason = check_git(tokens, here)
        elif prog == "gh":
            reason = check_gh(tokens, here)
        elif prog in HTTP_CLIENTS:
            reason = check_http(tokens)
        elif prog == "eval":
            reason = analyse(" ".join(tokens[1:]), here, depth + 1)
        elif prog in SHELLS:
            for k, tok in enumerate(tokens[1:-1], start=1):
                if re.fullmatch(r"-[a-zA-Z]*c[a-zA-Z]*", tok):
                    reason = analyse(tokens[k + 1], here, depth + 1)
                    break
        if reason:
            return reason
    return None


def looks_risky(text: str) -> bool:
    """Text-only check, used when the full analysis cannot run."""
    return bool(
        re.search(r"\bgit\b[^\n]*\b(push|tag)\b", text)
        or re.search(r"\bgh\b[^\n]*\b(merge|delete|release|workflow)\b", text)
        or re.search(
            r"(\bgh\b[^\n]*\bapi\b|api\.github\.com)[^\n]*"
            r"(-X|--method|--request|-f |-F |--field|--input|-d )",
            text,
        )
    )


def block(reason: str) -> None:
    """Report a blocked call and exit with Claude Code's blocking status."""
    sys.stderr.write(
        "Blocked by this repository's agent guard (.claude/hooks/guard.py): "
        f"{reason}.\n"
        "Agents here never push to main, push or create tags, force-push, delete "
        "remote refs, merge pull requests or publish releases. Push a "
        "<type>/<slug> branch and open a pull request; the maintainer merges it "
        "and runs the Release workflow.\n"
    )
    sys.exit(2)


def main() -> int:
    """Read one PreToolUse event from stdin and allow or block it."""
    raw = sys.stdin.read()
    try:
        data = json.loads(raw)
    except ValueError:
        if looks_risky(raw):
            block(
                "the hook input was not valid JSON, and it mentions a push, tag, "
                "merge, release or GitHub API write"
            )
        return 0
    if not isinstance(data, dict):
        return 0
    tool = str(data.get("tool_name") or "")
    if HA_TOOL.match(tool):
        block(
            "Home Assistant tools are not used from this repository; live checks "
            "happen in the home-configuration repository's sessions"
        )
    if tool != "Bash":
        return 0
    command = str((data.get("tool_input") or {}).get("command") or "")
    cwd = str(data.get("cwd") or Path.cwd())
    try:
        reason = analyse(command, cwd)
    except Exception as exc:  # never fail open on a command that looks risky
        reason = (
            f"guard error ({type(exc).__name__}) on a command that looks like a "
            "push, tag, merge, release or GitHub API write"
            if looks_risky(command)
            else None
        )
    if reason:
        block(reason)
    return 0


if __name__ == "__main__":
    sys.exit(main())
