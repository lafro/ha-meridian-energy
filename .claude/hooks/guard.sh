#!/bin/sh
# PreToolUse guard for the Bash tool, registered in .claude/settings.json.
#
# Blocks (exit 2, reason on stderr, which the agent sees) pushes to main, tag
# pushes and tag writes, force pushes and remote-ref deletion, merges, release
# writes and Release workflow dispatches, including GitHub API spellings of the
# same. Allows everything else (exit 0). The rules and their known limits are
# in guard.py next to this file; tests/test_agent_guard.py covers them.
#
# If python3 or guard.py is missing, or the check crashes, it falls back to a
# text check that blocks anything that looks like a push, tag, merge, release
# or GitHub API write.

dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
input=$(cat)

if command -v python3 >/dev/null 2>&1 && [ -f "$dir/guard.py" ]; then
  printf '%s' "$input" | python3 -I "$dir/guard.py"
  status=$?
  if [ "$status" -eq 0 ] || [ "$status" -eq 2 ]; then
    exit "$status"
  fi
fi

if printf '%s' "$input" | grep -Eq 'git[^"]*(push|tag)|gh[^"]*(merge|delete|release|workflow)|api\.github\.com|gh[^"]* api[^"]*(-X|--method|-f |-F |--field|--input)'; then
  echo "Blocked by this repository's agent guard (.claude/hooks/guard.sh): its full check could not run (python3 missing or failed), and this command looks like a push, tag, merge, release or GitHub API write. Fix the guard, or ask the maintainer to run the command." >&2
  exit 2
fi
exit 0
