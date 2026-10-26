"""Tests for user-facing source strings."""

from __future__ import annotations

import ast
import json
from collections import defaultdict
from functools import cache
from pathlib import Path
from typing import Any

import pytest

from custom_components.meridian_energy.const import (
    SETUP_IMPORT_BUDGET_SECONDS,
    SETUP_IMPORT_TIMEOUT_SECONDS,
)

_INTEGRATION_DIR = Path(__file__).parents[1] / "custom_components" / "meridian_energy"
_ENTITY_PLATFORMS = frozenset({"sensor"})
# Abort reasons Home Assistant's own helpers raise for this flow, so the code
# never names them: _abort_if_unique_id_configured and _abort_if_unique_id_mismatch.
_IMPLICIT_ABORT_REASONS = frozenset({"already_configured", "unique_id_mismatch"})
# The text Home Assistant shows for a created entry with no description.
_IMPLICIT_CREATE_ENTRY = frozenset({"default"})


def _source_strings() -> dict[str, Any]:
    return json.loads((_INTEGRATION_DIR / "strings.json").read_text())


def test_english_translation_matches_source_strings() -> None:
    """Keep the source strings HA loads aligned with the English translation."""
    source = _source_strings()
    english = json.loads((_INTEGRATION_DIR / "translations" / "en.json").read_text())

    assert source == english
    assert source["entity"]["sensor"]["last_sync"]["name"] == "Last data update"


class _ModuleKeys:
    """Translation keys one integration module uses, collected from its AST."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.module = path.stem
        self.tree = ast.parse(path.read_text())
        self.keys: dict[str, set[str]] = defaultdict(set)
        self.next_step_ids: set[str] = set()
        self.step_methods: set[str] = set()
        self._assignments: dict[str, list[ast.expr]] = defaultdict(list)
        self._functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        self._collect_definitions()
        self._collect_keys()

    def _collect_definitions(self) -> None:
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    self._assignments[ast.unparse(target)].append(node.value)
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                self._assignments[ast.unparse(node.target)].append(node.value)
            elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                self._functions[node.name] = node
                if node.name.startswith("async_step_"):
                    self.step_methods.add(node.name.removeprefix("async_step_"))

    def _resolve(self, expr: ast.expr, seen: frozenset[str] = frozenset()) -> set[str]:
        """Return every string an expression can evaluate to, or fail loudly."""
        found = self._resolve_or_none(expr, seen)
        if found is None:
            pytest.fail(
                f"{self.path.name}:{expr.lineno}: cannot resolve the translation key "
                f"{ast.unparse(expr)!r}; use a literal, a module-level table or a "
                "variable assigned only literals"
            )
        return found

    def _resolve_or_none(  # noqa: PLR0911 - one branch per expression shape
        self, expr: ast.expr, seen: frozenset[str]
    ) -> set[str] | None:
        if isinstance(expr, ast.Constant):
            return {expr.value} if isinstance(expr.value, str) else set()
        if isinstance(expr, ast.Name | ast.Attribute):
            name = ast.unparse(expr)
            if name in seen or name not in self._assignments:
                return None
            values: set[str] = set()
            for value in self._assignments[name]:
                values |= self._resolve(value, seen | {name})
            return values
        if isinstance(expr, ast.IfExp):
            return self._resolve(expr.body, seen) | self._resolve(expr.orelse, seen)
        if isinstance(expr, ast.Dict):
            values = set()
            for value in expr.values:
                values |= self._resolve(value, seen)
            return values
        if isinstance(expr, ast.Call):
            return self._resolve_call(expr, seen)
        return None

    def _resolve_call(self, call: ast.Call, seen: frozenset[str]) -> set[str] | None:
        func = call.func
        if isinstance(func, ast.Attribute) and func.attr == "get":
            # table.get(key, default): every value of the table, and the default.
            values = self._resolve(func.value, seen)
            if len(call.args) > 1:
                values |= self._resolve(call.args[1], seen)
            return values
        if isinstance(func, ast.Name) and func.id in self._functions:
            values = set()
            for node in ast.walk(self._functions[func.id]):
                if isinstance(node, ast.Return) and node.value is not None:
                    values |= self._resolve(node.value, seen)
            return values
        return None

    def _collect_keys(self) -> None:
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                self._collect_call(node)
            elif isinstance(node, ast.Assign) and any(
                # errors["base"] = ..., and module tables such as _IMPORT_ERRORS.
                _is_base_subscript(target) or _is_error_table(target)
                for target in node.targets
            ):
                self.keys["config.error"] |= self._resolve(node.value)
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values, strict=True):
                    if isinstance(key, ast.Constant) and key.value == "base":
                        self.keys["config.error"] |= self._resolve(value)

    def _collect_call(self, call: ast.Call) -> None:
        name = _call_name(call)
        for keyword in call.keywords:
            if keyword.arg == "translation_key":
                section = (
                    f"entity.{self.module}"
                    if self.module in _ENTITY_PLATFORMS and name.endswith("Description")
                    else "exceptions"
                )
                self.keys[section] |= self._resolve(keyword.value)
            elif keyword.arg == "step_id" and name == "async_show_form":
                self.keys["config.step"] |= self._resolve(keyword.value)
            elif keyword.arg == "progress_action":
                self.keys["config.progress"] |= self._resolve(keyword.value)
            elif keyword.arg == "reason" and name == "async_abort":
                self.keys["config.abort"] |= self._resolve(keyword.value)
            elif keyword.arg == "description" and name == "async_create_entry":
                self.keys["config.create_entry"] |= self._resolve(keyword.value)
            elif keyword.arg == "next_step_id":
                self.next_step_ids |= self._resolve(keyword.value)


def _call_name(call: ast.Call) -> str:
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return ""


def _is_error_table(target: ast.expr) -> bool:
    return isinstance(target, ast.Name) and target.id.endswith("_ERRORS")


def _is_base_subscript(target: ast.expr) -> bool:
    return (
        isinstance(target, ast.Subscript)
        and isinstance(target.slice, ast.Constant)
        and target.slice.value == "base"
    )


@cache
def _used_keys() -> tuple[dict[str, frozenset[str]], frozenset[str], frozenset[str]]:
    keys: dict[str, set[str]] = defaultdict(set)
    next_step_ids: set[str] = set()
    step_methods: set[str] = set()
    for path in sorted(_INTEGRATION_DIR.glob("*.py")):
        module = _ModuleKeys(path)
        for section, values in module.keys.items():
            keys[section] |= values
        if module.module == "config_flow":
            next_step_ids |= module.next_step_ids
            step_methods |= module.step_methods
    return (
        {section: frozenset(values) for section, values in keys.items()},
        frozenset(next_step_ids),
        frozenset(step_methods),
    )


def _section(strings: dict[str, Any], dotted: str) -> dict[str, Any]:
    value: Any = strings
    for part in dotted.split("."):
        value = value.get(part, {})
    assert isinstance(value, dict)
    return value


@pytest.mark.parametrize(
    "section",
    [
        "config.abort",
        "config.create_entry",
        "config.error",
        "config.progress",
        "config.step",
        "exceptions",
        "entity.sensor",
    ],
)
def test_every_translation_key_the_code_uses_exists(section: str) -> None:
    """A key used in code but missing from strings.json shows as a raw key."""
    used, _, _ = _used_keys()
    assert used.get(section), f"no {section} keys found; the collector is broken"
    missing = sorted(used[section] - _section(_source_strings(), section).keys())
    assert not missing, f"{section} keys missing from strings.json: {missing}"


@pytest.mark.parametrize(
    "section",
    [
        "config.abort",
        "config.create_entry",
        "config.error",
        "config.progress",
        "config.step",
        "exceptions",
        "entity.sensor",
    ],
)
def test_every_source_string_is_still_used(section: str) -> None:
    """A string the code no longer uses is stale text that misleads reviewers."""
    used, _, _ = _used_keys()
    allowed = used.get(section, frozenset())
    if section == "config.abort":
        allowed |= _IMPLICIT_ABORT_REASONS
    if section == "config.create_entry":
        allowed |= _IMPLICIT_CREATE_ENTRY
    unused = sorted(_section(_source_strings(), section).keys() - allowed)
    assert not unused, f"{section} keys no code uses: {unused}"


def test_progress_done_targets_are_flow_steps() -> None:
    """Every async_show_progress_done target is a step the flow defines."""
    _, next_step_ids, step_methods = _used_keys()
    assert next_step_ids
    assert next_step_ids <= step_methods


def test_flow_form_steps_have_strings_and_methods() -> None:
    """Every step in strings.json is a step the flow can show."""
    _, _, step_methods = _used_keys()
    steps = _section(_source_strings(), "config.step")
    assert steps.keys() <= step_methods


def test_the_setup_time_limit_is_5_minutes_wherever_it_is_stated() -> None:
    """
    The setup cap is 300 s, and the dialog and README say 5 minutes.

    Every test of the cap patches it to a moment, so without this a change to
    the constant would leave the progress text, the import_timeout error and
    the README naming the old limit. Each also says that the time Home
    Assistant takes to save an import is not counted, since setup's limit
    moves on by it (the coordinator's ``_async_uncounted``): the 5 minutes
    are not the longest setup can take. The progress text names the
    import's own 4 minutes too (``SETUP_IMPORT_BUDGET_SECONDS``), which end
    it with what it imported, as the 5 minutes, a backstop, do not; an
    import they end with nothing fails as Meridian not answering in time,
    not with ``import_timeout``.
    """
    assert SETUP_IMPORT_TIMEOUT_SECONDS == 300
    assert SETUP_IMPORT_BUDGET_SECONDS == 240
    stated = f"after {SETUP_IMPORT_TIMEOUT_SECONDS // 60} minutes"
    uncounted = f"{stated}, not counting the time Home Assistant takes to save it"
    budget = f"{SETUP_IMPORT_BUDGET_SECONDS // 60} minutes"
    config = _source_strings()["config"]
    for text in (
        config["error"]["import_timeout"],
        config["progress"]["initial_import"],
    ):
        assert uncounted in text
        assert "the longest it waits" not in text
    assert budget in config["progress"]["initial_import"]
    readme = (_INTEGRATION_DIR.parents[1] / "README.md").read_text()
    assert f"still running {uncounted}" in readme
    assert f"The import has {budget}" in readme
