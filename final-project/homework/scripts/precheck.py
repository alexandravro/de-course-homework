"""Швидка перевірка «рішення взагалі написане»: без Docker, Spark і dbt. ДАНО.

    uv run python -m scripts.precheck ingest transform orchestrate

Заглушка (`raise NotImplementedError`, модель dbt з `raise_compiler_error("TODO…`, DAG без
`rides_medallion`) означає, що етапу ще нема. Тоді verify.sh і pytest зупиняються ДО того, як
підніметься будь-який контейнер: без рішення нічого запускати не потрібно.

Це лише перевірка на заглушки й синтаксис. Чи рішення правильне, вирішують тести етапу.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DAG_ID = "rides_medallion"
DAG_TASKS = ("bronze_spark", "bronze_contract", "silver", "gold", "reconcile")

LABEL = {
    "ingest": "етап 1 (ingest)",
    "transform": "етап 2 (transform)",
    "orchestrate": "етап 3 (orchestrate)",
}
# Етап 3 запускає ваші bronze_job.py і dbt-моделі, тож потребує й етапів 1, 2.
PREREQUISITES = {
    "ingest": ["ingest"],
    "transform": ["transform"],
    "orchestrate": ["ingest", "transform", "orchestrate"],
}
SPEC_SECTION = {"ingest": "2–3", "transform": "4", "orchestrate": "5"}


def _rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _stub_message(node: ast.Raise) -> str | None:
    """Текст заглушки, якщо це `raise NotImplementedError(...)`; інакше None."""
    exc = node.exc
    target = exc.func if isinstance(exc, ast.Call) else exc
    if not (isinstance(target, ast.Name) and target.id == "NotImplementedError"):
        return None
    if isinstance(exc, ast.Call) and exc.args and isinstance(exc.args[0], ast.Constant):
        return str(exc.args[0].value)
    return ""


def _python_problems(root: Path, files: tuple[str, ...]) -> list[str]:
    problems: list[str] = []
    for name in files:
        path = root / name
        if not path.exists():
            problems.append(f"{name}: файла немає")
            continue
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError as exc:
            problems.append(f"{name}: синтаксична помилка, рядок {exc.lineno}: {exc.msg}")
            continue

        def walk(node: ast.AST, scope: str, name: str = name) -> None:
            for child in ast.iter_child_nodes(node):
                inner = f"{child.name}()" if isinstance(child, ast.FunctionDef) else scope
                if isinstance(child, ast.Raise):
                    message = _stub_message(child)
                    if message is not None:
                        suffix = f" — {message}" if message else ""
                        problems.append(f"{name}: {scope} ще не реалізовано{suffix}")
                walk(child, inner)

        walk(tree, "модуль")
    return problems


def _model_problems(root: Path) -> list[str]:
    problems: list[str] = []
    for path in sorted((root / "dbt_rides" / "models").rglob("*.sql")):
        text = path.read_text()
        if "raise_compiler_error" in text and "TODO" in text:
            problems.append(f"{_rel(path, root)}: модель ще не реалізована (заглушка TODO)")
    return problems


def _dag_problems(root: Path) -> list[str]:
    path = root / "dags" / f"{DAG_ID}.py"
    name = _rel(path, root) if path.exists() else f"dags/{DAG_ID}.py"
    if not path.exists():
        return [f"{name}: файла немає"]
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError as exc:
        return [f"{name}: синтаксична помилка, рядок {exc.lineno}: {exc.msg}"]
    strings = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    if DAG_ID not in strings:
        return [f"{name}: DAG {DAG_ID} ще не створено (немає dag_id)"]
    missing = [task for task in DAG_TASKS if task not in strings]
    return [f"{name}: не знайдено задач {missing}"] if missing else []


def unfinished(stage: str, root: Path = ROOT) -> list[str]:
    """Що в цьому етапі ще заглушка. Порожній список = етап написаний (не обов'язково правильно)."""
    if stage == "ingest":
        return _python_problems(root, ("stream/consumer.py", "bronze_job.py"))
    if stage == "transform":
        return _model_problems(root)
    if stage == "orchestrate":
        return _dag_problems(root)
    raise ValueError(f"невідомий етап: {stage}")


def expand(stage: str) -> list[str]:
    """Етап разом із тими, від яких він залежить, у порядку виконання."""
    return PREREQUISITES[stage]


def main(argv: list[str]) -> int:
    stages: list[str] = []
    for requested in argv or ["orchestrate"]:
        for stage in expand(requested):
            if stage not in stages:
                stages.append(stage)
    failed = False
    for stage in stages:
        problems = unfinished(stage)
        if problems:
            failed = True
            print(f"{LABEL[stage]}: рішення ще нема (SPEC.md, розділ {SPEC_SECTION[stage]}):")
            for problem in problems:
                print(f"   - {problem}")
    if failed:
        print("\nДоки заглушки не замінено, Docker, Spark і dbt не запускаються.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
