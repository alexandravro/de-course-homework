"""Перевірка самої перевірки «рішення написане» (scripts/precheck.py). Docker не потрібен."""

from __future__ import annotations

from pathlib import Path

from scripts.precheck import expand, unfinished

TASKS = '["bronze_spark", "bronze_contract", "silver", "gold", "reconcile"]'


def _tree(root: Path, *, done: bool) -> Path:
    (root / "stream").mkdir(parents=True)
    consumer = "def f() -> int:\n    return 1\n"
    if not done:
        consumer = "def f() -> int:\n    raise NotImplementedError('TODO (1): f')\n"
    (root / "stream" / "consumer.py").write_text(consumer)
    (root / "bronze_job.py").write_text("X = 1\n")
    models = root / "dbt_rides" / "models" / "silver"
    models.mkdir(parents=True)
    model = "select 1\n" if done else '{{ exceptions.raise_compiler_error("TODO: events") }}\n'
    (models / "events.sql").write_text(model)
    (root / "dags").mkdir()
    dag = f'DAG_ID = "rides_medallion"\nTASKS = {TASKS}\n' if done else "# TODO: rides_medallion\n"
    (root / "dags" / "rides_medallion.py").write_text(dag)
    return root


def test_stubs_are_reported_in_every_stage(tmp_path: Path) -> None:
    root = _tree(tmp_path, done=False)
    assert any("f()" in p and "TODO (1)" in p for p in unfinished("ingest", root))
    assert any("events.sql" in p for p in unfinished("transform", root))
    assert any("DAG rides_medallion" in p for p in unfinished("orchestrate", root))


def test_finished_tree_has_no_problems(tmp_path: Path) -> None:
    root = _tree(tmp_path, done=True)
    for stage in ("ingest", "transform", "orchestrate"):
        assert unfinished(stage, root) == [], stage


def test_orchestrate_needs_the_previous_stages() -> None:
    assert expand("orchestrate") == ["ingest", "transform", "orchestrate"]
    assert expand("transform") == ["transform"]


def test_syntax_error_is_reported_instead_of_crashing(tmp_path: Path) -> None:
    root = _tree(tmp_path, done=True)
    (root / "stream" / "consumer.py").write_text("def broken(:\n")
    assert any("синтаксична помилка" in p for p in unfinished("ingest", root))


def test_dag_without_all_tasks_is_reported(tmp_path: Path) -> None:
    root = _tree(tmp_path, done=True)
    (root / "dags" / "rides_medallion.py").write_text(
        'DAG_ID = "rides_medallion"\nT = ["silver"]\n'
    )
    problems = unfinished("orchestrate", root)
    assert problems
    assert "bronze_spark" in problems[0]
