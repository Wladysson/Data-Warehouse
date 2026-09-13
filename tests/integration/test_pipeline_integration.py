from pathlib import Path

from src.orchestration.pipeline import PipelineOrchestrator, PipelineStatus


def test_pipeline_orchestrator_initializes():
    orchestrator = PipelineOrchestrator()

    assert orchestrator is not None
    assert orchestrator.status == PipelineStatus.IDLE


def test_pipeline_healthcheck_returns_status():
    orchestrator = PipelineOrchestrator()

    health = orchestrator.healthcheck()

    assert health is not None
    assert "status" in health


def test_pipeline_can_describe_configuration():
    orchestrator = PipelineOrchestrator()

    description = orchestrator.describe()

    assert description is not None
    assert isinstance(description, dict)


def test_pipeline_history_is_initially_empty():
    orchestrator = PipelineOrchestrator()

    assert orchestrator.history() == []


def test_pipeline_reset_clears_execution_state():
    orchestrator = PipelineOrchestrator()

    orchestrator.reset()

    assert orchestrator.status == PipelineStatus.IDLE
    assert orchestrator.history() == []


def test_pipeline_project_root_exists():
    project_root = Path(__file__).resolve().parents[2]

    assert project_root.exists()
    assert (project_root / "src").exists()
    assert (project_root / "configs").exists()
    assert (project_root / "data").exists()