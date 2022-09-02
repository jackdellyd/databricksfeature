from pathlib import Path
from typing import Tuple

import pytest

from pt_databricksfeature.utils import logger


@pytest.mark.slow
def test_full_classification_experiment(tmp_path: Path, dataset_path: Tuple) -> None:
    from typer.testing import CliRunner

    from pt_databricksfeature.console import app

    command_line = f"train " f"--data-path {dataset_path} " f"--output-path {tmp_path} "
    logger.info(command_line)
    runner = CliRunner()
    result = runner.invoke(app, args=command_line.split())
    assert result.exit_code == 0
    required_files = [
        "estimator.pkl",
    ]
    for f in required_files:
        assert (tmp_path / f).is_file()


def test_version() -> None:
    from typer.testing import CliRunner

    from pt_databricksfeature.console import app

    runner = CliRunner()

    result = runner.invoke(
        app,
        args=["--version"],
    )

    assert result.exit_code == 0
