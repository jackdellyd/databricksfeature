from pathlib import Path
from typing import Optional

import typer

from pt_databricksfeature.train import train


app = typer.Typer()


def version_callback(value: bool):
    if value:
        with open(Path(__file__).parent / "VERSION", mode="r") as file:
            version = file.read().replace("\n", "")
        typer.echo(f"{version}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
    )
):
    typer.echo("pt_databricksfeature")


@app.command("train")
def train_console(
    data_path: Path = typer.Option(..., "--data-path"),
    output_path: Path = typer.Option(..., "--output-path"),
):
    train(data_path, output_path)
