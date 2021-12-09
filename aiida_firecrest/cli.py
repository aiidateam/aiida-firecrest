"""Simple CLI for FireCrest."""
import json
import os
import posixpath
import tempfile
from pathlib import Path
from typing import Optional

import click
import firecrest as f7t
import yaml

from .transport import FirecrestTransport


class Connection:
    """Connection data FireCrest."""

    def __init__(self) -> None:
        """Initialize the connection."""
        self._path: Optional[Path] = None
        self._info: Optional[dict] = None

    def set_path(self, path: Path) -> None:
        """Set the path."""
        self._path = path

    @property
    def info(self) -> dict:
        """Get the info."""
        if self._info is not None:
            return self._info
        if self._path is None:
            raise ValueError("Path not set")
        info = json.loads(self._path.read_text())
        assert set(info).issuperset(
            {
                "url",
                "token_uri",
                "client_id",
                "client_secret",
                "machine",
            }
        )
        self._info = info
        return self._info

    @property
    def transport(self) -> FirecrestTransport:
        """Get the transport."""
        transport = FirecrestTransport(**self.info)
        if "scratch_path" in self.info:
            transport.chdir(self.info["scratch_path"], check_exists=False)
        return transport

    @property
    def client(self) -> f7t.Firecrest:
        """Get the client."""
        return self.transport._client


connection = click.make_pass_decorator(Connection, ensure=True)


@click.group()
@click.option(
    "--config",
    type=click.Path(exists=True),
    help="Path to the connection file (default: .firecrest-config.json).",
)
@connection
def main(connection, config):
    """FireCrest CLI."""
    if config is not None:
        connection.set_path(config)
    else:
        connection.set_path(Path.cwd() / ".firecrest-config.json")


@main.group("stat")
def status() -> None:
    """Status operations."""


@main.group("fs")
def fs() -> None:
    """File system operations."""


@main.group("slurm")
def slurm() -> None:
    """Slurm operations."""


@status.command("parameters")
@connection
def parameters(connection: Connection):
    """Get parameters that can be configured in environment files."""
    print(yaml.dump(connection.client.parameters()))


@status.command("services")
@connection
def services(connection: Connection):
    """List available services."""
    click.echo(yaml.dump(connection.client.all_services()))


@status.command("service")
@click.argument("service")
@connection
def service(connection: Connection, service: str):
    """Information about a service."""
    click.echo(yaml.dump(connection.client.service(service)))


@status.command("systems")
@connection
def systems(connection: Connection):
    """List available systems."""
    click.echo(yaml.dump(connection.client.all_systems()))


@status.command("system")
@click.argument("system")
@connection
def system(connection: Connection, system: str):
    """Information about a system."""
    click.echo(yaml.dump(connection.client.system(system)))


@fs.command("cwd")
@connection
def cwd(connection: Connection):
    """Get the current working directory."""
    click.echo(connection.transport.getcwd())


@fs.command("ls")
@click.argument("path", default=".")
@connection
def ls(connection: Connection, path: str):
    """List files in a path."""
    # todo want to allow for '/' to prepend folders here?
    click.echo(" ".join(connection.transport.listdir(path)))


@fs.command("stat")
@click.argument("path")
@connection
def stat(connection: Connection, path: str):
    """Get information about a file."""
    click.echo(yaml.dump(connection.transport.stat(path)))


@fs.command("chmod")
@click.argument("path")
@click.argument("mode")
@connection
def chmod(connection: Connection, path: str, mode: str):
    """Change the mode of a file."""
    connection.transport.chmod(path, mode)
    click.secho(f"Changed mode of {path} to {mode}", fg="green")


@fs.command("rm")
@click.argument("path")
@connection
def rm(connection: Connection, path: str):
    """Remove a file or directory."""
    connection.transport.remove(path)
    click.secho(f"Removed {path}", fg="green")


@fs.command("putfile")
@click.argument("source_path")
@click.argument("target_path")
@connection
def putfile(connection: Connection, source_path: str, target_path: str):
    """Upload file to the remote."""
    connection.transport.putfile(os.path.abspath(source_path), target_path)
    click.secho(f"Uploaded {source_path} to {target_path}", fg="green")


@fs.command("putfile-lg")
@click.argument("source_path")
@click.argument("target_folder")
@connection
def putfile_lg(connection: Connection, source_path: str, target_folder: str):
    """Upload file to the remote."""
    info = connection.info
    if "scratch_path" in info:
        target_folder = posixpath.join(info["scratch_path"], target_folder)
    data = connection.client.external_upload(
        info["machine"], source_path, target_folder
    )
    click.echo(str(data.object_storage_data))
    # TODO upload and polling


@fs.command("cat")
@click.argument("path")
@connection
def cat(connection: Connection, path: str):
    """Get the contents of a file."""
    with tempfile.NamedTemporaryFile() as f:
        connection.transport.getfile(path, f.name)
        click.echo(f.read())


@slurm.command("sacct")
@connection
def sacct(connection: Connection):
    """Retrieve information for all jobs."""
    click.echo(yaml.dump(connection.client.poll(connection.info["machine"])))


@slurm.command("squeue")
@connection
def squeue(connection: Connection):
    """Retrieves information for queued jobs."""
    click.echo(yaml.dump(connection.client.poll_active(connection.info["machine"])))


@slurm.command("submit")
@click.argument("path")
@click.option("--is-remote", is_flag=True, help="The path is on the remote.")
@connection
def submit(connection: Connection, path: str, is_remote: bool):
    """Submit a job script."""
    click.echo(
        connection.client.submit(
            connection.info["machine"], path, local_file=not is_remote
        )
    )
