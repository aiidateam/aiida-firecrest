"""Simple CLI for FireCrest."""
import json
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
                "scratch_path",
            }
        )
        self._info = info
        return self._info

    @property
    def transport(self) -> FirecrestTransport:
        """Get the transport."""
        return FirecrestTransport(**self.info)

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


@main.command("services")
@connection
def services(connection: Connection):
    """List available services."""
    click.echo(yaml.dump(connection.client.all_services()))


@main.command("service")
@click.argument("service")
@connection
def service(connection: Connection, service: str):
    """Information about a service."""
    click.echo(yaml.dump(connection.client.service(service)))


@main.command("systems")
@connection
def systems(connection: Connection):
    """List available systems."""
    click.echo(yaml.dump(connection.client.all_systems()))


@main.command("system")
@click.argument("system")
@connection
def system(connection: Connection, system: str):
    """Information about a system."""
    click.echo(yaml.dump(connection.client.system(system)))
