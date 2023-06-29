from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
from subprocess import check_call
from typing import Protocol


class CliArgs(Protocol):
    folder: str
    git_tag: str
    git_url: str
    build: bool


def parse_args(args: list[str] | None = None) -> CliArgs:
    """Parse the command line arguments."""
    parser = ArgumentParser(description="Create a FirecREST demo server.")
    parser.add_argument(
        "--folder",
        default=".demo-server",
        type=str,
        help="The folder to clone FirecREST into.",
    )
    parser.add_argument(
        "--git-url",
        type=str,
        default="https://github.com/eth-cscs/firecrest.git",
        help="The URL to clone FirecREST from.",
    )
    parser.add_argument(
        "--git-tag",
        type=str,
        default="v1.13.0",
        help="The tag to checkout FirecREST at.",
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="Don't build the docker environment.",
    )
    return parser.parse_args(args)


def main(args: list[str] | None = None):
    """A CLI to generate a FirecREST demo server."""
    # use argparse to get the folder to clone firecrest into
    parsed = parse_args(args)

    folder = Path(parsed.folder).absolute()

    if not folder.exists():
        print(f"Cloning FirecREST into {parsed.folder}")
        check_call(
            ["git", "clone", "--branch", parsed.git_tag, parsed.git_url, str(folder)]
        )
    else:
        print(f"FirecREST already exists in {folder!r}")

    # build the docker environment
    if not parsed.build:
        print("Skipping building the docker environment")
    else:
        print("Building the docker environment")
        check_call(["docker-compose", "build"], cwd=(folder / "deploy" / "demo"))

    # ensure permissions of SSH keys (chmod 400)
    print("Ensuring permissions of SSH keys")
    # tester/deploy/test-build/environment/keys/ca-key
    folder.joinpath("deploy", "test-build", "environment", "keys", "ca-key").chmod(
        0o400
    )
    folder.joinpath("deploy", "test-build", "environment", "keys", "user-key").chmod(
        0o400
    )

    # run the docker environment
    print("Running the docker environment")
    # could fail if required port in use
    # on MaOS, can use e.g. `lsof -i :8080` to check
    # TODO on MacOS port 7000 is used by AirPlay (afs3-fileserver),
    # https://github.com/cookiecutter/cookiecutter-django/issues/3499
    check_call(["docker-compose", "up", "--detach"], cwd=(folder / "deploy" / "demo"))


if __name__ == "__main__":
    main()
