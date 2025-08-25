################################################################################
# Copyright (c), The AiiDA team. All rights reserved.                          #
# This file is part of the AiiDA code.                                         #
#                                                                              #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-firecrest #
# For further information on the license, see the LICENSE.txt file             #
# For further information please visit http://www.aiida.net                    #
################################################################################
"""Check that the GitHub release tag matches the package version."""

import argparse
import ast
from pathlib import Path


def get_version_from_module(content: str) -> str:
    """Get the __version__ value from a module."""
    # adapted from setuptools/config.py
    try:
        module = ast.parse(content)
    except SyntaxError as err:
        raise OSError("Unable to parse module") from err
    try:
        return next(
            ast.literal_eval(statement.value)
            for statement in module.body
            if isinstance(statement, ast.Assign)
            for target in statement.targets
            if isinstance(target, ast.Name) and target.id == "__version__"
        )
    except StopIteration as err:
        raise OSError("Unable to find __version__ in module") from err


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("GITHUB_REF", help="The GITHUB_REF environmental variable")
    args = parser.parse_args()
    assert args.GITHUB_REF.startswith(
        "refs/tags/v"
    ), f'GITHUB_REF should start with "refs/tags/v": {args.GITHUB_REF}'
    tag_version = args.GITHUB_REF[11:]
    pypi_version = get_version_from_module(
        Path("aiida_firecrest/__init__.py").read_text(encoding="utf-8")
    )
    assert (
        tag_version == pypi_version
    ), f"The tag version {tag_version} != {pypi_version} specified in `pyproject.toml`"
