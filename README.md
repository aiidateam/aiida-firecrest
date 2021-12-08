# aiida-firecrest [IN-DEVELOPMENT]

AiiDA Transport/Scheduler plugins for interfacing with [FirecREST](https://products.cscs.ch/firecrest/)

## Installation

(pip not yet available)

```bash
pip install aiida-firecrest
```

Or for development:

```bash
git clone
cd aiida-firecrest
pip install -e .
```

## CLI Usage

```console
$ aiida-firecrest-cli --help
Usage: aiida-firecrest-cli [OPTIONS] COMMAND [ARGS]...

  FireCrest CLI.

Options:
  --config PATH  Path to the connection file (default: .firecrest-
                 config.json).
  --help         Show this message and exit.

Commands:
  chmod       Change the mode of a file.
  cwd         Get the current working directory.
  ls          List files in a path.
  parameters  Get parameters that can be configured in environment files.
  putfile     Upload file to the remote.
  service     Information about a service.
  services    List available services.
  stat        Get information about a file.
  system      Information about a system.
  systems     List available systems.
```

The configuration file should look like this:

```json
{
    "url": "https://firecrest.cscs.ch",
    "token_uri": "https://auth.cscs.ch/auth/realms/cscs/protocol/openid-connect/token",
    "client_id": "username-client",
    "client_secret": "xyz",
    "machine": "daint",
    "scratch_path": "/scratch/snx3000/username"
}
```

`scratch_path` is optional.
If specified, all operations will be relative to this path.

## Code Style

To format the code and lint it, run [pre-commit](https://pre-commit.com/):

```bash
pre-commit run --all-files
```

## Testing

It is recommended to run the tests via [tox](https://tox.readthedocs.io/en/latest/).

```bash
tox
```

By default, the tests are run using a mock FirecREST server (in a temporary folder).
You can also provide connections details to a real FirecREST server:

```bash
tox -- --firecrest-config=".firecrest-config.json"
```

The format of the `.firecrest-config.json` file is:

```json
{
    "url": "https://firecrest.cscs.ch",
    "token_uri": "https://auth.cscs.ch/auth/realms/cscs/protocol/openid-connect/token",
    "client_id": "username-client",
    "client_secret": "xyz",
    "machine": "daint",
    "scratch_path": "/scratch/snx3000/username"
}
```
