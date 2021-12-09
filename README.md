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
  fs      File system operations.
  slurm   Slurm operations.
  stat    Status operations.
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

```console
$ aiida-firecrest-cli stat
Usage: aiida-firecrest-cli stat [OPTIONS] COMMAND [ARGS]...

  Status operations.

Options:
  --help  Show this message and exit.

Commands:
  parameters  Get parameters that can be configured in environment files.
  service     Information about a service.
  services    List available services.
  system      Information about a system.
  systems     List available systems.
```

```console
$ aiida-firecrest-cli fs
Usage: aiida-firecrest-cli fs [OPTIONS] COMMAND [ARGS]...

  File system operations.

Options:
  --help  Show this message and exit.

Commands:
  cat      Get the contents of a file.
  chmod    Change the mode of a file.
  cwd      Get the current working directory.
  ls       List files in a path.
  putfile  Upload file to the remote.
  stat     Get information about a file.
```

```console
$ aiida-firecrest-cli slurm
Usage: aiida-firecrest-cli slurm [OPTIONS] COMMAND [ARGS]...

  Slurm operations.

Options:
  --help  Show this message and exit.

Commands:
  sacct   Retrieve information for all jobs.
  squeue  Retrieves information for queued jobs.
  submit  Submit a job script.
```

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
