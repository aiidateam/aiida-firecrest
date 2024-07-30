# aiida-firecrest [IN-DEVELOPMENT]

[![Coverage Status][codecov-badge]][codecov-link]
[![Code style: black][black-badge]][black-link]

AiiDA Transport/Scheduler plugins for interfacing with [FirecREST](https://products.cscs.ch/firecrest/), via [pyfirecrest](https://github.com/eth-cscs/pyfirecrest).

It is currently tested against [FirecREST v2.6.0](https://github.com/eth-cscs/pyfirecrest/tree/v2.6.0).


## Installation

Install via GitHub or PyPI:

```bash
pip install git+https://github.com/aiidateam/aiida-firecrest.git
# pip not yet available
pip install aiida-firecrest
```

You should then be able to see the `firecrest` transport and scheduler plugins loaded in AiiDA:

```console
$ verdi plugin list aiida.transports firecrest
A plugin to connect to a FirecREST server.
It must be used together with the 'firecrest' scheduler plugin.
Authentication parameters:
  url: URL to the FirecREST server
  token_uri: URI for retrieving FirecREST authentication tokens
  client_id: FirecREST client ID
  client_secret: FirecREST client secret
  client_machine: FirecREST machine secret
  small_file_size_mb: Below this size, file bytes will be sent in a single API call.

$ verdi plugin list aiida.schedulers firecrest
A plugin to connect to a FirecREST server.
It must be used together with the 'firecrest' transport plugin.
```

You can then create a `Computer` in AiiDA:

```console
$ verdi computer setup
Report: enter ? for help.
Report: enter ! to ignore the default and set no value.
Computer label: firecrest-client                 # your choice
Hostname: unused                                 # your choice, irrelevant
Description []: My FirecREST client plugin       # your choice
Transport plugin: firecrest
Scheduler plugin: firecrest
Shebang line (first line of each script, starting with #!) [#!/bin/bash]:
Work directory on the computer [/scratch/{username}/aiida/]:
Mpirun command [mpirun -np {tot_num_mpiprocs}]:
Default number of CPUs per machine:  2           # depending on your compute resource
Default amount of memory per machine (kB).: 100  # depending on your compute resource
Escape CLI arguments in double quotes [y/N]:
Success: Computer<3> firecrest-client created
Report: Note: before the computer can be used, it has to be configured with the command:
Report:   verdi -p MYPROFILE computer configure firecrest firecrest-client
```

```console
$ verdi -p MYPROFILE computer configure firecrest firecrest-client
Report: enter ? for help.
Report: enter ! to ignore the default and set no value.
Server URL: https://firecrest.cscs.ch          # this for CSCS
Token URI: https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token
Client ID: username-client
Client Secret: xyz
Compute resource (Machine): daint
Temp directory on server: /scratch/something/ # "A temp directory on user's space on the server for creating temporary files (compression, extraction, etc.)"
FirecREST api version [Enter 0 to get this info from server] [0]: 0
Maximum file size for direct transfer (MB) [Enter 0 to get this info from server] [0]: 0
Report: Configuring computer firecrest-client for user chrisj_sewell@hotmail.com.
Success: firecrest-client successfully configured for chrisj_sewell@hotmail.com
```

You can always check your config with
```console
$ verdi computer show firecrest-client
```

See also the [pyfirecrest CLI](https://github.com/eth-cscs/pyfirecrest), for directly interacting with a FirecREST server.


After this, everything should function normally through AiiDA with no problems.
See [tests/test_calculation.py](tests/test_calculation.py) for a working example of how to use the plugin, via the AiiDA API.

If you encounter any problems/bug, please don't hesitate to open an issue on this repository.

### Current Issues

Calculations are now running successfully, however, there are still issues regarding efficiency, Could be improved:

1. Monitoring / management of API request rates could to be improved. Currently this is left up to PyFirecREST.
2. Each transfer request includes 2 seconds of `sleep` time, imposed by `pyfirecrest`. One can takes use of their `async` client, but with current design of `aiida-core`, the gain will be minimum. (see the [closing comment of issue#94 on pyfirecrest](https://github.com/eth-cscs/pyfirecrest/issues/94) and [PR#6079 on aiida-core ](https://github.com/aiidateam/aiida-core/pull/6079))

## For developers

```bash
git clone
cd aiida-firecrest
pip install -e .[dev]
```

### Code Style

To format the code and lint it, run [pre-commit](https://pre-commit.com/):

```bash
pre-commit run --all-files
```

### Testing

It is recommended to run the tests via [tox](https://tox.readthedocs.io/en/latest/).

```bash
tox
```

By default, the tests are run using a monkey patched PyFirecREST.
This allows for quick testing and debugging of the plugin, without needing to connect to a real server, but is obviously not guaranteed to be fully representative of the real behaviour.

To have a guaranteed proof, you may also provide connections details to a real FirecREST server:

```bash
tox -- --firecrest-config=".firecrest-demo-config.json"
```


If a config file is provided, tox sets up a client environment with the information
in the config file and uses pyfirecrest to communicate with the server.
```plaintext
┌─────────────────┐───►┌─────────────┐───►┌──────────────────┐
│ aiida_firecrest │    │ pyfirecrest │    │ FirecREST server │
└─────────────────┘◄───└─────────────┘◄───└──────────────────┘
```

if a config file is not provided, it monkeypatches pyfirecrest so we never actually communicate with a server.
```plaintext
┌─────────────────┐───►┌─────────────────────────────┐
│ aiida_firecrest │    │ pyfirecrest (monkeypatched) │
└─────────────────┘◄───└─────────────────────────────┘
```

The format of the `.firecrest-demo-config.json` file, for example is like:


```json
 {
    "url": "https://firecrest-tds.cscs.ch",
    "token_uri": "https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token",
    "client_id": "username-client",
    "client_secret": "path-to-secret-file",
    "compute_resource": "daint",
    "temp_directory": "/scratch/snx3000/username/",
    "small_file_size_mb": 5.0,
    "workdir": "/scratch/snx3000/username/",
    "api_version": "1.16.0"
}
```

In this mode, if you want to inspect the generated files, after a failure, you can use:

```bash
tox -- --firecrest-config=".firecrest-demo-config.json" --firecrest-no-clean
```

**These tests were successful against [FirecREST v1.16.0](https://github.com/eth-cscs/firecrest/releases/tag/v1.16.0), except those who require to list directories in a symlink directory, which fail due to a bug in FirecREST. [An issue](https://github.com/eth-cscs/firecrest/issues/205) is open on FirecREST repo about this.**

Instead of a real server (which requires an account and credential), tests can also run against a docker image provided by FirecREST. See [firecrest_demo.py](firecrest_demo.py) for how to start up a demo server, and also [server-tests.yml](.github/workflows/server-tests.yml) for how the tests are run against the demo server on GitHub Actions.

<!-- If you want to analyse statistics of the API requests made by each test,
you can use the `--firecrest-requests` option:

```bash
tox -- --firecrest-requests
``` -->

#### Notes on using the demo server on MacOS

A few issues have been noted when using the demo server on MacOS (non-Mx):

`docker-compose up` can fail, with an error that port 7000 is already in use.
Running `lsof -i :7000` you may see it is used by `afs3-fileserver`,
which can be fixed by turning off the Airplay receiver
(see <https://github.com/cookiecutter/cookiecutter-django/issues/3499>)

Large file uploads can fail, because the server provides a URL with ``192.168.220.19`` that actually needs to be ``localhost``.
To fix this, ensure that you set `FIRECREST_LOCAL_TESTING = true` in your environment
(set by default if running `tox`).

Large file downloads has the same problem, but even with this fix, it will still fail with a 403 HTTP error, due to a signature mismatch.
No automatic workaround has been found for this yet,
although it is of note that you can find these files directly where you your `firecrest` Github repo is cloned, `/path/to/firecrest/deploy/demo/minio/` plus the path of the URL.

[codecov-badge]: https://codecov.io/gh/aiidateam/aiida-firecrest/branch/main/graph/badge.svg
[codecov-link]: https://codecov.io/gh/aiidateam/aiida-firecrest
[black-badge]: https://img.shields.io/badge/code%20style-black-000000.svg
[black-link]: https://github.com/ambv/black


### :bug: Fishing :bug: Bugs :bug:

First, start with running tests locally with no `config` file given, that would monkeypatch `pyfirecrest`. These set of test do not guarantee that the whole firecrest protocol is working, but it's very useful to quickly check if `aiida-firecrest` is behaving as it's expected to do. To run just simply use `pytest` or `tox`.

If these tests pass and the bug persists, consider providing a `config` file to run the tests on a docker image or directly on a real server. Be aware of versioning, `pyfirecrest` doesn't check which version of api it's interacting with.  (TODO: open an issue on this)

If the bug persists and test still passes, then most certainly it's a problem of `aiida-firecrest`.
If not, probably the issue is from FirecREST, you might open an issue to [`pyfirecrest`](https://github.com/eth-cscs/pyfirecrest) or [`FirecREST`](https://github.com/eth-cscs/firecrest).
