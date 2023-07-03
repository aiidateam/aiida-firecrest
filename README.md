# aiida-firecrest [IN-DEVELOPMENT]

[![Coverage Status][codecov-badge]][codecov-link]
[![Code style: black][black-badge]][black-link]

AiiDA Transport/Scheduler plugins for interfacing with [FirecREST](https://products.cscs.ch/firecrest/), via [pyfirecrest](https://github.com/eth-cscs/pyfirecrest).

It is currently tested against [FirecREST v1.13.0](https://github.com/eth-cscs/firecrest/releases/tag/v1.13.0).

**NOTE:** This plugin is currently dependent on a fork of `aiida-core` from [PR #6043](https://github.com/aiidateam/aiida-core/pull/6043)

## Usage

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
Computer label: firecrest-client
Hostname: unused
Description []: My FirecREST client plugin
Transport plugin: firecrest
Scheduler plugin: firecrest
Shebang line (first line of each script, starting with #!) [#!/bin/bash]:
Work directory on the computer [/scratch/{username}/aiida/]:
Mpirun command [mpirun -np {tot_num_mpiprocs}]:
Default number of CPUs per machine: 2
Default amount of memory per machine (kB).: 100
Escape CLI arguments in double quotes [y/N]:
Success: Computer<3> firecrest-client created
Report: Note: before the computer can be used, it has to be configured with the command:
Report:   verdi -p quicksetup computer configure firecrest firecrest-client
```

```console
$ verdi -p quicksetup computer configure firecrest firecrest-client
Report: enter ? for help.
Report: enter ! to ignore the default and set no value.
Server URL: https://firecrest.cscs.ch
Token URI: https://auth.cscs.ch/auth/realms/cscs/protocol/openid-connect/token
Client ID: username-client
Client Secret: xyz
Client Machine: daint
Maximum file size for direct transfer (MB) [5.0]:
Report: Configuring computer firecrest-client for user chrisj_sewell@hotmail.com.
Success: firecrest-client successfully configured for chrisj_sewell@hotmail.com
```

```console
$ verdi computer show firecrest-client
---------------------------  ------------------------------------
Label                        firecrest-client
PK                           3
UUID                         48813c55-1b2b-4afc-a1a1-e0d33a5b6868
Description                  My FirecREST client plugin
Hostname                     unused
Transport type               firecrest
Scheduler type               firecrest
Work directory               /scratch/{username}/aiida/
Shebang                      #!/bin/bash
Mpirun command               mpirun -np {tot_num_mpiprocs}
Default #procs/machine       2
Default memory (kB)/machine  100
Prepend text
Append text
---------------------------  ------------------------------------
```

See also the [pyfirecrest CLI](https://github.com/eth-cscs/pyfirecrest), for directly interacting with a FirecREST server.

See [tests/test_calculation.py](tests/test_calculation.py) for a working example of how to use the plugin, via the AiiDA API.

### Current Issues

Simple calculations are now running successfully [in the tests](tests/test_calculation.py), however, there are still some critical issues, before this could be production ready:

1. Currently uploading via firecrest changes `_aiidasubmit.sh` to `aiidasubmit.sh` 😱 ([see #191](https://github.com/eth-cscs/firecrest/issues/191)), so `metadata.options.submit_script_filename` should be set to this.

2. Handling of large (>5Mb) file uploads/downloads needs to be improved

3. Handling of the client secret, which should likely not be stored in the database

4. Monitoring / management of API request rates could to be improved

## Development

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

By default, the tests are run using a mock FirecREST server, in a temporary folder
(see [aiida_fircrest.utils_test.FirecrestConfig](aiida_firecrest/utils_test.py)).
This allows for quick testing and debugging of the plugin, without needing to connect to a real server,
but is obviously not guaranteed to be fully representative of the real behaviour.

You can also provide connections details to a real FirecREST server:

```bash
tox -- --firecrest-config=".firecrest-demo-config.json"
```

The format of the `.firecrest-demo-config.json` file is:

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

In this mode, if you want to inspect the generated files, after a failure, you can use:

```bash
tox -- --firecrest-config=".firecrest-demo-config.json" --firecrest-no-clean
```

See [firecrest_demo.py](firecrest_demo.py) for how to start up a demo server,
and also [server-tests.yml](.github/workflows/server-tests.yml) for how the tests are run against the demo server on GitHub Actions.

If you want to analyse statistics of the API requests made by each test,
you can use the `--firecrest-requests` option:

```bash
tox -- --firecrest-requests
```

### Notes on using the demo server on MacOS

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
