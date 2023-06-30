# aiida-firecrest [IN-DEVELOPMENT]

AiiDA Transport/Scheduler plugins for interfacing with [FirecREST](https://products.cscs.ch/firecrest/) (currently based on [v1.13.0](https://github.com/eth-cscs/firecrest/releases/tag/v1.13.0)),
via [pyfirecrest](https://github.com/eth-cscs/pyfirecrest).

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

### Current Issues

Simple calculations are now running successfully [in the tests](tests/test_calculation.py), however, there are still some critical issues, before this could be production ready:

1. Currently uploading via firecrest changes `_aiidasubmit.sh` to `aiidasubmit.sh` 😱 ([see #191](https://github.com/eth-cscs/firecrest/issues/191)), so `metadata.options.submit_script_filename` should be set to this.

2. Handling of large (>5Mb) files needs to be improved

3. Handling of the client secret, which should likely not be stored in the database

4. Monitoring / management of API requests could to be improved

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

See [firecrest_demo.py](firecrest_demo.py) for how to start up a demo server.
(note the issue with OSX and turning off the AirPlay port)
