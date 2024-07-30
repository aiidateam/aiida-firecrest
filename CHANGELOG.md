# Changelog

## v0.2.0 - (not released yet)

### Transport plugin
- Refactor `put` & `get` & `copy` now they mimic behavior `aiida-ssh` transport plugin.
- `put` & `get` & `copy` now support glob patterns.
- Added `dereference` option wherever relevant
- Added `recursive` functionality for `listdir`
- Added `_create_secret_file` to store user secret locally in `~/.firecrest/`
- Added `_validate_temp_directory` to allocate a temporary directory useful for `extract` and `compress` methods on FirecREST server.
- Added `_dynamic_info_direct_size` this is able to get info of direct transfer from the server rather than asking from users. Raise if fails to make a connection.
- Added `_dynamic_info_firecrest_version` to fetch which version of FirecREST api is interacting with.
- Added `_validate_checksum` to check integrity of downloaded/uploaded files.
- Added `_gettreetar` & `_puttreetar` to transfer directories as tar files internally.
- Added `payoff` function to calculate when is gainful to transfer as zip, and when to transfer individually.

### Scheduler plugin
- `get_job` now supports for pagination for retrieving active jobs
- `get_job` is parsing more data than before: `submission_time`, `wallclock_time_seconds`, `start_time`, `time_left`, `nodelist`. see open issues [39](https://github.com/aiidateam/aiida-firecrest/issues/39) & [40](https://github.com/aiidateam/aiida-firecrest/issues/40)
- bug fix: `get_job` won't raise if the job cannot be find (completed/error/etc.)
- `_convert_time` and `_parse_time_string` copied over from `slurm-plugin` see [open issue](https://github.com/aiidateam/aiida-firecrest/issues/42)

### Tests

- The testing utils responsible for mocking the FirecREST server (specifically FirecrestMockServer) have been replaced with utils monkeypatching pyfirecrest. The FirecREST mocking utils introduced a maintenance overhead that is not in the responsibility of this repository. We still continue to support running with a real FirecREST server and plan to continue running the tests with the [demo docker image](https://github.com/eth-cscs/firecrest/tree/master/deploy/demo) offered by CSCS. The docker image has been disabled for the moment due to some problems (see issue #47).


### Miscellaneous

- class `FcPath` is removed from interface here, as it has [merged](https://github.com/eth-cscs/pyfirecrest/pull/43) into pyfirecrest

## v0.1.0 (December 2021)

Initial release.
