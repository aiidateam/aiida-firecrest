# Changelog

## v0.2.0 - 2024-07-15 (not released yet)

### Transport plugin
- `dynamic_info()` is added to retrieve machine information without user input.
- Refactor `put` & `get` & `copy` now they mimic behavior `aiida-ssh` transport plugin.
- `put` & `get` & `copy` now support glob patterns.
- Added `dereference` option wherever relevant
- Added `recursive` functionality for `listdir`
- Added `_create_secret_file` to store user secret locally in `~/.firecrest/`
- Added `_validate_temp_directory` to allocate a temporary directory useful for `extract` and `compress` methods on FirecREST server.
- Added `_dynamic_info_direct_size` this is able to get info of direct transfer from the server rather than asking from users. Raise of user inputs fails to make a connection.
- Added `_validate_checksum` to check integrity of downloaded/uploaded files.
- Added `_gettreetar` & `_puttreetar` to transfer directories as tar files internally.
- Added `payoff` function to calculate when is gainful to transfer as zip, and when to transfer individually.

### Scheduler plugin
- `get_job` now supports for pagination for retrieving active jobs
- `get_job` is parsing more data than before: `submission_time`, `wallclock_time_seconds`, `start_time`, `time_left`, `nodelist`. see open issues [39](https://github.com/aiidateam/aiida-firecrest/issues/39) & [40](https://github.com/aiidateam/aiida-firecrest/issues/40)
- bug fix: `get_job` won't raise if the job cannot be find (completed/error/etc.)
- `_convert_time` and `_parse_time_string` copied over from `slurm-plugin` see [open issue](https://github.com/aiidateam/aiida-firecrest/issues/42)

### Tests

- Tests has completely replaced with new ones. Previously tests were mocking FirecREST server. Those test were a good practice to ensure that all three (`aiida-firecrest`, FirecREST, and PyFirecREST) work flawlessly.
The downside was debugging wasn't easy at all. Not always obvious which of the three is causing a bug.
Because of this, a new set of tests only verify the functionality of `aiida-firecrest` by directly mocking PyFirecREST. Maintaining this set in `tests/` is simpler because we just need to monitor the return values of PyFirecREST​. While maintaining the former is more difficult as you have to keep up with both FirecREST and PyFirecREST.


### Miscellaneous

- class `FcPath` is removed from interface here, as it has [merged](https://github.com/eth-cscs/pyfirecrest/pull/43) into `pyfirecrest`

## v0.1.0 (December 2021)

Initial release.
