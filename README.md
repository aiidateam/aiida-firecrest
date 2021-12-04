# aiida-firecrest

AiiDA Transport/Scheduler plugins for interfacing with [FirecREST](https://products.cscs.ch/firecrest/)

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
