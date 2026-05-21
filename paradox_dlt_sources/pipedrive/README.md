# pipedrive

Pipedrive dlt source — extracts CRM data from
[Pipedrive](https://www.pipedrive.com) using the v1 REST API.

## Resources

| Resource | Primary key | Write disposition | Endpoint | Cursor field |
|---|---|---|---|---|
| `users` | `id` | `append` | `GET /v1/recents?items=user` | `modified` |
| `persons` | `id` | `append` | `GET /v1/persons` | `update_time` |
| `leads` | `id` | `append` | `GET /v1/leads` | `update_time` |
| `organizations` | `id` | `append` | `GET /v1/organizations` | `update_time` |
| `deals` | `id` | `append` | `GET /v1/deals` | `update_time` |
| `activities` | `id` | `append` | `GET /v1/activities` | `update_time` |
| `stages` | `id` | `replace` | `GET /v1/stages` | *(none — full snapshot)* |

### Row transforms

- **`persons`**: `email` and `phone` arrays are flattened to `email_primary` /
  `phone_primary` (first entry's `value`). `owner_id` nested object is
  flattened to its scalar `.id`.
- **`deals`**: `user_id` and `creator_user_id` nested user objects are
  flattened to scalar `.id` values.

## Auth

Personal API token passed as `?api_token=<value>`. Pipedrive v1 only accepts
personal tokens via query parameter; Bearer auth requires OAuth access tokens
and the v2 API.

Mint a token at Pipedrive → Settings → Personal preferences → API.

## Config

`.dlt/secrets.toml`:

```toml
[sources.pipedrive]
api_key = "your_pipedrive_api_token"
```

## Example

```python
import dlt
from paradox_dlt_sources.pipedrive import pipedrive_source

pipeline = dlt.pipeline(
    pipeline_name="pipedrive_demo",
    destination="duckdb",
    dataset_name="pipedrive_data",
)
info = pipeline.run(pipedrive_source())
print(info)
```

Override the base URL for testing:

```python
pipedrive_source(api_key="test-key", base_url="http://localhost:9090")
```

## Implementation notes

- **`users` endpoint**: sourced via `/recents?items=user` rather than
  `/users` directly. Pipedrive removed the `modified` field from `/users`
  for non-admin tokens on 2023-02-09; `/recents` still surfaces it with
  `since_timestamp` filtering.
- **`activities` user scope**: `user_id=0` is passed to return all-company
  activities. Without this, the endpoint filters to the API-token user only,
  which silently changes row counts when the token owner changes.
- **`stages`**: full replace each run — no reliable update cursor on this
  endpoint. Typically fewer than 20 rows per account.
- **`RefreshTokenAuth`**: included in `helpers.py` for completeness; the
  active auth mechanism is API key (`APIKeyAuth`). If Pipedrive OAuth is
  needed in future, `RefreshTokenAuth` is ready to use.
