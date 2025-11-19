# OAI-PMH Client

A modern Python client for OAI-PMH (Open Archives Initiative Protocol for Metadata Harvesting).

## Installation

This project uses `uv` for package management. To install the client and its dependencies, you can use the following commands:

```bash
uv venv
source .venv/bin/activate
uv pip install .
```

## Documentation

Full documentation is available at [https://wellcomecollection.github.io/oai-pmh/](https://wellcomecollection.github.io/oai-pmh/).

### Updating the documentation

The static HTML docs under `docs/pdoc/` are generated with [pdoc](https://pdoc.dev/). To refresh them after making code changes:

1. Install the project (and dev extras if desired):

    ```bash
    uv pip install -e ".[dev]"
    ```

2. Regenerate the documentation:

    ```bash
    uv run python -m pdoc oai_pmh_client --output-dir docs/pdoc --docformat google
    ```

This overwrites the HTML (and supporting assets) inside `docs/pdoc/`. Commit the updated files if you want the published site to reflect the latest API.

## Usage

Here is a simple example of how to use the client:

```python
from oai_pmh_client.client import OAIClient

# Create a client for the arXiv OAI-PMH endpoint.
client = OAIClient("https://oaipmh.arxiv.org/oai")

# Get the repository's identity.
identity = client.identify()
print(identity)

# List the available metadata formats.
formats = client.list_metadata_formats()
print(formats)

# List the sets in the repository.
sets = client.list_sets()
print(sets)
```

### More Examples

#### Listing Records

You can list records with optional `from_date`, `until_date`, and `set_spec` filters.

```python
from datetime import datetime

# List all records updated since the start of 2024 in the "cs" (Computer Science) set
records = client.list_records(
    metadata_prefix="oai_dc",
    from_date=datetime(2024, 1, 1),
    set_spec="cs"
)
for record in records:
    print(record.header.identifier, record.header.datestamp)
```

##### Datestamp granularity

Different OAI-PMH repositories declare (via the `Identify` response) which datestamp granularity they accept for the `from` and `until` parameters:

* `YYYY-MM-DD` (day-level)
* `YYYY-MM-DDThh:mm:ssZ` (second-level, UTC)

The client automatically chooses the right format based on the `datetime` you provide: midnight values are sent with day-level precision, and any timestamp with a time component uses second-level precision. This makes it easy to harvest narrow windows (e.g. a few seconds) without additional configuration.

If you need to override the automatic behaviour—for example to force day-level timestamps for repositories that reject second-level granularity—you can set the `datestamp_granularity` argument when instantiating the client:

```python
client = OAIClient("https://oaipmh.arxiv.org/oai", datestamp_granularity="YYYY-MM-DD")

from datetime import datetime
records = client.list_records(
    metadata_prefix="oai_dc",
    from_date=datetime(2024, 1, 1, 12, 0, 0),  # still sent as 2024-01-01
)
```

You may also supply a pre-formatted string to override formatting entirely:

```python
records = client.list_records(
    metadata_prefix="oai_dc",
    from_date="2024-01-01",  # already correctly formatted
)
```

#### Getting a Single Record

Retrieve a single record by its identifier and a metadata prefix.

```python
record = client.get_record("oai:arXiv.org:2401.00001", "oai_dc")
print(record.metadata)
```

#### Handling Deleted Records

OAI-PMH repositories may return records that have been deleted. These records will have a header with `status="deleted"` and no metadata. The client exposes this via the `is_deleted` property on the record header.

```python
records = client.list_records(metadata_prefix="oai_dc")

for record in records:
    if record.header.is_deleted:
        print(f"Record {record.header.identifier} has been deleted.")
        continue
        
    # Process active records
    print(record.metadata)
```

#### Error Handling

The client will raise an `OAIError` subclass for errors returned by the OAI-PMH server.

```python
from oai_pmh_client.exceptions import IdDoesNotExistError

try:
    record = client.get_record("oai:arXiv.org:this-id-does-not-exist", "oai_dc")
except IdDoesNotExistError as e:
    print(f"Caught expected error: {e}")
```

#### Notebook example

See the [`notebooks/arxiv_recent_changes.ipynb`](notebooks/arxiv_recent_changes.ipynb) notebook for an example of using the client to fetch recent changes from the arXiv OAI-PMH endpoint.

## Testing

To run the tests, you will need to install the development dependencies:

```bash
uv pip install -e ".[dev]"
```

Then, you can run the tests using `pytest`:

```bash
pytest
```

## Releases

This repository uses [Release Please](https://github.com/googleapis/release-please) to automate releases.

When you merge a pull request to the `main` branch, Release Please will:

1.  Create or update a "Release PR" with the changelog and version bump.
2.  When you merge that Release PR, it will create a GitHub Release and tag the commit.

To trigger a release:

1.  Ensure your PR titles follow the [Conventional Commits](https://www.conventionalcommits.org/) specification (e.g., `feat: add new feature`, `fix: bug fix`).
2.  Merge your PRs into `main`.
3.  Review and merge the Release PR created by the bot.
