# Ion Pulse API

Backend API for the bilingual Ion Pulse gaming media platform.

## Requirements

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- PostgreSQL 16 or newer

## Local development

### PostgreSQL without Docker

Docker is optional. On Ubuntu/Debian, start the system PostgreSQL cluster and
create the development account once:

```bash
sudo pg_ctlcluster 16 main start
sudo -u postgres createuser --pwprompt ion_pulse
sudo -u postgres createdb --owner=ion_pulse ion_pulse
```

Confirm that it is running:

```bash
pg_isready -h localhost -p 5432 -U ion_pulse
```

The default `ION_PULSE_DATABASE_URL` in `.env.example` already points to this
local database. Docker Compose remains an optional isolated development setup.

### Run the API

```bash
cp .env.example .env
uv sync
uv run alembic upgrade head
uv run uvicorn ion_pulse.main:app --reload
```

### Baseline data and administrator

Migrations create the standard roles and categories. To also create or promote
a bootstrap administrator, set `ION_PULSE_BOOTSTRAP_ADMIN_EMAIL` and
`ION_PULSE_BOOTSTRAP_ADMIN_PASSWORD` in `.env`, then run:

```bash
uv run python -m ion_pulse.seeds
```

The command is idempotent and never stores a password in source control.
To deliberately reset an existing bootstrap account password, run it once with
`ION_PULSE_BOOTSTRAP_ADMIN_RESET_PASSWORD=true`, then remove that setting.

The API is available at `http://localhost:8000`. OpenAPI documentation is
available at `http://localhost:8000/docs`.

### Password recovery email

For local development recovery links are written to the API log. To deliver
them through SMTP set `ION_PULSE_PASSWORD_RESET_DELIVERY=smtp`,
`ION_PULSE_SMTP_HOST`, `ION_PULSE_SMTP_FROM_EMAIL`, and, when required,
`ION_PULSE_SMTP_USERNAME` / `ION_PULSE_SMTP_PASSWORD`.

### AI editorial review

The worker leaves reviews in the safe manual queue until a provider is configured.
For an OpenAI-compatible chat-completions endpoint set
`ION_PULSE_AI_REVIEW_PROVIDER=openai_compatible` and
`ION_PULSE_AI_REVIEW_API_KEY`; optionally set the base URL and model. The reviewer
stores structured reasons and confidence, and only a `pass` for a verified author
can auto-publish the current revision.

The translation worker uses the same OpenAI-compatible protocol when
`ION_PULSE_TRANSLATION_PROVIDER=openai_compatible` and
`ION_PULSE_TRANSLATION_API_KEY` are set. Without these values source material
remains public and jobs retain the safe retry state instead of inventing a translation.

The API also applies process-local sliding-window limits to registration, sign-in,
password-recovery requests, comments, and reports. Production should additionally
enforce equivalent distributed limits at the edge/load balancer.

### Background work

The one-shot worker publishes materials whose scheduled UTC time has arrived,
then processes translation jobs. Run it regularly from a scheduler in the
deployment environment:

```bash
uv run python -m ion_pulse.workers.translation
```

## Checks

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

## Initial endpoints

- `GET /api/v1/health` — liveness and build information.
- `GET /api/v1/ready` — verifies the database connection.
