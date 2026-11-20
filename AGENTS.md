# AGENTS.md

Guidance for coding agents working in this repository.

## What this repo is

The `file-watcher` repository contains two components that work together at the start of the
Flexible Interactive Automation (FIA) autoreduction pipeline (see `README.md`):

1. **`FileWatcher` (`file_watcher/`)**: Polls an instrument's `lastrun.txt` file on the mounted
   archive (`/archive/NDX<INSTRUMENT>/Instrument/logs/lastrun.txt`) or instrument PC (`/imat/lastrun.txt`)
   to detect newly created `.nxs` files — working around Linux `inotify` not reporting changes on
   network-mounted SMB shares. On startup and on each poll, it queries and updates the instrument's
   last known run via `FIA-API` (`GET`/`PUT /instrument/{instrument}/latest-run`), recovers any
   missed runs in between, and publishes full `.nxs` file paths to a RabbitMQ quorum queue
   (`EGRESS_QUEUE_NAME`, default `watched-files`).
2. **`FileWatcherOperator` (`file_watcher_operator/`)**: A Kopf-based Kubernetes operator that
   watches `fia.com/v1` `FileWatcher` Custom Resources (`filewatchers`) and provisions/updates the
   per-instrument SMB `PersistentVolume`, `PersistentVolumeClaim` (`/archive` or `/imat` when
   `spec.specialPV: imat`), and `filewatcher-<name>-deployment`.

## Position in the FIA service

### Overarching FIA architecture

FIA is a multi-repository system (all checked out as siblings under the same parent workspace
directory) that automatically reduces instrument data, and also lets users browse, plot, and rerun
that data interactively:

```text
[ISIS Instrument Archive (/archive, /imat) & lastrun.txt]
        │
        ▼
  file-watcher (THIS REPO) (Kopf Operator + Per-Instrument Watcher Pods)
        │  publishes file path to RabbitMQ: `watched-files`
        ▼
  run-detection ◄──── fetches instrument rules (`GET /instrument/{inst}/specification`) ────► FIA-API
        │  ingests .nxs metadata, verifies rules, publishes JobRequest JSON
        │  to RabbitMQ: `scheduled-jobs` (or retries via `failed-watched-files`)
        ▼
  jobcontroller (job_creator) ◄──── requests PreScript (`GET /script/{inst}`) & posts Job ──► FIA-API ◄──► autoreduction-scripts
        │  spawns Kubernetes Job in cluster:
        ├──► Main container: runners (`ghcr.io/fiaisis/mantid` or `mantid-imaging`) -> writes reduced data to `/ceph`
        └──► Sidecar container: jobcontroller (job_watcher) -> updates job status/outputs in FIA-API (`PATCH /job/{id}`)

[Interactive / Web & Live Data Paths]
  scigateway (Host App) ──► frontend (FIA React Plugin)
                                ├──► fia-auth (UOWS login, JWT verify/refresh, `/experiments` RB lookup)
                                ├──► FIA-API (runs, jobs, scripts, specifications, reruns -> `watched-files` / `scheduled-jobs`)
                                └──► plotting-service (h5grove NeXus/HDF5 & IMAT image serving from `/ceph` & `/imat`)

  Kafka (`<INST>_events`, `<INST>_sampleEnv`, `<INST>_runInfo`)
        └──► live-data (Live Data Operator + Mantid Live Data Processor fetching `/live-data/{inst}/script` from FIA-API)

  LLSP (Long Lived Script Processors: `LLSP-API` + RabbitMQ + Celery `LLSP-Worker` built on `ghcr.io/fiaisis/mantid`)

[Infrastructure & Deployment]
  k8s (Terraform + Ansible for OpenStack clusters, Redpanda, local Minikube)
  gitops (ArgoCD App-of-Apps & Kustomize `base`/`envs`/`variants` manifests for all FIA services)
```

### This repository's location (upstream, downstream & siblings)

- **Role**: Entry point of the automated reduction pipeline — bridges the ISIS instrument archive
  SMB mounts (`/archive` and `/imat`) to the RabbitMQ `watched-files` queue.
- **Upstream inputs**:
  - Instrument `lastrun.txt` and `.nxs` data files on `/archive/NDX<INSTRUMENT>/Instrument/...` or
    `/imat/...` (mounted read-only via SMB CSI PVs/PVCs created by `FileWatcherOperator`).
  - `fia.com/v1` `FileWatcher` Custom Resources applied to the cluster from `gitops` (`../gitops`).
- **Downstream outputs**:
  - RabbitMQ quorum queue `watched-files` (`EGRESS_QUEUE_NAME`) — consumed by `run-detection`
    (`../run-detection`).
  - `FIA-API` (`../FIA-API`) — calls `GET /instrument/{instrument}/latest-run`,
    `PUT /instrument/{instrument}/latest-run` (authenticated with `Authorization: Bearer
    <FIA_API_API_KEY>`), and `GET /healthz` (used before updating `/tmp/heartbeat`).
  - `gitops` (`../gitops`) — `.github/workflows/build_and_push.yml` builds and pushes both
    `ghcr.io/fiaisis/filewatcher` and `ghcr.io/fiaisis/filewatcher-operator` on every push to
    `main`, then automatically updates `FILE_WATCHER_SHA256` and the operator image digest in
    `components/file-watcher-operator/envs/staging/file-watcher-operator.yml`.
- **Sibling repositories to check when changing contracts**: `../run-detection` (expects the raw
  `.nxs` file path string on `watched-files`), `../FIA-API` (`latest-run` and `healthz` endpoints),
  and `../gitops` (`components/file-watcher-operator/` CRD instances and operator deployment).

### Core technologies
- **Python**: check `pyproject.toml` and `.github/workflows/` for the target Python version — do not
  hardcode a version here.
- **pika**: RabbitMQ blocking client (`BlockingConnection` / `BlockingChannel` with quorum queues).
- **requests**: HTTP client for `FIA-API` `latest-run` and `healthz` calls.
- **kopf** + **kubernetes** + **PyYAML**: Kubernetes operator framework and API client (under the
  `operator` optional dependency group).
- **pytest** (+ `pytest-cov`, `pytest-asyncio`): Unit test suite.
- **ruff**: Linting and formatting (`line-length = 120`, rules in `pyproject.toml`).
- **mypy**: Static type checking (`mypy --strict file_watcher` in CI).
- **Docker**: Two Dockerfiles (`container/file_watcher.D` and `container/file_watcher_operator.D`).

---

## Repository structure

```
file_watcher/                FileWatcher package — CLI entrypoint (`file_watcher`), RabbitMQ
                              producer, lastrun.txt polling & missed-run recovery logic, FIA-API
                              latest-run sync, and /tmp/heartbeat readiness/liveness writer
file_watcher_operator/       Kopf operator module — watches fia.com/v1 FileWatcher CRs and manages
                              per-instrument SMB PVs, PVCs (/archive or /imat), and Deployments
test/                        Pytest unit test suite covering file_watcher (main, monitor, health)
                              and file_watcher_operator (note: test/file_watcher/docker-compose.yml
                              is a legacy Memphis setup and is not used by pytest or CI)
container/                   Dockerfiles for `filewatcher` (`file_watcher.D`) and
                              `filewatcher-operator` (`file_watcher_operator.D`)
.github/workflows/           CI/CD workflows: tests.yml, formatting_and_linting.yml,
                              build_and_push.yml, trivy.yml, scorecard.yml, dependency-review.yml
pyproject.toml               Single source of truth for package metadata, pinned dependencies,
                              optional extras, and ruff configuration
```

---

## `pyproject.toml` — read this first

`pyproject.toml` is the single source of truth for dependencies, extras, and tool config. Read it
before adding dependencies or changing lint/type rules:

- Runtime dependencies (`pika`, `requests`) and optional extras (`operator`, `formatting`, `test`,
  `code-inspection`, `dev`, `all`) are pinned with `==` — keep new dependencies pinned the same way.
- Install everything for local development with `pip install ".[all]"`.
- Ruff (`[tool.ruff]`, `[tool.ruff.lint]`) enforces `line-length = 120` and a wide rule set
  (bandit `S`, bugbear `B`, pathlib `PTH`, type-checking `TCH`, no print `T20`, pylint `PL`, etc.).
- CI runs `mypy --strict file_watcher` (note `pika` lacks type stubs and is imported with
  `# type: ignore`).

---

## Configuration & CRD conventions

### `FileWatcher` environment variables
| Variable | Default | Description |
| :--- | :--- | :--- |
| `QUEUE_HOST` | `localhost` | RabbitMQ hostname |
| `QUEUE_USER` | `guest` | RabbitMQ username |
| `QUEUE_PASSWORD` | `guest` | RabbitMQ password |
| `EGRESS_QUEUE_NAME` | `watched-files` | Quorum queue where detected `.nxs` paths are published |
| `WATCH_FILE` | `/archive/NDXMARI/Instrument/logs/lastrun.txt` | Path to the instrument's `lastrun.txt` file |
| `FILE_PREFIX` | `MAR` | Prefix of the instrument's `.nxs` filenames |
| `INSTRUMENT_FOLDER` | `NDXMARI` | Instrument folder name (stripped of `NDX` when calling `FIA-API`) |
| `FIA_API_URL` | `localhost:8000` | Base URL for `FIA-API` (`/healthz` and `/instrument/{inst}/latest-run`) |
| `FIA_API_API_KEY` | `shh` | Bearer token for `FIA-API` requests |

*(Note: `LastRunDetector.is_an_instrument_pc` treats paths whose `last_run_file.parent.parent == Path("/")`,
such as `/imat/lastrun.txt`, as flat instrument-PC mounts where `.nxs` files live directly in
`last_run_file.parent` rather than under `.../data/cycle_YY_N/`.)*

### `FileWatcherOperator` environment variables & CRD fields
- **Operator env vars**:
  - `QUEUE_HOST` (default `rabbitmq-cluster.rabbitmq.svc.cluster.local`)
  - `EGRESS_QUEUE_NAME` (default `watched-files`)
  - `FILE_WATCHER_SHA256` (digest of `ghcr.io/fiaisis/filewatcher` to deploy in child Deployments)
  - `FILEWATCHER_NAMESPACE` (default `fia`)
  - `FIA_API_URL` (default `localhost:8000`)
- **`fia.com/v1` `FileWatcher` CRD `spec` fields**:
  - `lastrunFilePath` -> sets `WATCH_FILE` on the child deployment
  - `filePrefix` -> sets `FILE_PREFIX`
  - `instrumentFolder` -> sets `INSTRUMENT_FOLDER`
  - `specialPV` (optional, e.g. `imat` or `NONE`) -> mounts `/imat` (`smb-bridge-imat`) when set to
    `imat`, otherwise mounts `/archive` (`archive-creds`).

---

## Environment & running commands

Do not assume any particular machine already has a conda env, kubectl context, or virtualenv set up
— check what is available first. Prefer `conda` when available (`command -v conda` on Linux/macOS or
`where conda` on Windows); fall back to a Python `venv` only if conda is not installed.

1. Check existing conda environments:
   ```shell
   conda env list
   ```
   If a matching env exists (e.g. `file-watcher`), use it with `conda run -n <env-name> <command>`
   or `conda activate <env-name>`.
2. If no suitable conda env exists but `conda` is installed, create one using the Python version in
   `.github/workflows/`:
   ```shell
   conda create -n file-watcher python=<version-from-CI> -y
   conda run -n file-watcher pip install ".[all]"
   ```
3. Only if `conda` is not installed, fall back to a virtualenv:
   ```shell
   python3 -m venv .venv        # Linux/macOS (use `python` on Windows)
   . .venv/bin/activate         # POSIX shell (.venv\Scripts\Activate.ps1 on PowerShell)
   pip install ".[all]"
   ```

### Running tests, linting & containers
- **Unit tests, linting & type checking**:
  ```shell
  conda run -n <env-name> pytest .
  conda run -n <env-name> ruff check .
  conda run -n <env-name> ruff format --check .
  conda run -n <env-name> mypy --strict file_watcher
  ```
- **Docker builds** (from repo root, per `README.md`):
  ```shell
  docker build . -f ./container/file_watcher.D -t ghcr.io/fiaisis/filewatcher
  docker build . -f ./container/file_watcher_operator.D -t ghcr.io/fiaisis/filewatcher-operator
  ```

## Kubernetes commands

Available kubectl contexts vary per machine/operator — always run `kubectl config get-contexts`
first before targeting a cluster. Never rely on the current context; always pass `--context`
explicitly at the **end** of the command:

```shell
kubectl get pods -n fia --context <context-name>
kubectl get filewatchers -n fia --context <context-name>
```

Double- and triple-check the context name before running anything against a production-like cluster.

## Managing issues

This repo has no local `.github/ISSUE_TEMPLATE/`; GitHub serves the org-wide default template from
`fiaisis/.github` instead. Always use it rather than inventing an ad hoc issue format:

```shell
gh issue create --template Issue
```

Check `gh issue list` / `gh issue view <number>` first to avoid duplicates.

## Branch naming

Branches use `snake_case` descriptions of the work being done. Do not use category prefixes like
`feature/` or `fix/`.

- **Format**: `description_of_work` (e.g. `update_imat_smb_location`, `update_agent_guidelines`).

## Commit messages

`main`'s history (`git log --oneline main`) is predominantly **squash merges**: one commit per PR,
titled `<PR title> (#<PR number>)`, e.g. `Update imat smb location in the operator (#251)`. This has
two practical consequences:

- **The PR title is what actually survives into permanent history.** Individual commits on a working
  branch are squashed on merge, so getting the PR title right (see "Pull request process" below)
  matters more than crafting a commit-by-commit history.
- **Individual commit messages still matter for review and for the squash commit body.** By default,
  GitHub's squash merge concatenates every commit subject on the branch into the squashed commit's
  body. Write each commit as a focused, reviewable unit of work with a clear, imperative-mood
  summary line (and a short *why* body paragraph on non-trivial commits) — avoid noise like `wip` or
  `fix` that would leak into the squashed body. Leave auto-generated Dependabot commit messages as-is.

## Requirements for code changes

Only run tests, linters, and type checkers when they are **relevant to the files you actually
changed** — if a change only touches documentation (e.g. `.md` files), skip `pytest`, `ruff`, and
`mypy`. When you do change Python code, dependency manifests, or configuration, before considering
the change complete:
1. **Add or update unit tests** in `test/file_watcher/` or `test/file_watcher_operator/` for any new
   or changed behavior.
   *(Note: `test/file_watcher/docker-compose.yml` is a legacy Memphis-based E2E setup that is not
   wired into `pytest` or CI — there is currently no automated E2E suite to extend for this repo.)*
2. **Run the linter and type checker** (`ruff check .`, `ruff format .`,
   `mypy --strict file_watcher`) and fix everything reported — don't just rely on CI's auto-fix
   commit.
3. **Run the full test suite with coverage** and confirm coverage has not regressed compared to
   `main` (CI uploads results to Codecov; locally, add `--cov --cov-report=term-missing` to `pytest`
   to see uncovered lines before and after your change).

## Pull request process

1. **Local validation** before opening a PR (when `.py` / `pyproject.toml` / container files are
   changed; skip for `.md`-only changes):
   ```shell
   conda run -n <env-name> pytest .
   conda run -n <env-name> ruff check .
   conda run -n <env-name> ruff format .
   conda run -n <env-name> mypy --strict file_watcher
   ```
   Note: the CI workflow (`.github/workflows/formatting_and_linting.yml`) also runs `ruff format .`,
   `ruff check --fix`, and `mypy --strict file_watcher` on every non-`main` push and auto-commits
   any fixes back to the branch as `"Formatting and linting commit"`.
2. **PR title**: a clear, concise description in normal grammar/sentence case (e.g. `Update imat smb
   location in the operator`). Do not use `snake_case` for PR titles.
3. **PR description**: use the org-wide default template from `fiaisis/.github`
   (`.github/pull_request_template.md` — no local override in this repo), which is just
   `Closes # <Issue Number>` followed by a `## Description` section. Ask the user for the issue
   number if one isn't evident from the task, or confirm there is none to link.
4. **AI-assistance label**: attach a label to the PR indicating that it was made with the assistance
   of AI. Check existing labels first with `gh label list` (e.g. `AI` or `AI_code` in this org) and
   apply the matching label via `gh pr create --label <label>` or
   `gh pr edit <number> --add-label <label>`; if the repo does not have an AI label yet, create one
   first (`gh label create "AI" --description "Created with the assistance of AI"`).

## Handling Dependabot PRs

Dependabot creates PRs grouped as `python-packages` (weekly) and `action-packages` (daily, GitHub
Actions) per `.github/dependabot.yml`. When asked to handle these:

1. **List and identify**: `gh pr list` to find open Dependabot PRs.
2. **Review changes**: `gh pr diff <number>`, paying close attention to major version bumps in
   `pyproject.toml`, `container/*.D`, or `.github/workflows/`.
3. **Validate**:
   - Python/dependency changes: `gh pr checkout <number>`, install updated dependencies
     (`pip install ".[all]"`), and run `pytest .`, `ruff check .`, `ruff format --check .`, and
     `mypy --strict file_watcher`.
   - Action changes: verify CI checks pass with `gh pr checks <number>`.
4. **Approve and merge** once validation passes:
   ```shell
   gh pr review <number> --approve
   gh pr merge <number> --merge
   ```
   Use `--admin` if blocked by branch protection (`gh pr merge <number> --merge --admin`).
5. **Clean up**: close redundant/superseded PRs with `gh pr close <number> --delete-branch`.
6. **Final verification**: checkout `main`, pull latest changes, and re-run `pytest .` to confirm
   `main` is green.

## General working practices

- Prefer the `read` tool (with `offset`/`limit`) over `sed -n`, `cat`, or similar shell one-liners
  when inspecting file contents, and prefer the `write` and `edit` tools over `bash` (`cat <<EOF`,
  `sed -i`, `echo >`), Python one-liners, or other non-tool-based commands whenever creating or
  modifying files.
- Ask the user a clarifying question whenever a task is ambiguous (e.g. whether a change applies to
  `file_watcher` or `file_watcher_operator`, how an instrument's archive/SMB path is structured, or
  whether an issue number exists to link in a PR) rather than guessing.
- Keep dependency versions pinned with `==` in `pyproject.toml` consistent with the existing style.

## Keeping this document up to date

If a change you make would make any information in this `AGENTS.md` inaccurate or incomplete —
e.g. a new environment variable, a changed CRD field or directory layout, a new/renamed CI workflow,
an updated Dependabot grouping, a new sibling-repo contract, or a different testing/linting command
— update the relevant section of this file as part of the same change. Treat it the same way you
would treat `README.md`: documentation that goes stale is worse than no documentation, so don't
leave it to a future agent or PR to notice the drift.
