# Agents guide — chgk_results

Human spec: [`task.md`](task.md). How to run: [`readme.md`](readme.md).

The current loader writes Postgres schema `chgk`. The previous parquet pipeline is in `legacy/` and is not the target.

## Do not

- Run git commands while implementing the task in `task.md`.
- Execute SQL against a database. Schema lives in `schema.sql`; tests inspect that text and the Python row builders.
- Edit `*.ipynb`.
- Print or commit `secrets.yaml`, the API token, or `database_url`.

## Pipeline

`chgk_results.py` is the CLI: `load-one`, `update`, `load-days --days N`, `load-all`. Dependencies live in a project-local `.venv` (`python3 -m venv .venv` from the repo directory, or pyenv 3.10.12). The shared `/root/python/.venv` is not used and is not removed.

`write_sql` in `config.json` gates `PostgresStore.replace_tournament`. Local default is `config.example.json` (`write_sql` false). Production file is `config.prod.json`. The loader connects as `chgk_owner` to PostgreSQL 16 on `127.0.0.1:5434`. Port 5432 is the leftover PostgreSQL 12 cluster; do not create `chgk` there and do not stop that cluster. External clients use `chgk_reader` (`SELECT` only, `roles.sql`) at `45.132.18.2:5434` with `sslmode=require`. Checks run in both modes.

One tournament is fetched, checked, built, checked again, then replaced inside a single transaction: temp tables, count checks, delete, insert. A hidden payload does not replace a tournament that already has a main-packet mask.

`potential` is the mean difficulty of taken active questions. `c_star` is the notebook ratio on the sum `potential * k`. Removed questions (`X` or `canceledQuestions`) and shootout rows stay out of both.

Play date for a synch comes from `GET /tournaments/{id}/requests`, matched by `synchRequest.id`.

`load-days --days N` reloads tournaments with `dateEnd` inside that many days and does not skip unchanged rows. Resume for `load-all` reads `chgk.tournament_load` and skips `status = ok` with the same `last_edit_date`.
