# Chubut Well Production Explorer

Client-side explorer for all **22,573 Chubut (Argentina) wells** — monthly oil, gas and water production (2006–2025) — built on [petroDB](https://github.com/oskrgab/petrodb).

Everything runs in the browser: DuckDB-WASM queries pre-filtered Parquet files locally, so there is no backend and no server-side state.

## How it works

- `build_data.py` reads petroDB over HTTP Range from Hugging Face, filters to Chubut, sorts by well, and writes compact Parquet extracts into `data/`.
- `index.html` loads those files with DuckDB-WASM and provides search, filters, and per-well production charts (ECharts).

The `data/` directory is gitignored: it is regenerated on demand and never committed.

## Run locally

Requires [uv](https://docs.astral.sh/uv/).

```powershell
uv run python build_data.py          # build data/ (first time, or to refresh)
uv run python -m http.server 8000    # then open http://localhost:8000
```

## Deploy to GitHub Pages

Deployment is automated with GitHub Actions (`.github/workflows/deploy.yml`): on every push to `main` it rebuilds the data and deploys `index.html` + `data/` to Pages.

One-time setup:

1. Make the repository **public** (Pages on the free plan requires a public repo).
2. Enable Pages: **Settings → Pages → Build and deployment → Source → GitHub Actions**.

After that, every push to `main` redeploys automatically. The site is published at `https://<user>.github.io/Oil_dashboard/`.

### If a workflow run fails

- **`deploy` step fails** (error mentioning the Pages site / environment): Pages is not enabled yet. Do setup step 2 above, then re-run the failed job (**Actions → the failed run → Re-run jobs**) or push a new commit.
- **`build` step fails**: `build_data.py` could not reach Hugging Face or errored — check the job log.

## Data

- Source: [petroDB](https://huggingface.co/datasets/oskrgab/petrodb) (CC-BY-4.0; Argentina well data from the Secretaría de Energía public datasets).
- The extract is historical and is not a real-time production feed.

## Notes

- Argentina `prod_gas` is reported in thousands of m³; the dashboard multiplies by 1,000 so oil, gas and water all display in m³.
- Injection volumes (`iny_*`) are present in the data but not charted.
