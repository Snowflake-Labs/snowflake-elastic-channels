# snowflake-elastic-channels

A benchmark of **Snowpipe Streaming elastic channels** — high-throughput ingest using the
[snowflake-ingest-sdk](https://github.com/snowflake-eng/snowflake-ingest-sdk) (`snowpipe-streaming`
Python package), run on a persistent **Amazon EKS cluster** with one lightweight worker per
container.

Each worker pod runs a single SDK client that writes synthetic `RESORT_SALES` records at a fixed
target rate and reports its own throughput as JSON to stdout. A cluster-resident monitor tails
every worker's logs, aggregates them, and writes live performance data straight into a Snowflake
table — see [`docs/architecture.md`](./docs/architecture.md) for the full design.

## Highlights

- **Elastic channels** (`client.get_elastic_channel()`) scale cleanly to hundreds of lightweight
  worker processes with no explicit channel naming or offset tracking required — measured clean,
  linear scaling up to **38.97 GiB/s (133.5M rows/sec)** at 455 workers, the maximum supported
  worker count.
- A native-encode fast path (`ELASTIC_FAST_JSON_ENCODE`, on by default) pre-encodes each worker's
  payload in Rust before handing it to the SDK, keeping per-worker throughput high and consistent
  at every scale tested.
- Worker pods are right-sized from measured CPU/memory usage so a single cluster node hosts as
  many workers as possible, minimizing the EC2 footprint per unit of throughput.

## Quick start

One-time setup. EKS workers authenticate with a key pair (`SNOWFLAKE_PRIVATE_KEY` or
`SNOWFLAKE_PRIVATE_KEY_PATH` in `.env`). `setup_elastic_ramp.py` creates the default FDN
table and pipe, the metrics table, and the project network rule and policy. It does **not**
attach that policy. Re-running it truncates `RESORT_SALES` and drops any clustering key on
that table.

```bash
pip install -r requirements.txt
cp .env.example .env                        # Snowflake credentials (key pair required for EKS)
python scripts/setup_elastic_ramp.py --ingest-ip <IP>
./infra/eks/create_cluster.sh               # create the persistent EKS cluster
./infra/eks/build_and_push.sh               # build + push the worker/monitor image
./infra/eks/create_secret.sh                # Snowflake credentials -> k8s Secret
./infra/eks/allowlist_eks.sh                # merge NAT + this host into ELASTIC_RAMP_INGRESS
./infra/eks/deploy_monitor.sh               # deploy the persistent monitor
```

`allowlist_eks.sh` updates `ELASTIC_RAMP_INGRESS` and `ELASTIC_RAMP_POLICY` only. To enforce
the policy, re-run setup with `--attach-network-policy=user` (preferred) or
`--attach-network-policy=account` plus
`--i-understand-this-replaces-the-account-network-policy`. See
[`scripts/README.md`](./scripts/README.md).

`make eks-up` runs the cluster, image, secret, allowlist, and monitor steps. `make test` runs
the unit tests locally.

### Run a quick, low-cost test

A single worker for one minute — useful for verifying the pipeline end-to-end at minimal cost:

```bash
./infra/eks/run_job.sh quicktest-$(date +%Y%m%d%H%M%S) \
    --target-bps 100000000 --duration-seconds 60 --workers 1
```

### Run at any scale, up to the tested maximum

`--workers` accepts any value from **1 to 455**. The node group is scaled up to fit automatically
before the run and back to zero as soon as it finishes — there's no need to size the cluster by hand:

```bash
./infra/eks/run_job.sh <run-id> --target-bps <aggregate_bytes_per_sec> \
    --duration-seconds <seconds> --workers <1-455>
```

`make run-benchmark RUN_ID=<run-id> TARGET_BPS=<n> DURATION_SECONDS=<n> WORKERS=<n>` wraps the same call.
The default pipe is `RESORT_SALES_PIPE`. Pass `--pipe-name` to write to another pipe
(Iceberg or a clustered comparison table) after that pipe exists:

```bash
./infra/eks/run_job.sh <run-id> --target-bps <aggregate_bytes_per_sec> \
    --duration-seconds <seconds> --workers <1-455> \
    --pipe-name RESORT_SALES_ICEBERG_PIPE
```

Results stream live into Snowflake for the whole duration of the run — no local pull step:

```sql
SELECT * FROM SC_DB.SC_SCHEMA.ELASTIC_K8S_METRICS WHERE RUN_ID = '<run-id>' ORDER BY WINDOW_START;
```

Raw per-pod output (throughput, batch-ack latency) is also available directly from Kubernetes:

```bash
kubectl -n elastic-ramp logs -l run-id=<run-id> --prefix --tail=-1
```

A live dashboard is also available in Snowflake via [`streamlit/Monitor_App.py`](./streamlit/Monitor_App.py),
with a second page, [`streamlit/pages/01_Resort_Sales.py`](./streamlit/pages/01_Resort_Sales.py), visualizing
live resort sales — revenue, mix, geography, and a scrolling transaction feed. Deploy it with
`make deploy-streamlit` (or `./scripts/deploy_streamlit.sh`). That requires the Snowflake CLI
and a connection profile named `snowflake-elastic-channels` (override with `SNOWFLAKE_CLI_CONNECTION`).

## Landing target

`ELASTIC_PIPE_NAME` (default `RESORT_SALES_PIPE`) selects the pipe. `run_job.sh --pipe-name`
overrides it for one run.

| Target | Created by | Pipe |
|--------|------------|------|
| FDN, `ROW_TIMESTAMP = TRUE` | `scripts/setup_elastic_ramp.py` | `RESORT_SALES_PIPE` |
| Snowflake-managed Iceberg | `sql/setup/setup_elastic_iceberg.sql` | `RESORT_SALES_ICEBERG_PIPE` |

The Iceberg and clustered SQL files are not applied by setup. Substitute `{database}`,
`{schema}`, and `{ingest_role}`, then run them before pointing a job at those pipes.
Clustered comparison pipes are `RESORT_SALES_FDN_CLUSTERED_PIPE`,
`RESORT_SALES_ICEBERG_CLUSTERED_PIPE`, and `RESORT_SALES_ICEBERG_CLUSTERED_16MB_PIPE`
(`sql/setup/setup_elastic_clustered.sql`, `sql/setup/setup_elastic_iceberg_clustered_16mb.sql`).

Before a live run, `python scripts/preflight_ingest.py` opens an elastic channel on
`ELASTIC_PIPE_NAME` and checks that streaming ingest is reachable.

## Channel mode

Every worker calls `client.get_elastic_channel()` — the elastic-channel API this project is named
for, which pools channels automatically under one client with no explicit naming or offset
tracking required.

## Fast JSON encode path

By default, each worker pre-encodes its own NDJSON payload — in Rust via a small native extension
([`native/`](./native/), built into the worker image) when available, otherwise directly in Python
— and appends it straight through the elastic channel, skipping a redundant re-encode step. It
falls back automatically to the SDK's default encode path when the native extension isn't
installed. Pass `--fast-json-encode false` to disable it and use the SDK's default path instead:

```bash
./infra/eks/run_job.sh <run-id> --target-bps <n> --duration-seconds <n> --fast-json-encode false
```

## EKS

- Cluster: `snowflake-elastic-channels` (`us-west-2`), persistent — see [`docs/architecture.md`](./docs/architecture.md)
- Worker node group: `m7g.8xlarge` (Graviton/arm64), 31 worker pods/node, scaled to fit `--workers` per run and back to zero afterward
- Worker pod: 1 vCPU / 1Gi memory (right-sized from measured usage with fast JSON encode), one SDK client each
- Supported worker count: **1 to 455** per run
- Monitor: single persistent `Deployment` replica on the always-on `system` node group (`m7g.large`)
- Auth: key-pair via `SNOWFLAKE_PRIVATE_KEY`/`SNOWFLAKE_PRIVATE_KEY_PATH`, injected into pods via the `snowflake-credentials` Kubernetes Secret (`infra/eks/create_secret.sh`)

## Layout

| Path | Purpose |
|------|---------|
| [`src/elastic_channels/k8s_worker.py`](./src/elastic_channels/k8s_worker.py) | EKS pod entrypoint: one worker per pod, fixed-rate ingest, JSON stats to stdout |
| [`src/elastic_channels/k8s_monitor.py`](./src/elastic_channels/k8s_monitor.py) | Cluster-resident monitor: tails worker logs, aggregates, writes to Snowflake |
| [`src/elastic_channels/ingest_loop.py`](./src/elastic_channels/ingest_loop.py) | Shared SDK ingest batch loop |
| [`src/elastic_channels/sdk_stats.py`](./src/elastic_channels/sdk_stats.py) | Reads compressed/uncompressed throughput from the SDK's own metrics |
| [`src/elastic_channels/resort_sales.py`](./src/elastic_channels/resort_sales.py) | Synthetic `RESORT_SALES` row generator, with the fast NDJSON encode path |
| [`src/elastic_channels/fast_append.py`](./src/elastic_channels/fast_append.py) | Direct append path used by `ELASTIC_FAST_JSON_ENCODE` |
| [`native/`](./native/) | Rust NDJSON encoder, built into the worker image, used by default |
| [`infra/eks/run_job.sh`](./infra/eks/run_job.sh) | Per-run orchestrator: scale nodes, submit the worker job, wait, scale down |
| [`infra/eks/create_cluster.sh`](./infra/eks/create_cluster.sh) | One-time: create the persistent EKS cluster |
| [`scripts/setup_elastic_ramp.py`](./scripts/setup_elastic_ramp.py) | Create the FDN table, pipe, metrics table, and project network rule/policy |
| [`scripts/benchmark_client_cpu.py`](./scripts/benchmark_client_cpu.py) | Local, Snowflake-free client throughput micro-benchmark |
| [`scripts/run_worker_scale_probe.py`](./scripts/run_worker_scale_probe.py) | Step worker count up or down across runs to find a scaling ceiling |
| [`sql/setup/`](./sql/setup/) | Default FDN DDL, plus optional Iceberg and clustered landing targets |
| [`streamlit/Monitor_App.py`](./streamlit/Monitor_App.py) | Live throughput/latency dashboard |
| [`streamlit/pages/01_Resort_Sales.py`](./streamlit/pages/01_Resort_Sales.py) | Live resort-sales page: revenue, mix, geography, transaction feed |

See [`docs/architecture.md`](./docs/architecture.md) for the full design and
[`scripts/README.md`](./scripts/README.md) for the complete script reference.

## License

Copyright (c) Snowflake Inc. All rights reserved.

Licensed under the Apache 2.0 license.
