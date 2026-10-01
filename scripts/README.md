# Scripts

Utilities to set up Snowflake objects and run EKS-based ramp/sustain benchmarks for
elastic channels.

## Quick start

```bash
# 1. Install Python deps
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env

# 3. Create the default FDN objects (RESORT_SALES + RESORT_SALES_PIPE,
#    ELASTIC_K8S_METRICS, ELASTIC_RAMP_INGRESS + ELASTIC_RAMP_POLICY).
#    Pass ingest IP(s). The policy is not attached unless you add
#    --attach-network-policy=user (or account with the acknowledgement).
#    Re-running this truncates RESORT_SALES and drops any clustering key on it.
python scripts/setup_elastic_ramp.py --ingest-ip <your-public-ip>

# 4. One-time EKS cluster setup (see docs/architecture.md)
./infra/eks/create_cluster.sh
./infra/eks/build_and_push.sh
./infra/eks/create_secret.sh
./infra/eks/allowlist_eks.sh
./infra/eks/deploy_monitor.sh

# 5. Run a fixed-throughput benchmark (scales worker nodes up, runs, scales back to 0).
#    Default pipe is RESORT_SALES_PIPE. Add --pipe-name for Iceberg or a clustered pipe.
./infra/eks/run_job.sh <run-id> --target-bps <bytes_per_sec> --duration-seconds <seconds> \
    --workers 144 --pipe-name RESORT_SALES_PIPE

# 6. Query live/aggregated results directly from Snowflake — no local pull step;
#    the cluster-resident monitor writes continuously during the run.
#    SELECT * FROM SC_DB.SC_SCHEMA.ELASTIC_K8S_METRICS WHERE RUN_ID = '<run-id>' ORDER BY WINDOW_START;
```

Or use `make help` for shorthand targets.

## Scripts reference

| Script | Purpose |
|--------|---------|
| `scripts/setup_elastic_ramp.py` | Create `RESORT_SALES` (`ROW_TIMESTAMP = TRUE`) + `RESORT_SALES_PIPE`, `ELASTIC_K8S_METRICS`, and project-owned `ELASTIC_RAMP_INGRESS` / `ELASTIC_RAMP_POLICY`. Truncates `RESORT_SALES` and drops a clustering key on it. Default does **not** attach a network policy. |
| `scripts/preflight_ingest.py` | Open an elastic channel on `ELASTIC_PIPE_NAME` and verify streaming ingest is reachable |
| `scripts/run_worker_scale_probe.py` | Step worker count up or down across runs (`--pipe-name` selects the pipe). Leaves worker nodes up unless `--teardown-on-stop` |
| `scripts/benchmark_client_cpu.py` | Local, Snowflake-free micro-benchmark of row generation and worker-loop CPU cost |
| `scripts/render_architecture.sh` | Render `docs/architecture.png` from `docs/architecture.dot` |
| `scripts/deploy_streamlit.sh` | Deploy `ELASTIC_RAMP_MONITOR` (`streamlit/Monitor_App.py` + `streamlit/pages/`) with `snow streamlit deploy`. Needs a Snowflake CLI connection profile (`SNOWFLAKE_CLI_CONNECTION`, default `snowflake-elastic-channels`) and `SNOWFLAKE_PASSWORD` |
| `scripts/setup_streamlit_pypi.py` | Create the PyPI external access integration the Streamlit app needs. Invoked by `deploy_streamlit.sh` |
| `infra/eks/create_cluster.sh` | One-time: create the persistent EKS cluster + namespace/RBAC |
| `infra/eks/build_and_push.sh` | Build the worker/monitor container image (arm64) and push to ECR |
| `infra/eks/create_secret.sh` | Create/update the `snowflake-credentials` Kubernetes Secret from `.env` |
| `infra/eks/allowlist_eks.sh` | Merge the cluster NAT IPs and this host into `ELASTIC_RAMP_INGRESS` (one-time per cluster). Does not attach the network policy |
| `infra/eks/deploy_monitor.sh` | (Re)deploy the single persistent monitor Deployment |
| `infra/eks/scale_workers.sh` | Scale the "workers" managed node group up/down |
| `infra/eks/run_job.sh` | Run a fixed-throughput benchmark: scale nodes, apply the worker Indexed Job, wait, scale back down. `--pipe-name` (default `RESORT_SALES_PIPE` or `ELASTIC_PIPE_NAME`), `--fast-json-encode`, `--keep-nodes`, `--image` |
| `infra/eks/teardown_workers.sh` | Scale worker nodes to zero between runs |
| `infra/eks/teardown_cluster.sh` | Fully delete the EKS cluster (rarely needed; cluster is meant to be persistent) |

See [`docs/architecture.md`](../docs/architecture.md) for EKS-specific details (cluster
layout, worker sizing, manifests).

## Landing targets

`run_job.sh` and `run_worker_scale_probe.py` send every worker to one pipe. The default
setup script creates only the FDN pipe. The other SQL files use `{database}`, `{schema}`,
and `{ingest_role}` placeholders; substitute those and apply the file before using the pipe.

| Pipe | SQL |
|------|-----|
| `RESORT_SALES_PIPE` | `sql/setup/setup_elastic_ramp.sql` (via `setup_elastic_ramp.py`) |
| `RESORT_SALES_ICEBERG_PIPE` | `sql/setup/setup_elastic_iceberg.sql` |
| `RESORT_SALES_FDN_CLUSTERED_PIPE` | `sql/setup/setup_elastic_clustered.sql` |
| `RESORT_SALES_ICEBERG_CLUSTERED_PIPE` | `sql/setup/setup_elastic_clustered.sql` |
| `RESORT_SALES_ICEBERG_CLUSTERED_16MB_PIPE` | `sql/setup/setup_elastic_iceberg_clustered_16mb.sql` |

```bash
./infra/eks/run_job.sh <run-id> --target-bps <bytes_per_sec> --duration-seconds <seconds> \
    --workers <1-455> --pipe-name RESORT_SALES_ICEBERG_PIPE

python scripts/run_worker_scale_probe.py --direction up --pipe-name RESORT_SALES_PIPE
./infra/eks/teardown_workers.sh   # probe leaves nodes up unless --teardown-on-stop
```

## Network allowlist

`setup_elastic_ramp.py` creates and updates only project-owned objects:

- schema rule `{database}.{schema}.ELASTIC_RAMP_INGRESS` (ingest NAT IPs plus `--orchestrator-ip`)
- account policy `ELASTIC_RAMP_POLICY` (allowed-rule list = that ingress rule)

It does **not** mutate lab objects (`USER_IPS`, `limited_policy`) and does **not**
`ALTER ACCOUNT SET NETWORK_POLICY` unless you opt in.

```bash
# Default: update the rule + policy objects; print how to attach later
python scripts/setup_elastic_ramp.py --ingest-ips 203.0.113.1 --orchestrator-ip 198.51.100.2

# Enforce for the ingest user only (preferred)
python scripts/setup_elastic_ramp.py --ingest-ips 203.0.113.1 --orchestrator-ip 198.51.100.2 \
    --attach-network-policy=user

# Replace the *account* network policy (can lock other users out)
python scripts/setup_elastic_ramp.py --ingest-ips 203.0.113.1 --orchestrator-ip 198.51.100.2 \
    --attach-network-policy=account \
    --i-understand-this-replaces-the-account-network-policy

# Add the project rule to a policy you already have
python scripts/setup_elastic_ramp.py --ingest-ips 203.0.113.1 \
    --existing-network-policy MY_EXISTING_POLICY
```

`--bypass-mins` remains an ACCOUNTADMIN escape hatch for `SNOWFLAKE_USER`; it is not the default path.

## Python modules

| Module | Purpose |
|--------|---------|
| `src/elastic_channels/ingest_loop.py` | Shared SDK ingest batch loop, tier helpers, throughput checks |
| `src/elastic_channels/resort_sales.py` | Synthetic `RESORT_SALES` row generator, with the fast NDJSON encode path |
| `src/elastic_channels/fast_append.py` | Direct append path used by `ELASTIC_FAST_JSON_ENCODE` |
| `src/elastic_channels/auth.py` | Snowflake connector/ingest SDK credential resolution |
| `src/elastic_channels/sdk_stats.py` | Scrape + delta the ingest SDK's own Prometheus metrics (compression, buffering) |
| `src/elastic_channels/k8s_worker.py` | EKS pod entrypoint: one worker per pod, fixed-rate ingest, JSON stats to stdout |
| `src/elastic_channels/k8s_monitor.py` | EKS cluster-resident monitor: tails worker logs, aggregates, writes to Snowflake |
| `src/elastic_channels/network_setup.py` | Build SQL for `ELASTIC_RAMP_INGRESS` / `ELASTIC_RAMP_POLICY` and the opt-in attach modes |

## Local-only workflow

For unit tests and CPU micro-benchmarking without a live cluster or Snowflake account:

```bash
make test
python scripts/benchmark_client_cpu.py --workers 96 --aggregate-mbps 6900
```

Integration benchmarks against live Snowflake run on EKS (see Quick start above).
