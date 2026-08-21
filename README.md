# Graph Database Cloud Benchmarking Suite

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-brightgreen.svg)](https://www.python.org/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

An automated benchmark harness for comparing **CognoDB Cloud**, **Neo4j**, **Memgraph**, **FalkorDB**, and **ArangoDB** on one derived SNAP graph and one logical workload suite.

## What this repository measures

The full suite records:

- node and relationship ingestion throughput plus wall-clock load time;
- 1-hop, 2-hop, and 3-hop traversal latency;
- point-lookup and indexed-filter latency;
- category count/group-by latency;
- synchronized mixed read/write throughput at 1, 10, and 40 clients; and
- resource information that is observable from the client or local container runtime.

Read workloads report at least p50 and p95 latency after warm-up. The harness also retains p90, p99, mean, standard deviation, error counts, and limited error details in JSON.

## Platforms and resource configuration

The table below describes the configuration in `docker-compose.yml` and CognoDB Cloud's console specification:

| Platform | Version or tier | Deployment | CPU allocation | Memory allocation | Storage allocation |
|---|---|---|---:|---:|---|
| CognoDB Cloud | c0 free tier | Managed cloud (`us-east4`) | 0.5 burstable vCPU | 512 MB (console verified) | 1 GiB managed quota (500 IOPS) |
| Neo4j Community | `neo4j:5.18.0-community` | Local container | 0.5 vCPU | 512 MB cgroup (192MB heap, 64MB page cache) | Host filesystem |
| Memgraph | `memgraph/memgraph:2.16.0` | Local container | 0.5 vCPU | 512 MB cgroup (--memory-limit=384 in RAM) | Host filesystem |
| FalkorDB | `falkordb/falkordb:v4.2.1` | Local container | 0.5 vCPU | 512 MB cgroup (Redis GraphBLAS) | Host filesystem |
| ArangoDB | `arangodb:3.11.8` | Local container | 0.5 vCPU | 512 MB cgroup (RocksDB engine) | Host filesystem |

### Fairness, evidence, and network topology notes

- **Memory Parity (512 MB):** While earlier assessment documentation described c0 as 256 MB, live inspection of the CognoDB Cloud console on 21 August 2026 confirmed that the provisioned `c0` instance is allocated **512 MB RAM**, **1 GiB storage**, **0.5 burstable vCPU**, and up to **500 IOPS** in region `us-east4` (archived in `evidence/cognodb-c0-allocation.png`). All four local containerized comparators are configured with matching 512 MB cgroup ceilings in `docker-compose.yml`.
- **Storage Configuration:** CognoDB Cloud enforces a managed 1 GiB cloud storage quota with up to 500 disk IOPS. Local containers run on the host filesystem without an artificial disk quota.
- **Network Asymmetry:** CognoDB Cloud is reached over public TLS/WAN from South Asia (`ap-south-1`) to Northern Virginia (`us-east4`), incurring physical network round-trip latency ($\approx 287\text{ ms}$ RTT empirically measured via live TCP handshake) and managed cloud IOPS throttling. Local containers execute over `localhost` loopback ($<0.5\text{ ms}$ latency) on host NVMe SSD storage. This real-world comparison highlights the trade-offs of managed cloud free-tiers versus self-hosted containerized engines.

## Dataset

### Public source and loaded size

- **Source:** [Stanford SNAP ca-AstroPh collaboration network](https://snap.stanford.edu/data/ca-AstroPh.html)
- **Raw parsed source:** 18,771 nodes and 396,100 directional records, canonicalized to 198,050 unique undirected collaborations
- **Loaded nodes:** 17,441 sampled active endpoints
- **Loaded relationships:** 100,000
- **Derived files:** `data/nodes.csv`, `data/edges.csv`, and `data/dataset_stats.json`

The harness parses non-comment, non-self-loop pairs from the SNAP archive, canonicalizes each pair as `(min_id, max_id)`, and deduplicates the 396,100 directional records into 198,050 unique undirected collaborations. Dataset generator version 3 then chooses a uniform deterministic sample of 100,000 unique collaborations with seed `20240821`. It derives the 17,441-node set from those sampled endpoints and renumbers it to consecutive IDs, so the benchmark does not load isolated nodes that are absent from the sampled graph.

SNAP ca-AstroPh is an undirected collaboration network. Each retained pair is stored once as a `RELATION` edge; traversal adapters treat either endpoint as adjacent so that traversal semantics remain undirected across engines.

The source does not provide the benchmark properties used by the lookup and aggregation workloads. With seed 42, `data/download_dataset.py` deterministically adds:

- `name`, `category`, `year`, and `score` node properties;
- a numeric edge `weight`; and
- a synthetic `type` property chosen from `CITES`, `COLLABORATES`, and `REFERENCES`.

These are synthetic benchmark attributes, not original SNAP metadata. The dataset record includes the source URL, source-archive SHA-256, sampling strategy, and derived-file hashes. The final JSON also records a query-trace hash so every platform run can be tied to identical inputs and parameters.

The production path fails closed if the SNAP archive cannot be downloaded, parsed, or shown to contain at least 100,000 edges. A deterministic Barabási–Albert fallback exists only behind the explicit `allow_synthetic=True` development option and is labelled `synthetic_barabasi_albert`; it must not be reported as a SNAP benchmark run.

Traversal starts are selected only from active nodes and stratified across degree quartiles with seed `4242`. The runner uses the first 100 of the 200 recorded starts for each timed read workload. A median-degree active node is used for exact semantic validation against reference answers calculated directly from the CSV files.

## Methodology

### Common execution sequence

For each platform, the runner:

1. connects using the platform adapter;
2. clears the configured benchmark database and verifies zero nodes and zero relationships;
3. creates or verifies indexes;
4. loads the same node and edge CSV files in outer batches of 1,000;
5. hard-fails if a batch count or final database count differs from the CSV count;
6. performs semantic preflight queries;
7. warms each read workload before timing it;
8. runs the traversal, lookup, aggregation, and mixed workloads; and
9. writes JSON, a generated Markdown matrix, and charts.

All timings are measured on the same Python client with `time.perf_counter()`. The adapters express equivalent logical operations in Cypher, FalkorDB's Cypher interface, or AQL.

### Workload configuration

| Workload | Full-run configuration | Warm-up | Reported result |
|---|---|---:|---|
| Ingestion | 17,441 nodes and 100,000 unique undirected relationships; outer batch size 1,000 | N/A | nodes/s, relationships/s, node time, edge time, total time, verified counts |
| Traversal | 100 sampled start nodes for each of 1, 2, and 3 hops | 10 queries per hop | p50/p95 latency and supporting distribution/error fields |
| Point lookup | 100 deterministic node IDs | 5 queries | p50/p95 latency |
| Filtered lookup | 100 seed-42 `category` and minimum-`year` predicates | 5 queries | p50/p95 latency |
| Aggregation | 100 category count/group-by queries | 3 queries | p50/p95 latency |
| Mixed read/write | 1, 10, and 40 synchronized clients; deterministic target mix of 80% traversal reads and 20% score updates; 30-second steady-state window at each level | 2 warmup operations per client | successful QPS, p50/p95, empirical mix, attempts, successes, and errors |

The full mixed test uses synchronized, duration-based windows. Thirty seconds per concurrency level is the default and can be changed with `--mixed-duration-seconds`. Quick mode instead uses 15 operations per client and is not suitable for published throughput.

### Indexes

Each adapter creates separate indexes for `Node.id`, `Node.category`, and `Node.year` using the closest supported platform syntax. The filtered lookup uses category equality plus a minimum-year predicate. These are separate property indexes, not a claimed composite index.

### Resource observations

CognoDB resource consumption is not exposed through the benchmark connection, so the harness reports the advertised c0 quota and says that live usage is not observable. Local adapters query point-in-time container-memory observations after ingestion and again after the workload; FalkorDB also reports Redis memory where available. The current harness does not continuously sample peak memory and does not measure stored-data size. Those omissions must remain explicit in the final report.

## Reproduce the benchmark

### Prerequisites

- Python 3.11 or newer
- Docker with Docker Compose
- a dedicated CognoDB c0 instance

> **Destructive-operation warning:** every target configured in `.env` must be a disposable benchmark database. The ingestion stage deletes the existing graph before loading the dataset, and the mixed workload updates node scores. Never point this harness at a database containing data you need.

### 1. Create an environment and install dependencies

From a clone or checkout of this repository:

```bash
python -m venv .venv
```

Activate the environment for your shell, then install the exact versions:

```bash
python -m pip install -r requirements.txt
```

### 2. Configure credentials

Copy `.env.example` to `.env` and populate the CognoDB values:

```env
COGNODB_URI=bolt+s://<instance-id>.databases.cognodb.cloud
COGNODB_USER=cognodb
COGNODB_PASSWORD=<your-password>
```

The runner validates the assessment-specified `*.databases.cognodb.cloud` host before it
touches any database. If CognoDB has explicitly issued a legacy host with another suffix,
confirm it with the provider before setting `COGNODB_ALLOW_NONSTANDARD_HOST=1`.

`.env` is ignored by Git. Do not commit connection URIs or passwords.

Declare the actual locations used for the final run so they are retained in result metadata:

```env
BENCHMARK_CLIENT_REGION=<client-location>
COGNODB_REGION=<instance-region>
```

### 3. Start the local databases

```bash
docker compose up -d
```

The current Compose file has no health checks. The runner retries each connection up to 10 times with a three-second delay, but it is still preferable to wait for all four services to become ready before starting the expensive run. Connectivity tests require live services and configured cloud credentials:

```bash
python -m pytest -m integration tests/test_connections.py -q
```

Ordinary test runs exclude the marked live integration tests and are database-free:

```bash
python -m pytest -q
```

### 4. Run a quick preflight

```bash
python run_benchmark.py --all --quick --fresh
```

Quick mode is for functional validation only and must not be published as the full benchmark.

### 5. Run the full suite

```bash
python run_benchmark.py --all --fresh
```

Individual targets can be rerun with, for example:

```bash
python run_benchmark.py --target cognodb
```

## Results Matrix

All 5 platforms completed the full benchmarking suite with verified $17,441\text{ nodes}$ and $100,000\text{ relationships}$, passing exact CSV-derived ground-truth semantic correctness ($6/6\text{ checks}$).

### 1. Execution Status & Semantic Correctness
| Platform | Status | Semantic Validation | Run Timestamp | Concurrency Workload Errors |
|---|---|---|---|---|
| **CognoDB Cloud** | **complete** | **PASS (6/6 checks)** | 2026-08-21T05:30:07Z | 0 errors across 5,936 mixed operations |
| **Neo4j 5 (Capped)** | **complete** | **PASS (6/6 checks)** | 2026-08-21T06:32:50Z | 0 errors across 35,401 mixed operations |
| **Memgraph 2.16 (In-Memory C++)** | **complete** | **PASS (6/6 checks)** | 2026-08-21T05:46:37Z | 1 write timeout at 40 clients (0.004% error rate) |
| **FalkorDB 4.2.1 (GraphBLAS)** | **complete** | **PASS (6/6 checks)** | 2026-08-21T05:56:05Z | 0 errors across 81,674 mixed operations |
| **ArangoDB 3.11** | **complete** | **PASS (6/6 checks)** | 2026-08-21T05:58:07Z | 1 write error at 10 clients, 2 at 40 clients (0.01% error rate) |

### 2. Data Loading & Verified Throughput
| Platform | Loaded Nodes | Loaded Edges | Count Check | Wall Time (s) | Ingestion Nodes/s | Ingestion Rels/s |
|---|---:|---:|:---:|---:|---:|---:|
| **CognoDB Cloud** | 17,441 | 100,000 | **PASS** | 559.44 | 1,632.0 | 182.8 |
| **Neo4j 5 (Capped)** | 17,441 | 100,000 | **PASS** | 28.72 | 2,718.3 | 4,540.8 |
| **Memgraph 2.16 (In-Memory C++)** | 17,441 | 100,000 | **PASS** | 10.87 | 8,856.0 | 11,647.7 |
| **FalkorDB 4.2.1 (GraphBLAS)** | 17,441 | 100,000 | **PASS** | 10.66 | 9,897.3 | 11,654.0 |
| **ArangoDB 3.11** | 17,441 | 100,000 | **PASS** | 5.62 | 17,635.6 | 23,075.7 |

### 3. Read Query Latency (100 Timed Samples per Workload, ms)
| Platform | 1-Hop Traversal (p50 / p95) | 2-Hop Traversal (p50 / p95) | 3-Hop Traversal (p50 / p95) | Point Lookup (p50 / p95) | Filtered Lookup (p50 / p95) | Aggregation (p50 / p95) |
|---|---|---|---|---|---|---|
| **CognoDB Cloud** | 249.3 / 277.9 | 244.4 / 306.3 | 259.6 / 642.4 | 247.1 / 300.2 | 257.5 / 306.6 | 275.4 / 319.6 |
| **Neo4j 5 (Capped)** | 2.91 / 81.9 | 2.55 / 78.8 | 4.43 / 86.3 | 2.25 / 73.7 | 7.13 / 76.9 | 9.48 / 83.2 |
| **Memgraph 2.16** | 1.16 / 1.58 | 1.34 / 1.99 | 2.14 / 27.3 | 1.16 / 1.60 | 2.49 / 3.63 | 6.64 / 45.8 |
| **FalkorDB 4.2.1** | 1.05 / 1.19 | 1.26 / 2.44 | 3.95 / 85.1 | 0.89 / 1.03 | 2.08 / 3.30 | 5.18 / 45.7 |
| **ArangoDB 3.11** | 44.02 / 44.5 | 44.05 / 45.4 | 46.81 / 66.5 | 43.97 / 44.5 | 48.02 / 52.3 | 55.85 / 59.4 |

### 4. Mixed Read/Write Concurrency Scaling (80% Read / 20% Write, 30s Windows)
| Platform | 1 Client QPS (p50 / p95 ms) | 10 Clients QPS (p50 / p95 ms) | 40 Clients QPS (p50 / p95 ms) |
|---|---|---|---|
| **CognoDB Cloud** | 3.9 QPS (249.9 / 305.9 ms) | 39.5 QPS (245.3 / 303.1 ms) | **153.4 QPS** (251.2 / 304.7 ms) |
| **Neo4j 5 (Capped)** | 227.4 QPS (1.84 / 5.71 ms) | 320.8 QPS (6.64 / 91.03 ms) | **628.8 QPS** (75.15 / 106.5 ms) |
| **Memgraph 2.16** | 680.2 QPS (1.33 / 1.94 ms) | 1,048.3 QPS (8.39 / 15.82 ms) | **884.4 QPS** (39.37 / 71.60 ms) |
| **FalkorDB 4.2.1** | 839.2 QPS (1.15 / 1.53 ms) | 858.9 QPS (2.49 / 80.39 ms) | **1,022.8 QPS** (11.73 / 94.38 ms) |
| **ArangoDB 3.11** | 22.6 QPS (44.00 / 47.56 ms) | 211.5 QPS (47.15 / 52.89 ms) | **670.6 QPS** (59.01 / 82.74 ms) |

### 5. Observed Container Footprint & Storage Architecture
| Platform | Resource Limits | Observed Container Footprint | Storage Architecture |
|---|---|---|---|
| **CognoDB Cloud** | 0.5 burstable vCPU, 512MB RAM, 1GiB disk, 500 IOPS | Managed Cloud (Not directly observable via Bolt) | Proprietary managed graph store |
| **Neo4j 5 (Capped)** | 0.5 vCPU, 512MB RAM container limit | 509.6 MiB / 512 MiB | Native graph storage (192MB JVM Heap, 64MB PageCache) |
| **Memgraph 2.16** | 0.5 vCPU, 512MB RAM container limit | 97.7 MiB / 512 MiB | In-memory C++ pointer graph |
| **FalkorDB 4.2.1** | 0.5 vCPU, 512MB RAM container limit | 170.3 MiB / 512 MiB | GraphBLAS sparse matrices in Redis |
| **ArangoDB 3.11** | 0.5 vCPU, 512MB RAM container limit | 350.5 MiB / 512 MiB | Multi-model document/edge store (RocksDB) |

## Known limitations to carry into the final report

- **Execution Provenance:** The 5 platform results represent isolated per-target full runs on uniform 512MB RAM / 0.5 CPU cgroups consolidated into the canonical benchmark matrix.
- **ArangoDB Ingestion Durability:** The published ArangoDB ingestion result (5.62s / 23,075 rels/s) utilized asynchronous batch insertion (`insert_many(sync=False)`); the submitted repository defaults to synchronous write durability (`sync=True`) for parity with transactional Bolt UNWIND.
- **Mixed Workload Concurrency Errors:** Under peak 40-client saturation on 0.5 CPU containers, minor transient errors occurred on Memgraph (1 error out of 26,577 operations, 0.004%) and ArangoDB (1 error out of 6,353 ops at 10 clients, 2 errors out of 20,174 ops at 40 clients), which are fully disclosed in the results matrix.
- **Storage Parity:** CognoDB Cloud enforces a managed 1 GiB quota (500 IOPS), while local containers use the host filesystem under identical 512 MB RAM ceilings.
- **Network Topology:** CognoDB includes WAN/TLS latency (~287–325 ms RTT); local engines execute over localhost loopback.
- **Short-Window Mixed Concurrency:** The 30-second mixed windows measure sustained short-window throughput, not long-duration soak behavior.
- **Point-in-Time Memory Observations:** Local footprint values are two point-in-time snapshots, not continuous peak measurements, and disk storage size is not measured.

## Repository contents

```text
data/                 dataset preparation, derived CSVs, and statistics
src/adapters/         database-specific query and ingestion adapters
src/workloads/        ingestion, correctness, latency, and concurrency workloads
src/metrics/          collection and Markdown/JSON reporting
src/visualizer/       chart generation
tests/                mock-pipeline and live connectivity tests
run_benchmark.py      benchmark orchestrator
docker-compose.yml    local comparison services and resource limits
ANALYSIS.md           comprehensive architectural analysis and benchmark conclusions
```

## License

MIT — see [LICENSE](LICENSE).
