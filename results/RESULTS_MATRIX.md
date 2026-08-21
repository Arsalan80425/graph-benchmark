# Graph Database Cloud Benchmark Results Matrix
> Generated automatically by the Wexa AI Benchmarking Suite. Missing evidence is shown explicitly and is never treated as zero.

## 1. Execution Status & Semantic Validation
| Platform                      | Status   | Semantic Validation   | Run Timestamp                    | Error         |
|-------------------------------|----------|-----------------------|----------------------------------|---------------|
| CognoDB Cloud                 | complete | PASS (6/6 checks)     | 2026-08-21T05:30:07.683130+00:00 | None reported |
| Neo4j 5 (Capped)              | complete | PASS (6/6 checks)     | 2026-08-21T06:32:50.734619+00:00 | None reported |
| Memgraph 2.16 (In-Memory C++) | complete | PASS (6/6 checks)     | 2026-08-21T05:46:37.688804+00:00 | None reported |
| FalkorDB 4.2.1 (GraphBLAS)    | complete | PASS (6/6 checks)     | 2026-08-21T05:56:05.645505+00:00 | None reported |
| ArangoDB 3.11                 | complete | PASS (6/6 checks)     | 2026-08-21T05:58:07.144053+00:00 | None reported |

## 2. Evaluated Platforms & Resource Specs
| Platform                      | Type                            | Resource Limits                                             | Storage Engine                                                | Query Interface             |
|-------------------------------|---------------------------------|-------------------------------------------------------------|---------------------------------------------------------------|-----------------------------|
| CognoDB Cloud                 | Managed cloud service           | 0.5 vCPU (burstable), 512MB RAM, 1GB Disk (c0 free tier)    | CognoDB managed graph engine (implementation not disclosed)   | Bolt / Cypher               |
| Neo4j 5 (Capped)              | Resource-capped local container | 0.5 vCPU, 512MB container (192MB Heap, 64MB PageCache)      | Neo4j native storage engine                                   | Bolt / Cypher               |
| Memgraph 2.16 (In-Memory C++) | Resource-capped local container | 0.5 vCPU, 512MB container                                   | Memgraph in-memory transactional storage                      | Bolt / Cypher               |
| FalkorDB 4.2.1 (GraphBLAS)    | Resource-capped local container | 0.5 vCPU, 512MB container (Redis In-Memory Sparse Matrices) | FalkorDB GraphBLAS sparse matrices in Redis-compatible memory | Redis protocol / OpenCypher |
| ArangoDB 3.11                 | Resource-capped local container | 0.5 vCPU, 512MB container (RocksDB Engine)                  | ArangoDB RocksDB                                              | HTTP / AQL                  |

## 3. Data Loading, Throughput & Count Verification
| Platform                      | Status   |   Expected Nodes |   Verified Nodes |   Expected Edges |   Verified Edges | Count Check   |   Wall Time (s) |   Nodes/s |   Rels/s | Load Method                                                                                                                            |
|-------------------------------|----------|------------------|------------------|------------------|------------------|---------------|-----------------|-----------|----------|----------------------------------------------------------------------------------------------------------------------------------------|
| CognoDB Cloud                 | complete |           17,441 |           17,441 |          100,000 |          100,000 | PASS          |          559.44 |    1632   |    182.8 | Bolt auto-commit UNWIND with cloud-safe sub-batching; requested outer batch=1,000; effective node batch<=500; effective edge batch<=50 |
| Neo4j 5 (Capped)              | complete |           17,441 |           17,441 |          100,000 |          100,000 | PASS          |           28.72 |    2718.3 |   4540.8 | Bolt auto-commit UNWIND batches; requested outer batch=1,000; effective node batch<=1,000; effective edge batch<=1,000                 |
| Memgraph 2.16 (In-Memory C++) | complete |           17,441 |           17,441 |          100,000 |          100,000 | PASS          |           10.87 |    8856   |  11647.7 | Bolt auto-commit UNWIND batches; requested outer batch=1,000; effective node batch<=1,000; effective edge batch<=1,000                 |
| FalkorDB 4.2.1 (GraphBLAS)    | complete |           17,441 |           17,441 |          100,000 |          100,000 | PASS          |           10.66 |    9897.3 |  11654   | Redis GRAPH.QUERY parameterized UNWIND batches; requested outer batch=1,000; effective node batch<=1,000; effective edge batch<=1,000  |
| ArangoDB 3.11                 | complete |           17,441 |           17,441 |          100,000 |          100,000 | PASS          |            5.62 |   17635.6 |  23075.7 | HTTP collection.insert_many batches; requested outer batch=1,000; effective node batch<=1,000; effective edge batch<=1,000             |

## 4. Read Latency, Samples & Errors
| Platform                      | Status   | Workload                  |   Attempted |   Successful |   Errors |   p50 (ms) |   p95 (ms) |
|-------------------------------|----------|---------------------------|-------------|--------------|----------|------------|------------|
| CognoDB Cloud                 | complete | Traversal - 1 hop         |         100 |          100 |        0 |     249.3  |     277.85 |
| CognoDB Cloud                 | complete | Traversal - 2 hop         |         100 |          100 |        0 |     244.37 |     306.29 |
| CognoDB Cloud                 | complete | Traversal - 3 hop         |         100 |          100 |        0 |     259.6  |     642.43 |
| CognoDB Cloud                 | complete | Point lookup              |         100 |          100 |        0 |     247.07 |     300.22 |
| CognoDB Cloud                 | complete | Indexed / filtered lookup |         100 |          100 |        0 |     257.53 |     306.55 |
| CognoDB Cloud                 | complete | Aggregation               |         100 |          100 |        0 |     275.4  |     319.55 |
| Neo4j 5 (Capped)              | complete | Traversal - 1 hop         |         100 |          100 |        0 |       2.91 |      81.91 |
| Neo4j 5 (Capped)              | complete | Traversal - 2 hop         |         100 |          100 |        0 |       2.55 |      78.84 |
| Neo4j 5 (Capped)              | complete | Traversal - 3 hop         |         100 |          100 |        0 |       4.43 |      86.34 |
| Neo4j 5 (Capped)              | complete | Point lookup              |         100 |          100 |        0 |       2.25 |      73.72 |
| Neo4j 5 (Capped)              | complete | Indexed / filtered lookup |         100 |          100 |        0 |       7.13 |      76.94 |
| Neo4j 5 (Capped)              | complete | Aggregation               |         100 |          100 |        0 |       9.48 |      83.15 |
| Memgraph 2.16 (In-Memory C++) | complete | Traversal - 1 hop         |         100 |          100 |        0 |       1.16 |       1.58 |
| Memgraph 2.16 (In-Memory C++) | complete | Traversal - 2 hop         |         100 |          100 |        0 |       1.34 |       1.99 |
| Memgraph 2.16 (In-Memory C++) | complete | Traversal - 3 hop         |         100 |          100 |        0 |       2.14 |      27.28 |
| Memgraph 2.16 (In-Memory C++) | complete | Point lookup              |         100 |          100 |        0 |       1.16 |       1.6  |
| Memgraph 2.16 (In-Memory C++) | complete | Indexed / filtered lookup |         100 |          100 |        0 |       2.49 |       3.63 |
| Memgraph 2.16 (In-Memory C++) | complete | Aggregation               |         100 |          100 |        0 |       6.64 |      45.76 |
| FalkorDB 4.2.1 (GraphBLAS)    | complete | Traversal - 1 hop         |         100 |          100 |        0 |       1.05 |       1.19 |
| FalkorDB 4.2.1 (GraphBLAS)    | complete | Traversal - 2 hop         |         100 |          100 |        0 |       1.26 |       2.44 |
| FalkorDB 4.2.1 (GraphBLAS)    | complete | Traversal - 3 hop         |         100 |          100 |        0 |       3.95 |      85.07 |
| FalkorDB 4.2.1 (GraphBLAS)    | complete | Point lookup              |         100 |          100 |        0 |       0.89 |       1.03 |
| FalkorDB 4.2.1 (GraphBLAS)    | complete | Indexed / filtered lookup |         100 |          100 |        0 |       2.08 |       3.3  |
| FalkorDB 4.2.1 (GraphBLAS)    | complete | Aggregation               |         100 |          100 |        0 |       5.18 |      45.65 |
| ArangoDB 3.11                 | complete | Traversal - 1 hop         |         100 |          100 |        0 |      44.02 |      44.54 |
| ArangoDB 3.11                 | complete | Traversal - 2 hop         |         100 |          100 |        0 |      44.05 |      45.36 |
| ArangoDB 3.11                 | complete | Traversal - 3 hop         |         100 |          100 |        0 |      46.81 |      66.45 |
| ArangoDB 3.11                 | complete | Point lookup              |         100 |          100 |        0 |      43.97 |      44.5  |
| ArangoDB 3.11                 | complete | Indexed / filtered lookup |         100 |          100 |        0 |      48.02 |      52.31 |
| ArangoDB 3.11                 | complete | Aggregation               |         100 |          100 |        0 |      55.85 |      59.38 |

## 5. Concurrent Mixed Workload
| Platform                      | Status   |   Clients |   Attempted |   Successful |   Errors |   Reads |   Writes | Realized Mix               |    QPS |   p50 (ms) |   p95 (ms) |
|-------------------------------|----------|-----------|-------------|--------------|----------|---------|----------|----------------------------|--------|------------|------------|
| CognoDB Cloud                 | complete |         1 |         118 |          118 |        0 |      94 |       24 | 79.7% Reads / 20.3% Writes |    3.9 |     249.92 |     305.85 |
| CognoDB Cloud                 | complete |        10 |       1,188 |        1,188 |        0 |     951 |      237 | 80.1% Reads / 19.9% Writes |   39.5 |     245.27 |     303.09 |
| CognoDB Cloud                 | complete |        40 |       4,630 |        4,630 |        0 |   3,705 |      925 | 80.0% Reads / 20.0% Writes |  153.4 |     251.19 |     304.74 |
| Neo4j 5 (Capped)              | complete |         1 |       6,839 |        6,839 |        0 |   5,471 |    1,368 | 80.0% Reads / 20.0% Writes |  227.4 |       1.84 |       5.71 |
| Neo4j 5 (Capped)              | complete |        10 |       9,650 |        9,650 |        0 |   7,719 |    1,931 | 80.0% Reads / 20.0% Writes |  320.8 |       6.64 |      91.03 |
| Neo4j 5 (Capped)              | complete |        40 |      18,912 |       18,912 |        0 |  15,129 |    3,783 | 80.0% Reads / 20.0% Writes |  628.8 |      75.15 |     106.51 |
| Memgraph 2.16 (In-Memory C++) | complete |         1 |      20,408 |       20,408 |        0 |  16,326 |    4,082 | 80.0% Reads / 20.0% Writes |  680.2 |       1.33 |       1.94 |
| Memgraph 2.16 (In-Memory C++) | complete |        10 |      31,473 |       31,473 |        0 |  25,178 |    6,295 | 80.0% Reads / 20.0% Writes | 1048.3 |       8.39 |      15.82 |
| Memgraph 2.16 (In-Memory C++) | complete |        40 |      26,577 |       26,576 |        1 |  21,260 |    5,316 | 80.0% Reads / 20.0% Writes |  884.4 |      39.37 |      71.6  |
| FalkorDB 4.2.1 (GraphBLAS)    | complete |         1 |      25,180 |       25,180 |        0 |  20,144 |    5,036 | 80.0% Reads / 20.0% Writes |  839.2 |       1.15 |       1.53 |
| FalkorDB 4.2.1 (GraphBLAS)    | complete |        10 |      25,775 |       25,775 |        0 |  20,620 |    5,155 | 80.0% Reads / 20.0% Writes |  858.9 |       2.49 |      80.39 |
| FalkorDB 4.2.1 (GraphBLAS)    | complete |        40 |      30,719 |       30,719 |        0 |  24,577 |    6,142 | 80.0% Reads / 20.0% Writes | 1022.8 |      11.73 |      94.38 |
| ArangoDB 3.11                 | complete |         1 |         678 |          678 |        0 |     542 |      136 | 79.9% Reads / 20.1% Writes |   22.6 |      44    |      47.56 |
| ArangoDB 3.11                 | complete |        10 |       6,353 |        6,352 |        1 |   5,083 |    1,269 | 80.0% Reads / 20.0% Writes |  211.5 |      47.15 |      52.89 |
| ArangoDB 3.11                 | complete |        40 |      20,174 |       20,172 |        2 |  16,140 |    4,032 | 80.0% Reads / 20.0% Writes |  670.6 |      59.01 |      82.74 |

## 6. Resource Footprint
| Platform                      | Status   | Observed Footprint / Allocation                                                                                              | Notes                                                                                                                                                                                |
|-------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CognoDB Cloud                 | complete | Not observable via managed cloud interface (Console allocation: 0.5 burstable vCPU, 512MB RAM, 1GiB Storage, up to 500 IOPS) | Two point-in-time observation attempts (after ingestion and after the workload); unavailable values are labelled not observed. This is not a peak-memory or stored-size measurement. |
| Neo4j 5 (Capped)              | complete | Observed Container: 509.6MiB / 512MiB (JVM Heap 192MB, PageCache 64MB)                                                       | Two point-in-time observation attempts (after ingestion and after the workload); unavailable values are labelled not observed. This is not a peak-memory or stored-size measurement. |
| Memgraph 2.16 (In-Memory C++) | complete | Observed Container: 97.67MiB / 512MiB (In-Memory C++ Engine)                                                                 | Two point-in-time observation attempts (after ingestion and after the workload); unavailable values are labelled not observed. This is not a peak-memory or stored-size measurement. |
| FalkorDB 4.2.1 (GraphBLAS)    | complete | Observed Redis: N/A (GraphBLAS RAM), Container: 170.3MiB / 512MiB                                                            | Two point-in-time observation attempts (after ingestion and after the workload); unavailable values are labelled not observed. This is not a peak-memory or stored-size measurement. |
| ArangoDB 3.11                 | complete | Observed Container: 350.5MiB / 512MiB (RocksDB Engine)                                                                       | Two point-in-time observation attempts (after ingestion and after the workload); unavailable values are labelled not observed. This is not a peak-memory or stored-size measurement. |

## 7. Run Metadata & Provenance Audit
| Audit Field               | Recorded Value                                                   |
|---------------------------|------------------------------------------------------------------|
| Git Commit                | 61f168d28ea334fb8481cd657de368c82df02ed4                         |
| Client Region             | ap-south-1                                                       |
| CognoDB Cloud Region      | us-east4                                                         |
| Dataset Nodes CSV SHA-256 | 1e67bd2694789942cb999936d3505d661c221849373b50e88391b8244142d3d2 |
| Dataset Edges CSV SHA-256 | 90f62adec5e5e7cc2b2671defdca4e9675cf1d9add3d3f5e885e915abf851577 |
| Benchmark Harness SHA-256 | 1b039f0d153c4deb38245f404a95e7a73b5dc9107a0f04031d5df5ed669399ef |
| Run Started At            | 2026-08-21T06:32:50.308917+00:00                                 |
