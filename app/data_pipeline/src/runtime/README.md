# Runtime Adapters

These modules support the current production pipeline but are not alternate
Stage 1 implementations:

* `mineru_pipeline`: official-window, concurrency, profiling, and production
  MinerU runner entry points;
* `mineru34_hybrid_lake`: MinerU 3.4 Hybrid data-lake execution adapters;
* `dual_npu`: two-device discovery, service lifecycle, and Ray coordination.

Runtime commands should use package entry points such as
`python -m runtime.mineru_pipeline.official_window_runner`. Temporary paths
under `/tmp` are runtime storage, not source-code locations.

