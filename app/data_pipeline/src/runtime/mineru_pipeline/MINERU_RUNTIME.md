# MinerU Runtime Modules

The root-level MinerU analyzer and runner files are runtime implementations
derived from the validated MinerU window/concurrency work. They are not Stage
1 cleaning code.

They will be grouped under `runtime.mineru_pipeline` together with the dual-NPU
and MinerU 3.4 Hybrid S3 execution modules. Data semantics remain owned by the
Cleaning/QA pipeline; these modules only perform PDF parsing and runtime
scheduling.
