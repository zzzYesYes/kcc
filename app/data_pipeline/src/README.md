# Python Source Layout

The source tree is divided by responsibility rather than deployment history:

| Namespace | Status | Responsibility |
| --- | --- | --- |
| `data_lake` | current | Acquisition, S3 checks, and manifest utilities |
| `clean_qa` | current | Stage 1/2 production code and Dagster definitions |
| `runtime` | current support | MinerU execution adapters used by the production pipeline |
| `legacy` | compatibility only | Superseded implementations kept for controlled migration |
| `dagster_qwen_ops` | current independent | Portable Dagster-managed Qwen vLLM lifecycle and chat demo |

New application code must import the fully qualified namespace. Keep new code
inside one of these five ownership roots unless a new independent subsystem is
explicitly approved and documented.
