"""Launchpad defaults for the user-facing Dagster jobs."""

import os

SOURCE_PREFIX = os.environ.get(
    "K12_SOURCE_PREFIX",
    "source=tchMaterial-parser/batch_id=k12_pdf_full_20260713T062427Z",
)

REGISTER_BATCH_002_CONFIG = {
    "ops": {
        "validate_registration_config": {
            "config": {
                "batch_id": "batch-002",
                "input_bucket": "k12-textbook-raw",
                "input_prefix": SOURCE_PREFIX,
                "output_bucket": "k12-mineru-output",
                "output_prefix": "production/mineru/batch-002",
                "ray_job_id": "mineru-s3-all-20260717T072209Z",
                "sample_size": 10,
            }
        }
    }
}

MINERU_SMOKE_CONFIG = {
    "ops": {
        "validate_run_config": {
            "config": {
                "batch_id": "dagster-ui-smoke-001",
                "mode": "count",
                "count": 10,
                "input_bucket": "k12-textbook-raw",
                "input_prefix": SOURCE_PREFIX,
                "output_bucket": "k12-mineru-output",
                "output_prefix": "production/mineru/dagster-ui-smoke-001",
                "mineru_service_count": 2,
                "inference_slots": 4,
                "document_inflight_per_service": 5,
                "window_prefetch": 1,
                "download_workers": 2,
                "upload_workers": 4,
                "block_prepare_workers": 12,
                "render_workers": 6,
                "finalize_workers": 3,
                "archive_workers": 2,
                "multipart_chunksize_mib": 16,
                "multipart_max_concurrency": 4,
                "sample_size": 5,
                "document_ui_detail_limit": 30,
            }
        }
    }
}

# The production submission entry intentionally defaults to a small count run.
# Switching to mode=all must be an explicit Launchpad edit.
MINERU_SUBMIT_CONFIG = {
    "ops": {
        "validate_run_config": {
            "config": {
                "batch_id": "dagster-ui-submit-001",
                "mode": "count",
                "count": 10,
                "input_bucket": "k12-textbook-raw",
                "input_prefix": SOURCE_PREFIX,
                "output_bucket": "k12-mineru-output",
                "output_prefix": "production/mineru/dagster-ui-submit-001",
                "mineru_service_count": 2,
                "inference_slots": 4,
                "document_inflight_per_service": 5,
                "window_prefetch": 1,
                "download_workers": 2,
                "upload_workers": 4,
                "block_prepare_workers": 12,
                "render_workers": 6,
                "finalize_workers": 3,
                "archive_workers": 2,
                "multipart_chunksize_mib": 16,
                "multipart_max_concurrency": 4,
                "sample_size": 5,
                "document_ui_detail_limit": 0,
            }
        }
    }
}

MINERU_FINALIZE_CONFIG = {
    "ops": {
        "load_submission_state": {
            "config": {
                "batch_id": "dagster-smoke-001",
                "output_bucket": "k12-mineru-output",
            }
        }
    }
}

QWEN_VLLM_CONFIG = {
    "ops": {
        "configure_qwen_vllm": {
            "config": {
                "action": "status",
                "max_model_len": 32768,
                "max_num_seqs": 8,
                "max_num_batched_tokens": 4096,
                "gpu_memory_utilization": 0.85,
                "startup_timeout_seconds": 1800,
            }
        }
    }
}

QWEN_VLLM_8NPU_CONFIG = {
    "ops": {
        "configure_qwen_vllm_8npu": {
            "config": {
                "action": "status",
                "max_model_len": 32768,
                "max_num_seqs": 8,
                "max_num_batched_tokens": 4096,
                "gpu_memory_utilization": 0.85,
                "startup_timeout_seconds": 1800,
            }
        }
    }
}

QWEN_CHAT_CONFIG = {
    "ops": {
        "submit_qwen_chat": {
            "config": {
                "prompt": "请用一句中文介绍你自己。",
                "system_prompt": "You are a precise document analysis assistant.",
                "history_json": "[]",
                "temperature": 0.3,
                "max_tokens": 512,
                "enable_thinking": False,
                "timeout_seconds": 600,
            }
        }
    }
}

CLEANING_SMOKE_CONFIG = {
    "ops": {
        "scan_cleaning_manifest": {
            "config": {
                "batch_id": "cleaning-smoke-10-001",
                "count": 10,
                "parsed_bucket": "k12-mineru-output",
                "parsed_prefix": "production/mineru/batch-001",
                "output_bucket": "k12-cleaned-corpus",
                "output_prefix": "cleaning-smoke-10-001",
                "markdown_glob": "**/*.md",
                "document_ui_detail_limit": 10,
            }
        },
        "step_job1": {
            "config": {
                "parallelism": 10,
                "cpus_per_task": 2.0,
                "max_retries": 1,
                "task_timeout_seconds": 900.0,
                "remove_image_markdown": True,
                "remove_natural_image_details": True,
                "text_image_policy": "classify",
                "text_image_min_chars": 4,
                "repeated_line_min_occurrences": 3,
                "repeated_line_max_chars": 80,
                "watermark_regex": r"(?:仅供|内部|试读|样书|水印|www\.)",
                "remove_romanized_cover": True,
                "cover_scan_lines": 120,
                "isolated_text_max_chars": 1,
            }
        },
        "step_job2": {
            "config": {
                "parallelism": 10,
                "cpus_per_task": 2.0,
                "max_retries": 1,
                "task_timeout_seconds": 900.0,
                "preserve_latex": True,
                "convert_html_tables": True,
                "unicode_form": "NFKC",
                "max_consecutive_blank_lines": 1,
                "ocr_max_repeated_char_run": 8,
                "drop_severe_ocr_lines": False,
                "ocr_warn_line_ratio": 0.02,
            }
        },
        "step_job3": {
            "config": {
                "parallelism": 10,
                "cpus_per_task": 2.0,
                "max_retries": 1,
                "task_timeout_seconds": 900.0,
                "chapter_heading_max_level": 3,
                "sample_min_chars": 200,
                "sample_max_chars": 8000,
                "sample_overlap_chars": 0,
                "include_source_uri": True,
            }
        },
    }
}

_CLEANING_FULL_STAGE1 = {
    **CLEANING_SMOKE_CONFIG["ops"]["step_job1"]["config"],
    "parallelism": 32,
    "cpus_per_task": 1.0,
    "task_timeout_seconds": 1800.0,
}
_CLEANING_FULL_STAGE2 = {
    **CLEANING_SMOKE_CONFIG["ops"]["step_job2"]["config"],
    "parallelism": 32,
    "cpus_per_task": 1.0,
    "task_timeout_seconds": 1800.0,
}
_CLEANING_FULL_STAGE3 = {
    **CLEANING_SMOKE_CONFIG["ops"]["step_job3"]["config"],
    "parallelism": 32,
    "cpus_per_task": 1.0,
    "task_timeout_seconds": 1800.0,
}

CLEANING_FULL_CONFIG = {
    "ops": {
        "scan_cleaning_manifest": {
            "config": {
                "batch_id": "cleaning-full-batch-002-001",
                "count": 0,
                "parsed_bucket": "k12-mineru-output",
                "parsed_prefix": "production/mineru/batch-002",
                "output_bucket": "k12-cleaned-corpus",
                "output_prefix": "cleaning-full-batch-002-001",
                "markdown_glob": "**/*.md",
                "document_ui_detail_limit": 30,
            }
        },
        "step_job1": {"config": _CLEANING_FULL_STAGE1},
        "step_job2": {"config": _CLEANING_FULL_STAGE2},
        "step_job3": {"config": _CLEANING_FULL_STAGE3},
    }
}
