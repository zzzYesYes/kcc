from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

from .minio_client import ObjectStore


def document_artifacts(
    store: ObjectStore,
    bucket: str,
    source_prefix: str,
    document_id: str,
) -> dict[str, str]:
    prefix = f"{source_prefix.rstrip('/')}/{document_id}/"
    keys = store.list_keys(bucket, prefix)

    def one(suffix: str, required: bool = True) -> str | None:
        matches = [key for key in keys if key.endswith(suffix)]
        if len(matches) == 1:
            return matches[0]
        if required:
            raise ValueError(
                f"{document_id}: expected one {suffix}, found {len(matches)}"
            )
        return None

    return {
        "markdown_key": one(".md"),
        "content_list_key": one("_content_list.json"),
        "content_list_v2_key": one("_content_list_v2.json", False),
        "middle_key": one("_middle.json"),
        "model_key": one("_model.json", False),
        "images_archive_key": one("images.tar.zst", False),
        "mineru_success_key": one("_SUCCESS.json"),
    }


def resolve_manifest(
    store: ObjectStore,
    bucket: str,
    source_prefix: str,
    requested: list[dict[str, Any]] | None,
    limit: int,
) -> list[dict[str, Any]]:
    if requested:
        selected = requested
    else:
        prefix = source_prefix.rstrip("/") + "/"
        ids = sorted(
            {
                PurePosixPath(item["Key"][len(prefix) :]).parts[0]
                for item in store.iter_objects(bucket, prefix)
                if item["Key"].endswith("/_SUCCESS.json")
            }
        )
        selected = [{"document_id": document_id, "category": "production"} for document_id in ids]
    if limit > 0:
        selected = selected[:limit]
    documents = []
    for selected_row in selected:
        document_id = selected_row["document_id"]
        artifacts = document_artifacts(store, bucket, source_prefix, document_id)
        success = store.read_json(bucket, artifacts["mineru_success_key"])
        head = store.head(bucket, artifacts["markdown_key"])
        documents.append(
            {
                **selected_row,
                **artifacts,
                "source_bucket": bucket,
                "source_prefix": source_prefix.rstrip("/"),
                "source_size": int(head["ContentLength"]),
                "source_etag": head["ETag"].strip('"'),
                "source_input": success.get("input", {}),
                "page_count": success.get("page_count"),
                "image_count": success.get("image_count", 0),
            }
        )
    return documents

