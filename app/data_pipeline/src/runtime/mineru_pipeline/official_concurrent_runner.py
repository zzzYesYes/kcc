"""Cross-document MinerU test runner that preserves MinerU's official PDF path."""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import boto3
import zstandard
from boto3.s3.transfer import TransferConfig
from botocore.config import Config


INPUT_BUCKET = os.environ.get("MINERU_INPUT_BUCKET", "k12-textbook-raw")
OUTPUT_BUCKET = os.environ.get("MINERU_OUTPUT_BUCKET", "k12-mineru-output")
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
ALLOWLIST_SUFFIXES = {".md", ".json", ".txt"}


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_documents(manifest_key: str, document_ids: list[str]) -> list[dict]:
    manifest = json.loads(
        s3_client().get_object(Bucket=OUTPUT_BUCKET, Key=manifest_key)["Body"].read()
    )
    by_id = {row["document_id"]: row for row in manifest["documents"]}
    missing = [document_id for document_id in document_ids if document_id not in by_id]
    if missing:
        raise ValueError(f"document ids not in manifest: {missing}")
    return [by_id[document_id] for document_id in document_ids]


def run_mineru(
    source: Path,
    output_dir: Path,
    server_url: str,
    analyzer: str,
    window_prefetch: int,
):
    env = os.environ.copy()
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.pop(key, None)
    no_proxy = os.environ.get(
        "PIPELINE_NO_PROXY", "127.0.0.1,localhost,.svc,.svc.cluster.local"
    )
    env.update({"NO_PROXY": no_proxy, "no_proxy": no_proxy})
    if analyzer == "official":
        command = [
            "mineru",
            "--path", str(source),
            "--output", str(output_dir),
            "--backend", "vlm-http-client",
            "--url", server_url,
            "--method", "auto",
            "--image-analysis", "true",
            "--client-side-output-generation", "false",
        ]
    else:
        command = [
            sys.executable,
            "-m",
            "runtime.mineru_pipeline.official_window_runner",
            "--path", str(source),
            "--output", str(output_dir),
            "--url", server_url,
            "--profile-json", str(output_dir / "window_pipeline_profile.json"),
            "--window-prefetch", str(window_prefetch),
        ]
    return subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        timeout=7200,
        check=False,
    )


def archive_images(output_dir: Path, package_dir: Path) -> tuple[Path | None, int, int]:
    image_paths = [
        path for path in sorted(output_dir.rglob("*"))
        if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES
    ]
    if not image_paths:
        return None, 0, 0
    archive = package_dir / "images.tar.zst"
    with archive.open("wb") as raw_archive:
        with zstandard.ZstdCompressor(level=3, threads=-1).stream_writer(
            raw_archive, closefd=False
        ) as compressed_archive:
            with tarfile.open(fileobj=compressed_archive, mode="w|") as tar_archive:
                for path in image_paths:
                    tar_archive.add(
                        path,
                        arcname=path.relative_to(output_dir).as_posix(),
                        recursive=False,
                    )
    return archive, len(image_paths), sum(path.stat().st_size for path in image_paths)


def prepare_artifacts(output_dir: Path, package_dir: Path) -> tuple[list[tuple[Path, str]], dict]:
    selected: list[tuple[Path, str]] = []
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file() or path.suffix.lower() in IMAGE_SUFFIXES:
            continue
        if path.suffix.lower() in ALLOWLIST_SUFFIXES:
            selected.append((path, path.relative_to(output_dir).as_posix()))
    archive, image_count, image_bytes = archive_images(output_dir, package_dir)
    if archive:
        selected.append((archive, "images.tar.zst"))
    return selected, {
        "image_count": image_count,
        "image_bytes": image_bytes,
        "image_archive_bytes": archive.stat().st_size if archive else 0,
        "artifact_count": len(selected),
    }


def upload_one(path: Path, bucket: str, key: str, chunk_size: int, max_concurrency: int) -> dict:
    transfer = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=chunk_size,
        max_concurrency=max_concurrency,
        use_threads=True,
    )
    started = time.time()
    s3_client().upload_file(str(path), bucket, key, Config=transfer)
    finished = time.time()
    head_started = time.time()
    head = s3_client().head_object(Bucket=bucket, Key=key)
    head_finished = time.time()
    size = path.stat().st_size
    return {
        "key": key,
        "bytes": size,
        "upload_seconds": round(finished - started, 3),
        "upload_mib_s": round(size / 1024 / 1024 / (finished - started), 3) if finished > started else None,
        "multipart": size >= 8 * 1024 * 1024,
        "part_count": math.ceil(size / chunk_size) if size >= 8 * 1024 * 1024 else 1,
        "head_seconds": round(head_finished - head_started, 3),
        "head_content_length": head.get("ContentLength"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-s3-key", required=True)
    parser.add_argument("--document-ids", required=True, help="Comma-separated manifest ids")
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--server-url", default="http://127.0.0.1:30001")
    parser.add_argument("--job-workers", type=int, default=2)
    parser.add_argument("--parse-concurrency", type=int, default=2)
    parser.add_argument("--upload-workers", type=int, default=4)
    parser.add_argument("--multipart-chunksize-mib", type=int, default=16)
    parser.add_argument("--analyzer", choices=("official", "window-pipeline"), default="official")
    parser.add_argument("--window-prefetch", type=int, default=1)
    args = parser.parse_args()

    document_ids = [value.strip() for value in args.document_ids.split(",") if value.strip()]
    documents = load_documents(args.manifest_s3_key, document_ids)
    parse_slots = threading.Semaphore(args.parse_concurrency)
    chunk_size = args.multipart_chunksize_mib * 1024 * 1024
    run_started = time.time()

    def process_one(index: int, document: dict) -> dict:
        document_id = document["document_id"]
        started = time.time()
        root = Path(tempfile.mkdtemp(prefix=f"mineru-official-{document_id}-"))
        source = root / f"{document_id}.pdf"
        output_dir = root / "output"
        package_dir = root / "package"
        package_dir.mkdir()
        item_prefix = f"{args.output_prefix.rstrip('/')}/{document_id}"
        result = {
            "document_id": document_id,
            "input": {
                "bucket": INPUT_BUCKET,
                "key": document["object_key"],
                "etag": document.get("etag"),
                "manifest_page_count": document.get("page_count"),
            },
            "output_prefix": item_prefix,
            "started_at": started,
            "started_at_utc": utc_now(),
            "timings": {},
            "status": "failed",
        }
        try:
            download_started = time.time()
            s3_client().download_file(INPUT_BUCKET, document["object_key"], str(source))
            download_finished = time.time()
            result["timings"]["download"] = {
                "start": download_started,
                "end": download_finished,
                "seconds": round(download_finished - download_started, 3),
                "bytes": source.stat().st_size,
            }

            wait_started = time.time()
            parse_slots.acquire()
            parse_started = time.time()
            result["timings"]["parse_wait"] = {
                "start": wait_started,
                "end": parse_started,
                "seconds": round(parse_started - wait_started, 3),
            }
            try:
                completed = run_mineru(
                    source,
                    output_dir,
                    args.server_url,
                    args.analyzer,
                    args.window_prefetch,
                )
            finally:
                parse_slots.release()
            parse_finished = time.time()
            result["timings"]["official_mineru"] = {
                "start": parse_started,
                "end": parse_finished,
                "seconds": round(parse_finished - parse_started, 3),
                "returncode": completed.returncode,
            }
            s3_client().put_object(
                Bucket=OUTPUT_BUCKET,
                Key=f"{item_prefix}/mineru.log",
                Body=completed.stdout.encode(),
                ContentType="text/plain; charset=utf-8",
            )
            if completed.returncode:
                raise RuntimeError(f"official MinerU exited with {completed.returncode}")

            package_started = time.time()
            artifacts, artifact_profile = prepare_artifacts(output_dir, package_dir)
            package_finished = time.time()
            result["timings"]["package"] = {
                "start": package_started,
                "end": package_finished,
                "seconds": round(package_finished - package_started, 3),
            }
            result["artifact_profile"] = artifact_profile

            upload_started = time.time()
            uploaded = []
            with ThreadPoolExecutor(max_workers=args.upload_workers) as pool:
                futures = [
                    pool.submit(
                        upload_one,
                        path,
                        OUTPUT_BUCKET,
                        f"{item_prefix}/artifacts/{relative}",
                        chunk_size,
                        args.upload_workers,
                    )
                    for path, relative in artifacts
                ]
                for future in as_completed(futures):
                    uploaded.append(future.result())
            upload_finished = time.time()
            result["timings"]["upload"] = {
                "start": upload_started,
                "end": upload_finished,
                "seconds": round(upload_finished - upload_started, 3),
            }
            result["upload_profile"] = {
                "endpoint": os.environ.get("S3_ENDPOINT_URL"),
                "proxy_env": {key: os.environ.get(key) for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY")},
                "files": sorted(uploaded, key=lambda row: row["key"]),
                "uploaded_bytes": sum(row["bytes"] for row in uploaded),
            }
            result["status"] = "success"
        except Exception as exc:
            result["error"] = repr(exc)
        finally:
            result["finished_at"] = time.time()
            result["finished_at_utc"] = utc_now()
            result["elapsed_seconds"] = round(result["finished_at"] - started, 3)
            s3_client().put_object(
                Bucket=OUTPUT_BUCKET,
                Key=f"{item_prefix}/_RESULT.json",
                Body=json.dumps(result, ensure_ascii=False, indent=2).encode(),
                ContentType="application/json",
            )
            shutil.rmtree(root, ignore_errors=True)
        return result

    results = []
    with ThreadPoolExecutor(max_workers=args.job_workers) as pool:
        futures = [pool.submit(process_one, index, document) for index, document in enumerate(documents, 1)]
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda row: row["document_id"])
    summary = {
        "status": "success" if all(row["status"] == "success" for row in results) else "partial",
        "started_at": run_started,
        "finished_at": time.time(),
        "elapsed_seconds": round(time.time() - run_started, 3),
        "config": {
            "mode": (
                "runtime.mineru_pipeline.official_window_pipeline"
                if args.analyzer == "window-pipeline"
                else "official_mineru_pdf_internal_concurrency"
            ),
            "analyzer": args.analyzer,
            "window_prefetch": args.window_prefetch,
            "job_workers": args.job_workers,
            "parse_concurrency": args.parse_concurrency,
            "upload_workers": args.upload_workers,
            "multipart_chunksize_mib": args.multipart_chunksize_mib,
            "server_url": args.server_url,
            "client_side_output_generation": False,
            "upload_original_pdf": False,
        },
        "results": results,
    }
    s3_client().put_object(
        Bucket=OUTPUT_BUCKET,
        Key=f"{args.output_prefix.rstrip('/')}/_SUMMARY.json",
        Body=json.dumps(summary, ensure_ascii=False, indent=2).encode(),
        ContentType="application/json",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
