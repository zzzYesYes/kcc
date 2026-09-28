#!/usr/bin/env python3
import argparse
import hashlib
import hmac
import json
import mimetypes
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import requests


VERSION_URL = "https://s-file-1.ykt.cbern.com.cn/zxx/ndrs/resources/tch_material/version/data_version.json"
TCH_DETAIL_URL = "https://s-file-1.ykt.cbern.com.cn/zxx/ndrv2/resources/tch_material/details/{content_id}.json"
SPECIAL_DETAIL_URL = "https://s-file-1.ykt.cbern.com.cn/zxx/ndrs/special_edu/resources/details/{content_id}.json"
THEMATIC_LIST_URL = "https://s-file-1.ykt.cbern.com.cn/zxx/ndrs/special_edu/thematic_course/{content_id}/resources/list.json"
TOOL_REPO = "https://github.com/happycola233/tchMaterial-parser"
DEFAULT_PREFIX = "source=tchMaterial-parser"

DOWNLOAD_HEADERS = {
    "Authorization": "Bearer 0",
    "X-ND-AUTH": 'MAC id="0",nonce="0",mac="0"',
}
TOKEN_CONFIG_PATH = Path.home() / ".config" / "tchMaterial-parser" / "data.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def batch_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("k12_pdf_full_%Y%m%dT%H%M%SZ")


def safe_filename(value: str, fallback: str) -> str:
    value = value or fallback
    value = re.sub(r"[\\/:*?\"<>|\r\n\t]+", "_", value)
    value = re.sub(r"\s+", " ", value).strip(" .")
    return (value[:180] or fallback) + ".pdf"


def source_url(item: dict) -> str | None:
    url = item.get("ti_storage")
    if url:
        return url.replace("cs_path:${ref-path}", "https://r1-ndr-private.ykt.cbern.com.cn")
    return next((u for u in item.get("ti_storages") or [] if u), None)


def load_access_token() -> str:
    """Read a locally configured platform token without logging its value."""
    token = (os.environ.get("K12_ACCESS_TOKEN") or "").strip()
    if token:
        return token
    try:
        data = json.loads(TOKEN_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return ""
    return str(data.get("access_token") or "").strip()


def download_headers(access_token: str) -> dict[str, str]:
    if not access_token:
        return DOWNLOAD_HEADERS
    return {
        "Authorization": f"Bearer {access_token}",
        "X-ND-AUTH": f'MAC id="{access_token}",nonce="0",mac="0"',
    }


def jsonl_append(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def quote_path(path: str) -> str:
    return "/" + "/".join(urllib.parse.quote(part, safe="-_.~") for part in path.strip("/").split("/"))


class S3Client:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, region: str = "us-east-1"):
        self.endpoint = endpoint.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        parsed = urllib.parse.urlparse(self.endpoint)
        self.host = parsed.netloc
        self.http = requests.Session()
        self.http.trust_env = False

    def _signed_headers(self, method: str, bucket: str, key: str, payload_hash: str, content_type: str | None = None, content_length: int | None = None, query: dict | None = None) -> tuple[str, dict]:
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        object_path = f"{bucket}/{key}".rstrip("/")
        canonical_uri = quote_path(object_path)
        query = query or {}
        canonical_query = "&".join(
            f"{urllib.parse.quote(str(k), safe='-_.~')}={urllib.parse.quote(str(v), safe='-_.~')}"
            for k, v in sorted(query.items())
        )
        headers = {
            "host": self.host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if content_type:
            headers["content-type"] = content_type
        if content_length is not None:
            headers["content-length"] = str(content_length)

        signed_headers = ";".join(sorted(headers))
        canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
        canonical_request = "\n".join([
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ])
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = "\n".join([
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ])
        signing_key = sign(sign(sign(sign(("AWS4" + self.secret_key).encode("utf-8"), date_stamp), self.region), "s3"), "aws4_request")
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        headers["Authorization"] = (
            f"{algorithm} Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        url = self.endpoint + canonical_uri + (f"?{canonical_query}" if canonical_query else "")
        return url, headers

    def put_file(self, bucket: str, key: str, path: Path, sha256_hex: str) -> None:
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        size = path.stat().st_size
        url, headers = self._signed_headers("PUT", bucket, key, sha256_hex, content_type=content_type, content_length=size)
        with path.open("rb") as f:
            resp = self.http.put(url, data=f, headers=headers, timeout=300)
        if not resp.ok:
            raise RuntimeError(f"S3 PUT failed {resp.status_code}: {resp.text[:500]}")

    def put_bytes(self, bucket: str, key: str, body: bytes, content_type: str = "application/json") -> None:
        digest = hashlib.sha256(body).hexdigest()
        url, headers = self._signed_headers("PUT", bucket, key, digest, content_type=content_type, content_length=len(body))
        resp = self.http.put(url, data=body, headers=headers, timeout=120)
        if not resp.ok:
            raise RuntimeError(f"S3 PUT failed {resp.status_code}: {resp.text[:500]}")

    def head(self, bucket: str, key: str) -> int:
        digest = hashlib.sha256(b"").hexdigest()
        url, headers = self._signed_headers("HEAD", bucket, key, digest)
        resp = self.http.head(url, headers=headers, timeout=120)
        if not resp.ok:
            raise RuntimeError(f"S3 HEAD failed {resp.status_code}: {resp.text[:500]}")
        return int(resp.headers.get("Content-Length") or 0)


def make_session() -> requests.Session:
    session = requests.Session()
    session.proxies = {}
    session.trust_env = False
    return session


def iter_resources(session: requests.Session, scope: str):
    version = session.get(VERSION_URL, timeout=30)
    version.raise_for_status()
    version_data = version.json()
    for part_url in version_data["urls"].split(","):
        part = session.get(part_url, timeout=60)
        part.raise_for_status()
        for resource in part.json():
            if scope == "tagged" and not resource.get("tag_paths"):
                continue
            yield version_data, part_url, resource


def find_source_pdf(resource_type: str, detail: dict, session: requests.Session, content_id: str) -> dict | None:
    for item in detail.get("ti_items") or []:
        if item.get("ti_is_source_file") and (item.get("ti_format") == "pdf" or item.get("ti_file_flag") == "source"):
            url = source_url(item)
            if url:
                return {**item, "download_url": url}

    if resource_type == "thematic_course":
        resp = session.get(THEMATIC_LIST_URL.format(content_id=content_id), timeout=30)
        if resp.ok:
            for resource in resp.json():
                if resource.get("resource_type_code") != "assets_document":
                    continue
                for item in resource.get("ti_items") or []:
                    if item.get("ti_is_source_file") and (item.get("ti_format") == "pdf" or item.get("ti_file_flag") == "source"):
                        url = source_url(item)
                        if url:
                            return {**item, "download_url": url}
    return None


def resolve_resource(session: requests.Session, version_data: dict, part_url: str, resource: dict) -> dict:
    content_id = resource.get("content_id") or resource.get("id")
    if not content_id:
        raise ValueError("resource has no content_id/id")
    resource_type = resource.get("resource_type_code") or "assets_document"
    detail_url = SPECIAL_DETAIL_URL.format(content_id=content_id) if resource_type == "thematic_course" else TCH_DETAIL_URL.format(content_id=content_id)
    resp = session.get(detail_url, timeout=30)
    resp.raise_for_status()
    detail = resp.json()
    pdf_item = find_source_pdf(resource_type, detail, session, content_id)
    if not pdf_item:
        raise ValueError("no source PDF item found")

    title = detail.get("title") or resource.get("title") or resource.get("name") or content_id
    filename = safe_filename(title, content_id)
    return {
        "module_version": version_data.get("module_version"),
        "source_part_url": part_url,
        "resource_type_code": resource_type,
        "content_id": content_id,
        "title": title,
        "filename": filename,
        "tag_paths": resource.get("tag_paths") or [],
        "detail_url": detail_url,
        "preview_url": f"https://basic.smartedu.cn/tchMaterial/detail?contentType={resource_type}&contentId={content_id}&catalogType=tchMaterial&subCatalog=tchMaterial",
        "download_url": pdf_item["download_url"],
        "expected_size_bytes": int(pdf_item.get("ti_size") or 0),
        "ti_file_flag": pdf_item.get("ti_file_flag"),
        "ti_format": pdf_item.get("ti_format"),
    }


def build_plan(args, batch_dir: Path, plan_path: Path, failures_path: Path) -> tuple[int, int, int]:
    if plan_path.exists() and not args.rebuild_plan:
        resolved = list(read_jsonl(plan_path))
        return len(resolved), sum(int(r.get("expected_size_bytes") or 0) for r in resolved), 0

    if plan_path.exists():
        plan_path.unlink()
    if failures_path.exists():
        failures_path.unlink()

    session = make_session()
    resolved_count = 0
    failed_count = 0
    total_size = 0
    for idx, (version_data, part_url, resource) in enumerate(iter_resources(session, args.scope), start=1):
        if args.limit and resolved_count >= args.limit:
            break
        content_id = resource.get("content_id") or resource.get("id") or f"unknown-{idx}"
        try:
            record = resolve_resource(session, version_data, part_url, resource)
            jsonl_append(plan_path, record)
            resolved_count += 1
            total_size += int(record.get("expected_size_bytes") or 0)
            print(f"PLAN resolved={resolved_count} failed={failed_count} size={total_size} content_id={record['content_id']} title={record['title']}", flush=True)
        except Exception as exc:
            failed_count += 1
            jsonl_append(failures_path, {
                "stage": "resolve",
                "content_id": content_id,
                "resource_type_code": resource.get("resource_type_code"),
                "title": resource.get("title") or resource.get("name"),
                "error": repr(exc),
                "failed_at": utc_now(),
            })
            print(f"PLAN_FAILED failed={failed_count} content_id={content_id} error={exc!r}", flush=True)
        time.sleep(args.resolve_sleep)
    return resolved_count, total_size, failed_count


def load_uploaded(status_path: Path) -> dict[str, dict]:
    uploaded = {}
    for record in read_jsonl(status_path) or []:
        if record.get("status") == "uploaded":
            uploaded[record.get("content_id")] = record
    return uploaded


def download_pdf(session: requests.Session, record: dict, dest: Path, headers: dict[str, str]) -> tuple[int, str]:
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    hasher = hashlib.sha256()
    size = 0
    with session.get(record["download_url"], headers=headers, stream=True, timeout=180) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 512):
                if not chunk:
                    continue
                f.write(chunk)
                hasher.update(chunk)
                size += len(chunk)

    with tmp.open("rb") as f:
        if f.read(5) != b"%PDF-":
            raise RuntimeError("downloaded file is not a PDF")
    expected = int(record.get("expected_size_bytes") or 0)
    if expected and size != expected:
        raise RuntimeError(f"size mismatch: expected {expected}, got {size}")
    tmp.replace(dest)
    return size, hasher.hexdigest()


def is_permanent_http_error(exc: Exception) -> bool:
    if not isinstance(exc, requests.HTTPError) or exc.response is None:
        return False
    return exc.response.status_code in {400, 401, 403, 404, 410}


def retry(label: str, attempts: int, base_sleep: float, func: Callable):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            if is_permanent_http_error(exc):
                print(f"NO_RETRY {label} permanent_http_status={exc.response.status_code} error={exc!r}", flush=True)
                break
            if attempt >= attempts:
                break
            sleep_for = base_sleep * (2 ** (attempt - 1))
            print(f"RETRY {label} attempt={attempt}/{attempts} sleep={sleep_for:.1f}s error={exc!r}", flush=True)
            time.sleep(sleep_for)
    raise last_exc


def upload_meta_files(client: S3Client, args, batch_id: str, meta_dir: Path) -> None:
    for path in sorted(meta_dir.glob("*")):
        if not path.is_file():
            continue
        key = f"{args.prefix}/batch_id={batch_id}/{path.name}"
        ctype = "application/json" if path.suffix == ".json" else "text/plain"
        client.put_bytes(args.meta_bucket, key, path.read_bytes(), content_type=ctype)


def failure_record(args, record: dict, raw_key: str, exc: Exception, stage: str) -> dict:
    http_status = None
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        http_status = exc.response.status_code
    return {
        "batch_id": args.batch_id,
        "stage": stage,
        "source_platform": "国家中小学智慧教育平台",
        "capture_tool": "tchMaterial-parser-compatible-full-ingest",
        "tool_repo": TOOL_REPO,
        "content_id": record.get("content_id"),
        "title": record.get("title"),
        "filename": record.get("filename"),
        "resource_type_code": record.get("resource_type_code"),
        "tag_paths": record.get("tag_paths") or [],
        "module_version": record.get("module_version"),
        "source_part_url": record.get("source_part_url"),
        "detail_url": record.get("detail_url"),
        "preview_url": record.get("preview_url"),
        "download_url": record.get("download_url"),
        "expected_size_bytes": int(record.get("expected_size_bytes") or 0),
        "ti_file_flag": record.get("ti_file_flag"),
        "ti_format": record.get("ti_format"),
        "raw_bucket": args.raw_bucket,
        "planned_raw_key": raw_key,
        "planned_object_uri": f"s3://{args.raw_bucket}/{raw_key}",
        "http_status": http_status,
        "error": repr(exc),
        "failed_at": utc_now(),
        "status": "failed",
    }


def ingest(args, batch_dir: Path, plan_path: Path, status_path: Path, manifest_path: Path, failures_path: Path, summary_path: Path) -> int:
    session = make_session()
    client = S3Client(args.endpoint, args.access_key, args.secret_key)
    access_token = load_access_token()
    if args.require_access_token and not access_token:
        raise RuntimeError(
            f"no local Access Token found; set one in {TOKEN_CONFIG_PATH} or K12_ACCESS_TOKEN"
        )
    headers = download_headers(access_token)
    work_dir = batch_dir / "work"
    work_dir.mkdir(parents=True, exist_ok=True)

    uploaded = load_uploaded(status_path)
    plan_records = list(read_jsonl(plan_path) or [])
    total = len(plan_records)
    planned_bytes = sum(int(record.get("expected_size_bytes") or 0) for record in plan_records)
    completed = 0
    failed = 0
    uploaded_bytes = 0

    for index, record in enumerate(plan_records, start=1):
        content_id = record["content_id"]
        if content_id in uploaded:
            completed += 1
            uploaded_bytes += int(uploaded[content_id].get("size_bytes") or 0)
            continue
        local_path = work_dir / record["filename"]
        tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
        raw_key = f"{args.prefix}/batch_id={args.batch_id}/content_id={content_id}/pdf/{record['filename']}"
        try:
            print(f"INGEST {index}/{total} downloading content_id={content_id} title={record['title']}", flush=True)
            size, digest = retry(
                f"download content_id={content_id}",
                args.retry_attempts,
                args.retry_sleep,
                lambda: download_pdf(session, record, local_path, headers),
            )
            retry(
                f"upload content_id={content_id}",
                args.retry_attempts,
                args.retry_sleep,
                lambda: client.put_file(args.raw_bucket, raw_key, local_path, digest),
            )
            remote_size = retry(
                f"head content_id={content_id}",
                args.retry_attempts,
                args.retry_sleep,
                lambda: client.head(args.raw_bucket, raw_key),
            )
            if remote_size != size:
                raise RuntimeError(f"remote size mismatch: local {size}, remote {remote_size}")

            manifest_record = {
                "batch_id": args.batch_id,
                "source_platform": "国家中小学智慧教育平台",
                "capture_tool": "tchMaterial-parser-compatible-full-ingest",
                "tool_repo": TOOL_REPO,
                **record,
                "size_bytes": size,
                "sha256": digest,
                "object_uri": f"s3://{args.raw_bucket}/{raw_key}",
                "uploaded_at": utc_now(),
                "status": "uploaded",
            }
            jsonl_append(manifest_path, manifest_record)
            jsonl_append(status_path, {
                "content_id": content_id,
                "status": "uploaded",
                "size_bytes": size,
                "sha256": digest,
                "object_uri": manifest_record["object_uri"],
                "updated_at": utc_now(),
            })
            uploaded[content_id] = manifest_record
            completed += 1
            uploaded_bytes += size
            print(f"UPLOADED {completed}/{total} bytes={size} sha256={digest[:12]} key={raw_key}", flush=True)
        except Exception as exc:
            failed += 1
            failed_record = failure_record(args, record, raw_key, exc, "ingest")
            jsonl_append(failures_path, failed_record)
            jsonl_append(status_path, {**failed_record, "updated_at": utc_now()})
            print(f"FAILED {index}/{total} content_id={content_id} error={exc!r}", flush=True)
        finally:
            if not args.keep_local and local_path.exists():
                local_path.unlink()
            if not args.keep_local and tmp_path.exists():
                tmp_path.unlink()

        if args.upload_meta_every and (completed + failed) % args.upload_meta_every == 0:
            write_summary(summary_path, args, total, planned_bytes, completed, failed, uploaded_bytes, in_progress=True)
            upload_meta_files(client, args, args.batch_id, summary_path.parent)
        time.sleep(args.download_sleep)

    write_summary(summary_path, args, total, planned_bytes, completed, failed, uploaded_bytes, in_progress=False)
    upload_meta_files(client, args, args.batch_id, summary_path.parent)
    return 0 if failed == 0 else 2


def write_summary(path: Path, args, planned: int, planned_bytes: int, uploaded: int, failed: int, uploaded_bytes: int, in_progress: bool) -> None:
    summary = {
        "batch_id": args.batch_id,
        "scope": args.scope,
        "planned_count": planned,
        "planned_bytes": planned_bytes,
        "planned_gib": round(planned_bytes / (1024 ** 3), 3),
        "uploaded_count": uploaded,
        "failed_count": failed,
        "uploaded_bytes": uploaded_bytes,
        "uploaded_gib": round(uploaded_bytes / (1024 ** 3), 3),
        "raw_bucket": args.raw_bucket,
        "meta_bucket": args.meta_bucket,
        "prefix": args.prefix,
        "in_progress": in_progress,
        "updated_at": utc_now(),
    }
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Full K12 textbook PDF ingest into MinIO.")
    parser.add_argument("--batch-id", default=os.environ.get("K12_BATCH_ID") or batch_timestamp())
    parser.add_argument("--output-root", default=os.environ.get("K12_OUTPUT_ROOT") or "./k12-ingest")
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("K12_MINIO_ENDPOINT")
        or os.environ.get("S3_ENDPOINT_URL")
        or "http://127.0.0.1:19001",
    )
    parser.add_argument(
        "--access-key",
        default=os.environ.get("K12_MINIO_USER")
        or os.environ.get("AWS_ACCESS_KEY_ID"),
    )
    parser.add_argument(
        "--secret-key",
        default=os.environ.get("K12_MINIO_PASS")
        or os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )
    parser.add_argument("--raw-bucket", default="k12-textbook-raw")
    parser.add_argument("--meta-bucket", default="k12-textbook-meta")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--scope", choices=["tagged", "all"], default="tagged")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--rebuild-plan", action="store_true")
    parser.add_argument("--keep-local", action="store_true")
    parser.add_argument("--resolve-sleep", type=float, default=0.05)
    parser.add_argument("--download-sleep", type=float, default=0.5)
    parser.add_argument("--retry-attempts", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=3.0)
    parser.add_argument("--upload-meta-every", type=int, default=25)
    parser.add_argument("--require-access-token", action="store_true")
    args = parser.parse_args()

    if not args.plan_only and (not args.access_key or not args.secret_key):
        parser.error(
            "provide AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, "
            "K12_MINIO_USER/K12_MINIO_PASS, or command-line credentials"
        )

    batch_dir = Path(args.output_root) / args.batch_id
    meta_dir = batch_dir / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    plan_path = meta_dir / "resource_plan.jsonl"
    status_path = meta_dir / "status.jsonl"
    manifest_path = meta_dir / "manifest.jsonl"
    failures_path = meta_dir / "failures.jsonl"
    summary_path = meta_dir / "ingest_summary.json"

    resolved, expected_bytes, resolve_failed = build_plan(args, batch_dir, plan_path, failures_path)
    print(json.dumps({
        "batch_id": args.batch_id,
        "batch_dir": str(batch_dir),
        "planned_count": resolved,
        "expected_bytes": expected_bytes,
        "expected_gib": round(expected_bytes / (1024 ** 3), 3),
        "resolve_failed_count": resolve_failed,
        "plan_path": str(plan_path),
    }, ensure_ascii=False, indent=2), flush=True)

    if args.plan_only:
        return 0
    return ingest(args, batch_dir, plan_path, status_path, manifest_path, failures_path, summary_path)


if __name__ == "__main__":
    raise SystemExit(main())
