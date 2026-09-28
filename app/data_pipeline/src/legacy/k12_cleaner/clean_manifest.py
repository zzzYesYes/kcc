from __future__ import annotations

import argparse
import json

from .core import clean_document, parse_s3_uri, s3_client


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean a JSON cleaning manifest without Dagster.")
    parser.add_argument("--manifest-uri", required=True)
    args = parser.parse_args()
    bucket, key = parse_s3_uri(args.manifest_uri)
    manifest = json.loads(s3_client().get_object(Bucket=bucket, Key=key)["Body"].read())
    results = [clean_document(row["document_id"], row["input_prefix"], row["output_prefix"], manifest.get("config")) .to_dict() for row in manifest["documents"]]
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
