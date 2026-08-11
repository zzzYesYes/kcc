from __future__ import annotations

import argparse
import json

from .core import clean_document


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean one MinerU document directly in S3.")
    parser.add_argument("--document-id", required=True)
    parser.add_argument("--input-prefix", required=True)
    parser.add_argument("--output-prefix", required=True)
    args = parser.parse_args()
    print(json.dumps(clean_document(args.document_id, args.input_prefix, args.output_prefix).to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
