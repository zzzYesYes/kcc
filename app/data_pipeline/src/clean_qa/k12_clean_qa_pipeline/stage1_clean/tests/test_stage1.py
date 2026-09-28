from __future__ import annotations

import json
import unittest

from clean_qa.k12_clean_qa_pipeline.common.atomic_writer import jsonl_bytes
from clean_qa.k12_clean_qa_pipeline.stage1_clean.core import (
    build_stage1,
    html_table_to_markdown,
    normalize_formula,
    render_clean_markdown,
    split_raw_blocks,
)


SUCCESS = {
    "document_id": "pdf-test",
    "input": {"bucket": "raw", "key": "x/数学 六年级上册.pdf", "etag": "abc"},
    "page_count": 10,
    "image_count": 2,
}


class Stage1Tests(unittest.TestCase):
    def test_chapter_heading_recognition(self):
        blocks = split_raw_blocks("# 第一章 数\n\n## 1.1 分数\n\n正文")
        self.assertEqual(blocks[-1].chapter_path, ["第一章 数", "1.1 分数"])

    def test_question_number_is_not_heading(self):
        blocks = split_raw_blocks("# 练习\n\n1. 计算 1+1")
        self.assertIsNone(blocks[-1].heading_level)

    def test_details_and_decoration_filter(self):
        markdown = (
            "# 第一章\n\n正文\n\n![](images/a.jpg)\n"
            "<details><summary>natural_image</summary>decorative cartoon icon</details>"
        )
        result = build_stage1("pdf-test", markdown, [], SUCCESS)
        self.assertNotIn("<details>", result["clean_md"])
        self.assertEqual(result["images"][0]["classification"], "decorative")

    def test_standalone_details_and_floating_prompt_filter(self):
        result = build_stage1(
            "doc",
            "# 练习\n你能提出什么问题？\n"
            "<details><summary>natural_image</summary>decorative icon</details>\n"
            "<details><summary>text_image</summary>总人数 40 人，女生 22 人</details>",
            [],
            {"document_id": "doc", "input": {"key": "doc.pdf"}},
        )
        self.assertNotIn("<details>", result["clean_md"])
        self.assertNotIn("你能提出什么问题", result["clean_md"])
        self.assertIn("总人数 40 人", result["clean_md"])
        self.assertEqual(result["images"][0]["classification"], "decorative")

    def test_text_image_extraction(self):
        markdown = (
            "# 第一章\n\n![](images/a.jpg)\n"
            "<details><summary>text_image</summary>总人数 40 人，女生 22 人</details>"
        )
        result = build_stage1("pdf-test", markdown, [], SUCCESS)
        self.assertIn("总人数", result["clean_md"])

    def test_table_conversion(self):
        rendered, rows = html_table_to_markdown(
            "<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>"
        )
        self.assertIn("| A | B |", rendered)
        self.assertEqual({len(row) for row in rows}, {2})

    def test_formula_digit_spacing_repair(self):
        value, rules = normalize_formula("$1. 2 5 x = 3 0$")
        self.assertEqual(value, "$1.25 x = 30$")
        self.assertIn("digit_spacing_repair", rules)

    def test_copyright_filter(self):
        result = build_stage1(
            "pdf-test",
            "# 封面\n\n责任编辑：张三\n\n# 第一章\n\n数学正文。",
            [],
            SUCCESS,
        )
        self.assertNotIn("责任编辑", result["clean_md"])

    def test_self_evaluation_filter(self):
        result = build_stage1(
            "pdf-test",
            "# 第一章\n\n数学正文。\n\n## 自我评价\n\n老师评价____",
            [],
            SUCCESS,
        )
        self.assertNotIn("老师评价", result["clean_md"])

    def test_dangling_image_question_quarantine(self):
        result = build_stage1(
            "pdf-test",
            "# 练习\n\n1. 如右图，求阴影部分面积。",
            [],
            SUCCESS,
        )
        self.assertTrue(result["quarantine"])
        self.assertNotIn("如右图", result["clean_md"])

    def test_stable_block_id(self):
        markdown = "# 第一章\n\n定义：分数表示整体的一部分。\n\n## 练习\n\n1. 计算。"
        first = build_stage1("pdf-test", markdown, [], SUCCESS)
        second = build_stage1("pdf-test", markdown, [], SUCCESS)
        self.assertEqual(
            [row["block_id"] for row in first["blocks"]],
            [row["block_id"] for row in second["blocks"]],
        )

    def test_jsonl_serialization(self):
        body = jsonl_bytes([{"text": "数学"}, {"value": 1}])
        self.assertEqual(len([json.loads(line) for line in body.splitlines()]), 2)

    def test_clean_markdown_rendering(self):
        value = render_clean_markdown(
            [
                {"source_order": 2, "keep_in_clean_md": True, "clean_text": "B"},
                {"source_order": 1, "keep_in_clean_md": True, "clean_text": "A"},
                {"source_order": 3, "keep_in_clean_md": False, "clean_text": "C"},
            ]
        )
        self.assertEqual(value, "A\n\nB\n")


if __name__ == "__main__":
    unittest.main()
