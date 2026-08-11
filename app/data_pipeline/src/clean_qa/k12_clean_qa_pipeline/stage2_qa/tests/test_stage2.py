import json
import unittest

from clean_qa.k12_clean_qa_pipeline.stage2_qa.helpers import (
    deduplicate,
    json_from_content,
    merge_adjacent_blocks,
    rule_prefilter,
    select_units_by_chapter,
)
from clean_qa.k12_clean_qa_pipeline.stage2_qa.prompts import judge_batch_messages
from clean_qa.k12_clean_qa_pipeline.stage2_qa.validation import (
    evidence_valid,
    numeric_value,
    safe_calculate,
    validate_math,
    validate_mcq,
    validate_qa,
)


SOURCE = "圆的面积等于圆周率乘半径的平方。半径为2厘米时，面积为4π平方厘米。"


class Stage2Tests(unittest.TestCase):
    @staticmethod
    def _block(
        index: int,
        chapter: str = "第一章",
        block_type: str = "concept",
        text: str | None = None,
    ):
        value = text or f"这是第{index}个可用于生成训练数据的教材知识内容。"
        return {
            "document_id": "doc-1",
            "block_id": f"b{index}",
            "source_order": index,
            "chapter_path": [chapter],
            "block_type": block_type,
            "source_text": value,
            "clean_text": value,
            "formulas": [],
            "tables": [],
            "images": [],
        }

    def test_eligibility_schema(self):
        block = {
            "block_id": "b1",
            "clean_text": SOURCE,
            "qa_eligible_candidate": True,
            "image_required": False,
        }
        self.assertIsNone(rule_prefilter(block, set()))

    def test_prompt_json_parsing(self):
        self.assertEqual(json_from_content("```json\n{\"a\":1}\n```")["a"], 1)

    def test_evidence_reference(self):
        self.assertTrue(evidence_valid(["圆的面积"], SOURCE))
        self.assertFalse(evidence_valid(["外部知识"], SOURCE))

    def test_calculation_recheck(self):
        self.assertEqual(safe_calculate("(2+3)*4"), 20)

    def test_unit_validation(self):
        item = {
            "math_check": {"expression": "2*2", "expected": "4", "unit": "平方厘米"},
            "final_answer": "4",
        }
        self.assertEqual(validate_math(item), (False, "unit_error"))

    def test_mcq_unique_option(self):
        item = {
            "options": ["1", "2", "3", "4"],
            "correct_index": 1,
            "distractor_reasons": ["wrong", "correct", "wrong", "wrong"],
            "evidence": ["圆的面积"],
        }
        self.assertTrue(validate_mcq(item, SOURCE)[0])

    def test_mathematical_equivalent_options(self):
        item = {
            "options": ["0.5", "50%", "1", "2"],
            "correct_index": 0,
            "distractor_reasons": ["correct", "wrong", "wrong", "wrong"],
            "evidence": ["圆的面积"],
        }
        self.assertEqual(validate_mcq(item, SOURCE)[1], "multiple_correct_options")

    def test_distractor_reason_count(self):
        item = {
            "options": ["1", "2", "3", "4"],
            "correct_index": 0,
            "distractor_reasons": ["correct"],
            "evidence": ["圆的面积"],
        }
        self.assertFalse(validate_mcq(item, SOURCE)[0])

    def test_exact_deduplication(self):
        rows = [
            {"question": "1+1=?", "item_id": "a"},
            {"question": "1 + 1 = ?", "item_id": "b"},
        ]
        unique, rejected = deduplicate(rows)
        self.assertEqual((len(unique), len(rejected)), (1, 1))

    def test_near_duplicate_detection(self):
        rows = [
            {"question": "圆的面积计算公式是什么", "item_id": "a"},
            {"question": "圆的面积计算公式是什么？", "item_id": "b"},
        ]
        unique, rejected = deduplicate(rows)
        self.assertEqual((len(unique), len(rejected)), (1, 1))

    def test_qa_structure(self):
        item = {
            "question": "圆的面积公式是什么？",
            "answer": "圆周率乘半径的平方",
            "analysis": "直接依据定义。",
            "final_answer": "圆周率乘半径的平方",
            "question_type": "definition",
            "evidence": ["圆的面积等于圆周率乘半径的平方"],
        }
        self.assertTrue(validate_qa(item, SOURCE)[0])

    def test_sft_shape(self):
        row = {
            "messages": [
                {"role": "user", "content": "问题"},
                {"role": "assistant", "content": "答案"},
            ]
        }
        self.assertEqual(json.loads(json.dumps(row))["messages"][1]["role"], "assistant")

    def test_numeric_equivalence(self):
        self.assertEqual(numeric_value("0.5"), numeric_value("1/2"))

    def test_adjacent_blocks_merge_with_traceability(self):
        units = merge_adjacent_blocks(
            [self._block(1), self._block(2), self._block(3, block_type="exercise")],
            max_chars=1000,
            max_blocks=8,
        )
        self.assertEqual(len(units), 2)
        self.assertEqual(units[0]["block_id"], "b1")
        self.assertEqual(units[0]["source_block_ids"], ["b1", "b2"])
        self.assertEqual(units[0]["merged_block_count"], 2)
        self.assertEqual(units[1]["source_block_ids"], ["b3"])

    def test_merge_does_not_cross_chapter(self):
        units = merge_adjacent_blocks(
            [self._block(1, "第一章"), self._block(2, "第二章")],
            max_chars=1000,
            max_blocks=8,
        )
        self.assertEqual(len(units), 2)

    def test_chapter_quota_and_document_round_robin(self):
        units = [
            self._block(1, "第一章"),
            self._block(2, "第一章"),
            self._block(3, "第一章"),
            self._block(4, "第二章"),
            self._block(5, "第二章"),
            self._block(6, "第二章"),
        ]
        selected = select_units_by_chapter(
            units, chapter_max_units=2, document_max_units=3
        )
        self.assertEqual(len(selected), 3)
        self.assertEqual(
            {tuple(row["chapter_path"]) for row in selected},
            {("第一章",), ("第二章",)},
        )

    def test_judge_prompt_is_batched_and_item_aligned(self):
        item1 = {"item_id": "i1", "question": "问题1"}
        item2 = {"item_id": "i2", "question": "问题2"}
        messages = judge_batch_messages([(item1, SOURCE), (item2, SOURCE)])
        payload = json.loads(messages[1]["content"])
        self.assertEqual([row["item_id"] for row in payload["items"]], ["i1", "i2"])
        self.assertIn("results", payload["output_schema"])


if __name__ == "__main__":
    unittest.main()
