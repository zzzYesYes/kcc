from __future__ import annotations

import json
from typing import Any

from . import PROMPT_VERSION


SYSTEM_PROMPT = """你是K12教材数据工程器。只依据给定教材块生成数据，不补充外部知识。
输出必须是单个JSON对象，不要Markdown代码围栏。证据必须逐字摘自source_text。
若材料不适合生成，eligible=false并给出reason。选择题必须四选一，选项语义和数学值不得等价。
计算题必须提供math_check.expression、math_check.expected和math_check.unit，expression只能包含数字和+-*/()。"""


def generation_messages(blocks: list[dict[str, Any]]) -> list[dict[str, str]]:
    payload = [
        {
            "block_id": block["block_id"],
            "chapter_path": block.get("chapter_path", []),
            "block_type": block.get("block_type"),
            "source_text": block["clean_text"][:5000],
        }
        for block in blocks
    ]
    schema = {
        "results": [
            {
                "block_id": "...",
                "eligible": True,
                "reason": "",
                "facts": [{"statement": "...", "evidence": ["原文子串"]}],
                "qa": [
                    {
                        "question_type": "definition",
                        "difficulty": "easy",
                        "question": "...",
                        "answer": "...",
                        "analysis": "...",
                        "final_answer": "...",
                        "evidence": ["原文子串"],
                        "math_check": None,
                    }
                ],
                "mcq": [
                    {
                        "question": "...",
                        "options": ["A", "B", "C", "D"],
                        "correct_index": 0,
                        "analysis": "...",
                        "evidence": ["原文子串"],
                        "distractor_reasons": [
                            "correct",
                            "wrong_denominator",
                            "unit_dimension_error",
                            "arithmetic_error",
                        ],
                        "math_check": None,
                    }
                ],
            }
        ]
    }
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"prompt_version={PROMPT_VERSION}\n"
                f"输入块={json.dumps(payload, ensure_ascii=False)}\n"
                f"输出结构={json.dumps(schema, ensure_ascii=False)}"
            ),
        },
    ]


def judge_messages(item: dict[str, Any], source_text: str) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "你是独立教材质量Judge。只输出JSON。逐项判断grounded、question_clear、"
                "answer_correct、analysis_correct、age_appropriate、single_correct_option、"
                "source_supported。全部为true才accept=true。"
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "source_text": source_text[:5000],
                    "candidate": item,
                    "output_schema": {
                        "accept": True,
                        "checks": {
                            "grounded": True,
                            "question_clear": True,
                            "answer_correct": True,
                            "analysis_correct": True,
                            "age_appropriate": True,
                            "single_correct_option": True,
                            "source_supported": True,
                        },
                        "reason": "",
                    },
                },
                ensure_ascii=False,
            ),
        },
    ]

