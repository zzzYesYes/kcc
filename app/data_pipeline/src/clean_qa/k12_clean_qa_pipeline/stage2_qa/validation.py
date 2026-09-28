from __future__ import annotations

import ast
import operator
import re
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Any


ALLOWED_REJECTION_REASONS = {
    "source_not_answerable",
    "missing_image",
    "ocr_corruption",
    "formula_unverifiable",
    "unsupported_claim",
    "arithmetic_error",
    "unit_error",
    "multiple_correct_options",
    "duplicate_item",
    "judge_rejected",
    "open_ended_question",
}
OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def safe_calculate(expression: str) -> Decimal:
    if not re.fullmatch(r"[\d\s.+\-*/()]+", expression):
        raise ValueError("unsafe expression")

    def visit(node):
        if isinstance(node, ast.Expression):
            return visit(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return Decimal(str(node.value))
        if isinstance(node, ast.UnaryOp) and type(node.op) in OPS:
            return OPS[type(node.op)](visit(node.operand))
        if isinstance(node, ast.BinOp) and type(node.op) in OPS:
            return OPS[type(node.op)](visit(node.left), visit(node.right))
        raise ValueError("unsupported expression")

    return visit(ast.parse(expression, mode="eval"))


def numeric_value(value: str) -> Fraction | Decimal | None:
    text = str(value).strip().replace("％", "%")
    try:
        if text.endswith("%"):
            return Fraction(Decimal(text[:-1])) / 100
        if re.fullmatch(r"[-+]?\d+\s*/\s*\d+", text):
            left, right = text.split("/")
            return Fraction(int(left), int(right))
        if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", text):
            return Fraction(Decimal(text))
    except (ValueError, ZeroDivisionError, InvalidOperation):
        return None
    return None


def evidence_valid(evidence: list[str], source_text: str) -> bool:
    return bool(evidence) and all(
        isinstance(value, str) and value.strip() and value.strip() in source_text
        for value in evidence
    )


def validate_math(item: dict[str, Any]) -> tuple[bool, str | None]:
    check = item.get("math_check")
    if not check:
        return True, None
    try:
        actual = safe_calculate(str(check["expression"]))
        expected = Decimal(str(check["expected"]))
    except (KeyError, ValueError, InvalidOperation, ZeroDivisionError):
        return False, "formula_unverifiable"
    if actual != expected:
        return False, "arithmetic_error"
    final = str(item.get("final_answer") or item.get("answer") or "")
    unit = str(check.get("unit") or "")
    if unit and unit not in final:
        return False, "unit_error"
    return True, None


def validate_mcq(item: dict[str, Any], source_text: str) -> tuple[bool, str | None]:
    options = item.get("options")
    correct = item.get("correct_index")
    reasons = item.get("distractor_reasons")
    if (
        not isinstance(options, list)
        or len(options) != 4
        or len({str(value).strip() for value in options}) != 4
        or not isinstance(correct, int)
        or not 0 <= correct < 4
        or not isinstance(reasons, list)
        or len(reasons) != 4
    ):
        return False, "multiple_correct_options"
    values = [numeric_value(str(value)) for value in options]
    known = [value for value in values if value is not None]
    if len(known) != len(set(known)):
        return False, "multiple_correct_options"
    if not evidence_valid(item.get("evidence", []), source_text):
        return False, "unsupported_claim"
    return validate_math(item)


def validate_qa(item: dict[str, Any], source_text: str) -> tuple[bool, str | None]:
    required = ("question", "answer", "analysis", "final_answer", "question_type")
    if any(not str(item.get(field, "")).strip() for field in required):
        return False, "source_not_answerable"
    if re.search(r"(如右图|如下图|看图|观察图)", item["question"]):
        return False, "missing_image"
    if not evidence_valid(item.get("evidence", []), source_text):
        return False, "unsupported_claim"
    return validate_math(item)

