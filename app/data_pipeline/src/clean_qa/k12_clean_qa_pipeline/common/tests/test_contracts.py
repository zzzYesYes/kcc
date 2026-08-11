import unittest

from clean_qa.k12_clean_qa_pipeline.common.contracts import (
    prefixes_overlap,
    resume_contract_matches,
)
from clean_qa.k12_clean_qa_pipeline.common.hashing import canonical_sha256


class ContractTests(unittest.TestCase):
    def test_original_input_and_output_cannot_overlap(self):
        self.assertTrue(prefixes_overlap("b", "raw", "b", "raw/output"))
        self.assertFalse(prefixes_overlap("b", "raw", "out", "raw/output"))

    def test_prompt_change_invalidates_resume(self):
        old = {"source": "a", "prompt": "v1"}
        marker = {"input_contract_sha256": canonical_sha256(old)}
        self.assertFalse(
            resume_contract_matches(marker, {"source": "a", "prompt": "v2"})
        )

    def test_identical_contract_resumes(self):
        contract = {"source": "a", "version": "v1"}
        marker = {"input_contract_sha256": canonical_sha256(contract)}
        self.assertTrue(resume_contract_matches(marker, contract))


if __name__ == "__main__":
    unittest.main()
