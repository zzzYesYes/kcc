from .cleaned_documents import cleaned_documents
from .mineru_parsed_documents import mineru_parsed_documents
from .pdf_manifest import pdf_manifest
from .qa_mcq_documents import qa_mcq_documents
from .raw_pdf_batch import raw_pdf_batch
from .training_jsonl_dataset import training_jsonl_dataset


ALL_ASSETS = [
    raw_pdf_batch,
    pdf_manifest,
    mineru_parsed_documents,
    cleaned_documents,
    qa_mcq_documents,
    training_jsonl_dataset,
]
