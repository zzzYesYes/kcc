from .mineru_checks import MINERU_CHECKS
from .cleaning_checks import CLEANING_CHECKS


ALL_CHECKS = [*MINERU_CHECKS, *CLEANING_CHECKS]

__all__ = ["ALL_CHECKS", "MINERU_CHECKS", "CLEANING_CHECKS"]
