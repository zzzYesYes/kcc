from dagster import Definitions

from .jobs import ALL_JOBS


defs = Definitions(jobs=ALL_JOBS)
