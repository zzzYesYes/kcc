from .autoscale_nojudge_job import AUTOSCALE_NOJUDGE_JOBS
from .e2e_demo_job import E2E_DEMO_JOBS
from .stage1_jobs import STAGE1_JOBS
from .stage2_jobs import STAGE2_JOBS

ALL_PIPELINE_JOBS = [
    *STAGE1_JOBS,
    *STAGE2_JOBS,
    *E2E_DEMO_JOBS,
    *AUTOSCALE_NOJUDGE_JOBS,
]
