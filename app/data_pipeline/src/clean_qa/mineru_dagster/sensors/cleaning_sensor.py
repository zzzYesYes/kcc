from dagster import DefaultSensorStatus, SensorEvaluationContext, SkipReason, sensor


@sensor(minimum_interval_seconds=60, default_status=DefaultSensorStatus.STOPPED)
def cleaning_sensor(context: SensorEvaluationContext):
    """Guarded placeholder; enable only after the 10 and 100 document gates pass."""
    return SkipReason("Automatic cleaning is intentionally disabled pending the 100-document stability test.")
