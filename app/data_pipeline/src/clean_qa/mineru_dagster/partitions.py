from dagster import DynamicPartitionsDefinition


batch_partitions = DynamicPartitionsDefinition(name="batch_id")
