from sparksubmittermesos import SparkSubmitterMesos
from sparksubmitteryarn import SparkSubmitterYarn


class SparkSubmitterFactory:
    def get_spark_submitter(self, resource_manager, master_node, default_master, root_to_spark_submit):
        if resource_manager=="mesos":
            return SparkSubmitterMesos(master_node, default_master, root_to_spark_submit)
        if resource_manager=="yarn":
            return SparkSubmitterYarn(master_node, default_master, root_to_spark_submit)