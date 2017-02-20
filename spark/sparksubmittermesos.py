from sparksubmitter import SparkSubmitter

# We inherit because the behaviour of spark submit changes depending on the resource manager
class SparkSubmitterMesos(SparkSubmitter):

    def __init__(self,master_node,default_master, root_to_spark_submit):
        SparkSubmitter.__init__(self,master_node, default_master, root_to_spark_submit)

    def submit(self,class_in_jar, class_params, jar, master, submit_conf,scheduler_options):
        pass