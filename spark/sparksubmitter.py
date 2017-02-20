# A CLASS THAT IS GOING TO REPRESENT THE SPAR-SUBMIT INSTRUCTION AND ITS GOING TO EXECUTE IT
# MAYBE IS GOOD TO CREATE A SUBMIT CLASS OUTSIDE OF THE SPARK SCOPE AND MAKE THIS ONE TO INHERIT:
# NOT REALLY. THE SPARK-SUBMIT SCRIPT ITS ONLY VALID FOR SPARK. THE ONLY COMMON FEATURE WITH OTHER
# SUBMITTERS IS THE SUBMIT FUNCTION


class SparkSubmitter:

    def __init__(self, master_node, default_master, root_to_spark_submit):
        self.master_node = master_node
        self.default_master = default_master
        self.root_to_spark_submit = root_to_spark_submit

    # generate a string that is understood by spark-submit script with scheduler options specific to the res manager
    # scheduler_options: a list of tuples with the format (conf_option,conf_value)
    def generate_scheduler_options(self,scheduler_options):
        raise NotImplementedError( "Abstract method. To be implemented by subclasses" )

    # generate a string that is understandable by spark-submit script with the configuration options like
    # spark.executor.memory, spark.executor.cores, buffers sizes and so on...
    # submit_conf: a list of tuples with the format (conf_option,conf_value)
    # returns: a string to attach to the spark-submit command
    def generate_conf(self,submit_conf): # implemented here since it's common across resource managers
        string_conf = ""
        for tuple in submit_conf:
            if (tuple[0] == "executor.num"):
                string_conf += "--num-executors " + tuple[1] + " "
            else:
                string_conf += "--conf " + tuple[0] + "=" + tuple[1] + " "
        return string_conf[:-1] # remove the last space with [:-1]

    def submit(self,class_in_jar, class_params, jar, master, submit_conf,scheduler_options):
        raise NotImplementedError( "Abstract method. To be implemented by subclasses" )

