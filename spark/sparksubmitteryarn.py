from sparksubmitter import SparkSubmitter
from execo_g5k import g5k_configuration
from execo import Remote
import sys

# We inherit because the behaviour of spark submit changes depending on the resource manager
class SparkSubmitterYarn(SparkSubmitter):

    def __init__(self,master_node,default_master,root_to_spark_submit):
        SparkSubmitter.__init__(self,master_node,default_master, root_to_spark_submit)

    def generate_scheduler_options(self,scheduler_options):
        # For the moment and since we don't know how different are the resource manager options
        # we only return an empty space
        return ""

    ## This will build the necessary string to submit the application. it uses the spark-submit option.
    # ./bin/spark-submit \
    # --class <main-class> \
    # --master <master-url> \
    # --deploy-mode <deploy-mode> \
    # --conf <key>=<value> \
    # --packages
    # ... # other options
    # <application-jar> \
    # [application-arguments]
    ## FUTURE: We might add an option in submit that indicates if we want to monitor this application or not
    def submit(self,class_in_jar, class_params, jar, master, submit_conf,scheduler_options):
        """
        :param class_in_jar: the class we want to launch inside the jar
        :param class_params: the parameters expected by that class
        :param jar: the jar where the class is bundled
        :param master: the master that is going to take care of launching the app. ("yarn","spark:/192.168.0.1", etc..)
        :param submit_conf: a list of tuples with the form [["spark.executor.memory","2g"],["spark.executor.cores","1"]]
        :param scheduler_options: options that are only applicable to that resource manager e.g. (Mesos tags, yarn labels...)
        """
        if master is None:
            master = self.default_master
        if scheduler_options is None:
            scheduler_options = ""
        scheduler_str = self.generate_scheduler_options(scheduler_options)
        conf_str = self.generate_conf(submit_conf)
        cmd = "{0} --class {1} --master {2} --deploy-mode client {3} {4} {5} {6}".format(
            self.root_to_spark_submit,
            class_in_jar,
            master,
            conf_str,
            jar,
            " ".join(class_params),
            scheduler_str
        )
        Remote(cmd, hosts=self.master_node, connection_params={'user':g5k_configuration.get('g5k_user')}
               , process_args={'stdout_handlers': [sys.stdout], 'stderr_handlers': [sys.stderr]} ).run()


