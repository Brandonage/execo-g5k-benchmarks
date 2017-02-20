
#### A CLASS THAT IS GOING TO REPRESENT THE SPARK-BENCH BENCHMARK
#### IT's JUST GOING TO LINK THE SOURCE JARS OF THE BENCHMARK WITH ITS PARAMETERS AND PASS IT TO THE SUBMITTER TO FORMAT AND LAUNCH THROUGH EXECO

from sparksubmitterfactory import SparkSubmitterFactory
from defaultsettings import get_default_settings
from sizecalculator import get_npoints_for_size

# The object that represents the spark-bench benchmark of https://github.com/SparkTC/spark-bench.
# TODO: Decouple the SparkBench and SparkSubmitter objects and pass it as a parameter launchX(submitter1,params...)
# The only handicap of not doing this decoupling is that we will have to create a new Benchmark object
# for each type of cluster we have e.g ( A YARN cluster, a Mesos Cluster, a standalone... ). This have
# advantages tho since we can have different types of benchmark in our experiment without worrying about
# what submitter to pass as a parameter. e.g. (A flink Benchmark, Spark Benchmark and launch them in the same
# experiment)

class SparkBench:
    def __init__(self, home_directory, master_node, resource_manager, root_to_spark_submit, default_master): ## submit_type means the type of submitter (e.g. mesos, spark_standalone, yarn)
        """
        :param home_directory: the directory where the benchmark jar is
        :param master_node: the machine used by execo to launch the benchmark
        :param default_master: the default_master for the spark submitter. Avoids specifying the master every time
        :param resource_manager: the resource manager that the benchmark will use (yarn,mesos...)
        :param root_to_spark_submit: the path to the spark-submit script
        """
        factory = SparkSubmitterFactory()
        self.submitter = factory.get_spark_submitter(resource_manager, master_node, default_master,
                                                     root_to_spark_submit)  ## we include the default master
        self.home_directory = home_directory                                          ## to avoid verbosity but we can later
        ## this is the directory where the jar with the benchmark resides             ## change it on the submitter.submit method

    #   We forget about the concrete parameters of the class for the file generators. We are interested in a file size and number of partitions to split it into
    def launchgeneratetextfile(self,master,conf,output,size,npartitions):
        pass

    def launchgeneratesvmfile(self,output,size,nfeatures,npartitions,master,submit_conf,scheduler_options):
        npoints = get_npoints_for_size('svm',size)
        default_settings = get_default_settings('generatesvm')
        if nfeatures==None:  # if we don't have the parameters for that applicacion we get the default ones
            nfeatures = default_settings.get('nfeatures')
        class_params = [output,npoints,nfeatures,npartitions]
        self.submitter.submit(class_in_jar="com.abrandon.upm.GenerateSVMData",class_params=class_params,
                              jar=self.jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgenerategraphfile(self,output,size,npartitions,submit_conf,scheduler_options,master=None,mu=None,sigma=None):
        npoints = get_npoints_for_size('graph',size)
        default_settings = get_default_settings('generategraph')
        if mu==None:
            mu = default_settings.get('mu')
        if sigma==None:
            sigma = default_settings.get('sigma')
        class_params = [output,npoints,npartitions,mu,sigma]
        jar_directory = self.home_directory + "common/target/Common-1.0.jar"
        self.submitter.submit(class_in_jar="DataGen.src.main.scala.GraphDataGen",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    ## the class needs one parameter at the end that is not needed
    def launchconnectedcomponent(self,input,output,npartitions,useless,master,submit_conf,scheduler_options):
        default_settings = get_default_settings('connectedcomponent')
        if npartitions==None:
            npartitions = default_settings.get('npartitions')
        useless = default_settings.get('useless')
        if master==None:
            master=self.default_master
        class_params = [input,output,npartitions,useless]
        jar_directory = self.home_directory + "ConnectedComponent/target/ConnectedComponentApp-1.0.jar"
        self.submitter.submit(class_in_jar="src.main.scala.ConnectedComponentApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)




