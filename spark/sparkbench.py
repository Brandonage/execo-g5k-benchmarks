
#### A CLASS THAT IS GOING TO REPRESENT THE SPARK-BENCH BENCHMARK
#### IT's JUST GOING TO LINK THE SOURCE JARS OF THE BENCHMARK WITH ITS PARAMETERS AND PASS IT TO THE SUBMITTER TO FORMAT AND LAUNCH THROUGH EXECO

from sparksubmitter import SparkSubmitter
from defaultsettings import get_default_settings
from sizecalculator import get_npoints_for_size

class SparkBenchmark:
    def __init__(self,home_directory,default_master_scheduler): ## submit_type means the type of submitter (e.g. mesos, spark_standalone, yarn)
        self.submitter = SparkSubmitter()
        self.home_directory = home_directory
        self.default_master = default_master_scheduler ## We include this to not be verbose when choosing master schedulers
        self.jar_directory = self.home_directory + 'BenchMark-1.0-SNAPSHOT.jar'

    #   We forget about the concrete parameters of the class for the file generators. We are interested in a file size and number of partitions to split it into
    def launchGenerateTextFile(self,master,conf,output,size,npartitions):
        pass

    def launchGenerateSVMFile(self,output,size,nfeatures,npartitions,master,submit_conf,scheduler_options):
        npoints = get_npoints_for_size('svm',size)
        default_settings = get_default_settings('generatesvm')
        if nfeatures==None:  # if we don't have the parameters for that applicacion we get the default ones
            nfeatures = default_settings.get('nfeatures')
        if master==None:
            master=self.default_master
        class_params = [output,npoints,nfeatures,npartitions]
        self.submitter.submit(class_in_jar="com.abrandon.upm.GenerateSVMData",class_params=class_params,
                              jar=self.jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchGenerateGraphFile(self,output,size,npartitions,mu,sigma,master,submit_conf,scheduler_options):
        npoints = get_npoints_for_size('graph',size)
        default_settings = get_default_settings('generategraph')
        if mu==None:
            mu = default_settings.get('mu')
        if sigma==None:
            sigma = default_settings.get('sigma')
        if master==None:
            master=self.default_master
        class_params = [output,npoints,npartitions,mu,sigma]
        self.submitter.submit(class_in_jar="DataGen.src.main.scala.GraphDataGen",class_params=class_params,
                              jar=self.jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    ## the class needs one parameter at the end that is not needed
    def launchConnectedComponent(self,input,output,npartitions,useless,master,submit_conf,scheduler_options):
        default_settings = get_default_settings('connectedcomponent')
        if npartitions==None:
            npartitions = default_settings.get('npartitions')
        useless = default_settings.get('useless')
        if master==None:
            master=self.default_master
        class_params = [input,output,npartitions,useless]
        self.submitter.submit(class_in_jar="src.main.scala.ConnectedComponentApp",class_params=class_params,
                              jar=self.jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)




