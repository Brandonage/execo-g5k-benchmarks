from sparkbench import SparkBenchmark


class SparkBenchmarkMesos(SparkBenchmark):

    def __init__(self, home_directory, default_master_scheduler):
        SparkBenchmark.__init__(self, home_directory,default_master_scheduler)

    def launchGenerateSVMFile(self,output,size,nfeatures,npartitions,master,submit_conf,scheduler_options):
        ### Here we can put some logic specific to Mesos
        SparkBenchmark.launchGenerateSVMFile(self,output,size,nfeatures,npartitions,master,submit_conf,scheduler_options)

    def launchGenerateGraphFile(self,output,size,npartitions,mu,sigma,master,submit_conf,scheduler_options):
        SparkBenchmark.launchGenerateGraphFile(self,output,size,npartitions,mu,sigma,master,submit_conf,scheduler_options)

    def launchConnectedComponent(self,input,output,npartitions,useless,master,submit_conf,scheduler_options):
        SparkBenchmark.launchConnectedComponent(self,input,output,npartitions,useless,master,submit_conf,scheduler_options)
