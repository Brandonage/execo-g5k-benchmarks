
#### A CLASS THAT IS GOING TO REPRESENT THE SPARK-BENCH BENCHMARK
#### IT's JUST GOING TO LINK THE SOURCE JARS OF THE BENCHMARK WITH ITS PARAMETERS AND PASS IT TO THE SUBMITTER TO FORMAT AND LAUNCH THROUGH EXECO

from sparksubmitterfactory import SparkSubmitterFactory
from defaultsettings import get_default_settings
from sizecalculator import get_npoints_for_size
from execo_g5k import get_g5k_sites, g5k_configuration
from execo import Remote, Put


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

    #   There are two types of processes in this benchmark. generatefile and launchapplication.
    #   The main differences between the two are:
    #   - Generatefile doesn't have input parameter
    #   - Generatefile's npartitions is mandatory
    #   - Generatefile's output is mandatory


    #   BEGIN: generatefile suite. npartitions is mandatory on these

    def launchgeneratekmeansfile(self,output,size,npartitions,submit_conf,nclusters=None,
                                 ndimensions=None, scaling=None,scheduler_options=None,master=None):
        npoints = get_npoints_for_size('kmeans',size)
        default_settings = get_default_settings('generatekmeans')
        if nclusters is None:
            nclusters = default_settings.get('nclusters')
        if ndimensions is None:
            ndimensions = default_settings.get('ndimensions')
        if scaling is None:
            scaling = default_settings.get('scaling')
        class_params = [output,npoints,nclusters,ndimensions,scaling,str(npartitions)]
        jar_directory = self.home_directory + "KMeans/target/KMeansApp-1.0.jar"
        self.submitter.submit(class_in_jar="kmeans_min.src.main.scala.KmeansDataGen",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgeneratelogisticregfile(self,output,size,npartitions,submit_conf,nfeatures=None,
                                 eps=None, probone=None,scheduler_options=None,master=None):
        npoints = get_npoints_for_size('logistic',size)
        default_settings = get_default_settings('generatelogisticreg')
        if nfeatures is None:
            nfeatures = default_settings.get('nfeatures')
        if eps is None:
            eps = default_settings.get('eps')
        if probone is None:
            probone = default_settings.get('probone')
        class_params = [output,npoints,nfeatures,eps,probone,str(npartitions)]
        jar_directory = self.home_directory + "LogisticRegression/target/LogisticRegressionApp-1.0.jar"
        self.submitter.submit(class_in_jar="LogisticRegression.src.main.java.LogisticRegressionDataGen",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgeneratelinearregfile(self,output,size,npartitions,submit_conf,nfeatures=None,
                                 eps=None, probone=None,scheduler_options=None,master=None):
        npoints = get_npoints_for_size('linear',size)
        default_settings = get_default_settings('generatelinearreg')
        if nfeatures is None:
            nfeatures = default_settings.get('nfeatures')
        if eps is None:
            eps = default_settings.get('eps')
        if probone is None:
            probone = default_settings.get('probone')
        class_params = [output,npoints,nfeatures,eps,probone,str(npartitions)]
        jar_directory = self.home_directory + "LinearRegression/target/LinearRegressionApp-1.0.jar"
        self.submitter.submit(class_in_jar="LinearRegression.src.main.java.LinearRegressionDataGen",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgeneratepcafile(self,output,size,npartitions,submit_conf,nfeatures=None,
                                 scheduler_options=None,master=None):
        npoints = get_npoints_for_size('pca',size)
        default_settings = get_default_settings('generatepca')
        if nfeatures is None:
            nfeatures = default_settings.get('nfeatures')
        class_params = [output,npoints,nfeatures,str(npartitions)]
        jar_directory = self.home_directory + "PCA/target/PCAApp-1.0.jar"
        self.submitter.submit(class_in_jar="PCA.src.main.scala.PCADataGen",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgeneratedectreefile(self,output,size,npartitions,submit_conf,nfeatures=None,
                                 scheduler_options=None,master=None):
        npoints = get_npoints_for_size('decisiontree',size)
        default_settings = get_default_settings('generatedectree')
        if nfeatures is None:
            nfeatures = default_settings.get('nfeatures')
        class_params = [output,npoints,nfeatures,str(npartitions)]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.GenerateSVMData",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgeneratetextfile(self,output,size,npartitions,submit_conf,
                                 scheduler_options=None,master=None):
        npoints = get_npoints_for_size('words',size)
        default_settings = get_default_settings('generatetext')
        class_params = [output,npoints,str(npartitions)]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.GenerateRandomText",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgeneratesvmfile(self,output,size,npartitions,submit_conf,nfeatures=None,
                                 scheduler_options=None,master=None):
        npoints = get_npoints_for_size('svm',size)
        default_settings = get_default_settings('generatesvm')
        if nfeatures is None:
            nfeatures = default_settings.get('nfeatures')
        class_params = [output,npoints,nfeatures,str(npartitions)]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.GenerateSVMData",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgeneraterddrelationalfile(self,output,size,npartitions,submit_conf,
                                 scheduler_options=None,master=None):
        npoints = get_npoints_for_size('relational',size)
        class_params = [npoints,output,str(npartitions)]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.GenerateRDDRelation",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgenerategraphfile(self,output,size,npartitions,submit_conf,scheduler_options=None,master=None,mu=None,sigma=None):
        npoints = get_npoints_for_size('graph',size)
        default_settings = get_default_settings('generategraph')
        if mu is None:
            mu = default_settings.get('mu')
        if sigma is None:
            sigma = default_settings.get('sigma')
        class_params = [output,npoints,str(npartitions),mu,sigma]
        jar_directory = self.home_directory + "common/target/Common-1.0.jar"
        self.submitter.submit(class_in_jar="DataGen.src.main.scala.GraphDataGen",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    #   END: of the generate files suite

    #   BEGIN: launch applications suite

    def launchgrep(self,input,submit_conf,output=None,npartitions=None,keyword=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('grep')
        if keyword is None:
            keyword = default_settings.get('keyword')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,keyword,output,str(npartitions)]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.Grep",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchkmeans(self,input,submit_conf,output=None,npartitions=None,nclusters=None,max_iterations=None,num_run=None,
                     scheduler_options=None,master=None):
        default_settings = get_default_settings('kmeans')
        if nclusters is None:
            nclusters = default_settings.get('nclusters')
        if max_iterations is None:
            max_iterations = default_settings.get('max_iterations')
        if num_run is None:
            num_run = default_settings.get('num_run')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,output,nclusters,max_iterations,num_run,str(npartitions)]
        jar_directory = self.home_directory + "KMeans/target/KMeansApp-1.0.jar"
        self.submitter.submit(class_in_jar="KmeansApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchsort(self,input,submit_conf,output=None,npartitions=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('sort')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,output,str(npartitions)]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.Sort",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchsvm(self,input,submit_conf,npartitions=None,niterations=None,
                     scheduler_options=None,master=None):
        default_settings = get_default_settings('svm')
        if niterations is None:
            niterations = default_settings.get('niterations')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,niterations,str(npartitions)]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.SupportVectorMachine",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchwordcount(self,input,submit_conf,output=None,npartitions=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('wordcount')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,output,str(npartitions)]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.WordCount",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchgroupby(self,submit_conf, num_kvpairs=None, value_size=None, nmappers=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('groupby')
        if num_kvpairs is None:
            num_kvpairs = default_settings.get('num_kvpairs')
        if value_size is None:
            value_size = default_settings.get('value_size')
        if nmappers is None:
            nmappers = default_settings.get('nmappers')
        class_params = [num_kvpairs,value_size,nmappers]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.GroupByTest",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchrdd(self,input,submit_conf,output=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('rddrelational')
        if output is None:
            output = default_settings.get('output')
        class_params = [input,output]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.RDDRelation",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchngrams(self,input,submit_conf,output=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('ngrams')
        if output is None:
            output = default_settings.get('output')
        class_params = [input,output]
        jar_directory = self.home_directory + "BenchMark-1.0-SNAPSHOT.jar"
        self.submitter.submit(class_in_jar="com.abrandon.upm.NGramsExample",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    ## the class needs one parameter at the end that is not needed
    def launchconnectedcomponent(self,input,submit_conf,output=None,npartitions=None,scheduler_options=None,master=None,useless=None):
        default_settings = get_default_settings('connectedcomponent')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        if useless is None:
            useless = default_settings.get('useless')
        class_params = [input,output,npartitions,useless]
        jar_directory = self.home_directory + "ConnectedComponent/target/ConnectedComponentApp-1.0.jar"
        self.submitter.submit(class_in_jar="src.main.scala.ConnectedComponentApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchdecisiontrees(self,input,submit_conf,output=None,nclasses=None,impurity=None,
                            max_depth=None,max_bins=None,mode=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('decisiontree')
        if nclasses is None:
            nclasses = default_settings.get('nclasses')
        if impurity is None:
            impurity = default_settings.get('impurity')
        if max_depth is None:
            max_depth = default_settings.get('max_depth')
        if max_bins is None:
            max_bins = default_settings.get('max_bins')
        if mode is None:
            mode = default_settings.get('mode')
        if output is None:
            output = default_settings.get('output')
        class_params = [input,output,nclasses,impurity,max_depth,max_bins,mode]
        jar_directory = self.home_directory + "DecisionTree/target/DecisionTreeApp-1.0.jar"
        self.submitter.submit(class_in_jar="DecisionTree.src.main.java.DecisionTreeApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchpagerank(self,input,submit_conf,output=None,npartitions=None,max_iterations=None,tolerance=None,
                            reset_prob=None,storage_level=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('pagerank')
        if max_iterations is None:
            max_iterations = default_settings.get('max_iterations')
        if tolerance is None:
            tolerance = default_settings.get('tolerance')
        if reset_prob is None:
            reset_prob = default_settings.get('reset_prob')
        if storage_level is None:
            storage_level = default_settings.get('storage_level')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,output,npartitions,max_iterations,tolerance,reset_prob,storage_level]
        jar_directory = self.home_directory + "PageRank/target/PageRankApp-1.0.jar"
        self.submitter.submit(class_in_jar="src.main.scala.pagerankApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchpca(self,input,submit_conf,dimensions=None,scheduler_options=None,master=None):
        default_settings = get_default_settings('pca')
        if dimensions is None:
            dimensions = default_settings.get('dimensions')
        class_params = [input,dimensions]
        jar_directory = self.home_directory + "PCA/target/PCAApp-1.0.jar"
        self.submitter.submit(class_in_jar="PCA.src.main.scala.PCAApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchshortestpaths(self,input,submit_conf,output=None,npartitions=None,numv=None,
                            scheduler_options=None,master=None):
        default_settings = get_default_settings('shortestpaths')
        if numv is None:
            numv = default_settings.get('numv')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,output,npartitions,numv]
        jar_directory = self.home_directory + "ShortestPaths/target/ShortestPathsApp-1.0.jar"
        self.submitter.submit(class_in_jar="src.main.scala.ShortestPathsApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchtrianglecount(self,input,submit_conf,output=None,npartitions=None,storage_level=None,
                            scheduler_options=None,master=None):
        default_settings = get_default_settings('trianglecount')
        if storage_level is None:
            storage_level = default_settings.get('storage_level')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,output,npartitions,storage_level]
        jar_directory = self.home_directory + "TriangleCount/target/TriangleCountApp-1.0.jar"
        self.submitter.submit(class_in_jar="src.main.scala.triangleCountApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchsvdplus(self,input,submit_conf,output=None,npartitions=None,niterations=None,rank=None,
                            minval=None,maxval=None,gamma1=None,gamma2=None,gamma6=None,gamma7=None,storage_level=None,
                            scheduler_options=None,master=None):
        default_settings = get_default_settings('svd')
        if niterations is None:
            niterations = default_settings.get('niterations')
        if rank is None:
            rank = default_settings.get('rank')
        if minval is None:
            minval = default_settings.get('minval')
        if maxval is None:
            maxval = default_settings.get('maxval')
        if gamma1 is None:
            gamma1 = default_settings.get('gamma1')
        if gamma2 is None:
            gamma2 = default_settings.get('gamma2')
        if gamma6 is None:
            gamma6 = default_settings.get('gamma6')
        if gamma7 is None:
            gamma7 = default_settings.get('gamma7')
        if storage_level is None:
            storage_level = default_settings.get('storage_level')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,output,npartitions,niterations,rank,minval,maxval,gamma1,gamma2,gamma6,gamma7,storage_level]
        jar_directory = self.home_directory + "SVDPlusPlus/target/SVDPlusPlusApp-1.0.jar"
        self.submitter.submit(class_in_jar="src.main.scala.SVDPlusPlusApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchlinearregression(self,input,submit_conf,output=None,max_iterations=None,
                            scheduler_options=None,master=None):
        default_settings = get_default_settings('linearregression')
        if max_iterations is None:
            max_iterations = default_settings.get('max_iterations')
        if output is None:
            output = default_settings.get('output')
        class_params = [input,output,max_iterations]
        jar_directory = self.home_directory + "LinearRegression/target/LinearRegressionApp-1.0.jar"
        self.submitter.submit(class_in_jar="LinearRegression.src.main.java.LinearRegressionApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchlogisticregression(self,input,submit_conf,output=None,max_iterations=None,storage_level=None,
                            scheduler_options=None,master=None):
        default_settings = get_default_settings('logisticregression')
        if max_iterations is None:
            max_iterations = default_settings.get('max_iterations')
        if storage_level is None:
            storage_level = default_settings.get('storage_level')
        if output is None:
            output = default_settings.get('output')
        class_params = [input,output,max_iterations,storage_level]
        jar_directory = self.home_directory + "LogisticRegression/target/LogisticRegressionApp-1.0.jar"
        self.submitter.submit(class_in_jar="LogisticRegression.src.main.java.LogisticRegressionApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    def launchstronglyconnectedcomponent(self,input,submit_conf,output=None,npartitions=None,scheduler_options=None,
                                         master=None):
        default_settings = get_default_settings('stronglyconnected')
        if output is None:
            output = default_settings.get('output')
        if npartitions is None:
            npartitions = default_settings.get('npartitions')
        class_params = [input,output,npartitions]
        jar_directory = self.home_directory + "StronglyConnectedComponent/target/StronglyConnectedComponentApp-1.0.jar"
        self.submitter.submit(class_in_jar="src.main.scala.StronglyConnectedComponentApp",class_params=class_params,
                              jar=jar_directory, master=master,submit_conf=submit_conf,scheduler_options=scheduler_options)

    #   END: end applications suite

    #   A simple helper function that uploads the source code of this benchmark to all the frontends in g5k
    #   IMPORTANT: It needs the configuration specified in:
    #   http://execo.gforge.inria.fr/doc/latest-stable/userguide.html#use-execo-from-outside-grid5000
    #   to be able to upload the source code from your local computer to the grid5000 frontends
    def upload_to_all_g5k_sites(self):
        sites = get_g5k_sites()
        sitesg5k = [s + ".g5k" for s in sites] # we add the .g5k to be able to ssh to the frontend
        #   delete any existent spark-benchdirectories
        Remote("rm -rf $HOME/spark-bench",sitesg5k,connection_params={'user': g5k_configuration.get("g5k_user")}).run()
        #   upload the source code to all sites
        Put(sitesg5k, ["spark/spark-bench"], "~/", connection_params = {'user': g5k_configuration.get("g5k_user")}).run()

    #   A function to create all the files of the spark-bench benchmark. useful to test if everything is correct
    def create_all_bench_files(self,prefix,size,npartitions,conf):
        """
        :param conf: a configuration tuple like [["spark.executor.memory","7g"]]
        :param npartitions: the number of partitions the files wwill be split into
        :param size: The size of the files
        :param sb: an SparkBenchmark object
        :param prefix: the prefix we want to append to the file name of this round of files
        """
        self.launchgenerategraphfile(output="graph" + prefix,size=size,npartitions=npartitions,submit_conf=conf)
        self.launchgeneratedectreefile(output="dec" + prefix,size=size,npartitions=npartitions,submit_conf=conf)
        self.launchgeneratekmeansfile(output="kmeans" + prefix,size=size,npartitions=npartitions,submit_conf=conf)
        self.launchgeneratelinearregfile(output="linear" + prefix,size=size,npartitions=npartitions,submit_conf=conf)
        self.launchgeneratelogisticregfile(output="logistic" + prefix,size=size,npartitions=npartitions,submit_conf=conf)
        self.launchgeneratepcafile(output="pca" + prefix,size=size,npartitions=npartitions,submit_conf=conf)
        self.launchgeneraterddrelationalfile(output="rdd" + prefix,size=size,npartitions=npartitions,submit_conf=conf)
        self.launchgeneratesvmfile(output="svm" + prefix,size=size,npartitions=npartitions,submit_conf=conf)
        self.launchgeneratetextfile(output="words" + prefix,size=size,npartitions=npartitions,submit_conf=conf)

    #   A function to launch all of the applications in the benchmark. useful to test if everything is installed correctly
    def launch_all_apps(self,prefix, conf):
        """

        :param prefix: The prefix of the files. Normally self.prefix = create_all_bench_files.prefix
        :param conf: a configuration tuple like [["spark.executor.memory","7g"]]
        """
        self.launchconnectedcomponent(input="graph" + prefix,submit_conf=conf)
        self.launchdecisiontrees(input="dec" + prefix,submit_conf=conf)
        self.launchgrep(input="words" + prefix,submit_conf=conf)
        self.launchgroupby(submit_conf=conf)
        self.launchkmeans(input="kmeans" + prefix,submit_conf=conf)
        self.launchlinearregression(input="linear" + prefix,submit_conf=conf)
        self.launchlogisticregression(input="logistic" + prefix,submit_conf=conf )
        self.launchngrams(input="words" + prefix, submit_conf=conf)
        self.launchpagerank(input="graph" + prefix,submit_conf=conf)
        self.launchpca(input="pca" + prefix, submit_conf=conf)
        self.launchrdd(input="rdd" + prefix, submit_conf=conf)
        self.launchshortestpaths(input="graph" + prefix, submit_conf=conf)
        self.launchsort(input="words" + prefix,submit_conf=conf)
        self.launchstronglyconnectedcomponent(input="graph" + prefix, submit_conf=conf)
        self.launchsvm(input="svm" + prefix, submit_conf=conf)
        self.launchsvdplus(input="graph" + prefix, submit_conf=conf)
        self.launchwordcount(input="words" + prefix, submit_conf=conf)










