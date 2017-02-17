### This function is put into a different

def get_default_settings(app_name):
    # We will put here all the defaut settings for the different apps we have.
    # We won't include the input file parameter of course as we want it to be chosen by the user
    dict_of_settings = {
        'decisiontree' : {'output':'DecisionTreesOutput','nclasses':'10','impurity':'gini','max_depth':'5','max_bins':'100','mode':'Classification'},
        'pagerank' : {'output':'PageRankOutput','npartitions':'0','max_iterations':'5','tolerance':'0.001','reset_prob':'0.15','storage_level':'MEMORY_AND_DISK'},
        'pca' : {'dimensions':'50'},
        'shortestpaths' : {'output':'ShortestPathsOutput','npartitions':'0','numv':'10000'},
        'trianglecount' : {'output':'TriangleCountOutput','npartitions':'0','storage_level': 'MEMORY_AND_DISK'},
        'svd' : {'output':'SVDOutput','npartitions':'0','niterations':'2','rank':'50','minval':'0.0','maxval':'5.0',
                 'gamma1':'0.007','gamma2':'0.007','gamma6':'0.005','gamma7':'0.015','storage_level':'MEMORY_AND_DISK'},
        'grep' : {'keyword':'pneumonalgia','output': "grepOutput.txt",'npartitions':"0"},
        'wordcount' : {'output':"WordCountOutput.txt",'npartitions':"0"},
        'sort' : {'output':"SortOutput.txt",'npartitions':"0"},
        'connnectedcomponent' : {'output':'ConnectedComponentOutput','npartitions':'0','useless':'0'},
        'svm' : {'niterations':'3','npartitions':'0'},
        'kmeans' : {'output':'kMeansBenchOutput','nclusters':'10','max_iterations':'4','num_run':'1'},
        'linearregression' : {'output':'LinearRegressionOutput','max_iterations':'5'},
        'logisticregression' : {'output':'LogisticOutput','max_iterations':'5','storage_level':'MEMORY_AND_DISK'},
        'stronglyconnected' : {'output':'ConnectedComponentOutput','npartitions':'0'},
        'groupby' : {'num_kvpairs':'15600000','value_size':'10000','npartitions':'168'},
        'rddrelational' : {'output':"RDDOutput.txt"},
        'ngrams' : {'output':"NgramsOutput.txt"},
        'generatetext' : {'output':'words.txt','npoints':'10000','npartitions':'30'},
        'generatesvm' : {'output':'svm.txt','npoints':'10000','nfeatures':'10','npartitions':'30'},
        'generatekmeans' : {'output':'kmeans.txt','npoints':'10000','nclusters':'10','ndimensions':'20','scaling':'0.6','npartitions':'30'},
        'generatelogisticreg' : {'output':'logistic.txt','npoints':'10000','nfeatures':'20','eps':'0.5','probone':'0.2','npartitions':'30'},
        'generatelinearreg' : {'output':'linear.txt','npoints':'10000','nfeatures':'20','eps':'0.5','probone':'0.1','npartitions':'30'},
        'generatepca' : {'output':'pca.txt','npoints':'10000','nfeatures':'1000','npartitions':'30'},
        'generatedectree' : {'output':'dec.txt','npoints':'10000','nfeatures':'6','npartitions':'30'},
        'generateRDD' : {'output':'rdd.txt','npoints':'10000','npartitions':'30'},
        'generategraph' :  {'output':'graph.txt','npoints':'10000','npartitions':'30','mu':'4.0','sigma':'1.3'}
    }
    return dict_of_settings.get(app_name)