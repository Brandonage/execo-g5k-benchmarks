# execo-g5k-benchmarks
Benchmarks to use with execo and g5k. This project is meant to be used together with execo-utilities-g5k (https://github.com/Brandonage/execo-utilities-g5k)
in order to perform reproducible experiments. This Current version includes SparkBench (https://github.com/SparkTC/spark-bench) 
and YCSB (https://github.com/brianfrankcooper/YCSB).
To make it work we need to add in the in the .execo.conf.py file of our home directory the following key which will
be merged with the default execo.config.g5k_configuration

g5k_configuration = {
    'g5k_user' : 'abrandon',
    'oar_job_key_file': '/home/abrandon/.ssh/id_rsa',
}

## How to use it


### SparkBench

In your execo script, create the SparkBench object provided by this project. The parameters needed are:
1. The home_directory of a previously built SparkBench project
2. The master_node from where we will execute the execo commands that will control the SparkBench execution
3. The resource manager that we want to use to submit the job to the Spark cluster. This is needed since there is functionality
and parameters specific to each resource manager
4. The path to the spark submit folder
5. The address of the default spark master to use  

```python
from spark.sparkbench import SparkBench
sb = SparkBench(home_directory="/home/abrandon/execo-g5k-benchmarks/spark/spark-bench/", master_node='griffon-1',
                    resource_manager="yarn",root_to_spark_submit="/opt/spark/bin/spark-submit",default_master="yarn")
```


After creating the object we can start launching the different workloads. Example:

```python
sb.launchgeneratetextfile(output="words.txt", size=60,npartitions=200,submit_conf=[["spark.executor.memory","7g"]])
sb.launchngrams(input="words.txt",submit_conf=[["spark.executor.memory","4g"]])
```

Check the class in https://github.com/Brandonage/execo-g5k-benchmarks/blob/master/spark/sparkbench.py to see the different
methods provided 


### YCSB

Prerequisite: JDK 8 must be installed in all nodes

In contrast to SparkBench in which the benchmark has to be installed in advance, the YCSB class is able to install 
the benchmark directly on the desired nodes. So far only the Cassandra benchmark of YCSB can be used. 
In your execo script, create the CassandraYCSB object provided by this project. The parameteres needed are:

1. The nodes where you want to install YCSB
2. The execo connection parameters to connect to the nodes. (e.g execo.config.default_connection_params)
3. The nodes where cassandra is installed


```python
cassandra_ycsb = CassandraYCSB(install_nodes=nodes_to_install,
                                    execo_conn_params=default_connection_params, 
                                    cassandra_nodes=cassandra_nodes)
```

After creating the object we can start launching the different workloads. 

1. Prepare the workload (Check the documentation of YCSB for further info about the parameters 
https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties)

```python
cassandra_ycsb.load_workload(from_node=yscb_clients, # the nodes where YCSB are installed
                            workload='workloada',
                            recordcount='1000',
                            threadcount='1',
                            fieldlength='500')
```

2. Run the workload

```python

cassandra_ycsb.run_workload(iteration=i, # we can use this iteration counter to have several executions on a loop
                            res_dir="results/execution" + str(i), # where do we want to store the results of the execution
                            from_node=yscb_clients,
                            workload='workloada',
                            recordcount='1000',
                            threadcount='1',
                            fieldlength='500',
                            target='40')

```

#### Analysing results from YCSB

In addition, the YCSB class provides a method to analyse the result files generated in all the different hosts for the Throughput.
Use it in the following way:
```python
cassandra_ycsb.analyse_results(
                    directory=d, # the directory where it should search for
                    workload=w, # the workload
                    metric="Throughput") # the metric we want to analyse, at the moment only throughput
```

The method returns a three-tuple with all the throughput metrics registered for each host together with the mean 
and the variance for the set of throughput metrics

##Bugs or problems

Please open an issue or contact me directly and I can help you to set everything up 




