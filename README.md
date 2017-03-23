# execo-g5k-benchmarks
Benchmarks to use with execo and g5k from a frontend. Current version only includes the sparkBenchmark in 
https://github.com/SparkTC/spark-bench.  
To make it work we need to add in the in the .execo.conf.py file of our home directory the following key which will
be merged with the default execo.config.g5k_configuration

g5k_configuration = {
    'g5k_user' : 'abrandon',
    'oar_job_key_file': '/home/abrandon/.ssh/id_rsa',
}

After that we can use it in our experiments like in https://github.com/Brandonage/execo-utilities-g5k examples
