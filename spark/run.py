
from sparkbench import SparkBench
from spark.sparksubmitterfactory import SparkSubmitterFactory

#benchmark = SparkBench(home_directory="/home/abrandon/spark-bench", master_node="master_machine", resource_manager="yarn",
#                       root_to_spark_submit="/opt/spark/bin/spark_submit", default_master="yarn")

if __name__ == '__main__':
    factory = SparkSubmitterFactory()
    submit = factory.get_spark_submitter("yarn","griffon-1","yarn","/opt/spark/bin/spark-submit")
    submit.submit("src.Worcount.org",["tocho","neumonia","caraperro"],"Bencmark-1.0-SNAPSHOT.jar","yarn",
                  [["spark.executor.memory","2g"],["spark.executor.cores","1"]],"")



