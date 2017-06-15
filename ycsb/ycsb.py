from execo import Remote
from glob import glob
from numpy import mean
import re
from common.common_utils import check_if_file_exists

class YCSB:
    def __init__(self,install_nodes,execo_conn_params):
        """
        This is the parent class for all the YSCB benchmarks. It needs as parameters the nodes where we want to install
        it and the execo_conn_params (e.g. vagrantg5k, g5k_user, etc...)
        :type install_nodes: set
        :type execo_conn_params: dict
        """
        self.install_nodes = install_nodes
        self.execo_conn_params = execo_conn_params
        # before downloading anything let's check if the files already exists in the home directory
        not_exist = check_if_file_exists("ycsb-0.12.0.tar.gz",install_nodes,self.execo_conn_params) # a set
        if (not_exist.__len__() != 0):
            print "Downloading YSCB from Git for nodes: {0}".format(not_exist)
            Remote("curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.12.0/ycsb-0.12.0.tar.gz ycsb-0.12.0.tar.gz",
               hosts=not_exist,
               connection_params=execo_conn_params).run()
        print "Untar ycsb-0.12.0.tar.gz"
        Remote("tar xfvz ycsb-0.12.0.tar.gz",
               hosts=install_nodes,
               connection_params=execo_conn_params).run()
        print "YSCB installed in nodes: {0}".format(install_nodes)
        print "Remember to prepare the DB before running any workload. More info on https://github.com/brianfrankcooper/YCSB"

    def load_workload(self,from_node, workload, recordcount, insertcount, insertstart,threadcount=1):
        """
        Run a workload from the core workloads on YCSB
        :param from_node: from which node you want to run the workload. This is a set variable
        :param workload: the type of workload e.g. workloada, workloadb, workloadc ...
        :param recordcount: the total number of records to insert
        :param insertcount: the number of records to insert with this execution
        :param insertstart: from where to start inserting. Useful if we have different clients and each one will
        insert in different ranges e.g.[client1:0-10000,client2:10000-20000,...]
        :param threadcount: the number of threads for each client. Increase this to increase the load on the system
        """
        raise NotImplementedError


    def run_workload(self,from_node, workload, threadcount=1):
        """
        Run a workload from the core workloads on YCSB
        :param from_node: from which node you want to run the workload. This is a set variable
        :param workload: the type of workload e.g. workloada, workloadb, workloadc ...
        :param threadcount: the number of threads for each client. Increase this to increase the load on the system
        """
        raise NotImplementedError

    @staticmethod
    def analyse_results(directory, workload, metric):
        """
        This function is going to analyse the output files from the execution of a YSCB benchmark for a given workload
        and extract a summary statistic from one metric. Returns a list with all the metrics for the different iterations
        and the mean
        :rtype: list, float
        :param directory: the directory where it should search for
        :param workload: the workload
        :param metric: the metric we want to analyse e.g. throughput
        """
        pattern = r'(\[OVERALL\], Throughput\(ops/sec\), )(\d+\.\d+)'
        regex = re.compile(pattern)
        list_of_metrics = []
        # Remember that the names of the files are going to be output_" + workload + "_{{{host}}}_it" + str(iteration)
        for filename in glob(directory + '/*' + workload + '*'):
            with open(filename, 'r') as file:
                for line in file:
                    line = line.rstrip()
                    result = regex.match(line)
                    if result is not None:
                        throughput = float(result.group(2))
                        list_of_metrics.append(throughput)
        metrics_mean = mean(list_of_metrics)
        return list_of_metrics, metrics_mean

if __name__ == '__main__':
    directory='/Users/alvarobrandon/execo_experiments/dcosvagrant__06_Jun_2017_14:42/no_fmone'
    workload='workloada'
    metric='throughput'
    pattern = r'(\[OVERALL\], Throughput\(ops/sec\), )(\d+\.\d+)'
    regex = re.compile(pattern)
    list_of_metrics = []
    for filename in glob(directory + '/*' + workload + '*'):
        with open(filename, 'r') as file:
            for line in file:
                print line
                line = line.rstrip()
                result = regex.match(line)
                if result is not None:
                    throughput = float(result.group(2))
                    list_of_metrics.append(throughput)
    result = mean(list_of_metrics)
    print result



