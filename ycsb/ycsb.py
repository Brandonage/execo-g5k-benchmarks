from execo import Remote


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
        print "Downloading YSCB from Git"
        Remote("curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.12.0/ycsb-0.12.0.tar.gz",
               hosts=install_nodes,
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


