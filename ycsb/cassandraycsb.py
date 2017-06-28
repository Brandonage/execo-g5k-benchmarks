from ycsb import YCSB
from execo import Remote
import sys


class CassandraYCSB(YCSB):
    def __init__(self, install_nodes, execo_conn_params, cassandra_nodes):
        """

        :param install_nodes: a set containing the nodes where ycsb will be installed 
        :param execo_conn_params: a dictionary including the connection parameters for execo
        :param cassandra_nodes: a set containing the nodes that constitute the cassandra cluster
        """
        YCSB.__init__(self, install_nodes, execo_conn_params)
        self.cassandra_nodes = cassandra_nodes

    def load_workload(self, from_node, workload, recordcount, threadcount,fieldlength):
        """
        Run a workload from the core workloads with the CassandraDB db wrapper
        :param from_node: from which node you want to run the workload. This is a set variable
        :param workload: the type of workload e.g. workloada, workloadb, workloadc ...
        :param recordcount: the total number of records to insert
        :param insertcount: the number of records to insert with this execution
        :param insertstart: from where to start inserting. Useful if we have different clients and each one will
        insert in different ranges e.g.[client1:0-10000,client2:10000-20000,...]
        :param threadcount: the number of threads for each client. Increase this to increase the load on the system
        """
        # we transform the set to a str with the format 'node1,node2,node3...'
        cassandra_nodes_str = ','.join(list(self.cassandra_nodes))
        # divide the number of records equally among clients
        insertcount = int(recordcount) / from_node.__len__()
        insertstart_steps = list(range(from_node.__len__()))
        insertstart_values = map(lambda x: x * insertcount, insertstart_steps)
        # We load the data into the cassandra database
        # This could be changed to an execo generator expression like insertstart={{[x for x in insertstart_values]}}
        #"ycsb-0.12.0/bin/ycsb.sh load cassandra-cql -P ycsb-0.12.0/workloads/" + workload + " -p hosts=" + cassandra_nodes_str + " -p recordcount=" + recordcount + " -p insertstart={{[x for x in insertstart_values]}} -p insertcount=" + str(insertcount) + " -p threadcount=" + threadcount + " -p fieldlength=" + str(fieldlength)
        Remote(
            "ycsb-0.12.0/bin/ycsb.sh load cassandra-cql -P ycsb-0.12.0/workloads/" + workload + " -p hosts=" + cassandra_nodes_str + " -p recordcount=" + recordcount + " -p insertstart={{[x for x in insertstart_values]}} -p insertcount=" + str(insertcount) + " -p threadcount=" + threadcount + " -p fieldlength=" + str(fieldlength),
            hosts=from_node,
            connection_params=self.execo_conn_params,
            process_args={'stdout_handlers': [sys.stdout], 'stderr_handlers': [sys.stderr]}).run(timeout=900)

    def run_workload(self, iteration, res_dir, from_node, workload, recordcount, threadcount, fieldlength, target):
        # we transform the set to a str with the format 'node1,node2,node3...'
        """
        Run a given workload. it receives an iteration parameter to be able to repeat the workload several times
        :param iteration: the iteration number. Will be appended to the name of the file with the output
        :param from_node: from which nodes should we execute the benchmark
        :param workload: the type of workload   
        :param threadcount: the number of threads
        """
        cassandra_nodes_str = ','.join(list(self.cassandra_nodes))
        # We run the workload
        Remote(
            "ycsb-0.12.0/bin/ycsb.sh run cassandra-cql -P ycsb-0.12.0/workloads/" + workload + " -p hosts=" + cassandra_nodes_str + " -p recordcount=" + recordcount + " -p threadcount=" + str(threadcount) + " -p fieldlength=" + str(fieldlength) + " -p exportfile=" + res_dir + "/output_" + workload + "_{{{host}}}_it" + str(iteration),
            hosts=from_node,
            connection_params=self.execo_conn_params,
            process_args={'stdout_handlers': [sys.stdout], 'stderr_handlers': [sys.stderr]}).run(timeout=900)
