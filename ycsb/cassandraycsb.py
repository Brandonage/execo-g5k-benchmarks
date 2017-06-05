from ycsb import YCSB
from execo import Remote
import sys

class CassandraYCSB(YCSB):

    def __init__(self,install_nodes,execo_conn_params,cassandra_nodes):
        """

        :param install_nodes: a set containing the nodes where ycsb will be installed 
        :param execo_conn_params: a dictionary including the connection parameters for execo
        :param cassandra_nodes: a set containing the nodes that constitute the cassandra cluster
        """
        YCSB.__init__(self,install_nodes,execo_conn_params)
        self.cassandra_nodes = cassandra_nodes

    def load_workload(self,from_node, workload, recordcount, insertcount, insertstart,threadcount=1):
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
        # We load the data into the cassandra database
        Remote("ycsb-0.12.0/bin/ycsb.sh load cassandra-cql -P ycsb-0.12.0/workloads/{0} -p hosts={1} -p recordcount={2} -p insertstart={3} -p insertcount={4} -p threadcount={5}"
               .format(workload,cassandra_nodes_str,recordcount,insertstart,insertcount,threadcount),
               hosts=from_node,
               connection_params=self.execo_conn_params,
               process_args={'stdout_handlers': [sys.stdout], 'stderr_handlers': [sys.stderr]}).run()

    def run_workload(self,from_node, workload, threadcount=1):
        # we transform the set to a str with the format 'node1,node2,node3...'
        cassandra_nodes_str = ','.join(list(self.cassandra_nodes))
        # We run the workload
        Remote("ycsb-0.12.0/bin/ycsb.sh run cassandra-cql -P ycsb-0.12.0/workloads/{0} -p hosts={1} -p threadcount={2}"
               .format(workload,cassandra_nodes_str,threadcount),
               hosts=from_node,
               connection_params=self.execo_conn_params,
               process_args={'stdout_handlers': [sys.stdout], 'stderr_handlers': [sys.stderr]}).run()