from execo import Remote


def check_if_file_exists(file_name, nodes, connection_params):
    """
    Check if a file exists in a set of nodes. It returns the set of nodes that DON'T have the files
    :param file_name: 
    :param nodes: 
    :param connection_params: 
    :return: 
    """
    r = Remote(cmd="test -e " + file_name,
               hosts=nodes,
               connection_params=connection_params,
               process_args={"nolog_exit_code": True}
               )
    r.run()
    not_ok = filter(lambda p: p.ok is not True, r.processes)
    return set([node.host.address for node in not_ok])
