__author__ = 'richardxx'


import mongoctl.repository as repository
from mongoctl.utils import document_pretty_string
from mongoctl.mongoctl_logging import log_info, log_error

from mongoctl.objects.sharded_cluster import ShardedCluster
from mongoctl.objects.replicaset_cluster import ReplicaSetCluster

from mongoctl.errors import MongoctlException

from mongoctl.commands.server.start import start_server
from mongoctl.commands.server.stop import stop_server


###############################################################################
# start/stop sharding/replicaset cluster command
###############################################################################
def start_cluster_command(parsed_options):
    cluster_id = parsed_options["ClusterId"]
    cluster = repository.lookup_and_validate_cluster(cluster_id)

    if isinstance(cluster, ShardedCluster):
        start_shard_cluster(cluster)
    else:
        start_replicaset_cluster(cluster)


def stop_cluster_command(parsed_options):
    cluster_id = parsed_options["ClusterId"]
    cluster = repository.lookup_and_validate_cluster(cluster_id)

    if isinstance(cluster, ShardedCluster):
        stop_shard_cluster(cluster)
    else:
        stop_replicaset_cluster(cluster)


def start_shard_cluster(cluster):
    #List of list of required servers
    list_of_member_list = [cluster.shards, cluster.config_members, cluster.mongos_members]
    #Record the list of started members
    started_members = []

    # We first start the servers
    for member_list in list_of_member_list:
        for server_descriptor in member_list:
            shard = server_descriptor.get_shard()

            try:
                if not isinstance(shard, ReplicaSetCluster):
                    if not shard.is_online():
                        # We set the server working in background compulsorily
                        shard.set_cmd_option("fork", True)
                        start_server(shard)
                else:
                    # TODO: Instructions for starting a replicaset cluster
                    pass

                started_members.append(shard)
            except MongoctlException, e:
                shutdown_severs(started_members)
                log_error(e.message)
                return False

    # We add the shard servers to the sharding configuration
    for server_descriptor in cluster.shards:
        shard = server_descriptor.get_server()
        if not cluster.is_shard_configured(shard):
            # It is never added to this sharding cluster
            cluster.add_shard(shard)

    return True


def start_replicaset_cluster(cluster):
    pass


def stop_shard_cluster(cluster, username=None, password=None):
    """
    We only stop the mongos servers.
    The shards participants may serve queries independently.
    @param cluster:
    @return:
    """
    list_of_server_list = [cluster.mongos_members()]
    started_servers = []

    for server_list in list_of_server_list:
        for server_descriptor in server_list:
            shard = server_descriptor.get_shard()
            started_servers.append(shard)

    shutdown_severs(started_servers)


def stop_replicaset_cluster():
    pass

def shutdown_severs(started_members):
    """
    Close the sharding members given in the list
    @param started_members: A list of correctly opened sharding members
    @return:
    """
    for shard in started_members:
        try:
            if not isinstance(shard, ReplicaSetCluster):
                stop_server(shard)
            else:
                pass

        except MongoctlException, e:
            log_error(e.message)

