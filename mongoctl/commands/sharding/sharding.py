__author__ = 'abdul'

import mongoctl.repository as repository
from mongoctl.utils import document_pretty_string
from mongoctl.mongoctl_logging import log_info, log_error

from mongoctl.objects.sharded_cluster import ShardedCluster
from mongoctl.objects.replicaset_cluster import ReplicaSetCluster

from mongoctl.errors import MongoctlException

from mongoctl.commands.server.start import start_server
from mongoctl.commands.server.stop import stop_server


###############################################################################
# configure shard cluster command
###############################################################################
def configure_shard_cluster_command(parsed_options):
    cluster_id = parsed_options.cluster
    cluster = repository.lookup_and_validate_cluster(cluster_id)

    if not isinstance(cluster, ShardedCluster):
        raise MongoctlException("Cluster '%s' is not a shardset cluster" %
                                cluster.id)

    if parsed_options.dryRun:
        dry_run_configure_shard_cluster(cluster)
    else:
        configure_shard_cluster(cluster)

###############################################################################
# ShardedCluster Methods
###############################################################################

def configure_shard_cluster(cluster):
    cluster.configure_shardset()

###############################################################################
def dry_run_configure_shard_cluster(cluster):

    log_info("\n************ Dry Run ************\n")

    db_command = cluster.get_shardset_configure_command()

    log_info("Executing the following command")
    log_info(document_pretty_string(db_command))

###############################################################################
# Add shard command
###############################################################################
def add_shard_command(parsed_options):
    shard_id = parsed_options.shardId
    _do_add_shard_command(shard_id, parsed_options.dryRun)

def _do_add_shard_command(shard_id, dry_run=False):
    # determine if the shard is a replicaset cluster or a server
    shard = repository.lookup_cluster(shard_id)

    if not shard:
        shard = repository.lookup_server(shard_id)

    if not shard:
        raise MongoctlException("Unknown shard '%s'" % shard_id)

    # We obtain which cluster this shard belongs to
    sharded_cluster = repository.lookup_cluster_by_shard(shard)

    if not sharded_cluster:
        raise MongoctlException("'%s' is not a shard" % shard_id)

    if dry_run:
        dry_run_add_shard(shard, sharded_cluster)
    else:
        sharded_cluster.add_shard(shard)

###############################################################################
def dry_run_add_shard(shard, sharded_cluster):
    log_info("\n************ Dry Run ************\n")

    shard_member = sharded_cluster.get_shard_member(shard)
    db_command = sharded_cluster.get_add_shard_command(shard_member)

    log_info("Executing the following command")
    log_info(document_pretty_string(db_command))

###############################################################################
# Remove shard command
###############################################################################
def remove_shard_command(parsed_options):
    shard_id = parsed_options.shardId

    # determine if the shard is a replicaset cluster or a server
    shard = repository.lookup_cluster(shard_id)

    if not shard:
        shard = repository.lookup_server(shard_id)

    if not shard:
        raise MongoctlException("Unknown shard '%s'" % shard_id)

    sharded_cluster = repository.lookup_cluster_by_shard(shard)

    if not sharded_cluster:
        raise MongoctlException("'%s' is not a shard" % shard_id)


    dest = getattr(parsed_options, "unshardedDataDestination")
    synchronized = getattr(parsed_options, "synchronized")

    if parsed_options.dryRun:
        dry_run_remove_shard(shard, sharded_cluster)
    else:
        sharded_cluster.remove_shard(shard,
                                      unsharded_data_dest_id=dest,
                                      synchronized=synchronized)



###############################################################################
def dry_run_remove_shard(shard, sharded_cluster):
    log_info("\n************ Dry Run ************\n")

    shard_member = sharded_cluster.get_shard_member(shard)
    db_command = sharded_cluster.get_validate_remove_shard_command(
        shard_member)

    log_info("Executing the following command")
    log_info(document_pretty_string(db_command))