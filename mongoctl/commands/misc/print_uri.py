__author__ = 'abdul'

import mongoctl.repository as repository
from mongoctl.errors import MongoctlException
import mongoctl

###############################################################################
# print uri command
###############################################################################
def print_uri_command(parsed_options):
    id = parsed_options.id
    db = parsed_options.db
    # check if the id is a server id

    print "in print_uri_command:"
    print "\t " + str(id)

    uri = ""
    server = repository.lookup_server(id)

    if server:
        print "\t As a server"
        uri = server.get_mongo_uri_template(db=db)
        print "\t " + str(uri)
    else:
        print "\t As a cluster"
        cluster = repository.lookup_cluster(id)
        if cluster:
            uri = cluster.get_mongo_uri_template(db=db)
        else:
            print "\t Raise an error?"
            raise MongoctlException("Cannot find a server or a cluster with"
                                    " id '%s'" % id)

    if mongoctl.is_service() is False:
        print uri

    return uri
