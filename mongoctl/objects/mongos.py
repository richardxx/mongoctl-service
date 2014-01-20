__author__ = 'abdul'

from mongoctl.utils import resolve_path
import server

###############################################################################
# CONSTANTS
###############################################################################


###############################################################################
# MongosServer Class
###############################################################################

class MongosServer(server.Server):

    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, server_doc):
        super(MongosServer, self).__init__(server_doc)

    ###########################################################################
    def export_cmd_options(self, options_override=None):
        """
            Override!
        :return:
        """
        cmd_options = super(MongosServer, self).export_cmd_options(
            options_override=options_override)

        # Add configServers arg
        cluster = self.get_validate_cluster()
        config_addresses = ",".join(cluster.get_config_member_addresses())
        cmd_options["configdb"] = config_addresses

        return cmd_options

    ###########################################################################
    # Properties
    ###########################################################################
    def get_db_path(self):
        dbpath = self.get_cmd_option("dbpath")
        if not dbpath:
            dbpath = super(MongosServer, self).get_server_home()
        if not dbpath:
            dbpath = server.DEFAULT_DBPATH

        return resolve_path(dbpath)


    def get_server_home(self):
        """
            Override!
        :return:
        """
        home_dir = super(MongosServer, self).get_server_home()
        if not home_dir:
            home_dir = self.get_db_path()

        return home_dir
