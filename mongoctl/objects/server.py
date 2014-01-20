__author__ = 'abdul'

import os

import mongoctl.repository as repository

from base import DocumentWrapper
from mongoctl.utils import resolve_path, document_pretty_string, is_host_local
from pymongo.errors import AutoReconnect
from mongoctl.mongoctl_logging import (
    log_verbose, log_error, log_warning, log_exception, log_debug
    )
from mongoctl.mongo_version import version_obj

from mongoctl.config import get_default_users
from mongoctl.errors import MongoctlException
from mongoctl.prompt import read_username, read_password

from bson.son import SON

from pymongo.connection import Connection

import datetime

from mongoctl import config
from mongoctl import users

###############################################################################
# CONSTANTS
###############################################################################

# This is mongodb's default dbpath
DEFAULT_DBPATH = '/data/db'

# default pid file name
PID_FILE_NAME = "pid.txt"

LOG_FILE_NAME = "mongodb.log"

KEY_FILE_NAME = "keyFile"

# This is mongodb's default port
DEFAULT_PORT = 27017

# db connection timeout, 10 seconds
CONN_TIMEOUT = 10000

REPL_KEY_SUPPORTED_VERSION = '2.0.0'

###############################################################################
# Server Class
###############################################################################

class Server(DocumentWrapper):
    ###########################################################################
    # Constructor
    ###########################################################################
    def __init__(self, server_doc):
        DocumentWrapper.__init__(self, server_doc)
        self.__db_connection__ = None
        self.__seed_users__ = None
        self.__login_users__ = {}
        self.__mongo_version__ = None
        self._connection_address = None

    ###########################################################################
    # Properties
    ###########################################################################

    ###########################################################################
    def get_description(self):
        return self.get_property("description")

    ###########################################################################
    def set_description(self, desc):
        return self.set_property("description", desc)

    ###########################################################################
    def get_server_home(self):
        home_dir = self.get_property("serverHome")
        if home_dir:
            return resolve_path(home_dir)
        else:
            return None

    ###########################################################################
    def set_server_home(self, val):
        self.set_property("serverHome", val)

    ###########################################################################
    def get_pid_file_path(self):
        return self.get_server_file_path("pidfilepath", PID_FILE_NAME)

    ###########################################################################
    def get_log_file_path(self):
        return self.get_server_file_path("logpath", LOG_FILE_NAME)

    ###########################################################################
    def get_key_file(self):
        kf = self.get_cmd_option("keyFile")
        if kf:
            return resolve_path(kf)

    ###########################################################################
    def get_default_key_file_path(self):
        return self.get_server_file_path("keyFile", KEY_FILE_NAME)


    ###########################################################################
    def get_server_file_path(self, cmd_prop, default_file_name):
        file_path = self.get_cmd_option(cmd_prop)
        if file_path is not None:
            return resolve_path(file_path)
        else:
            return self.get_default_file_path(default_file_name)

    ###########################################################################
    def get_default_file_path(self, file_name):
        return self.get_server_home() + os.path.sep + file_name

    ###########################################################################
    def get_address(self):
        """
        @return: the address set in the server descriptor manually.
        If no address is given, NOTE THAT this function returns None.
        """
        address = self.get_property("address")

        if address is not None:
            if address.find(":") > 0:
                return address
            else:
                return "%s:%s" % (address, self.get_port())
        else:
            return None

    ###########################################################################
    def get_address_display(self):
        """
        @return: A synthesized usable address for this server.
        Always call this function if you want to connect to a server.
        """
        display = self.get_address()
        if display is None:
            display = self.get_local_address()
        return display

    ###########################################################################
    def get_host_address(self):
        if self.get_address() is not None:
            return self.get_address().split(":")[0]
        else:
            return None

    ###########################################################################
    def get_connection_host_address(self):
        return self.get_connection_address().split(":")[0]

    ###########################################################################
    def set_address(self, address):
        self.set_property("address", address)

    ###########################################################################
    def get_local_address(self):
        return "localhost:%s" % self.get_port()

    def is_local_address(self):
        address = self.get_address()
        if address is None or "localhost" in address:
            return True
        return False

    ###########################################################################
    def get_port(self):
        port = self.get_cmd_option("port")
        if port is None:
            port = DEFAULT_PORT
        return port

    ###########################################################################
    def set_port(self, port):
        self.set_cmd_option("port", port)

    ###########################################################################
    def is_fork(self):
        """
            @return: true if the server process is running in background as a deamon
        """
        fork = self.get_cmd_option("fork")
        return fork or fork is None

    ###########################################################################
    def get_mongo_version(self):
        """
        Gets mongo version of the server if it is running. Otherwise return
         version configured in mongoVersion property
        """
        if self.__mongo_version__:
            return self.__mongo_version__

        if self.is_online():
            mongo_version = self.get_db_connection().server_info()['version']
        else:
            mongo_version = self.get_property("mongoVersion")

        self.__mongo_version__ = mongo_version
        return self.__mongo_version__

    ###########################################################################
    def get_mongo_version_obj(self):
        version_str = self.get_mongo_version()
        if version_str is not None:
            return version_obj(version_str)
        else:
            return None

    ###########################################################################
    def get_cmd_option(self, option_name):
        cmd_options = self.get_cmd_options()

        if cmd_options and cmd_options.has_key(option_name):
            return cmd_options[option_name]
        else:
            return None

    ###########################################################################
    def set_cmd_option(self, option_name, option_value):
        cmd_options = self.get_cmd_options()

        if cmd_options:
            cmd_options[option_name] = option_value

    ###########################################################################
    def get_cmd_options(self):
        return self.get_property('cmdOptions')

    ###########################################################################
    def set_cmd_options(self, cmd_options):
        return self.set_property('cmdOptions', cmd_options)

    ###########################################################################
    def export_cmd_options(self, options_override=None):
        cmd_options = self.get_cmd_options().copy()
        # reset some props to exporting vals
        cmd_options['pidfilepath'] = self.get_pid_file_path()

        # apply the options override
        if options_override is not None:
            for (option_name, option_val) in options_override.items():
                cmd_options[option_name] = option_val

        # set the logpath if forking..

        if (self.is_fork() or (options_override is not None and
                                   options_override.get("fork"))):
            cmd_options['fork'] = True
            if "logpath" not in cmd_options:
                cmd_options["logpath"] = self.get_log_file_path()

        # Specify the keyFile arg if needed
        if self.needs_repl_key() and "keyFile" not in cmd_options:
            key_file_path = (self.get_key_file() or
                             self.get_default_key_file_path())
            cmd_options["keyFile"] = key_file_path
        return cmd_options

    ###########################################################################
    def get_seed_users(self):

        if self.__seed_users__ is None:
            seed_users = self.get_property('seedUsers')

            ## This hidden for internal user and should not be documented
            if not seed_users:
                seed_users = get_default_users()

            self.__seed_users__ = seed_users

        return self.__seed_users__

    ###########################################################################
    def get_login_user(self, dbname):
        login_user = self.__login_users__.get(dbname)
        # if no login user found then check global login

        if not login_user:
            login_user = users.get_global_login_user(self, dbname)

        # if dbname is local and we cant find anything yet
        # THEN assume that local credentials == admin credentials
        if not login_user and dbname == "local":
            login_user = self.get_login_user("admin")

        return login_user

    ###########################################################################
    def lookup_password(self, dbname, username):
        # look in seed users
        db_seed_users = self.get_db_seed_users(dbname)
        if db_seed_users:
            user = filter(lambda user: user['username'] == username,
                          db_seed_users)
            if user and "password" in user[0]:
                return user[0]["password"]

    ###########################################################################
    def set_login_user(self, dbname, username, password):
        self.__login_users__[dbname] = {
            "username": username,
            "password": password
        }

    ###########################################################################
    def get_admin_users(self):
        return self.get_db_seed_users("admin")

    ###########################################################################
    def get_db_seed_users(self, dbname):
        return self.get_seed_users().get(dbname)

    ###########################################################################
    def get_cluster(self):
        return repository.lookup_cluster_by_server(self)

    ###########################################################################
    def get_validate_cluster(self):
        cluster = repository.lookup_cluster_by_server(self)
        if not cluster:
            raise MongoctlException("No cluster found for server '%s'" %
                                    self.id)
        repository.validate_cluster(cluster)
        return cluster

    ###########################################################################
    def is_cluster_member(self):
        return self.get_cluster() is not None

    ###########################################################################
    # DB Methods
    ###########################################################################

    def disconnecting_db_command(self, cmd, dbname):
        try:
            result = self.db_command(cmd, dbname)
            return result
        except AutoReconnect, e:
            log_verbose("This is an expected exception that happens after "
                        "disconnecting db commands: %s" % e)
        finally:
            self.__db_connection__ = None

    ###########################################################################
    def timeout_maybe_db_command(self, cmd, dbname):
        try:
            result = self.db_command(cmd, dbname)
            return result
        except Exception, e:
            log_exception(e)
            if "timed out" in str(e):
                log_warning("Command %s is taking a while to complete. "
                            "This is not necessarily bad. " %
                            document_pretty_string(cmd))
            else:
                raise
        finally:
            self.__db_connection__ = None

    ###########################################################################
    def db_command(self, cmd, dbname, **kwargs):

        need_auth = self.command_needs_auth(dbname, cmd)
        db = self.get_db(dbname, no_auth=not need_auth)

        if cmd.has_key("addShard"):
            shard_given_name = kwargs.get("name", "")
            return db.command("addShard", cmd.get('addShard'), name=shard_given_name)

        return db.command(cmd, **kwargs)

    ###########################################################################
    def command_needs_auth(self, dbname, cmd):
        return self.needs_to_auth(dbname)

    ###########################################################################
    def get_db(self, dbname, no_auth=False, username=None, password=None,
               retry=True, never_auth_with_admin=False):

        conn = self.get_db_connection()
        db = conn[dbname]

        # If the DB doesn't need to be authenticated to (or at least yet)
        # then don't authenticate. this piece of code is important for the case
        # where you are connecting to the DB on local host where --auth is on
        # but there are no admin users yet
        if no_auth:
            return db

        if (not username and
                (not self.needs_to_auth(dbname))):
            return db

        if username:
            self.set_login_user(dbname, username, password)

        login_user = self.get_login_user(dbname)

        # if there is no login user for this database then use admin db unless
        # it was specified not to
        if (not never_auth_with_admin and
                not login_user and
                    dbname not in ["admin", "local"]):
            # if this passes then we are authed!
            admin_db = self.get_db("admin", retry=retry)
            return admin_db.connection[dbname]

        auth_success = self.authenticate_db(db, dbname, retry=retry)

        # If auth failed then give it a try by auth into admin db unless it
        # was specified not to
        if (not never_auth_with_admin and
                not auth_success
            and dbname != "admin"):
            admin_db = self.get_db("admin", retry=retry)
            return admin_db.connection[dbname]

        if auth_success:
            return db
        else:
            raise MongoctlException("Failed to authenticate to %s db" % dbname)

    ###########################################################################
    def authenticate_db(self, db, dbname, retry=True):
        """
        Returns True if we manage to auth to the given db, else False.
        """
        login_user = self.get_login_user(dbname)
        username = None
        password = None

        auth_success = False

        if login_user:
            username = login_user["username"]
            if "password" in login_user:
                password = login_user["password"]

        # have three attempts to authenticate
        no_tries = 0

        while not auth_success and no_tries < 3:
            if not username:
                username = read_username(dbname)
            if not password:
                password = self.lookup_password(dbname, username)
                if not password:
                    password = read_password("Enter password for user '%s\%s'" %
                                             (dbname, username))

            # if auth success then exit loop and memoize login
            auth_success = db.authenticate(username, password)
            if auth_success or not retry:
                break
            else:
                log_error("Invalid login!")
                username = None
                password = None

            no_tries += 1

        if auth_success:
            self.set_login_user(dbname, username, password)

        return auth_success

    ###########################################################################
    def get_working_login(self, database, username=None, password=None):
        """
            authenticate to the specified database starting with specified
            username/password (if present), try to return a successful login
            within 3 attempts
        """
        #  this will authenticate and update login user
        self.get_db(database, username=username, password=password,
                    never_auth_with_admin=True)

        login_user = self.get_login_user(database)

        if login_user:
            username = login_user["username"]
            password = (login_user["password"] if "password" in login_user
                        else None)

        return username, password

    ###########################################################################
    def is_online(self):
        try:
            self.new_db_connection()
            return True
        except Exception, e:
            log_exception(e)
            return False

    ###########################################################################
    def can_function(self):
        status = self.get_status()
        if status['connection']:
            if 'error' not in status:
                return True
            else:
                log_verbose("Error while connecting to server '%s': %s " %
                            (self.id, status['error']))

    ###########################################################################
    def is_online_locally(self):
        return self.is_use_local() and self.is_online()

    ###########################################################################
    def is_use_local(self):
        return (self.get_address() is None or
                is_assumed_local_server(self.id)
                or self.is_local())

    ###########################################################################
    def is_local(self):
        try:
            server_host = self.get_host_address()
            return server_host is None or is_host_local(server_host)
        except Exception, e:
            log_exception(e)
            log_error("Unable to resolve address '%s' for server '%s'."
                      " Cause: %s" %
                      (self.get_host_address(), self.id, e))
        return False

    ###########################################################################
    def needs_to_auth(self, dbname):
        """
        Determines if the server needs to authenticate to the database.
        NOTE: we stopped depending on is_auth() since its only a configuration
        and may not be accurate
        """
        log_debug("Checking if server '%s' needs to auth on  db '%s'...." %
                  (self.id, dbname))
        try:
            conn = self.new_db_connection()
            db = conn[dbname]
            db.collection_names()
            result = False
        except (RuntimeError, Exception), e:
            log_exception(e)
            result = "authorized" in str(e)

        log_debug("needs_to_auth check for server '%s'  on db '%s' : %s" %
                  (self.id, dbname, result))
        return result

    ###########################################################################
    def get_status(self, admin=False):
        status = {}
        ## check if the server is online
        try:
            self.get_db_connection()
            status['connection'] = True

            # grab status summary if it was specified + if i am not an arbiter
            if admin:
                server_summary = self.get_server_status_summary()
                status["serverStatusSummary"] = server_summary

        except (RuntimeError, Exception), e:
            log_exception(e)
            self.sever_db_connection()   # better luck next time!
            status['connection'] = False
            status['error'] = "%s" % e
            if "timed out" in status['error']:
                status['timedOut'] = True

        return status

    ###########################################################################
    def get_server_status_summary(self):
        server_status = self.db_command(SON([('serverStatus', 1)]), "admin")
        server_summary = {
            "host": server_status['host'],
            "connections": server_status['connections'],
            "version": server_status['version']
        }
        return server_summary

    ###########################################################################
    def get_db_connection(self):
        if self.__db_connection__ is None:
            self.__db_connection__ = self.new_db_connection()
        return self.__db_connection__

    ###########################################################################
    def sever_db_connection(self):
        if self.__db_connection__ is not None:
            self.__db_connection__.close()
        self.__db_connection__ = None

    ###########################################################################
    def new_db_connection(self):
        return make_db_connection(self.get_connection_address())

    ###########################################################################
    def get_connection_address(self):

        if self._connection_address:
            return self._connection_address

        # try to get the first working connection address
        if (self.is_use_local() and
                self.has_connectivity_on(self.get_local_address())):
            self._connection_address = self.get_local_address()
        elif self.has_connectivity_on(self.get_address()):
            self._connection_address = self.get_address()

        # use old logic
        if not self._connection_address:
            if self.is_use_local():
                self._connection_address = self.get_local_address()
            else:
                self._connection_address = self.get_address()

        return self._connection_address

    ###########################################################################
    ###########################################################################
    def has_connectivity_on(self, address):

        try:
            log_verbose("Checking if server '%s' is accessible on "
                        "address '%s'" % (self.id, address))
            make_db_connection(address)
            return True
        except Exception, e:
            log_exception(e)
            log_verbose("Check failed for server '%s' is accessible on "
                        "address '%s': %s" % (self.id, address, e))
            return False

    ###########################################################################
    def get_rs_config(self):
        try:
            return self.get_db('local')['system.replset'].find_one()
        except (Exception, RuntimeError), e:
            log_exception(e)
            if type(e) == MongoctlException:
                raise e
            else:
                log_verbose("Cannot get rs config from server '%s'. "
                            "cause: %s" % (self.id, e))
                return None

    ###########################################################################
    def validate_local_op(self, op):

        # If the server has been assumed to be local then skip validation
        if is_assumed_local_server(self.id):
            log_verbose("Skipping validation of server's '%s' address '%s' to be"
                        " local because --assume-local is on" %
                        (self.id, self.get_host_address()))
            return

        log_verbose("Validating server address: "
                    "Ensuring that server '%s' address '%s' is local on this "
                    "machine" % (self.id, self.get_host_address()))
        if not self.is_local():
            log_verbose("Server address validation failed.")
            raise MongoctlException("Cannot %s server '%s' on this machine "
                                    "because server's address '%s' does not appear "
                                    "to be local to this machine. Pass the "
                                    "--assume-local option if you are sure that "
                                    "this server should be running on this "
                                    "machine." % (op,
                                                  self.id,
                                                  self.get_host_address()))
        else:
            log_verbose("Server address validation passed. "
                        "Server '%s' address '%s' is local on this "
                        "machine !" % (self.id, self.get_host_address()))


    ###########################################################################
    def log_server_activity(self, activity):

        if is_logging_activity():
            log_record = {"op": activity,
                          "ts": datetime.datetime.utcnow(),
                          "serverDoc": self.get_document(),
                          "server": self.id,
                          "serverDisplayName": self.get_description()}
            log_verbose("Logging server activity \n%s" %
                        document_pretty_string(log_record))

            repository.get_activity_collection().insert(log_record)

    ###########################################################################
    def needs_repl_key(self):
        """
         We need a repl key if you are auth + a cluster member +
         version is None or >= 2.0.0
        """
        cluster = self.get_cluster()
        return (self.supports_repl_key() and
                cluster is not None and cluster.get_repl_key() is not None)

    ###########################################################################
    def supports_repl_key(self):
        """
         We need a repl key if you are auth + a cluster member +
         version is None or >= 2.0.0
        """
        version = self.get_mongo_version_obj()
        return (version is None or
                version >= version_obj(REPL_KEY_SUPPORTED_VERSION))

    ###########################################################################
    def get_pid(self):
        pid_file_path = self.get_pid_file_path()
        if os.path.exists(pid_file_path):
            pid_file = open(pid_file_path, 'r')
            pid = pid_file.readline().strip('\n')
            if pid and pid.isdigit():
                return int(pid)
            else:
                log_warning("Unable to determine pid for server '%s'. "
                            "Not a valid number in '%s"'' %
                            (self.id, pid_file_path))
        else:
            log_warning("Unable to determine pid for server '%s'. "
                        "pid file '%s' does not exist" %
                        (self.id, pid_file_path))

        return None


def make_db_connection(address):

    try:
        return Connection(address,
                          socketTimeoutMS=CONN_TIMEOUT,
                          connectTimeoutMS=CONN_TIMEOUT)
    except Exception, e:
        log_exception(e)
        error_msg = "Cannot connect to '%s'. Cause: %s" % \
                    (address, e)
        raise MongoctlException(error_msg, cause=e)

###############################################################################
def is_logging_activity():
    return (repository.consulting_db_repository() and
            config.get_mongoctl_config_val("logServerActivity", False))

###############################################################################
__assumed_local_servers__ = []


def assume_local_server(server_id):
    global __assumed_local_servers__
    if server_id not in __assumed_local_servers__:
        __assumed_local_servers__.append(server_id)

###############################################################################
def is_assumed_local_server(server_id):
    global __assumed_local_servers__
    return server_id in __assumed_local_servers__
