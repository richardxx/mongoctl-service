#!/usr/bin/env python

# The MIT License

# Copyright (c) 2012 ObjectLabs Corporation

# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

__author__ = 'abdul, richardxx'

###############################################################################
# Imports
###############################################################################

import sys
import traceback

import os

import config
import objects.server

from dargparse import dargparse
from mongoctl_logging import (
    log_error, log_info, turn_logging_verbose_on, log_verbose, log_exception
)

from mongoctl_command_config import MONGOCTL_PARSER_DEF
from errors import MongoctlException
from prompt import (
    is_interactive_mode, set_interactive_mode, say_yes_to_everything,
    say_no_to_everything
)

from utils import namespace_get_property
from users import parse_global_login_user_arg

###############################################################################
# Constants
###############################################################################

CONF_ROOT_ENV_VAR = "MONGOCTL_CONF"
SERVER_ID_PARAM = "server"


parser = None
run_as_service = False


def setup(run_service=False):
    """
    This function should be called before doing anything else.
    """
    global parser
    global run_as_service

    parser = dargparse.build_parser(MONGOCTL_PARSER_DEF)
    run_as_service = run_service


def is_service():
    global run_as_service

    return run_as_service is True


def execute(args):
    """
        The real entry for processing requests.
    """
    try:
        return str(__do_execute(args))
    except MongoctlException, e:
        log_error(e)
        log_exception(e)
    except Exception, e:
        log_exception(e)

    return "Error"


###############################################################################
def __do_execute(args):
    header = """
-------------------------------------------------------------------------------------------
  __ _  ___  ___  ___ ____  ____/ /_/ /
 /  ' \/ _ \/ _ \/ _ `/ _ \/ __/ __/ / 
/_/_/_/\___/_//_/\_, /\___/\__/\__/_/  
                /___/ 
-------------------------------------------------------------------------------------------
   """
    global parser

    if len(args) < 1:
        print(header)
        parser.print_help()
        return

    # Parse the arguments and call the function of the selected cmd
    parsed_args = parser.parse_args(args)

    # turn on verbose if specified
    if namespace_get_property(parsed_args, "mongoctlVerbose"):
        turn_logging_verbose_on()

    # set interactive mode
    non_interactive = namespace_get_property(parsed_args, 'noninteractive')
    non_interactive = False if non_interactive is None else non_interactive

    set_interactive_mode(not non_interactive)

    if not is_interactive_mode():
        log_verbose("Running with noninteractive mode")

    # set global prompt value
    yes_all = parsed_args.yesToEverything
    no_all = parsed_args.noToEverything

    if yes_all and no_all:
        raise MongoctlException("Cannot have --yes and --no at the same time. "
                                "Please choose either --yes or --no")
    elif yes_all:
        say_yes_to_everything()
    elif no_all:
        say_no_to_everything()

    # set conf root if specified
    if parsed_args.configRoot is not None:
        config._set_config_root(parsed_args.configRoot)
    elif os.getenv(CONF_ROOT_ENV_VAR) is not None:
        config._set_config_root(os.getenv(CONF_ROOT_ENV_VAR))

    # get the function to call from the parser framework
    command_function = parsed_args.func

    # parse global login if present
    username = namespace_get_property(parsed_args, "username")
    password = namespace_get_property(parsed_args, "password")
    server_id = namespace_get_property(parsed_args, SERVER_ID_PARAM)
    parse_global_login_user_arg(username, password, server_id)

    if server_id is not None:
        # check if assumeLocal was specified
        assume_local = namespace_get_property(parsed_args, "assumeLocal")
        if assume_local:
            objects.server.assume_local_server(server_id)
    # execute command
    log_info("")
    return command_function(parsed_args)


###############################################################################
########################                   ####################################
########################     BOOTSTRAP     ####################################
########################                   ####################################
###############################################################################

if __name__ == '__main__':
    try:
        setup()
        execute(sys.argv[1:])
    except (SystemExit, KeyboardInterrupt), e:
        if e.code == 0:
            pass
        else:
            raise
    except:
        traceback.print_exc()
