"""
    A lightweight server that processes user requests for mongodb operations.
    The request interface is REST.
"""
__author__ = 'richardxx'


import json
from flask import Flask
from flask import request
from mongoctl import mongoctl
from mongoctl import mongoctl_logging
import signal
from mongoctl.commands.common.status import status_command
import subprocess
import re


# host address for this TingDB service
__ting_service_host = "localhost"
# Port of our TingDB service
__ting_service_port = 30000
# Port of the operational service
__ting_service_ops_port = 30001

# host address for our service database
__ting_service_db_host = "localhost"
# Port of out service database
__ting_service_db_port = 27017


app = Flask("__name__")
app.config['DEBUG'] = True


@app.route('/op-start', methods=['POST'])
def op_start():
    """
        Start a plan.
        Input parameters should be plan name and plan type.
        @return: "ok" or "fail"
    """
    print "Start command received..."

    plan_doc = request.form
    plan_name = plan_doc["plan_name"]
    plan_type = plan_doc["plan_type"]

    # We first test if the plan has started
    status = mongoctl.execute(["status", plan_name])
    print "Status of server %s:" % plan_name
    print status

    if status != "":
        status_json = json.loads(status)
        if status_json["connection"] is False:
            command = "start" if plan_type == "servers" else "start-cluster"
            if mongoctl.execute([command, plan_name]) is False:
                return "fail"

    return "ok"


@app.route('/op-geturi', methods=['POST'])
def op_geturi():
    """
        Get URI of a plan.
        Only plan name is needed.
        @return:
    """
    print "GetUri command received"

    plan_name = request.form["plan_name"]
    uri = mongoctl.execute(["print-uri", plan_name])
    return uri




#################################################

def sigint_handler(signal, frame):
    print "You press CTRL+C to terminate this program"
    shut_down_server = request.environ.get('werkzeug.server.shutdown')
    shut_down_server()
    exit(0)


def kill_listeners():
    """
        Perhaps the ops_server has not correctly quit last time.
        Kill itself or others that are listening to the required port.
    @return:
    """
    ports = [str(__ting_service_ops_port)]
    netstat_proc = subprocess.Popen(['netstat', '-lpn'],
                         shell=False,
                         stdout=subprocess.PIPE)
    (data, err) = netstat_proc.communicate()
    # netstat_proc.kill()

    pattern = "^tcp.*((?:{0})).* (?P<pid>[0-9]*)/.*$"
    pattern = pattern.format(')|(?:'.join(ports))
    prog = re.compile(pattern)
    for line in data.split('\n'):
        match = re.match(prog, line)
        if match:
            pid = match.group('pid')
            kill_proc = subprocess.Popen(['kill', '-9', pid])
            # kill_proc.kill()


if __name__ == "__main__":
    kill_listeners()
    mongoctl_logging.setup_logging(log_to_stdout=False)
    mongoctl.setup(True)

    app.run(port=__ting_service_ops_port)

