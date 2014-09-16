"""
Juju Client
-----------

A simple synchronous python client for the juju-core websocket api.

Example Usage::

   from jujuclient import Environment

   env = Environment("wss://instance-url:17070")
   env.login('secret')
   watcher = env.watch()

   env.deploy('loadbalancer', 'cs:precise/haproxy')
   env.deploy('db', 'cs:precise/mysql')
   env.deploy('blog', 'cs:precise/wordpress')

   env.add_relation('blog', 'db')
   env.add_relation('blog', 'loadbalancer')

   env.expose('loadbalancer')

   env.set_config('blog', {'engine': 'apache'})
   env.get_config('blog')
   env.set_constraints('blog', {'cpu-cores': 4})
   env.add_units('blog', 4)
   env.remove_units(['blog/0'])

   env.destroy_service('blog')

   for change_set in watcher:
       print change_set

Todo

- Provide a buffered in mem option with watches on a single conn.

Upstream/Server
  - bad constraints fail silently
  - need proper charm api
  -
"""
# License: GPLv3
# Author: Kapil Thangavelu <kapil.foss@gmail.com>

from base64 import b64encode
from contextlib import contextmanager
import errno
import httplib
import json
import logging
import os
import pprint
import signal
import socket
import ssl
import StringIO
import time
import websocket

# There are two pypi modules with the name websocket (python-websocket
# and websocket) We utilize python-websocket, sniff and error if we
# find the wrong one.
try:
    websocket.create_connection
except AttributeError:
    raise RuntimeError(
        "Expected 'python-websocket' egg "
        "found incompatible gevent 'websocket' egg")


websocket.logger = logging.getLogger("websocket")

log = logging.getLogger("jujuclient")


class AlreadyConnected(Exception):
    pass


class LoginRequired(Exception):
    pass


class TimeoutError(StopIteration):
    pass


class TimeoutWatchInProgress(Exception):
    pass


class UnitErrors(Exception):

    def __init__(self, errors):
        self.errors = errors


class EnvError(Exception):

    def __init__(self, error):
        self.error = error
        self.message = error['Error']
        # Call the base class initializer so that this exception can be pickled
        # (see http://bugs.python.org/issue1692335).
        super(EnvError, self).__init__(error)

    def __str__(self):
        stream = StringIO.StringIO()
        pprint.pprint(self.error, stream, indent=4)
        return "<Env Error - Details:\n %s >" % (
            stream.getvalue())


class Jobs(object):
    HostUnits = "JobHostUnits"
    ManageEnviron = "JobManageEnviron"
    ManageState = "JobManageState"


class RPC(object):

    _auth = False
    _request_id = 0
    _debug = False
    _reconnect_params = None
    conn = None

    def _rpc(self, op):
        if not self._auth and not op.get("Request") == "Login":
            raise LoginRequired()
        if not 'Params' in op:
            op['Params'] = {}
        op['RequestId'] = self._request_id
        self._request_id += 1
        if self._debug:
            log.debug("rpc request:\n%s" % (json.dumps(op, indent=2)))
        self.conn.send(json.dumps(op))
        raw = self.conn.recv()
        result = json.loads(raw)
        if self._debug:
            log.debug("rpc response:\n%s" % (json.dumps(result, indent=2)))

        if 'Error' in result:
            # The backend disconnects us on err, bug: http://pad.lv/1160971
            self.conn.connected = False
            raise EnvError(result)
        return result['Response']

    def login(self, password, user="user-admin", **ignore):
        """Login gets shared to watchers for reconnect."""
        if self.conn and self.conn.connected and self._auth:
            raise AlreadyConnected()
        # Store for constructing separate authenticated watch connections.
        self._creds = {'password': password, 'user': user}
        result = self._rpc(
            {"Type": "Admin", "Request": "Login",
             "Params": {"AuthTag": user, "Password": password}})
        self._auth = True
        return result

    def set_reconnect_params(self, params):
        self._reconnect_params = params

    def reconnect(self):
        if self.conn:
            self._auth = False
            self.conn.close()
        if not self._reconnect_params:
            return False

        log.info("Reconnecting client")
        self.conn = Connector.connect_socket_loop(
            self._reconnect_params['url'],
            self._reconnect_params['ca_cert'])
        self.login(**self._reconnect_params)
        return True


class Watcher(RPC):

    _auth = True

    def __init__(self, conn, auto_reconnect=True):
        self.conn = conn
        self.watcher_id = None
        self.running = False
        self.auto_reconnect = auto_reconnect
        # For debugging, attach the wrapper
        self.context = None

    def start(self):
        result = self._rpc({
            'Type': 'Client',
            'Request': 'WatchAll',
            'Params': {}})
        self.watcher_id = result['AllWatcherId']
        self.running = True
        return result

    def next(self):
        if self.watcher_id is None:
            self.start()
        if not self.running:
            raise StopIteration("Stopped")
        try:
            result = self._rpc({
                'Type': 'AllWatcher',
                'Request': 'Next',
                'Id': self.watcher_id})
        except EnvError, e:
            if "state watcher was stopped" in e.message:
                if not self.auto_reconnect:
                    raise
                if not self.reconnect():
                    raise
                return self.next()
            raise
        return result['Deltas']

    def reconnect(self):
        self.watcher_id = None
        self.running = False
        return super(Watcher, self).reconnect()

    def stop(self):
        if not self.conn.connected:
            return
        try:
            result = self._rpc({
                'Type': 'AllWatcher', 'Request': 'Stop',
                'Id': self.watcher_id})
        except (EnvError, socket.error):
            # We're about to close the connection.
            result = None
        self.conn.close()
        self.watcher_id = None
        self.running = False
        return result

    def set_context(self, context):
        self.context = context
        return self

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc, v, t):
        self.stop()


class TimeoutWatcher(Watcher):
    # A simple non concurrent watch using signals..

    _timeout = None

    def set_timeout(self, timeout):
        self._timeout = timeout

    def next(self):
        with self._set_alarm(self._timeout):
            return super(TimeoutWatcher, self).next()

    @classmethod
    @contextmanager
    def _set_alarm(cls, timeout):
        try:
            handler = signal.getsignal(signal.SIGALRM)
            if callable(handler):
                if handler.__name__ == '_set_alarm':
                    raise TimeoutWatchInProgress()
                raise RuntimeError(
                    "Existing signal handler found %r" % handler)
            signal.signal(signal.SIGALRM, cls._on_alarm)
            signal.alarm(timeout)
            yield None
        finally:
            signal.signal(signal.SIGALRM, signal.SIG_DFL)

    @classmethod
    def _on_alarm(cls, x, frame):
        raise TimeoutError()


class Connector(object):
    """Abstract out the details of connecting to state servers.

    Covers
    - finding state servers, credentials, certs for a named env.
    - verifying state servers are listening
    - connecting an environment or websocket to a state server.
    """

    retry_conn_errors = (errno.ETIMEDOUT, errno.ECONNREFUSED, errno.ECONNRESET)

    def run(self, cls, env_name):
        """Given an environment name, return an authenticated client to it."""
        jhome, data = self.parse_env(env_name)
        cert_path = self.write_ca(jhome, env_name, data)
        address = self.get_state_server(data)
        if not address:
            return
        return self.connect_env(
            cls, address, data['user'], data['password'], cert_path)

    def connect_env(self, cls, address, user, password, cert_path=None):
        """Given environment info return an authenticated client to it."""
        env = cls("wss://%s" % address, ca_cert=cert_path)
        env.login(user="user-%s" % user, password=password)
        return env

    @classmethod
    def connect_socket(cls, endpoint, cert_path=None):
        """Return a websocket connection to an endpoint."""
        sslopt = {'ssl_version': ssl.PROTOCOL_TLSv1}
        if cert_path:
            sslopt['ca_certs'] = cert_path
            # ssl.match_hostname is broken for us, need to disable per
            # https://github.com/liris/websocket-client/issues/105
            # when that's available, we can just selectively disable
            # the host name match, for now we have to disable cert
            # checking :-(
            sslopt['check_hostname'] = False
        else:
            sslopt['cert_reqs'] = ssl.CERT_NONE

        return websocket.create_connection(
            endpoint, origin=endpoint, sslopt=sslopt)

    def connect_socket_loop(self, endpoint, cert_path=None, timeout=120):
        """Retry websocket connections to an endpoint till its connected."""
        t = time.time()
        while (time.time() > t + timeout):
            try:
                return Connector.connect_socket(endpoint, cert_path)
            except socket.error as err:
                if not err.errno in self.retry_conn_errors:
                    raise
                time.sleep(1)
                continue

    def write_ca(self, cert_dir, cert_name, data):
        """Write ssl ca to the given."""
        cert_path = os.path.join(cert_dir, '%s-cacert.pem' % cert_name)
        with open(cert_path, 'w') as ca_fh:
            ca_fh.write(data['ca-cert'])
        return cert_path

    def get_state_server(self, data):
        """Given a list of state servers, return one that's listening."""
        found = False
        for s in data['state-servers']:
            if self.is_server_available(s):
                found = True
                break
        if not found:
            return
        return s

    def parse_env(self, env_name):
        import yaml
        jhome = os.path.expanduser(
            os.environ.get('JUJU_HOME', '~/.juju'))
        jenv = os.path.join(jhome, 'environments', '%s.jenv' % env_name)
        if not os.path.exists(jenv):
            raise ValueError("Environment %s not bootstrapped" % env_name)

        with open(jenv) as fh:
            data = yaml.safe_load(fh.read())
            return jhome, data

    def is_server_available(self, server):
        """ Given address/port, return true/false if it's up """
        address, port = server.split(":")
        try:
            socket.create_connection((address, port), 3)
            return True
        except socket.error as err:
            if err.errno in self.retry_conn_errors:
                return False
            else:
                raise


class Environment(RPC):

    def __init__(self, endpoint, conn=None, ca_cert=None):
        self.endpoint = endpoint
        self._watches = []
        # For watches.
        self._creds = None
        self._ca_cert = ca_cert

        if conn is not None:
            self.conn = conn
        else:
            self.conn = Connector.connect_socket(endpoint, self._ca_cert)

    def close(self):
        for w in self._watches:
            w.stop()
        if self.conn.connected:
            self.conn.close()

    @classmethod
    def connect(cls, env_name):
        return Connector().run(cls, env_name)

    # Charm ops
    def add_local_charm(self, charm_file, series, size=None):
        """Add a local charm to an environment.

        Uses an https endpoint at the same host:port as the wss.
        Supports large file uploads.
        """
        endpoint = self.endpoint.replace('wss://', '')
        host, port = endpoint.split(':')
        conn = httplib.HTTPSConnection(host, port)
        path = "/charms?series=%s" % (series)
        headers = {
            'Content-Type': 'application/zip',
            'Authorization': 'Basic %s' % b64encode(
                '%(user)s:%(password)s' % (self._creds))}
        # Specify if its a psuedo-file object,
        # httplib will try to stat non strings.
        if size:
            headers['Content-Length'] = size
        conn.request("POST", path, charm_file, headers)
        response = conn.getresponse()
        result = json.loads(response.read())
        if not response.status == 200:
            raise EnvError(result)
        return result

    def add_charm(self, charm_url):
        return self._rpc(
            {"Type": "Client",
             "Request": "AddCharm",
             "Params": {"URL": charm_url}})

    # Environment operations
    def info(self):
        return self._rpc({
            "Type": "Client",
            "Request": "EnvironmentInfo"})

    def status(self, filters=None):
        if isinstance(filters, basestring):
            filters = [filters]
        op = {"Type": "Client", "Request": "FullStatus"}
        if filters:
            op["Params"] = {'Patterns': filters}
        return self._rpc(op)

    def get_charm(self, charm_url):
        return self._rpc(
            {"Type": "Client",
             "Request": "CharmInfo",
             "Params": {
                 "CharmURL": charm_url}})

    def resolve_charm(self, partial_charm_url):
        """Call the ResolveCharms api to turn 'ubuntu' into its exact
        expansion.
        """
        result = self._rpc(
                {"Type": "Client",
                 "Request": "ResolveCharms",
                 "Params": {
                     "References": [partial_charm_url]
                     }
                 })
        return result["URLs"][0]

    # Environment
    def get_env_constraints(self):
        return self._rpc({
            "Type": "Client",
            "Request": "GetEnvironmentConstraints"})

    def set_env_constraints(self, constraints):
        return self._rpc({
            "Type": "Client",
            "Request": "SetEnvironmentConstraints",
            "Params": {}})

    def get_env_config(self):
        return self._rpc({
            "Type": "Client",
            "Request": "EnvironmentGet"})

    def set_env_config(self, config):
        return self._rpc({
            "Type": "Client",
            "Request": "EnvironmentSet",
            "Params": {"Config": config}})

    # Machine ops
    def add_machine(self, series="", constraints=None,
                    machine_spec="", parent_id="", container_type=""):

        """Allocate a new machine from the iaas provider.
        """
        if machine_spec:
            err_msg = "Cant specify machine spec with container_type/parent_id"
            assert not (parent_id or container_type), err_msg
            parent_id, container_type = machine_spec.split(":", 1)

        params = dict(
            Series=series,
            Constraints=self._prepare_constraints(constraints),
            ContainerType=container_type,
            ParentId=parent_id,
            Jobs=[Jobs.HostUnits])
        return self.add_machines([params])['Machines'][0]

    def add_machines(self, machines):
        """Allocate multiple machines from the iaas provider.

        See add_machine for format of parameters.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "AddMachines",
            "Params": {
                "MachineParams": machines}})

    def register_machine(self, instance_id, nonce, series, hardware, addrs):
        """Register/Enlist a machine into an environment state.

        The machine will need to have tools installed and subsequently
        connect to the state server with the given nonce
        credentials. The machine_config method can be used to
        construct a suitable set of commands.

        Parameters:

        nonce: is the initial password for the new machine.
        addrs: list of ip addresses for the machine.
        hw: is the hardware characterstics of the machine, applicable keys.
         - Arch
         - Mem
         - RootDisk size
         - CpuCores
         - CpuPower
         - Tags
        """
        params = dict(
            Series=series,
            InstanceId=instance_id,
            Jobs=[Jobs.HostUnits],
            HardwareCharacteristics=hardware,
            Addrs=addrs,
            Nonce=nonce)
        return self.register_machines([params])['Machines'][0]

    def register_machines(self, machines):
        return self._rpc({
            "Type": "Client",
            "Request": "InjectMachines",
            "Params": {
                "MachineParams": machines}})

    def destroy_machines(self, machine_ids, force=False):
        params = {"MachineNames": machine_ids}
        if force:
            params["Force"] = True
        return self._rpc({
            "Type": "Client",
            "Request": "DestroyMachines",
            "Params": params})

    def provisioning_script(self, machine_id, nonce,
                            data_dir="/var/lib/juju", disable_apt=False):
        return self._rpc({
            "Type": "Client",
            "Request": "ProvisioningScript",
            "Params": {
                "MachineId": machine_id,
                "Nonce": nonce,
                "DataDir": data_dir,
                "DisablePackageCommands": disable_apt}})

    def machine_config(self, machine_id, series, arch):
        """Return information needed to render cloudinit for a machine.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "MachineConfig",
            "Params": {
                "MachineId": machine_id,
                "Series": series,
                "Arch": arch}})

    def retry_provisioning(self, machines):
        """Mark machines for provisioner to retry iaas provisioning."""
        return self._rpc({
            "Type": "Client",
            "Request": "RetryProvisioning",
            "Params": {
                "Entities": map(lambda x: {"Tag": "machine-%s"}, machines)}})

    # Watch Wrapper methods
    def get_stat(self):
        """A status emulator using the watch api, returns immediately.
        """
        watch = self.get_watch()
        return StatusTranslator().run(watch)

    def wait_for_units(
            self, timeout=None, goal_state="started", callback=None):
        """Wait for all units to reach a given state.

        Any unit errors will cause an exception to be raised.
        """
        watch = self.get_watch(timeout)
        return WaitForUnits(watch, goal_state).run(callback)

    def wait_for_no_machines(self, timeout, callback=None):
        """For unit tests doing teardowns, or deployer during reset.
        """
        watch = self.get_watch(timeout)
        return WaitForNoMachines(watch).run(callback)

    def get_watch(self, timeout=None, connection=None, watch_class=None):
        # Separate conn per watcher to keep sync usage simple, else we have to
        # buffer watch results with requestid dispatch. At the moment
        # with the all watcher, an app only needs one watch, which is likely to
        # change to discrete watches on individual bits.
        if connection is None:
            watch_env = Environment(self.endpoint)
            watch_env.login(**self._creds)
        else:
            watch_env = connection

        p = dict(self._creds)
        p.update({
            'url': self.endpoint,
            'origin': self.endpoint,
            'ca_cert': self._ca_cert})
        if timeout is not None:
            if watch_class is None:
                watch_class = TimeoutWatcher
            watcher = watch_class(watch_env.conn)
            watcher.set_timeout(timeout)
        else:
            if watch_class is None:
                watch_class = Watcher
            watcher = watch_class(watch_env.conn)
        watcher.set_reconnect_params(p)
        self._watches.append(watcher)
        watcher.start()
        return watcher

    watch = get_watch

    def _prepare_strparams(self, d):
        r = {}
        for k, v in d.items():
            r[k] = str(v)
        return r

    def _prepare_constraints(self, constraints):
        for k in ['cpu-cores', 'cpu-power', 'mem']:
            if constraints.get(k):
                constraints[k] = int(constraints[k])
        return constraints

    # Relations
    def add_relation(self, endpoint_a, endpoint_b):
        return self._rpc({
            'Type': 'Client',
            'Request': 'AddRelation',
            'Params': {
                'Endpoints': [endpoint_a, endpoint_b]
            }})

    def remove_relation(self, endpoint_a, endpoint_b):
        return self._rpc({
            'Type': 'Client',
            'Request': 'DestroyRelation',
            'Params': {
                'Endpoints': [endpoint_a, endpoint_b]
            }})

    # Service
    def deploy(self, service_name, charm_url, num_units=1,
               config=None, constraints=None, machine_spec=None):
        """Deploy a charm

        Does not support local charms
        """
        svc_config = {}
        if config:
            svc_config = self._prepare_strparams(config)

        svc_constraints = {}
        if constraints:
            svc_constraints = self._prepare_constraints(constraints)

        return self._rpc(
            {"Type": "Client",
             "Request": "ServiceDeploy",
             "Params": {
                 "ServiceName": service_name,
                 "CharmURL": charm_url,
                 "NumUnits": num_units,
                 "Config": svc_config,
                 "Constraints": svc_constraints,
                 "ToMachineSpec": machine_spec}})

    def set_config(self, service_name, config):
        assert isinstance(config, dict)
        svc_config = self._prepare_strparams(config)
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceSet",
            "Params": {
                "ServiceName": service_name,
                "Options": svc_config}})

    def unset_config(self, service_name, config_keys):
        """Unset configuration values of a service to restore charm defaults.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceUnset",
            "Params": {
                "ServiceName": service_name,
                "Options": config_keys}})

    def set_charm(self, service_name, charm_url, force=False):
        """Set the charm url for a service.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceSetCharm",
            "Params": {
                "ServiceName": service_name,
                "CharmUrl": charm_url,
                "Force": force}})

    def get_service(self, service_name):
        """Returns dict of Charm, Config, Constraints, Service keys.

        Charm -> charm used by service
        Service -> service name
        Config -> Currently configured options and descriptions
        Constraints -> Constraints set on service (not environment inherited).
        """
        return self._rpc(
            {"Type": "Client",
             "Request": "ServiceGet",
             "Params": {
                 "ServiceName": service_name}})

    def get_config(self, service_name):
        """Returns service configuration.
        """
        return self.get_service(service_name)['Config']

    def get_constraints(self, service_name):
        return self._rpc(
            {"Type": "Client",
             "Request": "GetServiceConstraints",
             "Params": {
                 "ServiceName": service_name}})['Constraints']

    def set_constraints(self, service_name, constraints):
        return self._rpc(
            {"Type": "Client",
             "Request": "SetServiceConstraints",
             "Params": {
                 "ServiceName": service_name,
                 "Constraints": self._prepare_constraints(constraints)}})

    def update_service(self, service_name, charm_url="", force_charm_url=False,
                       min_units=None, settings=None, constraints=None):
        """Update a service.

        Can update a service's charm, modify configuration, constraints,
        and the minimum number of units.
        """
        svc_config = {}
        if settings:
            svc_config = self._prepare_strparams(settings)

        return self._rpc(
            {"Type": "Client",
             "Request": "ServiceUpdate",
             "Params": {
                 "ServiceName": service_name,
                 "CharmUrl": charm_url,
                 "MinUnits": min_units,
                 "SettingsStrings": svc_config,
                 "Constraints": self._prepare_constraints(constraints)}})

    def destroy_service(self, service_name):
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceDestroy",
            "Params": {
                "ServiceName": service_name}})

    def expose(self, service_name):
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceExpose",
            "Params": {
                "ServiceName": service_name}})

    def unexpose(self, service_name):
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceUnexpose",
            "Params": {
                "ServiceName": service_name}})

    def valid_relation_names(self, service_name):
        """All possible relation names of a service.

        Per its charm metadata.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceCharmRelations",
            "Params": {
                "ServiceName": service_name}})

    # Units
    def add_units(self, service_name, num_units=1):
        return self._rpc({
            "Type": "Client",
            "Request": "AddServiceUnits",
            "Params": {
                "ServiceName": service_name,
                "NumUnits": num_units}})

    def add_unit(self, service_name, machine_spec=None):
        params = {
            "ServiceName": service_name,
            "NumUnits": 1}
        if machine_spec:
            params["ToMachineSpec"] = machine_spec
        return self._rpc({
            "Type": "Client",
            "Request": "AddServiceUnits",
            "Params": params})

    def remove_units(self, unit_names):
        return self._rpc({
            "Type": "Client",
            "Request": "DestroyServiceUnits",
            "Params": {
                "UnitNames": unit_names}})

    def resolved(self, unit_name, retry=False):
        return self._rpc({
            "Type": "Client",
            "Request": "Resolved",
            "Params": {
                "UnitName": unit_name,
                "Retry": retry}})

    # Multi-context

    def get_public_address(self, target):
        # Return the public address of the machine or unit.
        return self._rpc({
            "Type": "Client",
            "Request": "PublicAddress",
            "Params": {
                "Target": target}})

    # Annotations
    def set_annotation(self, entity, entity_type, annotation):
        """
        Set annotations on an entity.

        Valid entity types are 'service', 'unit', 'machine', 'environment'.
        """
        # valid entity types
        a = self._prepare_strparams(annotation)
        return self._rpc({
            "Type": "Client",
            "Request": "SetAnnotations",
            "Params": {
                "Tag": entity_type + '-' + entity.replace("/", "-"),
                "Pairs": a}})

    def get_annotation(self, entity, entity_type):
        return self._rpc({
            "Type": "Client",
            "Request": "GetAnnotations",
            "Params": {
                "Tag": "%s-%s" % (entity_type, entity.replace("/", "-"))}})


# Unit tests for the watch wrappers are in lp:juju-deployer/darwin
class WatchWrapper(object):

    def __init__(self, watch):
        self.watch = watch

    def run(self, callback=None):
        seen_initial = False
        with self.watch.set_context(self):
            for change_set in self.watch:
                for change in change_set:
                    self.process(*change)
                    if seen_initial and callable(callback):
                        callback(*change)
                if self.complete() is True:
                    self.watch.stop()
                    break
                seen_initial = True

    def process(self):
        """process watch events."""

    def complete(self):
        """watch wrapper complete """


class WaitForUnits(WatchWrapper):
    """
    Wait for units of the environment to reach a particular goal state.
    """
    def __init__(self, watch, state='started', service=None):
        super(WaitForUnits, self).__init__(watch)
        self.units = {}
        self.goal_state = state
        self.service = service

    def process(self, entity_type, change, data):
        if entity_type != "unit":
            return
        if change == "remove" and data['Name'] in self.units:
            del self.units[data['Name']]
        else:
            self.units[data['Name']] = data

    def complete(self):
        state = {'pending': [], 'errors': []}
        for k, v in self.units.items():
            if v['Status'] == "error":
                state['errors'] = [v]
            elif v['Status'] != self.goal_state:
                state['pending'] = [v]
        if not state['pending'] and not state['errors']:
            return True
        if state['errors'] and not self.goal_state == "removed":
            raise UnitErrors(state['errors'])
        return state['pending']


class WaitForNoMachines(WatchWrapper):
    """
    Wait for all non state servers to be terminated.
    """

    def __init__(self, watch):
        super(WaitForNoMachines, self).__init__(watch)
        self.machines = {}

    def process(self, entity_type, change, data):
        if entity_type != 'machine':
            return
        if change == 'remove' and data['Id'] in self.machines:
            del self.machines[data['Id']]
        else:
            self.machines[data['Id']] = data

    def complete(self):
        if self.machines.keys() == ['0']:
            return True


class StatusTranslator(object):
    """
    Status emulation from watch api.
    """

    key_map = {
        'InstanceId': 'instance-id',
        'PublicAddress': 'public-address',
        'Status': 'agent-state',
        "MachineId": "Machine",
        'CharmURL': 'charm',
        'StatusInfo': 'agent-state-info',
        "Number": 'port',
        "Ports": "open-ports"
    }
    remove_keys = set(['Life', "PrivateAddress", "MinUnits"])
    skip_empty_keys = set(['StatusInfo', "Ports"])

    def run(self, watch):
        self.data = {'machines': {}, 'services': {}}
        with watch:
            change_set = watch.next()
            for change in change_set:
                entity_type, change_kind, d = change
                if entity_type == "machine":
                    self._machine(d)
                elif entity_type == "service":
                    self._service(d)
                elif entity_type == "unit":
                    self._unit(d)
                elif entity_type == "relation":
                    self._relation(d)
        result = dict(self.data)
        self.data.clear()
        return result

    def _translate(self, d):
        r = {}
        for k, v in d.items():
            if k in self.remove_keys:
                continue
            if k in self.skip_empty_keys and not v:
                continue
            tk = self.key_map.get(k, k)

            r[tk.lower()] = v
        return r

    def _machine(self, d):
        mid = d.pop('Id')
        self.data.setdefault('machines', {})[mid] = self._translate(d)

    def _unit(self, d):
        svc_units = self.data.setdefault("services", {}).setdefault(
            d['Service'], {}).setdefault('units', {})
        d.pop("Service")
        d.pop("Series")
        d.pop("CharmURL")
        name = d.pop('Name')
        ports = d.pop('Ports')
        tports = d.setdefault('Ports', [])
        for p in ports:
            tports.append("%s/%s" % (p['Number'], p['Protocol']))
        svc_units[name] = self._translate(d)

    def _service(self, d):
        d.pop('Config')
        d.pop('Constraints')
        name = d.pop('Name')
        svc = self.data.setdefault('services', {}).setdefault(name, {})
        svc.update(self._translate(d))

    def _relation(self, d):
        d['Endpoints'][0]['RemoteService'] = d['Endpoints'][0]['ServiceName']
        if len(d['Endpoints']) != 1:
            d['Endpoints'][1]["RemoteService"] = d[
                'Endpoints'][0]['ServiceName']
            d['Endpoints'][0]["RemoteService"] = d[
                'Endpoints'][1]['ServiceName']
        for ep in d['Endpoints']:
            svc_rels = self.data.setdefault(
                'services', {}).setdefault(
                    ep['ServiceName'], {}).setdefault('relations', {})
            svc_rels.setdefault(
                ep['Relation']['Name'], []).append(ep['RemoteService'])


def main():
    import os
    juju_url, juju_token = (
        os.environ.get("JUJU_URL"),
        os.environ.get("JUJU_TOKEN"))
    if not juju_url or not juju_token:
        raise ValueError(
            "JUJU_URL and JUJU_TOKEN should be defined for tests.")
    env = Environment(juju_url)
    env.login(juju_token)
    watcher = env.get_watch(timeout=3)

    print "Env info", env.info()

    for change_set in watcher:
        for change in change_set:
            print "state change", change

    env.deploy("test-blog", "cs:wordpress")
    env.deploy("test-db", "cs:mysql")
    env.add_relation("test-db", "test-blog")

    print "waiting for changes for 30s"
    watcher.set_timeout(30)
    for change_set in watcher:
        for change in change_set:
            print "state change", change

    env.destroy_service('test-blog')
    env.destroy_service('test-db')

if __name__ == '__main__':
    main()
