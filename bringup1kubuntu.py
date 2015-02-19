#!/usr/bin/env python

import os
import sys
import subprocess
import threading
import time
import yaml
import Queue

import jujuclient


def run(opts, cmd):
    """Run the command if dry_run is not set.

    Always print the command that would be run.
    """
    if sys.stdout.isatty():
        tail = '\r'
    else:
        tail = '\n'
    sys.stdout.write("          %s%s" % (' '.join(cmd), tail))
    sys.stdout.flush()
    tstart = time.time()
    try:
        if not opts.dry_run:
            subprocess.check_call(cmd)
    finally:
        tend = time.time()
        sys.stdout.write("%8.3fs\n" % (tend - tstart,))


class Waiter(object):
    """Just a fake Popen that immediately returns."""

    def wait(self):
        self.returncode = 0
        return 0


def start(opts, cmd):
    """Start the given command.

    This assumes that the command should be run asynchronously.
    """
    #print cmd
    if opts.dry_run:
        return Waiter()
    else:
        return subprocess.Popen(cmd)
    return None


def run_async(opts, cmds):
    """Run a bunch of commands asynchronously and wait for them to finish.
    """
    if sys.stdout.isatty():
        tail = '\r'
    else:
        tail = '\n'
    sys.stdout.write('          starting %d: %s%s'
            % (len(cmds), ' '.join(cmds[0]), tail))
    sys.stdout.flush()
    tstart = time.time()
    try:
        running = []
        for cmd in cmds:
            running.append((cmd, start(opts, cmd)))
        failed = False
        for (rcmd, r) in running:
            retcode = r.wait()
            if retcode != 0:
                print "failed to run: %s" % (rcmd,)
                failed = True
        if failed:
            raise subprocess.CalledProcessError("failed to run all commands")
    finally:
        tend = time.time()
        sys.stdout.write("%8.3fs\n" % (tend - tstart,))


def envcmd(opts, command):
    cmd = ["juju", command]
    if opts.environment is not None:
        cmd.extend(("-e", opts.environment))
    return cmd


def bootstrap(opts):
    cmd = envcmd(opts, "bootstrap")
    if opts.constraints_0:
        cmd.extend(("--constraints", opts.constraints_0))
    if opts.upload_tools:
        cmd.append("--upload-tools")
    run(opts, cmd)


def status(opts):
    cmd = envcmd(opts, "status")
    run(opts, cmd)


def current_environment_name(opts):
    if opts.environment:
        return opts.environment
    cmd = ["juju", "switch"]
    return suprocess.check_output(cmd).strip()


def connect_to_environment(opts):
    name = current_environment_name(opts)
    jenv = os.path.expanduser("~/.juju/environments/%s.jenv" % (name,))
    with open(jenv, 'rb') as jenv_file:
        config = yaml.load(jenv_file)
    # For now, we can't pass 'ca-cert' because then it tries to validate the CA
    # Cert but check_hostname isn't available on my system.
    #env = jujuclient.Environment('wss://%s' % (config['state-servers'][0],),
    #        ca_cert=config['ca-cert'])
    env = jujuclient.Environment('wss://%s' % (config['state-servers'][0],))
    env.login(config['password'], user='user-'+config['user'])
    return env


def ha(opts):
    if not opts.ha:
        return
    cmd = envcmd(opts, "ensure-availability")
    run(opts, cmd)


def reset_constraints(opts):
    if not opts.constraints_1:
        return
    cmd = envcmd(opts, "set-constraints")
    cmd.extend(opts.constraints_1.split(' '))
    run(opts, cmd)


def deploy_machines(opts, env):
    charm_url = 'cs:trusty/ubuntu-0'
    env.deploy('ubuntu', charm_url, num_units=opts.num_machines)
    ##cmd = envcmd(opts, "deploy")
    ##cmd.extend(("ubuntu", "-n", str(opts.num_machines)))
    ##run(opts, cmd)


failureCount = 0
successCount = 0

def work_on_queue(env, queue):
    while True:
        #sys.stdout.write('waiting for queue item\n'); sys.stdout.flush()
        nextItem = queue.get()
        try:
            #sys.stdout.write('got %s\n' % (nextItem,)); sys.stdout.flush()
            funcName, kwargs = nextItem
            if funcName == 'stop':
                return
            func = getattr(env, funcName)
            try:
                result = func(**kwargs)
            except jujuclient.EnvError as e:
                global failureCount
                failureCount += 1
            else:
                global successCount
                successCount += 1
        finally:
            queue.task_done()


def create_env_workers(envs):
    """Create a thread worker for each env"""
    queue = Queue.Queue()
    threads = []
    for env in envs:
        t = threading.Thread(target=work_on_queue, args=(env, queue))
        threads.append(t)
        t.start()
    return queue, threads


def enqueue(queue, funcName, **kwargs):
    queue.put((funcName, kwargs))

def add_lxcs(opts, queue):
    tfirst = time.time()
    for j in range(opts.num_lxc):
        tstart = time.time()
        for i in range(opts.num_machines):
            enqueue(queue,
                    'add_unit',
                    service_name='ubuntu',
                    machine_spec='lxc:%d' % (i+1,))
        queue.join()
        tend = time.time()
        sys.stdout.write('%8.3fs added %d lxc machines\n'
                % (tend - tstart, opts.num_machines))
    tend = time.time()
    sys.stdout.write('%8.3fs added total of %d lxc machines\n'
            % (tend - tfirst, opts.num_machines*opts.num_lxc))

    ##cmd = envcmd(opts, "add-unit")
    ##cmd.extend(("ubuntu", "--to"))
    ##for j in range(opts.num_lxc):
    ##    torun = []
    ##    for i in range(1, opts.num_machines+1):
    ##        torun.append(cmd[:] + ['lxc:%d' % (i,)])
    ##    run_async(opts, torun)


def add_units(opts, queue):
    """Add all the units to the machines that we asked for.
    """
    tfirst = time.time()
    for j in range(opts.num_units):
        tstart = time.time()
        for i in range(opts.num_machines):
            enqueue(queue,
                    'add_unit',
                    service_name='ubuntu',
                    machine_spec='%d' % (i+1,))
        queue.join()
        tend = time.time()
        sys.stdout.write('%8.3fs added %d units\n'
                % (tend - tstart, opts.num_machines))
    tend = time.time()
    sys.stdout.write('%8.3fs added total of %d units\n'
            % (tend - tfirst, opts.num_machines*opts.num_units))
    ##cmd = envcmd(opts, "add-unit")
    ##cmd.extend(("ubuntu", "--to"))
    ##for j in range(opts.num_units):
    ##    torun = []
    ##    for i in range(1, opts.num_machines+1):
    ##        torun.append(cmd[:] + ['%d' % (i,)])
    ##    run_async(opts, torun)


def build_env(opts):
    bootstrap(opts)
    status(opts)
    ha(opts)
    reset_constraints(opts)
    if opts.dry_run:
        return
    envs = []
    for _ in range(opts.in_parallel):
        envs.append(connect_to_environment(opts))
    deploy_machines(opts, envs[0])
    status(opts)
    queue, threads = create_env_workers(envs)
    sys.stdout.write('created %d workers\n' %(len(threads),)); sys.stdout.flush()
    add_lxcs(opts, queue)
    status(opts)
    add_units(opts, queue)
    status(opts)
    #sys.stdout.write('stopping\n'); sys.stdout.flush()
    for _ in range(opts.in_parallel):
        queue.put(('stop', {}))
    #sys.stdout.write('waiting for queue\n'); sys.stdout.flush()
    queue.join()
    #sys.stdout.write('waiting for threads\n'); sys.stdout.flush()
    successPercent = 100.0
    failurePercent = 0.0
    if successCount > 0 or failureCount > 0:
        successPercent = 100.0 * float(successCount) / (successCount + failureCount)
        failurePercent = 100.0 * float(failureCount) / (successCount + failureCount)
    sys.stdout.write('success count: %d %.2f%%\nfailure count: %d %.2f%%\n'
            % (successCount, successPercent, failureCount, failurePercent))

    for t in threads:
        t.join()


bigMem = 'mem=29G cpu-cores=8'
medMem = 'mem=7G cpu-cores=2'
smallMem = 'mem=2G cpu-cores=1'

def parse_args(args):
        import argparse
        p = argparse.ArgumentParser(description='description of program')
        p.add_argument('--version', action='version', version='%(prog)s 0.1')
        p.add_argument('--verbose', action='store_true', help='Be chatty')
        p.add_argument('--environment', '-e', default=None, help='set the environment to run on')
        p.add_argument('--ha', action='store_true',
                help='change the state servers to be in HA mode')
        p.add_argument('--constraints-0', '-0', default=bigMem,
                help='Set the size of the root machine. By default it is an m3.2xlarge')
        p.add_argument('--constraints-1', '-1', default=medMem,
                help='Set the constraints for machines other that bootstrap, default is m3.large')
        p.add_argument('--upload-tools', default=False, action='store_true',
                help='pass --upload-tools to juju bootstrap')
        p.add_argument('--num-machines', '-n', default=15, type=int,
                help='How many virtual machines to allocate (default 15)')
        p.add_argument('--num-lxc', '-l', default=0, type=int,
                help='How many LXC machines per virtual machine')
        p.add_argument('--num-units', '-u', default=100, type=int,
                help='How many units of Ubuntu per Machine')
        p.add_argument('--in-parallel', type=int, default=1,
                help="create multiple connections for creating units")
        p.add_argument('--dry-run', action='store_true', default=False,
                help="print what you would do, don't do it yet")
        p.add_argument('--no-dry-run', dest='dry_run', action='store_false',
                help="override --dry-run and just do it")

        return p.parse_args(args)

def main(args):
        opts = parse_args(args)
        build_env(opts)

if __name__ == '__main__':
        main(sys.argv[1:])

