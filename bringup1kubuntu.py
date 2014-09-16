#!/usr/bin/env python

import sys
import subprocess
import time


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
    run(opts, cmd)


def status(opts):
    cmd = envcmd(opts, "status")
    run(opts, cmd)


def reset_constraints(opts):
    cmd = envcmd(opts, "set-constraints")
    cmd.extend(opts.constraints_1.split(' '))
    run(opts, cmd)


def deploy_machines(opts):
    cmd = envcmd(opts, "deploy")
    cmd.extend(("ubuntu", "-n", str(opts.num_machines)))
    run(opts, cmd)


def add_lxcs(opts):
    cmd = envcmd(opts, "add-unit")
    cmd.extend(("ubuntu", "--to"))
    for j in range(opts.num_lxc):
        torun = []
        for i in range(1, opts.num_machines+1):
            torun.append(cmd[:] + ['lxc:%d' % (i,)])
        run_async(opts, torun)


def add_units(opts):
    """Add all the units to the machines that we asked for.
    """
    cmd = envcmd(opts, "add-unit")
    cmd.extend(("ubuntu", "--to"))
    for j in range(opts.num_units):
        torun = []
        for i in range(1, opts.num_machines+1):
            torun.append(cmd[:] + ['%d' % (i,)])
        run_async(opts, torun)


def build_env(opts):
    bootstrap(opts)
    status(opts)
    reset_constraints(opts)
    deploy_machines(opts)
    status(opts)
    add_lxcs(opts)
    add_units(opts)


def main(args):
        import argparse
        p = argparse.ArgumentParser(description='description of program')
        p.add_argument('--version', action='version', version='%(prog)s 0.1')
        p.add_argument('--verbose', action='store_true', help='Be chatty')
        p.add_argument('--environment', '-e', default=None, help='set the environment to run on')
        p.add_argument('--constraints-0', '-0', default='mem=29G cpu-cores=8',
                help='Set the size of the root machine. By default it is an m3.2xlarge')
        p.add_argument('--constraints-1', '-1', default='mem=7G cpu-cores=2',
                help='Set the constraints for machines other that bootstrap, default is m3.large')
        p.add_argument('--num-machines', '-n', default=15, type=int,
                help='How many virtual machines to allocate (default 15)')
        p.add_argument('--num-lxc', '-l', default=0, type=int,
                help='How many LXC machines per virtual machine')
        p.add_argument('--num-units', '-u', default=100, type=int,
                help='How many units of Ubuntu per Machine')
        p.add_argument('--dry-run', action='store_true', default=True,
            help="print what you would do, don't do it yet")
        p.add_argument('--no-dry-run', dest='dry_run', action='store_false',
            help="override --dry-run and just do it")

        opts = p.parse_args(args)
        build_env(opts)

if __name__ == '__main__':
        main(sys.argv[1:])

