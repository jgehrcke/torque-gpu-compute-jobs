#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2013 Jan-Philip Gehrcke (http://gehrcke.de)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
PBS/Torque job wrapper script. Executes shell command in a child process.

This script is not called directly by the user.

Usage: wrapper "command lala lulu" "working_directory" [stdout_stderr_file]

Collects standard output and standard error to a file whose name is either
given by the user or chosen automatically.

PBS/Torque uses process session ids to identify processes belonging to a job.
Signals are sent to all processes in a session:
http://www.supercluster.org/pipermail/torqueusers/2005-March/001460.html
A subprocess created from this wrapper belongs to this session, i.e. there is
no need for this wrapper to monitor and forward signals to its child process.

As a feature, the command provided is run through a shell.
"""


import os
import sys
import time
import socket
import logging
import subprocess


logging.basicConfig(
    format='%(asctime)s,%(msecs)-6.1f: %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)
#log.setLevel(logging.INFO)
SYSTEM_HOSTNAME = socket.gethostname()


def generate_output_filename():
    log.debug("Generate output filename from PBS_JOBID, time, and hostname.")
    pbs_jobid = os.environ.get('PBS_JOBID')
    if pbs_jobid is None:
        log.debug("PBS_JOBID environment variable not set.")
        pbs_jobid = "%s.%s" % (os.urandom(2).encode('hex'), SYSTEM_HOSTNAME)
    jobid = "job_%s" % pbs_jobid
    timestr = time.strftime('%y%m%d-%H%M%S-', time.localtime())
    suffix = ".log"
    return "".join([timestr, jobid, suffix])


def set_cuda_visible_devices_from_pbs_gpufile():
    pbs_gpufile = os.environ.get('PBS_GPUFILE')
    if pbs_gpufile is None:
        sys.exit("PBS_GPUFILE environment variable not set. Exit.")
    if not os.path.isfile(pbs_gpufile):
        sys.exit("Not a file: '%s' (PBS_GPUFILE). Exit." % pbs_gpufile)
    log.debug("Valid file path '%s' read from PBS_GPUFILE" % pbs_gpufile)
    try:
        with open(pbs_gpufile) as f:
            allocated_gpu_lines = [l for l in f]
    except OSError:
        sys.exit("Cannot open file: '%s' (PBS_GPUFILE). Exit." % pbs_gpufile)
    log.debug("'%s' content:\n%s" % (pbs_gpufile,"".join(allocated_gpu_lines)))
    if len(allocated_gpu_lines) != 1:
        sys.exit("Only one line in '%s' (PBS_GPUFILE) is supported. Exit." %
            pbs_gpufile)
    # Evaluate string of the form 'pi-gpu1'.
    hostname, allocated_gpu = allocated_gpu_lines[0].strip().split("-")
    if hostname != SYSTEM_HOSTNAME:
        sys.exit(("PBS_GPUFILE hostname ('%s') does not match system "
                  "hostname ('%s')." % (hostname, SYSTEM_HOSTNAME)))
    log.debug("PBS_GPUFILE hostname ('%s') matches system hostname." %
        hostname)
    if not len(allocated_gpu) > 3:
        sys.exit("Allocated gpu identifier '%s' too short." % allocated_gpu)
    allocated_gpu_id = allocated_gpu[3:]
    try:
        allocated_gpu_id = int(allocated_gpu_id)
    except ValueError:
        sys.exit("Unexpected gpu identifier '%s'." % allocated_gpu)
    log.debug("Allocated GPU ID given by PBS: %s" % allocated_gpu_id)
    log.debug("Setting CUDA_VISIBLE_DEVICES.")
    os.environ["CUDA_VISIBLE_DEVICES"] = str(allocated_gpu_id)


def main():
    command = sys.argv[1]
    working_directory = sys.argv[2]
    if len(sys.argv) > 3:
        out_err_file = sys.argv[3]
    else:
        out_err_file = generate_output_filename()

    # Collect both, stdout and stderr, of the child process to `out_err_file`.
    # Also redirect stdout/stderr of this wrapper script. The logging output of
    # this wrapper script still goes to original stderr.
    try:
        f = None
        returncode = None
        f = open(out_err_file, 'w')
        child_stdout = f
        child_stderr = subprocess.STDOUT
        sys.stdout = f
        sys.stderr = sys.stdout

        # Evaluate PBS_GPUFILE and set CUDA_VISIBLE_DEVICES. Exit on failure.
        set_cuda_visible_devices_from_pbs_gpufile()

        # Flush output stream, call child process with shell command.
        sys.stdout.flush()
        returncode = subprocess.call(
            args=command,
            stdout=child_stdout,
            stderr=child_stderr,
            cwd=working_directory,
            shell=True)
    finally:
        if f is not None:
            f.close()
        if returncode is None:
            sys.exit(1)
        sys.exit(returncode)


if __name__ == "__main__":
    main()
