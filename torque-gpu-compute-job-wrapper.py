#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2013 Jan-Philip Gehrcke.

"""
PBS/Torque job wrapper script.

script "command lala lulu" "working_directory" [stdout_stderr_file]

Collects standard output and standard error to a file (optionally).
Only rudimentary error checking, is not called directly by the user.

PBS/Torque use process session ids to identify processes belonging to a job
and signals are sent to all processes in a session:
http://www.supercluster.org/pipermail/torqueusers/2005-March/001460.html

A subprocess created from this wrapper belongs to this session i.e. there is no
need for this wrapper to forward signals to its subprocess.

The command provided is run through a shell, because this is what the user
expects.

TODO: evaluate GPUFILE and set CUDA_VISIBLE_DEVICES.
"""


import subprocess
import sys
import os
import socket


def set_cuda_visible_devices_from_pbs_gpufile():
    # Be graceful regarding errors.
    pbs_gpufile = os.environ.get('PBS_GPUFILE')
    if pbs_gpufile is None:
        sys.exit("PBS_GPUFILE environment variable not set.")
    if not os.path.isfile(pbs_gpufile):
        sys.exit("Not a file: '%s' (PBS_GPUFILE)." % pbs_gpufile)
    try:
        with open(pbs_gpufile) as f:
            allocated_gpu_lines = [l for l in f] 
    except OSError:
        sys.exit("Could not open file: '%s' (PBS_GPUFILE)." % pbs_gpufile)
    if not allocated_gpu_lines:
        sys.exit("Not even one line in '%s' (PBS_GPUFILE)." % pbs_gpufile)
    if len(allocated_gpu_lines) > 1:
        sys.exit("More than one line in '%s' (PBS_GPUFILE). Not supported." %
             pbs_gpufile)
    # Interpret string of the form 'pi-gpu1'.
    hostname, allocated_gpu = allocated_gpu_lines[0].strip().split("-")
    system_hostname = socket.gethostname()
    if system_hostname != hostname:
        sys.exit(("PBS_GPUFILE hostname ('%s') does not match system "
                  "hostname ('%s')" % (hostname, system_hostname)))
    if not len(allocated_gpu) > 3:
        sys.exit("Allocated gpu identifier '%s' too short." % allocated_gpu)
    allocated_cpu_id = allocated_gpu[3:]
    try:
        allocated_cpu_id = int(allocated_cpu_id)
    except ValueError:
        sys.exit("Unexpected gpu identifier '%s' ." % allocated_gpu)
    os.environ["CUDA_VISIBLE_DEVICES"] = str(allocated_cpu_id)


def main():
    command = sys.argv[1]
    working_directory = sys.argv[2]
    if len(sys.argv) > 3:
        out_err_file = sys.argv[3]
    else:
        out_err_file = None

    # If `out_err_file` is provided, collect both, stdout and stderr, of the
    # child process to this file. Also, collect output of this wrapper script
    # to this file.
    try:
        f = None
        returncode = None
        child_stdout = None
        child_stderr = None
        if out_err_file is not None:
            f = open(out_err_file, 'w')
            child_stdout = f
            child_stderr = subprocess.STDOUT
            sys.stdout = f
            sys.stderr = sys.stdout
        
        set_cuda_visible_devices_from_pbs_gpufile()    
            
        # With the default settings of None for `stdout`, `stderr`, `stdin` no
        # redirection will occur; the child's file handles are inherited from
        # the parent.
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

