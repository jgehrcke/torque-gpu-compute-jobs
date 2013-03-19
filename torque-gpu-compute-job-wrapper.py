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


def main():
    command = sys.argv[1]
    working_directory = sys.argv[2]
    if len(sys.argv) > 3:
        out_err_file = sys.argv[3]
    else:
        out_err_file = None
    
    # If `out_err_file` is provided, collect both, stdout and stderr, of the
    # child process to this file.
    try:
        f = None
        returncode = None
        child_stdout = None
        child_stderr = None
        if out_err_file is not None:
            f = open(out_err_file, 'w')
            child_stdout = f
            child_stderr = subprocess.STDOUT  
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


