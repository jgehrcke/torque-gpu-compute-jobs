# -*- coding: utf-8 -*-
# Copyright 2013 Jan-Philip Gehrcke.

"""
PBS/Torque job submission wrapper script.

To be used by the end user.

Usage:

submit-gpu-job "command [arg1 [arg2]]" [-o outerrfile]

Basically does:

echo "python compute-job-wrapper.py 'command' $PWD [-o ]" | \
    qsub -l nodes=1:gpus=1:ppn=1 -d $PWD
"""

from optparse import OptionParser
import subprocess
import sys


def main():
    u = "\n\n%prog 'shell command' [-o output-file-path]"
    d = ("Submit job to Torque GPU queue. The shell command argument must be "
         "properly quoted.")
    parser = OptionParser(usage=u, description=d)
    parser.add_option("-o", "--output-file", dest="output_file_path",
        help="The stdout and stderr of the job is written to this file.",
        metavar="PATH"
        )

    # Get, validate and process user input.
    options, args = parser.parse_args()
    if len(args) < 1:
        parser.error("The shell command argument is required.")
    shell_command = args[0]

    if options.output_file_path:
        if os.path.isfile(options.output_file_path):
            sys.exit("'%s' already exists. Exit." % options.output_file_path)




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


