#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2013 Jan-Philip Gehrcke.

"""
PBS/Torque GPU job submission script. Wraps Torque's qsub command.

Usage:

submit-gpu-job "command [arg1 [arg2]]" [-o outerrfile]

In Bashism, it does:

    echo "python torque-gpu-job-wrapper 'command' $PWD outerrfile " | \
        qsub -l nodes=1:gpus=1:ppn=1 -d $PWD

The wrapper on the executing node ensures that the environment variable
CUDA_VISIBLE_DEVICES is set according to the GPU assigned by PBS/Torque.
Furthermore it collects stdout and stderr ob the job script to a file in the
submission directory. If the file name is not explicitly given, a unique one is
chosen.
"""


from optparse import OptionParser
import subprocess
import sys
import os


def main():
    u = "\n\n%prog 'shell command' [-o output-filename]"
    d = ("Submit job to Torque GPU queue. The shell command argument must be "
         "properly quoted. The working directory of the job is the current "
         "working directory.")
    parser = OptionParser(usage=u, description=d)
    parser.add_option("-o", "--output", dest="output_filename",
        help=("stdout and stderr of the job will be written to this file in "
              "the current working directory. If not provided, a unique name "
              "is chosen."),
        metavar="FILENAME"
        )

    # Get, validate and process user input.
    options, args = parser.parse_args()
    if len(args) < 1:
        parser.error("The shell command argument is required.")
    user_shell_command = args[0]

    cwd = os.getcwd()
    output_file_path = ""
    if options.output_file_path:
        if os.path.isfile(options.output_file_path):
            sys.exit("'%s' already exists. Exit." % options.output_file_path)
        output_file_path = options.output_file_path

    # TODO: validate output filename, do not allow path separators, set
    # output_filename.

    # The torque-compute-job-wrapper expects three arguments:
    # "shell command lala lulu" "working_directory" "stdout_stderr_filename"

    qsub_stdin = """torque-gpu-job-wrapper '%s' %s %s""" % (
        user_shell_command, cwd, output_filename)

    qsub_command = ['qsub', '-l', 'nodes=1:gpus=1:ppn=1', '-d', cwd]

    sp = subprocess.Popen(args=qsub_command)
    out, err = sp.communicate(qsub_stdin)

    returncode = sp.returncode

if __name__ == "__main__":
    main()


