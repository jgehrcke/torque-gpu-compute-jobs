# -*- coding: utf-8 -*-
# Copyright 2013 Jan-Philip Gehrcke.

"""
PBS/Torque job submission wrapper script.

Usage:
submit-gpu-job "command [arg1 [arg2]]" [-o outerrfile]

It basically does:

    echo "python compute-job-wrapper.py 'command' $PWD [-o ]" | \
        qsub -l nodes=1:gpus=1:ppn=1 -d $PWD

The wrapper on the executing node ensures that stdout and stderror are
collected and that the environment variable CUDA_VISIBLE_DEVICES is set
according to the GPU assigned by PBS/Torque.
"""


from optparse import OptionParser
import subprocess
import sys
import os


def main():
    u = "\n\n%prog 'shell command' [-o output-file-path]"
    d = ("Submit job to Torque GPU queue. The shell command argument must be "
         "properly quoted. The working directory of the job is the current "
         "working directory.")
    parser = OptionParser(usage=u, description=d)
    parser.add_option("-o", "--output-file", dest="output_file_path",
        help="The stdout and stderr of the job is written to this file.",
        metavar="PATH"
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

    # The torque-compute-job-wrapper expects two or three arguments:
    # "shell command lala lulu" "working_directory" [stdout_stderr_file]

    shell_command = ("""echo "torque-compute-job-wrapper '%s' %s %s" | qsub -l nodes=1:gpus=1:ppn=1 -d %s""" %
        (user_shell_command, cwd, output_file_path, cwd))

    print shell_command


    returncode = subprocess.call(args=shell_command, shell=True)

if __name__ == "__main__":
    main()


