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
from subprocess import Popen, PIPE
import sys
import os
import time


def main():
    u = "\n\n%prog 'shell command' [-o output-filename]"
    d = ("Submit job to Torque GPU queue. The shell command argument must be "
         "properly quoted (as shown in the example above). The working "
         "directory of the job will be the current working directory.")
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
    if options.output_filename:
        if os.sep in options.output_filename:
            sys.exit("'%s' must not contain '%s' characters. Exit." % (
                options.output_filename, os.sep))
        if os.path.isdir(options.output_filename):
            print "'%s' is a directory. Exit." % options.output_filename
        if os.path.isfile(options.output_filename):
            print "Warning: '%s' already exists." % options.output_filename
        output_filename = options.output_filename
    else:
        rndstr = os.urandom(2).encode('hex')
        timestr = time.strftime('%y%m%d-%H%M%S-', time.localtime())
        suffix = "_gpujob.log"
        output_filename = "%s%s%s" % (timestr, rndstr, suffix)

    # The torque-compute-job-wrapper expects three arguments:
    # "shell command lala lulu" "working_directory" "stdout_stderr_filename"
    qsub_stdin = """torque-gpu-job-wrapper '%s' %s %s\n""" % (
        user_shell_command, cwd, output_filename)

    # Request one node, one GPU and one virtual core. Tell qsub that the
    # job's working dir is the current working dir.
    qsub_command = [
        'qsub',
        '-l', 'nodes=1:gpus=1:ppn=1',
        '-d', cwd,
        '-k', 'oe']
    sp = Popen(args=qsub_command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = sp.communicate(qsub_stdin)
    if out:
        print "qsub stdout:"
        print out
    if err:
        print "qsub stderr:"
        print err
    returncode = sp.returncode
    # print "qsub returncode: %s" % returncode
    if returncode == 0:
        print "Job stdout/stderr filename: '%s'" % output_filename


if __name__ == "__main__":
    main()
