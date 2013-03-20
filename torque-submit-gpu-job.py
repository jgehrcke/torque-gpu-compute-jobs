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
PBS/Torque GPU job submission script. Wraps Torque's qsub command.

Usage:

submit-gpu-job 'shell command' [-o outerrfile]

In Bashism, it does:

    echo "torque-gpu-job-wrapper 'command' $PWD outerrfile " | \
        qsub -l nodes=1:gpus=1:ppn=1 -d $PWD -k oe

The wrapper on the executing node ensures that the environment variable
CUDA_VISIBLE_DEVICES is set according to the GPU assigned by PBS/Torque.
Furthermore it collects stdout and stderr of the job script to a file in the
submission directory. If the file name is not explicitly given, a unique one is
chosen automatically.
"""


import os
import sys
import logging
from optparse import OptionParser
from subprocess import Popen, PIPE

logging.basicConfig(
    format='%(asctime)s,%(msecs)-6.1f: %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)
#log.setLevel(logging.INFO)


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
    log.debug("Shell command:'%s'" % user_shell_command)

    cwd = os.getcwd()
    output_filename = ""
    if options.output_filename:
        if os.sep in options.output_filename:
            sys.exit("'%s' must not contain '%s' characters. Exit." % (
                options.output_filename, os.sep))
        if os.path.isdir(options.output_filename):
            print "'%s' is a directory. Exit." % options.output_filename
        if os.path.isfile(options.output_filename):
            print "Warning: '%s' already exists." % options.output_filename
        output_filename = options.output_filename

    # The torque-compute-job-wrapper expects three arguments:
    # "shell command lala lulu" "working_directory" "stdout_stderr_filename"
    qsub_stdin = """torque-gpu-job-wrapper '%s' %s %s\n""" % (
        user_shell_command, cwd, output_filename)

    # Request one node, one GPU and one virtual core. Tell qsub that the
    # job's working dir is the current working dir. Keep stdout and stderr in
    # home directory (as long as it leaks the wrapper redirection).
    qsub_command = [
        'qsub',
        '-l', 'nodes=1:gpus=1:ppn=1',
        '-d', cwd,
        '-k', 'oe']
    sp = Popen(args=qsub_command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = sp.communicate(qsub_stdin)
    if out:
        print "qsub stdout:"
        print "\n".join(l.rstrip() for l in out.splitlines() if l.strip())
    if err:
        print "qsub stderr:"
        print "\n".join(l.rstrip() for l in err.splitlines() if l.strip())
    returncode = sp.returncode
    # print "qsub returncode: %s" % returncode
    if returncode == 0:
        if output_filename:
            print "Job stdout/stderr filename: '%s'." % output_filename
        else:
            print "Job stdout/stderr filename will be chosen automatically."


if __name__ == "__main__":
    main()
