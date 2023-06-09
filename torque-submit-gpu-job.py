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

    echo "torque-gpu-job-wrapper command_file_name $PWD outerrfile " | \
        qsub -l nodes=1:gpus=1:ppn=1 -d $PWD -V -k oe

The wrapper on the executing node ensures that the environment variable
CUDA_VISIBLE_DEVICES is set according to the GPU assigned by PBS/Torque.
Furthermore it collects stdout and stderr of the job script to a file in the
submission directory. If the file name is not explicitly given, a unique one is
chosen automatically.
"""


import os
import sys
import time
import socket
import logging
from optparse import OptionParser
from subprocess import Popen, PIPE

logging.basicConfig(
    format='%(asctime)s,%(msecs)-6.1f: %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger()
#log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)
SYSTEM_HOSTNAME = socket.gethostname()


def write_shell_command_file(user_shell_command):
    prefix = "gpu_job_command_"
    rnd = os.urandom(5).encode('hex')
    timestr = time.strftime('%y%m%d%H%M%S-', time.localtime())
    suffix = ".%s.tmp" % SYSTEM_HOSTNAME
    command_file_name = "".join([prefix, timestr, rnd, suffix])
    with open(command_file_name, 'w') as f:
        f.write(user_shell_command)
    return command_file_name


def main():
    u = "\n\n%prog 'shell command' [-o output-filename]"
    d = ("Submit job to Torque GPU queue. The shell command argument must be "
         "properly quoted (as shown in the example above). The working "
         "directory of the job will be the current working directory.")
    e = "Author: Jan-Philip Gehrcke (http://gehrcke.de)"
    parser = OptionParser(usage=u, description=d, epilog=e)
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

    # Write user-given shell command to a temp file with unique filename.
    command_file_name = write_shell_command_file(user_shell_command)

    # The torque-compute-job-wrapper expects three arguments:
    # "command_file_name" "working_directory" "stdout_stderr_filename"
    qsub_stdin = """torque-gpu-job-wrapper '%s' %s %s\n""" % (
        command_file_name, cwd, output_filename)

    # Request one node, one GPU and one virtual core. Tell qsub that the
    # job's working dir is the current working dir. Keep stdout and stderr in
    # home directory (as long as it leaks the wrapper redirection).
    qsub_command = [
        'qsub',
        '-l', 'nodes=1:gpus=1:ppn=1',
        '-d', cwd,
        '-V',
        '-k', 'oe']
    try:
        sp = Popen(args=qsub_command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = sp.communicate(qsub_stdin)
    except:
        # Remove tempfile and re-raise exception.
        os.remove(command_file_name)
        raise
    if out:
        print "qsub stdout:"
        print "\n".join(l.rstrip() for l in out.splitlines() if l.strip())
    if err:
        print "qsub stderr:"
        print "\n".join(l.rstrip() for l in err.splitlines() if l.strip())
    returncode = sp.returncode
    log.debug("qsub returncode: %s" % returncode)
    if returncode == 0:
        print "Submission success."
        if output_filename:
            print "Job stdout/stderr filename: '%s'." % output_filename
        else:
            print "Job stdout/stderr filename will be chosen automatically."
        print "Job command temporarily stored in '%s'. Don't delete." % (
            command_file_name)
    else:
        print "Submission error."
        os.remove(command_file_name)


if __name__ == "__main__":
    main()
