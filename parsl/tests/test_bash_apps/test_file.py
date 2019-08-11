import argparse
import os

import pytest

import parsl
import parsl.app.errors as perror
from parsl.app.app import App
from parsl.tests.configs.local_threads import config


@App('bash')
def echo_to_streams(msg, stderr='std.err', stdout='std.out'):
    return 'echo "{0}"; echo "{0}" >&2'.format(msg)

def test_stdout_abspath():

    out = os.getcwd() + '/t1.out'
    err = os.getcwd() + '/t1.err'
    os.system('rm -f ' + out + ' ' + err)

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len1 = len(open(out).readlines())
    assert len1 == 1,  "Line count of first output should be 1 but len1={}".format(len1)

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len2 = len(open(out).readlines())
    assert len2 == 2,  "Line count of second output should be 2 but len2={}".format(len2)

    os.system('rm -f ' + out + ' ' + err)


def test_stdout_subdir():

    out = 'subdir/t1.out'
    err = 'subdir/t1.err'
    os.system('rm -f ' + out + ' ' + err)

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len1 = len(open(out).readlines())
    assert len1 == 1,  "Line count of first output should be 1 but len1={}".format(len1)

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len2 = len(open(out).readlines())
    assert len2 == 2,  "Line count of second output should be 2 but len2={}".format(len2)

    os.system('rm -f ' + out + ' ' + err)

