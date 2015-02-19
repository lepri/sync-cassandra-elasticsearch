#!/usr/bin/env python

import sys, os, time, atexit, argparse

import signal

finalize = False;


def signal_handler(signum, frame):
    global finalize
    if signum == signal.SIGTERM or signum == signal.SIGINT or signum == signal.SIGQUIT:
        finalize = True

