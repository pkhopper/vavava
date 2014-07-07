#!/usr/bin/env python
# coding=utf-8

import re
import sys
import os
import time
import chardet
import json

get_time_string = lambda: time.strftime("%Y%m%d%H%M%S", time.localtime())
get_charset = lambda ss: chardet.detect(ss)['encoding']
set_default_utf8 = lambda: reload(sys).setdefaultencoding("utf8")
get_file_sufix = lambda name: os.path.splitext(name)[1][1:]

import signal
import threading

class SignalHandlerBase:
    """handle SIGTERM signal for current process """
    def __init__(self, sig=signal.SIGINT, callback=None):
        signal.signal(sig, lambda signum, frame:self._handle(signum, frame))
        self.callback, self.sig = callback, sig
        if not callback:
            self.event = threading.Event()
            self.callback = lambda :self.event.set()
            self.isSet = lambda :self.event.isSet()
    def _handle(self, signum, frame):
        if signum is self.sig:
            if self.callback:
                self.callback()

import logging


def get_logger(logfile=None, level=logging.DEBUG):
    logger = logging.getLogger()
    if logfile:
        hdlr = logging.FileHandler(logfile)
        hdlr.setLevel(level=level)
        formatter = logging.Formatter("%(asctime)s[%(levelname)s] %(message)s")
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter("%(asctime)s[%(levelname)s] %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)
    logger.setLevel(level)
    return logger

def get_file_path(file_fullname):
    script_path = file_fullname
    if os.path.islink(file_fullname):
        script_path = os.path.dirname(os.path.abspath(os.readlink(file_fullname)))
    else:
        script_path = os.path.dirname(os.path.abspath(file_fullname))
    return script_path

class JsonConfig:
    def __init__(self, path=None, *attrs):
        for attr in attrs:
            setattr(self, attr, None)
        if path:
            for k, v in json.load(open(path)).items():
                setattr(self, k, v)


def reg_helper(text, reg_str="", mode=re.I | re.S):
    reg = re.compile(reg_str, mode)
    return reg.findall(text)


def import_any_module(name):
    """ import module at any place """
    try:
        return __import__(name, fromlist=[''])
    except:
        try:
            i = name.rfind('.')
            mod = __import__(name[:i], fromlist=[''])
            return getattr(mod, name[i + 1:])
        except:
            raise RuntimeError('No module of: %s found' % name)


def readlines(name):
    """ open().readlines() """
    fullname = os.path.abspath(name)
    path = os.path.split(fullname)[0]
    if not os.path.isdir(path):
        os.makedirs(path)
    return open(name, "r").readlines()


def assure_path(path):
    fullpath = os.path.abspath(path)
    path_trace = []
    while fullpath != '/' and not os.path.exists(fullpath):
        path_trace.append(fullpath)
        fullpath = os.path.dirname(fullpath)
    path_trace.reverse()
    for dir in path_trace:
        os.mkdir(dir)


def walk_dir(top, topdown=True, onerror=None, followlinks=False):
    """
    os.walk() ==> top, dirs, nondirs
    walk() ==> top, dirs, files, dirlinks, filelinks, others
    """
    isfile, islink, join, isdir = os.path.isfile, os.path.islink, os.path.join, os.path.isdir
    try:
        names = os.listdir(top)
    except os.error, os.err:
        if onerror is not None:
            onerror(os.err)
        return

    dirs, files, dlns, flns, others = [], [], [], [], []
    for name in names:
        fullname = join(top, name)
        if isdir(fullname):
            if islink(fullname):
                dlns.append(name)
            else:
                dirs.append(name)
        elif isfile(fullname):
            if islink(fullname):
                flns.append(name)
            else:
                files.append(name)
        else:
            others.append(name)

    if topdown:
        yield top, dirs, files, dlns, flns, others

    for name in dirs:
        for x in walk_dir(join(top, name), topdown, onerror, followlinks):
            yield x

    if followlinks is True:
        for dlink in dlns:
            for x in walk_dir(join(top, dlink), topdown, onerror, followlinks):
                yield x

    if not topdown:
        yield top, dirs, files, dlns, flns, others
