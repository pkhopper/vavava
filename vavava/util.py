﻿#!/usr/bin/env python
# coding=utf-8

import re
import sys
import os
import time
import chardet
import json


if sys.version < '3':
    set_default_utf8 = lambda: reload(sys).setdefaultencoding("utf8")
else:
    set_default_utf8 = None

get_time_string = lambda: time.strftime("%Y%m%d%H%M%S", time.localtime())
get_charset = lambda ss: chardet.detect(ss)['encoding']
file_sufix = lambda name: os.path.splitext(name)[1][1:]


import signal
import threading
import subprocess



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


def script_path(file_fullname):
    """ return path of file_fullname
        if file_full_name is link file, return origin file path
    """
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


def reg_1(pattern, string):
    m = re.search(pattern, string)
    if m:
        return m.group(1)


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


def assure_path(path):
    fullpath = os.path.abspath(path)
    path_trace = []
    while fullpath != '/' and not os.path.exists(fullpath):
        path_trace.append(fullpath)
        fullpath = os.path.dirname(fullpath)
    path_trace.reverse()
    for dir in path_trace:
        os.mkdir(dir)
    return os.path.exists(path)


def walk_dir(top, topdown=True, onerror=None, followlinks=False):
    """
    os.walk() ==> top, dirs, nondirs
    walk() ==> top, dirs, files, dirlinks, filelinks, others
    """
    isfile, islink, join, isdir = os.path.isfile, os.path.islink, os.path.join, os.path.isdir
    try:
        names = os.listdir(top)
    except Exception as e:
        pass
    # except os.error, os.err:
    #     if onerror is not None:
    #         onerror(os.err)
    #     return

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


def md5_for_file(f, block_size=2**20):
    import hashlib
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()


def check_cmd(cmd):
    for cmdpath in os.environ['PATH'].split(':'):
        if os.path.isdir(cmdpath) and cmd in os.listdir(cmdpath):
            return True


class Monitor:
    def __init__(self, log=None):
        self.last = time.time()
        self.log = log
    def report(self, str, duration=5):
        if self.last + duration < time.time():
            if self.log:
                self.log.error(str)
            else:
                sys.stderr.writelines([str, '\n'])
            self.last = time.time()


from io import BytesIO
class SynFileContainer:
    def __init__(self, fp):
        self.mutex = threading.Lock()
        if hasattr(fp, 'read'):
            self.__fp = fp
            self.name = fp.name
        elif isinstance(fp, BytesIO):
            self.__fp = fp
            self.name = 'memory_file.%s' % hash(fp)
        else:
            raise ValueError('must be a file or ByteIO')

    def seek_write(self, b, pos=-1, whence=0):
        with self.mutex:
            if not self.__fp.closed:
                if pos != -1:
                    self.__fp.seek(pos, whence)
                self.__fp.write(b)


def get_local_ip(ifname='eth0'):
    if sys.version >= '3':
        import socket
        import fcntl
        import struct
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])


def splitfile(f, strip_line=None, del_empty_line=True):
    if not os.path.exists(f):
        return []
    if not os.path.isfile(f):
        raise ValueError('should be a file, {} is dir not file.'.format(f))
    with open(f, 'r') as fp:
        results = fp.readlines()
        if strip_line:
            results = [l.strip(strip_line) for l in results]
        if del_empty_line:
            results = [l for l in results if l != '']
    return results


class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        # verion 3.*
        if sys.version >= '3':
            try:
                self.process = subprocess.Popen(self.cmd, shell=True)
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                print("timeout ....")
                return None
        # version 2.*
        def target():
            print('Thread started')
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()
            print('Thread finished')
        thread = threading.Thread(target=target)
        thread.start()
        while True:
            thread.join(1)
            if not thread.is_alive():
                return self.process.returncode
            if timeout > 0:
                timeout -= 1
            else:
                print('Terminating process')
                self.process.terminate()
                break

        timeout = 3

        while True:
            thread.join(1)
            if not thread.is_alive():
                return self.process.returncode
            if timeout > 0:
                timeout -= 1
            else:
                print('kill process')
                self.process.kill()

        return self.process.returncode