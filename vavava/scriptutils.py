#!/usr/bin/env python
# coding=utf-8

import os
import util
from ConfigParser import ConfigParser


class BaseConfig:

    def set_args(self, argv):
        raise  NotImplementedError()

    def set_ini_attrs(self, argv):
        raise  NotImplementedError()

    def read_cmdline_config(self, ini, argv=None, script=__file__):
        self.ini = ini
        self.ini_attrs = self.set_args()
        self.argv = argv
        self.script_path = util.script_path(script)
        if argv:
            self.parse_argv()
        else:
            self.parse_ini()
        return self.cfg

    def parse_ini(self):
        cfg = ConfigParser()
        if os.path.exists(self.ini):
            self.cfg = cfg.read(os.path.abspath(self.ini))
        else:
            self.ini = os.path.join(self.script_path, self.ini)
            self.cfg = cfg.read(self.ini)
        for k, v in self.ini_attrs.items():
            a, b = k.split('.')
            if not isinstance(v, str):
                setattr(self, b, v(cfg))
            elif v in ('s'):
                setattr(self, b, cfg.get(section=a, option=b))
            elif v in('i'):
                setattr(self, b, cfg.getint(section=a, option=b))
            elif v in('f'):
                setattr(self, b, cfg.getfloat(section=a, option=b))
            elif v in('b'):
                setattr(self, b, cfg.getboolean(section=a, option=b))
            else:
                assert False
        return cfg

    def parse_argv(self):
        self.parse_ini()
        args = self.set_args(self.argv)
        if args:
            for k, v in args.__dict__.items():
                if v:
                    setattr(self, k, v)

    def __str__(self):
        str = ''
        for k, v in self.ini_attrs.items():
            a, b = k.split('.')
            str += '{} = {}\n'.format(b, getattr(self, b))
        return str