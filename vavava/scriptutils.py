#!/usr/bin/env python
# coding=utf-8

import os
import util
from ConfigParser import ConfigParser


class BaseConfig:

    def get_ini_attrs(self):
        raise  NotImplementedError()

    def get_args(self, argv):
        raise  NotImplementedError()

    def read_cmdline_config(self, ini, script, argv=None):
        self.ini = ini
        self.ini_attrs = self.get_ini_attrs()
        self.argv = argv
        self.script_path = util.script_path(script)
        if argv:
            self.parse_argv()
        else:
            self.parse_ini()
        return self

    def parse_ini(self):
        cfg = ConfigParser()
        if os.path.exists(self.ini):
            self.cfg = cfg.read(os.path.abspath(self.ini))
        else:
            self.ini = os.path.join(self.script_path, self.ini)
            self.cfg = cfg.read(self.ini)
        for k, v in self.ini_attrs.items():
            a, b, c = k.split('|')
            a = a.strip()
            b = b.strip()
            c = c.strip()

            if a == '' or b == '' or c == '':
                pass
            elif c in ('s'):
                setattr(self, b, cfg.get(section=a, option=b))
            elif c in('i'):
                setattr(self, b, cfg.getint(section=a, option=b))
            elif c in('f'):
                setattr(self, b, cfg.getfloat(section=a, option=b))
            elif c in('b'):
                setattr(self, b, cfg.getboolean(section=a, option=b))
            else:
                assert False

            if v:
                setattr(self, b, v(cfg))
        return cfg

    def parse_argv(self):
        self.parse_ini()
        args = self.get_args(self.argv)
        if args:
            for k, v in args.__dict__.items():
                if hasattr(self, k) and v is None:
                    continue
                setattr(self, k, v)

    def __str__(self):
        str = ''
        for k, v in self.ini_attrs.items():
            a, b, c = k.split('|')
            a = a.strip()
            b = b.strip()
            c = c.strip()
            str += '{} = {}\n'.format(b, getattr(self, b))
        return str


def get_log_from_config():
    def __func(cfg):
        LOGLVL = {
            'critical' : 50,
            'fatal' : 50,
            'error' : 40,
            'warning' : 30,
            'warn' : 30,
            'info' : 20,
            'debug' : 10,
            'notset' : 0
        }
        log_level = cfg.get('default', 'log_level')
        if log_level:
            log_level = LOGLVL[log_level.strip().lower()]
        else:
            log_level = LOGLVL['info']
        log_name = cfg.get('default', 'log')
        if log_name:
            return util.get_logger(logfile=log_name, level=log_level)
    return __func


def get_enabled_value_func(section, enable_name, value_name):
    def __func(cfg):
        if cfg.getboolean(section, enable_name):
            return cfg.get(section, value_name)
    return __func
