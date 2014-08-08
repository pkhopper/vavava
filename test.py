#!/usr/bin/env python
# coding=utf-8

import sys
import unittest
import os

from vavava import util
from vavava import httputil
from vavava import threadutil

sys.path.insert(0, '.')


__all__ = ['TestHttputil', 'TestUtil', 'TestSqliteutil']

util.set_default_utf8()


class TestHttputil(unittest.TestCase):
    url = r'http://cdn.mysql.com/Downloads/Connector-J/mysql-connector-java-gpl-5.1.31.msi'
    orig_md5 = r'140c4a7c9735dd3006a877a9acca3c31'
    
    def test_get(self):
        print 'test httputil.get()'
        url = r'http://www.baidu.com'
        client = httputil.HttpUtil()
        content = client.get(url)
        print len(content.decode('utf8'))

    def test_fetch(self):
        print 'test httputil.fetch()'
        httputil.main_test()


class TestUtil(unittest.TestCase):
    def test_assure_path(self):
        print 'test_assure_path'
        tmp_path = r'./tmp/a/b/c/d'
        util.assure_path(tmp_path)
        self.assertTrue(os.path.exists(tmp_path))

    def test_check_cmd(self):
        print 'test_check_cmd'
        self.assertTrue(util.check_cmd('ls'))
        self.assertTrue(not util.check_cmd('XXXXX'))
        self.assertTrue(util.check_cmd('ls'))


class TestSqliteutil(unittest.TestCase):
    def test_ok(self):
        print 'test sqliteutil ok'


class TestThreadutil(unittest.TestCase):
    def test_ok(self):
        print 'test threadutil ok'
        threadutil.ws_test()


def make_suites():
    test_cases = {
        'httputil': 'TestHttputil',
        'util': 'TestUtil',
        'sqliteuitl': 'TestSqliteutil',
        'threadutil': 'TestThreadutil'
    }
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        cases = [x for y, x in test_cases.items()]
    else:
        cases = [test_cases[x] for x in sys.argv[1:]]
    mod = sys.modules[__name__]
    for cls_name in cases:
        testcase = getattr(mod, cls_name)
        for attr, obj in testcase.__dict__.items():
            if attr.startswith('test_'):
                suite.addTest(testcase(attr))
    return suite


if __name__ == "__main__":
    try:
        runner = unittest.TextTestRunner()
        runner.run(make_suites())
    except KeyboardInterrupt as e:
        print 'stop by user'
        exit(0)
    except Exception as e:
        raise
    finally:
        pass