#!/usr/bin/env python
# coding=utf-8

import sys
import unittest
import os

from vavava import util
from vavava import httputil

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

    def test_post(self):
        print 'test_post'
        pass
        # print 'test httputil.post()'
        # url = r'http://www.2kdy.com/search.asp'
        # post = {'searchword': r'lie'}
        # client = httputil.HttpUtil(charset='gb2312')
        # content=client.post(url, post)
        # print content

    def test_fetch(self):
        print 'test httputil.fetch()'
        client = httputil.HttpUtil()
        #client.set_proxy({"http":"http://127.0.0.1:8087"})
        with open('tmp', 'w') as fp:
            handle = httputil.DownloadStreamHandler(fp, duration=30)
            client.fetch(TestHttputil.url, handle)
        with open('tmp', 'r') as fp:
            self.assertTrue(TestHttputil.orig_md5 == util.md5_for_file(fp))
        os.remove('tmp')

    def test_miniaxel(self):
        print 'test_miniaxel'
        multi = r'test_multi'
        single = r'test_single'
        multi_md5 = singl_md5 = ''
        try:
            progress_bar = httputil.ProgressBar()
            axel = httputil.MiniAxel(progress_bar=progress_bar)
            axel.dl(TestHttputil.url, out=multi, n=9)
            axel.dl(TestHttputil.url, out=single, n=1)
            with open(multi, 'rb') as fp:
                multi_md5 = util.md5_for_file(fp)
                print ''
                print multi_md5
            with open(single, 'rb') as fp:
                singl_md5 = util.md5_for_file(fp)
                print ''
                print singl_md5
        except Exception as e:
            print e
        finally:
            if os.path.exists(multi):
                os.remove(multi)
            if os.path.exists(single):
                os.remove(single)
        self.assertTrue(TestHttputil.orig_md5 == multi_md5)
        self.assertTrue(TestHttputil.orig_md5 == singl_md5)


class TestUtil(unittest.TestCase):
    def test_assure_path(self):
        print 'test_assure_path'
        tmp_path = r'/Users/pk/tmp/a/b/c/d'
        util.assure_path(tmp_path)
        self.assertTrue(os.path.exists(tmp_path))

    def test_check_cmd(self):
        print 'test_check_cmd'
        self.assertTrue(util.check_cmd('axel'))
        self.assertTrue(not util.check_cmd('XXXXX'))
        self.assertTrue(util.check_cmd('ls'))


class TestSqliteutil(unittest.TestCase):
    def test_ok(self):
        print 'test sqliteutil ok'


def make_suites():
    test_cases = {
        'httputil': 'TestHttputil',
        'util': 'TestUtil',
        'sqliteuitl': 'TestSqliteutil'
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