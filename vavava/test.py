#!/usr/bin/env python
# coding=utf-8

import sys
import unittest
import os

import util
from vavava import httputil


__all__ = ['TestHttputil', 'TestUtil','TestSqliteutil']

util.set_default_utf8()

class TestHttputil(unittest.TestCase):
    def test_get(self):
        print 'test httputil.get()'
        url = r'http://www.baidu.com'
        client = httputil.HttpUtil()
        content = client.get(url)
        print len(content.decode('utf8'))

    def test_post(self):
        print 'test httputil.post()'
        return
        url = r'http://www.2kdy.com/search.asp'
        post = {'searchword': r'lie'}
        client = httputil.HttpUtil(charset='gb2312')
        content=client.post(url, post)
        print content

    def test_fetch(self):
        print 'test httputil.fetch()'
        url = r'http://pb.hd.sohu.com.cn/stats.gif?msg=caltime&vid=772959&tvid=596204&ua=pp&isHD=21&pid=348552429&uid=13832983422211404270&out=0&playListId=5029335&nid=353924663&tc=2400&type=vrs&cateid=&userid=&uuid=779b9c99-3c3a-52bc-2622-8bb0218cad5d&isp2p=0&catcode=101&systype=0&act=&st=144792%3B6560%3B143697%3B143699&ar=10&ye=2010&ag=5%u5C81%u4EE5%u4E0B&lb=2&xuid=&passport=&fver=201311211515&url=http%3A//tv.sohu.com/20120925/n353924663.shtml&lf=http%253A%252F%252Fv.baidu.com%252Fv%253Fword%253D%2525CA%2525AE%2525D2%2525BB%2525C2%2525DE%2525BA%2525BA%2526ct%253D301989888%2526rn%253D20%2526pn%253D0%2526db%253D0%2526s%253D0%2526fbl%253D800&autoplay=1&refer=http%3A//tv.sohu.com/20120925/n353924666.shtml&t=0.24127451563254'
        client = httputil.HttpUtil()
        #client.set_proxy({"http":"http://127.0.0.1:8087"})
        handle = httputil.DownloadStreamHandler(open('/Users/pk/Downloads/tmp.flv', 'w'), duration=10)
        client.fetch(url, handle)

    def test_miniaxel(self):
        url = r'http://localhost/w/video/mv.flv'
        n = 10
        orig_md5 = r'31e4a49026402f41770c3f78c658c685'
        multi = r'test_multi.flv'
        single = r'test_single.flv'
        multi_md5 = None
        singl_md5 = None
        try:
            with open(multi, 'w') as fp:
                httputil.MiniAxel().dl(url, fp=fp, n=9)
            with open(multi, 'w') as fp:
                httputil.MiniAxel().dl(url, fp=fp, n=1)
            with open(multi, 'rb') as fp:
                multi_md5 = util.md5_for_file(fp)
            with open(single, 'rb') as fp:
                singl_md5 = util.md5_for_file(fp)
        except Exception as e:
            print e
        finally:
            os.remove(multi)
            os.remove(single)
        self.assertTrue(orig_md5 == multi_md5)
        self.assertTrue(orig_md5 == singl_md5)


class TestUtil(unittest.TestCase):
    def test_assure_path(self):
        dir = r'/Users/pk/tmp/a/b/c/d'
        util.assure_path(dir)
        self.assertTrue(os.path.exists(dir))

    def test_check_cmd(self):
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


