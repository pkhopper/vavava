#!/usr/bin/env python
# coding=utf-8

import urllib
import urllib2
import socket
import cookielib # import LWPCookieJar
from io import BytesIO
from vavava import util

DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 6.1; ' \
                     'en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
DEFAULT_REFERER = "http://www.baidu.com/"
DEFAULT_BUFFER_SIZE = 1024*1024
DEFAULT_CHARSET = "utf8"
DEFAULT_TIMEOUT = 30 #MS
DEFAULT_DEBUG_LVL = 0

class HttpUtil(object):
    """ a simple client of http"""
    def __init__(self,
                 charset=DEFAULT_CHARSET,
                 timeout=DEFAULT_TIMEOUT,
                 debug_lvl=DEFAULT_DEBUG_LVL,
                 proxy=None,
                 log=None):
        self._cookie = cookielib.CookieJar()
        self._timeout = timeout
        self._proxy = proxy
        self._opener = None
        self._buffer_size = DEFAULT_BUFFER_SIZE
        self._charset = DEFAULT_CHARSET
        self._set_debug_level(debug_lvl)
        self._headers = {}
        self._headers['Referer'] = DEFAULT_REFERER
        self._headers['User-Agent'] = DEFAULT_USER_AGENT
        self._headers['Connection'] = 'keep-alive'
        self._headers['Accept'] = r'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        self._headers['Cache-Control'] = 'max-age=0'

    def get(self, url, headers=None):
        self.response = self._request(url, headers=headers)
        return self.response.read()

    def post(self, url, post_dic, headers=None):
        self.response = self._request(url, headers=headers,
            post_data=urllib.urlencode(post_dic).encode(self._charset)
        )
        return self.response.read()

    def fetch(self, url, handler, post_data=None, headers=None):
        if handler is None:
            raise ValueError, "need handler"
        handler.set_parent(self)
        resp = self._request(url, post_data=post_data, headers=headers)
        handler.handle(req=None, resp=resp)

    def parse_charset(self):
        if self.response:
            import re
            charset = re.search(r'charset=([\w-]+)',
                             self.response.headers['content-type'])
            if not charset:
                raise ValueError('content-type filed not found.')
            return charset.group(1)

    def add_header(self, key, value):
        self._headers[key] = value

    def set_proxy(self, proxy):
        self._proxy = proxy

    def _set_debug_level(self, level=0):
        from httplib import HTTPConnection
        HTTPConnection.debuglevel = level
        self._debug_lvl = level

    def _request(self, url, post_data=None, headers=None):
        #if self.handler and hasattr(self.handler, 'start'):
        #    self._headers['Content-Range'] = 'bytes %s-%s/%s' % (handler.start, handler.end, handler.len)
        #    self._headers['Content-Length'] = str(handler.len - handler.start)
        if self._opener is None:
            self._opener = urllib2.build_opener(ContentEncodingProcessor())
        if self._cookie != None:
            self._opener.add_handler(urllib2.HTTPCookieProcessor(self._cookie))
        if self._proxy != None:
            self._opener.add_handler(urllib2.ProxyHandler(self._proxy))
        if headers:
            for k, v in self._headers.items():
                if not headers.has_key(k):
                    headers[k] = v
        else:
            headers = self._headers
        req = urllib2.Request(url, data=post_data, headers=headers)
        return self._opener.open(req, timeout=self._timeout)

import time
import threading
class DownloadStreamHandler:
    def __init__(self, fp, duration=0, size=DEFAULT_BUFFER_SIZE):
        self.parent = None
        self.buff_size = size
        self.fp = fp
        self.duration = duration
        self.ev = threading.Event()
        if duration > 0:
            self.stop_time = duration + time.time()

    def syn_stop(self):
        self.ev.set()

    def set_parent(self, p):
        self.parent = p

    def handle(self, req, resp):
        self.ev.clear()
        while not self.ev.is_set() \
            and self._handle(resp.read(self.buff_size)):
            pass

    def _handle(self, data):
        if not data:
            return False
        self.fp.write(data)
        return self.duration <= 0 or self.stop_time > time.time()

import gzip
import zlib
class ContentEncodingProcessor(urllib2.BaseHandler):
    """A handler to add gzip capabilities to urllib2 requests """
    # add headers to requests

    def __init__(self):
        try:
            self.deflate = lambda data: zlib.decompress(data, -1*zlib.MAX_WBITS)
        except zlib.error:
            self.deflate = lambda data: zlib.decompress(data)

    def http_request(self, req):
        req.add_header("Accept-Encoding", "gzip,deflate")
        return req

    def http_response(self, req, resp):
        old_resp = resp
        if resp.headers.get("content-encoding") == "gzip":
            gz = gzip.GzipFile( fileobj=BytesIO(resp.read()), mode="r" )
            resp = urllib2.addinfourl(gz, old_resp.headers, old_resp.url, old_resp.code)
            resp.msg = old_resp.msg
            # deflate
        if resp.headers.get("content-encoding") == "deflate":
            gz = BytesIO( self.deflate(resp.read()) )
            resp = urllib2.addinfourl(gz, old_resp.headers, old_resp.url, old_resp.code)
            resp.msg = old_resp.msg
        return resp

