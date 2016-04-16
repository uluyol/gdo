# pylint: disable=too-few-public-methods

import gevent.queue

class _ChanClosed(Exception):
	pass

class _RWChan(object):
	def __init__(self):
		self._c = gevent.queue.Channel()
		self._closed = False

	def close(self):
		self._closed = True

	def send(self, msg):
		if self._closed:
			raise _ChanClosed()
		return self._c.put(msg)

	def recv(self):
		if self._closed:
			raise _ChanClosed()
		return self._c.get()

	def ro(self):
		return _RChan(self)

	def wo(self):
		return _WChan(self)

class _RChan(object):
	def __init__(self, rwc):
		self._rw = rwc

	def recv(self):
		return self._rw.recv()

class _WChan(object):
	def __init__(self, rwc):
		self._rwc = rwc

	def send(self, msg):
		self._rwc.send(msg)

	def close(self):
		self._rwc.close()

def _chan():
	return _RWChan()
