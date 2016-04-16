
from __future__ import print_function

import os
import sys
import threading

# pylint: disable=bad-builtin,deprecated-lambda

class _Color(object):
	CLEAR = "\033[0m"
	RED = "\033[0;31m"
	GREEN = "\033[0;32m"
	YELLOW = "\033[0;33m"
	BLUE = "\033[0;34m"

class _DummyLock(object):
	def __enter__(self, *a):
		pass

	def __exit__(self, *a):
		pass

class _Enum(frozenset):
	def __getattr__(self, name):
		if name in self:
			return name
		raise AttributeError

_ProcStates = _Enum({"OK", "SKIP", "INT", "ERR", "TODO"})

class _Display(object):
	def __init__(self, try_fancy=True):
		self._mu = threading.RLock()
		self._state = []
		self._wip_count = 0
		self._fancy = try_fancy and sys.stdout.isatty() and os.getenv("TERM", "dumb") != "dumb"

	def append(self, title):
		with self._mu:
			assert sum(1 for row in self._state if row[1] == title) == 0
			self._state.append([_ProcStates.TODO, title, False])
			self._wip_count += 1

	def set(self, title, state):
		assert state in _ProcStates
		with self._mu:
			row = list(filter(lambda row: row[1] == title, self._state))
			assert len(row) == 1
			row[0][0] = state
			self.draw()

	def skiprest(self):
		with self._mu:
			for i in range(len(self._state)):
				if self._state[i][0] == _ProcStates.TODO:
					self._state[i][0] = _ProcStates.SKIP

	def draw(self, everything=False):
		with self._mu:
			self._outreset()
			for i in range(len(self._state)):
				state, name, has_drawn = self._state[i]
				if has_drawn:
					continue
				if state != _ProcStates.TODO or everything:
					self._outstate(name, state)
					self._state[i][2] = True
					self._wip_count -= 1
			self._outstatus()
			if everything and self._fancy:
				print()

	def _outreset(self):
		if self._fancy:
			sys.stdout.write("\r\033[K")
			sys.stdout.flush()

	def _outstate(self, name, state):
		if self._fancy:
			color = ""
			if state == _ProcStates.OK:
				color = _Color.GREEN
			elif state == _ProcStates.ERR or state == _ProcStates.INT:
				color = _Color.RED
			elif state == _ProcStates.SKIP:
				color = _Color.YELLOW
			fmt = " " + color + "% 4s " + _Color.CLEAR + "%s"
			print(fmt % (state, name))
		else:
			print(" % 4s %s" % (state, name))
		sys.stdout.flush()

	def _outstatus(self):
		if self._fancy:
			n = len(self._state)
			print("%d/%d complete" % (n-self._wip_count, n), end="")
			sys.stdout.flush()
