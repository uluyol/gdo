
from __future__ import print_function

import operator
import subprocess

import goless

# pylint: disable=multiple-statements

class ExecError(Exception):
	pass

def _is_str(x):
	return isinstance(x, str) or isinstance(x, bytes)

class _Vertex(object):
	def __init__(self, name, cmd, vid):
		assert _is_str(name)
		if not callable(cmd):
			assert isinstance(cmd, tuple)
			for c in cmd:
				assert _is_str(c)
		assert isinstance(vid, int)
		self._name = name
		self._cmd = cmd
		self._vid = vid

	@property
	def name(self): return self._name
	@property
	def cmd(self): return self._cmd
	@property
	def vid(self): return self._vid

class _Edge(object):
	def __init__(self, src, dst):
		assert isinstance(src, int)
		assert isinstance(dst, int)
		self._src = src
		self._dst = dst

	@property
	def src(self): return self._src
	@property
	def dst(self): return self._dst

def _make_vertices(*args):
	vertices = []
	vid_for = {}
	if len(args)%2 != 0:
		raise ValueError("missing command for name")
	for i in range(len(args)/2):
		name = args[i*2]
		cmd = args[i*2+1]
		assert _is_str(name)
		vid_for[name] = len(vertices)
		if not callable(cmd):
			cmd = _clean_cmd(cmd)
		vertices.append(_Vertex(name, cmd, len(vertices)))
	return vertices, vid_for

class RunGraph(object):
	"""RunGraph is the primary gdo type.
	
	RunGraphs are used to store the commands that must be executed
	and dependencies between them. To execute a RunGraph, see
	concurrent() and sequential().
	"""
	def __init__(self, *args):
		self._vertices, self._vid_for = _make_vertices(*args)
		self._edges = []
		self._log_dir = ""

	@property
	def vertices(self): return self._vertices
	@property
	def edges(self): return self._edges
	@property
	def vid_for(self): return self._vid_for
	@property
	def log_dir(self): return self._log_dir

	@classmethod
	def _dup(cls, rg):
		# pylint: disable=protected-access
		assert isinstance(rg, RunGraph)
		n = cls.__new__(cls)
		n._vertices = rg.vertices
		n._edges = rg.edges
		n._vid_for = rg.vid_for
		n._log_dir = rg.log_dir
		return n

	def _set_vertices(self, vs):
		assert isinstance(vs, list)
		for v in vs:
			assert isinstance(v, _Vertex)
		self._vertices = vs
		return self

	def _set_edges(self, edges):
		assert isinstance(edges, list)
		for e in edges:
			assert isinstance(e, _Edge)
		self._edges = edges
		return self

	def req(self, src, dst):
		"""Add a requirement that dst must happen after src."""
		# pylint: disable=protected-access
		assert _is_str(src)
		assert _is_str(dst)
		e = self.edges[:]
		e.append(_Edge(self.vid_for[src], self.vid_for[dst]))
		e.sort(key=operator.attrgetter("src", "dst"))
		return RunGraph._dup(self)._set_edges(e)

	def req_all(self, dst):
		"""Require everything before running dst."""
		# pylint: disable=protected-access
		assert _is_str(dst)
		vids = []
		dst_vid = None
		for i in range(len(self.vertices)):
			if self.vertices[i] != dst:
				vids.append(dst)
			else:
				dst_vid = i
		if dst_vid is None:
			raise ValueError("destination is not a vertex")
		e = self.edges[:]
		e.extend(_Edge(i, dst_vid) for i in vids)
		return RunGraph._dup(self)._set_edges(e)

	def with_log_dir(self, log_dir):
		"""Sets the destination dir for logs."""
		# pylint: disable=protected-access
		assert _is_str(log_dir)
		n = RunGraph._dup(self)
		n._log_dir = log_dir
		return n

def _clean_cmd(cmd):
	"""_clean_cmd canonicalizes commands into a tuple of strings."""
	if _is_str(cmd):
		cmd = cmd.split()
	if isinstance(cmd, list):
		cmd = tuple(cmd)
	assert isinstance(cmd, tuple)
	for p in cmd:
		assert _is_str(p)
	return cmd

def sequential(rg):
	"""Execute the RunGraph sequentially.

	This function will execute the RunGraph one item at a time. It
	is equivalent to running concurrent(rg, max_concurrent=1).

	Raises ExecExecution if a command errors.
	"""
	concurrent(rg, max_concurrent=1)

def concurrent(rg, max_concurrent=int(0)):
	"""Execute the RunGraph concurrently.

	This function will execute the RunGraph while respecting
	dependencies between commands. If any command fails, the
	execution of queued commands will be canceled. Changing the
	value of max_concurrent will limit the amount of concurrency
	possible so that at any given time, at most max_concurrent
	commands will be execution simultaneously.

	Raises ExecExecution if a command errors.
	"""
	assert isinstance(rg, RunGraph)
	assert isinstance(max_concurrent, int)

	deps = [set() for _ in rg.vertices]
	for e in rg.edges:
		deps[e.dst].add(e.src)

	to_complete = 0
	q = []
	for i in range(len(deps)):
		if len(deps[i]) == 0:
			q.append(rg.vertices[i])
			to_complete += 1

	jobs = goless.chan()
	results = goless.chan()

	def sendq():
		for v in q:
			jobs.send(v)
	goless.go(sendq)

	goless.go(lambda: _dispatcher(jobs, results, max_concurrent))

	complete = 0
	stop = False
	while True:
		res = None
		try:
			res = results.recv()
		except goless.ChannelClosed:
			break
		complete += 1
		if res.e is not None:
			jobs.close()
			stop = True
			print("ERROR", res.v.name)
		else:
			print("DONE ", res.v.name)
			if not stop:
				for e in _find_edges_from(rg.edges, res.v.vid):
					deps[e.dst].remove(res.v.vid)
					if len(deps[e.dst]) == 0:
						jobs.send(rg.vertices[e.dst])
						to_complete += 1
		if complete == to_complete:
			if not stop:
				jobs.close()
			break
	success = not stop
	if not success:
		raise ExecError("one or more of the tasks failed, see logs in " + rg.log_dir + " for details")

def _find_edges_from(edges, vid):
	l = int(0)
	r = int(len(edges))

	eid = -1
	while l < r:
		mid = int((l + r) / 2)
		if edges[mid].src > vid:
			r = mid
		elif edges[mid].src == vid:
			eid = mid
			break
		else:
			l = mid + 1
	if eid < 0:
		return []
	while eid-1 >= 0 and edges[eid-1].src == vid:
		eid -= 1
	end = eid
	while end+1 < len(edges) and edges[end+1].src == vid:
		end += 1
	return edges[eid:end+1]

def _dispatcher(jobs, results, max_concurrent):
	if max_concurrent <= 0:
		while True:
			job = None
			try:
				job = jobs.recv()
			except goless.ChannelClosed:
				break
			# need to use a separate factory function
			# so that job is closed inside function
			# instead of reusing job (which changes)
			goless.go(_mk_run_job(job, results))
		return

	def worker():
		while True:
			job_w = None
			try:
				job_w = jobs.recv()
			except goless.ChannelClosed:
				break
			_run_job(job_w, results)
	for _ in range(max_concurrent):
		goless.go(worker)

def _mk_run_job(job, results):
	return lambda: _run_job(job, results)

def _run_job(job, results):
	if callable(job.cmd):
		try:
			job.cmd()
		except (KeyboardInterrupt, SystemExit):
			raise
		except Exception as e:
			results.send(_Result(job, e))
			return
		results.send(_Result(job, None))
		return
	ret = subprocess.call(job.cmd)
	res = _Result(job, None)
	if ret != 0:
		res.e = ExecError("exit status " + str(ret))
	results.send(res)

class _Result(object):
	def __init__(self, vertex, err_code):
		self.v = vertex
		self.e = err_code