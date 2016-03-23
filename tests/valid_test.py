import unittest

import gdo

class ValidationTest(unittest.TestCase):
	def test(self):
		value_error_cases = [
			("",),
			("", (), "adsf"),
		]
		assert_error_cases = [
			("", 2),
			(1, 1),
			(None, None),
			((), ""),
		]
		for c in value_error_cases:
			with self.assertRaises(ValueError):
				gdo.RunGraph(*c)
		for c in assert_error_cases:
			with self.assertRaises(AssertionError):
				gdo.RunGraph(*c)

class CreationTest(unittest.TestCase):
	def test(self):
		cases = [
			("first echo", "echo hello world"),
			("f1", "echo 2", "g2", ("sleep", "1"), "q1", "true"),
		]
		for c in cases:
			self.assertIsInstance(gdo.RunGraph(*c), gdo.RunGraph)

class BasicConcurrentTest(unittest.TestCase):
	def test(self):
		gdo.concurrent(
			gdo.RunGraph(
				"e", "echo 1",
				"f", ("echo", "2"))
			.req("f", "e")
		)
		with self.assertRaises(gdo.ExecError):
			gdo.concurrent(
				gdo.RunGraph(
					"t", ("true",),
					"f", "false")
				.req("t", "f")
			)
		gdo.concurrent(
			gdo.RunGraph(
				"py", lambda: 1,
				"cmd", "true")
		)
		def f():
			raise Exception
		with self.assertRaises(gdo.ExecError):
			gdo.concurrent(gdo.RunGraph("pyfn", f))
