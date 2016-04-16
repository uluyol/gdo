import gdo

def f():
	import time
	time.sleep(10)

gdo.concurrent(
	gdo.RunGraph(
		"slee", "sleep 2",
		"slle2", "sleep 5",
		"pysleep", f,
		"true", "true")
	.req("slee", "true")
)