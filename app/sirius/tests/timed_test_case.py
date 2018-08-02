import time, unittest

class TimedTestCase(unittest.TestCase):
    """Customized TestCase with time reporting for individual tests."""
    def setUp(self):
        self._start_time = time.time()

    def tearDown(self):
        duration = time.time() - self._start_time
        print(f">> {self.shortDescription():60s} {duration:.2f} s")
