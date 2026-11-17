import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from outbox import Outbox

class OutboxTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'test.db'
        self.box = Outbox(self.path)
    def tearDown(self): self.temp.cleanup()
    def test_retry_returns_same_job(self):
        first = self.box.enqueue('c1', {'b':2,'a':1})
        self.assertEqual(first, self.box.enqueue('c1', {'a':1,'b':2}))
        self.assertEqual(len(self.box.jobs()), 1)
    def test_payload_mismatch(self):
        self.box.enqueue('c1', {'a':1})
        with self.assertRaises(ValueError): self.box.enqueue('c1', {'a':2})
    def test_concurrent_enqueue(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            ids = list(pool.map(lambda _: self.box.enqueue('same', {'text':'hello'}), range(30)))
        self.assertEqual(len(set(ids)), 1)
        self.assertEqual(len(self.box.jobs()), 1)
    def test_survives_reopen(self):
        self.box.enqueue('c1', {})
        self.assertEqual(len(Outbox(self.path).jobs()), 1)
    def test_delivery_and_no_repeat(self):
        self.box.enqueue('c1', {'text':'hello'})
        delivered=[]
        self.assertEqual(self.box.process_one(lambda *v: delivered.append(v), now=0), 'sent')
        self.assertIsNone(self.box.process_one(lambda *v: delivered.append(v), now=0))
        self.assertEqual(delivered, [('c1', {'text':'hello'})])
    def test_backoff_and_dead_letter(self):
        self.box.enqueue('c1', {})
        def fail(*args): raise RuntimeError('simulated failure')
        self.assertEqual(self.box.process_one(fail, 0), 'pending')
        self.assertIsNone(self.box.process_one(fail, 0))
        self.assertEqual(self.box.process_one(fail, 1), 'pending')
        self.assertIsNone(self.box.process_one(fail, 2))
        self.assertEqual(self.box.process_one(fail, 3), 'dead')
        self.assertIsNone(self.box.process_one(fail, 100))
        self.assertEqual(self.box.jobs()[0]['attempts'], 3)
    def test_invalid_key(self):
        with self.assertRaises(ValueError): self.box.enqueue('', {})
    def test_empty_queue(self): self.assertIsNone(self.box.process_one(lambda *args: None, 0))
    def test_rollback_on_process_interruption(self):
        self.box.enqueue('c1', {})
        def interrupt(*args): raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt): self.box.process_one(interrupt, 0)
        self.assertEqual(self.box.jobs()[0]['attempts'], 0)

if __name__ == '__main__': unittest.main()
