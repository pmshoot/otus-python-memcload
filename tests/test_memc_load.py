import collections
import functools
import queue
import threading
import unittest

from memc_load import utils
from memc_load.memc_load_th import get_task_data, get_workers, parse_appsinstalled

options = collections.namedtuple("options", ["workers", "processes", "buffer", "dry", "idfa", "gaid", "adid", "dvid"])


def cases(cases):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args):
            for c in cases:
                new_args = args + (c if isinstance(c, tuple) else (c,))
                f(*new_args)

        return wrapper

    return decorator


class MockMemcacheClient:
    def __init__(self, *args, **kwargs):
        self._store = {}

    def set(self, key, packed):
        self._store[key] = packed

    def set_multi(self, data_map: dict):
        self._store.update(data_map)

    def get(self, key):
        return self._store.get(key)


class TestMemcLoadThreading(unittest.TestCase):
    """"""

    @cases([
        (options(1, 0, 5, False, None, None, None, None), 1),
        (options(4, 0, 2, False, None, None, None, None), 4),
        (options(10, 0, 1, False, None, None, None, None), 10),
    ])
    def test_get_workers(self, *data):
        opts, res = data
        task_queue = queue.Queue()
        workers = get_workers(task_queue, queue.Queue(), threading.Event(), opts, client_class=MockMemcacheClient)
        self.assertTrue(task_queue.empty())
        self.assertEqual(len(workers), res)
        for worker in workers:
            self.assertTrue(worker.is_alive(), 'worker not alive')

        for _ in workers:
            task_queue.put('quit')

        for worker in workers:
            worker.join()
            self.assertFalse(worker.is_alive(), 'worker must be die')

    @cases([
        (options(1, 0, 5, False, None, None, None, None), 1),
        (options(4, 0, 5, False, None, None, None, None), 4),
        (options(10, 0, 5, False, None, None, None, None), 10),
    ])
    def test_workers_result(self, *data):
        opts, res = data
        task_queue = queue.Queue()
        task_result_queue = queue.Queue()
        event = threading.Event()
        workers = get_workers(task_queue, task_result_queue, event, opts, client_class=MockMemcacheClient)
        for worker in workers:
            worker.errors = 1
            worker.processed = 1
        self.assertTrue(task_result_queue.empty(), 'task_result_queue is not empty. wrong state')
        self.assertFalse(event.is_set(), 'event is set. wrong state')

        for _ in workers:
            task_queue.put('result')
        task_queue.join()

        for _ in workers:
            result = task_result_queue.get(timeout=1)
            self.assertIsInstance(result, dict)
            self.assertEqual(len(result.keys()), 3)
            self.assertListEqual(list(result.keys()), ['worker', 'errors', 'processed'])
            self.assertEqual(result.get('errors'), 1)
            self.assertEqual(result.get('processed'), 1)
            task_result_queue.task_done()

        self.assertTrue(task_result_queue.empty())

        event.set()
        for _ in workers:
            task_queue.put('quit')
        for worker in workers:
            worker.join()

    @cases([
        ('idfa', "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23", 'idfa:1rfw452y52g2gq4g',
         b'\x08\x8f\x0b\x08+\x08\xb7\x04\x08\x03\x08\x07\x08\x17\x11fffff\xc6K@\x19\xf6(\\\x8f\xc25E@'),
        ('gaid', "gaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424", 'gaid:7rfw452y52g2gq4g',
         b'\x08\xff9\x08\xa8\x03\x11fffff\xc6K@\x19\xf6(\\\x8f\xc25E@'),
    ])
    def test_workers_set_data_memcache(self, *data):
        memc, line, memc_key, memc_val = data
        opts = options(1, 0, 2, False, None, None, None, None)
        task_queue = queue.Queue()
        task_result_queue = queue.Queue()
        event = threading.Event()
        workers = get_workers(task_queue, task_result_queue, event, opts, client_class=MockMemcacheClient)
        preprocessed = utils.Accumulator(buf_size=2)
        for _ in workers:
            appsinstalled = parse_appsinstalled(line)
            dev_type, key, packed = get_task_data(appsinstalled)
            preprocessed.put(dev_type, key, packed)
            while not preprocessed.is_empty():
                dev_type, data_map = preprocessed.pop_ready(drain=True)
                if dev_type:
                    task_queue.put((dev_type, data_map))

        for _ in workers:
            task_queue.put('result')
        task_queue.join()

        for worker in workers:
            _ = task_result_queue.get(timeout=1)
            task_result_queue.task_done()
            client = worker.device_memc.get(memc)
            self.assertIsNotNone(client)
            self.assertTrue(memc_key in client._store)
            data = client.get(memc_key)
            self.assertEqual(data, memc_val)

        event.set()
        for _ in workers:
            task_queue.put('quit')
        for worker in workers:
            worker.join()


class TestAccumulator(unittest.TestCase):
    def setUp(self) -> None:
        self.acc = utils.Accumulator(buf_size=3)

    @cases([
        ([], {}),
        ([('qwer', 1, 'lksjghlkajfh')], {'qwer': {1: 'lksjghlkajfh'}}),
        ([('qwer', 1, 'lksjghlkajfh'), ('qwer', 2, 'qwerty')], {'qwer': {1: 'lksjghlkajfh', 2: 'qwerty'}}),
        ([('qwer', 1, 'lksjghlkajfh'), ('qwer', 2, 'qwerty'), ('qwer', 2, 'qwerty')], {'qwer': {1: 'lksjghlkajfh', 2: 'qwerty'}}),
        ([('qwer', 1, 'lksjghlkajfh'), ('qwer', 2, 'qwerty'), ('rewq', 2, 'qwerty')], {'qwer': {1: 'lksjghlkajfh', 2: 'qwerty'}, 'rewq': {2: 'qwerty'}}),
    ])
    def test_put_data(self, data, res_map):
        acc = utils.Accumulator()
        for d in data:
            acc.put(*d)
        self.assertDictEqual(acc._store, res_map)

    @cases([
        ([], None),
        ([('qwer', 1, 'lksjghlkajfh')], None),
        ([('qwer', 1, 'lksjghlkajfh'), ('rewq', 1, 'lksjghlkajfh')], None),
        ([('qwer', 1, 'lksjghlkajfh'), ('qwer', 2, 'lksjghlkajfh')], ('qwer', {2: 'lksjghlkajfh', 1: 'lksjghlkajfh'})),
    ])
    def test_pop_ready(self, data, res):
        acc = utils.Accumulator(2)
        for d in data:
            acc.put(*d)
        dev_type, data_map = acc.pop_ready()
        if res is None:
            self.assertIsNone(dev_type)
            self.assertIsNone(data_map)
        else:
            dev_type_res, data_map_res = res
            self.assertEqual(dev_type, dev_type_res)
            self.assertDictEqual(data_map, data_map_res)

    @cases([
        ([('qwer', 1, 'lksjghlkajfh')], ('qwer', {1: 'lksjghlkajfh'})),
        ([('asdf', 1, 'lksjghlkajfh'), ('asdf', 2, 'lksjghlkajfh')], ('asdf', {2: 'lksjghlkajfh', 1: 'lksjghlkajfh'})),
    ])
    def test_pop_ready_drain(self, data, res):
        acc = utils.Accumulator(2)
        for d in data:
            acc.put(*d)
        dev_type, data_map = acc.pop_ready(drain=True)
        dev_type_res, data_map_res = res
        self.assertEqual(dev_type, dev_type_res)
        self.assertDictEqual(data_map, data_map_res)
        self.assertTrue(acc.is_empty())

    @cases([1, 2, 3, 4, 10])
    def test_buffer_size(self, buf_size):
        acc = utils.Accumulator(buf_size=buf_size)
        self.assertEqual(acc.buf_size, buf_size)

    def test_empty_clear(self):
        acc = utils.Accumulator(2)
        self.assertTrue(acc.is_empty())
        acc.put('123', 1, 'qwerqewrt')
        self.assertFalse(acc.is_empty())
        acc.clear()
        self.assertTrue(acc.is_empty())
