#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import glob
import gzip
import logging
import multiprocessing as mp
import os
import queue
import sys
import threading
import time
from optparse import OptionParser

# pip install python-memcached
from memcache import Client

# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
from memc_load import appsinstalled_pb2
from utils import Accumulator

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class ProcessWorker(mp.Process):
    def __init__(self, task_queue, task_result_queue, event, opts):
        super().__init__()
        self.task_queue: mp.JoinableQueue = task_queue
        self.task_result_queue: mp.JoinableQueue = task_result_queue
        self.event: mp.Event = event
        self.opts = opts
        self.worker_task_queue = mp.JoinableQueue(maxsize=100)
        self.worker_result_queue = mp.JoinableQueue()
        self.worker_event = mp.Event()
        self.worksers = self.get_workers()

    def run(self) -> None:
        """"""
        errors = 0
        preprocessed = Accumulator(buf_size=self.opts.buffer)
        while True:
            line = self.task_queue.get()
            if line == 'result':
                # drain accumulator
                while not preprocessed.is_empty():
                    dev_type, data_map = preprocessed.pop_ready(drain=True)
                    if dev_type:
                        self.worker_task_queue.put((dev_type, data_map))

                result_total = {
                    'errors': 0,
                    'processed': 0,
                    'worker': self.name,
                }

                for _ in range(len(self.worksers)):
                    self.worker_task_queue.put(line)
                self.worker_task_queue.join()

                for _ in range(len(self.worksers)):
                    worker_result = self.worker_result_queue.get()
                    logging.debug(f'{self.name}: got results: {worker_result}')
                    result_total['errors'] += worker_result['errors']
                    result_total['processed'] += worker_result['processed']
                    self.worker_result_queue.task_done()
                if errors:
                    result_total['errors'] += errors
                self.task_result_queue.put(result_total)
                logging.debug(f'Process {self.name}: Результаты выгружены: {result_total}')
                self.task_queue.task_done()
                logging.debug(f'Process {self.name}: ожидание сигнала продолжения')
                self.event.wait()
                self.worker_event.set()
                errors = 0
            elif line == 'quit':
                # завершаем работу и закрываем воркеров
                self.task_queue.task_done()
                for _ in range(len(self.worksers)):
                    self.worker_task_queue.put(line)
                for worker in self.worksers:
                    worker.join()
                logging.debug('Workers end up')
                break
            else:
                appsinstalled = self.parse_appsinstalled(line)
                if appsinstalled:
                    ua = appsinstalled_pb2.UserApps()
                    ua.lat = appsinstalled.lat
                    ua.lon = appsinstalled.lon
                    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
                    ua.apps.extend(appsinstalled.apps)
                    packed = ua.SerializeToString()

                    preprocessed.put(appsinstalled.dev_type, key, packed)
                    _, data_map = preprocessed.pop_ready()
                    if data_map:
                        # в тред
                        self.worker_task_queue.put((appsinstalled.dev_type, data_map))
                else:
                    errors += self.opts.buffer
                self.task_queue.task_done()
                if self.worker_event.is_set():
                    self.worker_event.clear()

    def get_workers(self):
        workers = []
        for _ in range(self.opts.workers):
            w = ThreadWorker(self.worker_task_queue, self.worker_result_queue, self.worker_event, self.opts)
            w.start()
            workers.append(w)
        return workers

    def parse_appsinstalled(self, line):
        line_parts = line.strip().split("\t")
        if len(line_parts) < 5:
            return
        dev_type, dev_id, lat, lon, raw_apps = line_parts
        if not dev_type or not dev_id:
            return
        try:
            apps = [int(a.strip()) for a in raw_apps.split(",")]
        except ValueError:
            apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
            logging.info("Not all user apps are digits: `%s`" % line)
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info("Invalid geo coords: `%s`" % line)
        return AppsInstalled(dev_type, dev_id, lat, lon, apps)


class ThreadWorker(threading.Thread):
    def __init__(self, worker_task_queue, worker_result_queue, event, opts):
        super().__init__()
        self.worker_task_queue = worker_task_queue
        self.worker_result_queue = worker_result_queue
        self.event = event
        self.dry_run = opts.dry
        self.errors = self.processed = 0
        self.device_memc = self.get_memc(opts)

    @staticmethod
    def get_memc(server):
        return {
            "idfa": Client([opts.idfa]),
            "gaid": Client([opts.gaid]),
            "adid": Client([opts.adid]),
            "dvid": Client([opts.dvid]),
        }

    def run(self) -> None:
        """"""
        while True:
            # обрабатываем очередь
            try:
                task = self.worker_task_queue.get(timeout=2)
            except queue.Empty:
                logging.debug(f'{self.name}: can\'t get task from {self.worker_task_queue}')
                continue
            if isinstance(task, str):
                if task == 'result':
                    # файл прочитан, очередь отработана, выгружаем результат
                    res = {
                        'worker': self.name,
                        'errors': self.errors,
                        'processed': self.processed
                    }
                    self.worker_result_queue.put(res)
                    self.worker_task_queue.task_done()
                    logging.debug(f'{self.name}: Результаты выгружены: {res}')
                    self.errors = self.processed = 0
                    logging.debug(f'{self.name}: ожидание сигнала продолжения')
                    self.event.wait()  # ждем чтения следующего файла
                    continue
                elif task == 'quit':
                    self.worker_task_queue.task_done()
                    logging.debug(f'{self.name}: Выход')
                    break
            else:
                dev_type, data_map = task
                memc = self.device_memc.get(dev_type)
                if not memc:
                    self.errors += 1
                    logging.error("%s: Unknown device type: %s" % (self.name, task.dev_type))
                    continue
                ok = self.insert_appsinstalled(memc, data_map, self.dry_run)
                if ok:
                    self.processed += len(data_map)
                else:
                    self.errors += len(data_map)
                self.worker_task_queue.task_done()

    def insert_appsinstalled(self, memc, data_map, dry_run=False):
        # ua = appsinstalled_pb2.UserApps()
        # ua.lat = task.lat
        # ua.lon = task.lon
        # key = "%s:%s" % (task.dev_type, task.dev_id)
        # ua.apps.extend(task.apps)
        # packed = ua.SerializeToString()
        try:
            if dry_run:
                logging.debug("%s: %s --> %s" % (self.name, memc.servers, data_map))
            else:
                memc.set_multi(data_map)
        except Exception as e:
            logging.exception("%s: Cannot write to memc %s: %s" % (self.name, memc.servers, e))
            return False
        return True


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def main(options):
    task_queue = mp.JoinableQueue()
    task_result_queue = mp.JoinableQueue()
    event = mp.Event()
    proc_list = []

    for _ in range(options.processes):
        p = ProcessWorker(task_queue, task_result_queue, event, options)
        p.start()
        proc_list.append(p)

    for fn in glob.iglob(options.pattern):
        processed = errors = 0
        event.clear()
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn, 'rt')

        for line in fd:
            line = line.strip()
            if not line:
                continue
            task_queue.put(line)

        for _ in range(len(proc_list)):
            task_queue.put('result')
        task_queue.join()

        for _ in range(len(proc_list)):
            result = task_result_queue.get()
            logging.debug(f'Main: got results: {result}')
            errors += result['errors']
            processed += result['processed']
            task_result_queue.task_done()

        if not processed:
            fd.close()
            if not options.dry:
                dot_rename(fn)
            event.set()
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        if not options.dry:
            dot_rename(fn)
        event.set()

    # завершаем работу и закрываем процессы и воркеров
    for _ in range(len(proc_list)):
        task_queue.put('quit')
    for proc in proc_list:
        proc.join()
    logging.debug('End up')


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    ncpu = os.cpu_count()
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("-w", "--workers", type=int, action="store", default=4)
    op.add_option("-p", "--processes", type=int, action="store", default=ncpu)
    op.add_option("-b", "--buffer", action="store", type=int, default=5)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="data/appsinstalled/*.tsv.gz")
    # op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    # op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    # op.add_option("--adid", action="store", default="127.0.0.1:33015")
    # op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        st = time.monotonic()
        main(opts)
        fn = time.monotonic()
        logging.info(f'Duration: {fn - st} sec')
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
