#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import queue
import sys
import glob
import logging
import collections
import threading
import time
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
from memc_load import appsinstalled_pb2
# pip install python-memcached
from memcache import Client

# from pymemcache.client.retrying import RetryingClient
# from pymemcache.exceptions import MemcacheUnexpectedCloseError

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class Worker(threading.Thread):
    def __init__(self, task_queue, result_queue, event, opts, client_class=Client):
        super().__init__()
        self.task_queue: queue.Queue = task_queue
        self.result_queue: queue.Queue = result_queue
        self.event: threading.Event = event
        self.dry_run = opts.dry
        self.errors = 0
        self.processed = 0
        self.device_memc = self.get_memc(opts, client_class)

    @staticmethod
    def get_memc(opts, client_class):
        """"""
        return {
            "idfa": client_class([opts.idfa]),
            "gaid": client_class([opts.gaid]),
            "adid": client_class([opts.adid]),
            "dvid": client_class([opts.dvid]),
        }

    def run(self) -> None:
        """"""
        while True:
            # обрабатываем очередь
            task = self.task_queue.get()
            if isinstance(task, str):
                if task == 'result':
                    # файл прочитан, очередь отработана, выгружаем результат
                    res = {
                        'worker': self.name,
                        'errors': self.errors,
                        'processed': self.processed
                    }
                    self.result_queue.put(res)
                    self.task_queue.task_done()
                    logging.debug(f'{self.name}: Результаты выгружены: {res}')
                    self.errors = self.processed = 0
                    self.event.wait()  # ждем чтения следующего файла
                    continue
                elif task == 'quit':
                    self.task_queue.task_done()
                    logging.debug(f'{self.name}: Выход')
                    break
                else:
                    # wrong str task
                    continue
            else:
                dev_type, key, packed = task
                memc = self.device_memc.get(dev_type)
                if not memc:
                    self.errors += 1
                    logging.error("%s: Unknown device type: %s" % (self.name, dev_type))
                    self.task_queue.task_done()
                    continue
                ok = self.insert_appsinstalled(memc, key, packed, self.dry_run)
                if ok:
                    self.processed += 1
                else:
                    self.errors += 1
                self.task_queue.task_done()

    def insert_appsinstalled(self, memc: Client, key, packed, dry_run=False):
        # ua = appsinstalled_pb2.UserApps()
        # ua.lat = task.lat
        # ua.lon = task.lon
        # key = "%s:%s" % (task.dev_type, task.dev_id)
        # ua.apps.extend(task.apps)
        # packed = ua.SerializeToString()
        try:
            if dry_run:
                # logging.debug("%s: %s - %s -> %s" % (self.name, memc.servers, key, str(ua).replace("\n", " ")))
                logging.debug("%s: %s - %s -> %s" % (self.name, memc.servers, key, packed))
            else:
                memc.set(key, packed)
        except Exception as e:
            logging.exception("%s: Cannot write to memc %s: %s" % (self.name, memc.servers, e))
            return False
        return True


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def parse_appsinstalled(line):
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


def get_workers(task_queue, result_queue, event, opt, **kwargs):
    """Формирование пула воркеров"""
    client_class = kwargs.get('client_class', Client)
    workers = []
    for _ in range(opt.workers):
        w = Worker(task_queue, result_queue, event, opt, client_class=client_class)
        w.start()
        workers.append(w)
    return workers


def get_task_data(appsinstalled: AppsInstalled) -> tuple:
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    return appsinstalled.dev_type, key, packed


def main(options, workers_list):
    for fn in glob.iglob(options.pattern):
        processed = errors = 0
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn, 'rt')
        event.clear()
        for line in fd:
            line = line.strip()
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            # заполняем очередь для тредов-труженников
            task_queue.put(get_task_data(appsinstalled))
        # сигнал для подготовки результатов работы воркеров
        for _ in range(len(workers_list)):
            task_queue.put('result')
        task_queue.join()
        # забор данных результатов работы воркеров
        for _ in range(len(workers_list)):
            result = result_queue.get()
            logging.debug(f'got results: {result}')
            errors += result['errors']
            processed += result['processed']
            result_queue.task_done()
        if not processed:
            fd.close()
            if not options.dry:
                """"""
                dot_rename(fn)
            event.set()  # сигнал для продолжения работы воркеров
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        if not options.dry:
            """"""
            dot_rename(fn)
        event.set()

    # завершаем работу и закрываем воркеров
    for _ in range(len(workers_list)):
        task_queue.put('quit')
    for worker in workers_list:
        worker.join()
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
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("-w", "--workers", type=int, action="store", default=2)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    # op.add_option("--idfa", action="store", default="nas:33013")
    # op.add_option("--gaid", action="store", default="nas:33014")
    # op.add_option("--adid", action="store", default="nas:33015")
    # op.add_option("--dvid", action="store", default="nas:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    task_queue = queue.Queue(maxsize=100)
    result_queue = queue.Queue()
    event = threading.Event()
    workers_list = get_workers(task_queue, result_queue, event, opts)
    try:
        st = time.monotonic()
        main(opts, workers_list)
        fn = time.monotonic()
        logging.info(f'Duration: {fn - st} sec')
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
