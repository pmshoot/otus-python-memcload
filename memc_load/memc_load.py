#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import glob
import gzip
import logging
import os
import sys
from optparse import OptionParser

# pip install python-memcached
import memcache

# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
from utils import Accumulator

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, data_map, dry_run=False):
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s --> %s" % (memc_addr, data_map))
        else:
            memc = memcache.Client([memc_addr])
            memc.set_multi(data_map)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


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


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    preprocessed = Accumulator(buf_size=options.buffer)
    for fn in glob.iglob(options.pattern):
        processed = errors = 0
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn, 'rt')
        _l = 0
        for line in fd:
            line = line.strip()
            if not line:
                continue
            _l += 1
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            if appsinstalled.dev_type not in device_memc:
                errors += 1
                logging.error("Unknown device type: %s" % appsinstalled.dev_type)
                continue
            #
            ua = appsinstalled_pb2.UserApps()
            ua.lat = appsinstalled.lat
            ua.lon = appsinstalled.lon
            key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
            ua.apps.extend(appsinstalled.apps)
            packed = ua.SerializeToString()
            #
            preprocessed.put(appsinstalled.dev_type, key, packed)
            dev_type, data_map = preprocessed.pop_ready()
            if not dev_type:
                continue
            #
            memc_addr = device_memc.get(dev_type)
            ok = insert_appsinstalled(memc_addr, data_map, options.dry)
            if ok:
                processed += options.buffer
            else:
                errors += options.buffer

        # drain accumulator
        while not preprocessed.is_empty():
            dev_type, data_map = preprocessed.pop_ready(drain=True)
            if not dev_type:
                continue
            #
            memc_addr = device_memc.get(dev_type)
            ok = insert_appsinstalled(memc_addr, data_map, options.dry)
            if ok:
                processed += len(data_map)
            else:
                errors += len(data_map)

        if not processed:
            fd.close()
            if not options.dry:
                dot_rename(fn)
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        if not options.dry:
            dot_rename(fn)


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
    op.add_option("-b", "--buffer", action="store", type=int, default=5)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
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
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
