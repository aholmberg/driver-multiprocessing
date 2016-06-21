#!/usr/bin/env python

import itertools
from multiprocessing import Pool
import sys
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import tuple_factory


def query_gen(n):
    for _ in range(n):
        yield ('local', )


class QueryManager(object):

    concurrency = 100  # chosen to match the default in execute_concurrent_with_args

    def __init__(self, cluster, process_count=None):
        self.pool = Pool(processes=process_count, initializer=self._setup, initargs=(cluster,))

    @classmethod
    def _setup(cls, cluster):
        cls.session = cluster.connect()
        cls.session.row_factory = tuple_factory
        cls.prepared = cls.session.prepare('SELECT * FROM system.local WHERE key=?')

    def close_pool(self):
        self.pool.close()
        self.pool.join()

    def get_results(self, params):
        params = list(params)
        results = self.pool.map(_multiprocess_get, (params[n:n + self.concurrency] for n in range(0, len(params), self.concurrency)))
        return list(itertools.chain(*results))

    @classmethod
    def _results_from_concurrent(cls, params):
        return [results[1] for results in execute_concurrent_with_args(cls.session, cls.prepared, params)]

def _multiprocess_get(params):
    return QueryManager._results_from_concurrent(params)


if __name__ == '__main__':
    try:
        iterations = int(sys.argv[1])
        processes = int(sys.argv[2]) if len(sys.argv) > 2 else None
    except (IndexError, ValueError):
        print("Usage: %s <num iterations> [<num processes>]" % sys.argv[0])
        sys.exit(1)

    cluster = Cluster()
    qm = QueryManager(cluster, processes)

    start = time.time()
    rows = qm.get_results(query_gen(iterations))
    delta = time.time() - start
    print("%d queries in %s seconds (%s/s)" % (iterations, delta, iterations / delta))
