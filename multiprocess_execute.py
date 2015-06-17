#!/usr/bin/env python

from multiprocessing import Pool
import sys
import time

from cassandra.cluster import Cluster
from cassandra.query import tuple_factory

def query_gen(n):
    for _ in xrange(n):
        yield ('local', )


class QueryManager(object):

    batch_size = 10

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
        results = self.pool.map(_get_multiproc, params, self.batch_size)
        return results

    @classmethod
    def _execute_request(cls, params):
        return cls.session.execute(cls.prepared, params)

def _get_multiproc(params):
    return QueryManager._execute_request(params)


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
    print "%d queries in %s seconds (%s/s)" % (iterations, delta, iterations / delta)
