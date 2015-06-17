#!/usr/bin/env python

import sys
import time
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import tuple_factory


def query_gen(n):
    for _ in xrange(n):
        yield ('local', )


class QueryManager(object):

    def __init__(self, cluster):
        self._setup(cluster)

    def _setup(self, cluster):
        self.session = cluster.connect()
        self.session.row_factory = tuple_factory
        self.prepared = self.session.prepare('SELECT * FROM system.local WHERE key=?')

    def get_results(self, params):
        return self._results_from_concurrent(params)

    def _results_from_concurrent(self, params):
        return [results[1] for results in execute_concurrent_with_args(self.session, self.prepared, params)]


if __name__ == '__main__':
    try:
        iterations = int(sys.argv[1])
    except (IndexError, ValueError):
        print("Usage: %s <num iterations>" % sys.argv[0])
        sys.exit(1)

    cluster = Cluster()
    qm = QueryManager(cluster)

    start = time.time()
    rows = qm.get_results(query_gen(iterations))
    delta = time.time() - start
    print "%d queries in %s seconds (%s/s)" % (iterations, delta, iterations / delta)
