Example code accompanying `a blog post on driver multiprocessing <http://www.datastax.com/dev/blog/datastax-python-driver-multiprocessing-example-for-improved-bulk-data-throughput>`_.

Each script is self-contained for easy side-by-side comparison. To run::

    ccm create -v 2.1.6 -n 1 -s cassandra216x1

    single_process_concurrent.py <iterations>
    multiprocess_execute.py <iterations> <# pool processes>
    multiprocess_concurrent.py <iterations> <# pool processes>
