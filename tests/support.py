import unittest
from capture import Archive, Bodies, Database, FaultPlan, Jobs, run
from capture.samples import source


class MemoryCase(unittest.TestCase):
    def setUp(self):
        self.fault = FaultPlan()
        self.db = Database(fault=self.fault)
        self.addCleanup(self.db.close)
        self.jobs = Jobs(self.db)
        self.archive = Archive(self.db)
        self.bodies = Bodies(self.db)

    def collect(self, name, key=None, raise_network=False):
        src = source(name, key, raise_network)
        target = src.targets[0]
        job = self.jobs.enqueue(target.url, target.request_key)
        result = run(job, src, self.db)
        return job, result['chain_id']

    def counts(self):
        return tuple(self.db.connection.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                     for table in ('jobs', 'attempts', 'chains_v1', 'hops_v1',
                                   'bodies_v1', 'receipts_v1'))
