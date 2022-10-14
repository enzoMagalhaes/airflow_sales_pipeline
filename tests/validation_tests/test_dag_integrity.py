
class TestDagValidation:

    LOAD_SECOND_THRESHOLD = 4

    def test_import_dags(self, dagbag):
        """
            test if dags have any errors (typos, cycles, etc)
        """
        assert len(dagbag.import_errors) == 0, "DAG failures detected! Got: {}".format(
            dagbag.import_errors
        )

    def test_time_import_dags(self, dagbag):
        """
            check the dags loading time
        """
        stats = dagbag.dagbag_stats
        slow_dags = list(filter(lambda f: f.duration.seconds > self.LOAD_SECOND_THRESHOLD, stats))
        res = ', '.join(map(lambda f: f.file[1:], slow_dags))

        assert len(slow_dags) == 0, "The following DAGs take more than {0}s to load: {1}".format(
            self.LOAD_SECOND_THRESHOLD,
            res
        )
