from colour_runner import runner
import unittest
import specs

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(specs)
    results = runner.ColourTextTestRunner(verbosity=2).run(suite)
