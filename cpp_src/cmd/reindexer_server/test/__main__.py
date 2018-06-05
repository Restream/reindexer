from colour_runner import runner
import unittest
import specs
import sys


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(specs)
    ret_code = not runner.ColourTextTestRunner(
        verbosity=2).run(suite).wasSuccessful()
    sys.exit(ret_code)
