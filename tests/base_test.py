import os
import sys
import unittest

class BaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        if project_root not in sys.path:
            sys.path.insert(0, project_root)