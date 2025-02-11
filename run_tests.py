#!/usr/bin/env python3
import unittest
import sys
import os

def main():
    # Determine the project root (assumes this script is in the project root).
    project_root = os.path.abspath(os.path.dirname(__file__))
    # Ensure the project root is on the Python path.
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Discover all test modules in the "tests" directory.
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=os.path.join(project_root, "tests"), pattern="test*.py")

    # Create a test runner that displays results in a verbose format.
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    # Exit with an appropriate status code (0 if all tests pass).
    sys.exit(not result.wasSuccessful())

if __name__ == "__main__":
    main()