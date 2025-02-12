import unittest
import sys
import os


def run_all_tests():
    # Add project root to sys.path
    project_root = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, project_root)

    # Discover tests in the tests directory and its subdirectories
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(os.path.join(project_root, "tests"), pattern="test*.py")

    # Run tests verbosely
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)

    # Exit with a nonzero code if tests failed
    if not result.wasSuccessful():
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()