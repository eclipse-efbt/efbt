import unittest
from utils.traverser import SubtypeExploder


class SimpleTestCase(unittest.TestCase):
    '''
    Now a template to check we can import ok and run test framework
    Priority is to complete these tests for SubtypeExploder
    ''' 

    def setUp(self):
        """Call before every test case."""
        pass


    def tearDown(self):
        """Call after every test case."""
        pass

    def testA(self):
        """Test case A. note that all test method names must begin with 'test.'"""
        assert True, "True"

    def testB(self):
        """test case B"""
        assert 1 ==1 , "Tests 1 ==1"

    


class OtherTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testBlah(self):
        assert "blah2" == "blah", "blah isn't blahing blahing correctly"


if __name__ == "__main__":
    unittest.main() # run all tests