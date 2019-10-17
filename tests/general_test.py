# -*- coding: utf-8 -*-
import mock, sys
import unittest, allure

sys.path.append("../")
from utils.sparkClient import SparkClient
from utils.general import (
    confirmSpark,
    parseListNodes,
    checkRestPort,
    checkHTTPPort,
    whine,
    isValidFile,
    request_stop,
)

class GeneralTestCase(unittest.TestCase):
    def setUp(self):
        self.sClient = SparkClient("127.0.0.1", 7077, "127.0.0.1", "test", "test")

    @mock.patch("socket.socket")
    def test_confirmSpark(self, mock_socket):
        with allure.step("When Spark is identified"):
            mock_socket.return_value.recv.return_value = b"\x00\x00\x00\x00\x00\x00\x00\xc5\x03\x62\x05\x32\x92\xe7\xca\x6d\xaa\x00\x00\x00\xb0"
            ret = confirmSpark(self.sClient)
            self.assertTrue(ret == 0, "Should return 0 but returned %s " % ret)

        with allure.step("When Spark requires authentication"):
            mock_socket.return_value.recv.side_effect = [
                b"\x00\x00\x00\x00\x00\x00\x00\xc5\x03\x62\x05\x32\x92\xe7\xca\x6d\xaa\x00\x00\x00\xb0",
                b"Expected SaslMessage",
            ]
            ret = confirmSpark(self.sClient)
            self.assertTrue(ret == 1, "Should return 1 but returned %s " % ret)

        with allure.step("When it's not possible to fingerprint spark"):
            mock_socket.return_value.recv.side_effect = None
            mock_socket.return_value.recv.return_value = b"erererqerer"
            ret = confirmSpark(self.sClient)
            self.assertTrue(ret == -1, "Should return -1 but returned %s " % ret)


    @mock.patch("requests.get")
    def test_checkRestPort(self, mock_get):
        with allure.step("When REST API is reachable"):
            mock_get.return_value.text = b'{"serverSparkVersion":"2.4.3"}'
            ret = checkRestPort(self.sClient)
            self.assertTrue(ret == 0, "Should return 0 but returned %s " % ret)
        
        with allure.step("When REST API returns weird results"):
            mock_get.return_value.text = b'sddsdsdsdsdsd'
            ret = checkRestPort(self.sClient)
            self.assertTrue(ret == None, "Should return None but returned %s " % ret)
    
    @mock.patch("requests.get")
    def test_checkHTTPPort(self, mock_get):
        with allure.step("When REST API is reachable"):
            mock_get.return_value.text = b'<html></html>'
            ret = checkHTTPPort(self.sClient)
            self.assertTrue(ret, "Should return True but returned %s " % ret)
        
        with allure.step("When REST API returns weird results"):
            mock_get.return_value.text = None
            ret = checkHTTPPort(self.sClient)
            self.assertFalse(ret, "Should return None but returned %s " % ret)
    
    


if __name__ == "__main__":
    unittest.main()
