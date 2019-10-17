# -*- coding: utf-8 -*-
import mock, sys, os
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
from utils.auth import isSecretSaslValid

class AuthTestCase(unittest.TestCase):
    def setUp(self):
        self.sClient = SparkClient("127.0.0.1", 7077, "127.0.0.1", "test", "test")
        os.environ["TEST_FLAG"] = "True"

    @mock.patch("socket.socket")
    def test_isSecretSaslValid(self, mock_socket):
        with allure.step("When secret is valid"):
            mock_socket.return_value.recv.side_effect = [
                b"\x00\x00\x00\x00\x00\x00\x00\xaf\x04\x41\x4d\x9e\xbf\x8c\x7e\x68\x26\x00\x00\x00\x9a",
                b'realm="default",nonce="Yd07csXzyt3qBD4DJWczAIoPyiNQQoKMRaenE2im",qop="auth-conf,auth",charset=utf-8,cipher="3des,rc4,des,rc4-56,rc4-40",algorithm=md5-sess',
                b"\x00\x00\x00\x00\x00\x00\x00\x3d\x04\x68\xbd\x9f\xb5\x86\x94\xf2\x94\x00\x00\x00\x28",
                b"rspauth=f400608b77fda244b212d77b813c066f",
            ]
            ret = isSecretSaslValid(self.sClient, "CCCC")
            self.assertTrue(ret, "Should return True but returned %s " % ret)
            mock_socket.return_value.send.assert_any_call(b'\xea\x00\x00\x00\rsparkSaslUser\x00\x00\x01\x06charset=utf-8,username="c3BhcmtTYXNsVXNlcg==",realm="default",nonce="Yd07csXzyt3qBD4DJWczAIoPyiNQQoKMRaenE2im",nc=00000001,cnonce="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",digest-uri="null/default",maxbuf=65536,response=810c0df910d8e2248bf30c6b8a348479,qop=auth')
        
        with allure.step("When secret is not valid"):
            mock_socket.return_value.recv.side_effect = [
                b"\x00\x00\x00\x00\x00\x00\x00\xaf\x04\x41\x4d\x9e\xbf\x8c\x7e\x68\x26\x00\x00\x00\x9a",
                b'realm="default",nonce="Yd07csXzyt3qBD4DJWczAIoPyiNQQoKMRaenE2im",qop="auth-conf,auth",charset=utf-8,cipher="3des,rc4,des,rc4-56,rc4-40",algorithm=md5-sess',
                b"\x00\x00\x00\x00\x00\x00\x00\x3d\x04\x68\xbd\x9f\xb5\x86\x94\xf2\x94\x00\x00\x00\x28",
                b"javax.security.sasl.SaslException: DIGEST-MD5: digest response format violation. Mismatched response.",
            ]
            ret = isSecretSaslValid(self.sClient, "DDDD")
            self.assertTrue(not ret, "Should return False but returned %s " % ret)
            mock_socket.return_value.send.assert_any_call(b'\xea\x00\x00\x00\rsparkSaslUser\x00\x00\x01\x06charset=utf-8,username="c3BhcmtTYXNsVXNlcg==",realm="default",nonce="Yd07csXzyt3qBD4DJWczAIoPyiNQQoKMRaenE2im",nc=00000001,cnonce="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",digest-uri="null/default",maxbuf=65536,response=bbc3a600e824f93d58a555a719261a1b,qop=auth')

        with allure.step("When cannot get nonce from server"):
            mock_socket.return_value.recv.side_effect = [
                b"\x00\x00\x00\x00\x00\x00\x00\xaf\x04\x41\x4d\x9e\xbf\x8c\x7e\x68\x26\x00\x00\x00\x9a",
                b'dddddddddddddddd',
            ]
            ret = isSecretSaslValid(self.sClient, "DDDD")
            self.assertTrue(ret is None, "Should return None but returned %s " % ret)

        
    def tearDown(self):
        os.environ["TEST_FLAG"] = ""

if __name__ == "__main__":
    unittest.main()
