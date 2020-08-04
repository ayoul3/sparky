# -*- coding: utf-8 -*-
import mock, sys, base64
import unittest, allure

sys.path.append("../")
from utils.sparkClient import SparkClient
from utils.cmd import parseCommandOutput, restCommandExec


class CMDTestCase(unittest.TestCase):
    def setUp(self):
        self.sClient = SparkClient("127.0.0.1", 7077, "127.0.0.1", "test", "test")
        self.sClient.sc = mock.Mock()

    def test_parseCommandOutput(self):
        with allure.step("When command execution works"):
            countInstances = mock.Mock()
            countInstances.size.return_value = "2"
            self.sClient.sc._jsc.sc.return_value.getExecutorMemoryStatus.return_value = (
                countInstances
            )

            result = mock.Mock()
            result.collect.return_value = [b"test"]
            self.sClient.sc.parallelize.return_value.map.return_value = result

            interpreterArgs = ["/bin/bash", "-c", "echo test"]
            parseCommandOutput(self.sClient, interpreterArgs, 1)
            result.collect.assert_called_once()

    @mock.patch("requests.post")
    def test_restCommandExec(self, mock_post):
        with allure.step("When REST command execution works"):
            mock_post.return_value.text = """{
                        "action" : "CreateSubmissionResponse",
                        "message" : "Driver successfully submitted as driver-20151008145126-0000",
                        "serverSparkVersion" : "1.5.0",
                        "submissionId" : "driver-20151008145126-0000",
                        "success" : true
                        }"""

            ret = restCommandExec(
                self.sClient,
                "/bin/bash",
                base64.b64encode(b"echo test"),
                "spark://127::1",
                2,
            )
            self.assertTrue(ret, "Should return True but returned %s " % ret)

        with allure.step("When provided an invalid JAR url"):
            mock_post.return_value.text = b"error"
            ret = restCommandExec(
                self.sClient,
                "/bin/bash",
                base64.b64encode(b"echo test"),
                "weird command",
                2,
            )
            self.assertTrue(ret is None, "Should return True but returned %s " % ret)


if __name__ == "__main__":
    unittest.main()
