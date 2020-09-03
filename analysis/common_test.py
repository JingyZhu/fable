import unittest

import common

class TestCommon(unittest.TestCase):

    def test_order_archived(self):
        test = [
            common.Archived('https://foo.com/?k1=v1&k2=v2', ''),
            common.Archived('https://foo.com/?k1=v1', ''),
            common.Archived('https://foo.com/?k1=v1', ''),
            common.Archived('https://foo.com/?k2=v2', '')
        ]
        expected = [
            common.Archived('https://foo.com/?k1=v1', ''),
            common.Archived('https://foo.com/?k1=v1', ''),
            common.Archived('https://foo.com/?k1=v1&k2=v2', ''),
            common.Archived('https://foo.com/?k2=v2', '')
        ]
        self.assertSequenceEqual(sorted(test), expected)

if __name__ == '__main__':
    unittest.main()
