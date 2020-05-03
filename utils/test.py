"""
Test behavior correctness
"""
import sys

sys.path.append('../')
import config
from utils import sic_transit

def test_sic_transit_broken():
    """
    Test broken classification is True or not
    """
    testcase = [
        ('https://webstores.activenetwork.com/school-software/west_linn_high_schoo/index.php?l=product_detail&p=433#.Xq8NDahKibh', False),
        ('https://webstores.activenetwork.com/school-software/wilsonville_high_sch/index.php?l=cart_view', False),
        ('https://webstores.activenetwork.com/school-software/wasatch_junior_hi2zk/index.php/', True),
        ('https://info.activenetwork.com/solutions/rtp-one?mode=detail&id=2009-12-03', False)
    ]
    print('url\tlabel\tclassification')
    for tc in testcase:
        url, label = tc
        clas = sic_transit.broken(url)[0]
        print(f'{url}\t{label}\t{clas}')