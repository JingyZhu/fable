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
        ('https://info.activenetwork.com/solutions/rtp-one?mode=detail&id=2009-12-03', False),
        ('http://www.decathlon.co.uk/dreamheat-socks-boys-id_8284633.html', True),
        ('http://www.decathlon.co.uk/capyba-halterneck-bikini-id_8300968.html', True),
        ('http://www.decathlon.co.uk/C-309970-quechua/N-66470-price~from-25-to-50/N-66470-price~from-15-to-20/N-66470-price~from-200-to-250', False),
        ('http://www.eclipse.org/documentation/?topic=/org.eclipse.platform.doc.isv/guide/intro_extending_content.htm', False),
        ('http://findavideo.com/findavideo/reviews.tam?cart=99C02nex.ftq&lpg=/findavideo/stars.tam&lpt=936225424&xax=5998&TITLE.ctx=Twice+Upon+a+Time', True),
        ('http://blog.imageworksllc.com/blog/bid/341454/js.hubspot.com/analytics/', False)
    ]
    print('url\tlabel\tclassification')
    for tc in testcase:
        url, label = tc
        clas = sic_transit.broken(url)[0]
        print(f'{url}\t{label}\t{clas}')