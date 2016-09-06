import pytest
import conftest


pytestmark = pytest.mark.usefixtures("rep")


TEST_ARGS = [
    (normal, attachments, num)
    for normal in [False, True]
    for attachments in [ 64, 100000, [(i,1) for i in xrange(1,11)] ]
    for num in [1, 100]
]


@pytest.mark.parametrize("normal,attachments,num", TEST_ARGS)
def test_attachments(normal, attachments, num):
    rep = conftest.get_rep()
    rep.replicate_n_to_n_and_compare(n=2, cycles=2, num=num, normal=normal,
                                     attachments=attachments)

