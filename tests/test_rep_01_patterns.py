import pytest
import conftest


pytestmark = pytest.mark.usefixtures("rep")

TEST_ARGS = [
    (n, normal, db_per_doc)
    for n in [1, 10]
    for normal in [False, True]
    for db_per_doc in [False, True]
]


@pytest.mark.parametrize("n,normal,db_per_doc", TEST_ARGS)
def test_n_to_n_pattern(n, normal, db_per_doc):
    rep = conftest.get_rep()
    rep.replicate_n_to_n_and_compare(n=n, num=100, normal=normal,
                                     db_per_doc=db_per_doc)


@pytest.mark.parametrize("n,normal,db_per_doc", TEST_ARGS)
def test_1_to_n_pattern(n, normal, db_per_doc):
    rep = conftest.get_rep()
    rep.replicate_1_to_n_and_compare(n=n, num=100, normal=normal,
                                     db_per_doc=db_per_doc)


def test_n_to_1_pattern(rep):
    rep = conftest.get_rep()
    rep.replicate_n_to_1_and_compare(n=10, num=10, normal=False)


def test_n_chain_pattern(rep):
    rep = conftest.get_rep()
    rep.replicate_n_chain_and_compare(n=10, num=10, normal=False)


def test_all_pattern_continuous(rep):
    rep = conftest.get_rep()
    rep.replicate_all_and_compare(n=10, num=10, normal=False)
