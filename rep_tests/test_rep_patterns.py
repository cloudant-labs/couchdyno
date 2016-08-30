import pytest
import rep


TEST_ARGS = [
    (n, normal, db_per_doc, num)
    for n in [1, 100]
    for normal in [False, True]
    for db_per_doc in [False, True]
    for num in [1, 1000]
]


@pytest.mark.parametrize("n,normal,db_per_doc,num", TEST_ARGS)
def test_1_to_n_pattern(n, normal, db_per_doc, num):
    rep.replicate_1_to_n_and_compare(n=n, cycles=2, num=num, normal=normal,
                                     db_per_doc=db_per_doc)


@pytest.mark.parametrize("n,normal,db_per_doc,num", TEST_ARGS)
def test_n_to_1_pattern(n, normal, db_per_doc, num):
    rep.replicate_1_to_n_and_compare(n=n, cycles=2, num=num, normal=normal,
                                     db_per_doc=db_per_doc)


@pytest.mark.parametrize("n,normal,db_per_doc,num", TEST_ARGS)
def test_n_to_n_pattern(n, normal, db_per_doc, num):
    rep.replicate_n_to_n_and_compare(n=n, cycles=2, num=num, normal=normal,
                                     db_per_doc=db_per_doc)


@pytest.mark.parametrize("n,normal,db_per_doc,num", TEST_ARGS)
def test_n_chain_pattern(n, normal, db_per_doc, num):
    rep.replicate_n_chain_and_compare(n=n, cycles=2, num=num, normal=normal,
                                     db_per_doc=db_per_doc)


def test_all_pattern_continuous():
    rep.replicate_n_chain_and_compare(n=25, cycles=2, num=10, normal=False)


def test_all_pattern_normal():
    rep.replicate_n_chain_and_compare(n=25, cycles=2, num=10, normal=True)
