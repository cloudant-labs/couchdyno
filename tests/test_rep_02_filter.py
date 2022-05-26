import pytest
import conftest

pytestmark = pytest.mark.usefixtures("rep")

TEST_ARGS = [normal for normal in [False, True]]


def test_n_to_n_view_filter():
    rep = conftest.get_rep()
    view_map = "function(doc) { emit(doc._id, null); };"
    rep.replicate_n_to_n_and_compare(n=10, num=10, normal=True, filter_view=view_map)


@pytest.mark.parametrize("normal", TEST_ARGS)
def test_n_to_n_js_filter(normal):
    rep = conftest.get_rep()
    filter_js = "function(doc, req) {return true;}"
    rep.replicate_n_to_n_and_compare(n=10, num=10, normal=normal, filter_js=filter_js)


def test_n_to_n_doc_ids_filter():
    rep = conftest.get_rep()
    cfg = rep.getcfg()
    n = 10
    doc_ids = [cfg.prefix + "-%07d" % i for i in range(1, n + 1)]
    rep.replicate_n_to_n_and_compare(n=10, num=10, normal=True, filter_doc_ids=doc_ids)


@pytest.mark.parametrize("normal", TEST_ARGS)
def test_n_to_n_mango_filter(normal):
    rep = conftest.get_rep()
    selector = {"_id": {"$ne": None}}
    rep.replicate_n_to_n_and_compare(n=10, num=10, normal=normal, filter_mango=selector)
