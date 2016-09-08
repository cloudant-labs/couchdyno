import pytest
import conftest

pytestmark = pytest.mark.usefixtures("rep")

TEST_ARGS = [
    (normal, attachments)
    for normal in [False, True]
    for attachments in [None, 1]
]


@pytest.mark.parametrize("normal,attachments", TEST_ARGS)
def test_n_to_n_view_filter(normal, attachments):
    rep = conftest.get_rep()
    view_map =  "function(doc) { emit(doc._id, null); };"
    rep.replicate_n_to_n_and_compare(n=10, num=10, normal=normal,
                                     attachments=attachments, filter_view=view_map)


@pytest.mark.parametrize("normal,attachments", TEST_ARGS)
def test_n_to_n_js_filter(normal, attachments):
    rep = conftest.get_rep()
    filter_js = "function(doc, req) {return true;}"
    rep.replicate_n_to_n_and_compare(n=10, num=10, normal=normal,
                                     attachments=attachments, filter_js=filter_js)

@pytest.mark.parametrize("normal,attachments", TEST_ARGS)
def test_n_to_n_doc_ids_filter(normal, attachments):
    rep = conftest.get_rep()
    cfg = rep.getcfg()
    n = 10
    doc_ids = [cfg.prefix + '-%07d' % i for i in xrange(1, n+1)]
    rep.replicate_n_to_n_and_compare(n=10, num=10, normal=normal,
                                     attachments=attachments, filter_doc_ids=doc_ids)


@pytest.mark.parametrize("normal,attachments", TEST_ARGS)
def test_n_to_n_mango_filter(normal, attachments):
    rep = conftest.get_rep()
    selector = {"_id": {"$ne": None}}
    rep.replicate_n_to_n_and_compare(n=10, num=10, normal=normal,
                                     attachments=attachments, filter_mango=selector)

