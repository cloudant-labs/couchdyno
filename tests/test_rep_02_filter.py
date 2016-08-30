import pytest
import rep


TEST_ARGS = [
    (n, normal, attachments)
    for n in [1, 50]
    for normal in [False, True]
    for attachments in [None, 1]
]


@pytest.mark.parametrize("n,normal,attachments", TEST_ARGS)
def test_n_to_n_js_filter(n, normal, attachments):
    filter_js = "function(doc, req) {return true;}"
    rep.replicate_n_to_n_and_compare(n=n, cycles=2, num=10, normal=normal,
                                     attachments=attachments, filter_js=filter_js)

@pytest.mark.parametrize("n,normal,attachments", TEST_ARGS)
def test_n_to_n_doc_ids_filter(n, normal, attachments):
    cfg = rep.getcfg()
    doc_ids = [cfg.prefix + '_%07d' % i for i in xrange(1, n+1)]
    rep.replicate_n_to_n_and_compare(n=1, cycles=2, num=10, normal=normal,
                                     attachments=attachments, filter_doc_ids=doc_ids)


@pytest.mark.parametrize("n,normal,attachments", TEST_ARGS)
def test_n_to_n_mango_filter(n, normal, attachments):
    selector = {"_id": {"$ne": None}}
    rep.replicate_n_to_n_and_compare(n=n, cycles=2, num=10, normal=normal,
                                     attachments=attachments, filter_mango=selector)


@pytest.mark.parametrize("n,normal,attachments", TEST_ARGS)
def test_n_to_n_view_filter(n, normal, attachments):
    view_map =  "function(doc) { emit(doc._id, null); };"
    rep.replicate_n_to_n_and_compare(n=n, cycles=2, num=10, normal=normal,
                                     attachments=attachments, filter_view=view_map)
