
# rep is a pytest fixture from conftest.py

def test_basic_continuous(rep):
    rep.replicate_1_to_n_and_compare(1)


def test_basic_10_continuous(rep):
    rep.replicate_1_to_n_and_compare(10)


def test_basic_10_continuous_1000_docs(rep):
    rep.replicate_1_to_n_and_compare(10, num=1000, cycles=2)

def test_basic_normal(rep):
    rep.replicate_1_to_n_and_compare(1, normal=True)


def test_basic_10_normal(rep):
    rep.replicate_1_to_n_and_compare(10, normal=True)


def test_basic_attachment(rep):
    rep.replicate_1_to_n_and_compare(1, attachments=1)


def test_basic_js_filter(rep):
    rep.replicate_n_to_n_and_compare(1, filter_js=True)

