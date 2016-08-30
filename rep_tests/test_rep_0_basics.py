import pytest
import rep


def test_basic_continuous():
    rep.replicate_1_to_n_and_compare(1)

def test_basic_normal():
    rep.replicate_1_to_n_and_compare(1, normal=True)

def test_basic_attachment():
    rep.replicate_1_to_n_and_compare(1, attachments=1)

def test_basic_js_filter():
    rep.replicate_n_to_n_and_compare(1, filter_js=True)

