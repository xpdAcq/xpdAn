from xpdan.search import fuzzy_search, super_fuzzy_search
import pytest


@pytest.mark.parametrize(('search_str', 'target_str'),
                         [('chris', 'chris'), ('christ', 'chris'),
                          ('chry', 'chris'), ('tim', 'tim')])
def test_fuzzy_searches(exp_db, search_str, target_str):
    search_result = fuzzy_search(exp_db, 'pi_name', search_str)
    assert search_result[0]['start']['pi_name'] == target_str


@pytest.mark.parametrize(('search_str', 'target_str'),
                         [('chris', 'chris'), ('christ', 'chris'),
                          ('chry', 'chris'), ('tim', 'tim')])
def test_super_fuzzy_search(exp_db, search_str, target_str):
    search_result = super_fuzzy_search(exp_db, search_str)
    assert search_result[0]['start']['pi_name'] == target_str
