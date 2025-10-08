from utils.models import Speaker, CompanyCategory
from utils.classify import heuristic_category

def test_heuristics():
    assert heuristic_category(Speaker(name="A", company="Skanska", bio=None, talk_titles=[], url="u")) == CompanyCategory.builder
    assert heuristic_category(Speaker(name="B", company="Network Rail", bio=None, talk_titles=[], url="u")) == CompanyCategory.owner
    assert heuristic_category(Speaker(name="C", company="Pix4D", bio=None, talk_titles=[], url="u")) == CompanyCategory.competitor
