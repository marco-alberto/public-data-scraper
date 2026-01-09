from src.scraper.models import JobPostingV1


def test_dedupe_key_is_stable():
    k1 = JobPostingV1.dedupe_key("ibm_careers", "80194")
    k2 = JobPostingV1.dedupe_key("ibm_careers", "80194")
    k3 = JobPostingV1.dedupe_key("ibm_careers", "99999")

    assert k1 == k2
    assert k1 != k3
    assert k1 == "ibm_careers::80194"
