from src.scraper.mappers import extract_job_id, jobposting_from_api_record


def test_extract_job_id_from_url():
    url = "https://ibmglobal.avature.net/careers/JobDetail?jobId=80194"
    assert extract_job_id(url) == "80194"


def test_jobposting_from_api_record_maps_fields():
    record = {
        "_id": "some_hash",
        "_source": {
            "language": "en",
            "url": "https://ibmglobal.avature.net/careers/JobDetail?jobId=80194",
            "title": "Package Consultant-SAP HANA EPM",
            "description": "Short description...",
            "field_keyword_19": "Hyderabad, IN",
        },
    }

    jp = jobposting_from_api_record(record, source="ibm_careers")

    assert jp.source == "ibm_careers"
    assert jp.external_id == "80194"
    assert jp.title == "Package Consultant-SAP HANA EPM"
    assert str(jp.job_url) == "https://ibmglobal.avature.net/careers/JobDetail?jobId=80194"
    assert jp.location_raw == "Hyderabad, IN"
    assert jp.location_city == "Hyderabad"
    assert jp.location_country == "IN"
