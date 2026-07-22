from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.download_policy_sources import (
    _machine_readable_rule_status,
    _machine_readable_stem,
    _release_kind,
    _release_record,
    _release_year,
)


def test_release_parsing_for_2017_basic() -> None:
    release = {
        "name": "basicCorrections2",
        "description": "2017 HTSA Basic Edition",
        "title": "(2017)",
        "releaseStartDate": "02/09/2017",
        "target": "02/09/2017",
    }
    assert _release_year(release) == 2017
    assert _release_kind(release) == ("basic", None)
    assert _machine_readable_stem(release) == "hts_2017_basic_data"
    assert _machine_readable_rule_status(release) == "known_html_only"


def test_release_parsing_for_2018_revision() -> None:
    release = {
        "name": "2018HTSARevision14",
        "description": "2018 HTSA Revision 14",
        "title": "Revision 14 (2018)",
        "releaseStartDate": "11/20/2018",
        "target": "11/20/2018",
    }
    assert _release_year(release) == 2018
    assert _release_kind(release) == ("revision", "14")
    assert _machine_readable_stem(release) == "hts_2018_revision_14_data"
    assert _machine_readable_rule_status(release) == "candidate_machine_readable"


def test_release_parsing_for_point_revision_marks_likely_html_only() -> None:
    release = {
        "name": "2018HTSARevision14_1",
        "description": "2018 HTSA Revision 14.1",
        "title": "Revision 14.1 (2018)",
        "releaseStartDate": "11/20/2018",
        "target": "11/20/2018",
    }
    assert _release_kind(release) == ("revision", "14_1")
    assert _machine_readable_rule_status(release) == "likely_html_only"


def test_release_record_builds_expected_urls() -> None:
    release = {
        "name": "2019HTSABASICA",
        "description": "2019 HTSA Basic Edition",
        "title": "Basic Edition (2019)",
        "date": "02/15/2019",
        "releaseStartDate": "02/15/2019",
        "releaseEndDate": "03/25/2019",
        "status": "archive",
        "creator": "Ryan Kane",
        "target": "02/15/2019",
        "mergedRevisions": [],
    }
    details = {"pdfList": {"Change Record": "", "Chapter 1": ""}}
    record = _release_record(release, details)
    assert record["archive_pdf_url"].endswith("release=2019HTSABASICA&filename=finalCopy")
    assert record["annual_zip_url"].endswith("tariff_data_2019.zip")
    assert record["archive_machine_readable_urls"]["csv"].endswith("hts_2019_basic_data.csv")
    assert record["pdf_section_count"] == 2
