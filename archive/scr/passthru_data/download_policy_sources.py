"""Source catalog and metadata download helpers for HTS policy inputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from html import unescape
import json
import logging
import re
import time
from urllib.parse import urlencode, urljoin, urlparse

import pandas as pd
import requests

from .config import PipelineConfig
from .io_utils import ensure_dir, write_metadata_json, write_parquet

LOGGER = logging.getLogger("passthru_data.policy")

RESTSTOP_BASE = "https://hts.usitc.gov/reststop/"
RESTSTOP_EXPORT_LIST = RESTSTOP_BASE + "exportList"
RESTSTOP_RANGES = RESTSTOP_BASE + "ranges"
HTS_ARCHIVE_INDEX = "https://hts.usitc.gov/download/archive"
USITC_ARCHIVE_LIST = "https://www.usitc.gov/harmonized_tariff_information/hts/archive/list"
USITC_ARCHIVE_LIST_PAGE_MAX = 8  # archive pages 0..7
ANNUAL_ZIP_PATTERN = "https://www.usitc.gov/tariff_affairs/documents/tariff_data/tariff_data_{year}.zip"
ARCHIVE_DOWNLOAD_PAGE = "https://hts.usitc.gov/download/?release={release}"
ARCHIVE_PDF_PATTERN = "https://hts.usitc.gov/reststop/file?release={release}&filename=finalCopy"
ARCHIVE_DATA_BASE = "https://www.usitc.gov/sites/default/files/tata/hts/{stem}.{ext}"
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0"}
SKIPPED_CHAPTERS = {77}

REVISION_RE = re.compile(r"revision\s+(?P<major>\d+)(?:\.(?P<minor>\d+))?", re.I)
ARCHIVE_MACHINE_READABLE_LINK_RE = re.compile(
    r'href=[\'"](?P<href>[^\'"]*?/sites/default/files/tata/hts/[^\'"]+\.(?:csv|xls|xlsx|json))[\'"]',
    re.I,
)
ARCHIVE_RELEASE_LINK_RE = re.compile(r"release=(?P<release>[A-Za-z0-9_]+)", re.I)
ARCHIVE_FILENAME_RE = re.compile(r"^hts_(?P<year>(?:19|20)\d{2})_(?P<body>.+)$", re.I)
DRIVER_DIR = Path(__file__).resolve().parents[2] / "data" / "selenium-drive"
DATAWEB_ANNUAL_URL = "https://dataweb.usitc.gov/tariff/annual"
DOWNLOAD_RELEASE_URL = "https://hts.usitc.gov/view/release"


def _policy_dir(config: PipelineConfig) -> Path:
    return ensure_dir(config.raw_dir / "policy")


def _policy_years(config: PipelineConfig) -> list[int]:
    start_year = int(config.start_period[:4])
    end_year = int(config.end_period[:4])
    return list(range(start_year, end_year + 1))


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    return session


def _warm_session(session: requests.Session) -> None:
    for url in (HTS_ARCHIVE_INDEX, RESTSTOP_BASE + "currentRelease"):
        try:
            session.get(url, timeout=60)
        except Exception:
            LOGGER.debug("Warm-up request failed for %s", url, exc_info=True)


def _json_get(
    url: str,
    session: requests.Session | None = None,
    timeout: int = 60,
    retries: int = 3,
    backoff_seconds: float = 1.0,
) -> Any:
    client = session or _build_session()
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = client.get(url, headers=REQUEST_HEADERS, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt + 1 >= retries:
                break
            time.sleep(backoff_seconds * (attempt + 1))
    assert last_error is not None
    raise last_error


def _safe_head(url: str, session: requests.Session | None = None, timeout: int = 30) -> dict[str, Any]:
    client = session or _build_session()
    try:
        response = client.head(url, headers=REQUEST_HEADERS, allow_redirects=True, timeout=timeout)
        return {
            "ok": response.ok,
            "status_code": response.status_code,
            "content_type": response.headers.get("Content-Type"),
            "content_length": response.headers.get("Content-Length"),
            "final_url": response.url,
            "error": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "content_type": None,
            "content_length": None,
            "final_url": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _download_binary(
    url: str,
    destination: Path,
    session: requests.Session | None = None,
    timeout: int = 300,
    retries: int = 3,
    backoff_seconds: float = 1.0,
) -> dict[str, Any]:
    client = session or _build_session()
    ensure_dir(destination.parent)
    if destination.exists():
        return {
            "status": "reused",
            "path": str(destination),
            "size_bytes": destination.stat().st_size,
        }
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = client.get(url, headers=REQUEST_HEADERS, stream=True, timeout=timeout)
            response.raise_for_status()
            tmp = destination.with_suffix(destination.suffix + ".partial")
            if tmp.exists():
                tmp.unlink()
            with tmp.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
            tmp.replace(destination)
            return {
                "status": "downloaded",
                "path": str(destination),
                "size_bytes": destination.stat().st_size,
                "content_type": response.headers.get("Content-Type"),
                "final_url": response.url,
            }
        except Exception as exc:
            last_error = exc
            if destination.with_suffix(destination.suffix + ".partial").exists():
                destination.with_suffix(destination.suffix + ".partial").unlink()
            if attempt + 1 >= retries:
                break
            time.sleep(backoff_seconds * (attempt + 1))
    assert last_error is not None
    return {
        "status": "failed",
        "path": str(destination),
        "error": f"{type(last_error).__name__}: {last_error}",
    }


def _parse_version_tuple(version: str) -> tuple[int, ...]:
    parts: list[int] = []
    for token in version.split("."):
        try:
            parts.append(int(token))
        except ValueError:
            break
    return tuple(parts)


def _find_local_driver(exe_prefix: str) -> Path | None:
    if not DRIVER_DIR.exists():
        return None
    candidates = list(DRIVER_DIR.glob(f"{exe_prefix}-*.exe"))
    if candidates:
        def sort_key(path: Path) -> tuple[int, ...]:
            stem = path.stem
            version = stem.split("-", 1)[-1] if "-" in stem else ""
            return _parse_version_tuple(version)
        return max(candidates, key=sort_key)
    fallback = DRIVER_DIR / f"{exe_prefix}.exe"
    return fallback if fallback.exists() else None


def _wait_for_download(download_dir: Path, expected_name: str, timeout_seconds: int = 300) -> Path | None:
    deadline = time.time() + timeout_seconds
    expected = download_dir / expected_name
    last_size: int | None = None
    stable_count = 0
    while time.time() < deadline:
        partials = list(download_dir.glob("*.crdownload")) + list(download_dir.glob("*.tmp"))
        if expected.exists() and not partials:
            size = expected.stat().st_size
            if last_size == size:
                stable_count += 1
            else:
                stable_count = 0
            last_size = size
            if stable_count >= 2:
                return expected
        time.sleep(2)
    return None


def _download_annual_zips_via_selenium_batch(
    items: list[dict[str, Any]],
    browser: str,
    timeout_seconds: int = 300,
) -> list[dict[str, Any]]:
    """Download multiple annual ZIPs in a single Selenium session.

    Each item must have keys: year (int), url (str), destination (Path).
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as exc:  # pragma: no cover
        return [
            {"year": item.get("year"), "status": "failed", "error": f"SeleniumImportError: {exc}"}
            for item in items
        ]

    driver_path = _find_local_driver("msedgedriver" if browser == "edge" else "chromedriver")
    if not driver_path:
        return [
            {
                "year": item.get("year"),
                "status": "failed",
                "error": f"DriverNotFound: {browser} driver missing under {DRIVER_DIR}",
            }
            for item in items
        ]

    # Use a shared temp directory for downloads; move each completed ZIP into place.
    annual_dir = items[0]["destination"].parent if items else None
    if annual_dir is None:
        return []
    tmp_root = ensure_dir(annual_dir / "_tmp_selenium_batch" / browser)
    profile_dir = ensure_dir(tmp_root / "profile")

    prefs = {
        "download.default_directory": str(tmp_root.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }

    driver = None
    results: list[dict[str, Any]] = []
    try:
        if browser == "edge":
            from selenium.webdriver.edge.service import Service as EdgeService
            from selenium.webdriver.edge.options import Options as EdgeOptions

            options = EdgeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-minimized")
            options.add_argument(f"--user-data-dir={str(profile_dir.resolve())}")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Edge(service=EdgeService(str(driver_path)), options=options)
        else:
            from selenium.webdriver.chrome.service import Service as ChromeService
            from selenium.webdriver.chrome.options import Options as ChromeOptions

            options = ChromeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-minimized")
            options.add_argument(f"--user-data-dir={str(profile_dir.resolve())}")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Chrome(service=ChromeService(str(driver_path)), options=options)

        try:
            driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": str(tmp_root.resolve())},
            )
        except Exception:
            pass

        driver.set_page_load_timeout(60)
        driver.get(DATAWEB_ANNUAL_URL)

        # Ensure the page has rendered some content.
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a")))

        for item in items:
            year = int(item["year"])
            url = str(item["url"])
            destination: Path = item["destination"]
            expected_name = f"tariff_data_{year}.zip"

            if destination.exists():
                results.append({"year": year, "status": "reused", "path": str(destination), "size_bytes": destination.stat().st_size})
                continue

            # Cleanup old downloads for this year.
            for candidate in tmp_root.glob(f"tariff_data_{year}*.zip"):
                try:
                    candidate.unlink()
                except Exception:
                    pass

            link_css = f"a[href*='tariff_data_{year}.zip']"
            try:
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, link_css)))
                link = driver.find_element(By.CSS_SELECTOR, link_css)
                driver.execute_script("arguments[0].click();", link)
            except Exception:
                # Fallback: direct navigation after having loaded DataWeb (keeps session context).
                driver.get(url)

            downloaded = _wait_for_download(tmp_root, expected_name, timeout_seconds=timeout_seconds)
            if not downloaded or not downloaded.exists():
                results.append(
                    {
                        "year": year,
                        "status": "failed",
                        "error": "DownloadTimeout: annual ZIP did not appear",
                        "download_dir": str(tmp_root),
                        "driver": str(driver_path),
                    }
                )
                continue

            downloaded.replace(destination)
            results.append({"year": year, "status": "downloaded", "path": str(destination), "size_bytes": destination.stat().st_size, "driver": str(driver_path)})

        return results
    except Exception as exc:
        for item in items:
            results.append({"year": item.get("year"), "status": "failed", "error": f"{type(exc).__name__}: {exc}", "driver": str(driver_path)})
        return results
    except Exception as exc:
        return [
            {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "driver": str(driver_path),
                **{k: v for k, v in item.items() if k != "destination"},
            }
            for item in items
        ]
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def _download_files_via_selenium_batch(
    items: list[dict[str, Any]],
    browser: str,
    start_url: str = HTS_ARCHIVE_INDEX,
    timeout_seconds: int = 45,
) -> list[dict[str, Any]]:
    """Download arbitrary files through a browser session.

    Each item must have keys: url (str), destination (Path), and optional metadata keys.
    """
    try:
        from selenium import webdriver
    except Exception as exc:  # pragma: no cover
        return [{"status": "failed", "error": f"SeleniumImportError: {exc}", **{k: v for k, v in item.items() if k != "destination"}} for item in items]

    driver_path = _find_local_driver("msedgedriver" if browser == "edge" else "chromedriver")
    if not driver_path:
        return [
            {
                "status": "failed",
                "error": f"DriverNotFound: {browser} driver missing under {DRIVER_DIR}",
                **{k: v for k, v in item.items() if k != "destination"},
            }
            for item in items
        ]

    if not items:
        return []
    tmp_root = ensure_dir(items[0]["destination"].parent / "_tmp_selenium_batch" / f"{browser}_generic")
    profile_dir = ensure_dir(tmp_root / "profile")
    prefs = {
        "download.default_directory": str(tmp_root.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }

    driver = None
    results: list[dict[str, Any]] = []
    try:
        if browser == "edge":
            from selenium.webdriver.edge.service import Service as EdgeService
            from selenium.webdriver.edge.options import Options as EdgeOptions

            options = EdgeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-minimized")
            options.add_argument(f"--user-data-dir={str(profile_dir.resolve())}")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Edge(service=EdgeService(str(driver_path)), options=options)
        else:
            from selenium.webdriver.chrome.service import Service as ChromeService
            from selenium.webdriver.chrome.options import Options as ChromeOptions

            options = ChromeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-minimized")
            options.add_argument(f"--user-data-dir={str(profile_dir.resolve())}")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Chrome(service=ChromeService(str(driver_path)), options=options)

        try:
            driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": str(tmp_root.resolve())},
            )
        except Exception:
            pass

        driver.set_page_load_timeout(90)
        driver.get(start_url)
        time.sleep(2)

        for item in items:
            destination: Path = item["destination"]
            url = str(item["url"])
            metadata = {k: v for k, v in item.items() if k != "destination"}
            if destination.exists():
                results.append(metadata | {"status": "reused", "path": str(destination), "size_bytes": destination.stat().st_size, "driver": str(driver_path)})
                continue

            ensure_dir(destination.parent)
            expected_name = destination.name
            tmp_target = tmp_root / expected_name
            if tmp_target.exists():
                try:
                    tmp_target.unlink()
                except Exception:
                    pass

            try:
                driver.get(url)
                title = (driver.title or "").lower()
                current_url = (driver.current_url or "").lower()
                if "page not found" in title or "access denied" in title or "/tata/hts/" in current_url:
                    results.append(
                        metadata
                        | {
                            "status": "failed",
                            "error": "BrowserNavigationFailed: endpoint unavailable from browser session",
                            "driver": str(driver_path),
                            "final_url": driver.current_url,
                            "title": driver.title,
                        }
                    )
                    continue
                downloaded = _wait_for_download(tmp_root, expected_name, timeout_seconds=timeout_seconds)
                if downloaded and downloaded.exists():
                    downloaded.replace(destination)
                    results.append(metadata | {"status": "downloaded", "path": str(destination), "size_bytes": destination.stat().st_size, "driver": str(driver_path)})
                else:
                    # Some endpoints render in-page instead of triggering a file download.
                    # Preserve this as an explicit failure to keep fallback routing transparent.
                    results.append(
                        metadata
                        | {
                            "status": "failed",
                            "error": "DownloadTimeout: file not downloaded from browser navigation",
                            "driver": str(driver_path),
                            "final_url": driver.current_url,
                            "title": driver.title,
                        }
                    )
            except Exception as exc:
                results.append(metadata | {"status": "failed", "error": f"{type(exc).__name__}: {exc}", "driver": str(driver_path)})

        return results
    except Exception as exc:
        return [
            {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "driver": str(driver_path),
                **{k: v for k, v in item.items() if k != "destination"},
            }
            for item in items
        ]
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def _download_annual_zip_via_selenium(
    year: int,
    annual_zip_url: str,
    destination: Path,
    browser: str,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Download annual ZIP using a real browser session (fallback for 403s)."""
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as exc:  # pragma: no cover
        return {"status": "failed", "error": f"SeleniumImportError: {exc}"}

    ensure_dir(destination.parent)
    if destination.exists():
        return {"status": "reused", "path": str(destination), "size_bytes": destination.stat().st_size}

    driver_path = _find_local_driver("msedgedriver" if browser == "edge" else "chromedriver")
    if not driver_path:
        return {"status": "failed", "error": f"DriverNotFound: {browser} driver missing under {DRIVER_DIR}"}

    tmp_root = ensure_dir(destination.parent / "_tmp_selenium" / browser / str(year))
    expected_name = f"tariff_data_{year}.zip"
    expected_path = tmp_root / expected_name
    if expected_path.exists():
        expected_path.unlink()

    prefs = {
        "download.default_directory": str(tmp_root.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }

    driver = None
    try:
        if browser == "edge":
            from selenium.webdriver.edge.service import Service as EdgeService
            from selenium.webdriver.edge.options import Options as EdgeOptions

            options = EdgeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-minimized")
            options.add_argument(f"--user-data-dir={str((tmp_root / 'profile').resolve())}")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Edge(service=EdgeService(str(driver_path)), options=options)
        else:
            from selenium.webdriver.chrome.service import Service as ChromeService
            from selenium.webdriver.chrome.options import Options as ChromeOptions

            options = ChromeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-minimized")
            options.add_argument(f"--user-data-dir={str((tmp_root / 'profile').resolve())}")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Chrome(service=ChromeService(str(driver_path)), options=options)

        # Headless Chromium requires explicit download permission.
        try:
            driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": str(tmp_root.resolve())},
            )
        except Exception:
            pass

        driver.set_page_load_timeout(60)
        driver.get(DATAWEB_ANNUAL_URL)

        link_css = f"a[href*='tariff_data_{year}.zip']"
        try:
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, link_css)))
            link = driver.find_element(By.CSS_SELECTOR, link_css)
            driver.execute_script("arguments[0].click();", link)
        except Exception:
            driver.get(annual_zip_url)

        downloaded = _wait_for_download(tmp_root, expected_name, timeout_seconds=timeout_seconds)
        if not downloaded or not downloaded.exists():
            return {
                "status": "failed",
                "error": "DownloadTimeout: annual ZIP did not appear",
                "download_dir": str(tmp_root),
                "driver": str(driver_path),
            }

        downloaded.replace(destination)
        return {
            "status": "downloaded",
            "path": str(destination),
            "size_bytes": destination.stat().st_size,
            "driver": str(driver_path),
        }
    except Exception as exc:
        return {"status": "failed", "error": f"{type(exc).__name__}: {exc}", "driver": str(driver_path)}
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def _download_release_export_csv_via_selenium(
    release_name: str,
    release_date: str | None,
    destination: Path,
    browser: str,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    """Use HTS UI export modal to download a full CSV for a release."""
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait, Select
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as exc:  # pragma: no cover
        return {"status": "failed", "error": f"SeleniumImportError: {exc}"}

    ensure_dir(destination.parent)
    if destination.exists():
        return {"status": "reused", "path": str(destination), "size_bytes": destination.stat().st_size}

    driver_path = _find_local_driver("msedgedriver" if browser == "edge" else "chromedriver")
    if not driver_path:
        return {"status": "failed", "error": f"DriverNotFound: {browser} driver missing under {DRIVER_DIR}"}

    tmp_root = ensure_dir(destination.parent / "_tmp_selenium_release_export" / browser / release_name)
    profile_dir = ensure_dir(tmp_root / "profile")
    for old in tmp_root.glob("htsdata*.csv"):
        try:
            old.unlink()
        except Exception:
            pass

    prefs = {
        "download.default_directory": str(tmp_root.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }

    driver = None
    try:
        if browser == "edge":
            from selenium.webdriver.edge.service import Service as EdgeService
            from selenium.webdriver.edge.options import Options as EdgeOptions

            options = EdgeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-maximized")
            options.add_argument(f"--user-data-dir={str(profile_dir.resolve())}")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Edge(service=EdgeService(str(driver_path)), options=options)
        else:
            from selenium.webdriver.chrome.service import Service as ChromeService
            from selenium.webdriver.chrome.options import Options as ChromeOptions

            options = ChromeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-maximized")
            options.add_argument(f"--user-data-dir={str(profile_dir.resolve())}")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Chrome(service=ChromeService(str(driver_path)), options=options)

        try:
            driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": str(tmp_root.resolve())},
            )
        except Exception:
            pass

        release_url = _build_query_url(DOWNLOAD_RELEASE_URL, {"release": release_name})
        driver.set_page_load_timeout(90)
        driver.get(release_url)
        wait = WebDriverWait(driver, 45)

        try:
            chapter_button = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Export Chapter 1']")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", chapter_button)
            driver.execute_script("arguments[0].click();", chapter_button)
        except Exception:
            # Older/special releases can expose the export form directly without chapter buttons.
            pass

        from_input = wait.until(EC.visibility_of_element_located((By.ID, "from")))
        to_input = wait.until(EC.visibility_of_element_located((By.ID, "to")))
        export_format = wait.until(EC.visibility_of_element_located((By.ID, "exportFormat")))

        from_input.clear()
        from_input.send_keys("0101.00.0000")
        to_input.clear()
        to_input.send_keys("9999.99.9999")
        Select(export_format).select_by_value("CSV")

        submit = wait.until(EC.element_to_be_clickable((By.NAME, "exportSubmit")))
        driver.execute_script("arguments[0].click();", submit)

        downloaded = _wait_for_download(tmp_root, "htsdata.csv", timeout_seconds=timeout_seconds)
        if not downloaded or not downloaded.exists():
            return {
                "status": "failed",
                "error": "DownloadTimeout: release export CSV did not appear",
                "release_url": release_url,
                "driver": str(driver_path),
            }

        downloaded.replace(destination)
        return {
            "status": "downloaded",
            "path": str(destination),
            "size_bytes": destination.stat().st_size,
            "release_url": release_url,
            "driver": str(driver_path),
        }
    except Exception as exc:
        return {"status": "failed", "error": f"{type(exc).__name__}: {exc}", "driver": str(driver_path)}
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def _build_query_url(base_url: str, params: dict[str, Any]) -> str:
    return f"{base_url}?{urlencode(params)}"


def _release_year(release: dict[str, Any]) -> int | None:
    for field in ("description", "title"):
        value = str(release.get(field, ""))
        match = re.search(r"(19|20)\d{2}", value)
        if match:
            return int(match.group(0))
    for field in ("releaseStartDate", "target", "releaseEndDate", "date"):
        value = str(release.get(field) or "")
        match = re.search(r"(19|20)\d{2}", value)
        if match:
            return int(match.group(0))
    return None


def _release_kind(release: dict[str, Any]) -> tuple[str, str | None]:
    description = str(release.get("description", "")).lower()
    if "preliminary" in description:
        return "preliminary", None
    if "basic" in description:
        return "basic", None
    revision_match = REVISION_RE.search(description)
    if revision_match:
        token = revision_match.group("major")
        if revision_match.group("minor"):
            token = f"{token}_{revision_match.group('minor')}"
        return "revision", token
    return "other", None


def _machine_readable_stem(release: dict[str, Any]) -> str | None:
    year = _release_year(release)
    if year is None:
        return None
    kind, revision_token = _release_kind(release)
    if kind == "preliminary":
        return f"hts_{year}_preliminary_data"
    if kind == "basic":
        return f"hts_{year}_basic_data"
    if kind == "revision" and revision_token:
        return f"hts_{year}_revision_{revision_token}_data"
    return None


def _machine_readable_rule_status(release: dict[str, Any]) -> str:
    year = _release_year(release)
    kind, revision_token = _release_kind(release)
    if year == 2017 and kind == "preliminary":
        return "known_machine_readable"
    if year == 2017 and kind in {"basic", "revision"}:
        return "known_html_only"
    if kind == "revision" and revision_token and "_" in revision_token:
        return "likely_html_only"
    if year is not None and year >= 2018 and kind in {"basic", "revision", "preliminary"}:
        return "candidate_machine_readable"
    return "unknown"


def _release_record(release: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
    year = _release_year(release)
    stem = _machine_readable_stem(release)
    machine_readable_urls = {
        ext: ARCHIVE_DATA_BASE.format(stem=stem, ext=ext)
        for ext in ("csv", "xls", "json")
    } if stem else {}
    pdf_files = sorted(str(name) for name in details.get("pdfList", {}).keys())
    return {
        "release_name": release.get("name"),
        "release_description": release.get("description"),
        "release_title": release.get("title"),
        "release_date": release.get("date"),
        "release_time": release.get("time"),
        "release_target": release.get("target"),
        "release_start_date": release.get("releaseStartDate"),
        "release_end_date": release.get("releaseEndDate"),
        "release_status": release.get("status"),
        "release_creator": release.get("creator"),
        "year": year,
        "edition_kind": _release_kind(release)[0],
        "revision_token": _release_kind(release)[1],
        "machine_readable_stem": stem,
        "machine_readable_stem_core": stem.removesuffix("_data") if stem else None,
        "machine_readable_rule_status": _machine_readable_rule_status(release),
        "archive_download_page_url": ARCHIVE_DOWNLOAD_PAGE.format(release=release.get("name")),
        "archive_pdf_url": ARCHIVE_PDF_PATTERN.format(release=release.get("name")),
        "archive_machine_readable_urls": machine_readable_urls,
        "annual_zip_url": ANNUAL_ZIP_PATTERN.format(year=year) if year is not None else None,
        "merged_revisions": release.get("mergedRevisions") or [],
        "pdf_section_count": len(pdf_files),
        "pdf_section_names": pdf_files,
    }


def _archive_list_page_url(page: int) -> str:
    return _build_query_url(USITC_ARCHIVE_LIST, {"page": page})


def _extract_archive_links(html: str, page_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for match in ARCHIVE_MACHINE_READABLE_LINK_RE.finditer(html):
        href = unescape(match.group("href")).strip()
        if not href:
            continue
        url = urljoin(page_url, href)
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _extract_release_name(url: str) -> str | None:
    match = ARCHIVE_RELEASE_LINK_RE.search(url)
    if not match:
        return None
    return match.group("release")


def _parse_archive_filename(url: str) -> dict[str, Any] | None:
    name = Path(urlparse(url).path).name
    if not name:
        return None
    ext = Path(name).suffix.lstrip(".").lower()
    if ext not in {"csv", "xls", "xlsx", "json"}:
        return None
    stem = Path(name).stem
    match = ARCHIVE_FILENAME_RE.match(stem)
    if not match:
        return None
    year = int(match.group("year"))
    body = match.group("body")
    tokens = [token for token in body.lower().split("_") if token]
    if not tokens:
        return None
    # Some machine-readable links include a trailing format token in the stem:
    # e.g. hts_2018_revision_1_csv.csv
    if tokens[-1] in {"csv", "xls", "xlsx", "json", "excel"}:
        tokens = tokens[:-1]
    if tokens and tokens[-1].isdigit() and len(tokens) >= 2 and tokens[-2] in {"csv", "xls", "xlsx", "json", "excel"}:
        tokens = tokens[:-2]
    if not tokens:
        return None
    has_data_suffix = tokens[-1] == "data"
    if has_data_suffix:
        tokens = tokens[:-1]
    is_basic_variant = "basic" in tokens

    edition_kind = "other"
    revision_token: str | None = None
    if tokens[0] == "preliminary":
        edition_kind = "preliminary"
    elif tokens[0] == "basic":
        edition_kind = "basic"
    elif tokens[0] in {"revision", "revisions"}:
        edition_kind = "revision"
        numeric_parts: list[str] = []
        for token in tokens[1:]:
            if token.isdigit():
                numeric_parts.append(token)
                continue
            break
        if numeric_parts:
            revision_token = "_".join(numeric_parts)

    stem_core = f"hts_{year}_{'_'.join(tokens)}"
    return {
        "url": url,
        "file_name": name,
        "file_ext": ext,
        "machine_readable_stem": stem,
        "machine_readable_stem_core": stem_core,
        "year": year,
        "edition_kind": edition_kind,
        "revision_token": revision_token,
        "is_basic_variant": is_basic_variant,
        "has_data_suffix": has_data_suffix,
    }


def _fetch_archive_revision_index(
    config: PipelineConfig,
    session: requests.Session,
    max_pages: int = USITC_ARCHIVE_LIST_PAGE_MAX,
) -> list[dict[str, Any]]:
    # Selenium-first strategy: walk archive page cards/divs and collect direct machine-readable links.
    for browser in ("edge", "chrome"):
        selenium_records = _fetch_archive_revision_index_via_selenium(config, browser=browser, max_pages=max_pages)
        if selenium_records:
            selenium_records.sort(key=lambda row: (int(row["year"]), str(row["edition_kind"]), str(row.get("revision_token") or ""), str(row["file_name"])))
            return selenium_records

    # Requests fallback when Selenium is unavailable.
    years = set(_policy_years(config))
    records: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    hit_nonempty_page = False

    for page in range(max_pages):
        page_url = _archive_list_page_url(page)
        try:
            response = session.get(page_url, headers=REQUEST_HEADERS, timeout=60)
            if response.status_code >= 400:
                if hit_nonempty_page:
                    break
                continue
            links = _extract_archive_links(response.text, page_url)
        except Exception:
            if hit_nonempty_page:
                break
            continue
        page_added = 0
        for url in links:
            if url in seen_urls:
                continue
            parsed = _parse_archive_filename(url)
            if not parsed:
                continue
            if parsed["year"] not in years:
                continue
            seen_urls.add(url)
            records.append(parsed | {"archive_list_page": page})
            page_added += 1
        if page_added > 0:
            hit_nonempty_page = True
        elif hit_nonempty_page:
            break

    records.sort(key=lambda row: (int(row["year"]), str(row["edition_kind"]), str(row.get("revision_token") or ""), str(row["file_name"])))
    return records


def _fetch_archive_revision_index_via_selenium(
    config: PipelineConfig,
    browser: str,
    max_pages: int = USITC_ARCHIVE_LIST_PAGE_MAX,
) -> list[dict[str, Any]]:
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
    except Exception:
        return []

    driver_path = _find_local_driver("msedgedriver" if browser == "edge" else "chromedriver")
    if not driver_path:
        return []

    years = set(_policy_years(config))
    records: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    hit_nonempty_page = False
    tmp_root = ensure_dir(_policy_dir(config) / "archive" / "_tmp_selenium_index" / browser)
    profile_dir = ensure_dir(tmp_root / "profile")
    driver = None
    try:
        if browser == "edge":
            from selenium.webdriver.edge.service import Service as EdgeService
            from selenium.webdriver.edge.options import Options as EdgeOptions

            options = EdgeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-minimized")
            options.add_argument(f"--user-data-dir={str(profile_dir.resolve())}")
            driver = webdriver.Edge(service=EdgeService(str(driver_path)), options=options)
        else:
            from selenium.webdriver.chrome.service import Service as ChromeService
            from selenium.webdriver.chrome.options import Options as ChromeOptions

            options = ChromeOptions()
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--start-minimized")
            options.add_argument(f"--user-data-dir={str(profile_dir.resolve())}")
            driver = webdriver.Chrome(service=ChromeService(str(driver_path)), options=options)

        driver.set_page_load_timeout(90)
        for page in range(max_pages):
            page_url = _archive_list_page_url(page)
            try:
                driver.get(page_url)
            except Exception:
                if hit_nonempty_page:
                    break
                continue
            time.sleep(2)
            links: list[tuple[str, str | None]] = []
            # Parse by archive "cards" / div sections keyed by release links.
            release_anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/view/release?release=']")
            if release_anchors:
                for anchor in release_anchors:
                    href = (anchor.get_attribute("href") or "").strip()
                    if not href:
                        continue
                    release_name = _extract_release_name(href)
                    containers = []
                    try:
                        containers.append(anchor.find_element(By.XPATH, "ancestor::div[1]"))
                    except Exception:
                        pass
                    try:
                        containers.append(anchor.find_element(By.XPATH, "ancestor::div[2]"))
                    except Exception:
                        pass
                    if not containers:
                        continue
                    machine_urls: list[str] = []
                    for container in containers:
                        found = container.find_elements(By.CSS_SELECTOR, "a[href*='/sites/default/files/tata/hts/']")
                        for node in found:
                            u = (node.get_attribute("href") or "").strip()
                            if u:
                                machine_urls.append(urljoin(page_url, u))
                        if machine_urls:
                            break
                    for u in machine_urls:
                        links.append((u, release_name))
            # Always union with full-page parser to avoid missing links outside the nearest card container.
            page_links = _extract_archive_links(driver.page_source or "", page_url)
            known_page_urls = {u for u, _ in links}
            for u in page_links:
                if u not in known_page_urls:
                    links.append((u, None))
            page_added = 0
            for url, release_name in links:
                if url in seen_urls:
                    continue
                parsed = _parse_archive_filename(url)
                if not parsed:
                    continue
                if parsed["year"] not in years:
                    continue
                seen_urls.add(url)
                records.append(
                    parsed
                    | {
                        "archive_list_page": page,
                        "archive_index_source": f"selenium_{browser}",
                        "archive_release_name": release_name,
                    }
                )
                page_added += 1
            if page_added > 0:
                hit_nonempty_page = True
            elif hit_nonempty_page:
                break
        return records
    except Exception:
        return []
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def _archive_urls_for_release(release: dict[str, Any], archive_index: list[dict[str, Any]]) -> dict[str, str]:
    release_year = release.get("year")
    if release_year is None:
        return {}
    release_name = str(release.get("name") or "")
    release_kind, release_token = _release_kind(release)
    candidates = [row for row in archive_index if row.get("year") == release_year]
    if not candidates:
        return {}

    # Priority 1: direct release-name mapping from archive page cards.
    matches = [row for row in candidates if str(row.get("archive_release_name") or "") == release_name]
    if not matches:
        if release_kind == "revision":
            matches = [row for row in candidates if row.get("edition_kind") == "revision" and row.get("revision_token") == release_token]
        elif release_kind in {"basic", "preliminary"}:
            matches = [row for row in candidates if row.get("edition_kind") == release_kind]
        else:
            matches = []

    if not matches:
        return {}

    out: dict[str, str] = {}
    for row in matches:
        ext = str(row.get("file_ext") or "").lower()
        if ext not in out:
            out[ext] = str(row["url"])
    return out


def _fetch_release_catalog(
    config: PipelineConfig,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    policy_dir = _policy_dir(config)
    catalog_dir = ensure_dir(policy_dir / "archive" / "catalog")
    detail_dir = ensure_dir(catalog_dir / "details")

    client = session or _build_session()

    release_list = _json_get(RESTSTOP_BASE + "releaseList", session=client)
    current_release = _json_get(RESTSTOP_BASE + "currentRelease", session=client)

    write_metadata_json(catalog_dir / "release_list.json", {"releases": release_list})
    write_metadata_json(catalog_dir / "current_release.json", current_release)

    years = set(_policy_years(config))
    filtered_releases = [release for release in release_list if _release_year(release) in years]
    filtered_releases.sort(key=lambda item: ((item.get("releaseStartDate") or ""), item.get("description") or ""))

    archive_index = _fetch_archive_revision_index(config, session=client)
    archive_links_by_url = {row["url"]: row for row in archive_index}
    catalog: list[dict[str, Any]] = []
    for release in filtered_releases:
        detail = _json_get(RESTSTOP_BASE + f"releaseDetails?release={release['name']}", session=client)
        write_metadata_json(detail_dir / f"{release['name']}.json", detail)
        record = _release_record(release, detail)
        record["archive_machine_readable_urls"] = _archive_urls_for_release(release, archive_index)
        record["archive_machine_readable_index_rows"] = [
            archive_links_by_url[url] for url in record["archive_machine_readable_urls"].values() if url in archive_links_by_url
        ]
        catalog.append(record)

    current_name = current_release.get("name")
    current_description = current_release.get("description")
    return catalog, archive_index, {
        "current_release_name": current_name,
        "current_release_description": current_description,
        "requested_years": sorted(years),
        "archive_revision_index_count": len(archive_index),
    }


def _probe_catalog_urls(
    catalog: list[dict[str, Any]],
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    client = session or _build_session()
    for record in catalog:
        annual_url = record.get("annual_zip_url")
        if annual_url and annual_url not in seen:
            probe = _safe_head(annual_url, session=client)
            records.append(
                {
                    "source_type": "annual_zip",
                    "release_name": None,
                    "year": record.get("year"),
                    "url": annual_url,
                    **probe,
                }
            )
            seen.add(annual_url)
        for ext, url in (record.get("archive_machine_readable_urls") or {}).items():
            if not url or url in seen:
                continue
            probe = _safe_head(url, session=client)
            records.append(
                {
                    "source_type": f"archive_{ext}",
                    "release_name": record.get("release_name"),
                    "year": record.get("year"),
                    "url": url,
                    **probe,
                }
            )
            seen.add(url)
        pdf_url = record.get("archive_pdf_url")
        if pdf_url and pdf_url not in seen:
            probe = _safe_head(pdf_url, session=client)
            records.append(
                {
                    "source_type": "archive_pdf",
                    "release_name": record.get("release_name"),
                    "year": record.get("year"),
                    "url": pdf_url,
                    **probe,
                }
            )
            seen.add(pdf_url)
    return records


def _inspect_local_files(config: PipelineConfig) -> dict[str, Any]:
    policy_dir = _policy_dir(config)
    annual_dir = ensure_dir(policy_dir / "annual")
    archive_dir = ensure_dir(policy_dir / "archive")
    current_dir = ensure_dir(policy_dir / "current")
    return {
        "annual_files": sorted(path.name for path in annual_dir.iterdir() if path.is_file()),
        "archive_catalog_files": sorted(path.name for path in (archive_dir / "catalog").iterdir() if path.is_file()) if (archive_dir / "catalog").exists() else [],
        "archive_pdf_files": sorted(path.name for path in (archive_dir / "pdf").iterdir() if path.is_file()) if (archive_dir / "pdf").exists() else [],
        "archive_data_files": sorted(path.name for path in (archive_dir / "data").iterdir() if path.is_file()) if (archive_dir / "data").exists() else [],
        "current_export_files": sorted(path.name for path in (current_dir / "export").iterdir() if path.is_file()) if (current_dir / "export").exists() else [],
        "current_range_files": sorted(path.name for path in (current_dir / "ranges").iterdir() if path.is_file()) if (current_dir / "ranges").exists() else [],
    }


def _serialize_nested_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    for column in out.columns:
        if out[column].dtype != "object":
            continue
        sample = next((value for value in out[column] if isinstance(value, (dict, list))), None)
        if sample is None:
            continue
        out[column] = out[column].map(
            lambda value: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
        )
    return out


def _write_source_markdown(config: PipelineConfig, catalog: list[dict[str, Any]], probes: list[dict[str, Any]], context: dict[str, Any]) -> Path:
    docs_path = config.repo_root / "scr" / "docs" / "hts_policy_sources.md"
    ensure_dir(docs_path.parent)

    known_html_only = sorted(record["release_description"] for record in catalog if record["machine_readable_rule_status"] == "known_html_only")
    likely_html_only = sorted(record["release_description"] for record in catalog if record["machine_readable_rule_status"] == "likely_html_only")
    candidate_count = sum(1 for record in catalog if record["machine_readable_rule_status"] == "candidate_machine_readable")
    annual_ok = sum(1 for probe in probes if probe["source_type"] == "annual_zip" and probe["ok"])
    archive_data_ok = sum(1 for probe in probes if probe["source_type"].startswith("archive_") and probe["source_type"] != "archive_pdf" and probe["ok"])

    lines = [
        "# HTS Policy Sources",
        "",
        "## Machine-Readable-First Source Map",
        "",
        "Primary metadata source:",
        f"- `https://hts.usitc.gov/reststop/releaseList`",
        f"- `https://hts.usitc.gov/reststop/currentRelease`",
        f"- `https://hts.usitc.gov/reststop/releaseDetails?release=<release>`",
        "",
        "Primary historical revision index source (machine-readable links):",
        f"- `https://www.usitc.gov/harmonized_tariff_information/hts/archive/list?page=<n>` (current implementation paginates `n=0..7`)",
        "",
        "Primary working machine-readable source for the current HTS release:",
        f"- `https://hts.usitc.gov/reststop/ranges?docNumber=<chapter>`",
        f"- `https://hts.usitc.gov/reststop/exportList?from=<start>&to=<end>&format=CSV&styles=true`",
        "",
        "Annual baseline candidate source:",
        f"- `https://www.usitc.gov/tariff_affairs/documents/tariff_data/tariff_data_<year>.zip`",
        "",
        "Archive machine-readable patterns observed from the index:",
        f"- `https://www.usitc.gov/sites/default/files/tata/hts/hts_<year>_<edition>_<format>.<ext>`",
        f"- `https://www.usitc.gov/sites/default/files/tata/hts/hts_<year>_<edition>_data.<ext>`",
        "",
        "Archive PDF fallback pattern:",
        f"- `https://hts.usitc.gov/reststop/file?release=<release>&filename=finalCopy`",
        "",
        "Machine-readable UI export fallback page:",
        f"- `https://hts.usitc.gov/view/release?release=<release>`",
        "",
        "## Current Implementation",
        "",
        "- The pipeline now treats HTS `reststop` endpoints as the canonical release catalog source.",
        "- It uses `ranges` plus `exportList` to download machine-readable CSV chapter exports for the current HTS release.",
        "- It builds annual ZIP candidates for each requested year.",
        "- It paginates `usitc.gov/.../archive/list` to build an explicit machine-readable revision index.",
        "- It maps release metadata onto indexed archive links when possible, and downloads unmatched indexed links by year as well.",
        "- If indexed CSV/JSON/XLSX files are unavailable for a release, it attempts UI-export fallback (`from=0101.00.0000`, `to=9999.99.9999`, format=CSV) before PDF fallback.",
        "- It uses Selenium browser fallback for annual ZIP and archive machine-readable downloads when direct HTTP is blocked.",
        "- It uses archive full-edition PDFs as fallback when archive machine-readable retrieval is unavailable.",
        "",
        "## Coverage Notes",
        "",
        f"- Requested years in this repo run: `{', '.join(str(year) for year in context['requested_years'])}`",
        f"- Current HTS release at fetch time: `{context['current_release_name']}` / `{context['current_release_description']}`",
        f"- Releases in catalog for requested years: `{len(catalog)}`",
        f"- Archive machine-readable links indexed for requested years: `{context.get('archive_revision_index_count', 0)}`",
        f"- Annual ZIP URLs returning success on HEAD probe: `{annual_ok}`",
        f"- Archive machine-readable URLs returning success on HEAD probe: `{archive_data_ok}`",
        "",
        "## Retrieval Findings",
        "",
        "- `exportList` is a working machine-readable endpoint on `hts.usitc.gov`.",
        "- `ranges?docNumber=<chapter>` returns the correct start/end bounds for chapter-level exports.",
        "- The current HTS frontend does not pass a release identifier into `exportList`; testing release/session variants produced identical output, so the endpoint should be treated as current-release only.",
        "- Direct downloads from `www.usitc.gov/tariff_affairs/...` and `www.usitc.gov/sites/default/files/...` can be blocked from this environment with `Access Denied` responses.",
        "- The archive-list pages provide a much better link-discovery surface than deterministic filename guessing.",
        "- Annual ZIP downloads are recoverable through Selenium browser sessions in this environment.",
        "- Archive full-edition PDFs remain retrievable via `reststop/file?release=<release>&filename=finalCopy`.",
        "",
        "Known HTML-only releases from the current ruleset:",
    ]
    if known_html_only:
        lines.extend(f"- `{description}`" for description in known_html_only)
    else:
        lines.append("- none recorded")
    lines.extend([
        "",
        "Likely HTML-only releases requiring manual confirmation:",
    ])
    if likely_html_only:
        lines.extend(f"- `{description}`" for description in likely_html_only)
    else:
        lines.append("- none recorded")
    lines.extend([
        "",
        "Candidate machine-readable releases in the requested window:",
        f"- `{candidate_count}` releases marked as likely CSV/XLS/JSON candidates",
        "",
        "## Important Caveat",
        "",
        "- In this environment, the current-release machine-readable endpoints on `hts.usitc.gov` are retrievable.",
        "- Direct GET downloads from `www.usitc.gov/sites/default/files/...` and the annual ZIP pattern can still return `403 Access Denied`.",
        "- The downloader retries blocked annual/archive URLs via Selenium and records both direct-request and browser-attempt outcomes in the manifest.",
        "- For releases where archive machine-readable files still cannot be obtained, PDF fallback remains active.",
    ])

    docs_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return docs_path


def build_policy_inventory(
    config: PipelineConfig,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    policy_dir = _policy_dir(config)
    catalog_dir = ensure_dir(policy_dir / "archive" / "catalog")
    client = session or _build_session()
    _warm_session(client)

    catalog, archive_index, context = _fetch_release_catalog(config, session=client)
    probes = _probe_catalog_urls(catalog, session=client)

    catalog_frame = pd.DataFrame(catalog)
    if not catalog_frame.empty:
        serial_catalog = _serialize_nested_columns(catalog_frame)
        write_parquet(serial_catalog, config.reference_dir / "policy_release_catalog.parquet")
        serial_catalog.to_csv(config.reference_dir / "policy_release_catalog.csv", index=False)

    probe_frame = pd.DataFrame(probes)
    if not probe_frame.empty:
        write_parquet(probe_frame, config.reference_dir / "policy_source_probes.parquet")
        probe_frame.to_csv(config.reference_dir / "policy_source_probes.csv", index=False)

    archive_index_frame = pd.DataFrame(archive_index)
    if not archive_index_frame.empty:
        serial_index = _serialize_nested_columns(archive_index_frame)
        write_parquet(serial_index, config.reference_dir / "policy_archive_revision_index.parquet")
        serial_index.to_csv(config.reference_dir / "policy_archive_revision_index.csv", index=False)
    write_metadata_json(catalog_dir / "archive_revision_index.json", {"records": archive_index})

    source_doc = _write_source_markdown(config, catalog, probes, context)
    inventory = {
        "requested_years": context["requested_years"],
        "current_release_name": context["current_release_name"],
        "current_release_description": context["current_release_description"],
        "archive_revision_index_count": context["archive_revision_index_count"],
        "release_count": len(catalog),
        "releases": catalog,
        "archive_revision_index": archive_index,
        "probes": probes,
        "local_files": _inspect_local_files(config),
        "catalog_dir": str(catalog_dir),
        "source_doc": str(source_doc),
    }
    write_metadata_json(config.verification_dir / "policy_source_inventory.json", inventory)
    return inventory


def _download_current_release_exports(config: PipelineConfig, session: requests.Session) -> dict[str, Any]:
    policy_dir = _policy_dir(config)
    current_dir = ensure_dir(policy_dir / "current")
    ranges_dir = ensure_dir(current_dir / "ranges")
    export_dir = ensure_dir(current_dir / "export")

    current_release = _json_get(RESTSTOP_BASE + "currentRelease", session=session)
    current_release_name = str(current_release.get("name") or "currentRelease")
    current_release_description = str(current_release.get("description") or current_release_name)

    range_records: list[dict[str, Any]] = []
    download_attempts: list[dict[str, Any]] = []

    for chapter in range(1, 100):
        if chapter in SKIPPED_CHAPTERS:
            range_records.append(
                {
                    "chapter_number": chapter,
                    "starting_number": None,
                    "ending_number": None,
                    "current_release_name": current_release_name,
                    "current_release_description": current_release_description,
                    "status": "skipped",
                    "error": "reserved chapter with no HTS schedule",
                }
            )
            download_attempts.append(
                {
                    "source_type": "current_export_csv",
                    "chapter_number": chapter,
                    "current_release_name": current_release_name,
                    "current_release_description": current_release_description,
                    "range_start": None,
                    "range_end": None,
                    "url": None,
                    "download": {
                        "status": "skipped",
                        "reason": "reserved chapter with no HTS schedule",
                    },
                }
            )
            continue
        try:
            range_payload = _json_get(
                _build_query_url(RESTSTOP_RANGES, {"docNumber": str(chapter)}),
                session=session,
            )
            start_number = str(range_payload["Starting_Number"])
            end_number = str(range_payload["Ending_Number"])
            range_record = {
                "chapter_number": chapter,
                "starting_number": start_number,
                "ending_number": end_number,
                "current_release_name": current_release_name,
                "current_release_description": current_release_description,
                "status": "ok",
            }
            range_records.append(range_record)
            write_metadata_json(ranges_dir / f"{current_release_name}_chapter_{chapter:02d}.json", range_payload)

            export_url = _build_query_url(
                RESTSTOP_EXPORT_LIST,
                {
                    "from": start_number,
                    "to": end_number,
                    "format": "CSV",
                    "styles": "true",
                },
            )
            destination = export_dir / f"{current_release_name}_chapter_{chapter:02d}.csv"
            download_attempt = {
                "source_type": "current_export_csv",
                "chapter_number": chapter,
                "current_release_name": current_release_name,
                "current_release_description": current_release_description,
                "range_start": start_number,
                "range_end": end_number,
                "url": export_url,
                "download": _download_binary(export_url, destination, session=session),
            }
            download_attempts.append(download_attempt)
        except Exception as exc:
            LOGGER.warning("Current HTS chapter export failed for chapter %s: %s", chapter, exc)
            range_records.append(
                {
                    "chapter_number": chapter,
                    "starting_number": None,
                    "ending_number": None,
                    "current_release_name": current_release_name,
                    "current_release_description": current_release_description,
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            download_attempts.append(
                {
                    "source_type": "current_export_csv",
                    "chapter_number": chapter,
                    "current_release_name": current_release_name,
                    "current_release_description": current_release_description,
                    "range_start": None,
                    "range_end": None,
                    "url": None,
                    "download": {
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                }
            )
        time.sleep(0.15)

    range_frame = pd.DataFrame(range_records)
    if not range_frame.empty:
        write_parquet(range_frame, config.reference_dir / "policy_current_release_ranges.parquet")
        range_frame.to_csv(config.reference_dir / "policy_current_release_ranges.csv", index=False)

    current_manifest = {
        "current_release_name": current_release_name,
        "current_release_description": current_release_description,
        "chapter_count": len(range_records),
        "ranges": range_records,
        "downloads": download_attempts,
    }
    write_metadata_json(config.verification_dir / "policy_current_release_exports.json", current_manifest)
    return current_manifest


def run_policy_source_download(config: PipelineConfig) -> dict[str, Any]:
    """Fetch HTS release metadata and attempt machine-readable source downloads."""
    session = _build_session()
    _warm_session(session)

    inventory = build_policy_inventory(config, session=session)
    policy_dir = _policy_dir(config)
    annual_dir = ensure_dir(policy_dir / "annual")
    archive_data_dir = ensure_dir(policy_dir / "archive" / "data")
    archive_pdf_dir = ensure_dir(policy_dir / "archive" / "pdf")
    probe_by_url = {probe["url"]: probe for probe in inventory["probes"]}

    download_attempts: list[dict[str, Any]] = []
    # Annual ZIPs exist independently of the HTS release catalog. Build them directly
    # from the requested year range so older years are not skipped if releaseList is truncated.
    annual_urls: dict[int, str] = {year: ANNUAL_ZIP_PATTERN.format(year=year) for year in _policy_years(config)}
    for year, url in sorted(annual_urls.items()):
        destination = annual_dir / f"tariff_data_{year}.zip"
        attempt = {"source_type": "annual_zip", "year": year, "url": url}
        probe = probe_by_url.get(url) or _safe_head(url, session=session)
        attempt["head"] = probe
        attempt["download"] = _download_binary(url, destination, session=session)
        if attempt["download"]["status"] == "failed" and not destination.exists():
            status_code = probe.get("status_code")
            if status_code in {401, 403} or "403" in str(attempt["download"].get("error") or ""):
                # Defer to a single batch Selenium session to avoid launching a browser per year.
                attempt["selenium_batch_deferred"] = True
        download_attempts.append(attempt)

    deferred_years = []
    for attempt in download_attempts:
        if attempt.get("source_type") == "annual_zip" and attempt.get("selenium_batch_deferred"):
            year = int(attempt["year"])
            url = str(attempt["url"])
            destination = annual_dir / f"tariff_data_{year}.zip"
            deferred_years.append({"year": year, "url": url, "destination": destination})

    if deferred_years:
        batch_edge = _download_annual_zips_via_selenium_batch(deferred_years, browser="edge")
        batch_by_year = {int(row["year"]): row for row in batch_edge}
        for attempt in download_attempts:
            if attempt.get("source_type") == "annual_zip" and attempt.get("selenium_batch_deferred"):
                attempt["selenium_edge"] = batch_by_year.get(int(attempt["year"]), {"status": "failed", "error": "MissingBatchResult"})

        remaining = [item for item in deferred_years if batch_by_year.get(int(item["year"]), {}).get("status") != "downloaded"]
        if remaining:
            batch_chrome = _download_annual_zips_via_selenium_batch(remaining, browser="chrome")
            chrome_by_year = {int(row["year"]): row for row in batch_chrome}
            for attempt in download_attempts:
                if attempt.get("source_type") == "annual_zip" and attempt.get("selenium_batch_deferred"):
                    year = int(attempt["year"])
                    if attempt.get("selenium_edge", {}).get("status") != "downloaded":
                        attempt["selenium_chrome"] = chrome_by_year.get(year, {"status": "failed", "error": "MissingBatchResult"})

    machine_readable_success: dict[str, bool] = {}
    downloaded_archive_urls: set[str] = set()
    deferred_archive_items: list[dict[str, Any]] = []
    deferred_archive_attempts: list[dict[str, Any]] = []
    for release in inventory["releases"]:
        if release.get("machine_readable_rule_status") == "known_html_only":
            machine_readable_success[str(release["release_name"])] = False
            continue
        release_name = str(release["release_name"])
        stem = release.get("machine_readable_stem")
        if not stem:
            machine_readable_success[release_name] = False
            continue
        release_success = False
        for ext, url in sorted((release.get("archive_machine_readable_urls") or {}).items()):
            file_name = Path(urlparse(url).path).name or f"{stem}.{ext}"
            destination = archive_data_dir / file_name
            attempt = {
                "source_type": f"archive_{ext}",
                "release_name": release_name,
                "year": release.get("year"),
                "url": url,
            }
            probe = probe_by_url.get(url) or _safe_head(url, session=session)
            attempt["head"] = probe
            attempt["download"] = _download_binary(url, destination, session=session)
            if attempt["download"]["status"] in {"downloaded", "reused"}:
                release_success = True
                downloaded_archive_urls.add(url)
            elif not destination.exists():
                status_code = probe.get("status_code")
                if status_code in {401, 403} or "403" in str(attempt["download"].get("error") or ""):
                    attempt["selenium_batch_deferred"] = True
                    deferred_archive_items.append(
                        {
                            "url": url,
                            "destination": destination,
                            "release_name": release_name,
                            "source_type": attempt["source_type"],
                            "year": release.get("year"),
                        }
                    )
                    deferred_archive_attempts.append(attempt)
            download_attempts.append(attempt)
        machine_readable_success[release_name] = release_success

    # Download archive links that were indexed but not matched to a specific release
    # (or not attempted yet). This captures revision files with naming variants.
    indexed_rows = inventory.get("archive_revision_index", [])
    orphan_rows = [row for row in indexed_rows if str(row.get("url")) not in downloaded_archive_urls]
    for row in orphan_rows:
        url = str(row["url"])
        file_name = str(row.get("file_name") or Path(urlparse(url).path).name)
        destination = archive_data_dir / file_name
        source_ext = str(row.get("file_ext") or Path(file_name).suffix.lstrip(".").lower())
        attempt = {
            "source_type": f"archive_index_{source_ext}",
            "release_name": row.get("archive_release_name"),
            "year": row.get("year"),
            "url": url,
            "archive_index_page": row.get("archive_list_page"),
            "machine_readable_stem": row.get("machine_readable_stem"),
            "edition_kind": row.get("edition_kind"),
            "revision_token": row.get("revision_token"),
        }
        probe = probe_by_url.get(url) or _safe_head(url, session=session)
        attempt["head"] = probe
        attempt["download"] = _download_binary(url, destination, session=session)
        if attempt["download"]["status"] in {"downloaded", "reused"}:
            downloaded_archive_urls.add(url)
        elif not destination.exists():
            status_code = probe.get("status_code")
            if status_code in {401, 403} or "403" in str(attempt["download"].get("error") or ""):
                attempt["selenium_batch_deferred"] = True
                deferred_archive_items.append(
                    {
                        "url": url,
                        "destination": destination,
                        "release_name": row.get("archive_release_name"),
                        "source_type": attempt["source_type"],
                        "year": row.get("year"),
                    }
                )
                deferred_archive_attempts.append(attempt)
        download_attempts.append(attempt)

    if deferred_archive_items:
        edge_results = _download_files_via_selenium_batch(deferred_archive_items, browser="edge", start_url=HTS_ARCHIVE_INDEX)
        edge_by_key = {(row.get("release_name"), row.get("source_type"), row.get("url")): row for row in edge_results}
        for attempt in deferred_archive_attempts:
            key = (attempt.get("release_name"), attempt.get("source_type"), attempt.get("url"))
            attempt["selenium_edge"] = edge_by_key.get(key, {"status": "failed", "error": "MissingBatchResult"})

        remaining = [
            item
            for item in deferred_archive_items
            if edge_by_key.get((item.get("release_name"), item.get("source_type"), item.get("url")), {}).get("status") != "downloaded"
        ]
        if remaining:
            chrome_results = _download_files_via_selenium_batch(remaining, browser="chrome", start_url=HTS_ARCHIVE_INDEX)
            chrome_by_key = {(row.get("release_name"), row.get("source_type"), row.get("url")): row for row in chrome_results}
            for attempt in deferred_archive_attempts:
                key = (attempt.get("release_name"), attempt.get("source_type"), attempt.get("url"))
                if attempt.get("selenium_edge", {}).get("status") != "downloaded":
                    attempt["selenium_chrome"] = chrome_by_key.get(key, {"status": "failed", "error": "MissingBatchResult"})

    # Recompute machine-readable success after selenium fallback.
    machine_readable_success = {}
    for release in inventory["releases"]:
        release_name = str(release["release_name"])
        release_attempts = [
            item
            for item in download_attempts
            if str(item.get("release_name", "")) == release_name and str(item.get("source_type", "")).startswith("archive_")
        ]
        success = any(
            item.get("download", {}).get("status") in {"downloaded", "reused"}
            or item.get("selenium_edge", {}).get("status") in {"downloaded", "reused"}
            or item.get("selenium_chrome", {}).get("status") in {"downloaded", "reused"}
            for item in release_attempts
        )
        machine_readable_success[release_name] = success

    for release in inventory["releases"]:
        release_name = str(release["release_name"])
        release_year = release.get("year")
        if machine_readable_success.get(release_name):
            continue

        # UI export fallback (machine-readable) before PDF fallback.
        release_date = release.get("release_date")
        ui_destination = archive_data_dir / f"{release_name}_ui_export.csv"
        ui_attempt = {
            "source_type": "archive_release_ui_export_csv",
            "release_name": release_name,
            "year": release_year,
            "release_date": release_date,
            "url": _build_query_url(DOWNLOAD_RELEASE_URL, {"release": release_name}),
        }
        ui_attempt["selenium_edge"] = _download_release_export_csv_via_selenium(
            release_name=release_name,
            release_date=release_date,
            destination=ui_destination,
            browser="edge",
        )
        if ui_attempt["selenium_edge"].get("status") not in {"downloaded", "reused"}:
            ui_attempt["selenium_chrome"] = _download_release_export_csv_via_selenium(
                release_name=release_name,
                release_date=release_date,
                destination=ui_destination,
                browser="chrome",
            )
        download_attempts.append(ui_attempt)
        if ui_attempt["selenium_edge"].get("status") in {"downloaded", "reused"} or ui_attempt.get("selenium_chrome", {}).get("status") in {"downloaded", "reused"}:
            machine_readable_success[release_name] = True
            if release_year is not None:
                machine_readable_year_success[int(release_year)] = True
            continue

        pdf_url = str(release["archive_pdf_url"])
        destination = archive_pdf_dir / f"{release_name}.pdf"
        attempt = {
            "source_type": "archive_pdf_fallback",
            "release_name": release_name,
            "year": release.get("year"),
            "url": pdf_url,
        }
        attempt["head"] = probe_by_url.get(pdf_url) or _safe_head(pdf_url, session=session)
        attempt["download"] = _download_binary(pdf_url, destination, session=session)
        download_attempts.append(attempt)

    current_exports = _download_current_release_exports(config, session=session)

    result = {
        "inventory_path": str(config.verification_dir / "policy_source_inventory.json"),
        "download_attempt_count": len(download_attempts),
        "download_attempts": download_attempts,
        "current_release_exports_path": str(config.verification_dir / "policy_current_release_exports.json"),
        "current_release_export_count": len(current_exports["downloads"]),
        "local_files": _inspect_local_files(config),
    }
    write_metadata_json(config.verification_dir / "policy_source_downloads.json", result)
    return result


def run_policy_update_download(config: PipelineConfig) -> dict[str, Any]:
    """Compatibility wrapper for an explicit policy-update download step."""
    result = run_policy_source_download(config)
    result["invoked_as"] = "download_policy_updates"
    return result
