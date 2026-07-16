from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from scr.passthru_data.bridge_runner import (
    SOURCE_MODES,
    SPECS,
    OUTCOMES,
    _checkpoint_valid,
    _select_fit,
    _specification_hash,
    estimator_fingerprint,
    expected_fit_ids,
)


def test_bridge_grid_has_two_modes_two_specs_four_outcomes():
    assert len(expected_fit_ids()) == 16
    assert len(SOURCE_MODES) == 2
    assert len(SPECS) == 2
    assert len(OUTCOMES) == 4


def test_bridge_specification_and_estimator_fingerprints_are_stable():
    assert _specification_hash("event", "val") == _specification_hash("event", "val")
    assert _specification_hash("event", "val") != _specification_hash("dynamic", "val")
    assert len(estimator_fingerprint()) == 64


def test_select_fit_does_not_mix_p_with_pduty():
    coefficients = pd.DataFrame({"fit_id": [
        "package_common_sample_anchor|event|p",
        "package_common_sample_anchor|event|pduty",
    ], "event_time": [-6, -6], "estimate": [1.0, 2.0]})
    selected = _select_fit(coefficients, "package_common_sample_anchor", "event", "p")
    assert selected["fit_id"].tolist() == ["package_common_sample_anchor|event|p"]


def test_checkpoint_validation_rejects_missing_artifacts(tmp_path: Path):
    assert not _checkpoint_valid(
        tmp_path,
        "package_common_sample_anchor|event|val",
        "source",
        "sample",
        "treatment",
        "spec",
        "code",
    )


def _tiny_result():
    return pd.DataFrame(
        {
            "event_time": list(range(-6, 7)),
            "estimate": [0.0] * 13,
            "std_error": [0.1] * 13,
            "conf_low": [-0.2] * 13,
            "conf_high": [0.2] * 13,
            "nobs": [10] * 13,
        }
    )


def test_bridge_success_clears_current_fit_and_writes_partial_checkpoint(tmp_path, monkeypatch):
    import scr.passthru_data.bridge_runner as runner
    from scr.passthru_data.io_utils import write_parquet

    source = tmp_path / "source.parquet"
    write_parquet(pd.DataFrame({"id": [1], "cty_code": [1], "hs10": ["0101010101"], "year": [2018], "month": [1]}), source)
    monkeypatch.setattr(runner, "_source_paths", lambda config: {"package_common_sample_anchor": source, "raw_outcomes_package_policy": source})
    frame = pd.DataFrame({"id": [1], "cty_code": [1], "hs10": ["0101010101"], "year": [2018], "month": [1]})
    monkeypatch.setattr(runner, "_load_mode_frame", lambda path, spec: (frame, "source", "sample", "treatment"))
    monkeypatch.setattr(runner, "_run_event_study_one", lambda *args, **kwargs: SimpleNamespace(frame=_tiny_result()))

    config = SimpleNamespace(verification_dir=tmp_path, repo_root=tmp_path)
    result = runner.run_bridge(config, source_modes=("package_common_sample_anchor",), specs=("event",), outcomes=("val",), resume=False)
    root = runner.bridge_root(config)
    assert result["status"] == "partial"
    assert (root / "package_common_sample_anchor" / "event" / "val" / "manifest.json").exists()
    assert not (root / "current_fit.json").exists()


def test_bridge_failure_retains_current_fit_and_writes_fit_failure(tmp_path, monkeypatch):
    import scr.passthru_data.bridge_runner as runner
    from scr.passthru_data.io_utils import write_parquet

    source = tmp_path / "source.parquet"
    write_parquet(pd.DataFrame({"id": [1], "cty_code": [1], "hs10": ["0101010101"], "year": [2018], "month": [1]}), source)
    monkeypatch.setattr(runner, "_source_paths", lambda config: {"package_common_sample_anchor": source, "raw_outcomes_package_policy": source})
    frame = pd.DataFrame({"id": [1], "cty_code": [1], "hs10": ["0101010101"], "year": [2018], "month": [1]})
    monkeypatch.setattr(runner, "_load_mode_frame", lambda path, spec: (frame, "source", "sample", "treatment"))
    monkeypatch.setattr(runner, "_run_event_study_one", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("synthetic failure")))

    config = SimpleNamespace(verification_dir=tmp_path, repo_root=tmp_path)
    try:
        runner.run_bridge(config, source_modes=("package_common_sample_anchor",), specs=("event",), outcomes=("val",), resume=False)
    except ValueError:
        pass
    root = runner.bridge_root(config)
    assert (root / "current_fit.json").exists()
    assert (root / "failures" / "package_common_sample_anchor__event__val.json").exists()
