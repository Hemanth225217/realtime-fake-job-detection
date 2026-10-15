"""The README benchmark table must match the recorded results in benchmark_results.json."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = json.loads((ROOT / "models" / "evaluations" / "benchmark_results.json").read_text(encoding="utf-8"))
README = (ROOT / "README.md").read_text(encoding="utf-8")

ROWS = {
    "Baseline Logistic Regression": "Baseline_Logistic_Regression",
    "Baseline Random Forest": "Baseline_Random_Forest",
    "Proposed Calibrated LR (Ours)": "Proposed_Calibrated_LR",
    "Proposed StreamGuard LightGBM (Ours)": "Proposed_LightGBM_Ensemble",
}
COLUMNS = ["Accuracy", "Balanced_Accuracy", "Precision", "Recall", "F1_Score", "ROC_AUC", "PR_AUC", "Inference_Latency_ms"]


def _numbers(cell):
    return float(re.sub(r"[*%ms ]", "", cell))


def test_readme_table_matches_benchmark_json():
    for label, key in ROWS.items():
        line = next(l for l in README.splitlines() if l.startswith(f"| **{label}**"))
        cells = [c.strip() for c in line.strip().strip("|").split("|")][1:]
        assert len(cells) == len(COLUMNS), line
        for column, cell in zip(COLUMNS, cells):
            expected = RESULTS[key][column]
            shown = _numbers(cell)
            if cell.replace("*", "").strip().endswith("%"):
                shown /= 100
            assert abs(shown - expected) < 6e-5, f"{label} {column}: README {cell} vs JSON {expected}"
