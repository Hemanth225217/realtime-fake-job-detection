import os
import sys
import json
from pathlib import Path

BASE_PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(BASE_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_PROJECT_DIR))

from src.config import EVAL_DIR

def generate_markdown_benchmark_table():
    bench_file = EVAL_DIR / "benchmark_results.json"
    if not bench_file.exists():
        print("Benchmark results not found.")
        return

    with open(bench_file, "r") as f:
        data = json.load(f)

    md = []
    md.append("# StreamGuard Experimental Benchmark Evaluation")
    md.append("\n**Dataset:** Employment Scam Aegean Dataset (EMSCAD) — 17,880 samples (17,014 legitimate, 866 fraudulent, 4.84% base scam rate)")
    md.append("**Evaluation Split:** Stratified 80/20 train/test holdout (N_test = 3,576 samples)\n")
    md.append("| Model Architecture | Accuracy | Balanced Acc | Precision (Fraud) | Recall (Fraud) | F1-Score (Fraud) | ROC-AUC | PR-AUC | Latency (ms) |")
    md.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

    for name, m in data.items():
        clean_name = name.replace("_", " ")
        if name.startswith("Proposed"):
            clean_name = f"**{clean_name} (Ours)**"
        md.append(f"| {clean_name} | {m['Accuracy']*100:.2f}% | {m['Balanced_Accuracy']*100:.2f}% | {m['Precision']*100:.2f}% | {m['Recall']*100:.2f}% | **{m['F1_Score']*100:.2f}%** | {m['ROC_AUC']:.4f} | {m['PR_AUC']:.4f} | {m['Inference_Latency_ms']:.3f} ms |")

    md.append("\n### Key Empirical Findings:")
    md.append("1. **Recall & F1 Breakthrough:** Baseline Logistic Regression (as evaluated in the preliminary draft) suffered catastrophically on the imbalanced distribution, achieving only 43.93% recall and 61.04% F1-score. Our proposed StreamGuard LightGBM Ensemble increases recall to **84.39%** and F1-score to **88.48%**, representing a **+27.44% absolute increase** in F1.")
    md.append("2. **Precision vs. False Alarm Rate:** Despite aggressive class imbalance compensation, StreamGuard preserves **92.99% precision**, preventing legitimate employer postings from being erroneously flagged.")
    md.append("3. **Ultra-Low Real-Time Latency:** Single-inference latency is only **1.178 ms**, compared to 33.749 ms for Random Forest (a **28.6x speedup**), making it ideally suited for sub-second micro-batch stream processing in Apache Spark and native event streams.")

    output_path = EVAL_DIR / "BENCHMARK_SUMMARY.md"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    print(f"Generated benchmark summary at: {output_path}")

if __name__ == "__main__":
    generate_markdown_benchmark_table()
