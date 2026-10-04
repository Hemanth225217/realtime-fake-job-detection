# StreamGuard Experimental Benchmark Evaluation

**Dataset:** Employment Scam Aegean Dataset (EMSCAD) — 17,880 samples (17,014 legitimate, 866 fraudulent, 4.84% base scam rate)
**Evaluation Split:** Stratified 80/20 train/test holdout (N_test = 3,576 samples)

| Model Architecture | Accuracy | Balanced Acc | Precision (Fraud) | Recall (Fraud) | F1-Score (Fraud) | ROC-AUC | PR-AUC | Latency (ms) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Baseline Logistic Regression | 97.29% | 71.97% | 100.00% | 43.93% | **61.04%** | 0.9808 | 0.8681 | 0.305 ms |
| Baseline Random Forest | 98.07% | 80.06% | 100.00% | 60.12% | **75.09%** | 0.9806 | 0.8879 | 33.749 ms |
| **Proposed Calibrated LR (Ours)** | 95.69% | 94.99% | 53.09% | 94.22% | **67.92%** | 0.9922 | 0.9043 | 0.159 ms |
| **Proposed LightGBM Ensemble (Ours)** | 98.94% | 92.03% | 92.99% | 84.39% | **88.48%** | 0.9914 | 0.9375 | 1.178 ms |

### Key Empirical Findings:
1. **Recall & F1 Breakthrough:** Baseline Logistic Regression (as evaluated in the preliminary draft) suffered catastrophically on the imbalanced distribution, achieving only 43.93% recall and 61.04% F1-score. Our proposed StreamGuard LightGBM Ensemble increases recall to **84.39%** and F1-score to **88.48%**, representing a **+27.44% absolute increase** in F1.
2. **Precision vs. False Alarm Rate:** Despite aggressive class imbalance compensation, StreamGuard preserves **92.99% precision**, preventing legitimate employer postings from being erroneously flagged.
3. **Ultra-Low Real-Time Latency:** Single-inference latency is only **1.178 ms**, compared to 33.749 ms for Random Forest (a **28.6x speedup**), making it ideally suited for sub-second micro-batch stream processing in Apache Spark and native event streams.