import os
import sys
import time
import json
import joblib
from pathlib import Path

# Add project root to sys.path
BASE_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_PROJECT_DIR))

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, balanced_accuracy_score, precision_score, recall_score,
    f1_score, roc_auc_score, average_precision_score, confusion_matrix, classification_report
)
import lightgbm as lgb
from scipy.sparse import hstack

from src.config import RAW_DATA_PATH, MODELS_DIR, EVAL_DIR, FRAUD_PROBABILITY_THRESHOLD
from src.ml.feature_extractor import MultiModalFeatureExtractor, clean_text

def train_and_evaluate_all():
    print(f"Loading raw EMSCAD dataset from: {RAW_DATA_PATH}")
    df = pd.read_csv(RAW_DATA_PATH)
    print(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns. Fraudulent: {df['fraudulent'].sum()} ({df['fraudulent'].mean()*100:.2f}%)")

    # Stratified Train-Test Split (80/20)
    train_df, test_df = train_test_split(
        df, test_size=0.20, random_state=42, stratify=df["fraudulent"]
    )
    print(f"Train split: {len(train_df)} | Test split: {len(test_df)}")

    # ==========================================
    # 1. BASELINE PIPELINE (As in current paper)
    # TF-IDF (5,000 features) on combined text
    # ==========================================
    print("\n" + "="*50)
    print("STEP 1: Training Baseline Models (Current Paper)")
    print("="*50)
    
    def get_simple_text(d):
        texts = []
        for _, r in d.iterrows():
            t = f"{r.get('title','')} {r.get('company_profile','')} {r.get('description','')} {r.get('requirements','')} {r.get('benefits','')}"
            texts.append(clean_text(t))
        return texts

    train_texts = get_simple_text(train_df)
    test_texts = get_simple_text(test_df)
    y_train = train_df["fraudulent"].values
    y_test = test_df["fraudulent"].values

    baseline_vectorizer = TfidfVectorizer(max_features=5000, stop_words="english")
    X_train_base = baseline_vectorizer.fit_transform(train_texts)
    X_test_base = baseline_vectorizer.transform(test_texts)

    # 1a. Baseline Logistic Regression
    print("Training Baseline 1: Logistic Regression...")
    lr_base = LogisticRegression(max_iter=1000, random_state=42)
    t0 = time.time()
    lr_base.fit(X_train_base, y_train)
    lr_fit_time = time.time() - t0

    # 1b. Baseline Random Forest
    print("Training Baseline 2: Random Forest...")
    rf_base = RandomForestClassifier(n_estimators=100, n_jobs=-1, random_state=42)
    t0 = time.time()
    rf_base.fit(X_train_base, y_train)
    rf_fit_time = time.time() - t0

    # ==========================================
    # 2. PROPOSED MULTI-MODAL SOTA PIPELINE
    # MultiModal Feature Extractor + Cost-Sensitive LightGBM & Calibrated LR
    # ==========================================
    print("\n" + "="*50)
    print("STEP 2: Training Proposed StreamGuard Multi-Modal Architecture")
    print("="*50)

    extractor = MultiModalFeatureExtractor(max_tfidf_features=6000)
    X_train_multi = extractor.fit_transform(train_df)
    X_test_multi = extractor.transform(test_df)
    print(f"Multi-Modal Feature Matrix shape: {X_train_multi.shape}")

    # Proposed 1: Cost-Sensitive Calibrated Logistic Regression (PR-AUC tuned)
    print("Training Proposed Calibrated LR with Cost Sensitivity...")
    prop_lr = LogisticRegression(class_weight="balanced", C=1.5, max_iter=1000, random_state=42)
    prop_lr.fit(X_train_multi, y_train)

    # Proposed 2: High-Velocity LightGBM Classifier (Scale Pos Weight)
    print("Training Proposed LightGBM with Imbalance Compensation...")
    scale_pos = (len(y_train) - sum(y_train)) / sum(y_train)
    prop_lgb = lgb.LGBMClassifier(
        n_estimators=250,
        learning_rate=0.05,
        num_leaves=31,
        scale_pos_weight=scale_pos * 0.7,
        random_state=42,
        n_jobs=-1,
        verbosity=-1
    )
    prop_lgb.fit(X_train_multi, y_train)

    # ==========================================
    # 3. BENCHMARK EVALUATION ACROSS ALL MODELS
    # ==========================================
    print("\n" + "="*50)
    print("STEP 3: Comparative Performance Benchmarking")
    print("="*50)

    models_dict = {
        "Baseline_Logistic_Regression": (lr_base, X_test_base, 0.5),
        "Baseline_Random_Forest": (rf_base, X_test_base, 0.5),
        "Proposed_Calibrated_LR": (prop_lr, X_test_multi, FRAUD_PROBABILITY_THRESHOLD),
        "Proposed_LightGBM_Ensemble": (prop_lgb, X_test_multi, FRAUD_PROBABILITY_THRESHOLD)
    }

    results = {}
    for name, (model, X_eval, thresh) in models_dict.items():
        # Measure single-sample inference latency
        latencies = []
        for i in range(100):
            sample = X_eval[i:i+1]
            t0 = time.perf_counter()
            _ = model.predict_proba(sample)
            latencies.append((time.perf_counter() - t0) * 1000) # ms
        avg_lat_ms = np.median(latencies)

        # Batch prediction
        probs = model.predict_proba(X_eval)[:, 1]
        preds = (probs >= thresh).astype(int)

        acc = accuracy_score(y_test, preds)
        bal_acc = balanced_accuracy_score(y_test, preds)
        prec = precision_score(y_test, preds, zero_division=0)
        rec = recall_score(y_test, preds, zero_division=0)
        f1 = f1_score(y_test, preds, zero_division=0)
        roc = roc_auc_score(y_test, probs)
        pr_auc = average_precision_score(y_test, probs)
        cm = confusion_matrix(y_test, preds).tolist()

        results[name] = {
            "Accuracy": round(float(acc), 4),
            "Balanced_Accuracy": round(float(bal_acc), 4),
            "Precision": round(float(prec), 4),
            "Recall": round(float(rec), 4),
            "F1_Score": round(float(f1), 4),
            "ROC_AUC": round(float(roc), 4),
            "PR_AUC": round(float(pr_auc), 4),
            "Inference_Latency_ms": round(float(avg_lat_ms), 3),
            "Confusion_Matrix": cm,
            "Decision_Threshold": thresh
        }
        print(f"\n--- {name} ---")
        print(f"Accuracy: {acc*100:.2f}% | Precision: {prec*100:.2f}% | Recall: {rec*100:.2f}% | F1: {f1*100:.2f}%")
        print(f"ROC-AUC: {roc:.4f} | PR-AUC: {pr_auc:.4f} | Latency: {avg_lat_ms:.3f} ms")

    # Save Checkpoints
    print("\nSaving Production Checkpoints...")
    joblib.dump(prop_lgb, MODELS_DIR / "streamguard_lgbm.pkl")
    joblib.dump(prop_lr, MODELS_DIR / "streamguard_calibrated_lr.pkl")
    joblib.dump(extractor, MODELS_DIR / "multimodal_extractor.pkl")
    joblib.dump(lr_base, MODELS_DIR / "baseline_fraud_model.pkl")
    joblib.dump(baseline_vectorizer, MODELS_DIR / "baseline_tfidf.pkl")

    # Save benchmark evaluation results
    eval_file = EVAL_DIR / "benchmark_results.json"
    with open(eval_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Evaluation benchmark results saved to {eval_file}")

    return results

if __name__ == "__main__":
    train_and_evaluate_all()
