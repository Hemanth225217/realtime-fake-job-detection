import json
import numpy as np
from typing import Dict, Any, List
from src.ml.feature_extractor import extract_forensic_signals, clean_text

class DualEngineExplainer:
    """
    Combines Model-Intrinsic Weight Attribution (glass-box log-odds)
    with Multi-Vector Heuristic Forensic Auditing.
    Solves the fundamental explainability gap of previous research.
    """
    def __init__(self, model, feature_names: List[str], vectorizer):
        self.model = model
        self.feature_names = np.array(feature_names)
        self.vectorizer = vectorizer

    def explain(self, job: Dict[str, Any], fraud_prob: float) -> Dict[str, Any]:
        """
        Generates both model-level attribution and forensic risk audit.
        """
        forensic_signals = extract_forensic_signals(job)
        token_attributions = self._compute_token_attributions(job)
        risk_level = self._compute_risk_level(fraud_prob, forensic_signals["forensic_risk_score"])
        narrative_bullets, recommendation = self._generate_narrative(
            fraud_prob, risk_level, forensic_signals, token_attributions
        )

        return {
            "risk_level": risk_level,
            "fraud_probability": round(float(fraud_prob), 4),
            "forensic_risk_score": forensic_signals["forensic_risk_score"],
            "flagged_keywords": forensic_signals["flagged_keywords"],
            "token_attributions": token_attributions,
            "forensic_breakdown": {
                "identity_and_brand": {
                    "has_company_logo": bool(forensic_signals["has_company_logo"]),
                    "has_company_profile": forensic_signals["missing_company_profile"] == 0,
                    "profile_length": forensic_signals["profile_length"]
                },
                "financial_risks": {
                    "payment_scam_flags": forensic_signals["payment_scam_flags"],
                    "free_email_domain": bool(forensic_signals["free_email_domain"])
                },
                "communication_redirection": {
                    "off_platform_flags": forensic_signals["off_platform_flags"],
                    "telecommuting": bool(forensic_signals["telecommuting"])
                },
                "specification_ambiguity": {
                    "has_screening_questions": bool(forensic_signals["has_questions"]),
                    "word_count": forensic_signals["desc_word_count"],
                    "has_salary_range": bool(forensic_signals["has_salary_range"])
                }
            },
            "explanation_bullets": narrative_bullets,
            "recommendation": recommendation
        }

    def _compute_risk_level(self, fraud_prob: float, forensic_score: int) -> str:
        if fraud_prob >= 0.70 or forensic_score >= 60:
            return "CRITICAL"
        elif fraud_prob >= 0.40 or forensic_score >= 35:
            return "HIGH"
        elif fraud_prob >= 0.20 or forensic_score >= 20:
            return "MEDIUM"
        else:
            return "LOW"

    def _compute_token_attributions(self, job: Dict[str, Any], top_k: int = 8) -> Dict[str, List[Dict[str, Any]]]:
        """
        Computes exact token-level contribution weights derived from model parameters.
        """
        title = clean_text(job.get("title", ""))
        profile = clean_text(job.get("company_profile", ""))
        desc = clean_text(job.get("description", ""))
        reqs = clean_text(job.get("requirements", ""))
        benefits = clean_text(job.get("benefits", ""))
        combined_text = f"{title} {profile} {desc} {reqs} {benefits}".strip()

        # Extract words present in text
        words = list(set(combined_text.split()))
        if not words:
            return {"fraud_drivers": [], "legitimacy_drivers": []}

        vocab = self.vectorizer.vocabulary_
        present_indices = []
        token_list = []
        for w in words:
            if w in vocab:
                idx = vocab[w]
                present_indices.append(idx)
                token_list.append(w)

        if not present_indices:
            return {"fraud_drivers": [], "legitimacy_drivers": []}

        # Check model weights
        weights = None
        if hasattr(self.model, "coef_"):
            weights = self.model.coef_[0]
        elif hasattr(self.model, "feature_importances_"):
            weights = self.model.feature_importances_

        if weights is None:
            return {"fraud_drivers": [], "legitimacy_drivers": []}

        token_weights = []
        for word, idx in zip(token_list, present_indices):
            if idx < len(weights):
                w_val = float(weights[idx])
                token_weights.append({"token": word, "weight": round(w_val, 4)})

        token_weights.sort(key=lambda x: x["weight"], reverse=True)
        fraud_drivers = [t for t in token_weights if t["weight"] > 0][:top_k]
        legitimacy_drivers = [t for t in sorted(token_weights, key=lambda x: x["weight"]) if t["weight"] < 0][:top_k]

        return {
            "fraud_drivers": fraud_drivers,
            "legitimacy_drivers": legitimacy_drivers
        }

    def _generate_narrative(self, fraud_prob: float, risk_level: str,
                            signals: Dict[str, Any], token_attributions: Dict[str, Any]) -> Tuple[List[str], str]:
        bullets = []

        if signals["payment_scam_flags"] > 0:
            bullets.append(f"Financial Hazard: Detected {signals['payment_scam_flags']} phrase(s) referencing wire transfers, checks, or upfront payment fees.")
        if signals["off_platform_flags"] > 0:
            bullets.append(f"Off-Platform Evasion: Found {signals['off_platform_flags']} invitation(s) to unverified channels (e.g., Telegram, WhatsApp, or instant messengers).")
        if signals["free_email_domain"]:
            bullets.append("Identity Anomaly: Recruiter provided a public free webmail address (e.g. Gmail/Yahoo) instead of an authenticated enterprise domain.")
        if signals["missing_company_profile"]:
            bullets.append("Missing Enterprise Credibility: The company profile is completely empty.")
        if not signals["has_company_logo"]:
            bullets.append("Absence of Corporate Branding: No verifiable company logo provided.")
        if signals["desc_word_count"] < 35:
            bullets.append(f"Extreme Brevity: Job description is abnormally brief ({signals['desc_word_count']} words), typical of spam/phishing blasts.")
        if signals["urgency_flags"] > 0:
            bullets.append("Psychological Manipulation: High density of artificial urgency and hyperbolic earning guarantees.")

        top_fraud_tokens = [t["token"] for t in token_attributions.get("fraud_drivers", [])[:4]]
        if top_fraud_tokens:
            bullets.append(f"Model Statistical Fraud Drivers: Distinctive lexical correlations with historic scams found in terms: {', '.join(top_fraud_tokens)}.")

        # If clean
        if not bullets and risk_level == "LOW":
            bullets.append("Comprehensive corporate profile and verified structural job requirements detected.")
            bullets.append("Standard professional recruitment terminology with no financial red flags or evasive contact methods.")

        # Recommendation
        if risk_level in ["CRITICAL", "HIGH"]:
            recommendation = "ALERT: Quarantine post. High probability of recruitment fraud. Do not submit sensitive identity documents or bank account details."
        elif risk_level == "MEDIUM":
            recommendation = "CAUTION: Elevated risk factors detected. Verify employer registration and corporate website before applying."
        else:
            recommendation = "VERIFIED: Post exhibits authentic hiring parameters and compliant enterprise recruitment characteristics."

        return bullets, recommendation
