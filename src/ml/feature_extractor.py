import re
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple
from sklearn.feature_extraction.text import TfidfVectorizer

# Known recruitment scam patterns derived from FBI IC3, FTC, and cybersecurity advisories
SCAM_INDICATOR_PATTERNS = {
    "payment_scam": [
        r"\b(?:wire\s+transfer|cashier'?s?\s+check|money\s+order|western\s+union|moneygram)\b",
        r"\b(?:advance\s+fee|processing\s+fee|registration\s+fee|upfront\s+fee|starter\s+kit)\b",
        r"\b(?:pay\s+for\s+(?:equipment|training|background\s+check|software|laptop))\b",
        r"\b(?:bitcoin|cryptocurrency|crypto|usdt|binance|wallet\s+address)\b"
    ],
    "off_platform_contact": [
        r"\b(?:telegram|whatsapp|signal|google\s+hangouts|skype|viber)\b",
        r"\b(?:text\s+me\s+at|contact\s+hr\s+via|dm\s+on|add\s+on\s+telegram)\b",
        r"@[a-zA-Z0-9_]{4,}",  # Handle mentions (e.g. @hr_recruiter)
        r"\b(?:send\s+resume\s+to\s+[a-zA-Z0-9._%+-]+@(?:gmail|yahoo|hotmail|outlook|protonmail)\.com)\b"
    ],
    "urgency_and_hyperbole": [
        r"\b(?:immediate\s+start|urgent\s+hiring|start\s+today|no\s+experience\s+needed|no\s+interview)\b",
        r"\b(?:guaranteed\s+income|earn\s+\$?\d+[\d,]*\s*(?:weekly|daily|per\s+day))\b",
        r"\b(?:unlimited\s+earning\s+potential|be\s+your\s+own\s+boss|get\s+rich)\b"
    ],
    "vague_data_entry": [
        r"\b(?:data\s+entry\s+clerk|package\s+handler|mystery\s+shopper|reshipping\s+agent)\b",
        r"\b(?:work\s+from\s+home\s+(?:anywhere|simple\s+tasks|part\s+time))\b"
    ]
}

FREE_EMAIL_DOMAINS = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "protonmail.com"}


def clean_text(text: Any) -> str:
    """Cleans and standardizes raw text."""
    if not isinstance(text, str) or pd.isna(text):
        return ""
    text = re.sub(r"<[^>]+>", " ", text) # remove HTML
    text = re.sub(r"http\S+|www\.\S+", " url_link ", text) # normalize URLs
    text = re.sub(r"[^\w\s\$\%]", " ", text) # remove punctuation except $ and %
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def extract_forensic_signals(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts deterministic forensic risk indicators from text and metadata.
    """
    desc = clean_text(job.get("description", ""))
    profile = clean_text(job.get("company_profile", ""))
    reqs = clean_text(job.get("requirements", ""))
    benefits = clean_text(job.get("benefits", ""))
    title = clean_text(job.get("title", ""))
    combined = f"{title} {profile} {desc} {reqs} {benefits}"

    signals = {
        "payment_scam_flags": 0,
        "off_platform_flags": 0,
        "urgency_flags": 0,
        "vague_task_flags": 0,
        "free_email_domain": 0,
        "missing_company_profile": 1 if len(profile.strip()) == 0 else 0,
        "has_company_logo": int(job.get("has_company_logo", 0) or 0),
        "has_questions": int(job.get("has_questions", 0) or 0),
        "telecommuting": int(job.get("telecommuting", 0) or 0),
        "has_salary_range": 1 if str(job.get("salary_range", "")).strip() not in ["", "nan", "None"] else 0,
        "desc_length": len(desc),
        "desc_word_count": len(desc.split()),
        "profile_length": len(profile),
        "reqs_length": len(reqs),
        "title_word_count": len(title.split()),
        "flagged_keywords": []
    }

    # Match scam regex categories
    for cat, patterns in SCAM_INDICATOR_PATTERNS.items():
        count = 0
        for pat in patterns:
            matches = re.findall(pat, combined, flags=re.IGNORECASE)
            if matches:
                count += len(matches)
                for m in matches:
                    if m not in signals["flagged_keywords"]:
                        signals["flagged_keywords"].append(m)
        if cat == "payment_scam": signals["payment_scam_flags"] = count
        elif cat == "off_platform_contact": signals["off_platform_flags"] = count
        elif cat == "urgency_and_hyperbole": signals["urgency_flags"] = count
        elif cat == "vague_data_entry": signals["vague_task_flags"] = count

    # Detect free email in description or contact
    emails = re.findall(r"[\w\.-]+@([\w\.-]+)", combined)
    for domain in emails:
        if domain.lower() in FREE_EMAIL_DOMAINS:
            signals["free_email_domain"] = 1
            signals["flagged_keywords"].append(f"free_email:{domain}")
            break

    # Calculate overall forensic risk score (0 to 100)
    risk_score = 0
    risk_score += signals["payment_scam_flags"] * 30
    risk_score += signals["off_platform_flags"] * 25
    risk_score += signals["free_email_domain"] * 25
    risk_score += signals["urgency_flags"] * 10
    risk_score += signals["vague_task_flags"] * 15
    if signals["missing_company_profile"]: risk_score += 10
    if not signals["has_company_logo"]: risk_score += 10
    if not signals["has_questions"]: risk_score += 5
    if signals["desc_word_count"] < 30: risk_score += 15

    signals["forensic_risk_score"] = min(100, risk_score)
    return signals


class MultiModalFeatureExtractor:
    """
    Combines Textual TF-IDF Vectorization with Structured Metadata
    and Domain-Engineered Forensic Scam Indicators.
    """
    def __init__(self, max_tfidf_features: int = 5000):
        self.max_tfidf_features = max_tfidf_features
        self.vectorizer = TfidfVectorizer(
            max_features=max_tfidf_features,
            ngram_range=(1, 2),
            sublinear_tf=True,
            stop_words="english",
            min_df=3
        )
        self.feature_names_ = []

    def _prepare_text_and_meta(self, df: pd.DataFrame) -> Tuple[List[str], np.ndarray]:
        texts = []
        meta_features = []

        for _, row in df.iterrows():
            job_dict = row.to_dict()
            # Clean combined text
            title = clean_text(job_dict.get("title", ""))
            profile = clean_text(job_dict.get("company_profile", ""))
            desc = clean_text(job_dict.get("description", ""))
            reqs = clean_text(job_dict.get("requirements", ""))
            benefits = clean_text(job_dict.get("benefits", ""))
            combined_text = f"{title} {profile} {desc} {reqs} {benefits}".strip()
            texts.append(combined_text)

            # Forensic & structural features
            signals = extract_forensic_signals(job_dict)
            meta_vec = [
                signals["payment_scam_flags"],
                signals["off_platform_flags"],
                signals["urgency_flags"],
                signals["vague_task_flags"],
                signals["free_email_domain"],
                signals["missing_company_profile"],
                signals["has_company_logo"],
                signals["has_questions"],
                signals["telecommuting"],
                signals["has_salary_range"],
                np.log1p(signals["desc_length"]),
                np.log1p(signals["desc_word_count"]),
                np.log1p(signals["profile_length"]),
                np.log1p(signals["reqs_length"]),
                signals["title_word_count"],
                signals["forensic_risk_score"] / 100.0
            ]
            meta_features.append(meta_vec)

        return texts, np.array(meta_features, dtype=np.float32)

    def fit_transform(self, df: pd.DataFrame):
        texts, meta = self._prepare_text_and_meta(df)
        tfidf_sparse = self.vectorizer.fit_transform(texts)
        from scipy.sparse import hstack
        X = hstack([tfidf_sparse, meta]).tocsr()
        self.feature_names_ = list(self.vectorizer.get_feature_names_out()) + [
            "meta_payment_scam_flags", "meta_off_platform_flags", "meta_urgency_flags",
            "meta_vague_task_flags", "meta_free_email", "meta_missing_profile",
            "meta_has_logo", "meta_has_questions", "meta_telecommuting", "meta_has_salary",
            "meta_log_desc_len", "meta_log_word_cnt", "meta_log_prof_len",
            "meta_log_reqs_len", "meta_title_words", "meta_forensic_risk_norm"
        ]
        return X

    def transform(self, df: pd.DataFrame):
        texts, meta = self._prepare_text_and_meta(df)
        tfidf_sparse = self.vectorizer.transform(texts)
        from scipy.sparse import hstack
        X = hstack([tfidf_sparse, meta]).tocsr()
        return X
