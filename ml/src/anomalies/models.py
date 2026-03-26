from sklearn.ensemble import IsolationForest


def get_anomaly_models(random_state: int = 42) -> dict:
    return {
        "z_score": None,  # baseline statistique
        "isolation_forest": IsolationForest(
            n_estimators=200,
            contamination=0.02,  # 2% anomalies attendues
            random_state=random_state,
        ),
    }