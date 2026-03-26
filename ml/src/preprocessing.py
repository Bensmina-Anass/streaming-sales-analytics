from sklearn.preprocessing import StandardScaler


def fit_standard_scaler(X):
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    return scaler, X_scaled


def transform_with_scaler(scaler, X):
    return scaler.transform(X)