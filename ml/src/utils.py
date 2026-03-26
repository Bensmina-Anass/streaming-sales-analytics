from pathlib import Path
import joblib
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
MODELS_DIR = BASE_DIR / "models"
OUTPUTS_DIR = BASE_DIR / "outputs"


def ensure_directories() -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


def save_dataframe(df: pd.DataFrame, filename: str) -> Path:
    ensure_directories()
    output_path = OUTPUTS_DIR / filename
    df.to_csv(output_path, index=False)
    return output_path


def save_model(model, filename: str) -> Path:
    ensure_directories()
    model_path = MODELS_DIR / filename
    joblib.dump(model, model_path)
    return model_path