from pathlib import Path


ML_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ML_DIR.parent

DATA_DIR = PROJECT_ROOT / "data" / "oltp"

MODELS_DIR = ML_DIR / "models"
OUTPUTS_DIR = ML_DIR / "outputs"

FORECASTING_MODELS_DIR = MODELS_DIR / "forecasting"
SEGMENTATION_MODELS_DIR = MODELS_DIR / "segmentation"

METRICS_DIR = OUTPUTS_DIR / "metrics"
PREDICTIONS_DIR = OUTPUTS_DIR / "predictions"
REPORTS_DIR = OUTPUTS_DIR / "reports"


def ensure_project_dirs() -> None:
    directories = [
        MODELS_DIR,
        OUTPUTS_DIR,
        FORECASTING_MODELS_DIR,
        SEGMENTATION_MODELS_DIR,
        METRICS_DIR,
        PREDICTIONS_DIR,
        REPORTS_DIR,
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)