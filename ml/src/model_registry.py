import json
from datetime import datetime
from pathlib import Path
import joblib


def save_artifact(obj, artifact_path: Path) -> None:
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(obj, artifact_path)


def load_artifact(artifact_path: Path):
    return joblib.load(artifact_path)


def save_metadata(metadata: dict, metadata_path: Path) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    serializable_metadata = metadata.copy()
    serializable_metadata["saved_at"] = datetime.utcnow().isoformat()

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(serializable_metadata, f, ensure_ascii=False, indent=2)


def load_metadata(metadata_path: Path) -> dict:
    with open(metadata_path, "r", encoding="utf-8") as f:
        return json.load(f)