import os
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2]))

ARTIFACTS_ROOT_DIR = PROJECT_ROOT / "artifacts"
METADATA_ARTIFACT_DIR = ARTIFACTS_ROOT_DIR / "metadata"
PREPROCESS_ARTIFACTS_DIR = ARTIFACTS_ROOT_DIR / "preprocess"
POSTPROCESS_ARTIFACTS_DIR = ARTIFACTS_ROOT_DIR / "postprocess"
INFERENCE_ARTIFACTS_DIR = ARTIFACTS_ROOT_DIR / "inference"

CONFIGS_ROOT_DIR = PROJECT_ROOT / "configs"
PREPROCESS_CONFIGS_DIR = CONFIGS_ROOT_DIR / "preprocess"
POSTPROCESS_CONFIGS_DIR = CONFIGS_ROOT_DIR / "postprocess"
INFERENCE_CONFIGS_DIR = CONFIGS_ROOT_DIR / "inference"

MEDALLION_ROOT_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = MEDALLION_ROOT_DIR / "bronze"
SILVER_DIR = MEDALLION_ROOT_DIR / "silver"


def ensure_directories() -> None:
    """Create required directories if they do not exist."""
    for path in [
        ARTIFACTS_ROOT_DIR,
        METADATA_ARTIFACT_DIR,
        PREPROCESS_ARTIFACTS_DIR,
        POSTPROCESS_ARTIFACTS_DIR,
        INFERENCE_ARTIFACTS_DIR,
        CONFIGS_ROOT_DIR,
        PREPROCESS_CONFIGS_DIR,
        POSTPROCESS_CONFIGS_DIR,
        INFERENCE_CONFIGS_DIR,
        BRONZE_DIR,
        SILVER_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)
