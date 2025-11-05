from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "config"
INFRA_PATH = BASE_DIR / "infrastructure.yaml"
RULES_PATH = BASE_DIR / "rules.yaml"
