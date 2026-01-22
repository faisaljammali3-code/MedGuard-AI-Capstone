import yaml
from pathlib import Path
from loguru import logger

# تحديد مسار الجذر للمشروع (Project Root)
# بما أن هذا الملف في src/MedGuard_AI/config.py
# فالجذر هو ثلاث خطوات للخلف (Parent of src)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"

def load_config():
    """تحميل الإعدادات من ملف YAML"""
    if not CONFIG_PATH.exists():
        logger.critical(f"Config file not found at: {CONFIG_PATH}")
        raise FileNotFoundError(f"Config file missing: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r") as f:
        try:
            config = yaml.safe_load(f)
            logger.debug("Configuration loaded successfully.")
            return config
        except yaml.YAMLError as exc:
            logger.error(f"Error parsing YAML: {exc}")
            raise