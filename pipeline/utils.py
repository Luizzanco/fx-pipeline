import logging
from datetime import datetime, timezone

def setup_logger(name=__name__):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def filename_for_date(prefix="", ext="json", dt=None):
    from datetime import datetime
    if dt is None:
        dt = datetime.utcnow()
    return f"{prefix}{dt.strftime('%Y-%m-%d')}.{ext}"
