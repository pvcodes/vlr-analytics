import logging

logger = logging.getLogger("vlr-etl")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---- File handler ----
file_handler = logging.FileHandler("scraping.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# ---- Console handler ----
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# ---- Attach handlers ----
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# (Optional but recommended)
logger.propagate = False
