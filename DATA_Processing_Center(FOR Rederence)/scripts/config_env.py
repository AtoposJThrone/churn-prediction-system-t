import os


def env(name: str, default=None, required: bool = False):
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def env_int(name: str, default: int) -> int:
    return int(env(name, default))


def env_bool(name: str, default: bool = False) -> bool:
    value = str(env(name, str(default))).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}