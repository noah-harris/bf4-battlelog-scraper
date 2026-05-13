import os
import time
import random
from typing import Any
from requests import Session
from requests.exceptions import ConnectionError, ProxyError, ReadTimeout, Timeout

# --- Rate limiter ---

_base_delay: float = 1.0  # seconds
_jitter: float = 0.5       # ± seconds of uniform random noise


def set_rate(base: float = 1.0, jitter: float = 0.5) -> None:
    global _base_delay, _jitter
    _base_delay = base
    _jitter = jitter


def _throttle() -> None:
    delay = _base_delay + random.uniform(-_jitter, _jitter)
    print(f"Throttling for {delay:.2f} seconds...")
    time.sleep(max(0.0, delay))


# --- HTTP session ---

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "X-AjaxNavigation": "1",
    "Referer": "https://battlelog.battlefield.com/bf4/",
}

_session = Session()
_session.headers.update(_DEFAULT_HEADERS)

_warmed_up = False

def set_session_cookies(beaker_session_id: str, sso_remember: str = "1") -> None:
    """Attach Battlelog auth cookies to the module session."""
    _session.cookies.update({
        "beaker.session.id": beaker_session_id,
        "ssoremember": sso_remember,
    })

beaker = os.environ.get("BATTLELOG_SESSION", "42787af892295e21f998ed0ded19f8eb")
set_session_cookies(beaker)

# --- Proxy rotation ---

_PROXY_API = (
    "https://api.proxyscrape.com/v2/"
    "?request=getproxies&protocol=http&timeout=10000&country=all&anonymity=elite,anonymous"
)

_proxies: list[str] = [p.strip() for p in os.environ.get("PROXIES", "").split(",") if p.strip()]


def refresh_proxies() -> None:
    global _proxies
    resp = _session.get(_PROXY_API, timeout=15)
    resp.raise_for_status()
    _proxies = [f"http://{line.strip()}" for line in resp.text.splitlines() if line.strip()]
    print(f"Loaded {len(_proxies)} proxies")


if not _proxies:
    refresh_proxies()


def _random_proxy() -> dict[str, str] | None:
    if not _proxies:
        return None
    url = random.choice(_proxies)
    return {"http": url, "https": url}

def _warm_up() -> None:
    """Hit the Battlelog homepage once to let Akamai set its bot-detection cookies."""
    global _warmed_up
    if _warmed_up:
        return
    _throttle()
    r = _session.get("https://battlelog.battlefield.com/bf4/")
    r.raise_for_status()
    _warmed_up = True


_TIMEOUT = (5, 30)  # (connect, read) seconds
_MAX_PROXY_RETRIES = 3


def _get(url: str, **kwargs: Any):
    _warm_up()
    _throttle()
    kwargs.setdefault("timeout", _TIMEOUT)
    for _ in range(_MAX_PROXY_RETRIES):
        proxy = _random_proxy()
        try:
            r = _session.get(url, proxies=proxy, **kwargs)
            r.raise_for_status()
            return r
        except (ProxyError, ConnectionError, ReadTimeout, Timeout):
            if proxy and proxy["http"] in _proxies:
                _proxies.remove(proxy["http"])
    # all proxies failed — fall back to direct
    r = _session.get(url, **kwargs)
    r.raise_for_status()
    return r


def _get_json(url: str, **kwargs: Any) -> dict:
    return _get(url, **kwargs).json()


def _get_text(url: str, **kwargs: Any) -> str:
    return _get(url, **kwargs).text
