import time
from typing import Dict, Optional, Tuple, Any, List
import requests

from logging_config import get_logger

logger = get_logger(__name__)


class CMCClient:
    def __init__(
        self,
        api_key: str,
        request_timeout: float = 6.0,
        max_retries: int = 2,
        cache_ttl_seconds: int = 86400,
    ):
        self.api_key = api_key
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.cache_ttl_seconds = cache_ttl_seconds
        # cache: symbol_upper -> ({"contracts": dict, "price": float}, ts)
        self._contract_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}

    def _headers(self) -> Dict[str, str]:
        return {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": self.api_key,
        }

    def _get(self, url: str, params: Dict) -> Optional[Dict]:
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(
                    url, params=params, headers=self._headers(), timeout=self.request_timeout
                )
                if resp.status_code == 200:
                    return resp.json()
                # 429 or 5xx handling with backoff
                if resp.status_code in (429, 500, 502, 503, 504):
                    delay = min(2 ** attempt, 8)
                    logger.warning(f"CMC HTTP {resp.status_code}, retry in {delay}s")
                    time.sleep(delay)
                    continue
                logger.error(f"CMC error HTTP {resp.status_code}: {resp.text[:200]}")
                return None
            except Exception as e:
                last_exc = e
                delay = min(2 ** attempt, 8)
                logger.warning(f"CMC request error ({type(e).__name__}): {e}. Retry in {delay}s")
                time.sleep(delay)
        if last_exc:
            logger.error(f"CMC request failed: {last_exc}")
        return None

    def get_token_contracts(
        self,
        symbol: str,
        reference_price: Optional[float] = None,
        tolerance_percent: float = 10.0,
    ) -> Optional[Dict[str, str]]:
        """
        Returns a dict of chain -> contract_address, e.g. {'ETH': '0x...', 'BSC': '0x...'}
        Uses CMC /v2/cryptocurrency/info with symbol param.
        Caches results to minimize API usage.
        """
        if not self.api_key:
            logger.debug("CMC API key missing; skipping contract lookup")
            return None

        symbol_key = symbol.upper()
        cached = self._contract_cache.get(symbol_key)
        if cached and (time.time() - cached[1]) < self.cache_ttl_seconds:
            payload = cached[0]
            cached_contracts = payload.get("contracts", {})
            cached_price = payload.get("price")
            if self._within_tolerance(reference_price, cached_price, tolerance_percent):
                return cached_contracts if cached_contracts else None

        url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info"
        data = self._get(url, params={"symbol": symbol_key})
        if not data or "data" not in data:
            return None

        try:
            entries: List[Dict[str, Any]] = data["data"].get(symbol_key, [])
        except Exception:
            entries = []

        if not entries:
            return None

        quotes: Dict[str, float] = {}
        if reference_price and reference_price > 0:
            ids = [entry.get("id") for entry in entries if entry.get("id") is not None]
            if ids:
                quotes = self._fetch_quotes(ids)

        best_entry: Optional[Dict[str, Any]] = None
        best_diff = float("inf")
        selected_price: Optional[float] = None

        for entry in entries:
            contracts = self._extract_contracts(entry)
            if not contracts:
                continue

            entry_price = None
            entry_id = entry.get("id")
            if entry_id is not None:
                entry_price = quotes.get(str(entry_id))

            diff = float("inf")
            if self._valid_price(reference_price) and self._valid_price(entry_price):
                diff = abs(entry_price - reference_price) / reference_price * 100

            if self._valid_price(reference_price):
                if diff <= tolerance_percent and diff < best_diff:
                    best_entry = entry
                    best_diff = diff
                    selected_price = entry_price
            if best_entry is None:
                # fallback: choose first suitable entry or minimal diff even if > tolerance
                if self._valid_price(reference_price):
                    if diff < best_diff:
                        best_entry = entry
                        best_diff = diff
                        selected_price = entry_price
                else:
                    best_entry = entry
                    selected_price = entry_price

        if best_entry is None:
            best_entry = entries[0]

        contracts = self._extract_contracts(best_entry)
        payload = {"contracts": contracts, "price": selected_price}
        self._contract_cache[symbol_key] = (payload, time.time())
        return contracts if contracts else None

    def _extract_contracts(self, entry: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize contracts from CMC /v2/cryptocurrency/info payload.
        CMC may return contracts in different shapes:
          1) 'contract_address': [{'contract_address': '0x..', 'platform': {'name'|'symbol'|'slug'}}]
          2) 'platforms': [{'token_address': '0x..', 'name'|'symbol'|'slug': 'BSC'|'ETH'|...}]
        We support both.
        """
        result: Dict[str, str] = {}
        try:
            # Shape 1: contract_address array with nested platform object
            for item in entry.get("contract_address", []) or []:
                try:
                    address = (item.get("contract_address") or "").lower()
                    plat = item.get("platform") or {}
                    chain_name = (plat.get("name") or plat.get("symbol") or plat.get("slug") or "").upper()
                    if chain_name and address:
                        result[chain_name] = address
                except Exception:
                    continue
            # Shape 2: platforms array with token_address
            for platform in entry.get("platforms", []) or []:
                try:
                    chain_name = (
                        platform.get("name")
                        or platform.get("symbol")
                        or platform.get("slug")
                        or ""
                    ).upper()
                    address = (platform.get("token_address") or "").lower()
                    if chain_name and address:
                        result[chain_name] = address
                except Exception:
                    continue
        except Exception:
            return {}
        return result

    def _fetch_quotes(self, ids: List[int]) -> Dict[str, float]:
        if not ids:
            return {}
        id_param = ",".join(str(_id) for _id in ids)
        url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
        data = self._get(url, params={"id": id_param, "convert": "USD"})
        if not data or "data" not in data:
            return {}
        quotes: Dict[str, float] = {}
        for key, value in data["data"].items():
            try:
                price = (
                    value.get("quote", {})
                    .get("USD", {})
                    .get("price")
                )
                if price is not None:
                    quotes[str(key)] = float(price)
            except Exception:
                continue
        return quotes

    def _within_tolerance(
        self,
        reference_price: Optional[float],
        candidate_price: Optional[float],
        tolerance_percent: float,
    ) -> bool:
        if not self._valid_price(reference_price) or not self._valid_price(candidate_price):
            return True  # нет данных — не считаем несовпадением
        diff = abs(candidate_price - reference_price) / reference_price * 100
        return diff <= tolerance_percent

    @staticmethod
    def _valid_price(value: Optional[float]) -> bool:
        return value is not None and value > 0


