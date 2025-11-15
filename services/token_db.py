import json
import os
import time
import shutil
from datetime import datetime
from typing import Dict, Optional, Any, List, Tuple

from logging_config import get_logger

logger = get_logger(__name__)


class TokenDB:
    def __init__(
        self,
        db_path: str = "data/token_contracts.json",
        backup_dir: str = "data/backups",
        ttl_days: int = 30,
        hot_cache_size: int = 256,
    ):
        self.db_path = db_path
        self.backup_dir = backup_dir
        self.ttl_seconds = ttl_days * 24 * 3600
        self.hot_cache_size = hot_cache_size
        self._db: Dict[str, Any] = {}
        self._hot_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
        self._stats = {
            "hits": 0,
            "misses": 0,
            "api_calls": 0,
            "writes": 0,
            "backups": 0,
            "last_reset": datetime.utcnow().isoformat()
        }
        self._ensure_dirs()
        self._load()

    # ------------ public API ------------
    def get_contracts(
        self,
        symbol: str,
        reference_price: Optional[float] = None,
        tolerance_percent: float = 10.0,
    ) -> Optional[Dict[str, str]]:
        """Return contracts mapping if fresh (and, если задано, совпадает по цене); increments hit/miss statistics."""
        key = symbol.upper()

        # Hot cache check
        cached = self._hot_cache.get(key)
        if cached:
            entry = cached[0]
            if not self._is_price_mismatch(entry, reference_price, tolerance_percent):
                self._stats["hits"] += 1
                return entry.get("contracts")

        entry = self._db.get(key)
        if not entry:
            self._stats["misses"] += 1
            return None

        if self._is_expired(entry):
            self._stats["misses"] += 1
            return None

        if self._is_price_mismatch(entry, reference_price, tolerance_percent):
            self._stats["misses"] += 1
            return None

        self._cache_hot(key, entry)
        self._stats["hits"] += 1
        return entry.get("contracts")

    def upsert_token(
        self,
        symbol: str,
        contracts: Dict[str, str],
        exchanges_found: Optional[List[str]] = None,
        verified_ts: Optional[float] = None,
        reference_price: Optional[float] = None,
    ) -> None:
        key = symbol.upper()
        now = time.time()
        entry = self._db.get(key) or {}
        entry["symbol"] = key
        entry["contracts"] = {k.upper(): v.lower() for k, v in contracts.items()}
        entry["date_added"] = entry.get("date_added") or datetime.utcnow().date().isoformat()
        entry["last_verified"] = datetime.utcfromtimestamp(verified_ts or now).date().isoformat()
        if reference_price is not None:
            entry["reference_price"] = float(reference_price)

        if exchanges_found:
            prev = set(entry.get("exchanges_found", []))
            entry["exchanges_found"] = sorted(list(prev.union(set(exchanges_found))))

        self._db[key] = entry
        self._cache_hot(key, entry)
        self._persist()
        self._stats["writes"] += 1

    def mark_api_call(self):
        self._stats["api_calls"] += 1

    def get_stats(self) -> Dict[str, Any]:
        return dict(self._stats)

    # ------------ internal helpers ------------
    def _ensure_dirs(self):
        base_dir = os.path.dirname(self.db_path)
        if base_dir and not os.path.exists(base_dir):
            os.makedirs(base_dir, exist_ok=True)
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir, exist_ok=True)

    def _load(self):
        if not os.path.exists(self.db_path):
            self._db = {}
            return
        try:
            with open(self.db_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # normalize keys
            self._db = {k.upper(): v for k, v in data.items()}
            logger.info(f"📚 TokenDB loaded: {len(self._db)} entries from {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to load TokenDB: {e}")
            self._db = {}

    def _persist(self):
        try:
            tmp_path = f"{self.db_path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._db, f, ensure_ascii=False, indent=2)
            # rotate backup before replacing
            self._backup()
            os.replace(tmp_path, self.db_path)
        except Exception as e:
            logger.error(f"Failed to persist TokenDB: {e}")

    def _backup(self):
        try:
            if os.path.exists(self.db_path):
                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                dst = os.path.join(self.backup_dir, f"token_contracts_{ts}.json")
                shutil.copy2(self.db_path, dst)
                self._stats["backups"] += 1
        except Exception as e:
            logger.debug(f"TokenDB backup skipped: {e}")

    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        last_verified_str = entry.get("last_verified")
        if not last_verified_str:
            return True
        try:
            last_dt = datetime.fromisoformat(last_verified_str)
        except Exception:
            return True
        age = (datetime.utcnow().date() - last_dt).days
        return age * 24 * 3600 > self.ttl_seconds

    def _is_price_mismatch(
        self,
        entry: Dict[str, Any],
        reference_price: Optional[float],
        tolerance_percent: float,
    ) -> bool:
        if reference_price is None or reference_price <= 0:
            return False
        stored_price = entry.get("reference_price")
        if stored_price is None or stored_price <= 0:
            return False
        diff = abs(stored_price - reference_price) / reference_price * 100
        return diff > tolerance_percent

    def _cache_hot(self, key: str, entry: Dict[str, Any]):
        self._hot_cache[key] = (entry, time.time())
        if len(self._hot_cache) > self.hot_cache_size:
            # Drop oldest
            oldest_key = min(self._hot_cache.items(), key=lambda kv: kv[1][1])[0]
            self._hot_cache.pop(oldest_key, None)


