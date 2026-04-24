from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests
from eth_account import Account


BASE_URL = "https://inception.dachain.io"
DEFAULT_REF_CODE = "DAC1392613"
USER_AGENT = "dac-python-faucet/2.0"


class ApiError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status = status
        self.payload = payload


@dataclass
class AuthResult:
    created: bool
    wallet_address: str
    raw: dict[str, Any]


def derive_address(private_key: str) -> str:
    normalized = private_key.strip()
    if not normalized:
        raise ValueError("Private key is empty.")
    if not normalized.startswith("0x"):
        normalized = f"0x{normalized}"
    return Account.from_key(normalized).address


def normalize_proxy(proxy: str | None) -> str | None:
    if not proxy:
        return None

    value = proxy.strip()
    if not value:
        return None

    if "://" not in value:
        value = f"http://{value}"

    parsed = urlparse(value)
    if not parsed.scheme or not parsed.hostname:
        raise ValueError(
            "Proxy format is invalid. Expected host:port, login:password@host:port, "
            "or full URL with scheme."
        )

    return value


class DachainClient:
    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        ref_code: str | None = DEFAULT_REF_CODE,
        proxy: str | None = None,
        timeout: int = 20,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            }
        )
        if ref_code:
            self.session.cookies.set("ref_code", ref_code, domain="inception.dachain.io", path="/")
        if proxy:
            normalized_proxy = normalize_proxy(proxy)
            self.session.proxies.update({"http": normalized_proxy, "https": normalized_proxy})

    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}{path}"

    def _csrf_token(self) -> str:
        return self.session.cookies.get("csrftoken", "")

    def bootstrap_csrf(self) -> None:
        if self._csrf_token():
            return
        response = self.session.get(
            self._url("/csrf/"),
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        if not self._csrf_token():
            raise ApiError("CSRF bootstrap succeeded but csrftoken cookie is missing.")

    def get_json(self, path: str) -> dict[str, Any]:
        response = self.session.get(self._url(path), timeout=self.timeout)
        return self._decode_json(response)

    def post_json(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        self.bootstrap_csrf()
        response = self.session.post(
            self._url(path),
            json=payload,
            headers={
                "Content-Type": "application/json",
                "X-CSRFToken": self._csrf_token(),
            },
            timeout=self.timeout,
        )
        return self._decode_json(response)

    @staticmethod
    def _decode_json(response: requests.Response) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError:
            data = {}
        if not response.ok:
            message = data.get("error") if isinstance(data, dict) else None
            raise ApiError(message or response.reason, status=response.status_code, payload=data)
        if not isinstance(data, dict):
            raise ApiError("Unexpected non-object JSON response.", status=response.status_code, payload=data)
        return data

    def authenticate_wallet(self, wallet_address: str) -> AuthResult:
        data = self.post_json("/api/auth/wallet/", {"wallet_address": wallet_address})
        user = data.get("user") or {}
        return AuthResult(
            created=bool(data.get("created")),
            wallet_address=user.get("wallet_address", wallet_address),
            raw=data,
        )

    def profile(self) -> dict[str, Any]:
        return self.get_json("/api/inception/profile/")

    def claim_badge(self) -> dict[str, Any]:
        return self.post_json("/api/inception/claim-badge/")

    def claim_faucet(self) -> dict[str, Any]:
        return self.post_json("/api/inception/faucet/")

    def crate_history(self) -> dict[str, Any]:
        return self.get_json("/api/inception/crate/history/")

    def crate_open(self) -> dict[str, Any]:
        return self.post_json("/api/inception/crate/open/")

    def faucet_status(self, dispense_id: str) -> dict[str, Any]:
        return self.get_json(f"/api/inception/faucet/status/{dispense_id}/")

    def nft_claim_signature(self, rank_key: str) -> dict[str, Any]:
        return self.post_json("/api/inception/nft/claim-signature/", {"rank_key": rank_key})

    def nft_confirm_mint(self, rank_key: str, tx_hash: str) -> dict[str, Any]:
        return self.post_json("/api/inception/nft/confirm-mint/", {"rank_key": rank_key, "tx_hash": tx_hash})

    def poll_dispense(
        self,
        dispense_id: str,
        *,
        timeout_seconds: int,
        interval_seconds: float,
    ) -> dict[str, Any]:
        started = time.time()
        while True:
            status = self.faucet_status(dispense_id)
            state = status.get("status")
            if state in {"success", "failed"}:
                return status
            if time.time() - started >= timeout_seconds:
                raise TimeoutError(f"Timed out waiting for dispense_id={dispense_id}")
            time.sleep(interval_seconds)
