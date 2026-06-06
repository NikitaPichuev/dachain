from __future__ import annotations

import json
import logging
import os
import random
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from itertools import cycle
from pathlib import Path
from typing import Any

from dachain_client import ApiError, DachainClient, derive_address, normalize_proxy
from eth_account import Account
from web3 import Web3


ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config"
LOGS_DIR = ROOT / "logs"
SETTINGS_PATH = CONFIG_DIR / "settings.json"
PRIVATE_KEYS_PATH = CONFIG_DIR / "private_keys.txt"
PROXIES_PATH = CONFIG_DIR / "proxies.txt"
APP_LOG_PATH = LOGS_DIR / "app.log"
RUNNER_VERSION = "menu-faucet-badges-1"
DAC_TESTNET_CHAIN_ID = 21894
DAC_TESTNET_RPC_URL = "https://rpctest.dachain.tech"
RANK_BADGE_CONTRACT = "0xB36ab4c2Bd6aCfC36e9D6c53F39F4301901Bd647"
RANK_BADGE_ABI: list[dict[str, Any]] = [
    {
        "name": "claimRank",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "rankId", "type": "uint8"},
            {"name": "signature", "type": "bytes"},
        ],
        "outputs": [],
    },
    {
        "name": "hasMinted",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "", "type": "address"},
            {"name": "", "type": "uint8"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
]
CRATE_QE_COST = 150
QE_POOL_CONTRACT = "0x3691A78bE270dB1f3b1a86177A8f23F89A8Cef24"
QE_PER_DACC = 1000
QE_POOL_ABI: list[dict[str, Any]] = [
    {"name": "burnForQE", "type": "function", "stateMutability": "payable", "inputs": [], "outputs": []},
    {"name": "stake", "type": "function", "stateMutability": "payable", "inputs": [], "outputs": []},
    {
        "name": "unstake",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [{"name": "amount", "type": "uint256"}],
        "outputs": [],
    },
    {"name": "claimFees", "type": "function", "stateMutability": "nonpayable", "inputs": [], "outputs": []},
    {
        "name": "pendingFees",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "user", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "lps",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "", "type": "address"}],
        "outputs": [
            {"name": "staked", "type": "uint256"},
            {"name": "rewardDebt", "type": "uint256"},
        ],
    },
    {"name": "totalStaked", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "totalBurned", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "burnBps", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "uint16"}]},
]
MINTAURA_NATIVE_TOKEN = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
MINTAURA_NFTS: list[dict[str, Any]] = [
    {
        "name": "Inception",
        "contract": "0xcB98FA54D3dEa40795924112215A34004D078C10",
        "price_wei": "100000000000000000",
    },
    {
        "name": "Prism",
        "contract": "0xE644b94357466E6f45Cb27382608C6C4A3410Caa",
        "price_wei": "100000000000000000",
    },
    {
        "name": "Evie",
        "contract": "0xD2eb0557A0541ff66A86A41a8A966cbb6e38E234",
        "price_wei": "100000000000000000",
    },
    {
        "name": "Nyxia",
        "contract": "0x7CDD0E631372747764b70b9d97948932c1eBC706",
        "price_wei": "0",
    },
]
MINTAURA_ERC721_DROP_ABI: list[dict[str, Any]] = [
    {
        "name": "claim",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "_receiver", "type": "address"},
            {"name": "_quantity", "type": "uint256"},
            {"name": "_currency", "type": "address"},
            {"name": "_pricePerToken", "type": "uint256"},
            {
                "name": "_allowlistProof",
                "type": "tuple",
                "components": [
                    {"name": "proof", "type": "bytes32[]"},
                    {"name": "quantityLimitPerWallet", "type": "uint256"},
                    {"name": "pricePerToken", "type": "uint256"},
                    {"name": "currency", "type": "address"},
                ],
            },
            {"name": "_data", "type": "bytes"},
        ],
        "outputs": [],
    },
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "owner", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "totalMinted",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]


DEFAULT_SETTINGS: dict[str, Any] = {
    "base_url": "https://inception.dachain.io",
    "ref_code": "DAC1392613",
    "request_timeout_seconds": 20,
    "poll_timeout_seconds": 45,
    "faucet_poll_timeout_seconds": 120,
    "crate_poll_timeout_seconds": 90,
    "poll_interval_seconds": 3,
    "delay_between_wallets_min_seconds": 3,
    "delay_between_wallets_max_seconds": 6,
    "delay_between_rank_mints_min_seconds": 1,
    "delay_between_rank_mints_max_seconds": 4,
    "rank_mint_gas_limit": 180000,
    "use_proxy_for_rpc": True,
    "delay_between_faucet_requests_min_seconds": 2,
    "delay_between_faucet_requests_max_seconds": 5,
    "faucet_auth_retry_count": 5,
    "faucet_auth_retry_delay_min_seconds": 20,
    "faucet_auth_retry_delay_max_seconds": 40,
    "faucet_busy_retry_count": 2,
    "faucet_busy_retry_delay_min_seconds": 20,
    "faucet_busy_retry_delay_max_seconds": 35,
    "delay_between_crates_min_seconds": 1,
    "delay_between_crates_max_seconds": 3,
    "delay_between_crate_requests_min_seconds": 1,
    "delay_between_crate_requests_max_seconds": 2,
    "crate_retry_count": 5,
    "crate_retry_backoff_min_seconds": 20,
    "crate_retry_backoff_max_seconds": 40,
    "crate_poll_dacc_status": False,
    "delay_between_mintaura_mints_min_seconds": 2,
    "delay_between_mintaura_mints_max_seconds": 5,
    "mintaura_allow_paid": False,
    "exchange_operation": "burn",
    "exchange_percent_min": 5,
    "exchange_percent_max": 10,
    "exchange_transactions_count": 1,
    "delay_between_exchange_txs_min_seconds": 2,
    "delay_between_exchange_txs_max_seconds": 5,
    "exchange_gas_reserve_dacc": "0.00005",
    "exchange_burn_gas_limit": 350000,
    "exchange_stake_gas_limit": 350000,
    "exchange_withdraw_gas_limit": 350000,
    "exchange_claim_gas_limit": 220000,
    "exchange_rpc_tx_retry_count": 3,
    "rpc_fixed_gas_price_gwei": "1",
    "transactions_per_wallet": 1,
    "transaction_amount_min_dacc": "0.000000000000000001",
    "transaction_amount_max_dacc": "0.000000000000000003",
    "delay_between_transactions_min_seconds": 2,
    "delay_between_transactions_max_seconds": 5,
    "transaction_gas_reserve_dacc": "0.00005",
    "rpc_proxy_fallback_enabled": True,
    "rpc_proxy_probe_limit": 0,
    "cycle_proxies": True,
}


@dataclass
class WalletEntry:
    index: int
    private_key: str
    address: str
    proxy: str | None


def ensure_layout() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    if not SETTINGS_PATH.exists():
        SETTINGS_PATH.write_text(
            json.dumps(DEFAULT_SETTINGS, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    if not PRIVATE_KEYS_PATH.exists():
        PRIVATE_KEYS_PATH.write_text(
            "# One private key per line\n"
            "# Example:\n"
            "# 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
            encoding="utf-8",
        )
    if not PROXIES_PATH.exists():
        PROXIES_PATH.write_text(
            "# One proxy per line\n"
            "# Examples:\n"
            "# http://127.0.0.1:8080\n"
            "# login:password@127.0.0.1:8080\n",
            encoding="utf-8",
        )


def setup_logging() -> logging.Logger:
    ensure_layout()
    logger = logging.getLogger("dac")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_handler = logging.FileHandler(APP_LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def load_settings() -> dict[str, Any]:
    ensure_layout()
    try:
        data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"settings.json damaged: {exc}") from exc
    merged = dict(DEFAULT_SETTINGS)
    merged.update(data)
    return merged


def load_lines(path: Path) -> list[str]:
    ensure_layout()
    if not path.exists():
        return []
    values: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        values.append(line)
    return values


def is_retryable_api_error(exc: ApiError) -> bool:
    return exc.status in {500, 502, 503, 504}


def build_wallet_entries(logger: logging.Logger) -> list[WalletEntry]:
    private_keys = load_lines(PRIVATE_KEYS_PATH)
    proxies = load_lines(PROXIES_PATH)
    settings = load_settings()

    if not private_keys:
        raise RuntimeError(f"No private keys found in {PRIVATE_KEYS_PATH}")

    proxy_cycle = cycle(proxies) if proxies and settings.get("cycle_proxies", True) else None
    entries: list[WalletEntry] = []

    for idx, private_key in enumerate(private_keys, start=1):
        try:
            address = derive_address(private_key)
        except Exception as exc:
            logger.error("Wallet #%s skipped: invalid private key: %s", idx, exc)
            continue

        try:
            proxy = normalize_proxy(next(proxy_cycle) if proxy_cycle else None)
        except Exception as exc:
            logger.error("Wallet #%s skipped: invalid proxy format: %s", idx, exc)
            continue

        entries.append(WalletEntry(index=idx, private_key=private_key, address=address, proxy=proxy))

    return entries


def create_run_logger(wallet_index: int, address: str) -> logging.Logger:
    logger_name = f"dac.run.{wallet_index}.{address.lower()}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe_address = address.lower().replace("0x", "")
    run_log_path = LOGS_DIR / f"wallet_{wallet_index}_{safe_address}_{timestamp}.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.FileHandler(run_log_path, encoding="utf-8")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_web3(entry: WalletEntry, settings: dict[str, Any]) -> Web3:
    request_kwargs: dict[str, Any] = {"timeout": int(settings["request_timeout_seconds"])}
    if entry.proxy and bool(settings.get("use_proxy_for_rpc", False)):
        request_kwargs["proxies"] = {"http": entry.proxy, "https": entry.proxy}
    provider = Web3.HTTPProvider(DAC_TESTNET_RPC_URL, request_kwargs=request_kwargs)
    return Web3(provider)


def get_web3_for_proxy(settings: dict[str, Any], proxy: str | None) -> Web3:
    request_kwargs: dict[str, Any] = {"timeout": int(settings["request_timeout_seconds"])}
    if proxy and bool(settings.get("use_proxy_for_rpc", False)):
        request_kwargs["proxies"] = {"http": proxy, "https": proxy}
    provider = Web3.HTTPProvider(DAC_TESTNET_RPC_URL, request_kwargs=request_kwargs)
    return Web3(provider)


def rpc_proxy_candidates(entry: WalletEntry, settings: dict[str, Any]) -> list[str | None]:
    if not bool(settings.get("use_proxy_for_rpc", False)):
        return [None]

    candidates: list[str | None] = []
    seen: set[str] = set()
    if entry.proxy:
        candidates.append(entry.proxy)
        seen.add(entry.proxy)

    if bool(settings.get("rpc_proxy_fallback_enabled", True)):
        for raw_proxy in load_lines(PROXIES_PATH):
            try:
                proxy = normalize_proxy(raw_proxy)
            except Exception:
                continue
            if proxy and proxy not in seen:
                candidates.append(proxy)
                seen.add(proxy)

    probe_limit = int(settings.get("rpc_proxy_probe_limit", 0) or 0)
    if probe_limit > 0:
        return candidates[:probe_limit]
    return candidates


def get_connected_web3(
    entry: WalletEntry,
    settings: dict[str, Any],
    log: Any,
    log_error: Any,
    context: str,
    excluded_proxies: set[str] | None = None,
) -> tuple[Web3 | None, str | None]:
    excluded_proxies = excluded_proxies or set()
    candidates = [proxy for proxy in rpc_proxy_candidates(entry, settings) if not proxy or proxy not in excluded_proxies]
    if not candidates:
        log_error("%s RPC ERROR | no proxy candidates available", context)
        return None, None

    for attempt, proxy in enumerate(candidates, start=1):
        try:
            w3 = get_web3_for_proxy(settings, proxy)
            chain_id = int(w3.eth.chain_id)
            if chain_id != DAC_TESTNET_CHAIN_ID:
                log_error("%s RPC ERROR | attempt=%s/%s | proxy=%s | wrong_chain_id=%s", context, attempt, len(candidates), proxy or "-", chain_id)
                continue
            if proxy != entry.proxy:
                log("%s RPC proxy fallback OK | proxy=%s", context, proxy or "-")
            return w3, proxy
        except Exception as exc:
            log_error("%s RPC CONNECT ERROR | attempt=%s/%s | proxy=%s | %s", context, attempt, len(candidates), proxy or "-", exc)

    return None, None


def normalize_tx_hash(tx_hash: Any) -> str:
    value = str(tx_hash or "").strip()
    if value and not value.startswith("0x"):
        return f"0x{value}"
    return value


def parse_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, AttributeError):
        return default


def decimal_to_wei(value: Decimal) -> int:
    if value <= 0:
        return 0
    return int((value * Decimal(10**18)).to_integral_value(rounding=ROUND_DOWN))


def gwei_to_wei(value: Decimal) -> int:
    if value <= 0:
        return 0
    return int((value * Decimal(10**9)).to_integral_value(rounding=ROUND_DOWN))


def wei_to_decimal(value: int) -> Decimal:
    return Decimal(int(value)) / Decimal(10**18)


def format_dacc_wei(value: int, places: int = 6) -> str:
    quant = Decimal(1).scaleb(-places)
    return str(wei_to_decimal(value).quantize(quant, rounding=ROUND_DOWN).normalize())


def normalize_percent_range(min_percent: Any, max_percent: Any) -> tuple[Decimal, Decimal]:
    percent_min = max(parse_decimal(min_percent), Decimal("0"))
    percent_max = max(parse_decimal(max_percent), Decimal("0"))
    if percent_max < percent_min:
        percent_min, percent_max = percent_max, percent_min
    return min(percent_min, Decimal("100")), min(percent_max, Decimal("100"))


def random_percent_wei(base_wei: int, percent_min: Decimal, percent_max: Decimal) -> tuple[int, Decimal]:
    if base_wei <= 0 or percent_max <= 0:
        return 0, Decimal("0")
    chosen = Decimal(str(random.uniform(float(percent_min), float(percent_max))))
    amount_wei = int((Decimal(base_wei) * chosen / Decimal("100")).to_integral_value(rounding=ROUND_DOWN))
    return amount_wei, chosen


class TxSubmittedError(RuntimeError):
    def __init__(self, tx_hash: str, message: str) -> None:
        super().__init__(message)
        self.tx_hash = tx_hash


def find_last_rank_tx_hash(address: str, rank_key: str) -> str | None:
    if not APP_LOG_PATH.exists():
        return None

    current_badges_address: str | None = None
    target_address = address.lower()
    found: str | None = None

    for raw_line in APP_LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if "Wallet #" in line and "mode=badges" in line and "address=" in line:
            current_badges_address = line.split("address=", 1)[1].split(" |", 1)[0].strip().lower()
            continue

        marker = f"Rank mint sent | rank_key={rank_key}"
        if current_badges_address == target_address and marker in line and "tx_hash=" in line:
            tx_hash = line.split("tx_hash=", 1)[1].split()[0].strip()
            found = normalize_tx_hash(tx_hash)

    return found


def claim_early_badge(
    client: DachainClient,
    profile: dict[str, Any],
    log: Any,
    log_error: Any,
) -> dict[str, Any]:
    if profile.get("early_badge_claimed"):
        log("Early badge already claimed.")
        return profile

    try:
        client.claim_badge()
        log("Early badge claimed.")
        return client.profile()
    except ApiError as exc:
        log_error("EARLY BADGE ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        return profile
    except Exception as exc:
        log_error("UNEXPECTED EARLY BADGE ERROR | %s", exc)
        return profile


def badge_keys(profile: dict[str, Any]) -> set[str]:
    return {str(badge.get("badge__key", "")) for badge in profile.get("badges", [])}


def is_no_claimable_badge_error(exc: ApiError) -> bool:
    message = f"{exc} {exc.payload}".lower()
    if exc.status not in {400, 404, 409}:
        return False
    markers = (
        "already",
        "not available",
        "no badge",
        "no claim",
        "nothing",
        "window",
        "cooldown",
        "not eligible",
    )
    return any(marker in message for marker in markers)


def claim_available_badge(
    client: DachainClient,
    profile: dict[str, Any],
    log: Any,
    log_error: Any,
) -> dict[str, Any]:
    if profile.get("early_badge_claimed"):
        log("Early badge already claimed; checking other claimable badges.")

    before_keys = badge_keys(profile)

    try:
        payload = client.claim_badge()
        log("Badge claim requested | payload=%s", payload)
        updated_profile = client.profile()
        new_badges = sorted(badge_keys(updated_profile) - before_keys)
        if new_badges:
            log("Badge claimed | new_badges=%s", new_badges)
        else:
            log("Badge claim completed | no new badge keys detected")
        return updated_profile
    except ApiError as exc:
        if is_no_claimable_badge_error(exc):
            log("SKIP: no claimable badge | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
            return profile
        log_error("BADGE CLAIM ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        return profile
    except Exception as exc:
        log_error("UNEXPECTED BADGE CLAIM ERROR | %s", exc)
        return profile


def claim_flash_badge(
    client: DachainClient,
    profile: dict[str, Any],
    log: Any,
    log_error: Any,
) -> dict[str, Any]:
    flash_badge = profile.get("flash_badge") or {}
    state = str(flash_badge.get("state") or "").strip().lower()
    if state and state != "live":
        log("SKIP: flash badge not live | state=%s", state)
        return profile

    try:
        payload = client.claim_flash_badge()
        updated_flash_badge = payload.get("flash_badge") or {}
        log(
            "FLASH BADGE CLAIMED | state=%s | multiplier=%s | multiplier_expires_at=%s | payload=%s",
            updated_flash_badge.get("state"),
            updated_flash_badge.get("multiplier"),
            updated_flash_badge.get("multiplier_expires_at"),
            payload,
        )
        return client.profile()
    except ApiError as exc:
        if is_no_claimable_badge_error(exc):
            log("SKIP: no claimable flash badge | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
            return profile
        log_error("FLASH BADGE CLAIM ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        return profile
    except Exception as exc:
        log_error("UNEXPECTED FLASH BADGE CLAIM ERROR | %s", exc)
        return profile


def mint_rank_badges(
    client: DachainClient,
    entry: WalletEntry,
    profile: dict[str, Any],
    settings: dict[str, Any],
    log: Any,
    log_error: Any,
) -> tuple[bool, dict[str, Any], bool]:
    rank_badges = [
        badge
        for badge in profile.get("badges", [])
        if str(badge.get("badge__key", "")).startswith("rank_") and not badge.get("nft_tx_hash")
    ]

    try:
        dacc_balance = float(profile.get("dacc_balance") or 0)
    except (TypeError, ValueError):
        dacc_balance = 0.0

    if not rank_badges:
        log("No rank badges available for mint.")
        return True, profile, False

    try:
        w3, rpc_proxy = get_connected_web3(entry, settings, log, log_error, "RANK MINT")
        if not w3:
            return False, profile, True
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(RANK_BADGE_CONTRACT),
            abi=RANK_BADGE_ABI,
        )
        account = Account.from_key(entry.private_key if entry.private_key.startswith("0x") else f"0x{entry.private_key}")
        try:
            onchain_balance_wei = w3.eth.get_balance(account.address)
        except Exception as exc:
            onchain_balance_wei = decimal_to_wei(parse_decimal(str(profile.get("dacc_balance", "0") or "0")))
            log_error(
                "RANK MINT BALANCE RPC ERROR | using profile balance | profile_dacc_balance=%s | %s",
                profile.get("dacc_balance"),
                exc,
            )
        onchain_balance = onchain_balance_wei / 10**18
        log(
            "RPC OK | address=%s | rpc_proxy=%s | onchain_dacc_balance=%.18f | profile_dacc_balance=%s",
            account.address,
            rpc_proxy or "-",
            onchain_balance,
            profile.get("dacc_balance"),
        )
    except Exception as exc:
        log_error("RANK MINT SETUP ERROR | %s", exc)
        return False, profile, True

    if onchain_balance_wei <= 0:
        log(
            "SKIP: no onchain gas | onchain_dacc_balance=0 | profile_dacc_balance=%s | pending_rank_badges=%s",
            profile.get("dacc_balance"),
            len(rank_badges),
        )
        return False, profile, True

    all_ok = True
    current_profile = profile
    rank_mint_delay_min = float(settings.get("delay_between_rank_mints_min_seconds", 1))
    rank_mint_delay_max = float(settings.get("delay_between_rank_mints_max_seconds", 4))
    rank_mint_gas_limit = int(settings.get("rank_mint_gas_limit", 180000))
    fixed_gas_price_gwei = parse_decimal(settings.get("rpc_fixed_gas_price_gwei", "0"))
    rank_gas_price = gwei_to_wei(fixed_gas_price_gwei) if fixed_gas_price_gwei > 0 else w3.eth.gas_price
    if rank_mint_delay_max < rank_mint_delay_min:
        rank_mint_delay_min, rank_mint_delay_max = rank_mint_delay_max, rank_mint_delay_min

    for badge_index, badge in enumerate(rank_badges):
        rank_key = badge.get("badge__key")
        try:
            signature_data = client.nft_claim_signature(rank_key)
            rank_id = int(signature_data["rank_id"])
            signature_hex = str(signature_data["signature"])
            signature_bytes = bytes.fromhex(signature_hex.removeprefix("0x"))

            try:
                already_minted = contract.functions.hasMinted(account.address, rank_id).call()
            except Exception as exc:
                already_minted = False
                log_error("RANK HASMINTED RPC ERROR | rank_key=%s | rank_id=%s | minting anyway | %s", rank_key, rank_id, exc)
            if already_minted:
                previous_tx_hash = find_last_rank_tx_hash(account.address, str(rank_key))
                if not previous_tx_hash:
                    log(
                        "SKIP: rank already minted onchain but tx hash not found | rank_key=%s | rank_id=%s",
                        rank_key,
                        rank_id,
                    )
                    current_profile = client.profile()
                    all_ok = False
                    continue

                try:
                    client.nft_confirm_mint(rank_key, previous_tx_hash)
                    log("Rank mint recovered | rank_key=%s | tx_hash=%s", rank_key, previous_tx_hash)
                    current_profile = client.profile()
                except ApiError as exc:
                    all_ok = False
                    log_error(
                        "RANK RECOVER API ERROR | rank_key=%s | status=%s | message=%s | payload=%s",
                        rank_key,
                        exc.status,
                        exc,
                        exc.payload,
                    )
                continue

            nonce = w3.eth.get_transaction_count(account.address)
            function = contract.functions.claimRank(rank_id, signature_bytes)
            gas_limit = rank_mint_gas_limit
            estimated_fee_wei = gas_limit * rank_gas_price
            if onchain_balance_wei < estimated_fee_wei:
                estimated_fee = estimated_fee_wei / 10**18
                log(
                    "SKIP: insufficient onchain gas | rank_key=%s | onchain_dacc_balance=%.18f | estimated_fee=%.18f",
                    rank_key,
                    onchain_balance_wei / 10**18,
                    estimated_fee,
                )
                all_ok = False
                continue
            tx = function.build_transaction(
                {
                    "from": account.address,
                    "chainId": DAC_TESTNET_CHAIN_ID,
                    "nonce": nonce,
                    "gas": gas_limit,
                    "gasPrice": rank_gas_price,
                }
            )
            signed = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            tx_hash_hex = normalize_tx_hash(tx_hash.hex())
            log("Rank mint sent | rank_key=%s | rank_id=%s | tx_hash=%s", rank_key, rank_id, tx_hash_hex)

            try:
                receipt = w3.eth.wait_for_transaction_receipt(
                    tx_hash,
                    timeout=max(int(settings["poll_timeout_seconds"]), 120),
                )
            except Exception as exc:
                raise TxSubmittedError(tx_hash_hex, f"Rank mint submitted but receipt wait failed: {exc}") from exc
            log("Rank mint receipt | rank_key=%s | status=%s | block=%s", rank_key, getattr(receipt, "status", None), getattr(receipt, "blockNumber", None))
            if getattr(receipt, "status", 0) != 1:
                raise RuntimeError(f"Transaction reverted: {tx_hash_hex}")

            client.nft_confirm_mint(rank_key, tx_hash_hex)
            log("Rank mint confirmed | rank_key=%s | tx_hash=%s", rank_key, tx_hash_hex)
            current_profile = client.profile()
            try:
                onchain_balance_wei = w3.eth.get_balance(account.address)
            except Exception as exc:
                log_error("RANK MINT BALANCE REFRESH ERROR | rank_key=%s | %s", rank_key, exc)
        except ApiError as exc:
            all_ok = False
            log_error("RANK MINT API ERROR | rank_key=%s | status=%s | message=%s | payload=%s", rank_key, exc.status, exc, exc.payload)
        except TxSubmittedError as exc:
            all_ok = False
            log_error("RANK MINT SUBMITTED BUT RECEIPT FAILED | rank_key=%s | tx_hash=%s | %s", rank_key, exc.tx_hash, exc)
            try:
                client.nft_confirm_mint(rank_key, exc.tx_hash)
                log("Rank mint confirm after receipt failure | rank_key=%s | tx_hash=%s", rank_key, exc.tx_hash)
                current_profile = client.profile()
            except ApiError as api_exc:
                log_error(
                    "RANK MINT CONFIRM AFTER RECEIPT ERROR | rank_key=%s | status=%s | message=%s | payload=%s",
                    rank_key,
                    api_exc.status,
                    api_exc,
                    api_exc.payload,
                )
        except Exception as exc:
            all_ok = False
            log_error("RANK MINT ERROR | rank_key=%s | %s", rank_key, exc)

        if badge_index < len(rank_badges) - 1 and rank_mint_delay_max > 0:
            sleep_seconds = random.uniform(rank_mint_delay_min, rank_mint_delay_max)
            log("Sleeping between rank mints | seconds=%.2f", sleep_seconds)
            time.sleep(sleep_seconds)

    return all_ok, current_profile, True


def send_contract_tx(
    w3: Web3,
    account: Any,
    function: Any,
    settings: dict[str, Any],
    *,
    value_wei: int = 0,
    gas_limit_override: int | None = None,
    balance_check: bool = True,
) -> tuple[str, Any, int]:
    fixed_gas_price_gwei = parse_decimal(settings.get("rpc_fixed_gas_price_gwei", "0"))
    gas_price = gwei_to_wei(fixed_gas_price_gwei) if fixed_gas_price_gwei > 0 else w3.eth.gas_price
    if gas_limit_override and gas_limit_override > 0:
        gas_limit = int(gas_limit_override)
    else:
        tx_base = {
            "from": account.address,
            "value": value_wei,
        }
        gas_estimate = function.estimate_gas(tx_base)
        gas_limit = int(gas_estimate * 1.2) + 5000
    estimated_fee_wei = gas_limit * gas_price
    if balance_check:
        balance_wei = w3.eth.get_balance(account.address)
        if balance_wei < value_wei + estimated_fee_wei:
            raise RuntimeError(
                "Insufficient DACC for tx: "
                f"balance={wei_to_decimal(balance_wei)} required={wei_to_decimal(value_wei + estimated_fee_wei)}"
            )
    nonce = w3.eth.get_transaction_count(account.address, "pending")
    tx = function.build_transaction(
        {
            "from": account.address,
            "chainId": DAC_TESTNET_CHAIN_ID,
            "nonce": nonce,
            "gas": gas_limit,
            "gasPrice": gas_price,
            "value": value_wei,
        }
    )
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    tx_hash_hex = normalize_tx_hash(tx_hash.hex())
    try:
        receipt = w3.eth.wait_for_transaction_receipt(
            tx_hash,
            timeout=max(int(settings["poll_timeout_seconds"]), 120),
        )
    except Exception as exc:
        raise TxSubmittedError(tx_hash_hex, f"Transaction submitted but receipt wait failed: {exc}") from exc
    return tx_hash_hex, receipt, estimated_fee_wei


def generate_external_recipient(own_addresses: set[str], used_recipients: set[str]) -> str:
    while True:
        recipient = Account.create(str(random.random())).address
        recipient_key = recipient.lower()
        if recipient_key not in own_addresses and recipient_key not in used_recipients:
            used_recipients.add(recipient_key)
            return recipient


def send_native_transfer(
    w3: Web3,
    account: Any,
    to_address: str,
    amount_wei: int,
    settings: dict[str, Any],
) -> tuple[str, Any, int]:
    gas_price = w3.eth.gas_price
    gas_limit = 21_000
    estimated_fee_wei = gas_limit * gas_price
    balance_wei = w3.eth.get_balance(account.address)
    if balance_wei < amount_wei + estimated_fee_wei:
        raise RuntimeError(
            "Insufficient DACC for transfer: "
            f"balance={wei_to_decimal(balance_wei)} required={wei_to_decimal(amount_wei + estimated_fee_wei)}"
        )
    nonce = w3.eth.get_transaction_count(account.address, "pending")
    tx = {
        "from": account.address,
        "to": Web3.to_checksum_address(to_address),
        "chainId": DAC_TESTNET_CHAIN_ID,
        "nonce": nonce,
        "gas": gas_limit,
        "gasPrice": gas_price,
        "value": amount_wei,
    }
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    tx_hash_hex = normalize_tx_hash(tx_hash.hex())
    receipt = w3.eth.wait_for_transaction_receipt(
        tx_hash,
        timeout=max(int(settings["poll_timeout_seconds"]), 120),
    )
    return tx_hash_hex, receipt, estimated_fee_wei


def run_wallet_transactions_only(
    entry: WalletEntry,
    logger: logging.Logger,
    tx_options: dict[str, Any],
) -> bool:
    settings = load_settings()
    run_logger = create_run_logger(entry.index, entry.address)
    tx_count = max(int(tx_options.get("tx_count") or settings.get("transactions_per_wallet", 1)), 1)
    amount_min = parse_decimal(tx_options.get("amount_min", settings.get("transaction_amount_min_dacc", "0.000000000000000001")))
    amount_max = parse_decimal(tx_options.get("amount_max", settings.get("transaction_amount_max_dacc", "0.000000000000000003")))
    if amount_max < amount_min:
        amount_min, amount_max = amount_max, amount_min
    delay_min = float(settings.get("delay_between_transactions_min_seconds", 2))
    delay_max = float(settings.get("delay_between_transactions_max_seconds", 5))
    if delay_max < delay_min:
        delay_min, delay_max = delay_max, delay_min
    gas_reserve_wei = decimal_to_wei(parse_decimal(settings.get("transaction_gas_reserve_dacc", "0.00005")))
    own_addresses = tx_options.setdefault("own_addresses", set())
    used_recipients = tx_options.setdefault("used_recipients", set())

    def log(message: str, *args: Any) -> None:
        logger.info(message, *args)
        run_logger.info(message, *args)

    def log_error(message: str, *args: Any) -> None:
        logger.error(message, *args)
        run_logger.error(message, *args)

    log(
        "Wallet #%s | mode=transactions | tx_count=%s | amount=%s-%s DACC | address=%s | proxy=%s",
        entry.index,
        tx_count,
        amount_min,
        amount_max,
        entry.address,
        entry.proxy or "-",
    )

    client = DachainClient(
        base_url=str(settings["base_url"]),
        ref_code=str(settings["ref_code"]),
        proxy=entry.proxy,
        timeout=int(settings["request_timeout_seconds"]),
    )

    try:
        client.authenticate_wallet(entry.address)
    except ApiError as exc:
        log_error("TRANSACTION AUTH ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
    except Exception as exc:
        log_error("UNEXPECTED TRANSACTION AUTH ERROR | %s", exc)

    try:
        w3, rpc_proxy = get_connected_web3(entry, settings, log, log_error, "TRANSACTION")
        if not w3:
            return False
        log("TRANSACTION RPC OK | proxy=%s", rpc_proxy or "-")
        account = Account.from_key(entry.private_key if entry.private_key.startswith("0x") else f"0x{entry.private_key}")
    except Exception as exc:
        log_error("TRANSACTION SETUP ERROR | %s", exc)
        return False

    completed = 0
    for tx_index in range(1, tx_count + 1):
        try:
            balance_wei = w3.eth.get_balance(account.address)
            spendable_wei = max(balance_wei - gas_reserve_wei, 0)
            min_wei = decimal_to_wei(amount_min)
            max_wei = min(decimal_to_wei(amount_max), spendable_wei)
            if max_wei < min_wei or max_wei <= 0:
                log(
                    "SKIP: insufficient DACC for transfer | balance=%s DACC | reserve=%s DACC | min_amount=%s DACC",
                    format_dacc_wei(balance_wei),
                    format_dacc_wei(gas_reserve_wei),
                    amount_min,
                )
                break
            amount_wei = random.randint(min_wei, max_wei)
            recipient = generate_external_recipient(own_addresses, used_recipients)
            tx_hash, receipt, fee_wei = send_native_transfer(w3, account, recipient, amount_wei, settings)
            log(
                "TRANSFER SENT | tx=%s/%s | to=%s | amount=%s DACC | tx_hash=%s | fee_estimate=%s DACC",
                tx_index,
                tx_count,
                recipient,
                format_dacc_wei(amount_wei, places=9),
                tx_hash,
                format_dacc_wei(fee_wei, places=9),
            )
            if getattr(receipt, "status", 0) != 1:
                raise RuntimeError(f"Transfer transaction reverted: {tx_hash}")
            completed += 1
            if tx_index < tx_count and delay_max > 0:
                sleep_seconds = random.uniform(delay_min, delay_max)
                log("Sleeping between transactions | seconds=%.2f", sleep_seconds)
                time.sleep(sleep_seconds)
        except Exception as exc:
            log_error("TRANSFER ERROR | tx=%s/%s | %s", tx_index, tx_count, exc)
            break

    if completed > 0:
        try:
            sync_payload = client.sync_chain()
            log("SYNC COMPLETED | payload=%s", sync_payload)
        except ApiError as exc:
            log_error("SYNC ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        except Exception as exc:
            log_error("UNEXPECTED SYNC ERROR | %s", exc)

    log("TRANSACTIONS RESULT | completed=%s/%s", completed, tx_count)
    return completed > 0


def run_wallet(entry: WalletEntry, logger: logging.Logger) -> bool:
    settings = load_settings()
    run_logger = create_run_logger(entry.index, entry.address)

    def log(message: str, *args: Any) -> None:
        logger.info(message, *args)
        run_logger.info(message, *args)

    def log_error(message: str, *args: Any) -> None:
        logger.error(message, *args)
        run_logger.error(message, *args)

    log("Wallet #%s | address=%s | proxy=%s", entry.index, entry.address, entry.proxy or "-")

    client = DachainClient(
        base_url=str(settings["base_url"]),
        ref_code=str(settings["ref_code"]),
        proxy=entry.proxy,
        timeout=int(settings["request_timeout_seconds"]),
    )

    try:
        auth = client.authenticate_wallet(entry.address)
        profile = client.profile()
        log("Auth OK | created=%s | qe_balance=%s | dacc_balance=%s", auth.created, profile.get("qe_balance"), profile.get("dacc_balance"))
        log(
            "Profile | faucet_available=%s | faucet_seconds_left=%s | x_linked=%s | discord_linked=%s",
            profile.get("faucet_available"),
            profile.get("faucet_seconds_left"),
            profile.get("x_linked"),
            profile.get("discord_linked"),
        )
    except ApiError as exc:
        log_error("AUTH ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        return False
    except Exception as exc:
        log_error("UNEXPECTED AUTH ERROR | %s", exc)
        return False

    profile = claim_early_badge(client, profile, log, log_error)
    log("Badge step completed for wallet.")

    faucet_ok = False
    try:
        claim = client.claim_faucet()
        dispense_id = claim.get("dispense_id")
        if not dispense_id:
            log_error("FAUCET ERROR | unexpected response=%s", claim)
        else:
            log("Faucet accepted | dispense_id=%s", dispense_id)
            result = client.poll_dispense(
                dispense_id,
                timeout_seconds=int(settings["poll_timeout_seconds"]),
                interval_seconds=float(settings["poll_interval_seconds"]),
            )
            status = result.get("status")
            if status == "success":
                log("FAUCET SUCCESS | final_status=%s | payload=%s", status, result)
                faucet_ok = True
            else:
                log_error("FAUCET FAILED | final_status=%s | payload=%s", status, result)
    except ApiError as exc:
        log_error("FAUCET ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
    except Exception as exc:
        log_error("UNEXPECTED FAUCET ERROR | %s", exc)

    rank_ok, profile, rank_attempted = mint_rank_badges(client, entry, profile, settings, log, log_error)
    final_ok = faucet_ok or (rank_attempted and rank_ok)
    log(
        "WALLET RESULT | faucet_ok=%s | rank_attempted=%s | rank_ok=%s | final_ok=%s",
        faucet_ok,
        rank_attempted,
        rank_ok,
        final_ok,
    )
    return final_ok


def run_wallet_faucet_only(entry: WalletEntry, logger: logging.Logger) -> bool:
    settings = load_settings()
    run_logger = create_run_logger(entry.index, entry.address)
    delay_between_faucet_requests_min = float(settings.get("delay_between_faucet_requests_min_seconds", 2))
    delay_between_faucet_requests_max = float(settings.get("delay_between_faucet_requests_max_seconds", 5))
    faucet_busy_retry_count = max(int(settings.get("faucet_busy_retry_count", 2)), 0)
    faucet_busy_retry_delay_min = float(settings.get("faucet_busy_retry_delay_min_seconds", 20))
    faucet_busy_retry_delay_max = float(settings.get("faucet_busy_retry_delay_max_seconds", 35))
    faucet_auth_retry_count = max(int(settings.get("faucet_auth_retry_count", 5)), 0)
    faucet_auth_retry_delay_min = float(settings.get("faucet_auth_retry_delay_min_seconds", 20))
    faucet_auth_retry_delay_max = float(settings.get("faucet_auth_retry_delay_max_seconds", 40))
    if delay_between_faucet_requests_max < delay_between_faucet_requests_min:
        delay_between_faucet_requests_min, delay_between_faucet_requests_max = (
            delay_between_faucet_requests_max,
            delay_between_faucet_requests_min,
        )
    if faucet_busy_retry_delay_max < faucet_busy_retry_delay_min:
        faucet_busy_retry_delay_min, faucet_busy_retry_delay_max = (
            faucet_busy_retry_delay_max,
            faucet_busy_retry_delay_min,
        )
    if faucet_auth_retry_delay_max < faucet_auth_retry_delay_min:
        faucet_auth_retry_delay_min, faucet_auth_retry_delay_max = (
            faucet_auth_retry_delay_max,
            faucet_auth_retry_delay_min,
        )

    def log(message: str, *args: Any) -> None:
        logger.info(message, *args)
        run_logger.info(message, *args)

    def log_error(message: str, *args: Any) -> None:
        logger.error(message, *args)
        run_logger.error(message, *args)

    def sleep_range(min_seconds: float, max_seconds: float, message: str) -> None:
        if max_seconds <= 0:
            return
        sleep_seconds = random.uniform(min_seconds, max_seconds)
        log(message, sleep_seconds)
        time.sleep(sleep_seconds)

    def auth_call_with_retry(callback: Any, action_name: str) -> Any:
        attempt = 0
        while True:
            try:
                return callback()
            except ApiError as exc:
                attempt += 1
                if not is_retryable_api_error(exc) or attempt > faucet_auth_retry_count:
                    raise
                log_error(
                    "%s TEMP SERVER ERROR | attempt=%s/%s | status=%s | message=%s",
                    action_name,
                    attempt,
                    faucet_auth_retry_count,
                    exc.status,
                    exc,
                )
                sleep_range(
                    faucet_auth_retry_delay_min,
                    faucet_auth_retry_delay_max,
                    f"Sleeping before retrying {action_name} | seconds=%.2f",
                )
            except Exception as exc:
                attempt += 1
                if attempt > faucet_auth_retry_count:
                    raise
                log_error("%s NETWORK ERROR | attempt=%s/%s | %s", action_name, attempt, faucet_auth_retry_count, exc)
                sleep_range(
                    faucet_auth_retry_delay_min,
                    faucet_auth_retry_delay_max,
                    f"Sleeping before retrying {action_name} | seconds=%.2f",
                )

    log("Wallet #%s | mode=faucet | address=%s | proxy=%s", entry.index, entry.address, entry.proxy or "-")

    client = DachainClient(
        base_url=str(settings["base_url"]),
        ref_code=str(settings["ref_code"]),
        proxy=entry.proxy,
        timeout=int(settings["request_timeout_seconds"]),
    )

    try:
        auth = auth_call_with_retry(lambda: client.authenticate_wallet(entry.address), "AUTH")
        sleep_range(
            delay_between_faucet_requests_min,
            delay_between_faucet_requests_max,
            "Sleeping between faucet requests | seconds=%.2f",
        )
        profile = auth_call_with_retry(client.profile, "PROFILE")
        log("Auth OK | created=%s | qe_balance=%s | dacc_balance=%s", auth.created, profile.get("qe_balance"), profile.get("dacc_balance"))
        log(
            "Profile | faucet_available=%s | faucet_seconds_left=%s | x_linked=%s | discord_linked=%s",
            profile.get("faucet_available"),
            profile.get("faucet_seconds_left"),
            profile.get("x_linked"),
            profile.get("discord_linked"),
        )
    except ApiError as exc:
        log_error("AUTH ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        return False
    except Exception as exc:
        log_error("UNEXPECTED AUTH ERROR | %s", exc)
        return False

    attempt = 0
    while True:
        try:
            claim = client.claim_faucet()
            dispense_id = claim.get("dispense_id")
            if not dispense_id:
                log_error("FAUCET ERROR | unexpected response=%s", claim)
                return False

            log("Faucet accepted | dispense_id=%s", dispense_id)
            result = client.poll_dispense(
                dispense_id,
                timeout_seconds=int(settings.get("faucet_poll_timeout_seconds", settings["poll_timeout_seconds"])),
                interval_seconds=float(settings["poll_interval_seconds"]),
            )
            status = result.get("status")
            if status == "success":
                log("FAUCET SUCCESS | final_status=%s | payload=%s", status, result)
                return True

            log_error("FAUCET FAILED | final_status=%s | payload=%s", status, result)
            return False
        except ApiError as exc:
            if exc.status == 503 and isinstance(exc.payload, dict) and exc.payload.get("code") == "backlog_full" and attempt < faucet_busy_retry_count:
                attempt += 1
                log_error("FAUCET BUSY | retry=%s/%s | message=%s", attempt, faucet_busy_retry_count, exc)
                sleep_range(
                    faucet_busy_retry_delay_min,
                    faucet_busy_retry_delay_max,
                    "Sleeping before faucet retry | seconds=%.2f",
                )
                continue
            log_error("FAUCET ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
            return False
        except Exception as exc:
            log_error("UNEXPECTED FAUCET ERROR | %s", exc)
            return False


def run_wallet_badges_only(entry: WalletEntry, logger: logging.Logger) -> bool:
    settings = load_settings()
    run_logger = create_run_logger(entry.index, entry.address)

    def log(message: str, *args: Any) -> None:
        logger.info(message, *args)
        run_logger.info(message, *args)

    def log_error(message: str, *args: Any) -> None:
        logger.error(message, *args)
        run_logger.error(message, *args)

    log("Wallet #%s | mode=badges | address=%s | proxy=%s", entry.index, entry.address, entry.proxy or "-")

    client = DachainClient(
        base_url=str(settings["base_url"]),
        ref_code=str(settings["ref_code"]),
        proxy=entry.proxy,
        timeout=int(settings["request_timeout_seconds"]),
    )

    try:
        auth = client.authenticate_wallet(entry.address)
        profile = client.profile()
        log("Auth OK | created=%s | qe_balance=%s | dacc_balance=%s", auth.created, profile.get("qe_balance"), profile.get("dacc_balance"))
    except ApiError as exc:
        log_error("AUTH ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        return False
    except Exception as exc:
        log_error("UNEXPECTED AUTH ERROR | %s", exc)
        return False

    before_keys = badge_keys(profile)
    before_unminted_ranks = {
        str(badge.get("badge__key", ""))
        for badge in profile.get("badges", [])
        if str(badge.get("badge__key", "")).startswith("rank_") and not badge.get("nft_tx_hash")
    }

    profile = claim_flash_badge(client, profile, log, log_error)
    profile = claim_available_badge(client, profile, log, log_error)
    rank_ok, profile, rank_attempted = mint_rank_badges(client, entry, profile, settings, log, log_error)

    after_keys = badge_keys(profile)
    after_unminted_ranks = {
        str(badge.get("badge__key", ""))
        for badge in profile.get("badges", [])
        if str(badge.get("badge__key", "")).startswith("rank_") and not badge.get("nft_tx_hash")
    }

    new_badges = sorted(after_keys - before_keys)
    minted_ranks = sorted(before_unminted_ranks - after_unminted_ranks)
    badge_ok = bool(new_badges or minted_ranks or (rank_attempted and rank_ok))

    log(
        "BADGES RESULT | new_badges=%s | minted_ranks=%s | rank_attempted=%s | rank_ok=%s | final_ok=%s",
        new_badges,
        minted_ranks,
        rank_attempted,
        rank_ok,
        badge_ok,
    )
    return badge_ok


def run_wallet_exchange_only(
    entry: WalletEntry,
    logger: logging.Logger,
    exchange_options: dict[str, Any],
) -> bool:
    settings = load_settings()
    run_logger = create_run_logger(entry.index, entry.address)
    operation = str(exchange_options.get("operation") or settings.get("exchange_operation", "burn")).strip().lower()
    tx_count = max(int(exchange_options.get("tx_count") or settings.get("exchange_transactions_count", 1)), 1)
    percent_min, percent_max = normalize_percent_range(
        exchange_options.get("percent_min", settings.get("exchange_percent_min", 5)),
        exchange_options.get("percent_max", settings.get("exchange_percent_max", 10)),
    )
    delay_min = float(settings.get("delay_between_exchange_txs_min_seconds", 2))
    delay_max = float(settings.get("delay_between_exchange_txs_max_seconds", 5))
    if delay_max < delay_min:
        delay_min, delay_max = delay_max, delay_min
    gas_reserve_wei = decimal_to_wei(parse_decimal(settings.get("exchange_gas_reserve_dacc", "0.00005")))

    def log(message: str, *args: Any) -> None:
        logger.info(message, *args)
        run_logger.info(message, *args)

    def log_error(message: str, *args: Any) -> None:
        logger.error(message, *args)
        run_logger.error(message, *args)

    def sleep_between_exchange_txs(tx_index: int) -> None:
        if tx_index >= tx_count or delay_max <= 0:
            return
        sleep_seconds = random.uniform(delay_min, delay_max)
        log("Sleeping between exchange txs | seconds=%.2f", sleep_seconds)
        time.sleep(sleep_seconds)

    log(
        "Wallet #%s | mode=exchange | operation=%s | percent=%s-%s | tx_count=%s | address=%s | proxy=%s",
        entry.index,
        operation,
        percent_min,
        percent_max,
        tx_count,
        entry.address,
        entry.proxy or "-",
    )

    client = DachainClient(
        base_url=str(settings["base_url"]),
        ref_code=str(settings["ref_code"]),
        proxy=entry.proxy,
        timeout=int(settings["request_timeout_seconds"]),
    )

    try:
        auth = client.authenticate_wallet(entry.address)
        profile = client.profile()
        log("Auth OK | created=%s | qe_balance=%s | dacc_balance=%s", auth.created, profile.get("qe_balance"), profile.get("dacc_balance"))
    except ApiError as exc:
        log_error("EXCHANGE AUTH ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        return False
    except Exception as exc:
        log_error("UNEXPECTED EXCHANGE AUTH ERROR | %s", exc)
        return False

    try:
        w3, rpc_proxy = get_connected_web3(entry, settings, log, log_error, "EXCHANGE")
        if not w3:
            return False
        log("EXCHANGE RPC OK | proxy=%s", rpc_proxy or "-")
        account = Account.from_key(entry.private_key if entry.private_key.startswith("0x") else f"0x{entry.private_key}")
        contract = w3.eth.contract(address=Web3.to_checksum_address(QE_POOL_CONTRACT), abi=QE_POOL_ABI)
    except Exception as exc:
        log_error("EXCHANGE SETUP ERROR | %s", exc)
        return False

    profile_balance_wei = decimal_to_wei(parse_decimal(str(profile.get("dacc_balance", "0") or "0")))
    burn_gas_limit = int(settings.get("exchange_burn_gas_limit", 350000))
    stake_gas_limit = int(settings.get("exchange_stake_gas_limit", 350000))
    withdraw_gas_limit = int(settings.get("exchange_withdraw_gas_limit", 350000))
    claim_gas_limit = int(settings.get("exchange_claim_gas_limit", 220000))
    exchange_rpc_tx_retry_count = max(int(settings.get("exchange_rpc_tx_retry_count", 3)), 1)
    bad_rpc_proxies: set[str] = set()

    completed = 0
    for tx_index in range(1, tx_count + 1):
        try:
            if operation == "burn":
                try:
                    balance_wei = w3.eth.get_balance(account.address)
                    log("Exchange status | tx=%s/%s | balance=%s DACC", tx_index, tx_count, format_dacc_wei(balance_wei))
                except Exception as exc:
                    balance_wei = profile_balance_wei
                    log_error(
                        "EXCHANGE BALANCE RPC ERROR | using profile balance | tx=%s/%s | profile_balance=%s DACC | %s",
                        tx_index,
                        tx_count,
                        format_dacc_wei(balance_wei),
                        exc,
                    )
                spendable_wei = max(balance_wei - gas_reserve_wei, 0)
                amount_wei, chosen_percent = random_percent_wei(spendable_wei, percent_min, percent_max)
                if amount_wei <= 0:
                    log("SKIP: insufficient DACC for burn | balance=%s DACC", format_dacc_wei(balance_wei))
                    break
                function = contract.functions.burnForQE()
                tx_hash, receipt, fee_wei = send_contract_tx(
                    w3,
                    account,
                    function,
                    settings,
                    value_wei=amount_wei,
                    gas_limit_override=burn_gas_limit,
                    balance_check=False,
                )
                log(
                    "BURN SENT | amount=%s DACC | percent=%.4f | tx_hash=%s | fee_estimate=%s DACC",
                    format_dacc_wei(amount_wei),
                    float(chosen_percent),
                    tx_hash,
                    format_dacc_wei(fee_wei),
                )
                if getattr(receipt, "status", 0) != 1:
                    raise RuntimeError(f"Burn transaction reverted: {tx_hash}")
                confirm = client.exchange_confirm_burn(tx_hash)
                log(
                    "BURN CONFIRMED | tx_hash=%s | qe_credited=%s | payload=%s",
                    tx_hash,
                    confirm.get("qe_credited", int(wei_to_decimal(amount_wei) * QE_PER_DACC)),
                    confirm,
                )

            elif operation in {"deposit", "stake"}:
                try:
                    balance_wei = w3.eth.get_balance(account.address)
                    log("Exchange status | tx=%s/%s | balance=%s DACC", tx_index, tx_count, format_dacc_wei(balance_wei))
                except Exception as exc:
                    balance_wei = profile_balance_wei
                    log_error(
                        "EXCHANGE BALANCE RPC ERROR | using profile balance | tx=%s/%s | profile_balance=%s DACC | %s",
                        tx_index,
                        tx_count,
                        format_dacc_wei(balance_wei),
                        exc,
                    )
                spendable_wei = max(balance_wei - gas_reserve_wei, 0)
                amount_wei, chosen_percent = random_percent_wei(spendable_wei, percent_min, percent_max)
                if amount_wei <= 0:
                    log("SKIP: insufficient DACC for deposit | balance=%s DACC", format_dacc_wei(balance_wei))
                    break
                function = contract.functions.stake()
                tx_hash, receipt, fee_wei = send_contract_tx(
                    w3,
                    account,
                    function,
                    settings,
                    value_wei=amount_wei,
                    gas_limit_override=stake_gas_limit,
                    balance_check=False,
                )
                log(
                    "DEPOSIT SENT | amount=%s DACC | percent=%.4f | tx_hash=%s | fee_estimate=%s DACC",
                    format_dacc_wei(amount_wei),
                    float(chosen_percent),
                    tx_hash,
                    format_dacc_wei(fee_wei),
                )
                if getattr(receipt, "status", 0) != 1:
                    raise RuntimeError(f"Deposit transaction reverted: {tx_hash}")
                try:
                    confirm = client.exchange_confirm_stake(tx_hash)
                    log("DEPOSIT CONFIRMED | tx_hash=%s | payload=%s", tx_hash, confirm)
                except ApiError as exc:
                    log_error("DEPOSIT CONFIRM API ERROR | tx_hash=%s | status=%s | message=%s | payload=%s", tx_hash, exc.status, exc, exc.payload)

            elif operation in {"withdraw", "unstake"}:
                lp_position = contract.functions.lps(account.address).call()
                staked_wei = int(lp_position[0] if isinstance(lp_position, (list, tuple)) else getattr(lp_position, "staked", 0))
                log("Exchange status | tx=%s/%s | staked=%s DACC", tx_index, tx_count, format_dacc_wei(staked_wei))
                amount_wei, chosen_percent = random_percent_wei(staked_wei, percent_min, percent_max)
                if amount_wei <= 0:
                    log("SKIP: no staked DACC for withdraw | staked=%s DACC", format_dacc_wei(staked_wei))
                    break
                function = contract.functions.unstake(amount_wei)
                tx_hash, receipt, fee_wei = send_contract_tx(
                    w3,
                    account,
                    function,
                    settings,
                    gas_limit_override=withdraw_gas_limit,
                    balance_check=False,
                )
                log(
                    "WITHDRAW SENT | amount=%s DACC | percent=%.4f | tx_hash=%s | fee_estimate=%s DACC",
                    format_dacc_wei(amount_wei),
                    float(chosen_percent),
                    tx_hash,
                    format_dacc_wei(fee_wei),
                )
                if getattr(receipt, "status", 0) != 1:
                    raise RuntimeError(f"Withdraw transaction reverted: {tx_hash}")
                log("WITHDRAW CONFIRMED | tx_hash=%s", tx_hash)

            elif operation == "claim":
                pending_fees_wei: int | None
                try:
                    pending_fees_wei = int(contract.functions.pendingFees(account.address).call())
                    log("Exchange status | tx=%s/%s | pending_fees=%s DACC", tx_index, tx_count, format_dacc_wei(pending_fees_wei))
                except Exception as exc:
                    pending_fees_wei = None
                    log_error("CLAIM FEES READ ERROR | reconnecting RPC | tx=%s/%s | proxy=%s | %s", tx_index, tx_count, rpc_proxy or "-", exc)
                    if rpc_proxy:
                        bad_rpc_proxies.add(rpc_proxy)
                    for retry_index in range(1, exchange_rpc_tx_retry_count + 1):
                        retry_w3, retry_proxy = get_connected_web3(entry, settings, log, log_error, "EXCHANGE CLAIM", bad_rpc_proxies)
                        if not retry_w3:
                            log_error("CLAIM FEES READ RETRY ERROR | no replacement RPC proxy; sending claim anyway")
                            break
                        w3 = retry_w3
                        rpc_proxy = retry_proxy
                        contract = w3.eth.contract(address=Web3.to_checksum_address(QE_POOL_CONTRACT), abi=QE_POOL_ABI)
                        log("EXCHANGE CLAIM RPC RETRY OK | attempt=%s/%s | proxy=%s", retry_index, exchange_rpc_tx_retry_count, rpc_proxy or "-")
                        try:
                            pending_fees_wei = int(contract.functions.pendingFees(account.address).call())
                            log("Exchange status | tx=%s/%s | pending_fees=%s DACC", tx_index, tx_count, format_dacc_wei(pending_fees_wei))
                            break
                        except Exception as retry_exc:
                            log_error("CLAIM FEES READ RETRY ERROR | tx=%s/%s | proxy=%s | %s", tx_index, tx_count, rpc_proxy or "-", retry_exc)
                            if rpc_proxy:
                                bad_rpc_proxies.add(rpc_proxy)
                    if pending_fees_wei is None:
                        log_error("CLAIM FEES READ RETRIES EXHAUSTED | sending claim anyway")
                if pending_fees_wei is not None and pending_fees_wei <= 0:
                    log("SKIP: no pending fees to claim | pending_fees=0 DACC")
                    break
                claimable_text = format_dacc_wei(pending_fees_wei) if pending_fees_wei is not None else "unknown"
                log("CLAIM FEES START | claimable=%s DACC", claimable_text)
                function = contract.functions.claimFees()
                tx_hash, receipt, fee_wei = send_contract_tx(
                    w3,
                    account,
                    function,
                    settings,
                    gas_limit_override=claim_gas_limit,
                    balance_check=False,
                )
                log(
                    "CLAIM SENT | pending_fees=%s DACC | tx_hash=%s | fee_estimate=%s DACC",
                    claimable_text,
                    tx_hash,
                    format_dacc_wei(fee_wei),
                )
                if getattr(receipt, "status", 0) != 1:
                    raise RuntimeError(f"Claim transaction reverted: {tx_hash}")
                try:
                    pending_after_text = f"{format_dacc_wei(int(contract.functions.pendingFees(account.address).call()))} DACC"
                except Exception as exc:
                    pending_after_text = f"unknown ({exc})"
                log(
                    "CLAIM FEES CONFIRMED | tx_hash=%s | claimed=%s DACC | pending_after=%s",
                    tx_hash,
                    claimable_text,
                    pending_after_text,
                )

            else:
                log_error("EXCHANGE ERROR | unknown operation=%s", operation)
                return False

            completed += 1
            sleep_between_exchange_txs(tx_index)
        except TxSubmittedError as exc:
            log_error(
                "EXCHANGE TX SUBMITTED BUT RECEIPT FAILED | operation=%s | tx=%s/%s | tx_hash=%s | %s",
                operation,
                tx_index,
                tx_count,
                exc.tx_hash,
                exc,
            )
            completed += 1
            break
        except Exception as exc:
            log_error("EXCHANGE TX ERROR | operation=%s | tx=%s/%s | %s", operation, tx_index, tx_count, exc)
            break

    log("EXCHANGE RESULT | operation=%s | completed=%s/%s", operation, completed, tx_count)
    return completed > 0


def run_wallet_crates_only(entry: WalletEntry, logger: logging.Logger) -> bool:
    settings = load_settings()
    run_logger = create_run_logger(entry.index, entry.address)
    delay_between_crates_min = float(settings.get("delay_between_crates_min_seconds", 1))
    delay_between_crates_max = float(settings.get("delay_between_crates_max_seconds", 3))
    delay_between_crate_requests_min = float(settings.get("delay_between_crate_requests_min_seconds", 1))
    delay_between_crate_requests_max = float(settings.get("delay_between_crate_requests_max_seconds", 2))
    crate_retry_count = max(int(settings.get("crate_retry_count", 2)), 0)
    crate_retry_backoff_min = float(settings.get("crate_retry_backoff_min_seconds", 4))
    crate_retry_backoff_max = float(settings.get("crate_retry_backoff_max_seconds", 8))
    if delay_between_crates_max < delay_between_crates_min:
        delay_between_crates_min, delay_between_crates_max = delay_between_crates_max, delay_between_crates_min
    if delay_between_crate_requests_max < delay_between_crate_requests_min:
        delay_between_crate_requests_min, delay_between_crate_requests_max = (
            delay_between_crate_requests_max,
            delay_between_crate_requests_min,
        )
    if crate_retry_backoff_max < crate_retry_backoff_min:
        crate_retry_backoff_min, crate_retry_backoff_max = crate_retry_backoff_max, crate_retry_backoff_min

    def log(message: str, *args: Any) -> None:
        logger.info(message, *args)
        run_logger.info(message, *args)

    def log_error(message: str, *args: Any) -> None:
        logger.error(message, *args)
        run_logger.error(message, *args)

    def sleep_range(min_seconds: float, max_seconds: float, message: str) -> None:
        if max_seconds <= 0:
            return
        sleep_seconds = random.uniform(min_seconds, max_seconds)
        log(message, sleep_seconds)
        time.sleep(sleep_seconds)

    def call_with_retry(callback: Any, action_name: str) -> Any:
        attempt = 0
        while True:
            try:
                return callback()
            except ApiError as exc:
                attempt += 1
                if not is_retryable_api_error(exc) or attempt > crate_retry_count:
                    raise
                log_error(
                    "%s TEMP SERVER ERROR | attempt=%s/%s | status=%s | message=%s",
                    action_name,
                    attempt,
                    crate_retry_count,
                    exc.status,
                    exc,
                )
                sleep_range(
                    crate_retry_backoff_min,
                    crate_retry_backoff_max,
                    f"Sleeping before retrying {action_name} | seconds=%.2f",
                )
            except Exception as exc:
                attempt += 1
                if attempt > crate_retry_count:
                    raise
                log_error("%s NETWORK ERROR | attempt=%s/%s | %s", action_name, attempt, crate_retry_count, exc)
                sleep_range(
                    crate_retry_backoff_min,
                    crate_retry_backoff_max,
                    f"Sleeping before retrying {action_name} | seconds=%.2f",
                )

    log("Wallet #%s | mode=crates | address=%s | proxy=%s", entry.index, entry.address, entry.proxy or "-")

    client = DachainClient(
        base_url=str(settings["base_url"]),
        ref_code=str(settings["ref_code"]),
        proxy=entry.proxy,
        timeout=int(settings["request_timeout_seconds"]),
    )

    try:
        auth = call_with_retry(lambda: client.authenticate_wallet(entry.address), "CRATE AUTH")
        sleep_range(
            delay_between_crate_requests_min,
            delay_between_crate_requests_max,
            "Sleeping between crate requests | seconds=%.2f",
        )
        profile = call_with_retry(client.profile, "CRATE PROFILE")
        sleep_range(
            delay_between_crate_requests_min,
            delay_between_crate_requests_max,
            "Sleeping between crate requests | seconds=%.2f",
        )
        history = call_with_retry(client.crate_history, "CRATE HISTORY")
        qe_balance = float(profile.get("qe_balance") or 0) + float(profile.get("waitlist_qe") or 0)
        opens_today = int(history.get("opens_today") or 0)
        daily_open_limit = int(history.get("daily_open_limit") or 5)
        qe_today = int(history.get("qe_today") or 0)
        daily_qe_cap = int(history.get("daily_qe_cap") or 1000)
        log(
            "Crate status | qe_balance=%s | opens_today=%s/%s | qe_today=%s/%s",
            qe_balance,
            opens_today,
            daily_open_limit,
            qe_today,
            daily_qe_cap,
        )
    except ApiError as exc:
        log_error("CRATE AUTH ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
        return False
    except Exception as exc:
        log_error("UNEXPECTED CRATE AUTH ERROR | %s", exc)
        return False

    opened = 0
    while True:
        try:
            profile = call_with_retry(client.profile, "CRATE PROFILE")
            sleep_range(
                delay_between_crate_requests_min,
                delay_between_crate_requests_max,
                "Sleeping between crate requests | seconds=%.2f",
            )
            history = call_with_retry(client.crate_history, "CRATE HISTORY")
            qe_balance = float(profile.get("qe_balance") or 0) + float(profile.get("waitlist_qe") or 0)
            opens_today = int(history.get("opens_today") or 0)
            daily_open_limit = int(history.get("daily_open_limit") or 5)

            if qe_balance < CRATE_QE_COST:
                log("SKIP: insufficient QE | qe_balance=%s | required=%s", qe_balance, CRATE_QE_COST)
                break
            if opens_today >= daily_open_limit:
                log("SKIP: daily crate limit reached | opens_today=%s | limit=%s", opens_today, daily_open_limit)
                break

            if opened > 0:
                sleep_seconds = random.uniform(delay_between_crates_min, delay_between_crates_max)
                log("Sleeping before next crate open | seconds=%.2f", sleep_seconds)
                time.sleep(sleep_seconds)

            result = call_with_retry(client.crate_open, "CRATE OPEN")
            reward = result.get("reward") or {}
            label = reward.get("label") or reward.get("amount") or reward
            reward_type = reward.get("type")
            sleep_range(
                delay_between_crate_requests_min,
                delay_between_crate_requests_max,
                "Sleeping between crate requests | seconds=%.2f",
            )
            updated_profile = call_with_retry(client.profile, "CRATE PROFILE")
            updated_qe_balance = float(updated_profile.get("qe_balance") or 0) + float(updated_profile.get("waitlist_qe") or 0)
            log(
                "CRATE OPENED | reward_type=%s | reward=%s | qe_capped=%s | qe_balance=%s",
                reward_type,
                reward,
                result.get("qe_capped"),
                updated_qe_balance,
            )
            opened += 1

            tx_hash = reward.get("tx_hash")
            if reward_type == "dacc" and isinstance(tx_hash, str) and tx_hash.startswith("pending:"):
                dispense_id = tx_hash.replace("pending:", "", 1)
                if not bool(settings.get("crate_poll_dacc_status", False)):
                    log("CRATE DACC PENDING | dispense_id=%s | status_poll=disabled", dispense_id)
                else:
                    try:
                        dispense_result = client.poll_dispense(
                            dispense_id,
                            timeout_seconds=int(settings.get("crate_poll_timeout_seconds", settings["poll_timeout_seconds"])),
                            interval_seconds=float(settings["poll_interval_seconds"]),
                        )
                        log("CRATE DACC STATUS | dispense_id=%s | result=%s", dispense_id, dispense_result)
                    except Exception as exc:
                        log_error("CRATE DACC STATUS ERROR | dispense_id=%s | %s", dispense_id, exc)
        except ApiError as exc:
            log_error("CRATE OPEN ERROR | status=%s | message=%s | payload=%s", exc.status, exc, exc.payload)
            break
        except Exception as exc:
            log_error("UNEXPECTED CRATE OPEN ERROR | %s", exc)
            break

    log("CRATES RESULT | opened=%s", opened)
    return opened > 0


def run_all_wallets(logger: logging.Logger, mode: str, options: dict[str, Any] | None = None) -> int:
    entries = build_wallet_entries(logger)
    if not entries:
        logger.error("No valid wallets found.")
        return 1

    settings = load_settings()
    delay_between_wallets_min = float(settings.get("delay_between_wallets_min_seconds", 3))
    delay_between_wallets_max = float(settings.get("delay_between_wallets_max_seconds", 6))
    if delay_between_wallets_max < delay_between_wallets_min:
        delay_between_wallets_min, delay_between_wallets_max = delay_between_wallets_max, delay_between_wallets_min

    success = 0
    failed = 0

    logger.info("Starting run | version=%s | mode=%s | wallets=%s", RUNNER_VERSION, mode, len(entries))
    logger.info("Config files | keys=%s | proxies=%s | settings=%s", PRIVATE_KEYS_PATH, PROXIES_PATH, SETTINGS_PATH)

    if mode == "transactions":
        options = options or {}
        options.setdefault("own_addresses", {entry.address.lower() for entry in entries})
        options.setdefault("used_recipients", set())

    for entry in entries:
        print("-" * 72)
        if mode == "faucet":
            result = run_wallet_faucet_only(entry, logger)
        elif mode == "badges":
            result = run_wallet_badges_only(entry, logger)
        elif mode == "crates":
            result = run_wallet_crates_only(entry, logger)
        elif mode == "exchange":
            result = run_wallet_exchange_only(entry, logger, options or {})
        elif mode == "transactions":
            result = run_wallet_transactions_only(entry, logger, options or {})
        else:
            raise RuntimeError(f"Unknown mode: {mode}")
        if result:
            success += 1
        else:
            failed += 1
        if entry != entries[-1] and delay_between_wallets_max > 0:
            sleep_seconds = random.uniform(delay_between_wallets_min, delay_between_wallets_max)
            logger.info("Sleeping between wallets | seconds=%.2f", sleep_seconds)
            time.sleep(sleep_seconds)

    print("-" * 72)
    logger.info("Run completed | success=%s | failed=%s", success, failed)
    return 0 if success > 0 else 1


def prompt_exchange_options() -> dict[str, Any]:
    settings = load_settings()
    print()
    print("EXCHANGE")
    print("1. Burn DACC -> QE")
    print("2. Stake DACC")
    print("3. Withdraw staked DACC")
    print("4. Claim pending fees")
    operation_choice = input(f"Select operation [{settings.get('exchange_operation', 'burn')}]: ").strip().lower()
    operation_map = {
        "1": "burn",
        "burn": "burn",
        "2": "deposit",
        "deposit": "deposit",
        "stake": "deposit",
        "3": "withdraw",
        "withdraw": "withdraw",
        "unstake": "withdraw",
        "4": "claim",
        "claim": "claim",
        "fees": "claim",
        "claim_fees": "claim",
        "claim fees": "claim",
    }
    operation = operation_map.get(operation_choice, str(settings.get("exchange_operation", "burn")).lower())

    default_min = settings.get("exchange_percent_min", 5)
    default_max = settings.get("exchange_percent_max", 10)
    default_count = settings.get("exchange_transactions_count", 1)

    if operation == "claim":
        return {
            "operation": operation,
            "percent_min": parse_decimal(default_min, Decimal("5")),
            "percent_max": parse_decimal(default_max, Decimal("10")),
            "tx_count": 1,
        }

    percent_target = "DACC balance" if operation in {"burn", "deposit"} else "staked DACC"
    min_input = input(f"Min percent of {percent_target} [{default_min}]: ").strip()
    max_input = input(f"Max percent of {percent_target} [{default_max}]: ").strip()
    count_input = input(f"Transactions per wallet [{default_count}]: ").strip()

    percent_min = parse_decimal(min_input or default_min, parse_decimal(default_min, Decimal("5")))
    percent_max = parse_decimal(max_input or default_max, parse_decimal(default_max, Decimal("10")))
    try:
        tx_count = int(count_input or default_count)
    except ValueError:
        tx_count = int(default_count)

    return {
        "operation": operation,
        "percent_min": percent_min,
        "percent_max": percent_max,
        "tx_count": max(tx_count, 1),
    }


def prompt_transaction_options() -> dict[str, Any]:
    settings = load_settings()
    print()
    print("TRANSACTIONS")
    default_count = settings.get("transactions_per_wallet", 1)
    default_min = settings.get("transaction_amount_min_dacc", "0.000000000000000001")
    default_max = settings.get("transaction_amount_max_dacc", "0.000000000000000003")
    count_input = input(f"Transactions per wallet [{default_count}]: ").strip()
    min_input = input(f"Min amount DACC [{default_min}]: ").strip()
    max_input = input(f"Max amount DACC [{default_max}]: ").strip()

    try:
        tx_count = int(count_input or default_count)
    except ValueError:
        tx_count = int(default_count)

    return {
        "tx_count": max(tx_count, 1),
        "amount_min": parse_decimal(min_input or default_min, parse_decimal(default_min, Decimal("0.000000000000000001"))),
        "amount_max": parse_decimal(max_input or default_max, parse_decimal(default_max, Decimal("0.000000000000000003"))),
    }


def main() -> int:
    logger = setup_logging()
    ensure_layout()
    try:
        print()
        print("=" * 48)
        print("DACHAIN MENU")
        print("=" * 48)
        print("1. Faucet")
        print("2. Badges")
        print("3. Crates")
        print("4. Exchange")
        print("5. Transactions")
        print("0. Exit")
        print()
        choice = input("Select: ").strip()
        if choice == "1":
            return run_all_wallets(logger, "faucet")
        if choice == "2":
            return run_all_wallets(logger, "badges")
        if choice == "3":
            return run_all_wallets(logger, "crates")
        if choice == "4":
            return run_all_wallets(logger, "exchange", prompt_exchange_options())
        if choice == "5":
            return run_all_wallets(logger, "transactions", prompt_transaction_options())
        return 0
    except Exception as exc:
        logger.exception("FATAL ERROR | %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
