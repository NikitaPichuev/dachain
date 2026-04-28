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
    "use_proxy_for_rpc": True,
    "delay_between_faucet_requests_min_seconds": 2,
    "delay_between_faucet_requests_max_seconds": 5,
    "faucet_busy_retry_count": 2,
    "faucet_busy_retry_delay_min_seconds": 20,
    "faucet_busy_retry_delay_max_seconds": 35,
    "delay_between_crates_min_seconds": 1,
    "delay_between_crates_max_seconds": 3,
    "delay_between_crate_requests_min_seconds": 1,
    "delay_between_crate_requests_max_seconds": 2,
    "crate_retry_count": 2,
    "crate_retry_backoff_min_seconds": 4,
    "crate_retry_backoff_max_seconds": 8,
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


def normalize_tx_hash(tx_hash: Any) -> str:
    value = str(tx_hash or "").strip()
    if value and not value.startswith("0x"):
        return f"0x{value}"
    return value


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
        w3 = get_web3(entry, settings)
        if not w3.is_connected():
            log_error("RANK MINT ERROR | RPC connection failed.")
            return False, profile, True
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(RANK_BADGE_CONTRACT),
            abi=RANK_BADGE_ABI,
        )
        account = Account.from_key(entry.private_key if entry.private_key.startswith("0x") else f"0x{entry.private_key}")
        onchain_balance_wei = w3.eth.get_balance(account.address)
        onchain_balance = onchain_balance_wei / 10**18
        log(
            "RPC OK | address=%s | onchain_dacc_balance=%.18f | profile_dacc_balance=%s",
            account.address,
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
    if rank_mint_delay_max < rank_mint_delay_min:
        rank_mint_delay_min, rank_mint_delay_max = rank_mint_delay_max, rank_mint_delay_min

    for badge_index, badge in enumerate(rank_badges):
        rank_key = badge.get("badge__key")
        try:
            signature_data = client.nft_claim_signature(rank_key)
            rank_id = int(signature_data["rank_id"])
            signature_hex = str(signature_data["signature"])
            signature_bytes = bytes.fromhex(signature_hex.removeprefix("0x"))

            already_minted = contract.functions.hasMinted(account.address, rank_id).call()
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
            gas_price = w3.eth.gas_price
            function = contract.functions.claimRank(rank_id, signature_bytes)
            gas_estimate = function.estimate_gas({"from": account.address})
            gas_limit = int(gas_estimate * 1.2) + 5000
            estimated_fee_wei = gas_limit * gas_price
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
                    "gasPrice": gas_price,
                }
            )
            signed = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            tx_hash_hex = normalize_tx_hash(tx_hash.hex())
            log("Rank mint sent | rank_key=%s | rank_id=%s | tx_hash=%s", rank_key, rank_id, tx_hash_hex)

            receipt = w3.eth.wait_for_transaction_receipt(
                tx_hash,
                timeout=max(int(settings["poll_timeout_seconds"]), 120),
            )
            log("Rank mint receipt | rank_key=%s | status=%s | block=%s", rank_key, getattr(receipt, "status", None), getattr(receipt, "blockNumber", None))
            if getattr(receipt, "status", 0) != 1:
                raise RuntimeError(f"Transaction reverted: {tx_hash_hex}")

            client.nft_confirm_mint(rank_key, tx_hash_hex)
            log("Rank mint confirmed | rank_key=%s | tx_hash=%s", rank_key, tx_hash_hex)
            current_profile = client.profile()
            onchain_balance_wei = w3.eth.get_balance(account.address)
        except ApiError as exc:
            all_ok = False
            log_error("RANK MINT API ERROR | rank_key=%s | status=%s | message=%s | payload=%s", rank_key, exc.status, exc, exc.payload)
        except Exception as exc:
            all_ok = False
            log_error("RANK MINT ERROR | rank_key=%s | %s", rank_key, exc)

        if badge_index < len(rank_badges) - 1 and rank_mint_delay_max > 0:
            sleep_seconds = random.uniform(rank_mint_delay_min, rank_mint_delay_max)
            log("Sleeping between rank mints | seconds=%.2f", sleep_seconds)
            time.sleep(sleep_seconds)

    return all_ok, current_profile, True


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

    log("Wallet #%s | mode=faucet | address=%s | proxy=%s", entry.index, entry.address, entry.proxy or "-")

    client = DachainClient(
        base_url=str(settings["base_url"]),
        ref_code=str(settings["ref_code"]),
        proxy=entry.proxy,
        timeout=int(settings["request_timeout_seconds"]),
    )

    try:
        auth = client.authenticate_wallet(entry.address)
        sleep_range(
            delay_between_faucet_requests_min,
            delay_between_faucet_requests_max,
            "Sleeping between faucet requests | seconds=%.2f",
        )
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

    before_keys = {str(badge.get("badge__key", "")) for badge in profile.get("badges", [])}
    before_unminted_ranks = {
        str(badge.get("badge__key", ""))
        for badge in profile.get("badges", [])
        if str(badge.get("badge__key", "")).startswith("rank_") and not badge.get("nft_tx_hash")
    }

    profile = claim_early_badge(client, profile, log, log_error)
    rank_ok, profile, rank_attempted = mint_rank_badges(client, entry, profile, settings, log, log_error)

    after_keys = {str(badge.get("badge__key", "")) for badge in profile.get("badges", [])}
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
            except ApiError:
                raise
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


def run_all_wallets(logger: logging.Logger, mode: str) -> int:
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

    for entry in entries:
        print("-" * 72)
        if mode == "faucet":
            result = run_wallet_faucet_only(entry, logger)
        elif mode == "badges":
            result = run_wallet_badges_only(entry, logger)
        elif mode == "crates":
            result = run_wallet_crates_only(entry, logger)
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
        print("0. Exit")
        print()
        choice = input("Select: ").strip()
        if choice == "1":
            return run_all_wallets(logger, "faucet")
        if choice == "2":
            return run_all_wallets(logger, "badges")
        if choice == "3":
            return run_all_wallets(logger, "crates")
        return 0
    except Exception as exc:
        logger.exception("FATAL ERROR | %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
