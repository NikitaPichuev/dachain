# DACHAIN Automation

Automation tool for DACHAIN Inception testnet.

## Features

- Faucet
- Badge claiming
- Rank NFT minting
- Quantum crate opening
- Proxy support
- Per-wallet logs

## Setup

1. Install Python 3.11+.
2. Run `install.bat`.
3. Add private keys to `config/private_keys.txt`, one key per line.
4. Add proxies to `config/proxies.txt`, one proxy per line. Leave the file empty if proxies are not needed.
5. Run `run.bat`.

## Menu

```text
1. Faucet
2. Badges
3. Crates
0. Exit
```

## Private Keys

Accepted formats:

```text
0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

## Proxies

Accepted formats:

```text
http://host:port
http://login:password@host:port
login:password@host:port
```

## Settings

Main settings are in `config/settings.json`.

The default referral link is:

```text
https://inception.dachain.io/?ref=DAC1392613
```

## Notes

- This public package does not include private keys, proxies, logs, or test placeholders.
- `logs/` is created automatically after launch.
- `config/private_keys.txt` and `config/proxies.txt` are intentionally empty.
