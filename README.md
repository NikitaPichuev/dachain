# DACHAIN Automation

Automation tool for DACHAIN Inception testnet.

## Features

- Faucet
- Badge claiming
- Flash badge claiming
- Rank NFT minting
- Quantum crate opening
- QE Pool exchange: burn, stake, withdraw, claim pending fees
- Native DACC transactions to unique external addresses
- HOOD PIX public mint on Robinhood Chain through OpenSea SeaDrop
- Native ETH deposit from Ethereum mainnet to Robinhood Chain
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
4. Exchange
5. Transactions
6. Hood Pix NFT
7. Deposit to Robinhood Chain
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
http://host:port@login:password
```

## Exchange

The Exchange menu supports:

```text
1. Burn DACC -> QE
2. Stake DACC
3. Withdraw staked DACC
4. Claim pending fees
```

Burn, stake, and withdraw use configurable min/max percentages and transaction count.
Claim pending fees runs one claim transaction per wallet when fees are available.

## Transactions

The Transactions menu sends small native DACC transfers to freshly generated unique
external addresses. Recipients are checked against loaded wallet addresses and are
not reused during the same run.

## HOOD PIX NFT

The HOOD PIX menu mints the public free phase through the SeaDrop contract on
Robinhood Chain. Paid minting is blocked by default with `hoodpix_allow_paid=false`.

Default contract settings:

```text
Collection: 0xb324301d3a3707de79e6dbab524e6c7fcc544ad2
SeaDrop:    0x00005EA00Ac477B1030CE78506496e8C2dE24bf5
Chain ID:   4663
```

## Robinhood Chain Deposit

The Deposit menu sends native ETH from Ethereum mainnet to Robinhood Chain through
the canonical Delayed Inbox contract. The L2 recipient is the same wallet address.

Default contract settings:

```text
Ethereum chain ID: 1
Delayed Inbox:     0x6bCBA7cD81a5f12c10ca1Bf9b36761CC382658E8
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
