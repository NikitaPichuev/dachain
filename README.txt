DACHAIN public release

Contents:
- dachain_menu.py
- dachain_client.py
- dachain_testnet_faucet.py
- install.bat
- run.bat
- requirements.txt
- config\settings.json
- config\private_keys.txt
- config\proxies.txt

This package does not include:
- private keys
- proxies
- logs
- example placeholder lines in private_keys.txt and proxies.txt

Before launch:
1. Put one private key per line into config\private_keys.txt
2. Put one proxy per line into config\proxies.txt if proxies are needed
3. Optionally edit config\settings.json

Accepted private key formats:
- 0x...
- without 0x prefix

Accepted proxy formats:
- http://host:port
- http://login:password@host:port
- login:password@host:port

Launch:
1. Run install.bat
2. Run run.bat
3. In the menu choose:
   1 = Faucet
   2 = Badges
   3 = Crates

Notes:
- install.bat creates .venv and installs dependencies
- run.bat starts dachain_menu.py from the local virtual environment
- logs are created automatically in the logs folder after launch
- settings.json already contains the current base_url, ref_code and delays
