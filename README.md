# T212-Sync-buddy: Automated Trading 212 to Ghostfolio/Investbrain Sync

**T212-Sync-buddy** is a fully automated synchronization pipeline designed to safely extract your transaction history from the Trading 212 API and seamlessly import it into your Ghostfolio instance or Investbrain portfolio.

Built for reliability and data integrity, this service handles everything from initial data bootstraps and smart Trading 212 API rate-limit management to automated data normalization and strict cross-verification. Whether you manage a single portfolio or sync multiple distinct Trading 212 accounts simultaneously, this tool runs quietly in the background (via Docker or systemd) to keep your Ghostfolio dashboard or Investbrain portfolio perfectly in sync.

## Prerequisites

Make sure you have these dependencies installed:
```bash
# Check versions
python3 --version    # needs 3.10+
docker --version
jq --version
gawk --version

# Install missing dependencies if needed
sudo apt install docker.io jq gawk python3-full
```

For local test runs, the current Python test suite also uses `freezegun` in addition to the runtime requirements from `requirements.txt`.
---


If you like this project, consider supporting me:

<a href="https://www.buymeacoffee.com/dominatos"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" height="40"></a>

---
---

## 1. Project Structure

Your final directory structure should look like this:
```text
/path/to/T212-Sync-buddy/
├── .env.sample              # Template for API credentials and runtime options
├── .env                     # Your actual credentials and local overrides
├── .state/                  # Auto-created, stores per-account sync state and Yahoo cooldown marker
├── input/                   # Auto-created, fetched CSV exports land here
├── out/                     # Per-account JSON output and Investbrain success markers
├── cache/                   # Converter cache
├── temp/                    # Temporary working directory used during conversion/verification
├── t212_fetch.py            # Main Trading212 fetcher
├── run-all.sh               # Orchestrator for Ghostfolio conversion and Investbrain handoff
├── investbrain_import.py    # Investbrain integration with intraday workaround and symbol mapping
├── preprocess_isin.py       # Optional preprocessing for broker tickers before Ghostfolio conversion
├── Dockerfile               # Container image for the fetcher workflow
├── docker-compose.yml       # Containerized runner with Docker socket passthrough
├── tests/                   # Python and BATS test suite
├── systemdunits/            # Services and timers for automation
└── README.md                # This guide
```

---

<details>
<summary><h2>2. Script Explanations</h2></summary>

### t212_fetch.py
The core Python script for automated transaction retrieval from Trading 212 via API.
- **Bootstrapping**: On the first run, it detects your earliest transaction date and performs a full history fetch.
- **Incremental Updates**: Subsequent runs only fetch activity from the last 7 days.
- **Rate Limit Handling**: Automatically pauses and resumes to respect Trading 212 API limits.
- **Data Normalization**: Cleans and fixes T212 CSV structure (pads missing columns) for reliable conversion.
- **Flow**: After fetching, it automatically triggers `run-all.sh`.

### run-all.sh
The universal orchestrator for processing CSV exports.
- **Account Discovery**: Automatically finds all `PREFIX_*` accounts in `.env`.
- **Platform Handoff**: 
  - **Ghostfolio**: Launches the `dickwolff/export-to-ghostfolio` container.
  - **Investbrain**: Specifically optimized for accurate cost-basis and dividend reporting.
- **Smart Data Enrichment (Investbrain)**:
  - Automatically triggers `refresh:currency-data` before import to ensure accurate FX conversions.
  - Triggers `refresh:market-data` and `refresh:dividend-data` after import for instant portfolio updates.
- **Verification**: Cross-verifies Ghostfolio JSON output against the source CSV.
- **Organization**: Archives successful files to `input/done/` and quarantines failures.

### investbrain_import.py
A sophisticated importer designed to handle Investbrain-specific edge cases:
- **Intraday Workaround**: Automatically detects same-day trade conflicts and implements a sequential delay/shifting logic to bypass Investbrain validation bugs.
- **Symbol Mapping**: Auto-appends exchange suffixes (`.DE`, `.L`) to ensure compatibility with Yahoo Finance.
- **Error Handling**: Detailed reporting of HTTP 422 validation errors.

### systemdunits/t212-sync-buddy.service
A Linux systemd service unit that defines *how* to run the synchronization. It calls `t212_fetch.py` using the dedicated Python virtual environment.

### systemdunits/t212-sync-buddy.timer
A systemd timer that controls *when* the sync runs. In the current repository, it is configured to trigger the synchronization daily at **04:00 AM**.

</details>

---

## 3. Setup — Choose Your Method

<details>
<summary><h3>Option A: Python Virtual Environment (Bare Metal)</h3></summary>

Set up a dedicated Python environment for the fetcher script:

```bash
cd T212-Sync-buddy

# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate

# Install required Python packages
pip install requests python-dotenv
```

If you want to run the current Python unit tests locally as well:

```bash
pip install freezegun
```

</details>

<details>
<summary><h3>Option B: Docker Compose</h3></summary>

If you prefer running in Docker instead of a Python venv, use Docker Compose:

```bash
cd T212-Sync-buddy

# 1. Configure credentials (see Section 4 below)
cp .env.sample .env
nano .env

# 2. Set HOST_SCRIPTS_DIR to the absolute path of this directory on the HOST.
#    This is required because run-all.sh inside the container needs to pass
#    host paths when spawning the converter container via Docker socket.
echo 'HOST_SCRIPTS_DIR=/absolute/path/to/T212-Sync-buddy' >> .env

# 3. Build and run
docker compose build
docker compose run --rm t212-sync-buddy
```

> [!NOTE]
> The Docker method requires the Docker socket (`/var/run/docker.sock`) to be accessible.
> Your user must be in the `docker` group: `sudo usermod -aG docker $USER` (log out and back in).

> [!IMPORTANT]
> `HOST_SCRIPTS_DIR` must be an **absolute host path** (e.g., `/home/user/T212-Sync-buddy`).
> This is needed because `run-all.sh` inside the container spawns the converter container via Docker socket,
> and Docker volume mounts require host paths, not container paths.

</details>

---

<details>
<summary><h2>4. Configure Credentials</h2></summary>

Create your `.env` file from the sample:
```bash
cp .env.sample .env
nano .env
```

Add your Trading212 API credentials and Ghostfolio settings. For security, it is highly recommended to create API keys with "Read-only" permissions where possible.

```ini
# --- Trading212 API Keys ---
# Format: PREFIX_API_KEY and PREFIX_API_SECRET. For security, it is highly recommended to create API keys with "Read-only" permissions.
PREFIX1_API_KEY=your_prefix1_api_key_here
PREFIX1_API_SECRET=your_prefix1_api_secret_here
PREFIX1_GHOSTFOLIO_ACCOUNT_ID=your_ghostfolio_account_id
PREFIX1_INVESTBRAIN_PORTFOLIO_ID=your_investbrain_portfolio_id

PREFIX2_API_KEY=your_prefix2_api_key_here
PREFIX2_API_SECRET=your_prefix2_api_secret_here
PREFIX2_GHOSTFOLIO_ACCOUNT_ID=your_ghostfolio_account_id
PREFIX2_INVESTBRAIN_PORTFOLIO_ID=your_investbrain_portfolio_id

# --- Ghostfolio Settings ---
GHOSTFOLIO_URL=http://host.docker.internal:3333
GHOSTFOLIO_SECRET=your_ghostfolio_secret_here

# --- Investbrain Settings ---
# token can be created at http://your-investbrain-instance.com/user/api-tokens
INVESTBRAIN_URL=https://your-investbrain-instance.com
INVESTBRAIN_API_TOKEN=your_bearer_token_here

# --- Ghostfolio Runtime Options ---
GHOSTFOLIO_VALIDATE=true        # Validate activities against Ghostfolio before import
GHOSTFOLIO_IMPORT=true          # Automatically import transactions
GHOSTFOLIO_UPDATE_CASH=TRUE     # Update account cash balance after import
NODE_OPTIONS="--max-old-space-size=4000" # Memory limit for large CSV processing

# --- Investbrain Runtime Options ---
INVESTBRAIN_VALIDATE=true       # Validate CSV before importing to Investbrain
INVESTBRAIN_IMPORT=true         # Automatically import transactions to Investbrain
INVESTBRAIN_SAME_DAY_DELAY_SECONDS=2 # Delay between intraday trades to prevent 422 errors
INVESTBRAIN_AUTO_REFRESH=true   # Automatically trigger Investbrain server-side data refreshes

# --- Yahoo Rate Limit Handling ---
# t212_fetch.py checks Yahoo only when there are no Investbrain accounts configured.
# run-all.sh applies the Yahoo pre-check and cooldown only for Ghostfolio conversions.
# Cooldown state is stored in .state/yahoo_rate_limit.
YAHOO_RATE_LIMIT_COOLDOWN_SECONDS=300
YAHOO_RATE_LIMIT_CHECK_SYMBOL=AMZN

# Manual Yahoo rate-limit check
# Use this command to verify whether Yahoo Finance is currently rate limiting price lookups:
# curl -sS -m 10 "https://query1.finance.yahoo.com/v7/finance/quote?symbols=AMZN"
# If the command returns HTTP 429 or error text such as "Too Many Requests" then the pre-check will skip conversion.
# Rate-limited responses may not be valid JSON, so the command falls back to raw text output.
```

**How to get your credentials:**
1.  **Trading212 API Key**: In the Trading212 app/web, go to **Menu → Settings → API (Beta) → Generate key**. This will provide both the **API Key** (Key ID) and the **Secret**.
2.  **Ghostfolio Account ID**: In Ghostfolio, go to **Accounts**, select **Edit** for your account, and copy the ID shown in the URL or the edit window.
3.  **Investbrain Portfolio ID**: In Investbrain, go to your portfolio settings or API documentation to find your portfolio ID.
4.  **Investbrain API Token**: In Investbrain, go to **Settings → API Tokens** to generate a new token with appropriate permissions.

You can configure accounts to sync to Ghostfolio, Investbrain, or both destinations for the same Trading212 prefix because `run-all.sh` discovers both `PREFIX_GHOSTFOLIO_ACCOUNT_ID` and `PREFIX_INVESTBRAIN_PORTFOLIO_ID` independently.

Protect your credentials file:
```bash
chmod 600 .env
```

To add more accounts in the future, simply add another pair of lines with a new prefix.

> [!NOTE]
> All credentials are now managed in the `.env` file. Do not commit this file to version control.

</details>

---

<details>
<summary><h2>5. Understanding Account Prefixes</h2></summary>

The scripts use **Prefixes** (like `PREFIX1` or `PREFIX2`) to link your Trading212 credentials to your specific Ghostfolio accounts or Investbrain portfolios.

### Why use prefixes?
- **Multiple Accounts**: You can sync as many separate Trading212 accounts as you want in a single run.
- **Platform Choice**: Each account can sync to Ghostfolio, Investbrain, or both if both destination IDs are configured for the same prefix.
- **Mapping**: The script automatically pairs `PREFIX_API_KEY` with its corresponding `PREFIX_GHOSTFOLIO_ACCOUNT_ID` or `PREFIX_INVESTBRAIN_PORTFOLIO_ID`.
- **Organization**: Data for each account is stored in its own sub-folder (e.g., `out/prefix1/`).

### How to customize:
1. Choose any short name (e.g., `MYACC`, `JOHN`, `TRADING`).
2. Use this name in your `.env` file (e.g., `JOHN_API_KEY=...` and `JOHN_GHOSTFOLIO_ACCOUNT_ID=...` or `JOHN_INVESTBRAIN_PORTFOLIO_ID=...`).
3. The scripts will **automatically** find and process these prefixes.

> [!TIP]
> **Single account?** You can still use a named prefix, but `run-all.sh` also supports unprefixed `GHOSTFOLIO_ACCOUNT_ID` or `INVESTBRAIN_PORTFOLIO_ID` and maps them to the internal `default` prefix.
>
> **Mixed platforms?** You can have some accounts sync to Ghostfolio and others to Investbrain in the same configuration.
</details>

---

## 6. Copy and Test the Scripts

Ensure `t212_fetch.py` is in your project folder. Test the setup manually before automating:

<details>
<summary><h3>Bare Metal</h3></summary>

```bash
cd T212-Sync-buddy
source venv/bin/activate
python3 t212_fetch.py
```

</details>

<details>
<summary><h3>Docker Compose</h3></summary>

```bash
cd T212-Sync-buddy
docker compose run --rm t212-sync-buddy
```

</details>

The **first run** (bootstrap) will:
1.  **Auto-detect**: Find the date of your very first transaction.
2.  **Full Fetch**: Download your entire history (this may take a few minutes due to T212 API rate limits).
3.  **Save & Import**: Save CSVs to `input/` and trigger `run-all.sh` to hand them off to Ghostfolio and/or Investbrain.

> [!IMPORTANT]
> The scripts now operate exclusively within the `T212-Sync-buddy` directory. All input files (`input/`) and generated outputs (`out/`, `.state/`) will be found inside this folder.

After this run, `.state/prefix1.json` and `.state/prefix2.json` are created. All future runs will only fetch the last 7 days. Status is tracked in `.state/`.

## 7. Test Commands

Run these from the repository root:

```bash
python3 -m py_compile t212_fetch.py
bash -n run-all.sh
python3 -m unittest tests/test_t212_fetch.py -v
bats tests/test_run_all.bats
```

If `shellcheck` is installed locally, it is also useful for `run-all.sh`:

```bash
shellcheck run-all.sh
```

---

## 8. Set Up Automation (Systemd)

<details>
<summary><h3>Option A: Bare Metal Automation</h3></summary>

To run the sync automatically every day, set up a systemd timer.

1.  **Copy the unit files** (assuming you have them prepared in a `systemdunits` folder):
    ```bash
    sudo cp systemdunits/t212-sync-buddy.service /etc/systemd/system/
    sudo cp systemdunits/t212-sync-buddy.timer /etc/systemd/system/
    ```

2.  **Configure the service**:
    ```bash
    sudo nano /etc/systemd/system/t212-sync-buddy.service
    ```
    Ensure the `User`, `WorkingDirectory`, and `ExecStart` paths match your actual installation path.

3.  **Enable and start**:
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable --now t212-sync-buddy.timer
    ```

</details>

<details>
<summary><h3>Option B: Docker Automation</h3></summary>

If you use the Docker Compose setup (Section 3, Option B), use these systemd units instead:

1.  **Copy the Docker unit files**:
    ```bash
    sudo cp systemdunits/t212-sync-buddy-docker.service /etc/systemd/system/
    sudo cp systemdunits/t212-sync-buddy-docker.timer /etc/systemd/system/
    ```

2.  **Configure the service**:
    ```bash
    sudo nano /etc/systemd/system/t212-sync-buddy-docker.service
    ```
    Update `User`, `WorkingDirectory`, and `HOST_SCRIPTS_DIR` to match your actual installation path.

3.  **Enable and start**:
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable --now t212-sync-buddy-docker.timer
    ```

> [!TIP]
> Use **either** Option A (bare metal) **or** Option B (Docker) — not both.
> The Docker method does not require a Python venv on the host.

</details>

---

## 9. Monitoring and Maintenance

<details>
<summary><h3>Bare Metal Monitoring</h3></summary>

#### Verify the Timer
```bash
# Check if the timer is active
systemctl status t212-sync-buddy.timer

# View next scheduled run
systemctl list-timers t212-sync-buddy.timer

# Manually trigger a run now to test
sudo systemctl start t212-sync-buddy.service

# View latest logs
journalctl -u t212-sync-buddy.service -n 50 --since "yesterday"
```

#### Routine Maintenance
- **Manual Cleanup**: To quickly reset your local data without running the fetcher:
  ```bash
  rm -rf out/ input/ .state/ temp/ cache/
  ```
- **Temporary Disable**: 
  ```bash
  sudo systemctl stop t212-sync-buddy.timer
  ```

</details>

<details>
<summary><h3>Docker Monitoring</h3></summary>

#### Verify the Timer
```bash
# Check if the timer is active
systemctl status t212-sync-buddy-docker.timer

# View next scheduled run
systemctl list-timers t212-sync-buddy-docker.timer

# Manually trigger a run now to test
sudo systemctl start t212-sync-buddy-docker.service

# View latest logs
journalctl -u t212-sync-buddy-docker.service -n 50 --since "yesterday"
```

#### Routine Maintenance
- **Manual Cleanup**: To quickly reset your local data without running the fetcher:
  ```bash
  rm -rf out/ input/ .state/ temp/ cache/
  ```
- **Rebuild the image** (after updating scripts):
  ```bash
  docker compose build --no-cache
  ```
- **View container logs** (if running interactively):
  ```bash
  docker compose logs t212-sync-buddy
  ```
- **Temporary Disable**: 
  ```bash
  sudo systemctl stop t212-sync-buddy-docker.timer
  ```

</details>

---

<details>
<summary><h2>Troubleshooting</h2></summary>

### Log Levels

Control log verbosity with the `T212_LOG_LEVEL` environment variable:

| Level | Output | Use Case |
| :--- | :--- | :--- |
| `TRACE` | Everything (per-second countdowns, per-variable dumps) | Deep debugging |
| `DEBUG` | Diagnostics (HTTP requests, config summaries, state saves) | Troubleshooting issues |
| **`INFO`** | Operational progress (default) | Production / daily cron |
| `WARN` | Non-fatal problems (rate limits, skipped files) | Quiet monitoring |
| `ERROR` | Failed operations only | Minimal output |
| `FATAL` | Unrecoverable exits only | Silent except on crash |

Set in `.env` or inline:
```bash
# In .env
T212_LOG_LEVEL=DEBUG

# Or inline for a single run
T212_LOG_LEVEL=TRACE python3 t212_fetch.py
```

| Problem | Solution |
| :--- | :--- |
| `No accounts found in .env` | Ensure your `.env` variables use the correct `PREFIX_API_KEY` format. |
| `429 Too Many Requests` | Trading212 rate limits are retried automatically. Yahoo rate limits cause Ghostfolio conversion to be skipped until the cooldown in `.state/yahoo_rate_limit` expires. |
| `Invalid Record Length` | Handled automatically by the script's normalization logic. |
| `❌ JSON not found` | Check if Docker is running properly: `sudo systemctl status docker`. |
| Timer not running | Inspect logs for path or permission errors: `journalctl -u t212-sync-buddy.service`. |
| `docker: command not found` (inside container) | Rebuild the image: `docker compose build --no-cache`. Ensure the Dockerfile uses multi-stage `COPY --from=docker-cli`. |
| `HOST_SCRIPTS_DIR` warning | Set `HOST_SCRIPTS_DIR` in `.env` to the absolute host path of the `T212-Sync-buddy` directory. |
| Too much log output | Set `T212_LOG_LEVEL=WARN` in `.env` to see only warnings and errors. |
| Need more detail for debugging | Set `T212_LOG_LEVEL=TRACE` in `.env` for maximum verbosity. |

</details>

---

## Acknowledgments

This robust automation and synchronization pipeline is powered by the excellent CSV-to-JSON parsing engine provided by [dickwolff/Export-To-Ghostfolio](https://github.com/dickwolff/Export-To-Ghostfolio). Data extraction via API, orchestration, normalization, and scheduling are maintained within this project.



If you like this project, consider supporting me:

<a href="https://www.buymeacoffee.com/dominatos"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" height="40"></a>

---
