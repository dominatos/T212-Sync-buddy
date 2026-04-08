# T212-Sync-buddy: Automated Trading 212 to Ghostfolio Sync

**T212-Sync-buddy** is a fully automated synchronization pipeline designed to safely extract your transaction history from the Trading 212 API and seamlessly import it into your Ghostfolio instance. 

Built for reliability and data integrity, this service handles everything from initial data bootstraps and smart Trading 212 API rate-limit management to automated data normalization and strict cross-verification. Whether you manage a single portfolio or sync multiple distinct Trading 212 accounts simultaneously, this tool runs quietly in the background (via Docker or systemd) to keep your Ghostfolio dashboard perfectly in sync.

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

---

## 1. Project Structure

Your final directory structure should look like this:
```text
/path/to/T212-Sync-buddy/
├── .env.sample           # Template for API credentials for manual runs
└──               # logic and automation files
    ├── .env              # your actual API credentials (copied from .env.sample) for automatic/systemd runs
    ├── .state/           # auto-created, stores last fetch timestamps
    ├── input/            # auto-created, CSV exports land here
    ├── out/              # Ghostfolio-compatible JSONs land here
    ├── temp/             # temporary directory used by the conversion script
    ├── t212_fetch.py     # Main fetcher script
    ├── run-all.sh        # Universal sync and verification script
    └── systemdunits/     # Services and timers for automation
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
A universal Bash script for syncing CSV exports to Ghostfolio.
- **Account Discovery**: Automatically finds all prefixed accounts in `.env`.
- **Docker Integration**: Launches the `dickwolff/export-to-ghostfolio` container for each account.
- **Smart Verification**: Automatically detects column headers (Date, Symbol, Quantity) in any CSV format to cross-verify the resulting JSON against the source CSV.
- **Universal Support**: Works with 26+ brokers supported by the underlying `dickwolff/export-to-ghostfolio` repository (Trading 212, Revolut, IBKR, DEGIRO, etc.).
- **Organization**: Moves completed imports to `out/account_name/` and archives processed CSVs to `input/done/`.

### systemdunits/t212-sync-buddy.service
A Linux systemd service unit that defines *how* to run the synchronization. It calls `t212_fetch.py` using the dedicated Python virtual environment.

### systemdunits/t212-sync-buddy.timer
A systemd timer that controls *when* the sync runs. By default, it is configured to trigger the synchronization daily at **08:00 AM**.

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
# Format: PREFIX_API_KEY and PREFIX_API_SECRET. For security it is recommended to create api keys with limited permissions for only read.
PREFIX1_API_KEY=your_prefix1_api_key_here
PREFIX1_API_SECRET=your_prefix1_api_secret_here
PREFIX1_GHOSTFOLIO_ACCOUNT_ID=your_ghostfolio_account_id

PREFIX2_API_KEY=your_prefix2_api_key_here
PREFIX2_API_SECRET=your_prefix2_api_secret_here
PREFIX2_GHOSTFOLIO_ACCOUNT_ID=your_ghostfolio_account_id

# --- Ghostfolio Settings ---
GHOSTFOLIO_URL=http://host.docker.internal:3333
GHOSTFOLIO_SECRET=your_ghostfolio_secret_here

# --- Ghostfolio Runtime Options ---
GHOSTFOLIO_VALIDATE=true        # Validate activities against Ghostfolio before import
GHOSTFOLIO_IMPORT=true          # Automatically import transactions
GHOSTFOLIO_UPDATE_CASH=TRUE     # Update account cash balance after import
NODE_OPTIONS="--max-old-space-size=4000" # Memory limit for large CSV processing
```

**How to get your credentials:**
1.  **Trading212 API Key**: In the Trading212 app/web, go to **Menu → Settings → API (Beta) → Generate key**. This will provide both the **API Key** (Key ID) and the **Secret**.
2.  **Ghostfolio Account ID**: In Ghostfolio, go to **Accounts**, select **Edit** for your account, and copy the ID shown in the URL or the edit window.

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

The scripts use **Prefixes** (like `PREFIX1` or `PREFIX2`) to link your Trading212 credentials to your specific Ghostfolio accounts. 

### Why use prefixes?
- **Multiple Accounts**: You can sync as many separate Trading212 accounts as you want in a single run.
- **Mapping**: The script automatically pairs `PREFIX_API_KEY` with its corresponding `PREFIX_GHOSTFOLIO_ACCOUNT_ID`.
- **Organization**: Data for each account is stored in its own sub-folder (e.g., `out/prefix1/`).

### How to customize:
1. Choose any short name (e.g., `MYACC`, `JOHN`, `TRADING`).
2. Use this name in your `.env` file (e.g., `JOHN_API_KEY=...` and `JOHN_GHOSTFOLIO_ACCOUNT_ID=...`).
3. The scripts will **automatically** find and process these prefixes.

> [!TIP]
> **Single account?**  If you have just one account, you still need to use prefix in .env file!
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
3.  **Save & Import**: Save CSVs to `input/` and trigger `run-all.sh` to update Ghostfolio.

> [!IMPORTANT]
> The scripts now operate exclusively within the `T212-Sync-buddy` directory. All input files (`input/`) and generated outputs (`out/`, `.state/`) will be found inside this folder.

After this run, `.state/prefix1.json` and `.state/prefix2.json` are created. All future runs will only fetch the last 7 days. Status is tracked in `.state/`.

---

## 7. Set Up Automation (Systemd)

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

## 8. Monitoring and Maintenance

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
- **Force a full re-import**: If you need to rebuild your history from scratch, first remove the existing activities for that account in Ghostfolio, then run with `--force-initial-sync` (this wipes `out/`, `input/`, `.state/`, `temp/`, `cache/`):
  ```bash
  source venv/bin/activate
  python3 t212_fetch.py --force-initial-sync
  ```
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
- **Force a full re-import**: If you need to rebuild your history from scratch, first remove the existing activities for that account in Ghostfolio, then run with `--force-initial-sync` (this wipes `out/`, `input/`, `.state/`, `temp/`, `cache/`):
  ```bash
  docker compose run --rm t212-sync-buddy python3 t212_fetch.py --force-initial-sync
  ```
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

| Problem | Solution |
| :--- | :--- |
| `No accounts found in .env` | Ensure your `.env` variables use the correct `PREFIX_API_KEY` format. |
| `429 Too Many Requests` | The script handles this automatically; it will pause and resume when allowed. |
| `Invalid Record Length` | Handled automatically by the script's normalization logic. |
| `❌ JSON not found` | Check if Docker is running properly: `sudo systemctl status docker`. |
| Timer not running | Inspect logs for path or permission errors: `journalctl -u t212-sync-buddy.service`. |
| `docker: command not found` (inside container) | Rebuild the image: `docker compose build --no-cache`. Ensure the Dockerfile uses multi-stage `COPY --from=docker-cli`. |
| `HOST_SCRIPTS_DIR` warning | Set `HOST_SCRIPTS_DIR` in `.env` to the absolute host path of the `T212-Sync-buddy` directory. |

</details>

---

## Acknowledgments

This robust automation and synchronization pipeline is powered by the excellent CSV-to-JSON parsing engine provided by [dickwolff/Export-To-Ghostfolio](https://github.com/dickwolff/Export-To-Ghostfolio). Data extraction via API, orchestration, normalization, and scheduling are maintained within this project.
