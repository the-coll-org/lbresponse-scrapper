# Lebanon Crisis Response - Power BI Scraper

Scrapes data from the [Lebanon Crisis Response Power BI dashboard](https://app.powerbi.com/view?r=eyJrIjoiOThhYTMyN2ItMGNjMS00NDIzLWFhM2QtMjkzNmZkNjFiM2E5IiwidCI6ImU1YzM3OTgxLTY2NjQtNDEzNC04YTBjLTY1NDNkMmFmODBiZSIsImMiOjh9) and stores the extracted data in Firebase or CSV files.

No browser or headless driver needed. The scraper talks directly to Power BI's internal API, extracting structured data from all report pages and visuals.

---

## What It Scrapes

The dashboard contains **10 pages** with **82 visuals** covering:

- Service mapping (organizations, sectors, districts, contact info)
- Health service mapping (PHC and SHC facilities)
- A3 matrix data (services, partners, sectors)
- Pivot tables and geographic data

A full scrape extracts **~11,000 rows** in about 10 seconds.

---

## Prerequisites

- **Python 3.12+**
- **Docker** (optional, for containerized/scheduled runs)
- **Firebase project** (optional, for cloud storage)

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/the-coll-org/lbresponse-scrapper.git
cd lbresponse-scrapper
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run a scrape

```bash
# Export to CSV files (no Firebase needed)
python main.py --no-firebase --csv once

# Check the output
ls output/
```

That's it. CSV files will appear in the `output/` directory.

---

## Running With Docker

### 1. Copy the environment file

```bash
cp .env.example .env
# Edit .env if you need to change any settings
```

### 2. Build and run

```bash
# One-time scrape
docker compose run --rm scraper python main.py --no-firebase --csv once

# Scheduled scrape (every 6 hours by default)
docker compose up -d

# Check logs
docker compose logs -f scraper

# Stop
docker compose down
```

CSV files are written to the `output/` directory on your host machine.

---

## Firebase Setup (Optional)

If you want to store scraped data in Firebase:

### 1. Create a Firebase project

Go to [Firebase Console](https://console.firebase.google.com/) and create a project (or use an existing one).

### 2. Create a Realtime Database

In your Firebase project: **Build** > **Realtime Database** > **Create Database**. Choose a location and start in **test mode**.

### 3. Generate a service account key

**Project Settings** > **Service accounts** > **Generate new private key**. Download the JSON file.

> **WARNING**: Never commit credentials to git. The `.gitignore` blocks common credential filenames, but always double-check with `git status` before committing.

### 4. Configure

```bash
cp .env.example .env
```

Edit `.env` with your values:

```env
FIREBASE_DB_URL=https://YOUR-PROJECT-default-rtdb.firebaseio.com
FIREBASE_CRED_PATH=./your-firebase-credentials.json
FIREBASE_CRED_FILE=./your-firebase-credentials.json
```

### 5. Run with Firebase

```bash
# Local
python main.py once

# Docker
docker compose run --rm scraper python main.py once
```

Data is stored in Firebase under `powerbi_data/{visual_name}/rows`.

---

## All CLI Options

```
python main.py [OPTIONS] COMMAND

Options:
  --url URL          Power BI embed URL (default: Lebanon response dashboard)
  --csv              Also export data to CSV files in output/
  --no-firebase      Skip Firebase storage

Commands:
  once               Run a single scrape and exit
  schedule           Run scrapes on a repeating schedule
    --interval N     Hours between scrapes (default: 6)
```

### Examples

```bash
# CSV only, single run
python main.py --no-firebase --csv once

# Firebase + CSV, single run
python main.py --csv once

# Scheduled every 12 hours, Firebase only
python main.py schedule --interval 12

# Scrape a different Power BI report
python main.py --url "https://app.powerbi.com/view?r=..." --csv once
```

---

## Project Structure

```
lbresponse-scrapper/
├── main.py                  # CLI entry point
├── config.py                # Settings (URL, Firebase, schedule)
├── requirements.txt         # Python dependencies
├── Dockerfile               # Container image
├── docker-compose.yml       # Container orchestration
├── entrypoint.sh            # Docker entrypoint
├── .env.example             # Environment template
├── .gitignore
├── .pre-commit-config.yaml  # Pre-commit hooks
├── pyproject.toml           # Linting & type-check config
├── .github/
│   └── workflows/
│       └── ci.yml           # GitHub Actions pipeline
└── scraper/
    ├── embed_url.py         # Parse Power BI embed URL, resolve API cluster
    ├── api_client.py        # HTTP client for Power BI public API
    ├── report_explorer.py   # Parse report pages, visuals, queries
    ├── query_builder.py     # Build querydata API payloads
    ├── dsr_parser.py        # Decode Power BI's DSR response format
    └── firebase_store.py    # Firebase Realtime Database storage
```

---

## How It Works

Power BI public reports expose their data through internal REST APIs. This scraper:

1. **Parses the embed URL** to extract the resource key and tenant ID
2. **Resolves the API cluster** by fetching the embed page and extracting the cluster URI
3. **Fetches the report structure** (pages, visuals, queries) via the `modelsAndExploration` endpoint
4. **Extracts data** from each visual by replaying its prototype query against the `querydata` endpoint
5. **Decodes the response** from Power BI's compressed DSR format (dictionary encoding, repeat masks, null masks)
6. **Stores the data** in Firebase and/or CSV files

No authentication is needed for public reports.

---

## Development

### Setup

```bash
pip install -r requirements.txt
pip install ruff mypy types-requests pandas-stubs pre-commit

# Install pre-commit hooks
pre-commit install
```

### Linting & Type Checking

```bash
# Lint
ruff check .

# Auto-fix lint issues
ruff check --fix .

# Format
ruff format .

# Type check
mypy config.py main.py scraper/
```

### CI Pipeline

GitHub Actions runs on every push and PR to `main`:

| Job | What it does |
|-----|-------------|
| **Lint & Format** | `ruff check` + `ruff format --check` |
| **Type Check** | `mypy` strict type checking |
| **Docker Build** | Verifies the Docker image builds |
| **Security Scan** | `bandit` security analysis |

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POWERBI_EMBED_URL` | Lebanon response dashboard | Power BI report URL |
| `FIREBASE_DB_URL` | *(none — required for Firebase)* | Firebase RTDB URL |
| `FIREBASE_CRED_PATH` | *(none — required for Firebase)* | Path to Firebase service account JSON |
| `FIREBASE_CRED_FILE` | *(none — required for Docker)* | Host path to cred file for Docker mount |
| `SCHEDULE_INTERVAL_HOURS` | `6` | Hours between scheduled scrapes |
| `PYTHONUNBUFFERED` | `1` | Show logs immediately in Docker |

---

## License

MIT
