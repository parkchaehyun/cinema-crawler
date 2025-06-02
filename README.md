# Cinema Crawler & TMDB Poster Updater

## Overview

This repository contains two related AWS Lambda functions packaged as Docker images:

1. **Crawler Lambda**

   * Scrapes art-film screening schedules from multiple chains (CGV, Megabox, Lotte, Dtryx, TinyTicket, Moonhwain, KOFA)
   * Writes screening records into a Supabase PostgreSQL backend

2. **TMDB Poster Updater Lambda**

   * Queries Supabase for all movies with `poster_url IS NULL`
   * Calls TMDB’s Search API (using a Bearer token) for each title
   * Updates Supabase so that each movie row gets its `poster_url` filled in

These two functions are scheduled to run every day to keep screenings and poster images up to date.

---

## Architecture

```
┌──────────────┐     ┌───────────────────────────┐
│ EventBridge  │     │  Lambda (Crawler Image)   │
│ Rule [cron]  ├────▶|  (Python 3.11 + Chrome)   │
└──────────────┘     └───────────────────────────┘
                                   │
                                   ▼
                            ┌───────────────┐
                            │   Supabase    │
                            │  PostgreSQL   │
                            └───────────────┘
                                   ▲
                                   │
┌──────────────┐     ┌────────────────────────────┐
│ EventBridge  │     │ Lambda (TMDB-Updater Image)│
│ Rule [cron]  ├────▶|  (Python 3.11 + httpx)     │
└──────────────┘     └────────────────────────────┘
```

* **Crawler Lambda**

  * Runs headless Chrome/Selenium in a Python 3.11 container
  * Crawls multiple cinema-chain websites, parses screening times, and upserts into Supabase
  * Requires `SUPABASE_URL`, `SUPABASE_KEY`, and Chrome binary in `/opt/chrome`

* **TMDB Poster Updater Lambda**

  * Runs a lightweight Python 3.10 container with only `httpx` and `supabase-py`
  * Queries Supabase for all movies where `poster_url IS NULL`, then calls TMDB’s `/search/movie` using a Bearer token
  * Updates each row’s `poster_url` in Supabase

---

## Prerequisites

* **Docker 20.10+**
* **AWS CLI v2**
* A **Supabase** project with a `movies` table defined as:

  ```sql
  CREATE TABLE IF NOT EXISTS public.movies (
    id         BIGSERIAL PRIMARY KEY,
    title      TEXT NOT NULL,
    poster_url TEXT
  );
  ```
* **TMDB API token**

---

## Repository Structure

```
root/
├── crawlers/
│   ├── base.py                  # base class for all crawlers
│   ├── cgv.py                   # CGV crawler
│   ├── crawler_registry.py      # registry that returns each chain’s crawler class
│   ├── dtryx.py                 # Dtryx crawler
│   ├── kofa.py                  # KOFA crawler
│   ├── lambda_function.py       # entrypoint for the Crawler Lambda
│   ├── lotte.py                 # Lotte Cinema crawler
│   ├── megabox.py               # Megabox crawler
│   ├── moonhwain.py             # Moonhwain crawler
│   ├── offline_test.py          # local test script for crawlers
│   ├── poster_updater.py        # entrypoint for the TMDB-Updater Lambda
│   ├── supabase_client.py       # wrapper for Supabase REST interactions
│   ├── tinyticket.py            # TinyTicket crawler
│
├── chrome-deps.txt              # OS packages required by headless Chrome
├── Dockerfile                   # multi-stage Dockerfile (Stage 1 = crawler, Stage 2 = tmdb-updater)
├── install-browser.sh           # script to install headless Chrome/Chromedriver
├── models.py                    # Pydantic data models (Screening, Chain, etc.)
├── README.md                    # (this file)
├── requirements-crawler.txt     # pip dependencies for the Crawler (selenium, bs4, supabase-py, etc.)
└── requirements-tmdb.txt        # pip dependencies for the Poster Updater (supabase-py, httpx)
```

---

## Building Docker Images

Both Lambda functions are packaged as separate Docker images from the same `Dockerfile`. You can build each stage individually:

### 2.1. Build the Crawler Image

```bash
docker buildx build \
  --platform linux/amd64 \
  --target stage \
  --tag lambda-crawler:latest \
  --load \
  .
```

* **`--platform linux/amd64`**: ensures the image is built for x86\_64 (AWS Lambda’s CPU architecture).
* **`--target stage`**: picks the multi-stage Dockerfile’s first stage, which installs Chrome, Selenium, and the crawler code.
* **`--load`**: loads the resulting image into your local Docker daemon as `lambda-crawler:latest`.

Verify with `docker images | grep lambda-crawler`.

### 2.2. Build the TMDB Updater Image

```bash
docker buildx build \
  --platform linux/amd64 \
  --target builder-tmdb \
  --tag lambda-tmdb:latest \
  --load \
  .
```

* **`--target builder-tmdb`**: picks the TMDB updater stage (Python 3.10 + `httpx`, `supabase-py`, and `poster_updater.py`).
* **`--load`**: loads as `lambda-tmdb:latest` locally.

Verify with `docker images | grep lambda-tmdb`.

---

## AWS Deployment (ECR & Lambda)

### 3.1. Create an ECR Repository

Replace `<your-region>` and `<account-id>` as needed:

```bash
aws ecr create-repository \
  --repository-name poster-updater \
  --region ap-northeast-2
```

Note the returned `repositoryUri`, e.g.:

```
123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/poster-updater
```

For the crawler, you might create a separate repo:

```bash
aws ecr create-repository \
  --repository-name cinema-crawler \
  --region ap-northeast-2
```

### 3.2. Authenticate & Push Images

#### 3.2.1. Authenticate to ECR

```bash
aws ecr get-login-password --region ap-northeast-2 \
  | docker login --username AWS --password-stdin 123456789012.dkr.ecr.ap-northeast-2.amazonaws.com
```

#### 3.2.2. Tag & Push Crawler Image

```bash
docker tag lambda-crawler:latest \
  123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/cinema-crawler:latest

docker push 123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/cinema-crawler:latest
```

#### 3.2.3. Tag & Push TMDB Updater Image

```bash
docker tag lambda-tmdb:latest \
  123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/poster-updater:latest

docker push 123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/poster-updater:latest
```

---


## License

The MIT License (MIT)

Copyright (c) 2025 Chaehyun Park
