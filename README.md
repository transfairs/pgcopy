# PGCopy – PostgreSQL Data Copy Pipeline

<p align="left">
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue.svg" alt="Python Badge"/>
  <img src="https://img.shields.io/badge/PostgreSQL-FDW%2Fdblink-lightgrey.svg" alt="Postgres Badge"/>
  <img src="https://img.shields.io/badge/AWS-Lambda-orange.svg" alt="Lambda Badge"/>
  <img src="https://img.shields.io/badge/AWS-Secrets_Manager-yellow.svg" alt="Secrets Badge"/>
</p>

## ✨ Overview

PGCopy is a small Python package for copying data between PostgreSQL databases, typically as part of a data‑migration pipeline. Only overlapping columns of the source and target database tables will be considered for the migration. This can vary for each target.

It focuses on:

- Reading connection details from **AWS Secrets Manager**  
- Establishing secure connections via **SSH tunnelling**  
- Copying tables from a **source schema** (e.g. `snapshot`) to one or more **target databases**  
- Running in two modes:  
  - as a **standalone Python script**,  
  - as an **AWS Lambda function** (requiring a Lambda Layer for dependencies)

---

## 📦 Features

- ✅ Secrets stored in AWS Secrets Manager  
- ✅ SSH tunnel setup with fingerprint verification  
- ✅ Multi‑target routing and mapping logic  
- ✅ FDW/dblink‑based copy operations  
- ✅ Supports Lambda execution with external dependency layer  
- ✅ Secure and minimal configuration surface  

---

## 📁 Repository Structure

```text
pgcopy/
├── __init__.py                # Package marker
├── aws_secrets.py             # AWS Secrets Manager integration
├── config.py                  # Global configuration
├── connection.py              # SSH tunnels + PostgreSQL connections
├── fdw_copy.py                # FDW/dblink-style copy operations
├── lambda_function.py         # AWS Lambda entry point
├── main.py                    # Script entry point
├── mapping.py                 # Build routing from secrets
└── routing.py                 # Copy orchestration + logging
```

---

## 🏗️ Architecture

Execution flow:

```
Load secrets → Open SSH tunnel → Connect to source DB
        ↓
Build routing from mapping
        ↓
For each target DB:
    for each table:
        copy via FDW/dblink
        log status (✅/⚠️/❌)
```

Separating responsibilities:

- **aws_secrets.py**  
  Loads and normalises the JSON secrets (hostname, credentials, ssh key).

- **mapping.py**  
  Builds routing configuration including table lists.

- **routing.py**  
  Iterates tables, opens target connections and logs results.

- **fdw_copy.py**  
  Performs copy operations using dblink/FDW approaches.

---

## ⚙️ Configuration

### Target databases
The script inserts rows into existing tables, making it very convienent to work with existing data structures. Hence, the **target databases** need to exist and **columns** that are intended to copy **need to be compatible**.

### `config.py`
This file defines all global settings for execution.

- **AWS region** (e.g. `ap-southeast-2`)
- **SSH configuration**
  - `ssh_host`
  - `ssh_user`
  - `ssh_fingerprint` (see fingerprint section below)
- **Secrets Manager prefixes**
  - `sm_prefix`
  - `target_env`
- **Database identifiers**
  - `source`
  - `target_1`, `target_2`, ...

### `mapping.py`
The file defines:
- Which **tables** are copied to each target database
- How routing is constructed from secrets

A minimal example:

```python
aws_db_1 = format_secret(secrets_json.get(pgcopy.config.db_1))
...
    aws_db_1[0]: {
        "db": f"{pgcopy.config.db_prefix}_1",
        "password": aws_db_1[3],
        "tables": [
            "example_table",
            "traffic",
        ],
    },
```

### AWS Secrets Manager
Secrets in AWS must be structured with the database name as root and the following children:

- `host`
- `port`
- `username`
- `password`
- `dbInstanceIdentifier` (source only)
- `ssh` (source only, containing the private key)

#### Notes
- `ssh` (privateKey) is used by paramiko to create the SSH tunnel.
- `host` must be the bastion host (IP, domain).
- `dbInstanceIdentifier` must be the real RDS database identifier.

The hierarchy must follow this naming structure:
```
<sm_prefix>/<target_env>/<database_name>
```

---

# 🔑 Obtaining the SSH Server Fingerprint

The fingerprint ensures that the SSH tunnel is not vulnerable to man-in-the-middle attacks.

## Linux / macOS

Run:

```bash
ssh-keyscan -t rsa <SSH_HOST> > hostkey.pub
ssh-keygen -lf hostkey.pub
```

Example output:

```
2048 SHA256:abc123xyz... <SSH_HOST> (RSA)
```

Use exactly this fingerprint string in your configuration (without `SHA256:`):

```python
ssh_fingerprint = "abc123xyz..."
```

---

## 🔧 Local Development

### 1. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Run pipeline

```bash
python -m pgcopy.main
```

Logs will be stored under `log/audit_YYYYMMDD_HHMMSS.log`.

---

## ☁️ AWS Lambda Deployment

### Lambda Handler

```python
from pgcopy.main import start

def lambda_handler(event, context):
    start()
    return {"statusCode": 200, "body": "OK"}
```

### Lambda Layer Required

Because `psycopg2`, `paramiko`, and cryptographic dependencies are not included in Lambda’s standard environment, this project requires a **Lambda Layer**.

Build the layer:

```bash
mkdir -p layer/python
pip install \
    --platform manylinux2014_x86_64 \
    --only-binary=:all: \
    -t layer/python \
    -r requirements.txt

cd layer
zip -r pgcopy-layer.zip python
```

Upload `pgcopy-layer.zip` in AWS Console → Lambda Layers → Add to function.

Then upload the source code zip **without dependencies**.

---

## 📚 Logging and Monitoring

- Local execution → logs written to `log/`  
- Lambda execution → CloudWatch Logs  

Log format includes:

- `✅ Done: <table>`  
- `⚠️ Done with warnings: <table>`  
- `❌ Error copying <table>`  

---

# 🧪 Testing and CI with `tox`

The project uses a multi-stage CI pipeline via `tox`.

## Available Environments

Next to `py312`:

| Command           | Purpose                                                  |
|-------------------|----------------------------------------------------------|
| `tox -e audit`    | Dependency auditing (`pip-audit`)                        |
| `tox -e coverage` | Runs full tests and writes HTML (`coverage`, `pytest`)   |
| `tox -e format`   | Applies code formatting (`black`, `isort`)               |
| `tox -e lint`     | Static linting (`flake8`)                                |
| `tox -e security` | Security scanning (`bandit`)                             |
| `tox -e type`     | Static type checking (`mypy`)                            |

---

## Coverage Output

After running:

```bash
tox -e coverage
```

The coverage report will be created here:

```
log/index.html
```

---

## 🔐 Security Notes

- No secrets are stored in code
- AWS Secrets Manager ensures controlled rotation
- SSH fingerprint verification

---

## 🚀 Quickstart Summary

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python -m pgcopy.main
```

## 📄 Licence

This project is Open Source and licensed under the GNU v3.0 License.

