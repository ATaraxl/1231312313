# Comic Organizer

A structured Python project version of a comic archive / ebook organizer script.

## Features

- Multiple scan modes: `safe`, `repair`, `full`
- `dry-run` preview before real execution
- Real execution with confirmation protection
- Duplicate detection by size + quick hash + full hash
- Suspect duplicate detection by normalized title
- Session history persistence under `.history/`
- Rollback support for previous execution sessions
- Supports archives and ebook-like files such as `zip`, `rar`, `7z`, `cbz`, `cbr`, `epub`, `pdf`, `mobi`, `azw3`

## Installation

### Run directly

```bash
python3 -m comic_organizer /path/to/source --dry-run
```

### Optional editable install

```bash
pip install -e .
comic-organizer /path/to/source --dry-run
```

## Usage

```bash
python3 -m comic_organizer /path/to/source --dry-run --scan-mode safe
python3 -m comic_organizer /path/to/source --execute --scan-mode repair --yes
python3 -m comic_organizer /path/to/source --list-sessions
python3 -m comic_organizer /path/to/source --rollback latest
```

## Project Structure

```text
.
├── comic_organizer/
│   ├── __init__.py
│   ├── __main__.py
│   └── cli.py
├── pyproject.toml
├── README.md
├── README.zh-CN.md
└── .gitignore
```

## Notes

- This repository currently keeps the original logic mostly intact in a single package module for safe migration.
- The next step can be further refactoring into `config.py`, `session.py`, `detector.py`, `organizer.py`, etc.
- Runtime artifacts such as `.history/` and `整理日志.txt` are ignored by Git.
