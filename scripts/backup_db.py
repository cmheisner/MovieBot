"""
Backup MovieBot's SQLite database.

Two modes:
  - default: copy data/moviebot.db -> data/backups/moviebot-YYYY-MM-DD.db,
    prune local backups older than 30 days.
  - --upload-drive: do the local backup, then upload the new file to the
    "MovieBot Backups" folder on the service account's Google Drive
    (folder is auto-created on first run). Prunes Drive backups to the
    8 most recent.

Designed to run unattended from cron. Paths are resolved relative to this
file, so cwd doesn't matter.

Service-account Drive note: service accounts have ~15 GB of shared storage
of their own. A few-MB .db file is fine, but if we ever start backing up
much larger artifacts, revisit quota.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "data" / "moviebot.db"
BACKUP_DIR = REPO_ROOT / "data" / "backups"
LOCAL_RETENTION_DAYS = 30
DRIVE_FOLDER_NAME = "MovieBot Backups"
DRIVE_RETENTION_COUNT = 8
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def backup_db_locally() -> Path:
    """Copy the live db using SQLite's online backup API.

    A naive file copy of a WAL-mode db can produce a torn snapshot — the
    main file may lag uncommitted pages still in the -wal sidecar.
    sqlite3's backup() takes a consistent snapshot across both.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Source db not found: {DB_PATH}")

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    dest = BACKUP_DIR / f"moviebot-{stamp}.db"

    src = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(dest)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"[OK] Backed up to {dest.relative_to(REPO_ROOT)} ({size_mb:.1f} MB)")
    return dest


def prune_local_backups() -> int:
    """Delete local backup files older than LOCAL_RETENTION_DAYS."""
    if not BACKUP_DIR.exists():
        return 0

    cutoff = datetime.now() - timedelta(days=LOCAL_RETENTION_DAYS)
    pruned = 0
    for path in BACKUP_DIR.glob("moviebot-*.db"):
        # Prefer parsing the date from the filename so we don't depend on mtime.
        stem = path.stem  # moviebot-YYYY-MM-DD
        try:
            date_str = stem.split("moviebot-", 1)[1]
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
        except (IndexError, ValueError):
            # Unparseable name — fall back to mtime so we don't strand junk forever.
            file_date = datetime.fromtimestamp(path.stat().st_mtime)
        if file_date < cutoff:
            path.unlink()
            pruned += 1
    print(f"[OK] Pruned {pruned} old backups")
    return pruned


def _build_drive_service():
    """Lazy-import googleapiclient so default mode runs without it installed."""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH")
    if not sa_path:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_PATH is not set.")
    if not Path(sa_path).exists():
        raise FileNotFoundError(f"Service account file not found: {sa_path}")

    creds = Credentials.from_service_account_file(sa_path, scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def ensure_drive_folder(service) -> str:
    """Return the id of the MovieBot Backups folder, creating it if absent."""
    query = (
        f"name='{DRIVE_FOLDER_NAME}' "
        "and mimeType='application/vnd.google-apps.folder' "
        "and trashed=false"
    )
    resp = service.files().list(
        q=query,
        fields="files(id, name)",
        spaces="drive",
    ).execute()
    files = resp.get("files", [])
    if files:
        return files[0]["id"]

    folder = service.files().create(
        body={
            "name": DRIVE_FOLDER_NAME,
            "mimeType": "application/vnd.google-apps.folder",
        },
        fields="id",
    ).execute()
    return folder["id"]


def upload_to_drive(backup_path: Path) -> str:
    """Upload the backup to the MovieBot Backups folder. Returns the file id."""
    from googleapiclient.http import MediaFileUpload

    service = _build_drive_service()
    folder_id = ensure_drive_folder(service)

    media = MediaFileUpload(
        str(backup_path),
        mimetype="application/octet-stream",
        resumable=False,
    )
    created = service.files().create(
        body={"name": backup_path.name, "parents": [folder_id]},
        media_body=media,
        fields="id, name",
    ).execute()

    file_id = created["id"]
    print(f"[OK] Uploaded to Drive folder '{DRIVE_FOLDER_NAME}' (file id={file_id})")

    pruned = prune_drive_backups(service, folder_id)
    print(f"[OK] Pruned {pruned} old Drive backups")
    return file_id


def prune_drive_backups(service, folder_id: str) -> int:
    """Keep only the DRIVE_RETENTION_COUNT most recent backups in the folder."""
    resp = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc",
        spaces="drive",
        pageSize=100,
    ).execute()
    files = resp.get("files", [])
    to_delete = files[DRIVE_RETENTION_COUNT:]
    for f in to_delete:
        service.files().delete(fileId=f["id"]).execute()
    return len(to_delete)


def main() -> int:
    parser = argparse.ArgumentParser(description="Back up the MovieBot SQLite db.")
    parser.add_argument(
        "--upload-drive",
        action="store_true",
        help="After the local backup, upload to Google Drive.",
    )
    args = parser.parse_args()

    try:
        backup_path = backup_db_locally()
        prune_local_backups()
    except Exception:
        # Let the traceback hit stderr/cron — exit code 2 marks "local failed".
        raise SystemExit(2)

    if args.upload_drive:
        try:
            upload_to_drive(backup_path)
        except Exception as exc:
            # Local backup is already safe on disk; surface the upload error
            # but distinguish it from a total failure via exit code 1.
            import traceback
            traceback.print_exc()
            print(f"[ERR] Drive upload failed: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
