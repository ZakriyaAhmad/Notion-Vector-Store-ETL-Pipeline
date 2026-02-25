#!/usr/bin/env python3
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import tempfile
import time
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import requests
from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError
from openai import OpenAI

try:
    import boto3
except ImportError:
    boto3 = None  # Optional; used only if MANIFEST_S3 is set.
"""
Sequential Notion -> Vector Store ETL (Glue-friendly).

Features:
- Crawl Notion pages for file/image blocks only (same traversal as get_notion_data.py).
- Sequential processing: download -> extract -> chunk -> upload -> cache, one file at a time.
- Manifest persisted locally or to S3 for incremental runs (no automatic deletion).
- Dry-run and subset limits for safe testing.

Env/args:
  NOTION_API_KEY (required)
  OPENAI_API_KEY (required)
  ROOT_PAGE_ID / ROOT_PAGE_URL (optional seed)
  VECTOR_STORE_ID (optional existing store)
  MANIFEST_PATH (default: CACHE)
  MANIFEST_S3 (optional: s3://bucket/key.json to persist manifest)
  DOWNLOAD_DIR (default: notion_downloads_v2)
  DRY_RUN (bool)
  MAX_PAGES (int)  MAX_FILES (int)  PAGE_ID_FILTER (comma-separated ids)
  LOG_DETAILS (bool) print page titles + file names as they are discovered
"""
# ---------- Configuration defaults ----------
MODEL_VISION = "gpt-4o"
CHUNK_CHAR_LEN = 1200
MAX_OUTPUT_TOKENS = 8000
REQUEST_TIMEOUT = 15
BACKOFF_DELAYS = [0, 1, 2, 4]
# ---------- Utilities ----------
def parse_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def log_step(enabled: bool, message: str):
    if enabled:
        print(f"INFO: {message}")


def display_name_from_url(url: str) -> str:
    clean = url.split("?")[0].rstrip("/")
    base = os.path.basename(clean)
    if base:
        return unquote(base)
    parsed = urlparse(url)
    return parsed.netloc or url


def normalize_notion_id(notion_id: str) -> str:
    compact = notion_id.replace("-", "")
    if len(compact) != 32:
        raise ValueError("Notion ID must be 32 hex chars; use 'Copy link' to get it.")
    return f"{compact[:8]}-{compact[8:12]}-{compact[12:16]}-{compact[16:20]}-{compact[20:]}"


def extract_id_from_notion_url(url: str) -> str:
    cleaned = url.split("?")[0].replace("-", "")
    match = re.search(r"([0-9a-fA-F]{32})", cleaned)
    if not match:
        raise ValueError("Could not find a 32-character Notion page ID in URL.")
    return match.group(1)


def resolve_root_page_id(root_page_id: Optional[str], root_page_url: Optional[str]) -> Optional[str]:
    if root_page_id:
        try:
            return normalize_notion_id(root_page_id)
        except ValueError:
            if root_page_url:
                candidate = extract_id_from_notion_url(root_page_url)
                return normalize_notion_id(candidate)
            raise
    if root_page_url:
        candidate = extract_id_from_notion_url(root_page_url)
        return normalize_notion_id(candidate)
    return None


def hash_file(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def s3_split(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError("S3 URI must start with s3://")
    remainder = uri[5:]
    bucket, key = remainder.split("/", 1)
    return bucket, key


def format_api_error(exc: Exception) -> str:
    msg = getattr(exc, "message", None)
    if msg:
        return msg
    body = getattr(exc, "body", None)
    if isinstance(body, dict):
        body_msg = body.get("message")
        if body_msg:
            return body_msg
    return str(exc)


# ---------- Notion helpers ----------
def page_title_from_properties(page_obj: dict) -> str:
    props = page_obj.get("properties", {})
    for _, prop in props.items():
        if prop.get("type") == "title":
            title_text = "".join([t.get("plain_text", "") for t in prop.get("title", [])]).strip()
            if title_text:
                return title_text
    return page_obj.get("id", "Untitled page")


def block_display_name(block: dict, url: str) -> str:
    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}
    caption = ""
    if isinstance(data, dict):
        cap = data.get("caption")
        if isinstance(cap, list) and cap:
            caption = "".join([c.get("plain_text", "") for c in cap]).strip()
    return caption or display_name_from_url(url)


def safe_filename(name: str) -> str:
    cleaned = os.path.basename(name.strip())
    return cleaned or "file"


def file_info_from_block(block: dict) -> Optional[dict]:
    btype = block.get("type")
    if btype not in {"file", "image"}:
        return None
    data = block.get(btype, {})
    if not isinstance(data, dict):
        return None
    file_data = data.get("file") if data.get("type") == "file" else data.get("external")
    if not isinstance(file_data, dict):
        return None
    url = file_data.get("url")
    if not url:
        return None

    if btype == "file":
        filename = "file"
        cap = data.get("caption")
        if isinstance(cap, list) and cap:
            filename = cap[0].get("plain_text", "") or filename
        if "." not in filename:
            filename = url.split("?")[0].split("/")[-1]
    else:
        filename = url.split("?")[0].split("/")[-1]

    return {
        "url": url,
        "filename": safe_filename(filename),
        "display_name": block_display_name(block, url),
        "notion_type": btype,
    }


# ---------- Manifest ----------
def load_manifest(manifest_path: pathlib.Path, manifest_s3: Optional[str]) -> Dict:
    if manifest_s3:
        if not boto3:
            raise RuntimeError("boto3 is required for MANIFEST_S3 usage.")
        bucket, key = s3_split(manifest_s3)
        s3_client = boto3.client("s3")
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read())
        except s3_client.exceptions.NoSuchKey:
            return {"files": {}, "vector_store_id": None}
    if manifest_path.exists():
        return json.loads(manifest_path.read_text())
    return {"files": {}, "vector_store_id": None}


def save_manifest(data: Dict, manifest_path: pathlib.Path, manifest_s3: Optional[str]):
    serialized = json.dumps(data, indent=2)
    if manifest_s3:
        if not boto3:
            raise RuntimeError("boto3 is required for MANIFEST_S3 usage.")
        bucket, key = s3_split(manifest_s3)
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket, Key=key, Body=serialized.encode())
    else:
        manifest_path.write_text(serialized)


# ---------- Download ----------
def download_with_retries(session: requests.Session, url: str, path: pathlib.Path):
    for delay in BACKOFF_DELAYS:
        if delay:
            time.sleep(delay)
        try:
            with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                with path.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            return
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response else None
            if status not in {429, 500, 502, 503, 504}:
                raise
    raise RuntimeError(f"Failed to download after retries: {url}")


# ---------- OpenAI helpers ----------
def chunk_text(text: str, max_len: int) -> List[str]:
    lines = text.splitlines()
    chunks, cur = [], []
    cur_len = 0
    for line in lines:
        if cur_len + len(line) > max_len and cur:
            chunks.append("\n".join(cur).strip())
            cur, cur_len = [], 0
        cur.append(line)
        cur_len += len(line)
    if cur:
        chunks.append("\n".join(cur).strip())
    return [c for c in chunks if c]


def extract_with_gpt(client: OpenAI, file_path: pathlib.Path) -> str:
    prompt = (
        "You are a precise extractor for marketing assets (flyers, promos, price sheets). "
        "Return the full textual content, including prices, promo terms, dates, regions, and disclaimers. "
        "Do not summarize away details; capture all visible text."
    )
    suffix = file_path.suffix.lower()
    image_exts = {".png", ".jpg", ".jpeg", ".webp"}

    if suffix == ".pdf":
        uploaded = client.files.create(file=file_path.open("rb"), purpose="assistants")
        resp = client.responses.create(
            model=MODEL_VISION,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_file", "file_id": uploaded.id},
                    ],
                }
            ],
            max_output_tokens=MAX_OUTPUT_TOKENS,
        )
        return resp.output_text

    if suffix in image_exts:
        import base64

        mime = f"image/{'jpeg' if suffix in {'.jpg', '.jpeg'} else suffix.lstrip('.')}"
        data = file_path.read_bytes()
        b64 = base64.b64encode(data).decode("ascii")
        resp = client.responses.create(
            model=MODEL_VISION,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_image", "image_url": f"data:{mime};base64,{b64}"},
                    ],
                }
            ],
            max_output_tokens=MAX_OUTPUT_TOKENS,
        )
        return resp.output_text

    raise ValueError(f"Unsupported file type for extraction: {suffix}")


def build_chunks_for_file(client: OpenAI, path: pathlib.Path, file_hash: str, log_steps_enabled: bool) -> List[Dict]:
    log_step(log_steps_enabled, f"Extracting text: {path.name}")
    raw_text = extract_with_gpt(client, path)
    log_step(log_steps_enabled, f"Extracted {len(raw_text)} chars from {path.name}")
    chunks = chunk_text(raw_text, CHUNK_CHAR_LEN)
    log_step(log_steps_enabled, f"Chunked {path.name} into {len(chunks)} chunk(s)")
    records = []
    for idx, chunk in enumerate(chunks):
        chunk_id = hashlib.sha256(f"{path}|{file_hash}|{idx}".encode()).hexdigest()
        records.append(
            {
                "id": chunk_id,
                "chunk_index": idx,
                "source_path": str(path),
                "source_hash": file_hash,
                "text": chunk,
                "metadata": {
                    "source_file": path.name,
                    "ingested_at": datetime.datetime.utcnow().isoformat(),
                },
            }
        )
    return records


def upload_chunks_jsonl(client: OpenAI, vector_store_id: str, records: List[Dict]) -> List[str]:
    if not records:
        return []
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp:
        for rec in records:
            tmp.write(json.dumps({"text": rec["text"], **rec["metadata"]}) + "\n")
        tmp_path = tmp.name
    try:
        vs_file = client.vector_stores.files.upload_and_poll(
            vector_store_id=vector_store_id,
            file=open(tmp_path, "rb"),
        )
        file_id = getattr(vs_file, "id", None)
        return [file_id] if file_id else []
    finally:
        os.remove(tmp_path)


# ---------- Crawl ----------
class NotionCrawler:
    def __init__(
        self,
        client: Client,
        root_page_id: Optional[str],
        max_pages: Optional[int],
        page_filter: Optional[set],
        log_every_pages: int = 25,
        log_every_files: int = 200,
        log_details: bool = False,
    ):
        self.client = client
        self.root_page_id = root_page_id
        self.max_pages = max_pages
        self.page_filter = page_filter
        self.visited_pages = set()
        self.visited_dbs = set()
        self.database_data_sources = {}
        self.pages_seen = 0
        self.file_count = 0
        self.log_every_pages = log_every_pages
        self.log_every_files = log_every_files
        self.log_details = log_details
        self.logged_pages = set()

    def log_page_progress(self):
        if self.log_every_pages and self.pages_seen % self.log_every_pages == 0:
            print(f"INFO: Crawl progress - pages_seen={self.pages_seen}, sources_found={self.file_count}")

    def log_file_progress(self):
        if self.log_every_files and self.file_count % self.log_every_files == 0:
            print(f"INFO: Crawl progress - pages_seen={self.pages_seen}, sources_found={self.file_count}")

    def log_page_header(self, page_id: str, page_title: str):
        if not self.log_details:
            return
        if page_id in self.logged_pages:
            return
        self.logged_pages.add(page_id)
        print(f"PAGE: {page_title} ({page_id})")

    def log_source_detail(
        self,
        page_id: str,
        page_title: str,
        display_name: Optional[str],
        url: str,
        source_type: str,
        notion_type: str,
        depth: int = 1,
    ):
        if not self.log_details:
            return
        self.log_page_header(page_id, page_title)
        name = display_name or display_name_from_url(url)
        indent = "  " * max(depth, 1)
        kind = f"{source_type}:{notion_type}"
        print(f"{indent}- {name} [{kind}] {url}")

    def query_database(self, database_id: str, cursor: Optional[str]):
        if hasattr(self.client.databases, "query"):
            return self.client.databases.query(
                database_id=database_id,
                start_cursor=cursor,
                page_size=100,
            )
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        return self.client.request(
            path=f"databases/{database_id}/query",
            method="POST",
            body=body,
        )

    def get_database_data_source_ids(self, database_id: str) -> List[str]:
        if database_id in self.database_data_sources:
            return self.database_data_sources[database_id]
        db = self.client.databases.retrieve(database_id=database_id)
        ids: List[str] = []
        ds_list = db.get("data_sources")
        if isinstance(ds_list, list):
            for ds in ds_list:
                if isinstance(ds, dict) and ds.get("id"):
                    ids.append(ds["id"])
        if not ids:
            ds_id = db.get("data_source_id") or (db.get("data_source") or {}).get("id")
            if ds_id:
                ids.append(ds_id)
        self.database_data_sources[database_id] = ids
        return ids

    def iter_database_pages(self, database_id: str) -> Iterable[dict]:
        if hasattr(self.client, "data_sources") and hasattr(self.client.data_sources, "query"):
            ds_ids = self.get_database_data_source_ids(database_id)
            if ds_ids:
                for ds_id in ds_ids:
                    cursor = None
                    while True:
                        resp = self.client.data_sources.query(
                            data_source_id=ds_id,
                            start_cursor=cursor,
                            page_size=100,
                        )
                        for page in resp.get("results", []):
                            yield page
                        if not resp.get("has_more"):
                            break
                        cursor = resp.get("next_cursor")
                return
        cursor = None
        while True:
            resp = self.query_database(database_id, cursor)
            for page in resp.get("results", []):
                yield page
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")

    def fetch_accessible_pages(self) -> List[dict]:
        pages = []
        cursor = None
        while True:
            resp = self.client.search(filter={"property": "object", "value": "page"}, start_cursor=cursor, page_size=100)
            pages.extend(resp["results"])
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        if self.root_page_id:
            pages.append({"id": self.root_page_id})
        return pages

    def crawl(self, max_files: Optional[int]) -> Iterable[dict]:
        pages = self.fetch_accessible_pages()
        print(f"INFO: Search returned {len(pages)} page(s).")
        for page in pages:
            pid = page["id"]
            if self.page_filter and pid not in self.page_filter:
                continue
            yield from self.crawl_page(pid, max_files)
            if self.max_pages and self.pages_seen >= self.max_pages:
                break

    def crawl_page(self, page_id: str, max_files: Optional[int]) -> Iterable[dict]:
        if page_id in self.visited_pages:
            return
        self.visited_pages.add(page_id)
        self.pages_seen += 1
        self.log_page_progress()
        try:
            page_obj = self.client.pages.retrieve(page_id=page_id)
        except APIResponseError as exc:
            print(f"⚠️  Skipping page {page_id}: {format_api_error(exc)}")
            return
        except Exception as exc:
            print(f"⚠️  Skipping page {page_id}: {exc}")
            return
        page_title = page_title_from_properties(page_obj)
        self.log_page_header(page_id, page_title)
        yield from self.crawl_blocks(page_id, page_title, max_files, root_page_id=page_id)

    def crawl_database(self, database_id: str, max_files: Optional[int]) -> Iterable[dict]:
        if database_id in self.visited_dbs:
            return
        self.visited_dbs.add(database_id)
        try:
            for page in self.iter_database_pages(database_id):
                pid = page["id"]
                page_title = page_title_from_properties(page)
                self.log_page_header(pid, page_title)
                yield from self.crawl_blocks(pid, page_title, max_files, root_page_id=pid)
                if self.max_pages and self.pages_seen >= self.max_pages:
                    return
                self.pages_seen += 1
                self.log_page_progress()
        except APIResponseError as exc:
            print(f"⚠️  Skipping database {database_id}: {format_api_error(exc)}")
            return
        except Exception as exc:
            print(f"⚠️  Skipping database {database_id}: {exc}")
            return

    def crawl_blocks(
        self,
        block_id: str,
        page_title: str,
        max_files: Optional[int],
        depth: int = 0,
        root_page_id: Optional[str] = None,
    ) -> Iterable[dict]:
        cursor = None
        while True:
            try:
                resp = self.client.blocks.children.list(block_id=block_id, start_cursor=cursor, page_size=100)
            except APIResponseError as exc:
                print(f"⚠️  Skipping blocks for {block_id}: {format_api_error(exc)}")
                return
            except Exception as exc:
                print(f"⚠️  Skipping blocks for {block_id}: {exc}")
                return
            for block in resp["results"]:
                btype = block.get("type")
                file_info = file_info_from_block(block)
                if file_info:
                    parent = block.get("parent", {})
                    page_ref = parent.get("page_id") or parent.get("database_id") or parent.get("block_id") or block_id
                    log_page_id = root_page_id or page_ref
                    if max_files and self.file_count >= max_files:
                        return
                    self.file_count += 1
                    self.log_file_progress()
                    self.log_source_detail(
                        log_page_id,
                        page_title,
                        file_info.get("display_name"),
                        file_info["url"],
                        "block",
                        file_info.get("notion_type") or "unknown",
                        depth=depth + 1,
                    )
                    yield {
                        "source_type": "block",
                        "part_id": block["id"],
                        "block_id": block["id"],
                        "url": file_info["url"],
                        "filename": file_info["filename"],
                        "display_name": file_info.get("display_name"),
                        "notion_type": file_info.get("notion_type"),
                        "page_title": page_title,
                        "last_edited_time": block.get("last_edited_time"),
                        "page_id": page_ref,
                    }

                if btype == "child_page":
                    title = block["child_page"]["title"]
                    yield from self.crawl_page(block["id"], max_files)
                elif btype == "child_database":
                    yield from self.crawl_database(block["id"], max_files)
                elif btype == "link_to_page":
                    target = block["link_to_page"]
                    if target["type"] == "page_id":
                        yield from self.crawl_page(target["page_id"], max_files)
                    elif target["type"] == "database_id":
                        yield from self.crawl_database(target["database_id"], max_files)
                elif block.get("has_children"):
                    yield from self.crawl_blocks(block["id"], page_title, max_files, depth + 1, root_page_id=root_page_id)
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")


# ---------- Main ETL ----------
def ensure_vector_store(client: OpenAI, manifest: Dict) -> str:
    vs_id = manifest.get("vector_store_id")
    if vs_id:
        return vs_id
    vs = client.vector_stores.create(name="notion-etl-store")
    manifest["vector_store_id"] = vs.id
    return vs.id


def delete_vs_files(client: OpenAI, vector_store_id: str, file_ids: List[str]):
    for fid in file_ids:
        try:
            client.vector_stores.files.delete(vector_store_id=vector_store_id, file_id=fid)
        except Exception:
            pass


def run_etl(config):
    load_dotenv()
    notion = Client(auth=config.notion_api_key)
    os.environ["OPENAI_API_KEY"] = config.openai_api_key
    openai_client = OpenAI()
    download_dir = pathlib.Path(config.download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(config.manifest_path, config.manifest_s3)
    manifest.setdefault("files", {})
    if config.vector_store_id:
        manifest["vector_store_id"] = config.vector_store_id
    vector_store_id = ensure_vector_store(openai_client, manifest) if not config.dry_run else manifest.get("vector_store_id")

    try:
        root_id = resolve_root_page_id(config.root_page_id, config.root_page_url)
    except ValueError:
        raise SystemExit(
            "ROOT_PAGE_ID must be 32 hex chars (with or without hyphens). "
            "If you only have a URL, set ROOT_PAGE_URL instead."
        )

    def int_env(name: str, default: int) -> int:
        val = os.environ.get(name)
        if not val:
            return default
        try:
            return int(val)
        except ValueError:
            return default

    progress_pages = int_env("PROGRESS_PAGE_EVERY", 25)
    progress_files = int_env("PROGRESS_FILE_EVERY", 200)
    progress_vectors = int_env("PROGRESS_VECTOR_EVERY", 5)
    log_steps_enabled = config.log_steps
    if config.max_workers and config.max_workers != 1:
        print(f"INFO: Sequential mode enabled; ignoring max_workers={config.max_workers}.")

    page_filter = set(config.page_id_filter.split(",")) if config.page_id_filter else None
    crawler = NotionCrawler(
        client=notion,
        root_page_id=root_id,
        max_pages=config.max_pages,
        page_filter=page_filter,
        log_every_pages=progress_pages,
        log_every_files=progress_files,
        log_details=config.log_details,
    )

    log_step(
        log_steps_enabled,
        "Starting sequential crawl "
        f"(max_pages={config.max_pages or 'none'}, max_files={config.max_files or 'none'}, dry_run={config.dry_run}).",
    )

    session = requests.Session()
    processed = 0
    skipped = 0
    failed = 0
    vectorized = 0

    def process_source(src: dict) -> Tuple[str, bool]:
        block_id = src.get("block_id") or src.get("part_id")
        if not block_id:
            print(f"⚠️  Skipping source missing block id: {src.get('url')}")
            return "skipped", False

        entry = manifest.get("files", {}).get(block_id, {})
        filename = entry.get("filename") or src.get("filename") or display_name_from_url(src["url"])
        filename = safe_filename(filename)
        path = download_dir / filename
        last_edited_time = src.get("last_edited_time")
        url = src["url"]

        unchanged = entry and entry.get("last_edited_time") == last_edited_time and entry.get("vector_file_ids")
        if unchanged and not (config.persist_downloads and not path.exists()):
            log_step(log_steps_enabled, f"Cache hit; skipping unchanged file: {filename}")
            return "skipped", False

        if not path.exists() or not entry or entry.get("last_edited_time") != last_edited_time:
            log_step(log_steps_enabled, f"Downloading: {filename}")
            download_with_retries(session, url, path)

        file_hash = hash_file(path)
        vector_file_ids = entry.get("vector_file_ids", [])
        needs_vector = (
            not vector_file_ids
            or entry.get("hash") != file_hash
            or entry.get("last_edited_time") != last_edited_time
        )

        did_vectorize = False
        if needs_vector:
            if entry and vector_file_ids:
                delete_vs_files(openai_client, vector_store_id, vector_file_ids)
            records = build_chunks_for_file(openai_client, path, file_hash, log_steps_enabled)
            log_step(log_steps_enabled, f"Uploading {len(records)} chunk(s) for {filename}")
            vector_file_ids = upload_chunks_jsonl(openai_client, vector_store_id, records)
            did_vectorize = True
        else:
            log_step(log_steps_enabled, f"Skipping vectorization (unchanged): {filename}")

        manifest.setdefault("files", {})[block_id] = {
            "filename": filename,
            "last_edited_time": last_edited_time,
            "source_url": url,
            "hash": file_hash,
            "vector_file_ids": vector_file_ids,
            "page_id": src.get("page_id"),
            "page_title": src.get("page_title"),
            "notion_type": src.get("notion_type"),
            "display_name": src.get("display_name"),
            "updated_at": datetime.datetime.utcnow().isoformat(),
        }
        save_manifest(manifest, config.manifest_path, config.manifest_s3)

        if not config.persist_downloads:
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        return "processed", did_vectorize

    for src in crawler.crawl(max_files=config.max_files):
        if log_steps_enabled:
            label = src.get("filename") or src.get("display_name") or display_name_from_url(src.get("url", ""))
            page_label = src.get("page_title") or src.get("page_id") or "unknown"
            log_step(log_steps_enabled, f"Found file: {label} (page={page_label})")
        if config.dry_run:
            print(f"DRY RUN: {src.get('filename') or src.get('display_name')} ({src.get('url')})")
            continue
        try:
            if log_steps_enabled:
                log_step(log_steps_enabled, f"Processing: {src.get('filename') or src.get('display_name')}")
            status, did_vectorize = process_source(src)
            if status == "skipped":
                skipped += 1
            else:
                processed += 1
            if did_vectorize:
                vectorized += 1
        except Exception as exc:
            failed += 1
            print(f"❌ Failed processing {src.get('url')}: {exc}")
        total_seen = processed + skipped + failed
        if progress_vectors and total_seen % progress_vectors == 0:
            print(
                "INFO: Progress "
                f"total={total_seen}, processed={processed}, skipped={skipped}, failed={failed}, vectorized={vectorized}"
            )

    if not config.dry_run:
        save_manifest(manifest, config.manifest_path, config.manifest_s3)
    log_step(
        log_steps_enabled,
        "Crawl complete "
        f"pages_seen={crawler.pages_seen}, files_seen={crawler.file_count}, "
        f"processed={processed}, skipped={skipped}, failed={failed}, vectorized={vectorized}.",
    )
    if config.dry_run:
        print("Dry run complete.")


# ---------- CLI ----------
def build_arg_parser():
    parser = argparse.ArgumentParser(description="Sequential Notion -> Vector Store ETL")
    parser.add_argument("--dry-run", action="store_true", default=parse_bool(os.environ.get("DRY_RUN"), False))
    parser.add_argument("--max-pages", type=int, default=int(os.environ.get("MAX_PAGES", "0") or 0))
    parser.add_argument("--max-files", type=int, default=int(os.environ.get("MAX_FILES", "0") or 0))
    parser.add_argument("--page-id-filter", type=str, default=os.environ.get("PAGE_ID_FILTER"))
    parser.add_argument(
        "--log-steps",
        dest="log_steps",
        action="store_true",
        default=parse_bool(os.environ.get("LOG_STEPS"), True),
    )
    parser.add_argument("--no-log-steps", dest="log_steps", action="store_false")
    parser.add_argument(
        "--log-details",
        dest="log_details",
        action="store_true",
        default=parse_bool(os.environ.get("LOG_DETAILS"), False),
    )
    parser.add_argument("--no-log-details", dest="log_details", action="store_false")
    parser.add_argument("--root-page-id", type=str, default=os.environ.get("ROOT_PAGE_ID"))
    parser.add_argument("--root-page-url", type=str, default=os.environ.get("ROOT_PAGE_URL"))
    parser.add_argument("--manifest-path", type=str, default=os.environ.get("MANIFEST_PATH", "CACHE"))
    parser.add_argument("--manifest-s3", type=str, default=os.environ.get("MANIFEST_S3"))
    parser.add_argument("--download-dir", type=str, default=os.environ.get("DOWNLOAD_DIR", "notion_downloads_v2"))
    parser.add_argument("--max-workers", type=int, default=int(os.environ.get("MAX_WORKERS", "1")))
    parser.add_argument("--persist-downloads", action="store_true", default=parse_bool(os.environ.get("PERSIST_DOWNLOADS"), False))
    parser.add_argument("--notion-api-key", type=str, default=os.environ.get("NOTION_API_KEY"))
    parser.add_argument("--openai-api-key", type=str, default=os.environ.get("OPENAI_API_KEY"))
    parser.add_argument("--vector-store-id", type=str, default=os.environ.get("VECTOR_STORE_ID"))
    return parser


def parse_config():
    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args()
    if not args.notion_api_key:
        raise SystemExit("NOTION_API_KEY is required.")
    if not args.openai_api_key:
        raise SystemExit("OPENAI_API_KEY is required.")
    args.manifest_path = pathlib.Path(args.manifest_path)
    args.max_pages = args.max_pages or None
    args.max_files = args.max_files or None
    return args


if __name__ == "__main__":
    cfg = parse_config()
    run_etl(cfg)
