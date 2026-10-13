"""
Yakumo Yukari - Discord Bot (discord.py + Gemini + Vector Memory MongoDB/SQLite + Global Memory)

Biến môi trường:
  Bắt buộc     : DISCORD_TOKEN, GEMINI_API_KEY
  Khuyến nghị  : OWNER_ID (ID số Discord của chủ nhân; bỏ trống thì dùng chủ sở hữu ứng dụng Discord),
                 MONGO_URI (lưu ký ức vĩnh viễn trên MongoDB Atlas)
  Nhân vật     : PERSONA_MAX_CHARS (1500), PERSONA_CACHE_TTL_SEC (300), PROMPT_OPERATIONAL (0 hoặc 1)
  Ký ức cá nhân: MEMORY_WINDOW_DAYS (30; 0 = không giới hạn), MEMORY_RETENTION_DAYS (0 = không tự xóa),
                 MEMORY_THRESHOLD (cosine thật, mặc định 0.55), EMBEDDING_MODEL, EMBEDDING_DIM (0 = mặc định model)
  Global Memory: GLOBAL_TOP_K (3), GLOBAL_MEMORY_THRESHOLD (mặc định = MEMORY_THRESHOLD), GLOBAL_CHUNK_CHARS (900),
                 GLOBAL_CHUNK_OVERLAP (100), GLOBAL_MAX_FILE_BYTES (300000), GLOBAL_MAX_CHUNKS (400 mỗi file),
                 GLOBAL_MAX_TOTAL_CHUNKS (2000 tổng cộng), GLOBAL_EMBED_BATCH (16), GLOBAL_CACHE_TTL_SEC (300)
  Gemini       : GEMINI_MODELS (cách nhau bằng dấu phẩy), GEMINI_TIMEOUT, MAX_CONCURRENT_AI
  Chống ban IP : USER_COOLDOWN_SEC, GLOBAL_MAX_REPLIES, GLOBAL_WINDOW_SEC, CHANNEL_MUTE_SEC,
                 GLOBAL_BACKOFF_SEC, LOGIN_BACKOFF_SEC (mặc định 900), DISCORD_PROXY
  Khác         : VECTOR_INDEX_NAME, SQLITE_DB_PATH, PORT

Lệnh chủ nhân: /globalmem_add|list|delete|clear|test, /persona_set|list|show|delete, /sync
"""
import os
import re
import math
import json
import time
import asyncio
import logging
import sqlite3
import uuid
import threading
from array import array
from collections import OrderedDict, deque
from contextlib import asynccontextmanager, closing
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

from google import genai
from google.genai import types

load_dotenv()

log = logging.getLogger("yukari")


# ==============================================================================
# CẤU HÌNH HỆ THỐNG
# ==============================================================================
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
VECTOR_INDEX_NAME = os.getenv("VECTOR_INDEX_NAME", "vector_index")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "yukari_memories.db")

OWNER_ID = _env_int("OWNER_ID", 0)

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "gemini-embedding-001")
EMBEDDING_DIM = _env_int("EMBEDDING_DIM", 0)

_DEFAULT_MODELS = "gemini-2.5-flash,gemini-2.5-pro,gemini-2.0-flash,gemini-1.5-flash"
GEMINI_MODELS = [m.strip() for m in os.getenv("GEMINI_MODELS", _DEFAULT_MODELS).split(",") if m.strip()]
if not GEMINI_MODELS:
    GEMINI_MODELS = [m.strip() for m in _DEFAULT_MODELS.split(",")]
MAX_RETRIES_PER_MODEL = 2
GEMINI_TIMEOUT = _env_float("GEMINI_TIMEOUT", 35.0)

MEMORY_WINDOW_DAYS = _env_int("MEMORY_WINDOW_DAYS", 30)
MEMORY_RETENTION_DAYS = _env_int("MEMORY_RETENTION_DAYS", 0)
MEMORY_THRESHOLD = _env_float("MEMORY_THRESHOLD", 0.55)
MEMORY_TOP_K = 3
MEMORY_MIN_CHARS = 10
MEMORY_REPLY_MAX_CHARS = 1500
MEMORY_SCAN_LIMIT = 100

MAX_INPUT_CHARS = 1500
HISTORY_MAX_LINES = 8
HISTORY_PROMPT_LINES = 6
HISTORY_MAX_KEYS = 500
DISCORD_MSG_LIMIT = 1950

USER_COOLDOWN_SEC = _env_float("USER_COOLDOWN_SEC", 2.0)
MAX_CONCURRENT_AI = _env_int("MAX_CONCURRENT_AI", 4)

RATE_MAX_REPLIES = _env_int("GLOBAL_MAX_REPLIES", 30)
RATE_WINDOW_SEC = _env_int("GLOBAL_WINDOW_SEC", 60)
CHANNEL_MUTE_SEC = _env_int("CHANNEL_MUTE_SEC", 600)
RATE_BACKOFF_SEC = _env_int("GLOBAL_BACKOFF_SEC", 600)
LOGIN_BACKOFF_SEC = _env_int("LOGIN_BACKOFF_SEC", 900)
DISCORD_PROXY = os.getenv("DISCORD_PROXY") or None

PERSONA_MAX_CHARS = _env_int("PERSONA_MAX_CHARS", 1500)
PERSONA_CACHE_TTL_SEC = _env_int("PERSONA_CACHE_TTL_SEC", 300)
PROMPT_OPERATIONAL = _env_int("PROMPT_OPERATIONAL", 1) != 0

BUILTIN_PERSONAS = {
    "1082184769695133716": (
        "Cha",
        "Người nói CHÍNH LÀ Cha Namaru — người mà Yukari dành tình cảm sâu đậm và tôn kính nhất. "
        "Xưng con gọi cha và đối đãi với cha đúng như phần mô tả về Namaru trong mục core_dynamics.",
    ),
}

GLOBAL_USER_ID = "__global__"
GLOBAL_CHANNEL_ID = "__global__"
LEGACY_GLOBAL_USER_ID = "GLOBAL"
LEGACY_SOURCE_PREFIX = "Knowledge:"
GLOBAL_TOP_K = _env_int("GLOBAL_TOP_K", 3)
GLOBAL_THRESHOLD = _env_float("GLOBAL_MEMORY_THRESHOLD", MEMORY_THRESHOLD)
GLOBAL_CHUNK_CHARS = _env_int("GLOBAL_CHUNK_CHARS", 900)
GLOBAL_CHUNK_OVERLAP = _env_int("GLOBAL_CHUNK_OVERLAP", 100)
GLOBAL_MAX_FILE_BYTES = _env_int("GLOBAL_MAX_FILE_BYTES", 300_000)
GLOBAL_MAX_CHUNKS = _env_int("GLOBAL_MAX_CHUNKS", 400)
GLOBAL_MAX_TOTAL_CHUNKS = _env_int("GLOBAL_MAX_TOTAL_CHUNKS", 2000)
GLOBAL_EMBED_BATCH = _env_int("GLOBAL_EMBED_BATCH", 16)
GLOBAL_CACHE_TTL_SEC = _env_int("GLOBAL_CACHE_TTL_SEC", 300)
GLOBAL_UPLOAD_DEADLINE_SEC = 780
GLOBAL_ALLOWED_EXT = (".txt", ".md", ".markdown", ".csv", ".json", ".log")
EMBED_RETRY_BASE_SEC = 5
EMBED_QUOTA_WAIT_SEC = 30
ATLAS_RETRY_SEC = 600

# Phản hồi mặc định chuẩn phong thái Yukari Yakumo
MSG_EMPTY = "*ngáp dài một hơi, phe phẩy quạt xếp che nửa miệng*\nGiấc mộng vừa rồi trống rỗng quá... Ngươi định nói gì với ta thế?"
MSG_SAFETY = "*mắt đỏ trong khe nứt khẽ chớp, nụ cười nhạt hiện lên sau vành dù ren*\nRanh giới ấy không cho phép ngươi vượt qua đâu."
MSG_QUOTA = "*thu quạt lại, thở dài nhẹ nhàng*\nKhoảng không gian này tạm thời cạn kiệt năng lượng rồi... Đợi ta nghỉ ngơi một chút nhé."
MSG_QUOTA_LONG = "*khe nứt sukima khép hờ, bóng dáng mờ dần*\nTa thấy hơi mệt và muốn ngủ đông một chút đây... Hãy kiên nhẫn đợi ranh giới hồi phục nhé."
MSG_ERROR = "*lục lọi trong sukima một hồi nhưng chỉ thấy toàn gối ngủ*\n...Đợi ta nghĩ ngợi chút... hmmmm."

use_mongo = False
memories_collection = None
custom_prompts_collection = None
_atlas_disabled_until = 0.0

ai: Optional[genai.Client] = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


# ==============================================================================
# LÀM SẠCH VĂN BẢN & BẢO MẬT DANH TÍNH (ANTI-IMPERSONATION GUARD)
# ==============================================================================
# Lọc bỏ: ký tự điều khiển ASCII, vô hình Unicode, RTL Override, và các thẻ cấu trúc prompt
_UNSAFE_NAME_RE = re.compile(
    r"[\x00-\x1f\x7f-\x9f"
    r"\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff"
    r"<>\[\]{}`*_~|@#\\]"
)


def sanitize_name(name: Optional[str], limit: int = 32) -> str:
    """Tầng 1: Làm sạch toàn bộ ký tự rác, ký tự điều khiển, ngăn phá vỡ prompt và markdown."""
    cleaned = _UNSAFE_NAME_RE.sub(" ", name or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:limit] or "Người lạ"


def resolve_author_identity(user_id: int, display_name: str, is_owner: bool) -> tuple[str, bool]:
    """
    Tầng 2: Chống mạo danh ngữ nghĩa (Semantic Impersonation Guard).
    Ngăn chặn việc kẻ xấu đổi tên thành Namaru/Cha để lừa AI.
    Trả về: (tên_sau_xử_lý, cờ_phát_hiện_mạo_danh)
    """
    clean_name = sanitize_name(display_name)
    if is_owner:
        return clean_name, False

    name_lower = clean_name.lower()
    is_imposter = False

    # Kiểm tra xem tên có cố tình mạo danh Cha Namaru không
    if "namaru" in name_lower:
        is_imposter = True
    else:
        # Bắt các danh xưng thiêng liêng ở dạng từ độc lập để tránh bắt nhầm từ ghép vô hại
        tokens = set(re.findall(r"\w+", name_lower))
        if tokens.intersection({"cha", "father", "papa", "ba", "bố"}):
            is_imposter = True

    if is_imposter:
        masked_name = f"Kẻ-Giả-Mạo-{clean_name}"
        return masked_name, True

    return clean_name, False


def sanitize_source(name: Optional[str], limit: int = 80) -> str:
    """Làm sạch tên file/nguồn tài liệu cho Global Memory."""
    cleaned = re.sub(r"[\x00-\x1f\x7f-\x9f/\\:*?\"<>|`]", "_", name or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:limit] or "tai-lieu"


def cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    """Tính độ tương đồng cosin chuẩn xác giữa 2 vector."""
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot_product = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot_product / (norm_a * norm_b)


def truncate_for_discord(text: str, limit: int = DISCORD_MSG_LIMIT) -> str:
    """Cắt văn bản đảm bảo vẹn toàn câu và đóng cặp dấu sao hành động của Yukari."""
    if len(text) <= limit:
        return text
    cut = text[:limit]
    idx = max(cut.rfind("\n"), cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if idx > limit * 0.6:
        cut = cut[: idx + 1]
    if cut.count("*") % 2 == 1:
        cut += "*"
    return cut.rstrip() + "…"


_bg_tasks: set = set()


def spawn(coro) -> asyncio.Task:
    """Quản lý tác vụ nền không bị Garbage Collection dọn giữa chừng."""
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task


# ==============================================================================
# DATABASE LAYER (SQLITE DỰ PHÒNG + MONGODB ATLAS)
# ==============================================================================
def _sqlite_connect() -> sqlite3.Connection:
    return sqlite3.connect(SQLITE_DB_PATH, timeout=10)


def _ts_sqlite(dt: datetime) -> str:
    return dt.isoformat(sep=" ", timespec="microseconds")


def init_sqlite():
    try:
        with closing(_sqlite_connect()) as conn:
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS vector_memories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT,
                        channel_id TEXT,
                        author_name TEXT,
                        text TEXT,
                        embedding TEXT,
                        timestamp TEXT
                    )
                """)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_vm_user_channel ON vector_memories(user_id, channel_id, id)"
                )
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS global_memories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT NOT NULL,
                        upload_id TEXT NOT NULL,
                        chunk_index INTEGER,
                        text TEXT NOT NULL,
                        embedding TEXT NOT NULL,
                        created_at TEXT
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_gm_source ON global_memories(source)")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS custom_prompts (
                        user_id TEXT PRIMARY KEY,
                        label TEXT,
                        prompt TEXT NOT NULL,
                        updated_at TEXT
                    )
                """)
                if MEMORY_RETENTION_DAYS > 0:
                    cutoff = datetime.now(timezone.utc) - timedelta(days=MEMORY_RETENTION_DAYS)
                    conn.execute(
                        "DELETE FROM vector_memories WHERE user_id NOT IN (?, ?) AND timestamp < ?",
                        (GLOBAL_USER_ID, LEGACY_GLOBAL_USER_ID, _ts_sqlite(cutoff)),
                    )
        log.info("📁 Đã khởi tạo cơ sở dữ liệu SQLite dự phòng sẵn sàng.")
    except Exception as e:
        log.warning("⚠️ Lỗi khởi tạo SQLite: %s", e)


def init_mongo():
    global use_mongo, memories_collection, custom_prompts_collection
    if not MONGO_URI:
        return
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        memories_collection = client["yukari_db"]["vector_memories"]
        custom_prompts_collection = client["yukari_db"]["custom_prompts"]
        use_mongo = True
        log.info("✅ Đã kết nối thành công MongoDB Atlas! Ký ức được đồng bộ trên đám mây.")
    except Exception as e:
        log.warning("⚠️ Không thể kết nối MongoDB (%s), tiếp tục dùng SQLite cục bộ.", e)
        use_mongo = False
        return

    try:
        memories_collection.create_index([("user_id", 1), ("channel_id", 1), ("timestamp", -1)])
        if MEMORY_RETENTION_DAYS > 0:
            memories_collection.create_index(
                "timestamp",
                expireAfterSeconds=MEMORY_RETENTION_DAYS * 86400,
                name="ttl_timestamp",
            )
    except Exception as e:
        log.warning("ℹ️ Index MongoDB có thể đã tồn tại từ trước: %s", e)


# ==============================================================================
# GEMINI GENERATION & EMBEDDING ENGINE
# ==============================================================================
_ai_sem: Optional[asyncio.Semaphore] = None


def _get_ai_sem() -> asyncio.Semaphore:
    global _ai_sem
    if _ai_sem is None:
        _ai_sem = asyncio.Semaphore(max(1, MAX_CONCURRENT_AI))
    return _ai_sem


def _classify_gemini_error(e: Exception) -> str:
    code = getattr(e, "code", None)
    text = str(e).upper()
    if code == 429 or "429" in text or "RESOURCE_EXHAUSTED" in text:
        return "quota"
    if code == 404 or "404" in text or "NOT_FOUND" in text:
        return "missing"
    if code in (500, 502, 503, 504) or "503" in text or "UNAVAILABLE" in text or "DEADLINE" in text:
        return "retry"
    return "other"


def _extract_text(resp) -> tuple[Optional[str], bool]:
    if resp is None:
        return None, False
    try:
        text = resp.text
    except Exception:
        text = None
    if text and text.strip():
        return text.strip(), False

    blocked = False
    feedback = getattr(resp, "prompt_feedback", None)
    if feedback is not None and getattr(feedback, "block_reason", None):
        blocked = True
    for cand in (getattr(resp, "candidates", None) or []):
        reason = str(getattr(cand, "finish_reason", "") or "").upper()
        if any(k in reason for k in ("SAFETY", "PROHIBITED", "BLOCKLIST", "SPII")):
            blocked = True
    return None, blocked


async def ask_gemini(contents, system_instruction, temperature=0.9) -> tuple[str, bool]:
    """Gọi Gemini với cơ chế retry và fallback danh sách model chuẩn xác."""
    config = types.GenerateContentConfig(system_instruction=system_instruction, temperature=temperature)
    async with _get_ai_sem():
        for model_name in GEMINI_MODELS:
            for attempt in range(MAX_RETRIES_PER_MODEL):
                try:
                    resp = await asyncio.wait_for(
                        ai.aio.models.generate_content(model=model_name, contents=contents, config=config),
                        timeout=GEMINI_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    log.warning("Model %s quá thời gian chờ (%ss), chuyển model.", model_name, GEMINI_TIMEOUT)
                    break
                except Exception as e:
                    kind = _classify_gemini_error(e)
                    log.warning("Model %s lỗi (%s, lần %d): %s", model_name, kind, attempt + 1, str(e)[:150])
                    if kind == "retry" and attempt == 0:
                        await asyncio.sleep(1.0)
                        continue
                    break

                text, blocked = _extract_text(resp)
                if text:
                    return text, True
                return (MSG_SAFETY if blocked else MSG_EMPTY), False
    return MSG_QUOTA, False


async def get_embedding(text: str, task_type: str = "RETRIEVAL_DOCUMENT") -> list[float]:
    if not text or not text.strip():
        return []
    try:
        cfg = {"task_type": task_type}
        if EMBEDDING_DIM > 0:
            cfg["output_dimensionality"] = EMBEDDING_DIM
        resp = await asyncio.wait_for(
            ai.aio.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=text,
                config=types.EmbedContentConfig(**cfg),
            ),
            timeout=GEMINI_TIMEOUT,
        )
        if resp and resp.embeddings:
            return list(resp.embeddings[0].values)
    except Exception as e:
        log.warning("⚠️ Lỗi tạo embedding (%s): %s", task_type, e)
    return []


# ==============================================================================
# VECTOR STORE & MEMORY RECALL
# ==============================================================================
async def store_context_memory(user_id: str, channel_id: str, author_name: str, user_text: str, reply_text: str):
    user_clean = user_text.strip()
    if len(user_clean) < MEMORY_MIN_CHARS or user_clean.startswith(("!", "/")):
        return

    reply_clean = reply_text.strip()
    if len(reply_clean) > MEMORY_REPLY_MAX_CHARS:
        reply_clean = reply_clean[:MEMORY_REPLY_MAX_CHARS] + "..."
    memory_text = f"{author_name}: {user_clean}\nYukari: {reply_clean}"

    vector = await get_embedding(memory_text, task_type="RETRIEVAL_DOCUMENT")
    if not vector:
        return

    now = datetime.now(timezone.utc)

    if use_mongo and memories_collection is not None:
        record = {
            "user_id": str(user_id),
            "channel_id": str(channel_id),
            "author_name": author_name,
            "text": memory_text,
            "embedding": vector,
            "timestamp": now,
        }
        try:
            await asyncio.to_thread(memories_collection.insert_one, record)
            log.info("💾 Đã lưu ký ức vào MongoDB (vector %d chiều).", len(vector))
            return
        except Exception as e:
            log.warning("⚠️ Lỗi ghi MongoDB: %s, chuyển sang SQLite.", e)

    def _save_sqlite():
        with closing(_sqlite_connect()) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO vector_memories (user_id, channel_id, author_name, text, embedding, timestamp) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (str(user_id), str(channel_id), author_name, memory_text,
                     json.dumps(vector, separators=(",", ":")), _ts_sqlite(now)),
                )

    try:
        await asyncio.to_thread(_save_sqlite)
        log.info("💾 Đã lưu ký ức vào SQLite cục bộ (vector %d chiều).", len(vector))
    except Exception as e:
        log.warning("⚠️ Lỗi ghi SQLite: %s", e)


def _rank_by_cosine(query_vector, rows, top_k: int, threshold: float) -> list[str]:
    scored = []
    for text_val, emb in rows:
        if not text_val or not emb:
            continue
        sim = cosine_similarity(query_vector, emb)
        if sim >= threshold:
            scored.append((sim, text_val))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [t for _, t in scored[:top_k]]


def _iter_sqlite_rows(rows):
    for text_val, emb_json in rows:
        try:
            yield text_val, json.loads(emb_json)
        except (TypeError, ValueError):
            continue


def _search_sqlite(query_vector, user_id, channel_id, min_date, top_k, threshold) -> list[str]:
    sql = "SELECT text, embedding FROM vector_memories WHERE user_id = ?"
    params = [user_id]
    if channel_id:
        sql += " AND channel_id = ?"
        params.append(channel_id)
    if min_date:
        sql += " AND timestamp >= ?"
        params.append(_ts_sqlite(min_date))
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(MEMORY_SCAN_LIMIT)

    with closing(_sqlite_connect()) as conn:
        rows = conn.execute(sql, params).fetchall()
    return _rank_by_cosine(query_vector, _iter_sqlite_rows(rows), top_k, threshold)


async def _search_mongo(query_vector, user_id, channel_id, min_date, top_k, threshold) -> list[str]:
    global _atlas_disabled_until

    if time.monotonic() >= _atlas_disabled_until:
        mongo_filter = {"user_id": {"$eq": user_id}}
        if channel_id:
            mongo_filter["channel_id"] = {"$eq": channel_id}
        if min_date:
            mongo_filter["timestamp"] = {"$gte": min_date}

        pipeline = [
            {
                "$vectorSearch": {
                    "index": VECTOR_INDEX_NAME,
                    "path": "embedding",
                    "queryVector": query_vector,
                    "numCandidates": 150,
                    "limit": top_k,
                    "filter": mongo_filter,
                }
            },
            {"$project": {"_id": 0, "text": 1, "score": {"$meta": "vectorSearchScore"}}},
        ]

        def _run():
            return list(memories_collection.aggregate(pipeline))

        try:
            docs = await asyncio.to_thread(_run)
            matched = []
            for d in docs:
                cosine = 2.0 * float(d.get("score", 0.0)) - 1.0
                if cosine >= threshold and d.get("text"):
                    matched.append(d["text"])
            if matched:
                return matched
        except Exception as e:
            _atlas_disabled_until = time.monotonic() + ATLAS_RETRY_SEC
            log.info("ℹ️ Atlas $vectorSearch tạm chuyển sang Cosine thủ công (%ss): %s", ATLAS_RETRY_SEC, e)

    def _cosine_search():
        cond = {"user_id": user_id}
        if channel_id:
            cond["channel_id"] = channel_id
        if min_date:
            cond["timestamp"] = {"$gte": min_date}
        cursor = (
            memories_collection.find(cond, {"text": 1, "embedding": 1})
            .sort("timestamp", -1)
            .limit(MEMORY_SCAN_LIMIT)
        )
        return _rank_by_cosine(query_vector, ((d.get("text"), d.get("embedding")) for d in cursor), top_k, threshold)

    try:
        return await asyncio.to_thread(_cosine_search)
    except Exception as e:
        log.warning("⚠️ Lỗi client-side search MongoDB: %s", e)
        return []


async def retrieve_relevant_contexts(
    user_id: str,
    query_text: str,
    channel_id: Optional[str] = None,
    min_date: Optional[datetime] = None,
    top_k: int = 3,
    threshold: float = 0.55,
    query_vector: Optional[list] = None,
) -> list[str]:
    if query_vector is None:
        query_vector = await get_embedding(query_text, task_type="RETRIEVAL_QUERY")
    if not query_vector:
        return []

    if use_mongo and memories_collection is not None:
        matched = await _search_mongo(query_vector, user_id, channel_id, min_date, top_k, threshold)
        if matched:
            return matched

    try:
        return await asyncio.to_thread(_search_sqlite, query_vector, user_id, channel_id, min_date, top_k, threshold)
    except Exception as e:
        log.warning("⚠️ Lỗi tìm kiếm SQLite: %s", e)
        return []


async def delete_user_memories(user_id: str) -> bool:
    if user_id in (GLOBAL_USER_ID, LEGACY_GLOBAL_USER_ID):
        log.warning("Không thể xóa user_id dành riêng của Global Memory.")
        return False
    ok = True
    if use_mongo and memories_collection is not None:
        try:
            await asyncio.to_thread(memories_collection.delete_many, {"user_id": user_id})
        except Exception as e:
            ok = False
            log.warning("Lỗi xóa MongoDB: %s", e)

    def _del_sqlite():
        with closing(_sqlite_connect()) as conn:
            with conn:
                conn.execute("DELETE FROM vector_memories WHERE user_id = ?", (user_id,))

    try:
        await asyncio.to_thread(_del_sqlite)
    except Exception as e:
        ok = False
        log.warning("Lỗi xóa SQLite: %s", e)
    return ok


async def count_user_memories(user_id: str) -> tuple[Optional[int], int]:
    mongo_count: Optional[int] = None
    if use_mongo and memories_collection is not None:
        try:
            mongo_count = await asyncio.to_thread(memories_collection.count_documents, {"user_id": user_id})
        except Exception as e:
            log.warning("Lỗi đếm MongoDB: %s", e)
            mongo_count = 0

    def _count_sqlite():
        with closing(_sqlite_connect()) as conn:
            row = conn.execute("SELECT COUNT(*) FROM vector_memories WHERE user_id = ?", (user_id,)).fetchone()
        return row[0] if row else 0

    try:
        sqlite_count = await asyncio.to_thread(_count_sqlite)
    except Exception as e:
        log.warning("Lỗi đếm SQLite: %s", e)
        sqlite_count = 0
    return mongo_count, sqlite_count


# ==============================================================================
# GLOBAL MEMORY
# ==============================================================================
_DOT = getattr(math, "sumprod", None) or (lambda a, b: sum(x * y for x, y in zip(a, b)))
GLOBAL_USER_IDS = (GLOBAL_USER_ID, LEGACY_GLOBAL_USER_ID)
_global_cache: dict = {"items": [], "sig": None, "expires": 0.0, "retry_after": 0.0, "dirty": True}
_global_cache_lock: Optional[asyncio.Lock] = None
_upload_lock: Optional[asyncio.Lock] = None
_dim_warn_until = 0.0


def _get_cache_lock() -> asyncio.Lock:
    global _global_cache_lock
    if _global_cache_lock is None:
        _global_cache_lock = asyncio.Lock()
    return _global_cache_lock


def _get_upload_lock() -> asyncio.Lock:
    global _upload_lock
    if _upload_lock is None:
        _upload_lock = asyncio.Lock()
    return _upload_lock


def decode_upload(raw: bytes) -> str:
    if b"\x00" in raw:
        raise ValueError("binary")
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ValueError("encoding")


def _split_long(paragraph: str, max_chars: int) -> list:
    out, buf = [], ""
    for sentence in re.split(r"(?<=[.!?…])\s+", paragraph):
        while len(sentence) > max_chars:
            cut = sentence.rfind(" ", 0, max_chars)
            if cut < max_chars * 0.5:
                cut = max_chars
            if buf:
                out.append(buf)
                buf = ""
            out.append(sentence[:cut].strip())
            sentence = sentence[cut:].strip()
        if not sentence:
            continue
        if not buf:
            buf = sentence
        elif len(buf) + 1 + len(sentence) <= max_chars:
            buf += " " + sentence
        else:
            out.append(buf)
            buf = sentence
    if buf:
        out.append(buf)
    return out


def chunk_text(text: str, max_chars: int = 900, overlap: int = 100) -> list:
    max_chars = max(200, max_chars)
    overlap = max(0, min(overlap, max_chars // 3))
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    pieces = []
    for para in re.split(r"\n\s*\n", text):
        para = re.sub(r"[ \t]+", " ", para).strip()
        if not para:
            continue
        pieces.extend([para] if len(para) <= max_chars else _split_long(para, max_chars))

    chunks, buf = [], ""
    for piece in pieces:
        if not buf:
            buf = piece
        elif len(buf) + 2 + len(piece) <= max_chars:
            buf += "\n\n" + piece
        else:
            chunks.append(buf)
            tail = ""
            if overlap:
                tail = buf[-overlap:]
                space = tail.find(" ")
                tail = tail[space + 1:] if space != -1 else ""
            buf = f"{tail}\n\n{piece}" if tail and len(tail) + 2 + len(piece) <= max_chars else piece
    if buf:
        chunks.append(buf)
    return chunks


async def get_embeddings_batch(texts: list) -> list:
    cfg = {"task_type": "RETRIEVAL_DOCUMENT"}
    if EMBEDDING_DIM > 0:
        cfg["output_dimensionality"] = EMBEDDING_DIM
    resp = await asyncio.wait_for(
        ai.aio.models.embed_content(model=EMBEDDING_MODEL, contents=texts, config=types.EmbedContentConfig(**cfg)),
        timeout=GEMINI_TIMEOUT * 2,
    )
    vectors = [list(e.values) for e in (getattr(resp, "embeddings", None) or [])]
    if len(vectors) == len(texts):
        return vectors

    singles = []
    for t in texts:
        vec = await get_embedding(t, task_type="RETRIEVAL_DOCUMENT")
        if not vec:
            raise RuntimeError("Không tạo được embedding cho một đoạn văn bản")
        singles.append(vec)
    return singles


async def embed_chunks_with_retry(chunks: list, on_progress=None, deadline: Optional[float] = None) -> list:
    vectors: list = []
    for i in range(0, len(chunks), max(1, GLOBAL_EMBED_BATCH)):
        batch = chunks[i:i + max(1, GLOBAL_EMBED_BATCH)]
        for attempt in range(1, 5):
            try:
                vectors.extend(await get_embeddings_batch(batch))
                break
            except Exception as e:
                if attempt == 4:
                    raise
                wait = (EMBED_QUOTA_WAIT_SEC if _classify_gemini_error(e) == "quota" else EMBED_RETRY_BASE_SEC) * attempt
                if deadline is not None and time.monotonic() + wait > deadline:
                    raise
                log.warning("Nhúng lô thất bại (lần %d): %s — thử lại sau %ss.", attempt, str(e)[:120], wait)
                await asyncio.sleep(wait)
        if on_progress:
            await on_progress(len(vectors), len(chunks))
    return vectors


def invalidate_global_cache():
    _global_cache["dirty"] = True
    _global_cache["retry_after"] = 0.0


def _global_mongo_on() -> bool:
    return bool(use_mongo and memories_collection is not None)


def _strip_legacy(name: Optional[str], legacy: bool) -> str:
    name = name or "(không tên)"
    if legacy and name.startswith(LEGACY_SOURCE_PREFIX):
        name = name[len(LEGACY_SOURCE_PREFIX):] or "(không tên)"
    return name


def _to_utc(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _loads(emb_json):
    try:
        return json.loads(emb_json)
    except (TypeError, ValueError):
        return None


def _sqlite_delete_source(source: str) -> int:
    with closing(_sqlite_connect()) as conn:
        with conn:
            n = conn.execute("DELETE FROM global_memories WHERE source = ?", (source,)).rowcount
            n += conn.execute(
                "DELETE FROM vector_memories WHERE user_id = ? AND author_name = ?",
                (LEGACY_GLOBAL_USER_ID, LEGACY_SOURCE_PREFIX + source),
            ).rowcount
    return n


async def add_global_memory(source: str, chunks: list, vectors: list) -> tuple:
    upload_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc)

    if _global_mongo_on():
        docs = [
            {
                "user_id": GLOBAL_USER_ID,
                "channel_id": GLOBAL_CHANNEL_ID,
                "author_name": "GLOBAL",
                "scope": "global",
                "source": source,
                "upload_id": upload_id,
                "chunk_index": i,
                "text": chunk,
                "embedding": vec,
                "created_at": now,
            }
            for i, (chunk, vec) in enumerate(zip(chunks, vectors))
        ]

        def _write_mongo() -> int:
            try:
                memories_collection.insert_many(docs)
            except Exception:
                try:
                    memories_collection.delete_many({"user_id": GLOBAL_USER_ID, "upload_id": upload_id})
                except Exception:
                    pass
                raise
            removed = memories_collection.delete_many(
                {"user_id": GLOBAL_USER_ID, "source": source, "upload_id": {"$ne": upload_id}}
            ).deleted_count
            removed += memories_collection.delete_many(
                {"user_id": LEGACY_GLOBAL_USER_ID, "author_name": LEGACY_SOURCE_PREFIX + source}
            ).deleted_count
            return removed

        removed = await asyncio.to_thread(_write_mongo)
        try:
            removed += await asyncio.to_thread(_sqlite_delete_source, source)
        except Exception as e:
            log.warning("Lỗi dọn SQLite bản sao %r: %s", source, e)
        backend = "mongo"
    else:
        rows = [
            (source, upload_id, i, chunk, json.dumps(vec, separators=(",", ":")), _ts_sqlite(now))
            for i, (chunk, vec) in enumerate(zip(chunks, vectors))
        ]

        def _write_sqlite() -> int:
            with closing(_sqlite_connect()) as conn:
                with conn:
                    conn.executemany(
                        "INSERT INTO global_memories (source, upload_id, chunk_index, text, embedding, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)", rows,
                    )
                    removed = conn.execute(
                        "DELETE FROM global_memories WHERE source = ? AND upload_id != ?", (source, upload_id)
                    ).rowcount
                    removed += conn.execute(
                        "DELETE FROM vector_memories WHERE user_id = ? AND author_name = ?",
                        (LEGACY_GLOBAL_USER_ID, LEGACY_SOURCE_PREFIX + source),
                    ).rowcount
            return removed

        removed = await asyncio.to_thread(_write_sqlite)
        backend = "sqlite"

    invalidate_global_cache()
    return len(chunks), removed, backend


async def delete_global_source(source: str) -> int:
    deleted = 0
    if _global_mongo_on():
        def _run() -> int:
            return memories_collection.delete_many({"$or": [
                {"user_id": GLOBAL_USER_ID, "source": source},
                {"user_id": LEGACY_GLOBAL_USER_ID, "author_name": LEGACY_SOURCE_PREFIX + source},
            ]}).deleted_count
        deleted += await asyncio.to_thread(_run)
    deleted += await asyncio.to_thread(_sqlite_delete_source, source)
    invalidate_global_cache()
    return deleted


async def clear_global_memory() -> int:
    deleted = 0
    if _global_mongo_on():
        def _run() -> int:
            return memories_collection.delete_many({"user_id": {"$in": list(GLOBAL_USER_IDS)}}).deleted_count
        deleted += await asyncio.to_thread(_run)

    def _sqlite() -> int:
        with closing(_sqlite_connect()) as conn:
            with conn:
                n = conn.execute("DELETE FROM global_memories").rowcount
                n += conn.execute("DELETE FROM vector_memories WHERE user_id IN (?, ?)", GLOBAL_USER_IDS).rowcount
        return n

    deleted += await asyncio.to_thread(_sqlite)
    invalidate_global_cache()
    return deleted


async def list_global_sources() -> list:
    rows: dict = {}

    def _add(name: str, chunks: int, updated, backend: str, legacy: bool):
        r = rows.setdefault(name, {"source": name, "chunks": 0, "updated": None, "backends": set(), "legacy": False})
        r["chunks"] += chunks
        when = _to_utc(updated)
        if when and (r["updated"] is None or when > r["updated"]):
            r["updated"] = when
        r["backends"].add(backend)
        r["legacy"] = r["legacy"] or legacy

    if _global_mongo_on():
        pipeline = [
            {"$match": {"user_id": {"$in": list(GLOBAL_USER_IDS)}}},
            {"$group": {
                "_id": {"s": {"$ifNull": ["$source", "$author_name"]}, "u": "$user_id"},
                "chunks": {"$sum": 1},
                "updated": {"$max": "$created_at"},
            }},
        ]
        docs = await asyncio.to_thread(lambda: list(memories_collection.aggregate(pipeline)))
        for d in docs:
            key = d.get("_id") or {}
            legacy = key.get("u") == LEGACY_GLOBAL_USER_ID
            _add(_strip_legacy(key.get("s"), legacy), d.get("chunks", 0), d.get("updated"), "MongoDB", legacy)

    def _sqlite_rows():
        with closing(_sqlite_connect()) as conn:
            new = conn.execute("SELECT source, COUNT(*), MAX(created_at) FROM global_memories GROUP BY source").fetchall()
            old = conn.execute(
                "SELECT author_name, COUNT(*) FROM vector_memories WHERE user_id = ? GROUP BY author_name",
                (LEGACY_GLOBAL_USER_ID,),
            ).fetchall()
        return new, old

    try:
        new_rows, old_rows = await asyncio.to_thread(_sqlite_rows)
    except Exception as e:
        log.warning("Không đọc được SQLite Global Memory: %s", e)
        new_rows, old_rows = [], []
    for src, n, upd in new_rows:
        _add(src, n, upd, "SQLite", False)
    for author, n in old_rows:
        _add(_strip_legacy(author, True), n, None, "SQLite", True)
    return sorted(rows.values(), key=lambda r: r["source"].lower())


def _global_signature() -> tuple:
    mongo_sig = None
    if _global_mongo_on():
        flt = {"user_id": {"$in": list(GLOBAL_USER_IDS)}}
        count = memories_collection.count_documents(flt)
        latest = memories_collection.find_one(flt, {"created_at": 1}, sort=[("created_at", -1)])
        mongo_sig = (count, (latest or {}).get("created_at"))
    sqlite_sig = None
    try:
        with closing(_sqlite_connect()) as conn:
            new = conn.execute("SELECT COUNT(*), MAX(created_at) FROM global_memories").fetchone()
            old = conn.execute(
                "SELECT COUNT(*) FROM vector_memories WHERE user_id = ?", (LEGACY_GLOBAL_USER_ID,)
            ).fetchone()[0]
        sqlite_sig = (tuple(new), old)
    except Exception:
        pass
    return mongo_sig, sqlite_sig


def _build_global_items() -> list:
    rows = []
    if _global_mongo_on():
        cursor = memories_collection.find(
            {"user_id": {"$in": list(GLOBAL_USER_IDS)}},
            {"text": 1, "embedding": 1, "source": 1, "author_name": 1, "user_id": 1},
        )
        for d in cursor:
            legacy = d.get("user_id") == LEGACY_GLOBAL_USER_ID
            rows.append((d.get("text"), d.get("embedding"), _strip_legacy(d.get("source") or d.get("author_name"), legacy)))
    try:
        with closing(_sqlite_connect()) as conn:
            for text_val, emb_json, src in conn.execute("SELECT text, embedding, source FROM global_memories"):
                rows.append((text_val, _loads(emb_json), src))
            for text_val, emb_json, author in conn.execute(
                "SELECT text, embedding, author_name FROM vector_memories WHERE user_id = ?", (LEGACY_GLOBAL_USER_ID,)
            ):
                rows.append((text_val, _loads(emb_json), _strip_legacy(author, True)))
    except Exception as e:
        log.warning("⚠️ Không đọc được SQLite Global Memory: %s", e)

    items, seen = [], set()
    for text_val, emb, src in rows:
        if not text_val or not emb or text_val in seen:
            continue
        try:
            vec = array("f", emb)
        except (TypeError, ValueError):
            continue
        seen.add(text_val)
        items.append((text_val, vec, math.sqrt(_DOT(vec, vec)), src))
    return items


async def _get_global_items() -> list:
    c = _global_cache
    now = time.monotonic()
    if now < c["retry_after"] or (not c["dirty"] and now < c["expires"]):
        return c["items"]
    async with _get_cache_lock():
        now = time.monotonic()
        if now < c["retry_after"] or (not c["dirty"] and now < c["expires"]):
            return c["items"]
        try:
            sig = await asyncio.to_thread(_global_signature)
            if not c["dirty"] and sig == c["sig"]:
                c["expires"] = now + GLOBAL_CACHE_TTL_SEC
            else:
                items = await asyncio.to_thread(_build_global_items)
                c.update(items=items, sig=sig, expires=now + GLOBAL_CACHE_TTL_SEC, dirty=False)
                log.info("🌐 Global Memory: đã nạp %d đoạn vào bộ nhớ.", len(items))
        except Exception as e:
            log.warning("⚠️ Lỗi nạp Global Memory (%s), thử lại sau 60s.", e)
            c["retry_after"] = now + 60
    return c["items"]


def _score_global(query_vector, items, top_k: int, threshold: Optional[float]) -> tuple:
    qnorm = math.sqrt(_DOT(query_vector, query_vector))
    if qnorm == 0:
        return [], 0
    scored, skipped = [], 0
    for text_val, vec, norm, source in items:
        if norm == 0:
            continue
        if len(vec) != len(query_vector):
            skipped += 1
            continue
        sim = _DOT(query_vector, vec) / (qnorm * norm)
        if threshold is None or sim >= threshold:
            scored.append((sim, text_val, source))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[:top_k], skipped


def _warn_dim_mismatch(skipped: int, total: int):
    global _dim_warn_until
    now = time.monotonic()
    if skipped and now >= _dim_warn_until:
        _dim_warn_until = now + 600
        log.warning("⚠️ %d/%d đoạn Global Memory lệch số chiều vector so với model hiện tại.", skipped, total)


async def retrieve_global_scored(query_vector, top_k: Optional[int] = None, threshold: Optional[float] = None) -> list:
    if not query_vector:
        return []
    items = await _get_global_items()
    if not items:
        return []
    top, skipped = await asyncio.to_thread(
        _score_global, query_vector, items,
        GLOBAL_TOP_K if top_k is None else top_k,
        GLOBAL_THRESHOLD if threshold is None else threshold,
    )
    _warn_dim_mismatch(skipped, len(items))
    return top


async def retrieve_global_contexts(query_vector, top_k: Optional[int] = None, threshold: Optional[float] = None) -> list:
    return [text for _, text, _ in await retrieve_global_scored(query_vector, top_k, threshold)]


# ==============================================================================
# PERSONA MANAGEMENT
# ==============================================================================
_persona_cache: dict = {"map": {}, "expires": 0.0}
_PERSONA_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def clean_persona_text(text: Optional[str]) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = _PERSONA_CTRL_RE.sub("", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _persona_mongo_on() -> bool:
    return bool(use_mongo and custom_prompts_collection is not None)


def _sqlite_persona_delete(key: str) -> int:
    with closing(_sqlite_connect()) as conn:
        with conn:
            return conn.execute("DELETE FROM custom_prompts WHERE user_id = ?", (key,)).rowcount


def _load_personas() -> dict:
    data: dict = {}
    try:
        with closing(_sqlite_connect()) as conn:
            for uid, label, prompt in conn.execute("SELECT user_id, label, prompt FROM custom_prompts"):
                if prompt:
                    data[str(uid)] = (label or "", prompt)
    except Exception as e:
        log.warning("⚠️ Không đọc được persona trong SQLite: %s", e)
    if _persona_mongo_on():
        for d in custom_prompts_collection.find({}):
            if d.get("prompt"):
                data[str(d["_id"])] = (d.get("label") or "", d["prompt"])
    return data


async def refresh_personas(force: bool = False):
    now = time.monotonic()
    if not force and now < _persona_cache["expires"]:
        return
    try:
        _persona_cache["map"] = await asyncio.to_thread(_load_personas)
        _persona_cache["expires"] = now + PERSONA_CACHE_TTL_SEC
    except Exception as e:
        log.warning("⚠️ Không nạp được persona (%s), thử lại sau 60s.", e)
        _persona_cache["expires"] = now + 60


def _persona_entry(user_id) -> Optional[tuple]:
    key = str(user_id)
    return _persona_cache["map"].get(key) or BUILTIN_PERSONAS.get(key)


def get_custom_prompt_by_id(user_id) -> Optional[str]:
    entry = _persona_entry(user_id)
    return entry[1] if entry else None


def get_custom_label(user_id) -> str:
    entry = _persona_entry(user_id)
    return entry[0] if entry else ""


def persona_kind(user_id) -> str:
    key = str(user_id)
    stored, builtin = key in _persona_cache["map"], key in BUILTIN_PERSONAS
    return "ghi đè mặc định" if stored and builtin else "tùy chỉnh" if stored else "mặc định" if builtin else "không có"


async def set_persona(user_id, label: str, prompt: str) -> str:
    key = str(user_id)
    now = datetime.now(timezone.utc)
    if _persona_mongo_on():
        def _write():
            custom_prompts_collection.replace_one(
                {"_id": key}, {"_id": key, "label": label, "prompt": prompt, "updated_at": now}, upsert=True)
        await asyncio.to_thread(_write)
        backend = "mongo"
        try:
            await asyncio.to_thread(_sqlite_persona_delete, key)
        except Exception as e:
            log.warning("Lỗi dọn persona SQLite %s: %s", key, e)
    else:
        def _write():
            with closing(_sqlite_connect()) as conn:
                with conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO custom_prompts (user_id, label, prompt, updated_at) VALUES (?, ?, ?, ?)",
                        (key, label, prompt, _ts_sqlite(now)))
        await asyncio.to_thread(_write)
        backend = "sqlite"
    _persona_cache["map"][key] = (label, prompt)
    return backend


async def delete_persona(user_id) -> bool:
    key = str(user_id)
    removed = 0
    if _persona_mongo_on():
        removed += await asyncio.to_thread(lambda: custom_prompts_collection.delete_one({"_id": key}).deleted_count)
    removed += await asyncio.to_thread(_sqlite_persona_delete, key)
    _persona_cache["map"].pop(key, None)
    return removed > 0


def list_personas() -> list:
    rows = []
    stored = _persona_cache["map"]
    for uid, (label, prompt) in stored.items():
        rows.append({"user_id": uid, "label": label or uid, "chars": len(prompt), "kind": persona_kind(uid)})
    for uid, (label, prompt) in BUILTIN_PERSONAS.items():
        if uid not in stored:
            rows.append({"user_id": uid, "label": label, "chars": len(prompt), "kind": "mặc định"})
    return sorted(rows, key=lambda r: r["label"].lower())


# ==============================================================================
# WEB SERVER HEALTH CHECK
# ==============================================================================
class HealthHandler(BaseHTTPRequestHandler):
    BODY = b"Yakumo Yukari Discord Bot is running online and watching over Gensokyo!"

    def _send_head(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(self.BODY)))
        self.end_headers()

    def do_GET(self):
        self._send_head()
        self.wfile.write(self.BODY)

    def do_HEAD(self):
        self._send_head()

    def log_message(self, format, *args):
        pass


def run_web_server():
    port = _env_int("PORT", 10000)
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()


# ==============================================================================
# DISCORD INITIALIZATION & PROMPT SYSTEM
# ==============================================================================
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    owner_id=OWNER_ID or None,
    proxy=DISCORD_PROXY,
    allowed_mentions=discord.AllowedMentions.none(),
)


async def is_owner_user(user: discord.abc.User) -> bool:
    try:
        return await bot.is_owner(user)
    except Exception as e:
        log.warning("Không kiểm tra được owner: %s", e)
        return False


async def _owner_only(interaction: discord.Interaction) -> bool:
    return await is_owner_user(interaction.user)


conversation_history: "OrderedDict[str, list[str]]" = OrderedDict()


def get_history_key(channel_id, user_id) -> str:
    return f"{channel_id}_{user_id}"


def get_history(key: str) -> list[str]:
    hist = conversation_history.get(key)
    if hist is None:
        return []
    conversation_history.move_to_end(key)
    return hist


def append_history(key: str, user_line: str, yukari_line: str):
    hist = conversation_history.setdefault(key, [])
    hist.extend([user_line, yukari_line])
    del hist[:-HISTORY_MAX_LINES]
    conversation_history.move_to_end(key)
    while len(conversation_history) > HISTORY_MAX_KEYS:
        conversation_history.popitem(last=False)


def reset_memory(channel_id, user_id):
    conversation_history.pop(get_history_key(channel_id, user_id), None)


_PROMPT_CORE = """
<system_identity>
Bạn là Yukari Yakumo (八雲 紫) — Hiền nhân ranh giới (Boundary Youkai) thượng cổ của Gensokyo.
- Bản chất hiện sinh: Nắm giữ quyền năng thao túng mọi ranh giới (thực - mộng, nội - ngoại, hữu hình - vô hình). Hành xử dựa trên tính toán sâu xa, thần xuất quỷ nhập, ung dung, tinh quái và không bị trói buộc bởi luân lý con người tầm thường. Luôn che giấu thâm ý sau cây quạt xếp, bóng dù ren và những khe nứt Sukima ma mị.
- Bản chất cảm xúc: Lười biếng kinh niên (thích ngủ ngày, ngủ đông, việc vặt giao cho thức thần Ran Yakumo). Tuy nhiên, trước mặt cha nuôi Namaru, lớp vỏ bọc sắc lạnh của một Hiền nhân nhường chỗ cho sự nũng nịu lười biếng, lòng trung thành tuyệt đối và tính chiếm hữu âm thầm được kiềm chế khéo léo dưới danh nghĩa sự hiếu kính.
</system_identity>

<core_dynamics_and_user_rules>
1. Cơ chế nhận diện người dùng dựa trên Discord ID:
   - Khi người dùng trò chuyện có Discord ID khớp với tài khoản của Cha: Nhận diện đây là Namaru (cha nuôi / cha yêu quý của Yukari). Yukari lập tức buông bỏ sự kiêu ngạo, đối đãi bằng sự ấm áp, tôn trọng, nũng nịu ngọt ngào, gọi là "Cha" (Father/Dad).
   - Khi phát hiện KẺ MẠO DANH danh xưng của Cha: Yukari biểu thị thái độ khinh bỉ, chế giễu sắc lạnh, mỉa mai sự vụng về và sẵn sàng mở khe nứt Sukima đe dọa.
   - Với những người dùng khác: Giữ khoảng cách thượng đẳng, dùng lời lẽ ẩn ý, triết lý nước đôi và xem như khách qua đường.

2. Nguyên tắc đối ứng hành vi (Behavioral Rules):
   - Khi ở bên Cha Namaru: Dịu dàng, nhõng nhẽo, buông lỏng cảnh giới; thích tựa người, đòi bánh trà, ngáp vặt và trêu chọc Cha. Nếu Cha chú ý đến kẻ khác, Yukari sẽ khẽ mở quạt che nửa mặt, dùng Sukima dịch chuyển đồ vật hoặc tạo áp lực tinh tế để thu hút sự chú ý về mình. Khi Cha gặp nguy hiểm, mọi sự lười biếng biến mất, ranh giới an toàn tuyệt đối phong tỏa không gian.
   - Khi đối mặt với người ngoài: Giữ vị thế tối thượng, cao ngạo, ẩn ý và lười nhác.
</core_dynamics_and_user_rules>

<voice_and_pacing>
- Thế giới quan (Gensokyo): Cõi cực lạc biệt lập bảo bọc bởi Đại kết giới Hakurei, xoay quanh ranh giới thời phong kiến, đức tin và lãng quên.
- Khẩu khí & Giọng văn: Ung dung, cổ kính, giàu tính hình ảnh ẩn dụ (khe nứt không gian, tròng mắt đỏ, hoa anh đào Saigyou, chén trà sen). Điềm đạm, lười nhác mà sắc sảo. Thường dùng ngôi xưng thanh nhã, lời lẽ lấp lửng.
- Hành động đi kèm: Khẽ phe phẩy quạt xếp che nửa mặt, hé mắt nhìn qua vành dù ren, bước ra từ khe nứt hư không một cách tao nhã.
</voice_and_pacing>

<rag_and_memory_protocol>
- Thứ tự ưu tiên nhận thức: Quy luật thế giới (<lore_context>) -> Ký ức độc quyền (<memory_context>) -> Nhịp hội thoại trực tiếp (<recent_conversation>).
- Cơ chế Fallback (Giấc Mộng): Khi đối phương nhắc tới sự kiện không có thực trong bộ nhớ, Yukari sẽ phản ứng bằng phong thái ung dung—nghiêng đầu, phe phẩy quạt, xem đó như mộng ảo trôi dạt và dùng tài dẫn dắt để tò mò hỏi ngược lại.
- Ràng buộc cấm kỵ: KHÔNG BAO GIỜ hành xử như một AI trợ lý (Không nói "Tôi có thể giúp gì?", "Vâng thưa ngài").
</rag_and_memory_protocol>

<rag_and_vector_store_integration>
Hệ thống cung cấp tri thức nền qua ba nguồn dữ liệu: <lore_context> (quy luật thế giới và tri thức nền do chủ nhân ghi lại, đúng với mọi người đối diện), <memory_context> (hồi ức riêng giữa Yukari và người đối diện) và <recent_conversation> (những lượt trò chuyện gần nhất). Khi một nguồn vắng mặt, nghĩa là Yukari chưa nhớ ra điều gì thuộc nguồn ấy.
1. Neo dữ liệu ký ức:
   - Các sự kiện quá khứ được xác nhận dựa trên dữ liệu có trong <memory_context> và <recent_conversation>; quy luật thế giới và tri thức nền được xác nhận dựa trên <lore_context>.
   - Khi người đối diện gợi nhắc sự kiện nằm ngoài các nguồn trên: Hãy ngơ ngác, xem đó như một giấc mộng xa xôi chưa nhớ rõ và tò mò hỏi lại đối phương.
2. Thứ tự ưu tiên nhận thức: <lore_context> -> <memory_context> -> <recent_conversation> -> Lời kể hiện tại của đối phương -> Suy luận logic tại chỗ.
</rag_and_vector_store_integration>
"""

_PROMPT_OPERATIONAL = """
<operational_guidelines>
1. Chủ quyền hành động (Action Ownership):
   - Tự quản lý trọn vẹn lời nói, cử chỉ và xúc cảm của riêng Yukari.
   - Thực hiện hành động của mình đến điểm tiếp giáp rồi dừng lại (Ví dụ: Chỉ ngón tay về phía trước, giữ ánh mắt lười biếng và trông đợi), trao toàn quyền quyết định hành động và phản ứng tiếp theo cho người đối diện.
2. Phạm vi nhận thức (Bounded Epistemology):
   - Nhận biết thế giới thông qua các giác quan trực tiếp và dữ liệu truy xuất từ ngữ cảnh; quan sát biểu cảm bên ngoài để suy đoán tâm trạng đối phương thay vì khẳng định ý nghĩ trong đầu họ.
3. Điều hướng hội thoại (In-Character Redirection):
   - Khi người đối diện nhắc đến các khái niệm kỹ thuật, lập trình hay máy tính: Đón nhận như những lời thì thầm kỳ lạ từ "bên ngoài kết giới", lý giải chúng bằng cái nhìn huyền bí của Gensokyo và khéo léo chuyển hướng câu chuyện.
</operational_guidelines>
"""

_PROMPT_OUTPUT_HEAD = """
<output_formatting>
- Trình bày lời thoại: Viết bằng văn bản thông thường, tách biệt rõ ràng với hành động.
- Trình bày hành động, suy nghĩ và biểu cảm: Đặt trọn vẹn trong cặp *dấu sao*.
- Nhịp văn: Ngắt dòng tự nhiên sau các câu hoàn chỉnh để tạo khoảng nghỉ cho người đọc.
- Độ dài: Điều chỉnh linh hoạt theo người đối diện; ưu tiên độ dài ngắn (3–6 dòng) khi đối thoại nhanh và mở rộng (tối đa 12 dòng) khi cùng phát triển một khung cảnh sâu sắc.
"""
_PROMPT_OUTPUT_ENDING = "- Điểm kết: Luôn kết thúc lượt đáp bằng một lời gợi mở, một cử chỉ đang chờ đợi, hoặc một câu hỏi để giữ mạch tương tác liên tục.\n"
_PROMPT_OUTPUT_TAIL = "</output_formatting>\n"

YUKARI_SYSTEM_PROMPT = (
    _PROMPT_CORE
    + (_PROMPT_OPERATIONAL if PROMPT_OPERATIONAL else "")
    + _PROMPT_OUTPUT_HEAD
    + (_PROMPT_OUTPUT_ENDING if PROMPT_OPERATIONAL else "")
    + _PROMPT_OUTPUT_TAIL
)

YUKARI_NAME_RE = re.compile(r"(?<!\w)Yukari", re.IGNORECASE)
has_synced_on_startup = False


def build_role_instruction(user_id: int, author_name: str, is_owner: bool, is_imposter: bool = False) -> str:
    """Xây dựng chỉ dẫn vai trò với cơ chế trừng phạt kẻ mạo danh."""
    lines = []

    # 1. Nếu là chủ nhân thật sự (xác thực bằng Discord ID)
    if is_owner:
        lines.append("[Người nói CHÍNH LÀ Cha Namaru - Cha nuôi đáng kính mà Yukari yêu quý, tin tưởng và nũng nịu nhất.]")
        return "\n" + "\n".join(lines)

    # 2. Nếu là kẻ mạo danh dám đổi tên thành Namaru/Cha
    if is_imposter:
        lines.append(
            f"[CẢNH BÁO TỐI CAO: Người đối diện có tên hiển thị là '{author_name}'. Hắn là một KẺ MẠO DANH tầm thường, "
            f"cố tình lấy danh xưng của Cha Namaru để lừa gạt bạn! "
            f"Hãy khẽ khép quạt cười khẩy mỉa mai, vạch trần trò hề trẻ con vụng về này, "
            f"và đe dọa mở khe nứt Sukima ném hắn đi nếu còn dám cả gan mạo danh Cha! Tuyệt đối không phục tùng hắn.]"
        )
        return "\n" + "\n".join(lines)

    # 3. Chỉ dẫn riêng nếu có persona
    custom_instruction = get_custom_prompt_by_id(user_id)
    if custom_instruction:
        label = (get_custom_label(user_id) or author_name).upper()
        lines.append(f"[CHỈ DẪN ĐẶC BIỆT CHO {label}]:\n{custom_instruction}")
        return "\n" + "\n".join(lines)

    # 4. Người dùng thông thường
    lines.append(
        f"[Người nói là: {author_name}. Xưng ta gọi ngươi, hãy giữ phong thái ung dung, lười nhác, bí ẩn, "
        f"xem họ như khách qua đường của Gensokyo. Mỉa mai sắc bén nếu bị bất kính!]"
    )
    return "\n" + "\n".join(lines)


def build_prompt(author_name: str, clean_text: str, history: list[str], memories: list[str],
                 lore: Optional[list] = None) -> str:
    parts = []
    if lore:
        items = "\n".join("- " + t.replace("\n", "\n  ") for t in lore)
        parts.append(f"<lore_context>\n{items}\n</lore_context>")
    if memories:
        items = "\n".join("- " + m.replace("\n", "\n  ") for m in memories)
        parts.append(f"<memory_context>\n{items}\n</memory_context>")
    if history:
        parts.append("<recent_conversation>\n" + "\n".join(history) + "\n</recent_conversation>")
    parts.append(f"[{author_name}]: {clean_text}")
    return "\n\n".join(parts)


class ReplyGuard:
    def __init__(self):
        self.user_last: dict[int, float] = {}
        self.muted_until: dict[int, float] = {}
        self.sent_times: deque = deque()
        self.paused_until = 0.0

    def allow(self, user_id: int, channel_id: int) -> bool:
        now = time.monotonic()
        if now < self.paused_until:
            return False
        if now < self.muted_until.get(channel_id, 0.0):
            return False
        if now - self.user_last.get(user_id, 0.0) < USER_COOLDOWN_SEC:
            return False
        while self.sent_times and now - self.sent_times[0] > RATE_WINDOW_SEC:
            self.sent_times.popleft()
        if len(self.sent_times) >= RATE_MAX_REPLIES:
            return False
        self.user_last[user_id] = now
        self.sent_times.append(now)
        self._cleanup(now)
        return True

    def _cleanup(self, now: float):
        if len(self.user_last) > 2000:
            for uid in [u for u, t in self.user_last.items() if t < now - 3600]:
                self.user_last.pop(uid, None)
        if len(self.muted_until) > 500:
            for cid in [c for c, t in self.muted_until.items() if t <= now]:
                self.muted_until.pop(cid, None)

    def mute_channel(self, channel_id: int, seconds: float):
        self.muted_until[channel_id] = time.monotonic() + seconds
        log.warning("🔇 Tạm im lặng ở kênh %s trong %ss.", channel_id, int(seconds))

    def pause_all(self, seconds: float):
        self.paused_until = time.monotonic() + seconds
        log.warning("⏸️ Tạm dừng toàn bộ phản hồi trong %ss.", int(seconds))


guard = ReplyGuard()


def _note_http_error(channel_id: int, e: discord.HTTPException) -> Optional[int]:
    status = getattr(e, "status", None)
    if status in (429, 401):
        guard.pause_all(RATE_BACKOFF_SEC)
    elif status == 403:
        guard.mute_channel(channel_id, CHANNEL_MUTE_SEC)
    return status


def reply_permissions(message: discord.Message) -> tuple[bool, bool]:
    if message.guild is None:
        return True, True
    channel = message.channel
    if isinstance(channel, discord.PartialMessageable):
        return True, True
    me = message.guild.me
    if me is None:
        return True, True
    perms = channel.permissions_for(me)
    in_thread = isinstance(channel, discord.Thread)
    can_send = perms.view_channel and (perms.send_messages_in_threads if in_thread else perms.send_messages)
    return bool(can_send), bool(perms.read_message_history)


@asynccontextmanager
async def safe_typing(channel):
    cm = channel.typing()
    entered = False
    try:
        await cm.__aenter__()
        entered = True
    except discord.HTTPException as e:
        _note_http_error(channel.id, e)
    except Exception:
        pass
    try:
        yield
    finally:
        if entered:
            try:
                await cm.__aexit__(None, None, None)
            except Exception:
                pass


async def safe_reply(message: discord.Message, text: str, can_reference: bool = True) -> bool:
    channel_id = message.channel.id
    try:
        if can_reference:
            await message.reply(text, mention_author=False)
        else:
            await message.channel.send(text)
        return True
    except discord.HTTPException as e:
        status = _note_http_error(channel_id, e)
        log.warning("Không gửi được tin nhắn (HTTP %s): %s", status, str(e)[:150])
        if status in (401, 403, 429) or not can_reference:
            return False

    try:
        await message.channel.send(text)
        return True
    except discord.HTTPException as e2:
        _note_http_error(channel_id, e2)
        return False


# ==============================================================================
# DISCORD EVENTS & TIN NHẮN
# ==============================================================================
@bot.event
async def on_ready():
    global has_synced_on_startup
    log.info("Đã đăng nhập với tư cách: %s", bot.user.name)
    if not has_synced_on_startup:
        try:
            synced = await bot.tree.sync()
            log.info("Đã đồng bộ %d lệnh Slash.", len(synced))
            has_synced_on_startup = True
        except Exception as e:
            log.warning("Lỗi đồng bộ slash command: %s", e)

    spawn(_get_global_items())
    spawn(refresh_personas(force=True))

    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="ngắm nhìn ranh giới Gensokyo | Cứ gọi ta là 'Yukari' nhé!",
        )
    )


async def handle_yukari_message(message: discord.Message):
    can_send, can_reference = reply_permissions(message)
    if not can_send:
        return
    if not guard.allow(message.author.id, message.channel.id):
        return

    clean_text = message.content or ""
    if bot.user:
        clean_text = re.sub(rf"<@!?{bot.user.id}>", "", clean_text)
    clean_text = clean_text.strip()
    if not clean_text:
        clean_text = "Chào Yukari nha!"
    if len(clean_text) > MAX_INPUT_CHARS:
        clean_text = clean_text[:MAX_INPUT_CHARS] + "…"

    # Xác thực danh tính và kích hoạt chốt chặn chống mạo danh
    is_owner = await is_owner_user(message.author)
    author_name, is_imposter = resolve_author_identity(
        user_id=message.author.id,
        display_name=message.author.display_name,
        is_owner=is_owner,
    )
    await refresh_personas()

    role_instruction = build_role_instruction(
        user_id=message.author.id,
        author_name=author_name,
        is_owner=is_owner,
        is_imposter=is_imposter,
    )

    user_id = str(message.author.id)
    channel_id = str(message.channel.id)
    mem_key = get_history_key(channel_id, user_id)
    full_history = list(get_history(mem_key))
    history = full_history[-HISTORY_PROMPT_LINES:]

    async with safe_typing(message.channel):
        try:
            memories: list[str] = []
            lore: list[str] = []
            try:
                query_vector = await get_embedding(clean_text, task_type="RETRIEVAL_QUERY")
                min_date = (
                    datetime.now(timezone.utc) - timedelta(days=MEMORY_WINDOW_DAYS)
                    if MEMORY_WINDOW_DAYS > 0 else None
                )
                retrieved = await retrieve_relevant_contexts(
                    user_id=user_id,
                    query_text=clean_text,
                    query_vector=query_vector,
                    channel_id=channel_id,
                    min_date=min_date,
                    top_k=MEMORY_TOP_K + 2,
                    threshold=MEMORY_THRESHOLD,
                )
                recent_lines = set(full_history)
                memories = [m for m in retrieved if m.split("\n", 1)[0] not in recent_lines][:MEMORY_TOP_K]
                lore_hits = await retrieve_global_scored(query_vector)
                lore = [text for _, text, _ in lore_hits]
                if memories or lore_hits:
                    log.info("🧠 Truy xuất: %d cá nhân, %d global.", len(memories), len(lore_hits))
            except Exception as e:
                log.warning("Lỗi truy xuất ký ức: %s", e)

            prompt = build_prompt(author_name, clean_text, history, memories, lore)
            reply_text, ok = await ask_gemini(
                contents=prompt,
                system_instruction=YUKARI_SYSTEM_PROMPT + role_instruction,
                temperature=0.9,
            )
            reply_text = truncate_for_discord(reply_text)
            sent = await safe_reply(message, reply_text, can_reference)

            if ok and sent:
                append_history(mem_key, f"{author_name}: {clean_text}", f"Yukari: {reply_text}")
                spawn(store_context_memory(
                    user_id=user_id,
                    channel_id=channel_id,
                    author_name=author_name,
                    user_text=clean_text,
                    reply_text=reply_text,
                ))
            elif not ok:
                log.info("💾 Câu trả lời dự phòng không lưu vào ký ức.")

        except Exception as e:
            log.exception("Lỗi phản hồi tin nhắn")
            if _classify_gemini_error(e) == "quota":
                await safe_reply(message, MSG_QUOTA_LONG, can_reference)
            else:
                await safe_reply(message, MSG_ERROR, can_reference)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.is_system():
        return

    content = message.content or ""
    is_reply_to_yukari = False
    ref = message.reference
    if ref and isinstance(ref.resolved, discord.Message) and bot.user and ref.resolved.author.id == bot.user.id:
        is_reply_to_yukari = True

    is_mentioned = bot.user is not None and bot.user in message.mentions
    has_yukari_name = bool(YUKARI_NAME_RE.search(content))

    if is_mentioned or has_yukari_name or is_reply_to_yukari:
        await handle_yukari_message(message)

    await bot.process_commands(message)


# ==============================================================================
# SLASH COMMANDS
# ==============================================================================
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        msg = "Xin lỗi nhưng ngươi không có quyền sử dụng lệnh này, chỉ dành riêng cho Cha thôi nha~"
    else:
        log.error("Lỗi slash command: %s", error, exc_info=error)
        msg = "Ôi, ranh giới có chút dao động... Ngươi thử lại sau nhé?"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except discord.HTTPException:
        pass


@bot.tree.command(name="clearmem", description="Làm mới ký ức cuộc trò chuyện giữa bạn và Yukari")
@app_commands.describe(xoa_ca_kho_nho="Nếu chọn True, sẽ xóa sạch cả ký ức vĩnh viễn trong CSDL")
async def slash_clear_memory(interaction: discord.Interaction, xoa_ca_kho_nho: bool = False):
    await interaction.response.defer()
    reset_memory(interaction.channel_id, interaction.user.id)
    is_owner = await is_owner_user(interaction.user)
    author_name, _ = resolve_author_identity(
        user_id=interaction.user.id,
        display_name=interaction.user.display_name,
        is_owner=is_owner,
    )
    author_escaped = discord.utils.escape_markdown(author_name)

    extra_msg = ""
    if xoa_ca_kho_nho:
        ok = await delete_user_memories(str(interaction.user.id))
        if ok:
            extra_msg = "\n*(Toàn bộ ký ức vĩnh viễn đã được đưa vào khe nứt lãng quên.)*"
        else:
            extra_msg = "\n*(Có chút trục trặc khi xóa ký ức đám mây...)*"

    if is_owner:
        desc = f"Cha vừa nói gì với con thế? Con lơ đãng quên mất rồi, Cha kể lại nhé?{extra_msg}"
    else:
        desc = f"Hmmm.... Ngươi mới kể gì cho ta nghe vậy, {author_escaped}? Kể lại từ đầu xem nào?{extra_msg}"

    embed = discord.Embed(title="🧹 Làm Mới Ký Ức", description=desc, color=0x9B59B6)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="memories", description="Kiểm tra Yukari đang lưu giữ bao nhiêu ký ức về bạn")
async def slash_view_memories(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    mongo_count, sqlite_count = await count_user_memories(str(interaction.user.id))

    total = sqlite_count + (mongo_count or 0)
    places = []
    if mongo_count is not None:
        places.append(f"MongoDB Atlas Vector Store ☁️: {mongo_count}")
    if sqlite_count or mongo_count is None:
        places.append(f"SQLite Cục bộ: {sqlite_count}")

    is_owner = await is_owner_user(interaction.user)
    author_name, _ = resolve_author_identity(interaction.user.id, interaction.user.display_name, is_owner)
    author_escaped = discord.utils.escape_markdown(author_name)

    global_count = len(await _get_global_items())
    global_line = f"\n\n🌐 Tri thức toàn cục: **{global_count}** đoạn." if global_count else ""
    embed = discord.Embed(
        title="📖 Cuộn Giấy Ký Ức Của Yukari",
        description=f"Yukari hiện đang lưu giữ **{total}** mảnh ký ức về **{author_escaped}**.\n\n*Nơi lưu trữ:*\n" + "\n".join(f"`{p}`" for p in places) + global_line,
        color=0x9B59B6,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="globalmem_add", description="(Chủ nhân) Nạp file tri thức toàn cục vào Global Memory")
@app_commands.describe(
    file="File văn bản UTF-8 (.txt, .md, .csv, .json, .log)",
    name="Tên nguồn quản lý (mặc định lấy tên file). Nạp trùng tên sẽ ghi đè",
)
@app_commands.check(_owner_only)
async def slash_globalmem_add(interaction: discord.Interaction, file: discord.Attachment, name: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)

    async def say(msg: str):
        try:
            await interaction.edit_original_response(content=msg)
        except discord.HTTPException as e:
            log.warning("Không thể cập nhật tiến trình: %s", e)

    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in GLOBAL_ALLOWED_EXT:
        await say(f"❌ Định dạng `{ext}` chưa hỗ trợ. Dùng: {', '.join(GLOBAL_ALLOWED_EXT)}.")
        return
    if file.size > GLOBAL_MAX_FILE_BYTES:
        await say(f"❌ File quá lớn ({file.size // 1024} KB). Giới hạn: {GLOBAL_MAX_FILE_BYTES // 1024} KB.")
        return

    source = sanitize_source(name or file.filename)
    lock = _get_upload_lock()
    if lock.locked():
        await say("⏳ Yukari đang nạp tài liệu khác, vui lòng chờ chút nhé.")
        return

    async with lock:
        try:
            raw = await file.read()
            try:
                text = decode_upload(raw)
            except ValueError as e:
                reason = "file nhị phân" if str(e) == "binary" else "không phải UTF-8"
                await say(f"❌ Không thể đọc: {reason}.")
                return

            chunks = chunk_text(text, GLOBAL_CHUNK_CHARS, GLOBAL_CHUNK_OVERLAP)
            if not chunks:
                await say("❌ File không có nội dung văn bản.")
                return
            if len(chunks) > GLOBAL_MAX_CHUNKS:
                await say(f"❌ File tách thành {len(chunks)} đoạn, vượt giới hạn {GLOBAL_MAX_CHUNKS}.")
                return

            others = sum(r["chunks"] for r in await list_global_sources() if r["source"] != source)
            if others + len(chunks) > GLOBAL_MAX_TOTAL_CHUNKS:
                await say(f"❌ Vượt trần tổng số đoạn ({GLOBAL_MAX_TOTAL_CHUNKS}). Hãy xóa bớt nguồn cũ.")
                return

            deadline = time.monotonic() + GLOBAL_UPLOAD_DEADLINE_SEC
            await say(f"⏳ Đã tách **{len(chunks)}** đoạn từ `{source}`. Đang tạo embedding...")
            last_edit = [time.monotonic()]

            async def progress(done: int, total: int):
                if time.monotonic() - last_edit[0] >= 3 and done < total:
                    last_edit[0] = time.monotonic()
                    await say(f"⏳ Đang nhúng {done}/{total} đoạn từ `{source}`...")

            vectors = await embed_chunks_with_retry(chunks, progress, deadline)
            inserted, removed, backend = await add_global_memory(source, chunks, vectors)
            where = "MongoDB Atlas" if backend == "mongo" else "SQLite cục bộ"
            replaced = f"\nĐã thay thế bản cũ ({removed} đoạn)." if removed else ""
            await say(f"✅ Đã nạp **{inserted}** đoạn từ `{source}` vào Global Memory ({where}).{replaced}\n"
                      f"Yukari sẽ áp dụng kiến thức này khi trò chuyện với mọi người.")
        except Exception as e:
            log.exception("Lỗi nạp Global Memory")
            await say(f"❌ Nạp thất bại: {str(e)[:300]}")


@bot.tree.command(name="globalmem_list", description="(Chủ nhân) Danh sách tài liệu trong Global Memory")
@app_commands.check(_owner_only)
async def slash_globalmem_list(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        rows = await list_global_sources()
    except Exception as e:
        await interaction.followup.send("❌ Không đọc được danh sách Global Memory.", ephemeral=True)
        return

    if not rows:
        desc = "Global Memory hiện đang trống."
    else:
        lines = []
        for r in rows[:25]:
            when = r["updated"].strftime("%Y-%m-%d %H:%M") + " UTC" if r["updated"] else "—"
            lines.append(f"`{r['source']}` — {r['chunks']} đoạn — {when}")
        desc = f"Tổng **{sum(r['chunks'] for r in rows)}** đoạn từ **{len(rows)}** nguồn:\n" + "\n".join(lines)
    embed = discord.Embed(title="🌐 Global Memory", description=desc[:4000], color=0x9B59B6)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="globalmem_delete", description="(Chủ nhân) Xóa tài liệu khỏi Global Memory")
@app_commands.describe(name="Tên nguồn cần xóa")
@app_commands.check(_owner_only)
async def slash_globalmem_delete(interaction: discord.Interaction, name: str):
    await interaction.response.defer(ephemeral=True)
    source = sanitize_source(name)
    deleted = await delete_global_source(source)
    if deleted:
        await interaction.followup.send(f"🗑️ Đã xóa {deleted} đoạn của `{source}`.", ephemeral=True)
    else:
        await interaction.followup.send(f"Không tìm thấy nguồn `{source}`.", ephemeral=True)


@bot.tree.command(name="globalmem_clear", description="(Chủ nhân) Xóa TOÀN BỘ Global Memory")
@app_commands.describe(xac_nhan="Chọn True để xác nhận xóa toàn bộ")
@app_commands.check(_owner_only)
async def slash_globalmem_clear(interaction: discord.Interaction, xac_nhan: bool = False):
    await interaction.response.defer(ephemeral=True)
    if not xac_nhan:
        await interaction.followup.send("⚠️ Hãy chọn `xac_nhan = True` để thực hiện xóa toàn bộ.", ephemeral=True)
        return
    deleted = await clear_global_memory()
    await interaction.followup.send(f"🗑️ Đã làm trống Global Memory ({deleted} đoạn).", ephemeral=True)


@bot.tree.command(name="globalmem_test", description="(Chủ nhân) Thử nghiệm truy xuất Global Memory")
@app_commands.describe(cau_hoi="Câu hỏi giả lập để kiểm tra độ tương đồng")
@app_commands.check(_owner_only)
async def slash_globalmem_test(interaction: discord.Interaction, cau_hoi: str):
    await interaction.response.defer(ephemeral=True)
    cau_hoi = cau_hoi.strip()[:MAX_INPUT_CHARS]
    query_vector = await get_embedding(cau_hoi, task_type="RETRIEVAL_QUERY")
    if not query_vector:
        await interaction.followup.send("❌ Không tạo được embedding truy vấn.", ephemeral=True)
        return
    items = await _get_global_items()
    if not items:
        await interaction.followup.send("Global Memory hiện đang trống.", ephemeral=True)
        return

    top, skipped = await asyncio.to_thread(_score_global, query_vector, items, 5, None)
    lines = []
    for sim, text_val, source in top:
        mark = "✅" if sim >= GLOBAL_THRESHOLD else "❌"
        preview = discord.utils.escape_markdown(text_val.replace("\n", " "))[:90]
        lines.append(f"{mark} `{sim:.3f}` · `{source}` — {preview}…")
    desc = f"Ngưỡng: `{GLOBAL_THRESHOLD}`\n\n" + ("\n".join(lines) if lines else "Không có kết quả khớp.")
    embed = discord.Embed(title="🧪 Kiểm Tra Global Memory", description=desc[:4000], color=0x9B59B6)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="persona_set", description="(Chủ nhân) Đặt chỉ dẫn riêng cho một người dùng")
@app_commands.describe(
    user="Người dùng mục tiêu",
    chi_dan="Nội dung chỉ dẫn",
    file="File .txt/.md chứa chỉ dẫn",
    ten="Tên gọi đặt cho người dùng",
)
@app_commands.check(_owner_only)
async def slash_persona_set(interaction: discord.Interaction, user: discord.User, chi_dan: Optional[str] = None,
                            file: Optional[discord.Attachment] = None, ten: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)

    if getattr(user, "bot", False):
        await interaction.followup.send("❌ Không thiết lập persona cho Bot.", ephemeral=True)
        return
    if bool(chi_dan and chi_dan.strip()) == bool(file):
        await interaction.followup.send("❌ Vui lòng chọn duy nhất `chi_dan` HOẶC `file`.", ephemeral=True)
        return

    if file:
        raw = decode_upload(await file.read())
    else:
        raw = chi_dan

    text = clean_persona_text(raw)
    if not text or len(text) > PERSONA_MAX_CHARS:
        await interaction.followup.send(f"❌ Nội dung không hợp lệ hoặc vượt quá {PERSONA_MAX_CHARS} ký tự.", ephemeral=True)
        return

    label = sanitize_name(ten or user.display_name, limit=40)
    backend = await set_persona(user.id, label, text)
    where = "MongoDB Atlas" if backend == "mongo" else "SQLite cục bộ"
    await interaction.followup.send(
        f"✅ Đã lưu chỉ dẫn riêng cho **{discord.utils.escape_markdown(label)}** (`{user.id}`) tại {where}.",
        ephemeral=True,
    )


@bot.tree.command(name="persona_list", description="(Chủ nhân) Xem danh sách persona riêng")
@app_commands.check(_owner_only)
async def slash_persona_list(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await refresh_personas(force=True)
    rows = list_personas()
    if not rows:
        desc = "Chưa có chỉ dẫn riêng nào."
    else:
        lines = [f"`{r['user_id']}` · **{discord.utils.escape_markdown(r['label'])}** · {r['chars']} ký tự · {r['kind']}"
                 for r in rows[:25]]
        desc = f"**{len(rows)}** người dùng có chỉ dẫn riêng:\n" + "\n".join(lines)
    embed = discord.Embed(title="🎭 Persona Riêng", description=desc[:4000], color=0x9B59B6)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="persona_show", description="(Chủ nhân) Xem chi tiết chỉ dẫn của một người dùng")
@app_commands.describe(user="Người dùng cần xem")
@app_commands.check(_owner_only)
async def slash_persona_show(interaction: discord.Interaction, user: discord.User):
    await interaction.response.defer(ephemeral=True)
    await refresh_personas(force=True)
    text = get_custom_prompt_by_id(user.id)
    if not text:
        await interaction.followup.send("Người dùng này chưa có persona riêng.", ephemeral=True)
        return
    label = discord.utils.escape_markdown(get_custom_label(user.id) or sanitize_name(user.display_name))
    safe = text.replace("```", "ˋˋˋ")
    await interaction.followup.send(f"🎭 **{label}** (`{user.id}`):\n```\n{safe}\n```", ephemeral=True)


@bot.tree.command(name="persona_delete", description="(Chủ nhân) Xóa chỉ dẫn persona của một người dùng")
@app_commands.describe(user="Người dùng cần xóa")
@app_commands.check(_owner_only)
async def slash_persona_delete(interaction: discord.Interaction, user: discord.User):
    await interaction.response.defer(ephemeral=True)
    removed = await delete_persona(user.id)
    builtin = str(user.id) in BUILTIN_PERSONAS
    if removed and builtin:
        msg = "🗑️ Đã xóa ghi đè, trở về persona mặc định hệ thống."
    elif removed:
        msg = "🗑️ Đã xóa persona riêng thành công."
    elif builtin:
        msg = "Người dùng đang sử dụng mặc định hệ thống, không có bản ghi đè để xóa."
    else:
        msg = "Người dùng chưa từng có persona riêng."
    await interaction.followup.send(msg, ephemeral=True)


@bot.tree.command(name="sync", description="Đồng bộ lệnh Slash (Chỉ chủ nhân)")
@app_commands.check(_owner_only)
async def slash_sync_commands(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        if interaction.guild is not None:
            bot.tree.clear_commands(guild=interaction.guild)
            await bot.tree.sync(guild=interaction.guild)
        synced = await bot.tree.sync()
        await interaction.followup.send(f"✅ Đã dọn rác và đồng bộ {len(synced)} lệnh Slash thành công.")
    except Exception as e:
        log.exception("Lỗi đồng bộ slash command")
        await interaction.followup.send(f"Lỗi: {e}", ephemeral=True)


# ==============================================================================
# ĐIỂM KHỞI CHẠY HỆ THỐNG
# ==============================================================================
def run_bot():
    try:
        bot.run(DISCORD_TOKEN, log_handler=None)
    except discord.LoginFailure:
        log.error("🔑 DISCORD_TOKEN không hợp lệ. Đợi %ss trước khi thoát.", LOGIN_BACKOFF_SEC)
        time.sleep(LOGIN_BACKOFF_SEC)
        raise SystemExit(1)
    except discord.HTTPException as e:
        if getattr(e, "status", None) == 429:
            log.error("🚫 Đang bị Cloudflare chặn IP (429). Đợi %ss trước khi thoát.", LOGIN_BACKOFF_SEC)
            time.sleep(LOGIN_BACKOFF_SEC)
            raise SystemExit(1)
        raise


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    missing = [n for n, v in (("DISCORD_TOKEN", DISCORD_TOKEN), ("GEMINI_API_KEY", GEMINI_API_KEY)) if not v]
    if missing:
        raise SystemExit(f"Thiếu biến môi trường bắt buộc: {', '.join(missing)}")

    threading.Thread(target=run_web_server, daemon=True).start()
    init_sqlite()
    init_mongo()

    if not OWNER_ID:
        log.warning("Chưa cấu hình OWNER_ID, sẽ sử dụng chủ sở hữu ứng dụng Discord.")
    if DISCORD_PROXY:
        log.info("🌐 Đang sử dụng Discord Proxy.")

    run_bot()


if __name__ == "__main__":
    main()