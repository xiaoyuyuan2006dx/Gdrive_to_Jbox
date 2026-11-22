#!/usr/bin/env python3
"""Optimized Google Drive (Colab mount) -> SJTU Jbox (pan.sjtu.edu.cn) uploader (v2).

Enhancements over gdrive_to_jbox.py:
1. Parallel single-pass directory scan via os.scandir() with cached stat metadata
   (eliminates duplicate FUSE tree walks and per-file os.stat() during packing).
2. Multi-threaded bounded-memory prefetch archiving (12 Drive reader threads by default)
   with automatic streaming fallback for oversized files.
3. 1-ahead pack/upload pipeline overlap in `subdir` mode.
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import io
import json
import os
import re
import shutil
import stat
import sys
import tarfile
import threading
import time
import zipfile
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, quote, urlparse

import requests
from requests.adapters import HTTPAdapter
from tqdm.auto import tqdm

DEFAULT_BASE_URL = "https://pan.sjtu.edu.cn"
DEFAULT_WORK_DIR = "/content/.jbox_transfer"
DEFAULT_CHUNK_MB = 4
DEFAULT_PACK_WORKERS = 12
DEFAULT_PREFETCH_MB = 128
PREFETCH_MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_PARTS_PER_WAVE = 50
MAX_CHUNKS_PER_FILE = 10000
COPY_BUF_SIZE = 1024 * 1024  # 1 MiB
_TOKEN_EXPIRE_CODES = {"InvalidAccessToken", "AccessTokenExpired"}
_NOT_FOUND_CODES = {
    "DirOrFileNotFound",
    "NoSuchKey",
    "FileNotFound",
    "DirectoryNotFound",
    "PathNotFound",
}
_SESSION_EXPIRED_CODES = {
    "NoSuchUpload",
    "UploadNotFound",
    "ConfirmKeyNotFound",
    "UploadSessionExpired",
    "InvalidConfirmKey",
}
_SSO_REDIRECT_PATH = (
    "/user/v1/sign-in/sso-login-redirect/xpw8ou8y"
    "?auto_redirect=true&from=web&custom_state=4ycSqbzfqM9mPuzOKmvTUQ%25253D%25253D"
)


class JboxTransferError(RuntimeError):
    """Base error for SJTU Jbox transfer failures."""


class JboxAuthError(JboxTransferError):
    """Authentication or credential error."""


class JboxSessionExpiredError(JboxTransferError):
    """Multipart upload session (confirmKey) expired or not found."""


class JboxSignatureExpiredError(JboxTransferError):
    """Pre-signed COS part upload signature expired (HTTP 403)."""


def normalize_remote_path(path: str) -> str:
    """Normalize a remote Jbox path to start with '/' without trailing '/'."""
    parts = [seg for seg in (path or "").strip().split("/") if seg and seg != "."]
    if any(seg == ".." for seg in parts):
        raise ValueError(f"非法远端路径（不允许 '..'）: {path!r}")
    return "/" + "/".join(parts) if parts else "/"


def join_remote_path(*parts: str) -> str:
    """Join multiple path segments into a normalized remote Jbox path."""
    segs: list[str] = []
    for part in parts:
        for seg in (part or "").strip().split("/"):
            if seg and seg != ".":
                if seg == "..":
                    raise ValueError(f"非法远端路径片段: {part!r}")
                segs.append(seg)
    return "/" + "/".join(segs) if segs else "/"


def encode_remote_path(path: str) -> str:
    """Percent-encode each segment of a remote Jbox path without leading/trailing slashes."""
    norm = normalize_remote_path(path)
    if norm == "/":
        return ""
    return "/".join(quote(seg, safe="") for seg in norm.lstrip("/").split("/"))


def format_bytes(num_bytes: int) -> str:
    """Format byte size into a human-readable string."""
    val = float(max(0, num_bytes))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if val < 1024.0 or unit == "TiB":
            return f"{int(val)} B" if unit == "B" else f"{val:.2f} {unit}"
        val /= 1024.0
    return f"{num_bytes} B"


def clean_credential(raw: str, prefixes: tuple[str, ...] = ("UserToken=", "JAAuthCookie=")) -> str:
    """Strip whitespace, surrounding quotes, and optional cookie/token key prefixes."""
    val = (raw or "").strip().strip('"').strip("'").strip()
    for prefix in prefixes:
        if val.startswith(prefix):
            val = val[len(prefix) :].strip()
    return val


def normalize_auth_mode(auth_mode: str) -> str:
    """Normalize authentication mode string to 'UserToken' or 'JAAuthCookie'."""
    key = (auth_mode or "").strip().lower()
    if key in {"usertoken", "user_token", "token", "-t", "usertoken (-t)"}:
        return "UserToken"
    if key in {"jaauthcookie", "jacookie", "cookie", "-c", "jaauthcookie (-c)"}:
        return "JAAuthCookie"
    raise ValueError(f"不支持的 AUTH_MODE: {auth_mode!r}（仅支持 UserToken / JAAuthCookie）")


def pread_exact(fd: int, offset: int, length: int) -> bytes:
    """Read exact byte length from an open file descriptor at offset without mutating file cursor."""
    if length == 0:
        return b""
    chunks: list[bytes] = []
    remaining = length
    pos = offset
    while remaining > 0:
        buf = os.pread(fd, remaining, pos)
        if not buf:
            raise EOFError(f"读取文件提前到达 EOF (offset={pos}, remaining={remaining})")
        chunks.append(buf)
        pos += len(buf)
        remaining -= len(buf)
    return b"".join(chunks)


def atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    """Atomically persist JSON payload via a temporary file and os.replace."""
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


def _parse_expiration(exp_value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 expiration string into a timezone-aware UTC datetime."""
    if not exp_value:
        return None
    text = str(exp_value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def login_with_ja_cookie(
    ja_cookie: str,
    base_url: str = DEFAULT_BASE_URL,
    timeout: tuple[int, int] = (20, 60),
) -> str:
    """Exchange a jAccount JAAuthCookie for a pan.sjtu.edu.cn UserToken."""
    cookie_val = clean_credential(ja_cookie, prefixes=("JAAuthCookie=",))
    if not cookie_val:
        raise JboxAuthError("JAAuthCookie 不能为空")

    base = base_url.rstrip("/")
    with requests.Session() as sess:
        sess.cookies.set("JAAuthCookie", cookie_val, domain="jaccount.sjtu.edu.cn", path="/jaccount")
        try:
            res = sess.get(f"{base}{_SSO_REDIRECT_PATH}", allow_redirects=True, timeout=timeout)
        except requests.RequestException as exc:
            raise JboxAuthError(f"jAccount SSO 重定向请求失败: {exc}") from exc

        if not res.ok:
            raise JboxAuthError(f"jAccount SSO 登录重定向返回 HTTP {res.status_code}")

        parsed = urlparse(res.url)
        if "jaccount" in (parsed.netloc or "").lower():
            raise JboxAuthError("JAAuthCookie 无效或已过期（被重定向回 jAccount 登录页）")

        code_vals = parse_qs(parsed.query).get("code")
        code = code_vals[0] if code_vals else ""
        if not code:
            match = re.search(r"code=([^&]+)", res.url)
            code = match.group(1) if match else ""
        if not code:
            raise JboxAuthError("未能在 SSO 回调地址中找到授权 code")

        verify_url = (
            f"{base}/user/v1/sign-in/verify-account-login/xpw8ou8y"
            f"?device_id=Chrome+116.0.0.0&type=sso&credential={quote(code, safe='')}"
        )
        try:
            vres = sess.post(verify_url, timeout=timeout)
        except requests.RequestException as exc:
            raise JboxAuthError(f"验证 SSO 回调 code 失败: {exc}") from exc

        if not vres.ok:
            raise JboxAuthError(f"SSO 登录验证返回 HTTP {vres.status_code}")

        data = vres.json()
        if data.get("status", 0) != 0:
            raise JboxAuthError(f"SSO 登录验证失败: {data.get('message') or data}")

        token = str(data.get("userToken") or "").strip()
        if not token:
            raise JboxAuthError("SSO 登录响应中缺少 userToken")
        return token


class JboxClient:
    """Thread-safe HTTP client for SJTU Jbox (pan.sjtu.edu.cn / Tencent SMH) APIs."""

    def __init__(
        self,
        user_token: str,
        base_url: str = DEFAULT_BASE_URL,
        cos_scheme: str = "https",
        timeout: tuple[int, int] = (20, 60),
    ) -> None:
        token = clean_credential(user_token, prefixes=("UserToken=",))
        if not token:
            raise JboxAuthError("UserToken 不能为空")
        self._user_token = token
        self.base_url = base_url.rstrip("/")
        self.cos_scheme = cos_scheme
        self.timeout = timeout
        self._local = threading.local()
        self._cred_lock = threading.Lock()
        self._dir_lock = threading.Lock()
        self._space_cred: Optional[dict[str, Any]] = None
        self._cred_deadline: float = 0.0
        self._ensured_dirs: set[str] = {"/"}

    def _get_session(self) -> requests.Session:
        """Return a thread-local requests.Session configured with connection pooling."""
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16)
            sess.mount("https://", adapter)
            sess.mount("http://", adapter)
            self._local.session = sess
        return sess

    @staticmethod
    def _extract_error(res: requests.Response) -> tuple[str, str]:
        """Extract SMH error code and message from a response body."""
        try:
            body = res.json()
            if isinstance(body, dict):
                return str(body.get("code") or ""), str(body.get("message") or "")
        except ValueError:
            pass
        return "", (res.text or "")[:300]

    @staticmethod
    def _is_token_expired(res: requests.Response, code: str) -> bool:
        """Check whether a response indicates an expired or invalid SMH accessToken."""
        return res.status_code == 401 or code in _TOKEN_EXPIRE_CODES

    @staticmethod
    def _is_session_expired(res: requests.Response, code: str, msg: str) -> bool:
        """Check whether a response indicates a missing or expired confirmKey session."""
        if res.status_code == 404 or code in _SESSION_EXPIRED_CODES:
            return True
        low = msg.lower()
        return "confirmkey" in low or "upload not found" in low or "nosuchupload" in low

    def get_space_cred(self, force_refresh: bool = False) -> dict[str, Any]:
        """Obtain or refresh personal space credentials (libraryId, spaceId, accessToken)."""
        now = time.monotonic()
        with self._cred_lock:
            if not force_refresh and self._space_cred is not None and now < self._cred_deadline:
                return self._space_cred

            url = (
                f"{self.base_url}/user/v1/space/1/personal"
                f"?user_token={quote(self._user_token, safe='')}"
            )
            try:
                res = self._get_session().post(url, timeout=self.timeout)
            except requests.RequestException as exc:
                raise JboxAuthError(f"获取云盘空间凭证请求失败: {exc}") from exc

            if not res.ok:
                code, msg = self._extract_error(res)
                raise JboxAuthError(
                    f"获取云盘空间凭证失败 (HTTP {res.status_code} {code}): {msg or 'UserToken 可能无效或已过期'}"
                )

            data = res.json()
            if data.get("status", 0) != 0:
                raise JboxAuthError(f"云盘空间凭证接口返回错误: {data.get('message') or data}")

            for key in ("libraryId", "spaceId", "accessToken"):
                if not data.get(key):
                    raise JboxAuthError(f"云盘空间凭证缺少字段 {key}")

            expires_in = int(data.get("expiresIn") or 7200)
            ttl = max(60, expires_in - 120)
            self._space_cred = data
            self._cred_deadline = time.monotonic() + ttl
            return data

    def _request_smh(
        self,
        method: str,
        url_builder: Callable[[dict[str, Any]], str],
        retry_on_401: bool = True,
        **kwargs: Any,
    ) -> requests.Response:
        """Send an SMH request and transparently refresh accessToken once on 401/expired."""
        kwargs.setdefault("timeout", self.timeout)
        cred = self.get_space_cred(force_refresh=False)
        res = self._get_session().request(method, url_builder(cred), **kwargs)
        if not res.ok and retry_on_401:
            code, _ = self._extract_error(res)
            if self._is_token_expired(res, code):
                cred = self.get_space_cred(force_refresh=True)
                res = self._get_session().request(method, url_builder(cred), **kwargs)
        return res

    def get_quota(self) -> dict[str, Optional[int]]:
        """Query personal space quota (used bytes, available bytes, capacity bytes)."""
        url = f"{self.base_url}/user/v1/space/1?user_token={quote(self._user_token, safe='')}"
        res = self._get_session().get(url, timeout=self.timeout)
        if not res.ok:
            code, msg = self._extract_error(res)
            raise JboxTransferError(f"查询云盘配额失败 (HTTP {res.status_code} {code}): {msg}")
        data = res.json()
        if data.get("status", 0) != 0:
            raise JboxTransferError(f"查询云盘配额接口报错: {data.get('message') or data}")

        def _opt_int(val: Any) -> Optional[int]:
            if val is None or val == "":
                return None
            try:
                return int(str(val))
            except (TypeError, ValueError):
                return None

        return {
            "size": _opt_int(data.get("size")),
            "availableSpace": _opt_int(data.get("availableSpace")),
            "capacity": _opt_int(data.get("capacity")),
        }

    def get_item_info(self, remote_path: str) -> Optional[dict[str, Any]]:
        """Fetch file or directory metadata, returning None if the path does not exist."""
        encoded = encode_remote_path(remote_path)

        def _url(cred: dict[str, Any]) -> str:
            return (
                f"{self.base_url}/api/v1/directory/{cred['libraryId']}/{cred['spaceId']}/{encoded}"
                f"?info&access_token={quote(cred['accessToken'], safe='')}"
            )

        res = self._request_smh("GET", _url)
        if not res.ok:
            code, msg = self._extract_error(res)
            if res.status_code == 404 or code in _NOT_FOUND_CODES:
                return None
            raise JboxTransferError(f"查询远端路径 {remote_path} 失败 (HTTP {res.status_code} {code}): {msg}")

        data = res.json()
        if data.get("status", 0) != 0:
            code = str(data.get("code") or "")
            if code in _NOT_FOUND_CODES:
                return None
            raise JboxTransferError(f"查询远端路径 {remote_path} 返回错误: {data.get('message') or data}")
        return data

    def ensure_dir(self, remote_dir: str) -> None:
        """Create remote_dir and any missing parent directories on Jbox."""
        norm = normalize_remote_path(remote_dir)
        if norm == "/":
            return

        with self._dir_lock:
            if norm in self._ensured_dirs:
                return

            segments = norm.lstrip("/").split("/")
            prefix = ""
            for seg in segments:
                prefix = f"{prefix}/{seg}"
                if prefix in self._ensured_dirs:
                    continue
                encoded = encode_remote_path(prefix)

                def _url(cred: dict[str, Any], ep: str = encoded) -> str:
                    return (
                        f"{self.base_url}/api/v1/directory/{cred['libraryId']}/{cred['spaceId']}/{ep}"
                        f"?conflict_resolution_strategy=ask&access_token={quote(cred['accessToken'], safe='')}"
                    )

                res = self._request_smh("PUT", _url)
                if res.ok:
                    data = res.json() if res.content else {}
                    if isinstance(data, dict) and data.get("status", 0) != 0:
                        if data.get("code") != "SameNameDirectoryOrFileExists":
                            raise JboxTransferError(
                                f"创建远端目录 {prefix} 失败: {data.get('message') or data}"
                            )
                else:
                    code, msg = self._extract_error(res)
                    if code != "SameNameDirectoryOrFileExists":
                        raise JboxTransferError(
                            f"创建远端目录 {prefix} 失败 (HTTP {res.status_code} {code}): {msg}"
                        )
                self._ensured_dirs.add(prefix)

    def start_multipart(self, remote_path: str, part_numbers: list[int]) -> dict[str, Any]:
        """Initialize a multipart upload session and obtain credentials for up to 50 parts."""
        encoded = encode_remote_path(remote_path)

        def _url(cred: dict[str, Any]) -> str:
            return (
                f"{self.base_url}/api/v1/file/{cred['libraryId']}/{cred['spaceId']}/{encoded}"
                f"?multipart&conflict_resolution_strategy=rename"
                f"&access_token={quote(cred['accessToken'], safe='')}"
            )

        res = self._request_smh("POST", _url, json={"partNumberRange": part_numbers[:MAX_PARTS_PER_WAVE]})
        if not res.ok:
            code, msg = self._extract_error(res)
            raise JboxTransferError(f"初始化分片上传失败 (HTTP {res.status_code} {code}): {msg}")

        data = res.json()
        if data.get("status", 0) != 0:
            raise JboxTransferError(f"初始化分片上传返回错误: {data.get('message') or data}")
        return data

    def renew_multipart(self, confirm_key: str, part_numbers: list[int]) -> dict[str, Any]:
        """Renew signed part credentials for an existing multipart upload confirmKey."""
        key_enc = quote(confirm_key, safe="")

        def _url(cred: dict[str, Any]) -> str:
            return (
                f"{self.base_url}/api/v1/file/{cred['libraryId']}/{cred['spaceId']}/{key_enc}"
                f"?renew&access_token={quote(cred['accessToken'], safe='')}"
            )

        res = self._request_smh("POST", _url, json={"partNumberRange": part_numbers[:MAX_PARTS_PER_WAVE]})
        if not res.ok:
            code, msg = self._extract_error(res)
            if self._is_session_expired(res, code, msg):
                raise JboxSessionExpiredError(f"上传会话已过期或不存在 (HTTP {res.status_code} {code}): {msg}")
            raise JboxTransferError(f"刷新分片签名失败 (HTTP {res.status_code} {code}): {msg}")

        data = res.json()
        if data.get("status", 0) != 0:
            code = str(data.get("code") or "")
            msg = str(data.get("message") or "")
            if code in _SESSION_EXPIRED_CODES:
                raise JboxSessionExpiredError(f"上传会话已过期 ({code}): {msg}")
            raise JboxTransferError(f"刷新分片签名返回错误: {msg or data}")
        return data

    def get_upload_info(self, confirm_key: str) -> Optional[dict[str, Any]]:
        """Query server-side upload progress and uploaded part list for confirmKey."""
        key_enc = quote(confirm_key, safe="")

        def _url(cred: dict[str, Any]) -> str:
            return (
                f"{self.base_url}/api/v1/file/{cred['libraryId']}/{cred['spaceId']}/{key_enc}"
                f"?upload&access_token={quote(cred['accessToken'], safe='')}"
            )

        res = self._request_smh("GET", _url)
        if not res.ok:
            code, msg = self._extract_error(res)
            if self._is_session_expired(res, code, msg):
                return None
            raise JboxTransferError(f"查询分片上传状态失败 (HTTP {res.status_code} {code}): {msg}")

        data = res.json()
        if data.get("status", 0) != 0:
            code = str(data.get("code") or "")
            if code in _SESSION_EXPIRED_CODES:
                return None
            raise JboxTransferError(f"查询分片上传状态返回错误: {data.get('message') or data}")
        return data

    def upload_part(
        self,
        upload_ctx: dict[str, Any],
        part_number: int,
        data: bytes,
        timeout: tuple[int, int] = (30, 180),
    ) -> str:
        """Upload a single part directly to COS using pre-signed headers and verify its ETag."""
        parts_map = upload_ctx.get("parts") or {}
        part_entry = parts_map.get(str(part_number))
        if not part_entry:
            raise JboxSignatureExpiredError(f"分片 {part_number} 缺少上传签名")

        raw_path = str(upload_ctx.get("path") or "")
        cos_path = raw_path if raw_path.startswith("/") else "/" + raw_path
        upload_id = quote(str(upload_ctx.get("uploadId") or ""), safe="")
        url = (
            f"{self.cos_scheme}://{upload_ctx['domain']}{cos_path}"
            f"?uploadId={upload_id}&partNumber={part_number}"
        )
        headers = {str(k): str(v) for k, v in (part_entry.get("headers") or {}).items()}
        res = self._get_session().put(url, headers=headers, data=data, timeout=timeout)

        if res.status_code == 403:
            raise JboxSignatureExpiredError(f"分片 {part_number} 签名过期或被拒 (HTTP 403)")
        if not res.ok:
            raise JboxTransferError(
                f"分片 {part_number} 上传至 COS 失败 (HTTP {res.status_code}): {(res.text or '')[:200]}"
            )

        etag = (res.headers.get("ETag") or "").strip().strip('"').strip().lower()
        if len(etag) == 32 and all(c in "0123456789abcdef" for c in etag):
            expected_md5 = hashlib.md5(data).hexdigest()
            if etag != expected_md5:
                raise JboxTransferError(
                    f"分片 {part_number} ETag 校验不一致: remote={etag}, local={expected_md5}"
                )
        return etag

    def confirm_upload(self, confirm_key: str, conflict: str = "overwrite") -> dict[str, Any]:
        """Finalize and merge all uploaded parts on Jbox into the destination file."""
        key_enc = quote(confirm_key, safe="")
        conflict_enc = quote(conflict, safe="")

        def _url(cred: dict[str, Any]) -> str:
            return (
                f"{self.base_url}/api/v1/file/{cred['libraryId']}/{cred['spaceId']}/{key_enc}"
                f"?confirm&conflict_resolution_strategy={conflict_enc}"
                f"&access_token={quote(cred['accessToken'], safe='')}"
            )

        res = self._request_smh("POST", _url)
        if not res.ok:
            code, msg = self._extract_error(res)
            if self._is_session_expired(res, code, msg):
                raise JboxSessionExpiredError(f"确认合并时会话不存在或已过期 (HTTP {res.status_code}): {msg}")
            raise JboxTransferError(f"确认合并文件失败 (HTTP {res.status_code} {code}): {msg}")

        data = res.json()
        if data.get("status", 0) != 0:
            code = str(data.get("code") or "")
            msg = str(data.get("message") or "")
            if code in _SESSION_EXPIRED_CODES:
                raise JboxSessionExpiredError(f"确认合并时会话过期 ({code}): {msg}")
            raise JboxTransferError(f"确认合并文件返回错误: {msg or data}")
        return data

    def delete_file(self, remote_path: str) -> None:
        """Move a remote file to Jbox recycle bin (permanent=0)."""
        encoded = encode_remote_path(remote_path)

        def _url(cred: dict[str, Any]) -> str:
            return (
                f"{self.base_url}/api/v1/file/{cred['libraryId']}/{cred['spaceId']}/{encoded}"
                f"?permanent=0&access_token={quote(cred['accessToken'], safe='')}"
            )

        res = self._request_smh("DELETE", _url)
        if not res.ok and res.status_code != 204:
            code, msg = self._extract_error(res)
            raise JboxTransferError(f"删除远端文件 {remote_path} 失败 (HTTP {res.status_code} {code}): {msg}")


def compute_chunk_layout(
    file_size: int,
    chunk_mb: int = DEFAULT_CHUNK_MB,
    chunk_bytes: Optional[int] = None,
) -> tuple[int, int]:
    """Compute (chunk_size_bytes, chunk_count), keeping chunk_count <= 10000."""
    if chunk_bytes is not None:
        if int(chunk_bytes) < 1:
            raise ValueError(f"chunk_bytes 必须 >= 1，收到: {chunk_bytes}")
        chunk_size = int(chunk_bytes)
        step = max(chunk_size, 1)
    else:
        if chunk_mb < 1:
            raise ValueError(f"chunk_mb 必须 >= 1，收到: {chunk_mb}")
        step = 4 * 1024 * 1024
        chunk_size = chunk_mb * 1024 * 1024
    while file_size > MAX_CHUNKS_PER_FILE * chunk_size:
        chunk_size += step
    chunk_count = 1 if file_size == 0 else (file_size + chunk_size - 1) // chunk_size
    return chunk_size, chunk_count


def expected_part_size(file_size: int, chunk_size: int, part_number: int) -> int:
    """Return exact expected byte size of 1-indexed part_number."""
    if file_size == 0:
        return 0
    offset = (part_number - 1) * chunk_size
    return max(0, min(chunk_size, file_size - offset))


class ResumableUploader:
    """Chunked, multi-threaded uploader with confirmKey-based breakpoint resume."""

    def __init__(
        self,
        client: JboxClient,
        work_dir: str = DEFAULT_WORK_DIR,
        chunk_mb: int = DEFAULT_CHUNK_MB,
        workers: int = 4,
        conflict: str = "overwrite",
        force_upload: bool = False,
        max_retries: int = 5,
        chunk_bytes: Optional[int] = None,
    ) -> None:
        self.client = client
        self.work_dir = work_dir
        self.checkpoint_dir = os.path.join(work_dir, "checkpoints")
        self.chunk_mb = chunk_mb
        self.chunk_bytes = chunk_bytes
        self.workers = max(1, int(workers))
        self.conflict = conflict
        self.force_upload = force_upload
        self.max_retries = max(1, int(max_retries))

    def _checkpoint_path(self, remote_path: str) -> str:
        norm = normalize_remote_path(remote_path)
        digest = hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]
        return os.path.join(self.checkpoint_dir, f"{digest}.json")

    def _load_matching_checkpoint(
        self,
        ckpt_path: str,
        abs_local: str,
        norm_remote: str,
        file_size: int,
        file_mtime_ns: int,
        chunk_size: int,
        chunk_count: int,
    ) -> Optional[dict[str, Any]]:
        if not os.path.exists(ckpt_path):
            return None
        try:
            with open(ckpt_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if (
                data.get("local_path") == abs_local
                and data.get("remote_path") == norm_remote
                and data.get("file_size") == file_size
                and data.get("file_mtime_ns") == file_mtime_ns
                and data.get("chunk_size") == chunk_size
                and data.get("chunk_count") == chunk_count
                and data.get("confirm_key")
            ):
                return data
        except Exception:
            return None
        return None

    @staticmethod
    def _save_checkpoint(
        ckpt_path: str,
        abs_local: str,
        norm_remote: str,
        file_size: int,
        file_mtime_ns: int,
        chunk_size: int,
        chunk_count: int,
        confirm_key: str,
        completed_parts: set[int],
    ) -> None:
        atomic_write_json(
            ckpt_path,
            {
                "version": 1,
                "local_path": abs_local,
                "remote_path": norm_remote,
                "file_size": file_size,
                "file_mtime_ns": file_mtime_ns,
                "chunk_size": chunk_size,
                "chunk_count": chunk_count,
                "confirm_key": confirm_key,
                "completed_parts": sorted(completed_parts),
            },
        )

    @staticmethod
    def _remove_checkpoint(ckpt_path: str) -> None:
        if os.path.exists(ckpt_path):
            try:
                os.remove(ckpt_path)
            except OSError:
                pass

    def _verify_remote_size(self, remote_path: str, expected_size: int, attempts: int = 3) -> dict[str, Any]:
        last_info: Optional[dict[str, Any]] = None
        for idx in range(attempts):
            info = self.client.get_item_info(remote_path)
            last_info = info
            if info and info.get("type") == "file" and info.get("size") is not None:
                if int(str(info["size"])) == expected_size:
                    return info
            if idx + 1 < attempts:
                time.sleep(1.0)
        actual = last_info.get("size") if last_info else None
        raise JboxTransferError(
            f"远端校验失败: {remote_path} 期望 {expected_size} bytes，实际返回 {actual}"
        )

    @staticmethod
    def _is_ctx_expiring(upload_ctx: dict[str, Any], margin_seconds: int = 120) -> bool:
        exp_dt = _parse_expiration(upload_ctx.get("expiration"))
        if exp_dt is None:
            return False
        return (exp_dt - datetime.now(timezone.utc)).total_seconds() < margin_seconds

    def _reconcile_resume(
        self,
        ckpt: dict[str, Any],
        norm_remote: str,
        file_size: int,
        chunk_size: int,
        chunk_count: int,
    ) -> tuple[Optional[str], set[int], bool]:
        """Check server state for an existing confirm_key.

        Returns (confirm_key, completed_parts, already_confirmed_remotely).
        """
        confirm_key = str(ckpt["confirm_key"])
        info = self.client.get_upload_info(confirm_key)
        if info is None:
            return None, set(), False

        if info.get("confirmed") is True:
            remote_info = self.client.get_item_info(norm_remote)
            if (
                remote_info
                and remote_info.get("type") == "file"
                and remote_info.get("size") is not None
                and int(str(remote_info["size"])) == file_size
            ):
                return confirm_key, set(range(1, chunk_count + 1)), True
            return None, set(), False

        server_parts = info.get("parts")
        if isinstance(server_parts, list):
            verified: set[int] = set()
            for item in server_parts:
                if not isinstance(item, dict):
                    continue
                raw_num = item.get("PartNumber")
                if raw_num is None:
                    continue
                try:
                    pnum = int(raw_num)
                except (TypeError, ValueError):
                    continue
                if not (1 <= pnum <= chunk_count):
                    continue
                raw_sz = item.get("Size")
                if raw_sz is not None and int(raw_sz) != expected_part_size(file_size, chunk_size, pnum):
                    continue
                verified.add(pnum)
            return confirm_key, verified, False

        fallback = {
            int(p)
            for p in (ckpt.get("completed_parts") or [])
            if isinstance(p, int) and 1 <= p <= chunk_count
        }
        return confirm_key, fallback, False

    def upload_file(self, local_path: str, remote_path: str, label: Optional[str] = None) -> dict[str, Any]:
        """Upload a local file to remote_path with chunked resume and post-verification."""
        abs_local = os.path.abspath(local_path)
        norm_remote = normalize_remote_path(remote_path)
        if not os.path.isfile(abs_local):
            raise FileNotFoundError(f"找不到待上传文件: {abs_local}")

        st = os.stat(abs_local)
        file_size = st.st_size
        file_mtime_ns = st.st_mtime_ns
        display_name = label or os.path.basename(norm_remote)

        parent_remote = norm_remote.rsplit("/", 1)[0] or "/"
        self.client.ensure_dir(parent_remote)

        if not self.force_upload:
            existing = self.client.get_item_info(norm_remote)
            if (
                existing
                and existing.get("type") == "file"
                and existing.get("size") is not None
                and int(str(existing["size"])) == file_size
            ):
                print(f"⏭️ 远端已存在同名同大小文件 ({format_bytes(file_size)})，跳过: {norm_remote}")
                return {
                    "remote_path": norm_remote,
                    "size": file_size,
                    "skipped": True,
                    "chunks": 0,
                }

        chunk_size, chunk_count = compute_chunk_layout(
            file_size, self.chunk_mb, chunk_bytes=self.chunk_bytes
        )
        ckpt_path = self._checkpoint_path(norm_remote)
        confirm_key: Optional[str] = None
        completed_parts: set[int] = set()

        ckpt = self._load_matching_checkpoint(
            ckpt_path, abs_local, norm_remote, file_size, file_mtime_ns, chunk_size, chunk_count
        )
        if ckpt is not None:
            confirm_key, completed_parts, already_confirmed = self._reconcile_resume(
                ckpt, norm_remote, file_size, chunk_size, chunk_count
            )
            if already_confirmed:
                self._remove_checkpoint(ckpt_path)
                print(f"✅ 断点校验已完成，远端文件一致 ({format_bytes(file_size)}): {norm_remote}")
                return {
                    "remote_path": norm_remote,
                    "size": file_size,
                    "skipped": True,
                    "chunks": chunk_count,
                }
            if confirm_key:
                print(
                    f"♻️ 命中断点续传 ({display_name}): 已完成 {len(completed_parts)}/{chunk_count} 个分片"
                )
            else:
                self._remove_checkpoint(ckpt_path)

        for session_attempt in range(2):
            remaining = [p for p in range(1, chunk_count + 1) if p not in completed_parts]
            waves = [
                remaining[i : i + MAX_PARTS_PER_WAVE]
                for i in range(0, len(remaining), MAX_PARTS_PER_WAVE)
            ]
            done_bytes = sum(expected_part_size(file_size, chunk_size, p) for p in completed_parts)

            ctx_lock = threading.Lock()
            state_lock = threading.Lock()
            upload_ctx: dict[str, Any] = {}
            fd = os.open(abs_local, os.O_RDONLY)
            pbar = tqdm(
                total=file_size,
                initial=done_bytes,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"⬆️ {display_name}",
            )

            restart_requested = False
            try:
                for wave_parts in waves:
                    if confirm_key is None:
                        upload_ctx = self.client.start_multipart(norm_remote, wave_parts)
                        confirm_key = str(upload_ctx["confirmKey"])
                        self._save_checkpoint(
                            ckpt_path,
                            abs_local,
                            norm_remote,
                            file_size,
                            file_mtime_ns,
                            chunk_size,
                            chunk_count,
                            confirm_key,
                            completed_parts,
                        )
                    else:
                        try:
                            upload_ctx = self.client.renew_multipart(confirm_key, wave_parts)
                        except JboxSessionExpiredError:
                            if session_attempt == 0:
                                restart_requested = True
                                break
                            raise

                    def _ensure_valid_ctx(
                        part_num: int,
                        *,
                        force: bool = False,
                        prev_ctx: Optional[dict[str, Any]] = None,
                        current_wave: list[int] = wave_parts,
                    ) -> dict[str, Any]:
                        nonlocal upload_ctx
                        with ctx_lock:
                            if (
                                force
                                and prev_ctx is not None
                                and upload_ctx is not prev_ctx
                                and str(part_num) in (upload_ctx.get("parts") or {})
                                and not self._is_ctx_expiring(upload_ctx)
                            ):
                                return upload_ctx

                            need_renew = (
                                force
                                or not upload_ctx
                                or self._is_ctx_expiring(upload_ctx)
                                or str(part_num) not in (upload_ctx.get("parts") or {})
                            )
                            if need_renew:
                                with state_lock:
                                    uncompleted = [
                                        p for p in current_wave if p not in completed_parts
                                    ]
                                if part_num not in uncompleted:
                                    uncompleted.insert(0, part_num)
                                assert confirm_key is not None
                                upload_ctx = self.client.renew_multipart(
                                    confirm_key, uncompleted[:MAX_PARTS_PER_WAVE]
                                )
                            return upload_ctx

                    def _upload_one_part(part_num: int) -> None:
                        offset = (part_num - 1) * chunk_size
                        part_len = expected_part_size(file_size, chunk_size, part_num)
                        data = pread_exact(fd, offset, part_len)

                        last_exc: Optional[Exception] = None
                        prev_ctx: Optional[dict[str, Any]] = None
                        for attempt in range(self.max_retries + 1):
                            force_refresh = isinstance(last_exc, JboxSignatureExpiredError)
                            ctx = _ensure_valid_ctx(
                                part_num,
                                force=force_refresh,
                                prev_ctx=prev_ctx if force_refresh else None,
                            )
                            prev_ctx = ctx
                            try:
                                self.client.upload_part(ctx, part_num, data)
                                with state_lock:
                                    completed_parts.add(part_num)
                                    pbar.update(part_len)
                                    assert confirm_key is not None
                                    self._save_checkpoint(
                                        ckpt_path,
                                        abs_local,
                                        norm_remote,
                                        file_size,
                                        file_mtime_ns,
                                        chunk_size,
                                        chunk_count,
                                        confirm_key,
                                        completed_parts,
                                    )
                                return
                            except JboxSessionExpiredError:
                                raise
                            except (JboxSignatureExpiredError, JboxTransferError, requests.RequestException) as exc:
                                last_exc = exc
                                if attempt >= self.max_retries:
                                    raise
                                time.sleep(min(2 ** attempt, 30))

                    max_w = min(self.workers, len(wave_parts))
                    with ThreadPoolExecutor(max_workers=max_w) as pool:
                        futures = [pool.submit(_upload_one_part, p) for p in wave_parts]
                        try:
                            for fut in as_completed(futures):
                                fut.result()
                        except JboxSessionExpiredError:
                            for f in futures:
                                f.cancel()
                            if session_attempt == 0:
                                restart_requested = True
                                break
                            raise
                        except Exception:
                            for f in futures:
                                f.cancel()
                            raise

                    if restart_requested:
                        break
            except KeyboardInterrupt:
                print(
                    f"\n⚠️ 上传已中断。断点文件已保留在 {ckpt_path}，重新运行即可自动续传。"
                )
                raise
            except Exception:
                if not restart_requested:
                    print(
                        f"\n💡 断点进度已保留在 {ckpt_path}，修复问题后重新运行即可自动续传。"
                    )
                raise
            finally:
                pbar.close()
                os.close(fd)

            if restart_requested:
                print("⚠️ 云端上传会话已过期，正在清理旧断点并重新初始化上传...")
                self._remove_checkpoint(ckpt_path)
                confirm_key = None
                completed_parts = set()
                continue

            assert confirm_key is not None
            try:
                confirm_res = self.client.confirm_upload(confirm_key, conflict=self.conflict)
            except JboxSessionExpiredError:
                if session_attempt == 0:
                    print("⚠️ 合并确认时云端上传会话已过期，正在重新初始化上传...")
                    self._remove_checkpoint(ckpt_path)
                    confirm_key = None
                    completed_parts = set()
                    continue
                raise

            if confirm_res.get("size") is not None and int(str(confirm_res["size"])) != file_size:
                raise JboxTransferError(
                    f"合并响应大小不一致: 期望 {file_size}，实际 {confirm_res.get('size')}"
                )
            self._verify_remote_size(norm_remote, file_size, attempts=3)
            self._remove_checkpoint(ckpt_path)
            print(f"✅ 上传并校验完成 ({format_bytes(file_size)}, {chunk_count} 分片): {norm_remote}")
            return {
                "remote_path": norm_remote,
                "size": file_size,
                "skipped": False,
                "chunks": chunk_count,
            }

        raise JboxTransferError(f"上传失败（会话重试次数耗尽）: {norm_remote}")


def _scan_one_dir(
    dir_path: str, rel_posix: str
) -> tuple[
    list[tuple[str, str, int, int]],
    list[tuple[str, str, int, int, int]],
    int,
    int,
]:
    """Scan a single directory via os.scandir() and return cached stat entries."""
    sub_dirs: list[tuple[str, str, int, int]] = []
    sub_files: list[tuple[str, str, int, int, int]] = []
    local_bytes = 0
    local_max_mtime = 0

    with os.scandir(dir_path) as it:
        for entry in it:
            if entry.is_symlink():
                continue
            name = entry.name
            rel = f"{rel_posix}/{name}" if rel_posix else name
            st = entry.stat(follow_symlinks=False)
            if st.st_mtime_ns > local_max_mtime:
                local_max_mtime = st.st_mtime_ns
            if stat.S_ISDIR(st.st_mode):
                sub_dirs.append((entry.path, rel, st.st_mtime_ns, st.st_mode))
            elif stat.S_ISREG(st.st_mode):
                sub_files.append((entry.path, rel, st.st_size, st.st_mtime_ns, st.st_mode))
                local_bytes += st.st_size

    return sub_dirs, sub_files, local_bytes, local_max_mtime


def scan_tree(root_dir: str, scan_workers: int = 8) -> dict[str, Any]:
    """Walk a directory tree concurrently via os.scandir() and cache all stat metadata."""
    abs_root = os.path.abspath(root_dir)
    if not os.path.isdir(abs_root):
        raise NotADirectoryError(f"不是有效目录: {abs_root}")

    root_st = os.stat(abs_root)
    dirs: list[tuple[str, str, int, int]] = []
    files: list[tuple[str, str, int, int, int]] = []
    total_bytes = 0
    max_mtime_ns = 0

    max_w = max(1, min(int(scan_workers), 16))
    frontier: list[tuple[str, str]] = [(abs_root, "")]

    with ThreadPoolExecutor(max_workers=max_w) as pool:
        while frontier:
            if len(frontier) == 1 or max_w == 1:
                results = [_scan_one_dir(dpath, rpos) for dpath, rpos in frontier]
            else:
                futures = [pool.submit(_scan_one_dir, dpath, rpos) for dpath, rpos in frontier]
                results = [fut.result() for fut in futures]

            next_frontier: list[tuple[str, str]] = []
            for sub_dirs, sub_files, local_bytes, local_max_mtime in results:
                dirs.extend(sub_dirs)
                files.extend(sub_files)
                total_bytes += local_bytes
                if local_max_mtime > max_mtime_ns:
                    max_mtime_ns = local_max_mtime
                for dpath, rel, _, _ in sub_dirs:
                    next_frontier.append((dpath, rel))
            frontier = next_frontier

    dirs.sort(key=lambda item: item[1])
    files.sort(key=lambda item: item[1])
    return {
        "root_meta": (abs_root, root_st.st_mtime_ns, root_st.st_mode),
        "dirs": dirs,
        "files": files,
        "file_count": len(files),
        "total_bytes": total_bytes,
        "max_mtime_ns": max_mtime_ns,
    }


def ensure_free_disk(work_dir: str, total_source_bytes: int) -> None:
    """Verify local workspace has at least 105% + 64 MiB free space for temporary archive."""
    os.makedirs(work_dir, exist_ok=True)
    free_bytes = shutil.disk_usage(work_dir).free
    needed = int(total_source_bytes * 1.05) + 64 * 1024 * 1024
    if free_bytes < needed:
        raise JboxTransferError(
            f"本地临时空间不足 ({work_dir}): 可用 {format_bytes(free_bytes)} < 打包预计需要 {format_bytes(needed)}"
        )


def _make_zip_info_from_meta(
    arcname: str,
    size: int,
    mtime_ns: int,
    mode: int,
    is_dir: bool,
) -> zipfile.ZipInfo:
    """Build ZipInfo from cached stat metadata without triggering another FUSE os.stat()."""
    if is_dir and not arcname.endswith("/"):
        arcname = arcname + "/"
    tm = time.localtime(mtime_ns / 1_000_000_000)
    if tm.tm_year < 1980:
        date_time = (1980, 1, 1, 0, 0, 0)
    else:
        date_time = (tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
    zinfo = zipfile.ZipInfo(arcname, date_time=date_time)
    if is_dir:
        zinfo.external_attr = ((mode & 0xFFFF) << 16) | 0x10
        zinfo.file_size = 0
    else:
        zinfo.external_attr = (mode & 0xFFFF) << 16
        zinfo.file_size = size
    zinfo.compress_type = zipfile.ZIP_STORED
    return zinfo


def _make_tar_info_from_meta(
    arcname: str,
    size: int,
    mtime_ns: int,
    mode: int,
    is_dir: bool,
) -> tarfile.TarInfo:
    """Build TarInfo from cached stat metadata without triggering another FUSE os.stat()."""
    tinfo = tarfile.TarInfo(name=arcname)
    tinfo.mtime = int(mtime_ns // 1_000_000_000)
    tinfo.mode = stat.S_IMODE(mode)
    tinfo.type = tarfile.DIRTYPE if is_dir else tarfile.REGTYPE
    tinfo.size = 0 if is_dir else size
    return tinfo


def _read_file_bytes(path: str) -> bytes:
    """Read an entire file into memory for bounded prefetch archiving."""
    with open(path, "rb") as f:
        return f.read()


class _ProgressFileReader:
    """Wrap a binary file stream to advance a tqdm progress bar during tarfile streaming."""

    def __init__(self, raw_file: Any, pbar: tqdm) -> None:
        self._raw = raw_file
        self._pbar = pbar

    def read(self, size: int = -1) -> bytes:
        chunk = self._raw.read(size)
        if chunk:
            self._pbar.update(len(chunk))
        return chunk


def build_or_reuse_archive(
    src_dir: str,
    archive_base_name: str,
    archive_dir: str,
    archive_format: str = "zip",
    force_repack: bool = False,
    arc_root_name: Optional[str] = None,
    pre_scanned_tree: Optional[dict[str, Any]] = None,
    pack_workers: int = DEFAULT_PACK_WORKERS,
    prefetch_mb: int = DEFAULT_PREFETCH_MB,
    prefetch_max_file_bytes: int = PREFETCH_MAX_FILE_BYTES,
    show_progress: bool = True,
) -> tuple[str, int, bool]:
    """Build an uncompressed .zip or .tar archive using parallel prefetch, or reuse cached."""
    if archive_format not in {"zip", "tar"}:
        raise ValueError(f"不支持的 archive_format: {archive_format!r}")

    abs_src = os.path.abspath(src_dir)
    root_name = arc_root_name or os.path.basename(abs_src.rstrip("/"))
    archive_name = f"{archive_base_name}.{archive_format}"
    archive_path = os.path.join(archive_dir, archive_name)
    meta_path = os.path.join(archive_dir, f"{archive_name}.pack_meta.json")

    tree = pre_scanned_tree if pre_scanned_tree is not None else scan_tree(abs_src, scan_workers=pack_workers)
    src_count = tree["file_count"]
    src_bytes = tree["total_bytes"]
    src_max_mtime = tree["max_mtime_ns"]

    if not force_repack and os.path.isfile(archive_path) and os.path.isfile(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            st_arc = os.stat(archive_path)
            if (
                meta.get("source_path") == abs_src
                and meta.get("archive_format") == archive_format
                and meta.get("arc_root_name") == root_name
                and meta.get("source_file_count") == src_count
                and meta.get("source_total_bytes") == src_bytes
                and meta.get("source_max_mtime_ns") == src_max_mtime
                and meta.get("archive_size") == st_arc.st_size
                and meta.get("archive_mtime_ns") == st_arc.st_mtime_ns
            ):
                print(
                    f"♻️ 复用本地已有归档 {archive_name} ({format_bytes(st_arc.st_size)}, {src_count} 个文件)"
                )
                return archive_path, st_arc.st_size, True
        except Exception:
            pass

    os.makedirs(archive_dir, exist_ok=True)
    ensure_free_disk(archive_dir, src_bytes)
    tmp_path = f"{archive_path}.tmp.{os.getpid()}.{threading.get_ident()}"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    if "root_meta" in tree:
        _, root_mtime_ns, root_mode = tree["root_meta"]
    else:
        root_st = os.stat(abs_src)
        root_mtime_ns, root_mode = root_st.st_mtime_ns, root_st.st_mode

    files = tree["files"]
    dirs = tree["dirs"]
    p_workers = max(1, int(pack_workers))
    budget_bytes = max(1, int(prefetch_mb)) * 1024 * 1024
    max_file_prefetch = min(int(prefetch_max_file_bytes), max(1, budget_bytes // 2))
    max_inflight_files = max(p_workers * 3, 24)

    print(
        f"📦 正在无压缩打包 {archive_name} ({src_count} 个文件, {format_bytes(src_bytes)}, "
        f"预读线程={p_workers}, 缓冲={prefetch_mb} MiB)..."
    )
    t0 = time.monotonic()
    pbar = tqdm(
        total=src_bytes,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=f"📦 {archive_name}",
        disable=not show_progress,
    )

    pending: deque[tuple[tuple[str, str, int, int, int], Optional[Future[bytes]], int]] = deque()
    inflight_bytes = 0
    next_idx = 0

    def _write_entries(writer_cb: Callable[[tuple[str, str, int, int, int], Optional[bytes]], None]) -> None:
        nonlocal inflight_bytes, next_idx
        if p_workers <= 1 or len(files) == 0:
            for entry in files:
                writer_cb(entry, None)
            return

        with ThreadPoolExecutor(max_workers=p_workers) as pool:
            def _submit_more() -> None:
                nonlocal inflight_bytes, next_idx
                while next_idx < len(files) and len(pending) < max_inflight_files:
                    entry = files[next_idx]
                    fpath, _, fsize, _, _ = entry
                    if fsize <= max_file_prefetch:
                        if not pending or (inflight_bytes + fsize <= budget_bytes):
                            fut = pool.submit(_read_file_bytes, fpath)
                            pending.append((entry, fut, fsize))
                            inflight_bytes += fsize
                            next_idx += 1
                        else:
                            break
                    else:
                        pending.append((entry, None, 0))
                        next_idx += 1

            try:
                _submit_more()
                while pending:
                    entry, fut, reserved = pending.popleft()
                    if fut is not None:
                        data = fut.result()
                        inflight_bytes -= reserved
                        writer_cb(entry, data)
                        del data
                    else:
                        writer_cb(entry, None)
                    _submit_more()
            except Exception:
                for _, fut, _ in pending:
                    if fut is not None:
                        fut.cancel()
                raise

    try:
        if archive_format == "zip":
            with zipfile.ZipFile(
                tmp_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True
            ) as zf:
                zf.writestr(_make_zip_info_from_meta(root_name + "/", 0, root_mtime_ns, root_mode, True), b"")
                for _, d_rel, d_mtime_ns, d_mode in dirs:
                    zf.writestr(
                        _make_zip_info_from_meta(f"{root_name}/{d_rel}/", 0, d_mtime_ns, d_mode, True),
                        b"",
                    )

                def _write_zip_entry(entry: tuple[str, str, int, int, int], data: Optional[bytes]) -> None:
                    fpath, f_rel, fsize, f_mtime_ns, f_mode = entry
                    arcname = f"{root_name}/{f_rel}"
                    if data is not None:
                        f_zi = _make_zip_info_from_meta(arcname, len(data), f_mtime_ns, f_mode, False)
                        zf.writestr(f_zi, data)
                        pbar.update(len(data))
                    else:
                        f_zi = _make_zip_info_from_meta(arcname, fsize, f_mtime_ns, f_mode, False)
                        with zf.open(f_zi, "w", force_zip64=(fsize >= 0xFFFFFFFF)) as dst, open(
                            fpath, "rb"
                        ) as src:
                            while True:
                                buf = src.read(COPY_BUF_SIZE)
                                if not buf:
                                    break
                                dst.write(buf)
                                pbar.update(len(buf))

                _write_entries(_write_zip_entry)
        else:
            with tarfile.open(tmp_path, "w", format=tarfile.PAX_FORMAT) as tf:
                tf.addfile(_make_tar_info_from_meta(root_name, 0, root_mtime_ns, root_mode, True))
                for _, d_rel, d_mtime_ns, d_mode in dirs:
                    tf.addfile(
                        _make_tar_info_from_meta(f"{root_name}/{d_rel}", 0, d_mtime_ns, d_mode, True)
                    )

                def _write_tar_entry(entry: tuple[str, str, int, int, int], data: Optional[bytes]) -> None:
                    fpath, f_rel, fsize, f_mtime_ns, f_mode = entry
                    arcname = f"{root_name}/{f_rel}"
                    if data is not None:
                        tinfo = _make_tar_info_from_meta(arcname, len(data), f_mtime_ns, f_mode, False)
                        tf.addfile(tinfo, fileobj=io.BytesIO(data))
                        pbar.update(len(data))
                    else:
                        tinfo = _make_tar_info_from_meta(arcname, fsize, f_mtime_ns, f_mode, False)
                        with open(fpath, "rb") as src:
                            tf.addfile(tinfo, fileobj=_ProgressFileReader(src, pbar))

                _write_entries(_write_tar_entry)

        os.replace(tmp_path, archive_path)
        st_arc = os.stat(archive_path)
        atomic_write_json(
            meta_path,
            {
                "version": 1,
                "source_path": abs_src,
                "archive_format": archive_format,
                "arc_root_name": root_name,
                "source_file_count": src_count,
                "source_total_bytes": src_bytes,
                "source_max_mtime_ns": src_max_mtime,
                "archive_size": st_arc.st_size,
                "archive_mtime_ns": st_arc.st_mtime_ns,
            },
        )
        dt = max(time.monotonic() - t0, 1e-6)
        avg_mib_s = (st_arc.st_size / (1024 * 1024)) / dt
        print(
            f"📦 打包完成: {archive_name} ({format_bytes(st_arc.st_size)}, "
            f"耗时 {dt:.1f}s, 均速 {avg_mib_s:.2f} MiB/s)"
        )
        return archive_path, st_arc.st_size, False
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
    finally:
        pbar.close()


def _cleanup_archive_files(archive_path: str) -> None:
    """Remove temporary archive file and its pack metadata file."""
    for candidate in (archive_path, f"{archive_path}.pack_meta.json"):
        if os.path.exists(candidate):
            try:
                os.remove(candidate)
            except OSError:
                pass


def run_speedtest(
    client: JboxClient,
    remote_dir: str,
    work_dir: str = DEFAULT_WORK_DIR,
    workers: int = 1,
) -> float:
    """Upload a 10 MiB temporary random file to measure upload throughput, then clean up."""
    size_bytes = 10 * 1024 * 1024
    os.makedirs(work_dir, exist_ok=True)
    local_tmp = os.path.join(work_dir, f".jbox_speedtest_{os.getpid()}.bin")
    remote_tmp = join_remote_path(remote_dir, f".jbox_speedtest_{os.getpid()}_{int(time.time())}.bin")
    print("⏱️ 正在进行 10 MiB 上传测速...")
    try:
        with open(local_tmp, "wb") as f:
            f.write(os.urandom(size_bytes))
        uploader = ResumableUploader(
            client,
            work_dir=work_dir,
            chunk_mb=4,
            workers=workers,
            conflict="overwrite",
            force_upload=True,
        )
        t0 = time.monotonic()
        uploader.upload_file(local_tmp, remote_tmp, label="10MiB 测速")
        dt = max(time.monotonic() - t0, 1e-6)
        mbps = 10.0 / dt
        print(f"✅ 10 MiB 测速完成：耗时 {dt:.2f}s，速率 {mbps:.2f} MiB/s")
        return mbps
    finally:
        if os.path.exists(local_tmp):
            try:
                os.remove(local_tmp)
            except OSError:
                pass
        try:
            client.delete_file(remote_tmp)
        except Exception:
            pass


def _plan_transfer(
    abs_src: str,
    norm_remote_dir: str,
    pack_mode: str,
    archive_format: str,
    scan_workers: int = DEFAULT_PACK_WORKERS,
) -> dict[str, Any]:
    """Compute upload plan and estimated total bytes in a single tree pass."""
    base_name = os.path.basename(abs_src.rstrip("/"))
    if os.path.isfile(abs_src):
        st = os.stat(abs_src)
        remote_target = join_remote_path(norm_remote_dir, base_name)
        return {
            "is_file": True,
            "base_name": base_name,
            "total_bytes": st.st_size,
            "total_files": 1,
            "items": [
                {
                    "kind": "file",
                    "local_path": abs_src,
                    "remote_path": remote_target,
                    "est_bytes": st.st_size,
                    "file_count": 1,
                }
            ],
        }

    if pack_mode == "single":
        tree = scan_tree(abs_src, scan_workers=scan_workers)
        archive_name = f"{base_name}.{archive_format}"
        return {
            "is_file": False,
            "base_name": base_name,
            "total_bytes": tree["total_bytes"],
            "total_files": tree["file_count"],
            "items": [
                {
                    "kind": "archive_dir",
                    "local_dir": abs_src,
                    "archive_base": base_name,
                    "arc_root_name": base_name,
                    "remote_path": join_remote_path(norm_remote_dir, archive_name),
                    "est_bytes": tree["total_bytes"],
                    "file_count": tree["file_count"],
                    "tree": tree,
                }
            ],
        }

    target_root = join_remote_path(norm_remote_dir, base_name)
    if pack_mode == "subdir":
        entries = sorted(os.listdir(abs_src))
        items: list[dict[str, Any]] = []
        total_bytes = 0
        total_files = 0
        for entry in entries:
            epath = os.path.join(abs_src, entry)
            if os.path.islink(epath):
                continue
            if os.path.isdir(epath):
                subtree = scan_tree(epath, scan_workers=scan_workers)
                items.append(
                    {
                        "kind": "archive_dir",
                        "local_dir": epath,
                        "archive_base": entry,
                        "arc_root_name": entry,
                        "remote_path": join_remote_path(target_root, f"{entry}.{archive_format}"),
                        "est_bytes": subtree["total_bytes"],
                        "file_count": subtree["file_count"],
                        "tree": subtree,
                    }
                )
                total_bytes += subtree["total_bytes"]
                total_files += subtree["file_count"]
            elif os.path.isfile(epath):
                st = os.stat(epath)
                items.append(
                    {
                        "kind": "file",
                        "local_path": epath,
                        "remote_path": join_remote_path(target_root, entry),
                        "est_bytes": st.st_size,
                        "file_count": 1,
                    }
                )
                total_bytes += st.st_size
                total_files += 1
        return {
            "is_file": False,
            "base_name": base_name,
            "target_root": target_root,
            "total_bytes": total_bytes,
            "total_files": total_files,
            "items": items,
        }

    # pack_mode == "direct"
    tree = scan_tree(abs_src, scan_workers=scan_workers)
    items = [
        {
            "kind": "file",
            "local_path": fpath,
            "remote_path": join_remote_path(target_root, f_rel),
            "est_bytes": fsize,
            "file_count": 1,
        }
        for fpath, f_rel, fsize, _, _ in tree["files"]
    ]
    remote_dirs = [target_root] + [
        join_remote_path(target_root, d_rel) for _, d_rel, _, _ in tree["dirs"]
    ]
    return {
        "is_file": False,
        "base_name": base_name,
        "target_root": target_root,
        "remote_dirs": remote_dirs,
        "total_bytes": tree["total_bytes"],
        "total_files": tree["file_count"],
        "items": items,
    }


def _resolve_credential(auth_mode: str, credential: Optional[str]) -> str:
    """Resolve credential from argument, environment variable, or interactive prompt."""
    prefixes = ("UserToken=",) if auth_mode == "UserToken" else ("JAAuthCookie=",)
    if credential is not None:
        cleaned = clean_credential(credential, prefixes=prefixes)
        if cleaned:
            return cleaned

    env_key = "JBOX_USER_TOKEN" if auth_mode == "UserToken" else "JBOX_JA_COOKIE"
    env_val = clean_credential(os.environ.get(env_key, ""), prefixes=prefixes)
    if env_val:
        return env_val

    entered = getpass.getpass(f"🔑 粘贴 {auth_mode} 的 Value 后回车: ")
    cleaned = clean_credential(entered, prefixes=prefixes)
    if not cleaned:
        raise JboxAuthError(f"{auth_mode} 不能为空")
    return cleaned


def transfer(
    src_path: str,
    remote_dir: str = "/colab_transfer",
    pack_mode: str = "single",
    archive_format: str = "zip",
    auth_mode: str = "UserToken",
    credential: Optional[str] = None,
    workers: int = 4,
    chunk_mb: int = DEFAULT_CHUNK_MB,
    conflict: str = "overwrite",
    work_dir: str = DEFAULT_WORK_DIR,
    force_repack: bool = False,
    force_upload: bool = False,
    keep_temp: bool = False,
    speedtest: bool = False,
    dry_run: bool = False,
    base_url: str = DEFAULT_BASE_URL,
    cos_scheme: str = "https",
    chunk_bytes: Optional[int] = None,
    pack_workers: int = DEFAULT_PACK_WORKERS,
    prefetch_mb: int = DEFAULT_PREFETCH_MB,
    pipeline: bool = True,
    prefetch_max_file_bytes: int = PREFETCH_MAX_FILE_BYTES,
) -> dict[str, Any]:
    """Package and upload a Google Drive / local path to SJTU Jbox with parallel prefetch & resume."""
    if not src_path or not os.path.exists(src_path):
        raise FileNotFoundError(f"找不到源路径: {src_path}")
    if pack_mode not in {"single", "subdir", "direct"}:
        raise ValueError(f"不支持的 pack_mode: {pack_mode!r}（可选 single / subdir / direct）")
    if archive_format not in {"zip", "tar"}:
        raise ValueError(f"不支持的 archive_format: {archive_format!r}（可选 zip / tar）")
    if conflict not in {"overwrite", "rename", "ask"}:
        raise ValueError(f"不支持的 conflict 策略: {conflict!r}（可选 overwrite / rename / ask）")
    if int(workers) < 1 or int(chunk_mb) < 1 or int(pack_workers) < 1 or int(prefetch_mb) < 1:
        raise ValueError("workers, chunk_mb, pack_workers 与 prefetch_mb 必须 >= 1")

    t_start = time.monotonic()
    norm_auth = normalize_auth_mode(auth_mode)
    abs_src = os.path.abspath(src_path)
    norm_remote_dir = normalize_remote_path(remote_dir)

    t_scan = time.monotonic()
    plan = _plan_transfer(
        abs_src, norm_remote_dir, pack_mode, archive_format, scan_workers=int(pack_workers)
    )
    scan_dt = time.monotonic() - t_scan

    print(
        f"📋 传输计划：源={abs_src} | 模式={pack_mode} | 格式={archive_format} | "
        f"目标={norm_remote_dir} | 原始文件={plan['total_files']} 个 ({format_bytes(plan['total_bytes'])}) "
        f"[扫描 {scan_dt:.2f}s]"
    )

    if dry_run:
        print(f"🔎 Dry-run 预览（共 {len(plan['items'])} 个目标上传项）：")
        for idx, item in enumerate(plan["items"], 1):
            print(
                f"   [{idx}/{len(plan['items'])}] {item['remote_path']} "
                f"(预计 {format_bytes(item['est_bytes'])}, 含 {item['file_count']} 个源文件)"
            )
        print("ℹ️ Dry-run 模式：未校验登录态，未执行实际打包或上传。")
        return {
            "dry_run": True,
            "total_bytes": plan["total_bytes"],
            "total_files": plan["total_files"],
            "items": plan["items"],
        }

    cred_val = _resolve_credential(norm_auth, credential)
    user_token = (
        login_with_ja_cookie(cred_val, base_url=base_url)
        if norm_auth == "JAAuthCookie"
        else cred_val
    )
    client = JboxClient(user_token, base_url=base_url, cos_scheme=cos_scheme)

    print("🔗 正在验证交大云盘登录与空间配额...")
    client.get_space_cred()
    quota = client.get_quota()
    avail = quota.get("availableSpace")
    if avail is not None:
        print(f"✅ 登录成功！交大云盘剩余可用空间: {format_bytes(avail)}")
        if avail < plan["total_bytes"]:
            raise JboxTransferError(
                f"交大云盘剩余空间不足: 可用 {format_bytes(avail)} < 预计需要 {format_bytes(plan['total_bytes'])}"
            )
    else:
        print("✅ 登录成功！")

    client.ensure_dir(norm_remote_dir)
    if plan.get("target_root"):
        client.ensure_dir(plan["target_root"])
    for rdir in plan.get("remote_dirs") or []:
        client.ensure_dir(rdir)

    if speedtest:
        run_speedtest(client, norm_remote_dir, work_dir=work_dir, workers=1)

    uploader = ResumableUploader(
        client=client,
        work_dir=work_dir,
        chunk_mb=int(chunk_mb),
        workers=int(workers),
        conflict=conflict,
        force_upload=force_upload,
        chunk_bytes=chunk_bytes,
    )

    results: list[dict[str, Any]] = []
    uploaded_count = 0
    skipped_count = 0
    transferred_bytes = 0
    archive_root = (
        os.path.join(work_dir, "archives", plan["base_name"])
        if pack_mode == "subdir"
        else os.path.join(work_dir, "archives")
    )

    archive_item_count = sum(1 for item in plan["items"] if item["kind"] == "archive_dir")
    use_pipeline = pack_mode == "subdir" and bool(pipeline) and archive_item_count >= 2
    pack_pool = ThreadPoolExecutor(max_workers=1) if use_pipeline else None
    bg_futures: dict[int, Future[tuple[str, int, bool]]] = {}

    def _schedule_next_subdir_pack(after_idx: int) -> None:
        if pack_pool is None or bg_futures:
            return
        for j in range(after_idx + 1, len(plan["items"])):
            nxt = plan["items"][j]
            if nxt["kind"] != "archive_dir":
                continue
            nxt_name = f"{nxt['archive_base']}.{archive_format}"
            print(f"🔄 [后台预打包] 启动下一项: {nxt_name}")
            bg_futures[j] = pack_pool.submit(
                build_or_reuse_archive,
                src_dir=nxt["local_dir"],
                archive_base_name=nxt["archive_base"],
                archive_dir=archive_root,
                archive_format=archive_format,
                force_repack=force_repack,
                arc_root_name=nxt["arc_root_name"],
                pre_scanned_tree=nxt.get("tree"),
                pack_workers=int(pack_workers),
                prefetch_mb=int(prefetch_mb),
                prefetch_max_file_bytes=int(prefetch_max_file_bytes),
                show_progress=False,
            )
            return

    try:
        for idx, item in enumerate(plan["items"]):
            print(f"\n--- [{idx + 1}/{len(plan['items'])}] 目标: {item['remote_path']} ---")
            if item["kind"] == "archive_dir":
                if idx in bg_futures:
                    fut = bg_futures.pop(idx)
                    if not fut.done():
                        print(f"⏳ 等待后台预打包完成: {os.path.basename(item['remote_path'])}")
                    arc_path, _, _ = fut.result()
                else:
                    arc_path, _, _ = build_or_reuse_archive(
                        src_dir=item["local_dir"],
                        archive_base_name=item["archive_base"],
                        archive_dir=archive_root,
                        archive_format=archive_format,
                        force_repack=force_repack,
                        arc_root_name=item["arc_root_name"],
                        pre_scanned_tree=item.get("tree"),
                        pack_workers=int(pack_workers),
                        prefetch_mb=int(prefetch_mb),
                        prefetch_max_file_bytes=int(prefetch_max_file_bytes),
                        show_progress=True,
                    )

                _schedule_next_subdir_pack(idx)
                res = uploader.upload_file(
                    arc_path, item["remote_path"], label=os.path.basename(item["remote_path"])
                )
                if not keep_temp:
                    _cleanup_archive_files(arc_path)
            else:
                _schedule_next_subdir_pack(idx)
                res = uploader.upload_file(
                    item["local_path"],
                    item["remote_path"],
                    label=os.path.basename(item["remote_path"]),
                )

            results.append(res)
            transferred_bytes += res["size"]
            if res["skipped"]:
                skipped_count += 1
            else:
                uploaded_count += 1
    finally:
        if pack_pool is not None:
            for fut in bg_futures.values():
                fut.cancel()
            pack_pool.shutdown(wait=True, cancel_futures=True)

    total_dt = time.monotonic() - t_start
    print(
        f"\n🎉 全部处理完成！新传 {uploaded_count} 项，跳过 {skipped_count} 项，"
        f"合计 {format_bytes(transferred_bytes)} -> 交大云盘 {norm_remote_dir} (总耗时 {total_dt:.1f}s)"
    )
    return {
        "dry_run": False,
        "uploaded_files": uploaded_count,
        "skipped_files": skipped_count,
        "total_bytes": transferred_bytes,
        "results": results,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    """Create CLI argument parser for gdrive_to_jbox_v2."""
    parser = argparse.ArgumentParser(
        description="Google Drive (Colab mount) -> SJTU Jbox (pan.sjtu.edu.cn) 并发预读打包 + 原生分片断点续传工具 (v2)"
    )
    parser.add_argument("--src", required=True, help="本地或 Google Drive 源路径（文件或目录）")
    parser.add_argument("--remote-dir", default="/colab_transfer", help="交大云盘目标目录 (默认: /colab_transfer)")
    parser.add_argument(
        "--pack-mode",
        choices=("single", "subdir", "direct"),
        default="single",
        help="打包模式: single=整包单文件, subdir=按一级子目录独立打包, direct=直接逐文件上传",
    )
    parser.add_argument(
        "--archive-format",
        choices=("zip", "tar"),
        default="zip",
        help="无压缩归档格式 (默认: zip)",
    )
    parser.add_argument(
        "--auth-mode",
        choices=("UserToken", "JAAuthCookie"),
        default="UserToken",
        help="认证方式 (默认: UserToken)",
    )
    parser.add_argument("--token", default=None, help="直接传入 UserToken（也可通过 JBOX_USER_TOKEN 环境变量）")
    parser.add_argument("--ja-cookie", default=None, help="直接传入 JAAuthCookie（也可通过 JBOX_JA_COOKIE 环境变量）")
    parser.add_argument("--workers", type=int, default=4, help="并发上传分片线程数 (默认: 4)")
    parser.add_argument("--chunk-mb", type=int, default=DEFAULT_CHUNK_MB, help="分片大小 MiB (默认: 4)")
    parser.add_argument(
        "--pack-workers",
        type=int,
        default=DEFAULT_PACK_WORKERS,
        help=f"打包阶段并发从 Drive 预读文件的线程数 (默认: {DEFAULT_PACK_WORKERS})",
    )
    parser.add_argument(
        "--prefetch-mb",
        type=int,
        default=DEFAULT_PREFETCH_MB,
        help=f"打包预读内存缓冲上限 MiB (默认: {DEFAULT_PREFETCH_MB})",
    )
    parser.add_argument(
        "--no-pipeline",
        action="store_true",
        help="关闭 subdir 模式下「边上传边后台打包下一子目录」流水线",
    )
    parser.add_argument(
        "--conflict",
        choices=("overwrite", "rename", "ask"),
        default="overwrite",
        help="远端同名冲突策略 (默认: overwrite)",
    )
    parser.add_argument("--work-dir", default=DEFAULT_WORK_DIR, help=f"本地临时与断点目录 (默认: {DEFAULT_WORK_DIR})")
    parser.add_argument("--force-repack", action="store_true", help="忽略本地已有归档，强制重新打包")
    parser.add_argument("--force-upload", action="store_true", help="忽略远端同名同大小跳过检查，强制重传")
    parser.add_argument("--keep-temp", action="store_true", help="上传完成后保留本地临时归档")
    parser.add_argument("--speedtest", action="store_true", help="正式上传前先进行 10 MiB 测速")
    parser.add_argument("--dry-run", action="store_true", help="仅预览传输计划与体积，不要求凭证也不上传")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    auth_mode = args.auth_mode
    credential = args.token if auth_mode == "UserToken" else args.ja_cookie
    if args.ja_cookie and not args.token and args.auth_mode == "UserToken":
        auth_mode = "JAAuthCookie"
        credential = args.ja_cookie

    try:
        transfer(
            src_path=args.src,
            remote_dir=args.remote_dir,
            pack_mode=args.pack_mode,
            archive_format=args.archive_format,
            auth_mode=auth_mode,
            credential=credential,
            workers=args.workers,
            chunk_mb=args.chunk_mb,
            conflict=args.conflict,
            work_dir=args.work_dir,
            force_repack=args.force_repack,
            force_upload=args.force_upload,
            keep_temp=args.keep_temp,
            speedtest=args.speedtest,
            dry_run=args.dry_run,
            pack_workers=args.pack_workers,
            prefetch_mb=args.prefetch_mb,
            pipeline=not args.no_pipeline,
        )
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"❌ 错误: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
