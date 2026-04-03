from __future__ import annotations

import asyncio
import base64
import json
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp
from auth import MsTokenManager
from utils.cookie_utils import sanitize_cookies
from utils.logger import setup_logger
from utils.xbogus import XBogus

try:
    from utils.abogus import ABogus, BrowserFingerprintGenerator
except Exception:  # pragma: no cover - optional dependency
    ABogus = None
    BrowserFingerprintGenerator = None

logger = setup_logger("APIClient")

_USER_AGENT_POOL = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) "
        "Gecko/20100101 Firefox/133.0"
    ),
]


class DouyinAPIClient:
    BASE_URL = "https://www.douyin.com"
    _BROWSER_COOKIE_BLOCKLIST = {
        "sessionid",
        "sessionid_ss",
        "sid_tt",
        "sid_guard",
        "uid_tt",
        "uid_tt_ss",
        "passport_auth_status",
        "passport_auth_status_ss",
        "passport_assist_user",
        "passport_auth_mix_state",
        "passport_mfa_token",
        "login_time",
    }

    def __init__(self, cookies: Dict[str, str], proxy: Optional[str] = None):
        self.cookies = sanitize_cookies(cookies or {})
        self.proxy = str(proxy or "").strip()
        self._session: Optional[aiohttp.ClientSession] = None
        self._browser_post_aweme_items: Dict[str, Dict[str, Any]] = {}
        self._browser_post_stats: Dict[str, int] = {}
        self._browser_like_aweme_items: Dict[str, Dict[str, Any]] = {}
        self._browser_like_stats: Dict[str, int] = {}
        selected_ua = random.choice(_USER_AGENT_POOL)
        self.headers = {
            "User-Agent": selected_ua,
            "Referer": "https://www.douyin.com/",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }
        self._signer = XBogus(self.headers["User-Agent"])
        self._ms_token_manager = MsTokenManager(user_agent=self.headers["User-Agent"])
        self._ms_token = (self.cookies.get("msToken") or "").strip()
        self._abogus_enabled = (
            ABogus is not None and BrowserFingerprintGenerator is not None
        )

    async def __aenter__(self) -> "DouyinAPIClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=self.headers,
                cookies=self.cookies,
                timeout=aiohttp.ClientTimeout(total=30),
                raise_for_status=False,
            )

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_session(self) -> aiohttp.ClientSession:
        await self._ensure_session()
        if self._session is None:
            raise RuntimeError("Failed to create aiohttp session")
        return self._session

    async def _ensure_ms_token(self) -> str:
        if self._ms_token:
            return self._ms_token

        token = await asyncio.to_thread(
            self._ms_token_manager.ensure_ms_token,
            self.cookies,
        )
        self._ms_token = token.strip()
        if self._ms_token:
            self.cookies["msToken"] = self._ms_token
            if self._session and not self._session.closed:
                self._session.cookie_jar.update_cookies({"msToken": self._ms_token})
        return self._ms_token

    async def _default_query(self) -> Dict[str, Any]:
        ms_token = await self._ensure_ms_token()
        return {
            "device_platform": "webapp",
            "aid": "6383",
            "channel": "channel_pc_web",
            "pc_client_type": "1",
            "version_code": "170400",
            "version_name": "17.4.0",
            "cookie_enabled": "true",
            "screen_width": "1920",
            "screen_height": "1080",
            "browser_language": "zh-CN",
            "browser_platform": "Win32",
            "browser_name": "Chrome",
            "browser_version": "123.0.0.0",
            "browser_online": "true",
            "engine_name": "Blink",
            "engine_version": "123.0.0.0",
            "os_name": "Windows",
            "os_version": "10",
            "cpu_core_num": "8",
            "device_memory": "8",
            "platform": "PC",
            "downlink": "10",
            "effective_type": "4g",
            "round_trip_time": "50",
            "msToken": ms_token,
        }

    def sign_url(self, url: str) -> Tuple[str, str]:
        signed_url, _xbogus, ua = self._signer.build(url)
        return signed_url, ua

    def build_signed_path(self, path: str, params: Dict[str, Any]) -> Tuple[str, str]:
        query = urlencode(params)
        base_url = f"{self.BASE_URL}{path}"
        ab_signed = self._build_abogus_url(base_url, query)
        if ab_signed:
            return ab_signed
        return self.sign_url(f"{base_url}?{query}")

    def _build_abogus_url(self, base_url: str, query: str) -> Optional[Tuple[str, str]]:
        if not self._abogus_enabled:
            return None

        try:
            browser_fp = BrowserFingerprintGenerator.generate_fingerprint("Edge")
            signer = ABogus(fp=browser_fp, user_agent=self.headers["User-Agent"])
            params_with_ab, _ab, ua, _body = signer.generate_abogus(query, "")
            return f"{base_url}?{params_with_ab}", ua
        except Exception as exc:
            logger.warning("Failed to generate a_bogus, fallback to X-Bogus: %s", exc)
            return None

    async def _request_json(
        self,
        path: str,
        params: Dict[str, Any],
        *,
        suppress_error: bool = False,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        await self._ensure_session()
        delays = [1, 2, 5]
        last_exc: Optional[Exception] = None

        for attempt in range(max_retries):
            signed_url, ua = self.build_signed_path(path, params)
            try:
                async with self._session.get(
                    signed_url,
                    headers={**self.headers, "User-Agent": ua},
                    proxy=self.proxy or None,
                ) as response:
                    if response.status == 200:
                        data = await response.json(content_type=None)
                        return data if isinstance(data, dict) else {}
                    if response.status < 500 and response.status != 429:
                        log_fn = logger.debug if suppress_error else logger.error
                        log_fn(
                            "Request failed: path=%s, status=%s",
                            path,
                            response.status,
                        )
                        return {}
                    last_exc = RuntimeError(
                        f"HTTP {response.status} for {path}"
                    )
            except Exception as exc:
                last_exc = exc

            if attempt < max_retries - 1:
                delay = delays[min(attempt, len(delays) - 1)]
                logger.debug(
                    "Request retry %d/%d for %s in %ds",
                    attempt + 1, max_retries, path, delay,
                )
                await asyncio.sleep(delay)

        log_fn = logger.debug if suppress_error else logger.error
        log_fn("Request failed after %d attempts: path=%s, error=%s", max_retries, path, last_exc)
        return {}

    @staticmethod
    def _normalize_paged_response(
        raw_data: Any,
        *,
        item_keys: Optional[List[str]] = None,
        source: str = "api",
    ) -> Dict[str, Any]:
        raw = raw_data if isinstance(raw_data, dict) else {}
        keys = item_keys or []
        keys = ["items", *keys, "aweme_list", "mix_list", "music_list"]

        items: List[Dict[str, Any]] = []
        for key in keys:
            value = raw.get(key)
            if isinstance(value, list):
                items = value
                break

        has_more_value = raw.get("has_more", False)
        try:
            has_more = bool(int(has_more_value))
        except (TypeError, ValueError):
            has_more = bool(has_more_value)

        max_cursor_value = raw.get("max_cursor")
        if max_cursor_value is None:
            max_cursor_value = raw.get("cursor", 0)
        try:
            max_cursor = int(max_cursor_value or 0)
        except (TypeError, ValueError):
            max_cursor = 0

        status_code_value = raw.get("status_code", 0)
        try:
            status_code = int(status_code_value or 0)
        except (TypeError, ValueError):
            status_code = 0

        risk_flags = {
            "login_tip": bool(
                ((raw.get("not_login_module") or {}).get("guide_login_tip_exist"))
                if isinstance(raw.get("not_login_module"), dict)
                else False
            ),
            "verify_page": bool(raw.get("verify_ticket")),
        }

        normalized = {
            "items": items,
            "aweme_list": items,  # 兼容旧调用方
            "has_more": has_more,
            "max_cursor": max_cursor,
            "status_code": status_code,
            "source": source,
            "risk_flags": risk_flags,
            "raw": raw,
        }
        for key, value in raw.items():
            if key not in normalized:
                normalized[key] = value
        return normalized

    async def _build_user_page_params(
        self, sec_uid: str, max_cursor: int, count: int
    ) -> Dict[str, Any]:
        params = await self._default_query()
        params.update(
            {
                "sec_user_id": sec_uid,
                "max_cursor": max_cursor,
                "count": count,
                "locate_query": "false",
            }
        )
        return params

    async def get_video_detail(
        self, aweme_id: str, *, suppress_error: bool = False
    ) -> Optional[Dict[str, Any]]:
        params = await self._default_query()
        params.update(
            {
                "aweme_id": aweme_id,
                "aid": "1128",
            }
        )

        data = await self._request_json(
            "/aweme/v1/web/aweme/detail/",
            params,
            suppress_error=suppress_error,
        )
        if data:
            return data.get("aweme_detail")
        return None

    async def get_user_post(
        self, sec_uid: str, max_cursor: int = 0, count: int = 20
    ) -> Dict[str, Any]:
        params = await self._build_user_page_params(sec_uid, max_cursor, count)
        params.update(
            {
                "show_live_replay_strategy": "1",
                "need_time_list": "1",
                "time_list_query": "0",
                "whale_cut_token": "",
                "cut_version": "1",
                "publish_video_strategy_type": "2",
            }
        )
        raw = await self._request_json("/aweme/v1/web/aweme/post/", params)
        return self._normalize_paged_response(raw, item_keys=["aweme_list"])

    async def get_user_like(
        self, sec_uid: str, max_cursor: int = 0, count: int = 20
    ) -> Dict[str, Any]:
        params = await self._build_user_page_params(sec_uid, max_cursor, count)
        raw = await self._request_json("/aweme/v1/web/aweme/favorite/", params)
        return self._normalize_paged_response(raw, item_keys=["aweme_list"])

    async def get_user_mix(
        self, sec_uid: str, max_cursor: int = 0, count: int = 20
    ) -> Dict[str, Any]:
        params = await self._build_user_page_params(sec_uid, max_cursor, count)
        raw = await self._request_json("/aweme/v1/web/mix/list/", params)
        return self._normalize_paged_response(raw, item_keys=["mix_list"])

    async def get_user_music(
        self, sec_uid: str, max_cursor: int = 0, count: int = 20
    ) -> Dict[str, Any]:
        params = await self._build_user_page_params(sec_uid, max_cursor, count)
        raw = await self._request_json("/aweme/v1/web/music/list/", params)
        return self._normalize_paged_response(raw, item_keys=["music_list"])

    async def _build_collect_page_params(
        self, max_cursor: int, count: int
    ) -> Dict[str, Any]:
        params = await self._default_query()
        params.update(
            {
                "cursor": max_cursor,
                "count": count,
                "version_code": "170400",
                "version_name": "17.4.0",
            }
        )
        return params

    async def get_user_collects(
        self, sec_uid: str, max_cursor: int = 0, count: int = 10
    ) -> Dict[str, Any]:
        if sec_uid and sec_uid != "self":
            logger.warning("Collect folders currently require self sec_uid, got=%s", sec_uid)
            return self._normalize_paged_response(
                {}, item_keys=["collects_list"], source="api"
            )

        params = await self._build_collect_page_params(max_cursor, count)
        raw = await self._request_json("/aweme/v1/web/collects/list/", params)
        return self._normalize_paged_response(raw, item_keys=["collects_list"])

    async def get_collect_aweme(
        self, collects_id: str, max_cursor: int = 0, count: int = 10
    ) -> Dict[str, Any]:
        params = await self._build_collect_page_params(max_cursor, count)
        params.update({"collects_id": collects_id})
        raw = await self._request_json("/aweme/v1/web/collects/video/list/", params)
        return self._normalize_paged_response(raw, item_keys=["aweme_list"])

    async def get_user_collect_mix(
        self, sec_uid: str, max_cursor: int = 0, count: int = 12
    ) -> Dict[str, Any]:
        if sec_uid and sec_uid != "self":
            logger.warning("Collect mix currently require self sec_uid, got=%s", sec_uid)
            return self._normalize_paged_response(
                {}, item_keys=["mix_infos"], source="api"
            )

        params = await self._build_collect_page_params(max_cursor, count)
        raw = await self._request_json("/aweme/v1/web/mix/listcollection/", params)
        return self._normalize_paged_response(raw, item_keys=["mix_infos"])

    async def get_user_info(self, sec_uid: str) -> Optional[Dict[str, Any]]:
        params = await self._default_query()
        params.update({"sec_user_id": sec_uid})

        data = await self._request_json("/aweme/v1/web/user/profile/other/", params)
        if data:
            return data.get("user")
        return None

    async def get_mix_detail(self, mix_id: str) -> Optional[Dict[str, Any]]:
        params = await self._default_query()
        params.update({"mix_id": mix_id})
        data = await self._request_json("/aweme/v1/web/mix/detail/", params)
        if not data:
            return None
        return data.get("mix_info") or data.get("mix_detail") or data

    async def get_mix_aweme(
        self, mix_id: str, cursor: int = 0, count: int = 20
    ) -> Dict[str, Any]:
        params = await self._default_query()
        params.update({"mix_id": mix_id, "cursor": cursor, "count": count})
        raw = await self._request_json("/aweme/v1/web/mix/aweme/", params)
        return self._normalize_paged_response(raw, item_keys=["aweme_list"])

    async def get_music_detail(self, music_id: str) -> Optional[Dict[str, Any]]:
        params = await self._default_query()
        params.update({"music_id": music_id})
        data = await self._request_json("/aweme/v1/web/music/detail/", params)
        if not data:
            return None
        return data.get("music_info") or data.get("music_detail") or data

    async def get_music_aweme(
        self, music_id: str, cursor: int = 0, count: int = 20
    ) -> Dict[str, Any]:
        params = await self._default_query()
        params.update({"music_id": music_id, "cursor": cursor, "count": count})
        raw = await self._request_json("/aweme/v1/web/music/aweme/", params)
        return self._normalize_paged_response(raw, item_keys=["aweme_list"])

    async def resolve_short_url(self, short_url: str) -> Optional[str]:
        try:
            await self._ensure_session()
            async with self._session.get(
                short_url,
                allow_redirects=True,
                proxy=self.proxy or None,
            ) as response:
                return str(response.url)
        except Exception as e:
            logger.error("Failed to resolve short URL: %s, error: %s", short_url, e)
            return None

    async def collect_user_post_ids_via_browser(
        self,
        sec_uid: str,
        *,
        expected_count: int = 0,
        headless: bool = False,
        max_scrolls: int = 240,
        idle_rounds: int = 8,
        wait_timeout_seconds: int = 600,
    ) -> List[str]:
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            logger.warning(
                "Playwright not available, browser fallback disabled: %s", exc
            )
            return []

        target_url = f"{self.BASE_URL}/user/{sec_uid}"
        timeout_ms = max(30, int(wait_timeout_seconds)) * 1000
        ids: List[str] = []
        seen: set[str] = set()
        post_api_ids: List[str] = []
        post_api_seen: set[str] = set()
        post_api_aweme_items: Dict[str, Dict[str, Any]] = {}
        post_api_page_hits = 0
        self._browser_post_aweme_items = {}
        self._browser_post_stats = {}

        def _merge(new_ids: List[str]):
            for aweme_id in new_ids:
                if aweme_id and aweme_id not in seen:
                    seen.add(aweme_id)
                    ids.append(aweme_id)

        logger.warning(
            "API翻页受限，启动浏览器兜底采集（可在弹出页面手动通过验证码/登录）：%s",
            target_url,
        )

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context = await browser.new_context(
                user_agent=self.headers.get("User-Agent", ""),
                locale="zh-CN",
                viewport={"width": 1600, "height": 900},
            )
            cookies = self._browser_cookie_payload()
            if cookies:
                await context.add_cookies(cookies)

            page = await context.new_page()
            pending_response_tasks: List[asyncio.Task] = []

            async def _handle_response(response):
                nonlocal post_api_page_hits
                url = response.url or ""
                if "/aweme/v1/web/aweme/post/" not in url:
                    return
                try:
                    data = await response.json()
                except Exception:
                    return
                aweme_items = data.get("aweme_list") if isinstance(data, dict) else None
                if isinstance(aweme_items, list):
                    post_api_page_hits += 1
                    extracted: List[str] = []
                    for item in aweme_items:
                        if not isinstance(item, dict):
                            continue
                        aweme_id = item.get("aweme_id")
                        if not aweme_id:
                            continue
                        aweme_id_str = str(aweme_id)
                        extracted.append(aweme_id_str)
                        if aweme_id_str not in post_api_aweme_items:
                            post_api_aweme_items[aweme_id_str] = item
                    _merge(extracted)
                    for aweme_id in extracted:
                        if aweme_id not in post_api_seen:
                            post_api_seen.add(aweme_id)
                            post_api_ids.append(aweme_id)

            def _on_response(response):
                pending_response_tasks.append(
                    asyncio.create_task(_handle_response(response))
                )

            page.on("response", _on_response)

            try:
                try:
                    await page.goto(
                        target_url, wait_until="domcontentloaded", timeout=timeout_ms
                    )
                except Exception as exc:
                    logger.warning(
                        "Browser goto timeout or error, continue with current page state: %s",
                        exc,
                    )

                title = ""
                try:
                    title = await page.title()
                except Exception:
                    pass
                if "验证码" in title:
                    if callable(progress_callback):
                        try:
                            progress_callback(
                                {
                                    "event": "verification_required",
                                    "message": "verification_required",
                                }
                            )
                        except Exception as exc:
                            logger.debug("Like cleanup progress callback failed: %s", exc)
                    if headless:
                        logger.warning(
                            "检测到验证码页面且当前为 headless 模式，无法人工验证。"
                            "请将 browser_fallback.headless 设为 false。"
                        )
                        return []
                    logger.warning(
                        "检测到验证码页面，请在浏览器中完成验证，程序会自动继续采集。"
                    )
                    await self._wait_for_manual_verification(
                        page, wait_timeout_seconds=wait_timeout_seconds
                    )
                    if not page.is_closed():
                        try:
                            await page.goto(
                                target_url,
                                wait_until="domcontentloaded",
                                timeout=timeout_ms,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Reload user page after verification failed: %s", exc
                            )

                try:
                    warmup_seconds = min(20, max(3, int(wait_timeout_seconds)))
                    for _ in range(warmup_seconds):
                        if page.is_closed():
                            logger.warning("Browser page closed during warmup")
                            break
                        _merge(await self._extract_aweme_ids_from_page(page))
                        if ids:
                            break
                        await page.wait_for_timeout(1000)

                    stable_rounds = 0
                    max_scroll_rounds = max(1, int(max_scrolls))
                    idle_stop_rounds = max(1, int(idle_rounds))

                    for _ in range(max_scroll_rounds):
                        if page.is_closed():
                            logger.warning("Browser page closed during scrolling")
                            break
                        before = len(ids)
                        await page.mouse.wheel(0, 3800)
                        await page.wait_for_timeout(1200)

                        _merge(await self._extract_aweme_ids_from_page(page))
                        if len(ids) == before:
                            stable_rounds += 1
                        else:
                            stable_rounds = 0

                        if expected_count > 0 and len(ids) >= expected_count:
                            break
                        if expected_count <= 0 and stable_rounds >= idle_stop_rounds:
                            break
                except Exception as exc:
                    logger.warning(
                        "Browser collection interrupted, use collected ids so far: %s",
                        exc,
                    )
            finally:
                if pending_response_tasks:
                    await asyncio.gather(
                        *pending_response_tasks, return_exceptions=True
                    )
                try:
                    browser_cookies = await context.cookies(self.BASE_URL)
                    self._sync_browser_cookies(browser_cookies)
                except Exception as exc:
                    logger.debug("Sync browser cookies skipped: %s", exc)
                await context.close()
                await browser.close()

        selected_ids: List[str] = []
        selected_seen: set[str] = set()
        for aweme_id in post_api_ids + ids:
            if aweme_id and aweme_id not in selected_seen:
                selected_seen.add(aweme_id)
                selected_ids.append(aweme_id)
        self._browser_post_aweme_items = post_api_aweme_items
        self._browser_post_stats = {
            "merged_ids": len(ids),
            "post_api_ids": len(post_api_ids),
            "selected_ids": len(selected_ids),
            "post_items": len(post_api_aweme_items),
            "post_pages": post_api_page_hits,
        }
        logger.warning(
            "浏览器兜底采集 aweme_id: merged=%s, from_post_api=%s, selected=%s, post_items=%s",
            len(ids),
            len(post_api_ids),
            len(selected_ids),
            len(post_api_aweme_items),
        )
        return selected_ids

    async def collect_user_like_ids_via_browser(
        self,
        sec_uid: str,
        *,
        expected_count: int = 0,
        headless: bool = False,
        max_scrolls: int = 240,
        idle_rounds: int = 8,
        wait_timeout_seconds: int = 600,
        profile_dir: Optional[str] = None,
        login_confirmation_callback=None,
    ) -> List[str]:
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            logger.warning("Playwright not available for like browser fallback: %s", exc)
            return []

        normalized_sec_uid = str(sec_uid or "").strip() or "self"
        target_user = "self" if normalized_sec_uid == "self" else normalized_sec_uid
        target_url = f"{self.BASE_URL}/user/{target_user}?showTab=like"
        timeout_ms = max(30, int(wait_timeout_seconds)) * 1000
        ids: List[str] = []
        seen: set[str] = set()
        like_api_ids: List[str] = []
        like_api_seen: set[str] = set()
        like_api_aweme_items: Dict[str, Dict[str, Any]] = {}
        like_api_page_hits = 0
        self._browser_like_aweme_items = {}
        self._browser_like_stats = {}

        def _merge(values: List[str]) -> None:
            for aweme_id in values or []:
                aweme_id_str = str(aweme_id or "").strip()
                if aweme_id_str and aweme_id_str not in seen:
                    seen.add(aweme_id_str)
                    ids.append(aweme_id_str)

        logger.warning(
            "点赞页API受限，启动浏览器兜底采集（可在弹出页面手动通过验证码/登录）：%s",
            target_url,
        )

        async with async_playwright() as playwright:
            context, browser = await self._create_browser_context(
                playwright,
                headless=headless,
                include_sensitive_cookies=True,
                profile_dir=profile_dir,
            )
            pages = getattr(context, "pages", None)
            page = pages[0] if isinstance(pages, list) and pages else await context.new_page()
            pending_response_tasks: List[asyncio.Task] = []

            async def _handle_response(response) -> None:
                nonlocal like_api_page_hits
                url = response.url or ""
                if "/aweme/v1/web/aweme/favorite/" not in url:
                    return
                try:
                    data = await response.json()
                except Exception:
                    return
                aweme_items = data.get("aweme_list") if isinstance(data, dict) else None
                if not isinstance(aweme_items, list):
                    return

                like_api_page_hits += 1
                extracted: List[str] = []
                for item in aweme_items:
                    if not isinstance(item, dict):
                        continue
                    aweme_id = item.get("aweme_id")
                    if not aweme_id:
                        continue
                    aweme_id_str = str(aweme_id)
                    extracted.append(aweme_id_str)
                    if aweme_id_str not in like_api_aweme_items:
                        like_api_aweme_items[aweme_id_str] = item
                _merge(extracted)
                for aweme_id in extracted:
                    if aweme_id not in like_api_seen:
                        like_api_seen.add(aweme_id)
                        like_api_ids.append(aweme_id)

            def _on_response(response) -> None:
                pending_response_tasks.append(
                    asyncio.create_task(_handle_response(response))
                )

            page.on("response", _on_response)
            try:
                try:
                    await page.goto(
                        target_url, wait_until="domcontentloaded", timeout=timeout_ms
                    )
                except Exception as exc:
                    logger.warning(
                        "Browser goto timeout or error before like browser fallback: %s",
                        exc,
                    )

                title = ""
                try:
                    title = await page.title()
                except Exception:
                    pass
                if "验证码" in title:
                    if headless:
                        logger.warning(
                            "检测到验证码页面且当前为 headless 模式，无法人工验证。"
                            "请将 browser_fallback.headless 设为 false。"
                        )
                        return []
                    logger.warning(
                        "检测到验证码页面，请在浏览器中完成验证，程序会自动继续采集喜欢列表。"
                    )
                    await self._wait_for_manual_verification(
                        page, wait_timeout_seconds=wait_timeout_seconds
                    )
                    if not page.is_closed():
                        try:
                            await page.goto(
                                target_url,
                                wait_until="domcontentloaded",
                                timeout=timeout_ms,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Reload like page after verification failed: %s", exc
                            )

                if not headless and not await self._page_ready_for_like_actions(page):
                    logger.warning(
                        "未检测到完整登录态，如浏览器弹出登录页，请先完成登录后程序会继续采集喜欢列表。"
                    )
                    if callable(login_confirmation_callback):
                        try:
                            maybe_awaitable = login_confirmation_callback(
                                "请在弹出的浏览器中完成抖音登录，然后回到终端按 Enter 继续。"
                            )
                            if asyncio.iscoroutine(maybe_awaitable):
                                await maybe_awaitable
                        except Exception as exc:
                            logger.debug(
                                "Like browser fallback login confirmation callback failed: %s",
                                exc,
                            )
                    login_ready = await self._wait_for_manual_login(
                        page, wait_timeout_seconds=wait_timeout_seconds
                    )
                    if not login_ready:
                        logger.warning("手动登录后页面仍未进入已登录状态，停止喜欢列表回补。")
                        return []

                scroller = page.locator(
                    "div.parent-route-container.route-scroll-container, "
                    "div.parent-route-container.XoIW2IMs.route-scroll-container"
                ).first
                has_scroller = False
                try:
                    has_scroller = await scroller.count() > 0
                except Exception:
                    has_scroller = False

                try:
                    warmup_seconds = min(20, max(3, int(wait_timeout_seconds)))
                    for _ in range(warmup_seconds):
                        if page.is_closed():
                            logger.warning("Browser page closed during like fallback warmup")
                            break
                        if ids:
                            break
                        await page.wait_for_timeout(1000)

                    stable_rounds = 0
                    max_scroll_rounds = max(1, int(max_scrolls))
                    idle_stop_rounds = max(1, int(idle_rounds))

                    for _ in range(max_scroll_rounds):
                        if page.is_closed():
                            logger.warning("Browser page closed during like fallback scrolling")
                            break

                        before = len(ids)
                        if has_scroller:
                            try:
                                await scroller.evaluate("(el) => { el.scrollBy(0, 1600); }")
                            except Exception as exc:
                                logger.debug("Like list scroller scroll failed: %s", exc)
                                await page.mouse.wheel(0, 3200)
                        else:
                            await page.mouse.wheel(0, 3200)
                        await page.wait_for_timeout(1200)

                        if len(ids) == before:
                            stable_rounds += 1
                        else:
                            stable_rounds = 0

                        if expected_count > 0 and len(ids) >= expected_count:
                            break
                        if expected_count <= 0 and stable_rounds >= idle_stop_rounds:
                            break
                except Exception as exc:
                    logger.warning(
                        "Like browser collection interrupted, use collected ids so far: %s",
                        exc,
                    )
            finally:
                if pending_response_tasks:
                    await asyncio.gather(
                        *pending_response_tasks, return_exceptions=True
                    )
                try:
                    browser_cookies = await context.cookies(self.BASE_URL)
                    self._sync_browser_cookies(browser_cookies)
                except Exception as exc:
                    logger.debug("Sync browser cookies skipped: %s", exc)
                await context.close()
                if browser is not None:
                    await browser.close()

        selected_ids: List[str] = []
        selected_seen: set[str] = set()
        for aweme_id in like_api_ids:
            if aweme_id and aweme_id not in selected_seen:
                selected_seen.add(aweme_id)
                selected_ids.append(aweme_id)

        self._browser_like_aweme_items = like_api_aweme_items
        self._browser_like_stats = {
            "selected_ids": len(selected_ids),
            "like_items": len(like_api_aweme_items),
            "like_pages": like_api_page_hits,
        }
        logger.warning(
            "浏览器喜欢页回补采集 aweme_id: selected=%s, like_items=%s, like_pages=%s",
            len(selected_ids),
            len(like_api_aweme_items),
            like_api_page_hits,
        )
        return selected_ids

    async def cancel_likes_via_browser(
        self,
        aweme_ids: List[str],
        *,
        headless: bool = False,
        wait_timeout_seconds: int = 600,
        request_interval_ms: int = 1000,
        profile_dir: Optional[str] = None,
        progress_callback=None,
        login_confirmation_callback=None,
    ) -> Dict[str, Any]:
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            logger.warning(
                "Playwright not available, like cleanup disabled: %s", exc
            )
            normalized_ids = self._normalize_aweme_ids(aweme_ids)
            return {
                "requested": len(normalized_ids),
                "success_ids": [],
                "failed_ids": normalized_ids,
                "success_count": 0,
                "failed_count": len(normalized_ids),
            }

        normalized_ids = self._normalize_aweme_ids(aweme_ids)
        result = {
            "requested": len(normalized_ids),
            "success_ids": [],
            "failed_ids": [],
            "success_count": 0,
            "failed_count": 0,
        }
        if not normalized_ids:
            return result

        target_url = f"{self.BASE_URL}/user/self?showTab=like"
        timeout_ms = max(30, int(wait_timeout_seconds)) * 1000

        logger.warning(
            "启动浏览器取消点赞：count=%s, headless=%s",
            len(normalized_ids),
            headless,
        )

        async with async_playwright() as playwright:
            context, browser = await self._create_browser_context(
                playwright,
                headless=headless,
                include_sensitive_cookies=True,
                profile_dir=profile_dir,
            )
            pages = getattr(context, "pages", None)
            page = pages[0] if isinstance(pages, list) and pages else await context.new_page()
            try:
                try:
                    await page.goto(
                        target_url, wait_until="domcontentloaded", timeout=timeout_ms
                    )
                except Exception as exc:
                    logger.warning(
                        "Browser goto timeout or error before like cleanup: %s", exc
                    )

                title = ""
                try:
                    title = await page.title()
                except Exception:
                    pass
                if "验证码" in title:
                    if headless:
                        logger.warning(
                            "检测到验证码页面且当前为 headless 模式，无法人工验证。"
                            "请将 like_cleanup.headless 设为 false。"
                        )
                        result["failed_ids"] = normalized_ids
                        return self._finalize_like_cleanup_result(result)
                    logger.warning(
                        "检测到验证码页面，请在浏览器中完成验证，程序会自动继续取消点赞。"
                    )
                    await self._wait_for_manual_verification(
                        page, wait_timeout_seconds=wait_timeout_seconds
                    )
                    if not page.is_closed():
                        try:
                            await page.goto(
                                target_url,
                                wait_until="domcontentloaded",
                                timeout=timeout_ms,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Reload like page after verification failed: %s", exc
                            )

                if not headless and not await self._page_ready_for_like_actions(page):
                    if callable(progress_callback):
                        try:
                            progress_callback(
                                {
                                    "event": "login_state_missing",
                                    "message": "login_state_missing",
                                }
                            )
                        except Exception as exc:
                            logger.debug("Like cleanup progress callback failed: %s", exc)
                    logger.warning(
                        "未检测到完整登录态，如浏览器弹出登录页，请先完成登录后程序会继续。"
                    )
                    if callable(login_confirmation_callback):
                        try:
                            maybe_awaitable = login_confirmation_callback(
                                "请在弹出的浏览器中完成抖音登录，然后回到终端按 Enter 继续。"
                            )
                            if asyncio.iscoroutine(maybe_awaitable):
                                await maybe_awaitable
                        except Exception as exc:
                            logger.debug(
                                "Like cleanup login confirmation callback failed: %s",
                                exc,
                            )
                    login_ready = await self._wait_for_manual_login(
                        page, wait_timeout_seconds=wait_timeout_seconds
                    )
                    if not login_ready:
                        logger.warning(
                            "手动登录后页面仍未进入已登录状态，终止取消点赞。"
                        )
                        result["failed_ids"] = list(normalized_ids)
                        return self._finalize_like_cleanup_result(result)

                batch_size = 8
                processed_count = 0
                total_count = len(normalized_ids)
                for batch_start in range(0, total_count, batch_size):
                    batch_ids = normalized_ids[batch_start : batch_start + batch_size]
                    if page.is_closed():
                        logger.warning("Browser page closed during like cleanup")
                        result["failed_ids"].extend(normalized_ids[batch_start:])
                        break

                    try:
                        browser_cookies = await context.cookies(self.BASE_URL)
                        self._sync_browser_cookies(browser_cookies)
                    except Exception as exc:
                        logger.debug(
                            "Sync browser cookies before like cleanup request failed: %s",
                            exc,
                        )

                    batch_result = await self._cancel_like_batch_via_bulk_manage(
                        page, batch_ids
                    )
                    batch_response = batch_result.get("batch_response") or {}
                    status_code = self._as_int(
                        batch_response.get("status_code"), default=-1
                    )
                    needs_manual_reauth = status_code == 8 or self._as_int(
                        batch_response.get("http_status"), default=0
                    ) == 403
                    if needs_manual_reauth and not headless:
                        aweme_id = batch_ids[0] if batch_ids else ""
                        if callable(progress_callback):
                            try:
                                progress_callback(
                                    {
                                        "event": "login_required",
                                        "aweme_id": aweme_id,
                                        "message": "login_required",
                                    }
                                )
                            except Exception as exc:
                                logger.debug(
                                    "Like cleanup progress callback failed: %s", exc
                                )
                        logger.warning(
                            "批量取消点赞返回未登录或请求被拒绝，请在浏览器中完成登录/验证，程序会自动重试当前批次。"
                        )
                        login_ready = False
                        if callable(login_confirmation_callback):
                            try:
                                maybe_awaitable = login_confirmation_callback(
                                    f"当前批次取消点赞需要重新登录或验证。请先在浏览器完成操作，然后回到终端按 Enter 继续。"
                                )
                                if asyncio.iscoroutine(maybe_awaitable):
                                    await maybe_awaitable
                            except Exception as exc:
                                logger.debug(
                                    "Like cleanup login confirmation callback failed: %s",
                                    exc,
                                )
                        login_ready = await self._wait_for_manual_login(
                            page, wait_timeout_seconds=wait_timeout_seconds
                        )
                        if not login_ready:
                            logger.warning(
                                "手动登录后页面仍未进入已登录状态，停止后续取消点赞。"
                            )
                            result["failed_ids"].extend(normalized_ids[batch_start:])
                            break
                        if not page.is_closed():
                            try:
                                await page.goto(
                                    target_url,
                                    wait_until="domcontentloaded",
                                    timeout=timeout_ms,
                                )
                            except Exception as exc:
                                logger.warning(
                                    "Reload like page after manual login failed: %s",
                                    exc,
                                )
                            try:
                                browser_cookies = await context.cookies(self.BASE_URL)
                                self._sync_browser_cookies(browser_cookies)
                            except Exception as exc:
                                logger.debug(
                                    "Sync browser cookies after manual login failed: %s",
                                    exc,
                                )
                            batch_result = await self._cancel_like_batch_via_bulk_manage(
                                page, batch_ids
                            )

                    item_responses = batch_result.get("item_responses") or {}
                    for aweme_id in batch_ids:
                        processed_count += 1
                        response = item_responses.get(
                            aweme_id,
                            {
                                "http_status": 0,
                                "status_code": 5,
                                "status_msg": "bulk_unlike_unknown",
                                "body": "",
                            },
                        )
                        item_status_code = self._as_int(
                            response.get("status_code"), default=-1
                        )
                        if item_status_code == 0:
                            result["success_ids"].append(aweme_id)
                            progress_status = "success"
                        else:
                            result["failed_ids"].append(aweme_id)
                            progress_status = "failed"
                            logger.warning(
                                "取消点赞失败 aweme_id=%s, status_code=%s, status_msg=%s, http_status=%s",
                                aweme_id,
                                item_status_code,
                                response.get("status_msg") or "",
                                response.get("http_status"),
                            )

                        if callable(progress_callback):
                            try:
                                progress_callback(
                                    {
                                        "index": processed_count,
                                        "total": total_count,
                                        "aweme_id": aweme_id,
                                        "status": progress_status,
                                        "status_code": item_status_code,
                                        "status_msg": response.get("status_msg") or "",
                                    }
                                )
                            except Exception as exc:
                                logger.debug(
                                    "Like cleanup progress callback failed: %s", exc
                                )

                    if page.is_closed():
                        result["failed_ids"].extend(
                            normalized_ids[batch_start + len(batch_ids) :]
                        )
                        break

                    has_more_batches = batch_start + batch_size < total_count
                    if has_more_batches:
                        try:
                            await page.goto(
                                target_url,
                                wait_until="domcontentloaded",
                                timeout=timeout_ms,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Reload like page between batch operations failed: %s",
                                exc,
                            )

                    if has_more_batches and int(request_interval_ms or 0) > 0:
                        try:
                            await page.wait_for_timeout(int(request_interval_ms))
                        except Exception as exc:
                            logger.warning(
                                "Like cleanup interrupted while waiting next batch: %s",
                                exc,
                            )
                            result["failed_ids"].extend(
                                normalized_ids[batch_start + len(batch_ids) :]
                            )
                            break
            finally:
                try:
                    browser_cookies = await context.cookies(self.BASE_URL)
                    self._sync_browser_cookies(browser_cookies)
                except Exception as exc:
                    logger.debug("Sync browser cookies skipped: %s", exc)
                await context.close()
                if browser is not None:
                    await browser.close()

        return self._finalize_like_cleanup_result(result)

    def pop_browser_post_aweme_items(self) -> Dict[str, Dict[str, Any]]:
        items = self._browser_post_aweme_items
        self._browser_post_aweme_items = {}
        return items

    def pop_browser_post_stats(self) -> Dict[str, int]:
        stats = self._browser_post_stats
        self._browser_post_stats = {}
        return stats

    def pop_browser_like_aweme_items(self) -> Dict[str, Dict[str, Any]]:
        items = self._browser_like_aweme_items
        self._browser_like_aweme_items = {}
        return items

    def pop_browser_like_stats(self) -> Dict[str, int]:
        stats = self._browser_like_stats
        self._browser_like_stats = {}
        return stats

    def _browser_cookie_payload(
        self, *, include_sensitive: bool = False
    ) -> List[Dict[str, str]]:
        payload: List[Dict[str, str]] = []
        for name, value in self.cookies.items():
            if not name:
                continue
            if not include_sensitive and name in self._BROWSER_COOKIE_BLOCKLIST:
                continue
            payload.append(
                {
                    "name": str(name),
                    "value": str(value or ""),
                    "url": f"{self.BASE_URL}/",
                }
            )
        return payload

    async def _sync_context_cookies_from_client(
        self,
        context,
        *,
        include_sensitive: bool,
        overwrite_existing: bool,
    ) -> int:
        payload = self._browser_cookie_payload(include_sensitive=include_sensitive)
        if not payload:
            return 0

        existing: Dict[str, str] = {}
        try:
            current_cookies = await context.cookies(self.BASE_URL)
        except Exception:
            current_cookies = []

        for cookie in current_cookies or []:
            if not isinstance(cookie, dict):
                continue
            name = str(cookie.get("name") or "").strip()
            if not name:
                continue
            existing[name] = str(cookie.get("value") or "")

        pending: List[Dict[str, str]] = []
        for cookie in payload:
            name = str(cookie.get("name") or "").strip()
            value = str(cookie.get("value") or "")
            existing_value = existing.get(name)
            if overwrite_existing:
                if existing_value == value:
                    continue
            else:
                if existing_value:
                    continue
            pending.append(cookie)

        if not pending:
            return 0

        try:
            await context.add_cookies(pending)
        except Exception as exc:
            logger.debug("Sync browser cookies from config skipped: %s", exc)
            return 0

        logger.warning(
            "Synced %s cookie(s) from config into browser context%s",
            len(pending),
            " with overwrite" if overwrite_existing else "",
        )
        return len(pending)

    async def _create_browser_context(
        self,
        playwright,
        *,
        headless: bool,
        include_sensitive_cookies: bool,
        profile_dir: Optional[str] = None,
    ):
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ]
        base_context_kwargs = {
            "locale": "zh-CN",
            "viewport": {"width": 1600, "height": 900},
        }

        browser = None
        normalized_profile_dir = str(profile_dir or "").strip()
        if normalized_profile_dir:
            profile_path = Path(normalized_profile_dir).expanduser()
            profile_path.mkdir(parents=True, exist_ok=True)
            context = await playwright.chromium.launch_persistent_context(
                str(profile_path),
                headless=headless,
                args=launch_args,
                **base_context_kwargs,
            )
        else:
            context_kwargs = {
                **base_context_kwargs,
                "user_agent": self.headers.get("User-Agent", ""),
            }
            browser = await playwright.chromium.launch(
                headless=headless,
                args=launch_args,
            )
            context = await browser.new_context(**context_kwargs)

        # Persistent profiles can still miss non-persisted or recently refreshed
        # cookies such as msToken. Backfill only missing entries from config so
        # we preserve the profile's own login state while repairing gaps.
        if normalized_profile_dir:
            await self._sync_context_cookies_from_client(
                context,
                include_sensitive=include_sensitive_cookies,
                overwrite_existing=False,
            )
        else:
            cookies = self._browser_cookie_payload(
                include_sensitive=include_sensitive_cookies
            )
            if cookies:
                try:
                    await context.add_cookies(cookies)
                except Exception as exc:
                    logger.debug("Seed browser cookies skipped: %s", exc)

        return context, browser

    async def _extract_aweme_ids_from_page(
        self,
        page,
        *,
        root_selector: Optional[str] = None,
        include_page_html: bool = True,
    ) -> List[str]:
        script = """
({ rootSelector, includePageHtml }) => {
  const result = [];
  const seen = new Set();
  const push = (id) => {
    if (!id || seen.has(id)) return;
    seen.add(id);
    result.push(id);
  };

  const collectFrom = (text, pattern) => {
    if (!text) return;
    let match;
    while ((match = pattern.exec(text)) !== null) {
      push(match[1]);
    }
  };

  const root = rootSelector ? document.querySelector(rootSelector) : document;
  if (!root) {
    return result;
  }

  const links = root.querySelectorAll("a[href]");
  for (const node of links) {
    const href = node.getAttribute("href") || "";
    collectFrom(href, /\\/video\\/(\\d{15,20})/g);
    collectFrom(href, /\\/note\\/(\\d{15,20})/g);
  }

  if (includePageHtml) {
    const html = rootSelector
      ? (root.innerHTML || "")
      : (document.documentElement ? document.documentElement.innerHTML : "");
    collectFrom(html, /"aweme_id":"(\\d{15,20})"/g);
    collectFrom(html, /"group_id":"(\\d{15,20})"/g);
  }

  return result;
}
"""
        try:
            data = await page.evaluate(
                script,
                {
                    "rootSelector": str(root_selector or "").strip(),
                    "includePageHtml": bool(include_page_html),
                },
            )
            if isinstance(data, list):
                return [str(x) for x in data if x]
        except Exception as exc:
            logger.debug("Extract aweme_id from page failed: %s", exc)
        return []

    async def _wait_for_manual_verification(
        self, page, *, wait_timeout_seconds: int
    ) -> None:
        deadline = asyncio.get_running_loop().time() + max(
            30, int(wait_timeout_seconds)
        )
        while asyncio.get_running_loop().time() < deadline:
            if page.is_closed():
                logger.warning("Browser page closed while waiting manual verification")
                return
            title = ""
            try:
                title = await page.title()
            except Exception:
                pass
            if "验证码" not in title:
                logger.warning("验证码页面已退出，继续采集。")
                return
            await page.wait_for_timeout(1000)

        logger.warning(
            "等待手动验证超时（%ss），继续按当前页面状态采集。", wait_timeout_seconds
        )

    async def _wait_for_manual_login(
        self, page, *, wait_timeout_seconds: int
    ) -> bool:
        deadline = asyncio.get_running_loop().time() + max(
            30, int(wait_timeout_seconds)
        )
        reloaded_after_confirmation = False
        while asyncio.get_running_loop().time() < deadline:
            if page.is_closed():
                logger.warning("Browser page closed while waiting manual login")
                return False
            if await self._page_ready_for_like_actions(page):
                logger.warning("检测到浏览器已登录页面，继续执行取消点赞。")
                return True
            if not reloaded_after_confirmation:
                reload_page = getattr(page, "reload", None)
                if callable(reload_page):
                    try:
                        await reload_page(
                            wait_until="domcontentloaded",
                            timeout=10_000,
                        )
                    except Exception as exc:
                        logger.debug(
                            "Reload page after manual login confirmation failed: %s",
                            exc,
                        )
                    reloaded_after_confirmation = True
                    if await self._page_ready_for_like_actions(page):
                        logger.warning("刷新页面后检测到浏览器已登录，继续执行取消点赞。")
                        return True
            await page.wait_for_timeout(1000)

        logger.warning(
            "等待手动登录超时（%ss），页面仍未进入已登录状态。",
            wait_timeout_seconds,
        )
        return False

    async def _page_ready_for_like_actions(self, page) -> bool:
        return await self._page_has_login_cookies(
            page
        ) and not await self._page_shows_login_gate(page)

    async def _page_has_login_cookies(self, page) -> bool:
        context = getattr(page, "context", None)
        if context is None:
            return False

        try:
            cookies = await context.cookies(self.BASE_URL)
        except Exception:
            return False

        names = {
            str(cookie.get("name") or "").strip()
            for cookie in cookies or []
            if isinstance(cookie, dict)
        }
        return bool(
            names
            & {
                "sessionid",
                "sessionid_ss",
                "sid_tt",
                "sid_guard",
                "uid_tt",
                "uid_tt_ss",
            }
        )

    async def _page_shows_login_gate(self, page) -> bool:
        script = """
() => {
  const bodyText = (document.body && document.body.innerText) || "";
  const markers = [
    "未登录",
    "登录后即可观看喜欢、收藏的视频",
    "扫码登录",
    "验证码登录",
    "密码登录",
    "请先登录",
  ];
  if (markers.some((marker) => bodyText.includes(marker))) {
    return true;
  }

  const isVisible = (node) => {
    if (!(node instanceof Element)) return false;
    const style = window.getComputedStyle(node);
    if (style.display === "none" || style.visibility === "hidden") return false;
    const rect = node.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };

  const authRoots = document.querySelectorAll(
    '[role="dialog"], [class*="login"], [id*="login"], [data-e2e*="login"]'
  );
  for (const root of authRoots) {
    if (!isVisible(root)) continue;
    const text = (root.innerText || root.textContent || "").trim();
    if (!text) continue;
    if (text === "登录" || markers.some((marker) => text.includes(marker))) {
      return true;
    }
  }
  return false;
}
"""
        try:
            return bool(await page.evaluate(script))
        except Exception:
            return False

    async def _commit_digg_via_page(
        self, page, aweme_id: str, *, type_value: int
    ) -> Dict[str, Any]:
        script = """
async ({ aweme_id, type_value }) => {
  const getCookie = (name) => {
    const prefix = `${name}=`;
    const parts = document.cookie ? document.cookie.split("; ") : [];
    for (const part of parts) {
      if (part.startsWith(prefix)) {
        return decodeURIComponent(part.slice(prefix.length));
      }
    }
    return "";
  };

  const decodeGuardKey = () => {
    const raw = getCookie("bd_ticket_guard_client_data_v2");
    if (!raw) return "";
    try {
      const normalized = raw.replace(/-/g, "+").replace(/_/g, "/");
      const padding = "=".repeat((4 - (normalized.length % 4)) % 4);
      const decoded = atob(normalized + padding);
      const parsed = JSON.parse(decoded);
      return parsed && typeof parsed.ree_public_key === "string"
        ? parsed.ree_public_key
        : "";
    } catch (_err) {
      return "";
    }
  };

  const csrfToken =
    getCookie("passport_csrf_token") ||
    getCookie("passport_csrf_token_default") ||
    "";
  const headers = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "x-secsdk-csrf-request": "1",
    "x-requested-with": "XMLHttpRequest",
  };
  if (csrfToken) {
    headers["x-secsdk-csrf-token"] = csrfToken;
    headers["x-tt-passport-csrf-token"] = csrfToken;
  }
  const reePublicKey = decodeGuardKey();
  if (reePublicKey) {
    headers["bd-ticket-guard-ree-public-key"] = reePublicKey;
  }

  const body = new URLSearchParams({
    aweme_id: String(aweme_id || ""),
    item_type: "0",
    type: String(type_value || 0),
  });

  try {
    const response = await fetch("/aweme/v1/web/commit/item/digg/?aid=6383", {
      method: "POST",
      credentials: "include",
      headers,
      body: body.toString(),
    });
    const text = await response.text();
    let data = {};
    try {
      data = JSON.parse(text);
    } catch (_err) {
      data = {};
    }
    return {
      http_status: response.status,
      status_code: typeof data.status_code === "number" ? data.status_code : data.status_code ?? null,
      status_msg: data.status_msg || "",
      body: text.slice(0, 500),
    };
  } catch (error) {
    return {
      http_status: 0,
      status_code: null,
      status_msg: error && error.message ? error.message : "fetch_failed",
      body: "",
    };
  }
}
"""
        try:
            data = await page.evaluate(
                script,
                {"aweme_id": str(aweme_id or ""), "type_value": int(type_value)},
            )
        except Exception as exc:
            return {
                "http_status": 0,
                "status_code": None,
                "status_msg": str(exc),
                "body": "",
            }
        return data if isinstance(data, dict) else {}

    def _like_item_link_selector(self, aweme_id: str) -> str:
        aweme_id_str = str(aweme_id or "").strip()
        return (
            f'a[href="/video/{aweme_id_str}"], '
            f'a[href="https://www.douyin.com/video/{aweme_id_str}"], '
            f'a[href="/note/{aweme_id_str}"], '
            f'a[href="//www.douyin.com/note/{aweme_id_str}"], '
            f'a[href="https://www.douyin.com/note/{aweme_id_str}"]'
        )

    async def _ensure_like_bulk_manage_mode(self, page) -> bool:
        exit_manage = page.get_by_text("退出管理", exact=True)
        try:
            if await exit_manage.count() > 0:
                return True
        except Exception:
            pass

        manage_button = page.get_by_text("批量管理", exact=True)
        for _ in range(10):
            try:
                if await manage_button.count() > 0:
                    await manage_button.first.click(timeout=10_000)
                    await page.wait_for_timeout(500)
                    return await exit_manage.count() > 0
            except Exception as exc:
                logger.debug("Enter like bulk manage mode failed: %s", exc)
            await page.wait_for_timeout(500)
        return False

    async def _find_like_item_link(
        self,
        page,
        aweme_id: str,
        *,
        max_scroll_rounds: int = 120,
    ):
        selector = self._like_item_link_selector(aweme_id)
        link = page.locator(selector).first
        try:
            if await link.count() > 0:
                return link
        except Exception:
            pass

        scroller = await self._wait_for_like_list_scroller(page)
        if scroller is None:
            logger.debug("Like list scroller not ready while finding aweme %s", aweme_id)
            return None

        try:
            await scroller.evaluate("(el) => { el.scrollTo(0, 0); }")
            await page.wait_for_timeout(500)
        except Exception as exc:
            logger.debug("Reset like list scroll position failed: %s", exc)

        stagnant_rounds = 0
        for _ in range(max(1, int(max_scroll_rounds or 0))):
            try:
                if await link.count() > 0:
                    return link
            except Exception:
                pass

            try:
                previous_top = await scroller.evaluate("(el) => el.scrollTop")
                await scroller.evaluate("(el) => { el.scrollBy(0, 1600); }")
                await page.wait_for_timeout(800)
                current_top = await scroller.evaluate("(el) => el.scrollTop")
                if abs(float(current_top) - float(previous_top)) < 1:
                    await page.mouse.wheel(0, 1600)
                    await page.wait_for_timeout(800)
                    current_top = await scroller.evaluate("(el) => el.scrollTop")
            except Exception as exc:
                logger.debug("Scroll like list failed while finding aweme %s: %s", aweme_id, exc)
                return None

            if abs(float(current_top) - float(previous_top)) < 1:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
            if stagnant_rounds >= 8:
                break

        try:
            if await link.count() > 0:
                return link
        except Exception:
            pass
        return None

    async def _wait_for_like_list_scroller(self, page, *, timeout_ms: int = 10_000):
        scroller = page.locator(
            "div.parent-route-container.route-scroll-container, "
            "div.parent-route-container.XoIW2IMs.route-scroll-container"
        ).first
        deadline = asyncio.get_running_loop().time() + max(
            1.0, float(timeout_ms) / 1000.0
        )
        while asyncio.get_running_loop().time() < deadline:
            if page.is_closed():
                return None
            try:
                if await scroller.count() > 0:
                    return scroller
            except Exception as exc:
                logger.debug("Check like list scroller failed: %s", exc)
            await page.wait_for_timeout(250)
        return None

    async def _select_like_items_for_bulk_manage(
        self, page, aweme_ids: List[str]
    ) -> Dict[str, Any]:
        item_responses: Dict[str, Dict[str, Any]] = {}
        selected_ids: List[str] = []

        if not await self._page_ready_for_like_actions(page):
            response = {
                "http_status": 200,
                "status_code": 8,
                "status_msg": "用户未登录",
                "body": "",
            }
            for aweme_id in aweme_ids:
                item_responses[str(aweme_id or "")] = dict(response)
            return {
                "selected_ids": selected_ids,
                "item_responses": item_responses,
            }

        if not await self._ensure_like_bulk_manage_mode(page):
            response = {
                "http_status": 0,
                "status_code": 5,
                "status_msg": "bulk_manage_unavailable",
                "body": "",
            }
            for aweme_id in aweme_ids:
                item_responses[str(aweme_id or "")] = dict(response)
            return {
                "selected_ids": selected_ids,
                "item_responses": item_responses,
            }

        for aweme_id in aweme_ids:
            aweme_id_str = str(aweme_id or "").strip()
            link = await self._find_like_item_link(page, aweme_id_str)
            if link is None:
                item_responses[aweme_id_str] = {
                    "http_status": 200,
                    "status_code": 4,
                    "status_msg": "aweme_not_found_in_like_list",
                    "body": "",
                }
                continue

            try:
                await link.scroll_into_view_if_needed(timeout=10_000)
            except Exception:
                pass

            item = link.locator("xpath=ancestor::li[1]")
            checkbox = item.locator("input.semi-checkbox-input").first
            try:
                if await checkbox.count() == 0:
                    item_responses[aweme_id_str] = {
                        "http_status": 0,
                        "status_code": 5,
                        "status_msg": "bulk_checkbox_unavailable",
                        "body": "",
                    }
                    continue
                await checkbox.check(force=True, timeout=10_000)
                await page.wait_for_timeout(300)
            except Exception as exc:
                item_responses[aweme_id_str] = {
                    "http_status": 0,
                    "status_code": 5,
                    "status_msg": f"bulk_checkbox_failed:{exc}",
                    "body": "",
                }
                continue

            selected_ids.append(aweme_id_str)

        return {
            "selected_ids": selected_ids,
            "item_responses": item_responses,
        }

    async def _submit_like_bulk_unlike(
        self, page, verify_aweme_id: Optional[str] = None
    ) -> Dict[str, Any]:
        cancel_button = page.get_by_text("取消喜欢", exact=True)
        try:
            await cancel_button.click(timeout=10_000)
        except Exception as exc:
            return {
                "http_status": 0,
                "status_code": 5,
                "status_msg": f"bulk_cancel_click_failed:{exc}",
                "body": "",
            }

        confirm_button = page.get_by_text("确认取消", exact=True)
        try:
            await confirm_button.click(timeout=10_000)
        except Exception as exc:
            return {
                "http_status": 0,
                "status_code": 5,
                "status_msg": f"bulk_confirm_click_failed:{exc}",
                "body": "",
            }

        await page.wait_for_timeout(1200)
        if not await self._page_ready_for_like_actions(page):
            return {
                "http_status": 200,
                "status_code": 8,
                "status_msg": "用户未登录",
                "body": "",
            }

        return {
            "http_status": 200,
            "status_code": 0,
            "status_msg": "",
            "body": "",
        }

    async def _cancel_like_batch_via_bulk_manage(
        self, page, aweme_ids: List[str]
    ) -> Dict[str, Any]:
        selection = await self._select_like_items_for_bulk_manage(page, aweme_ids)
        selected_ids = list(selection.get("selected_ids") or [])
        item_responses = dict(selection.get("item_responses") or {})

        if not selected_ids:
            batch_response = item_responses.get(str(aweme_ids[0] or ""), {})
            return {
                "selected_ids": selected_ids,
                "item_responses": item_responses,
                "batch_response": batch_response,
            }

        batch_response = await self._submit_like_bulk_unlike(
            page, verify_aweme_id=selected_ids[0]
        )
        for aweme_id in selected_ids:
            item_responses[aweme_id] = dict(batch_response)

        return {
            "selected_ids": selected_ids,
            "item_responses": item_responses,
            "batch_response": batch_response,
        }

    async def _cancel_like_via_bulk_manage(self, page, aweme_id: str) -> Dict[str, Any]:
        result = await self._cancel_like_batch_via_bulk_manage(page, [aweme_id])
        item_responses = result.get("item_responses") or {}
        return item_responses.get(
            str(aweme_id or "").strip(),
            {
                "http_status": 0,
                "status_code": 5,
                "status_msg": "bulk_unlike_unknown",
                "body": "",
            },
        )

    async def _commit_digg_via_signed_request(
        self, aweme_id: str, *, type_value: int
    ) -> Dict[str, Any]:
        params = await self._default_query()
        params.update(
            {
                "aweme_id": str(aweme_id or ""),
                "item_type": "0",
                "type": str(type_value or 0),
            }
        )
        signed_url, ua = self.build_signed_path(
            "/aweme/v1/web/commit/item/digg/", params
        )
        csrf_token = (
            (self.cookies.get("passport_csrf_token") or "").strip()
            or (self.cookies.get("passport_csrf_token_default") or "").strip()
        )
        headers = {
            **self.headers,
            "User-Agent": ua,
            "Referer": f"{self.BASE_URL}/user/self?showTab=like",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-secsdk-csrf-request": "1",
            "x-requested-with": "XMLHttpRequest",
        }
        if csrf_token:
            headers["x-secsdk-csrf-token"] = csrf_token
            headers["x-tt-passport-csrf-token"] = csrf_token
        ree_public_key = self._decode_guard_public_key()
        if ree_public_key:
            headers["bd-ticket-guard-ree-public-key"] = ree_public_key

        payload = {
            "aweme_id": str(aweme_id or ""),
            "item_type": "0",
            "type": str(type_value or 0),
        }

        try:
            session = await self.get_session()
            async with session.post(
                signed_url,
                headers=headers,
                data=payload,
                proxy=self.proxy or None,
            ) as response:
                text = await response.text()
        except Exception as exc:
            return {
                "http_status": 0,
                "status_code": None,
                "status_msg": str(exc),
                "body": "",
            }

        data: Dict[str, Any] = {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = {}

        return {
            "http_status": response.status,
            "status_code": (
                data.get("status_code")
                if isinstance(data.get("status_code"), int)
                else data.get("status_code")
            ),
            "status_msg": str(data.get("status_msg") or ""),
            "body": text[:500],
        }

    def _decode_guard_public_key(self) -> str:
        raw = str(self.cookies.get("bd_ticket_guard_client_data_v2") or "").strip()
        if not raw:
            return ""
        try:
            normalized = raw.replace("-", "+").replace("_", "/")
            padding = "=" * ((4 - (len(normalized) % 4)) % 4)
            decoded = base64.b64decode(normalized + padding).decode("utf-8")
            payload = json.loads(decoded)
        except Exception:
            return ""
        if not isinstance(payload, dict):
            return ""
        return str(payload.get("ree_public_key") or "").strip()

    @staticmethod
    def _normalize_aweme_ids(aweme_ids: List[str]) -> List[str]:
        normalized: List[str] = []
        seen: set[str] = set()
        for aweme_id in aweme_ids or []:
            aweme_id_str = str(aweme_id or "").strip()
            if not aweme_id_str or aweme_id_str in seen:
                continue
            seen.add(aweme_id_str)
            normalized.append(aweme_id_str)
        return normalized

    @staticmethod
    def _as_int(value: Any, *, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _finalize_like_cleanup_result(result: Dict[str, Any]) -> Dict[str, Any]:
        result["success_count"] = len(result.get("success_ids") or [])
        result["failed_count"] = len(result.get("failed_ids") or [])
        return result

    def _sync_browser_cookies(self, browser_cookies: List[Dict[str, Any]]) -> None:
        merged: Dict[str, str] = {}
        for cookie in browser_cookies or []:
            if not isinstance(cookie, dict):
                continue
            name = str(cookie.get("name") or "").strip()
            value = str(cookie.get("value") or "").strip()
            domain = str(cookie.get("domain") or "")
            if not name or not value:
                continue
            if "douyin.com" not in domain:
                continue
            merged[name] = value

        if not merged:
            return

        self.cookies.update(merged)
        if self._session and not self._session.closed:
            self._session.cookie_jar.update_cookies(merged)
        logger.warning("Synced %s browser cookie(s) back to API client", len(merged))
