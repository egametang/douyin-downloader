import asyncio
import sys
import types

import pytest

from core.api_client import DouyinAPIClient


def test_default_query_uses_existing_ms_token():
    client = DouyinAPIClient({"msToken": "token-1"})
    params = asyncio.run(client._default_query())
    assert params["msToken"] == "token-1"


def test_build_signed_path_fallbacks_to_xbogus_when_abogus_disabled():
    client = DouyinAPIClient({"msToken": "token-1"})
    client._abogus_enabled = False
    signed_url, _ua = client.build_signed_path("/aweme/v1/web/aweme/detail/", {"a": 1})
    assert "X-Bogus=" in signed_url


def test_build_signed_path_prefers_abogus(monkeypatch):
    class _FakeFp:
        @staticmethod
        def generate_fingerprint(_browser):
            return "fp"

    class _FakeABogus:
        def __init__(self, fp, user_agent):
            self.fp = fp
            self.user_agent = user_agent

        def generate_abogus(self, params, body=""):
            return (f"{params}&a_bogus=fake_ab", "fake_ab", self.user_agent, body)

    import core.api_client as api_module

    monkeypatch.setattr(api_module, "BrowserFingerprintGenerator", _FakeFp)
    monkeypatch.setattr(api_module, "ABogus", _FakeABogus)

    client = DouyinAPIClient({"msToken": "token-1"})
    client._abogus_enabled = True

    signed_url, _ua = client.build_signed_path("/aweme/v1/web/aweme/detail/", {"a": 1})
    assert "a_bogus=fake_ab" in signed_url


def test_commit_digg_via_signed_request_uses_signed_post(monkeypatch):
    class _FakeResponse:
        def __init__(self):
            self.status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return

        async def text(self):
            return '{"status_code":0,"status_msg":""}'

    class _FakeSession:
        def __init__(self):
            self.calls = []

        def post(self, url, *, headers=None, data=None, proxy=None):
            self.calls.append(
                {
                    "url": url,
                    "headers": dict(headers or {}),
                    "data": dict(data or {}),
                    "proxy": proxy,
                }
            )
            return _FakeResponse()

    client = DouyinAPIClient(
        {
            "msToken": "token-1",
            "passport_csrf_token": "csrf-1",
        }
    )
    fake_session = _FakeSession()

    async def _fake_default_query():
        return {"aid": "6383", "msToken": "token-1"}

    async def _fake_get_session():
        return fake_session

    monkeypatch.setattr(client, "_default_query", _fake_default_query)
    monkeypatch.setattr(client, "get_session", _fake_get_session)
    monkeypatch.setattr(
        client,
        "build_signed_path",
        lambda path, params: ("https://www.douyin.com/signed-digg", "UA-1"),
    )

    result = asyncio.run(
        client._commit_digg_via_signed_request("111", type_value=0)
    )

    assert result["status_code"] == 0
    assert fake_session.calls[0]["url"] == "https://www.douyin.com/signed-digg"
    assert fake_session.calls[0]["data"] == {
        "aweme_id": "111",
        "item_type": "0",
        "type": "0",
    }
    assert fake_session.calls[0]["headers"]["User-Agent"] == "UA-1"
    assert fake_session.calls[0]["headers"]["x-secsdk-csrf-token"] == "csrf-1"


def test_browser_fallback_caps_warmup_wait(monkeypatch):
    class _FakeMouse:
        async def wheel(self, _x, _y):
            return

    class _FakePage:
        def __init__(self):
            self.mouse = _FakeMouse()
            self.wait_calls = 0
            self._response_handler = None

        def on(self, event_name, callback):
            if event_name == "response":
                self._response_handler = callback

        async def goto(self, *_args, **_kwargs):
            return

        async def title(self):
            return "抖音"

        def is_closed(self):
            return False

        async def wait_for_timeout(self, _ms):
            self.wait_calls += 1

    class _FakeContext:
        def __init__(self, page):
            self._page = page

        async def add_cookies(self, _cookies):
            return

        async def new_page(self):
            return self._page

        async def cookies(self, _base_url):
            return []

        async def close(self):
            return

    class _FakeBrowser:
        def __init__(self, context):
            self._context = context

        async def new_context(self, **_kwargs):
            return self._context

        async def close(self):
            return

    class _FakeChromium:
        def __init__(self, browser):
            self._browser = browser

        async def launch(self, **_kwargs):
            return self._browser

    class _FakePlaywright:
        def __init__(self, chromium):
            self.chromium = chromium

    class _FakePlaywrightManager:
        def __init__(self, playwright):
            self._playwright = playwright

        async def __aenter__(self):
            return self._playwright

        async def __aexit__(self, *_args):
            return

    page = _FakePage()
    context = _FakeContext(page)
    browser = _FakeBrowser(context)
    chromium = _FakeChromium(browser)
    playwright = _FakePlaywright(chromium)
    manager = _FakePlaywrightManager(playwright)

    fake_playwright_pkg = types.ModuleType("playwright")
    fake_async_api = types.ModuleType("playwright.async_api")
    fake_async_api.async_playwright = lambda: manager
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_async_api)

    client = DouyinAPIClient({"msToken": "token-1"})

    async def _fake_extract(_page):
        return []

    monkeypatch.setattr(client, "_extract_aweme_ids_from_page", _fake_extract)

    ids = asyncio.run(
        client.collect_user_post_ids_via_browser(
            "sec_uid_x",
            expected_count=0,
            headless=False,
            max_scrolls=240,
            idle_rounds=3,
            wait_timeout_seconds=600,
        )
    )

    assert ids == []
    # warmup should be capped instead of waiting full wait_timeout_seconds
    # and scrolling should stop after idle rounds even when no id is found
    assert page.wait_calls <= 30
    stats = client.pop_browser_post_stats()
    assert stats["selected_ids"] == 0
    assert client.pop_browser_post_stats() == {}


def test_collect_user_post_ids_via_browser_waits_for_idle_rounds_after_last_growth(
    monkeypatch,
):
    class _FakeMouse:
        async def wheel(self, _x, _y):
            return

    class _FakeResponse:
        url = "https://www.douyin.com/aweme/v1/web/aweme/post/?cursor=0"

        def __init__(self, aweme_id):
            self._aweme_id = aweme_id

        async def json(self):
            return {
                "status_code": 0,
                "aweme_list": [{"aweme_id": self._aweme_id}],
            }

    class _FakePage:
        def __init__(self):
            self.mouse = _FakeMouse()
            self._response_handler = None
            self.wait_calls = 0

        def on(self, event_name, callback):
            if event_name == "response":
                self._response_handler = callback

        async def goto(self, *_args, **_kwargs):
            return

        async def title(self):
            return "抖音"

        def is_closed(self):
            return False

        async def wait_for_timeout(self, _ms):
            self.wait_calls += 1
            payload_by_call = {
                1: "111",
                2: "222",
                3: "333",
                4: "444",
            }
            aweme_id = payload_by_call.get(self.wait_calls)
            if aweme_id and self._response_handler is not None:
                self._response_handler(_FakeResponse(aweme_id))
                await asyncio.sleep(0)

    class _FakeContext:
        def __init__(self, page):
            self._page = page

        async def add_cookies(self, _cookies):
            return

        async def new_page(self):
            return self._page

        async def cookies(self, _base_url):
            return []

        async def close(self):
            return

    class _FakeBrowser:
        def __init__(self, context):
            self._context = context

        async def new_context(self, **_kwargs):
            return self._context

        async def close(self):
            return

    class _FakeChromium:
        def __init__(self, browser):
            self._browser = browser

        async def launch(self, **_kwargs):
            return self._browser

    class _FakePlaywright:
        def __init__(self, chromium):
            self.chromium = chromium

    class _FakePlaywrightManager:
        def __init__(self, playwright):
            self._playwright = playwright

        async def __aenter__(self):
            return self._playwright

        async def __aexit__(self, *_args):
            return

    page = _FakePage()
    context = _FakeContext(page)
    browser = _FakeBrowser(context)
    chromium = _FakeChromium(browser)
    playwright = _FakePlaywright(chromium)
    manager = _FakePlaywrightManager(playwright)

    fake_playwright_pkg = types.ModuleType("playwright")
    fake_async_api = types.ModuleType("playwright.async_api")
    fake_async_api.async_playwright = lambda: manager
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_async_api)

    client = DouyinAPIClient({"msToken": "token-1"})

    async def _fake_extract(_page):
        return []

    monkeypatch.setattr(client, "_extract_aweme_ids_from_page", _fake_extract)

    ids = asyncio.run(
        client.collect_user_post_ids_via_browser(
            "sec_uid_x",
            expected_count=0,
            headless=False,
            max_scrolls=10,
            idle_rounds=2,
            wait_timeout_seconds=30,
        )
    )

    assert ids == ["111", "222", "333", "444"]
    assert page.wait_calls >= 6
    assert client.pop_browser_post_stats() == {
        "merged_ids": 4,
        "post_api_ids": 4,
        "selected_ids": 4,
        "post_items": 4,
        "post_pages": 4,
    }


def test_collect_user_like_ids_via_browser_uses_favorite_api_payload(monkeypatch):
    class _FakeMouse:
        async def wheel(self, _x, _y):
            return

    class _FakeLocator:
        def __init__(self):
            self.first = self

        async def count(self):
            return 1

        async def evaluate(self, _script):
            return None

    class _FakePage:
        def __init__(self):
            self.mouse = _FakeMouse()
            self._response_handler = None

        def on(self, event_name, callback):
            if event_name == "response":
                self._response_handler = callback

        async def goto(self, *_args, **_kwargs):
            if self._response_handler is not None:
                self._response_handler(_FakeResponse())
            return

        async def title(self):
            return "抖音"

        def is_closed(self):
            return False

        def locator(self, _selector):
            return _FakeLocator()

        async def wait_for_timeout(self, _ms):
            return

    class _FakeContext:
        def __init__(self, page):
            self._page = page
            self.pages = []

        async def new_page(self):
            return self._page

        async def cookies(self, _base_url):
            return []

        async def close(self):
            return

    class _FakeBrowser:
        async def close(self):
            return

    class _FakeResponse:
        url = "https://www.douyin.com/aweme/v1/web/aweme/favorite/?cursor=0"
        status = 200
        headers = {"content-type": "application/json"}

        async def json(self):
            return {
                "status_code": 0,
                "aweme_list": [
                    {"aweme_id": "111", "desc": "like-111"},
                    {"aweme_id": "222", "desc": "like-222"},
                ],
            }

    class _FakePlaywrightManager:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return

    fake_playwright_pkg = types.ModuleType("playwright")
    fake_async_api = types.ModuleType("playwright.async_api")
    fake_async_api.async_playwright = lambda: _FakePlaywrightManager()
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_async_api)

    client = DouyinAPIClient({"msToken": "token-1"})
    page = _FakePage()
    context = _FakeContext(page)
    browser = _FakeBrowser()

    async def _fake_create_browser_context(*_args, **_kwargs):
        return context, browser

    monkeypatch.setattr(client, "_create_browser_context", _fake_create_browser_context)

    ids = asyncio.run(
        client.collect_user_like_ids_via_browser(
            "sec_uid_x",
            headless=True,
            max_scrolls=1,
            idle_rounds=1,
            wait_timeout_seconds=30,
        )
    )

    assert ids == ["111", "222"]
    assert client.pop_browser_like_aweme_items() == {
        "111": {"aweme_id": "111", "desc": "like-111"},
        "222": {"aweme_id": "222", "desc": "like-222"},
    }
    assert client.pop_browser_like_stats() == {
        "selected_ids": 2,
        "like_items": 2,
        "like_pages": 1,
    }


def test_collect_user_like_ids_via_browser_waits_for_idle_rounds_after_last_growth(
    monkeypatch,
):
    class _FakeMouse:
        async def wheel(self, _x, _y):
            return

    class _FakeLocator:
        def __init__(self):
            self.first = self

        async def count(self):
            return 1

        async def evaluate(self, _script):
            return None

    class _FakeResponse:
        url = "https://www.douyin.com/aweme/v1/web/aweme/favorite/?cursor=0"

        def __init__(self, aweme_id):
            self._aweme_id = aweme_id

        async def json(self):
            return {
                "status_code": 0,
                "aweme_list": [{"aweme_id": self._aweme_id}],
            }

    class _FakePage:
        def __init__(self):
            self.mouse = _FakeMouse()
            self._response_handler = None
            self.wait_calls = 0

        def on(self, event_name, callback):
            if event_name == "response":
                self._response_handler = callback

        async def goto(self, *_args, **_kwargs):
            return

        async def title(self):
            return "抖音"

        def is_closed(self):
            return False

        def locator(self, _selector):
            return _FakeLocator()

        async def wait_for_timeout(self, _ms):
            self.wait_calls += 1
            payload_by_call = {
                1: "111",
                2: "222",
                3: "333",
                4: "444",
            }
            aweme_id = payload_by_call.get(self.wait_calls)
            if aweme_id and self._response_handler is not None:
                self._response_handler(_FakeResponse(aweme_id))
                await asyncio.sleep(0)

    class _FakeContext:
        def __init__(self, page):
            self._page = page
            self.pages = []

        async def new_page(self):
            return self._page

        async def cookies(self, _base_url):
            return []

        async def close(self):
            return

    class _FakeBrowser:
        async def close(self):
            return

    class _FakePlaywrightManager:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return

    fake_playwright_pkg = types.ModuleType("playwright")
    fake_async_api = types.ModuleType("playwright.async_api")
    fake_async_api.async_playwright = lambda: _FakePlaywrightManager()
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_async_api)

    client = DouyinAPIClient({"msToken": "token-1"})
    page = _FakePage()
    context = _FakeContext(page)
    browser = _FakeBrowser()

    async def _fake_create_browser_context(*_args, **_kwargs):
        return context, browser

    monkeypatch.setattr(client, "_create_browser_context", _fake_create_browser_context)

    ids = asyncio.run(
        client.collect_user_like_ids_via_browser(
            "sec_uid_x",
            headless=True,
            max_scrolls=10,
            idle_rounds=2,
            wait_timeout_seconds=30,
        )
    )

    assert ids == ["111", "222", "333", "444"]
    assert page.wait_calls >= 6
    assert client.pop_browser_like_stats() == {
        "selected_ids": 4,
        "like_items": 4,
        "like_pages": 4,
    }


def test_cancel_likes_via_browser_uses_sensitive_cookies(monkeypatch):
    class _FakePage:
        def __init__(self):
            self.context = None
            self.goto_calls = []
            self.wait_calls = 0

        async def goto(self, *args, **kwargs):
            self.goto_calls.append((args, kwargs))
            return

        async def title(self):
            return "抖音"

        def is_closed(self):
            return False

        async def wait_for_timeout(self, _ms):
            self.wait_calls += 1

    class _FakeContext:
        def __init__(self, page):
            self._page = page
            self._page.context = self
            self.added_cookies = []

        async def add_cookies(self, cookies):
            self.added_cookies.extend(cookies)

        async def new_page(self):
            return self._page

        async def cookies(self, _base_url):
            return [{"name": "sessionid", "value": "sess", "domain": ".douyin.com"}]

        async def close(self):
            return

    class _FakeBrowser:
        def __init__(self, context):
            self._context = context

        async def new_context(self, **_kwargs):
            return self._context

        async def close(self):
            return

    class _FakeChromium:
        def __init__(self, browser):
            self._browser = browser

        async def launch(self, **_kwargs):
            return self._browser

    class _FakePlaywright:
        def __init__(self, chromium):
            self.chromium = chromium

    class _FakePlaywrightManager:
        def __init__(self, playwright):
            self._playwright = playwright

        async def __aenter__(self):
            return self._playwright

        async def __aexit__(self, *_args):
            return

    page = _FakePage()
    context = _FakeContext(page)
    browser = _FakeBrowser(context)
    chromium = _FakeChromium(browser)
    playwright = _FakePlaywright(chromium)
    manager = _FakePlaywrightManager(playwright)

    fake_playwright_pkg = types.ModuleType("playwright")
    fake_async_api = types.ModuleType("playwright.async_api")
    fake_async_api.async_playwright = lambda: manager
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_async_api)

    client = DouyinAPIClient(
        {"msToken": "token-1", "sessionid": "sess", "sid_tt": "sid-tt"}
    )
    progress_events = []
    batch_calls = []

    async def _fake_cancel_like_batch(page, aweme_ids):
        assert page is not None
        aweme_ids = [str(aweme_id) for aweme_id in aweme_ids]
        batch_calls.append(list(aweme_ids))
        item_responses = {}
        for aweme_id in aweme_ids:
            if aweme_id == "111":
                item_responses[aweme_id] = {
                    "http_status": 200,
                    "status_code": 0,
                    "status_msg": "",
                }
            else:
                item_responses[aweme_id] = {
                    "http_status": 200,
                    "status_code": 5,
                    "status_msg": "failed",
                }
        return {
            "selected_ids": list(aweme_ids),
            "item_responses": item_responses,
            "batch_response": {
                "http_status": 200,
                "status_code": 0,
                "status_msg": "",
            },
        }

    monkeypatch.setattr(
        client, "_cancel_like_batch_via_bulk_manage", _fake_cancel_like_batch
    )

    result = asyncio.run(
        client.cancel_likes_via_browser(
            ["111", "222"],
            headless=False,
            wait_timeout_seconds=60,
            request_interval_ms=10,
            progress_callback=progress_events.append,
        )
    )

    assert result["requested"] == 2
    assert result["success_ids"] == ["111"]
    assert result["failed_ids"] == ["222"]
    assert any(cookie["name"] == "sessionid" for cookie in context.added_cookies)
    assert any(cookie["name"] == "sid_tt" for cookie in context.added_cookies)
    assert batch_calls == [["111", "222"]]
    assert [event["status"] for event in progress_events] == ["success", "failed"]


def test_cancel_likes_via_browser_uses_persistent_context_when_profile_dir_provided(
    monkeypatch, tmp_path
):
    class _FakePage:
        def __init__(self):
            self.context = None

        async def goto(self, *args, **kwargs):
            return

        async def title(self):
            return "抖音"

        def is_closed(self):
            return False

        async def wait_for_timeout(self, _ms):
            return

    class _FakeContext:
        def __init__(self, page):
            self._page = page
            self._page.context = self
            self.added_cookies = []
            self.pages = [page]
            self.closed = False

        async def add_cookies(self, cookies):
            self.added_cookies.extend(cookies)

        async def new_page(self):
            return self._page

        async def cookies(self, _base_url):
            return [{"name": "sessionid", "value": "sess", "domain": ".douyin.com"}]

        async def close(self):
            self.closed = True

    class _FakeChromium:
        def __init__(self, context):
            self._context = context
            self.launch_calls = []
            self.persistent_calls = []

        async def launch(self, **kwargs):
            self.launch_calls.append(dict(kwargs))
            raise AssertionError("launch should not be used for persistent context")

        async def launch_persistent_context(self, user_data_dir, **kwargs):
            self.persistent_calls.append(
                {"user_data_dir": user_data_dir, **dict(kwargs)}
            )
            return self._context

    class _FakePlaywright:
        def __init__(self, chromium):
            self.chromium = chromium

    class _FakePlaywrightManager:
        def __init__(self, playwright):
            self._playwright = playwright

        async def __aenter__(self):
            return self._playwright

        async def __aexit__(self, *_args):
            return

    page = _FakePage()
    context = _FakeContext(page)
    chromium = _FakeChromium(context)
    playwright = _FakePlaywright(chromium)
    manager = _FakePlaywrightManager(playwright)

    fake_playwright_pkg = types.ModuleType("playwright")
    fake_async_api = types.ModuleType("playwright.async_api")
    fake_async_api.async_playwright = lambda: manager
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_async_api)

    client = DouyinAPIClient(
        {"msToken": "token-1", "sessionid": "sess", "sid_tt": "sid-tt"}
    )
    profile_dir = tmp_path / "profile"
    batch_calls = []

    async def _fake_cancel_like_batch(page, aweme_ids):
        assert page is not None
        aweme_ids = [str(aweme_id) for aweme_id in aweme_ids]
        batch_calls.append(list(aweme_ids))
        return {
            "selected_ids": list(aweme_ids),
            "item_responses": {
                aweme_id: {
                    "http_status": 200,
                    "status_code": 0,
                    "status_msg": "",
                }
                for aweme_id in aweme_ids
            },
            "batch_response": {
                "http_status": 200,
                "status_code": 0,
                "status_msg": "",
            },
        }

    monkeypatch.setattr(
        client, "_cancel_like_batch_via_bulk_manage", _fake_cancel_like_batch
    )

    result = asyncio.run(
        client.cancel_likes_via_browser(
            ["111"],
            headless=False,
            wait_timeout_seconds=60,
            request_interval_ms=10,
            profile_dir=str(profile_dir),
        )
    )

    assert result["success_ids"] == ["111"]
    assert chromium.persistent_calls[0]["user_data_dir"] == str(profile_dir)
    assert profile_dir.exists() is True
    added_names = {cookie["name"] for cookie in context.added_cookies}
    assert {"msToken", "sid_tt"} <= added_names
    assert "sessionid" not in added_names
    assert batch_calls == [["111"]]


def test_cancel_likes_via_browser_batches_requests(monkeypatch):
    class _FakePage:
        def __init__(self):
            self.context = None

        async def goto(self, *args, **kwargs):
            return

        async def title(self):
            return "抖音"

        def is_closed(self):
            return False

        async def wait_for_timeout(self, _ms):
            return

    class _FakeContext:
        def __init__(self, page):
            self._page = page
            self._page.context = self
            self.pages = [page]

        async def add_cookies(self, _cookies):
            return

        async def new_page(self):
            return self._page

        async def cookies(self, _base_url):
            return [{"name": "sessionid", "value": "sess", "domain": ".douyin.com"}]

        async def close(self):
            return

    class _FakeBrowser:
        def __init__(self, context):
            self._context = context

        async def new_context(self, **_kwargs):
            return self._context

        async def close(self):
            return

    class _FakeChromium:
        def __init__(self, browser):
            self._browser = browser

        async def launch(self, **_kwargs):
            return self._browser

    class _FakePlaywright:
        def __init__(self, chromium):
            self.chromium = chromium

    class _FakePlaywrightManager:
        def __init__(self, playwright):
            self._playwright = playwright

        async def __aenter__(self):
            return self._playwright

        async def __aexit__(self, *_args):
            return

    page = _FakePage()
    context = _FakeContext(page)
    browser = _FakeBrowser(context)
    chromium = _FakeChromium(browser)
    playwright = _FakePlaywright(chromium)
    manager = _FakePlaywrightManager(playwright)

    fake_playwright_pkg = types.ModuleType("playwright")
    fake_async_api = types.ModuleType("playwright.async_api")
    fake_async_api.async_playwright = lambda: manager
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_async_api)

    client = DouyinAPIClient({"sessionid": "sess", "sid_tt": "sid-tt"})
    progress_events = []
    batch_calls = []

    async def _fake_cancel_like_batch(page, aweme_ids):
        assert page is not None
        aweme_ids = [str(aweme_id) for aweme_id in aweme_ids]
        batch_calls.append(list(aweme_ids))
        return {
            "selected_ids": list(aweme_ids),
            "item_responses": {
                aweme_id: {
                    "http_status": 200,
                    "status_code": 0,
                    "status_msg": "",
                }
                for aweme_id in aweme_ids
            },
            "batch_response": {
                "http_status": 200,
                "status_code": 0,
                "status_msg": "",
            },
        }

    monkeypatch.setattr(
        client, "_cancel_like_batch_via_bulk_manage", _fake_cancel_like_batch
    )

    aweme_ids = [str(100 + index) for index in range(9)]
    result = asyncio.run(
        client.cancel_likes_via_browser(
            aweme_ids,
            headless=False,
            wait_timeout_seconds=60,
            request_interval_ms=10,
            progress_callback=progress_events.append,
        )
    )

    assert result["success_ids"] == aweme_ids
    assert batch_calls == [aweme_ids[:8], aweme_ids[8:]]
    assert len(progress_events) == 9


def test_find_like_item_link_waits_for_scroller_after_reload():
    class _FakeMouse:
        async def wheel(self, _x, _y):
            return

    class _FakeLocator:
        def __init__(self, count_func, evaluate_func=None):
            self._count_func = count_func
            self._evaluate_func = evaluate_func
            self.first = self

        async def count(self):
            return self._count_func()

        async def evaluate(self, script):
            if self._evaluate_func is None:
                return None
            return self._evaluate_func(script)

    class _FakePage:
        def __init__(self):
            self.mouse = _FakeMouse()
            self.wait_calls = 0
            self.scroll_top = 0

        def is_closed(self):
            return False

        def locator(self, selector):
            if selector.startswith('a[href="/video/222"]'):
                return _FakeLocator(lambda: 1 if self.wait_calls >= 2 else 0)
            if selector.startswith("div.parent-route-container.route-scroll-container"):
                return _FakeLocator(
                    lambda: 1 if self.wait_calls >= 2 else 0,
                    self._evaluate_scroller,
                )
            raise AssertionError(f"unexpected selector: {selector}")

        async def wait_for_timeout(self, _ms):
            self.wait_calls += 1

        def _evaluate_scroller(self, script):
            if "scrollTo" in script:
                self.scroll_top = 0
                return None
            if "scrollBy" in script:
                previous = self.scroll_top
                self.scroll_top += 1600
                return previous
            if "scrollTop" in script:
                return self.scroll_top
            raise AssertionError(f"unexpected script: {script}")

    client = DouyinAPIClient({"sessionid": "sess", "sid_tt": "sid-tt"})
    page = _FakePage()

    link = asyncio.run(client._find_like_item_link(page, "222"))

    assert link is not None
    assert page.wait_calls >= 2


def test_submit_like_bulk_unlike_skips_scroll_verification_after_confirm(monkeypatch):
    class _FakeAction:
        def __init__(self):
            self.click_calls = 0

        async def click(self, timeout=None):
            assert timeout == 10_000
            self.click_calls += 1

    class _FakePage:
        def __init__(self):
            self.cancel = _FakeAction()
            self.confirm = _FakeAction()
            self.goto_calls = []
            self.wait_calls = []

        def get_by_text(self, text, exact=True):
            assert exact is True
            if text == "取消喜欢":
                return self.cancel
            if text == "确认取消":
                return self.confirm
            raise AssertionError(f"unexpected text: {text}")

        async def goto(self, url, wait_until=None, timeout=None):
            self.goto_calls.append(
                {
                    "url": url,
                    "wait_until": wait_until,
                    "timeout": timeout,
                }
            )

        async def wait_for_timeout(self, ms):
            self.wait_calls.append(ms)

    client = DouyinAPIClient({"sessionid": "sess", "sid_tt": "sid-tt"})
    page = _FakePage()
    async def _page_ready(_page):
        assert _page is page
        return True

    monkeypatch.setattr(client, "_page_ready_for_like_actions", _page_ready)

    result = asyncio.run(client._submit_like_bulk_unlike(page, verify_aweme_id="222"))

    assert result["status_code"] == 0
    assert page.cancel.click_calls == 1
    assert page.confirm.click_calls == 1
    assert page.wait_calls == [1200]
    assert page.goto_calls == []


def test_cancel_likes_via_browser_fails_fast_when_login_ui_still_visible(
    monkeypatch,
):
    class _FakePage:
        def __init__(self):
            self.context = None

        async def goto(self, *args, **kwargs):
            return

        async def title(self):
            return "抖音"

        def is_closed(self):
            return False

        async def wait_for_timeout(self, _ms):
            return

        async def evaluate(self, _script, payload=None):
            if payload is None:
                return True
            raise AssertionError("commit request should not run while login UI is visible")

    class _FakeContext:
        def __init__(self, page):
            self._page = page
            self._page.context = self
            self.pages = [page]

        async def add_cookies(self, _cookies):
            return

        async def new_page(self):
            return self._page

        async def cookies(self, _base_url):
            return [{"name": "sessionid", "value": "sess", "domain": ".douyin.com"}]

        async def close(self):
            return

    class _FakeBrowser:
        def __init__(self, context):
            self._context = context

        async def new_context(self, **_kwargs):
            return self._context

        async def close(self):
            return

    class _FakeChromium:
        def __init__(self, context):
            self._context = context

        async def launch(self, **_kwargs):
            return _FakeBrowser(self._context)

    class _FakePlaywright:
        def __init__(self, chromium):
            self.chromium = chromium

    class _FakePlaywrightManager:
        def __init__(self, playwright):
            self._playwright = playwright

        async def __aenter__(self):
            return self._playwright

        async def __aexit__(self, *_args):
            return

    page = _FakePage()
    context = _FakeContext(page)
    chromium = _FakeChromium(context)
    playwright = _FakePlaywright(chromium)
    manager = _FakePlaywrightManager(playwright)

    fake_playwright_pkg = types.ModuleType("playwright")
    fake_async_api = types.ModuleType("playwright.async_api")
    fake_async_api.async_playwright = lambda: manager
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_async_api)

    client = DouyinAPIClient({"sessionid": "sess", "sid_tt": "sid-tt"})
    prompts = []

    async def _fake_wait_for_manual_login(*_args, **_kwargs):
        return False

    monkeypatch.setattr(client, "_wait_for_manual_login", _fake_wait_for_manual_login)

    result = asyncio.run(
        client.cancel_likes_via_browser(
            ["111"],
            headless=False,
            wait_timeout_seconds=60,
            request_interval_ms=10,
            login_confirmation_callback=lambda message: prompts.append(message),
        )
    )

    assert result["success_ids"] == []
    assert result["failed_ids"] == ["111"]
    assert len(prompts) == 1


@pytest.mark.asyncio
async def test_get_user_post_returns_normalized_dto(monkeypatch):
    client = DouyinAPIClient({"msToken": "token-1"})
    captured_params = {}

    async def _fake_request_json(path, params, suppress_error=False):
        assert path == "/aweme/v1/web/aweme/post/"
        captured_params.update(params)
        return {
            "status_code": 0,
            "aweme_list": [{"aweme_id": "111"}],
            "has_more": 1,
            "max_cursor": 9,
        }

    monkeypatch.setattr(client, "_request_json", _fake_request_json)
    data = await client.get_user_post("sec-1", max_cursor=0, count=20)

    assert data["items"] == [{"aweme_id": "111"}]
    assert data["aweme_list"] == [{"aweme_id": "111"}]
    assert data["has_more"] is True
    assert data["max_cursor"] == 9
    assert data["status_code"] == 0
    assert data["source"] == "api"
    assert isinstance(data["raw"], dict)
    assert captured_params["show_live_replay_strategy"] == "1"
    assert captured_params["need_time_list"] == "1"
    assert captured_params["time_list_query"] == "0"


@pytest.mark.asyncio
async def test_user_mode_endpoints_use_shared_paged_normalization(monkeypatch):
    client = DouyinAPIClient({"msToken": "token-1"})
    called_requests = []

    async def _fake_request_json(path, params, suppress_error=False):
        called_requests.append((path, dict(params)))
        return {"status_code": 0, "aweme_list": [], "has_more": 0, "max_cursor": 0}

    monkeypatch.setattr(client, "_request_json", _fake_request_json)

    like_data = await client.get_user_like("sec-1", max_cursor=0, count=20)
    mix_data = await client.get_user_mix("sec-1", max_cursor=0, count=20)
    music_data = await client.get_user_music("sec-1", max_cursor=0, count=20)

    assert [path for path, _params in called_requests] == [
        "/aweme/v1/web/aweme/favorite/",
        "/aweme/v1/web/mix/list/",
        "/aweme/v1/web/music/list/",
    ]
    mix_params = called_requests[1][1]
    music_params = called_requests[2][1]
    for forbidden_key in (
        "show_live_replay_strategy",
        "need_time_list",
        "time_list_query",
    ):
        assert forbidden_key not in mix_params
        assert forbidden_key not in music_params
    assert like_data["items"] == []
    assert mix_data["items"] == []
    assert music_data["items"] == []


@pytest.mark.asyncio
async def test_collect_endpoints_use_expected_paths_and_normalization(monkeypatch):
    client = DouyinAPIClient({"msToken": "token-1"})
    called_requests = []

    async def _fake_request_json(path, params, suppress_error=False):
        called_requests.append((path, dict(params)))
        if path == "/aweme/v1/web/collects/list/":
            return {
                "status_code": 0,
                "collects_list": [{"collects_id_str": "collect-1"}],
                "has_more": 1,
                "cursor": 9,
            }
        if path == "/aweme/v1/web/collects/video/list/":
            return {
                "status_code": 0,
                "aweme_list": [{"aweme_id": "aweme-1"}],
                "has_more": 0,
                "cursor": 0,
            }
        if path == "/aweme/v1/web/mix/listcollection/":
            return {
                "status_code": 0,
                "mix_infos": [{"mix_id": "mix-1"}],
                "has_more": 0,
                "cursor": 0,
            }
        return {"status_code": 0, "has_more": 0, "cursor": 0}

    monkeypatch.setattr(client, "_request_json", _fake_request_json)

    collects_data = await client.get_user_collects("self", max_cursor=0, count=10)
    collect_aweme_data = await client.get_collect_aweme(
        "collect-1", max_cursor=0, count=10
    )
    collect_mix_data = await client.get_user_collect_mix(
        "self", max_cursor=0, count=12
    )

    assert [path for path, _params in called_requests] == [
        "/aweme/v1/web/collects/list/",
        "/aweme/v1/web/collects/video/list/",
        "/aweme/v1/web/mix/listcollection/",
    ]
    assert called_requests[0][1]["count"] == 10
    assert called_requests[0][1]["version_code"] == "170400"
    assert called_requests[1][1]["collects_id"] == "collect-1"
    assert called_requests[1][1]["count"] == 10
    assert called_requests[2][1]["count"] == 12
    assert collects_data["items"] == [{"collects_id_str": "collect-1"}]
    assert collects_data["has_more"] is True
    assert collects_data["max_cursor"] == 9
    assert collect_aweme_data["items"] == [{"aweme_id": "aweme-1"}]
    assert collect_mix_data["items"] == [{"mix_id": "mix-1"}]


@pytest.mark.asyncio
async def test_mix_and_music_endpoints_are_normalized(monkeypatch):
    client = DouyinAPIClient({"msToken": "token-1"})

    async def _fake_request_json(path, _params, suppress_error=False):
        if path == "/aweme/v1/web/mix/detail/":
            return {"mix_info": {"mix_id": "mix-1"}}
        if path == "/aweme/v1/web/mix/aweme/":
            return {"status_code": 0, "aweme_list": [{"aweme_id": "a-1"}], "has_more": 0}
        if path == "/aweme/v1/web/music/detail/":
            return {"music_info": {"id": "music-1"}}
        if path == "/aweme/v1/web/music/aweme/":
            return {"status_code": 0, "aweme_list": [{"aweme_id": "a-2"}], "has_more": 0}
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr(client, "_request_json", _fake_request_json)

    mix_detail = await client.get_mix_detail("mix-1")
    mix_page = await client.get_mix_aweme("mix-1", cursor=0, count=20)
    music_detail = await client.get_music_detail("music-1")
    music_page = await client.get_music_aweme("music-1", cursor=0, count=20)

    assert mix_detail == {"mix_id": "mix-1"}
    assert music_detail == {"id": "music-1"}
    assert mix_page["items"] == [{"aweme_id": "a-1"}]
    assert music_page["items"] == [{"aweme_id": "a-2"}]
