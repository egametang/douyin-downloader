from types import SimpleNamespace

from rich.console import Console

from core.downloader_base import DownloadResult
from cli.progress_display import ProgressDisplay


class _FakeProgress:
    def __init__(self):
        self.tasks = {}
        self.removed = []
        self._next_id = 1
        self.console = SimpleNamespace(print=lambda *_args, **_kwargs: None)

    def add_task(self, description, total, completed=0, detail="", **kwargs):
        task_id = self._next_id
        self._next_id += 1
        self.tasks[task_id] = {
            "description": description,
            "total": total,
            "completed": completed,
            "detail": detail,
        }
        self.tasks[task_id].update(kwargs)
        return task_id

    def update(self, task_id, **kwargs):
        self.tasks[task_id].update(kwargs)

    def advance(self, task_id, advance=1):
        self.tasks[task_id]["completed"] = self.tasks[task_id].get("completed", 0) + advance

    def remove_task(self, task_id):
        self.removed.append(task_id)
        self.tasks.pop(task_id, None)


class _FakeProgressContext:
    def __init__(self, progress):
        self.progress = progress
        self.exited = False

    def __enter__(self):
        return self.progress

    def __exit__(self, *_args):
        self.exited = True


class _FakeConsole:
    def __init__(self):
        self.messages = []

    def print(self, *args, **kwargs):
        self.messages.append((args, kwargs))


def test_show_banner_does_not_print_tool_title():
    display = ProgressDisplay()
    fake_console = _FakeConsole()
    display.console = fake_console

    display.show_banner()

    assert fake_console.messages == []


def test_show_result_renders_failed_and_skipped_reasons():
    display = ProgressDisplay()
    console = Console(record=True, width=120)
    display.console = console
    result = DownloadResult()
    result.total = 3
    result.success = 1
    result.record_skipped("222", "already downloaded item", "已存在本地文件")
    result.record_failed("333", "broken item", "获取视频详情失败")

    display.show_result(result)

    output = console.export_text()
    assert "Skipped Details" in output
    assert "already downloaded item" in output
    assert "已存在本地文件" in output
    assert "Failed Details" in output
    assert "broken item" in output
    assert "获取视频详情失败" in output


def test_single_url_overall_progress_follows_item_count(monkeypatch):
    display = ProgressDisplay()
    fake_progress = _FakeProgress()
    fake_ctx = _FakeProgressContext(fake_progress)
    monkeypatch.setattr(display, "create_progress", lambda: fake_ctx)

    display.start_download_session(1)
    overall_task_id = display._overall_task_id
    assert overall_task_id is not None
    assert fake_progress.tasks[overall_task_id]["total"] == 1

    display.start_url(1, 1, "https://example.com/u")
    display.set_item_total(5, "作品待下载")
    assert fake_progress.tasks[overall_task_id]["total"] == 5
    assert fake_progress.tasks[overall_task_id]["completed"] == 0

    display.advance_item("success", "a1")
    display.advance_item("failed", "a2")
    assert fake_progress.tasks[overall_task_id]["completed"] == 2

    display.complete_url(SimpleNamespace(success=3, failed=1, skipped=1))
    assert fake_progress.tasks[overall_task_id]["completed"] == 5


def test_multi_url_overall_progress_stays_url_based(monkeypatch):
    display = ProgressDisplay()
    fake_progress = _FakeProgress()
    fake_ctx = _FakeProgressContext(fake_progress)
    monkeypatch.setattr(display, "create_progress", lambda: fake_ctx)

    display.start_download_session(2)
    overall_task_id = display._overall_task_id
    assert overall_task_id is not None
    assert fake_progress.tasks[overall_task_id]["total"] == 2

    display.start_url(1, 2, "https://example.com/u1")
    display.set_item_total(8, "作品待下载")
    display.advance_item("success", "a1")
    assert fake_progress.tasks[overall_task_id]["completed"] == 0

    display.complete_url(SimpleNamespace(success=8, failed=0, skipped=0))
    assert fake_progress.tasks[overall_task_id]["completed"] == 1

    display.start_url(2, 2, "https://example.com/u2")
    display.fail_url("url failed")
    assert fake_progress.tasks[overall_task_id]["completed"] == 2
