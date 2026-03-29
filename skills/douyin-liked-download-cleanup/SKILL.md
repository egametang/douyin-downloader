---
name: douyin-liked-download-cleanup
description: Run the douyin-downloader project to download liked Douyin videos or galleries from a logged-in account and cancel likes after successful downloads. Use when the user wants this repo to fetch liked items with mode like, refresh cookies with Playwright, troubleshoot empty like lists caused by missing login cookies such as sessionid or sid_tt, or rerun unlike cleanup for already-downloaded items.
---

# Douyin Liked Download Cleanup

Use this skill only when the current workspace is the `douyin-downloader` project or an equivalent fork with the same commands and config structure.

## Workflow

1. Before any browser launch, check for residual cleanup or Playwright processes that still hold the like-cleanup profile and stop them.
2. Verify the local runtime before changing anything.
3. Inspect `config.yml` and confirm the like-download path is enabled.
4. Refresh cookies with Playwright when login cookies are missing or the first like run returns zero items unexpectedly.
5. Run the downloader and monitor the download phase and the unlike-cleanup phase separately.
6. If downloads already succeeded earlier, run the cleanup-only tool against the database or manifest.

Never treat "browser opened" as "login succeeded". After opening a browser for login, stop and wait for the user to explicitly confirm login success in chat before sending Enter or starting the next command.

## Verify Runtime

Check these commands first:

```bash
ps -o pid,ppid,etime,stat,command -ax | rg "tools/cancel_downloaded_likes.py|tools/cookie_fetcher.py|playwright/driver/node|Google Chrome for Testing.*playwright-like-cleanup-profile"
./.venv/bin/python -c "import importlib.util;mods=['playwright','aiohttp','yaml','rich'];print({m:bool(importlib.util.find_spec(m)) for m in mods})"
./.venv/bin/python -m playwright install --list
```

If `playwright` or Chromium is missing, install them before continuing.

If the residual-process check returns any older `cancel_downloaded_likes.py`, `cookie_fetcher.py`, Playwright driver, or Chrome-for-Testing process that still points at `config/playwright-like-cleanup-profile`, terminate them first and rerun the check until it is empty. Never launch a new browser against that profile while an older run still owns it.

## Verify Config

Inspect `config.yml` and confirm these fields:

```yaml
mode:
  - like

number:
  like: 0

like_cleanup:
  enabled: true
  headless: false
  persist_login: true
  profile_dir: ./config/playwright-like-cleanup-profile
```

Use `number.like: 0` for full history. Keep `like_cleanup.headless: false` so the browser can surface login or verification prompts during unlike cleanup.

Treat these cookie groups differently:

- Base API cookies: `msToken`, `ttwid`, `odin_tt`, `passport_csrf_token`
- Login cookies required for liked-list access and unlike cleanup: `sessionid`, `sessionid_ss`, `sid_tt`, `sid_guard`, `uid_tt`, `uid_tt_ss`

Report whether keys are present. Do not print secret cookie values back to the user.

## Refresh Cookies

Refresh cookies when either condition is true:

- Login cookies are missing.
- `mode: like` returns `0` items even though the user expects liked items.

Run:

```bash
./.venv/bin/python tools/cookie_fetcher.py --config config.yml --include-all --profile-dir config/playwright-like-cleanup-profile
```

Then:

1. Tell the user a browser window opened.
2. Ask the user to finish Douyin login in the browser.
3. Wait until the user explicitly says login succeeded, for example `已登录`.
4. Only after that confirmation, send Enter to the waiting command so it writes cookies back into `config.yml`.
5. Recheck that the login cookies listed above are now present.

If the user previously interrupted a run, repeat the residual-process check again before starting `cookie_fetcher.py`.

## Run The Main Flow

Run:

```bash
./.venv/bin/douyin-dl -c config.yml
```

Watch for these phases:

- Like list discovery: the run should move past zero items and report a total item count.
- Download phase: track success, failure, and skipped counts.
- Unlike phase: after successful like downloads, the tool should enter the `取消点赞` step. If the browser requests login again, stop and wait for the user to explicitly confirm login success before continuing.

Before rerunning the main flow after an interruption, repeat the residual-process check so you do not stack multiple Playwright sessions onto the same profile.

If the like list still comes back empty after cookie refresh, assume one of these before changing code:

- The account currently has no visible liked items.
- The liked list is private or otherwise unavailable to the current login.
- Douyin returned a risk or verification response and manual browser interaction is still required.

## Cleanup-Only Flow

If items were already downloaded earlier and only unlike cleanup needs to run, use:

```bash
./.venv/bin/python tools/cancel_downloaded_likes.py -c config.yml
```

Useful flags:

- `--limit N`
- `--source db`
- `--source manifest`
- `--aweme-id <id>` repeated for targeted retries
- `--profile-dir config/playwright-like-cleanup-profile`

Before any cleanup-only retry, repeat the residual-process check and clear old processes first.

## Troubleshooting

Read [references/workflow.md](references/workflow.md) for the exact checklist and common failure patterns before editing project code.
