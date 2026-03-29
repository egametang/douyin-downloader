# Workflow Reference

## One-Time Preparation

Run these checks:

```bash
ps -o pid,ppid,etime,stat,command -ax | rg "tools/cancel_downloaded_likes.py|tools/cookie_fetcher.py|playwright/driver/node|Google Chrome for Testing.*playwright-like-cleanup-profile"
./.venv/bin/python -c "import importlib.util;mods=['playwright','aiohttp','yaml','rich'];print({m:bool(importlib.util.find_spec(m)) for m in mods})"
./.venv/bin/python -m playwright install --list
```

Expected outcome:

- The residual-process check should be empty before any new browser launch.
- `playwright`, `aiohttp`, `yaml`, and `rich` should all be available.
- Chromium should appear in the Playwright browser list.

If the residual-process check finds older `cancel_downloaded_likes.py`, `cookie_fetcher.py`, Playwright driver, or Chrome-for-Testing processes still using `config/playwright-like-cleanup-profile`, terminate them and rerun the check until it is empty. Do this again after any user interruption before reopening the browser.

## Config Checklist

Minimal fields for this workflow:

```yaml
link:
  - https://www.douyin.com/user/<sec_uid>

mode:
  - like

number:
  like: 0

like_cleanup:
  enabled: true
  headless: false
  persist_login: true
  profile_dir: ./config/playwright-like-cleanup-profile
  request_interval_ms: 1000
  wait_timeout_seconds: 600
```

Recommended cookie presence check:

```bash
./.venv/bin/python -c "from config import ConfigLoader; c=ConfigLoader('config.yml'); cookies=c.get_cookies(); wanted=['sessionid','sessionid_ss','sid_tt','sid_guard','uid_tt','uid_tt_ss','msToken','ttwid','odin_tt']; print({k: bool(str(cookies.get(k,'')).strip()) for k in wanted})"
```

## Cookie Refresh Flow

Use this when liked items unexpectedly return zero.

```bash
./.venv/bin/python tools/cookie_fetcher.py --config config.yml --include-all --profile-dir config/playwright-like-cleanup-profile
```

Operator behavior:

1. Confirm the residual-process check is empty.
2. Let the browser window open.
3. Ask the user to complete Douyin login in that browser.
4. Do not proceed automatically. Wait for the user to explicitly say login is complete.
5. Submit Enter to the waiting process.
6. Confirm `sessionid` and `sid_tt` now exist in `config.yml`.

## Main Run

```bash
./.venv/bin/douyin-dl -c config.yml
```

Interpretation:

- Early `0` total items with valid cookies can be legitimate if the account has no visible liked items.
- Nonzero total items means the like list is accessible and downloads should proceed.
- The unlike step only applies to items that downloaded successfully in `mode: like`.
- If unlike cleanup surfaces a login page, pause and wait for explicit user confirmation before resuming.
- After any interrupted run, clear residual processes before launching the browser again.

## Cleanup-Only Run

```bash
./.venv/bin/python tools/cancel_downloaded_likes.py -c config.yml
```

Useful variants:

```bash
./.venv/bin/python tools/cancel_downloaded_likes.py -c config.yml --limit 20
./.venv/bin/python tools/cancel_downloaded_likes.py -c config.yml --source db
./.venv/bin/python tools/cancel_downloaded_likes.py -c config.yml --aweme-id 123 --aweme-id 456
```

Always run the residual-process check first so only one cleanup process owns the Playwright profile at a time.

## Common Failure Patterns

### Like list returns zero

Check in this order:

1. `mode` is actually `like`.
2. Login cookies are present.
3. The user logged into the intended account.
4. The liked list is visible to that login.

### Browser cleanup cannot continue

Check in this order:

1. No residual `cancel_downloaded_likes.py`, `cookie_fetcher.py`, Playwright driver, or Chrome-for-Testing process is still attached to `config/playwright-like-cleanup-profile`.
2. `like_cleanup.headless` is `false`.
3. `profile_dir` is stable across runs.
4. The user completed any verification or login prompt in the browser.

### Downloads succeeded but some unlikes failed

Use the cleanup-only tool and retry failed `aweme_id` values explicitly.
