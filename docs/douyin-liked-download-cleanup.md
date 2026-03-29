# 抖音点赞下载并自动取消点赞

这个项目已经支持完整流程：

1. 用 `mode: like` 拉取点赞作品。
2. 下载成功后，用 `like_cleanup` 通过浏览器自动取消点赞。

## 推荐配置

```yaml
link:
  - https://www.douyin.com/user/<sec_uid>

path: ~/Downloads/douyin/

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

## 关键 Cookie

基础接口通常至少需要这些键：

- `msToken`
- `ttwid`
- `odin_tt`
- `passport_csrf_token`

点赞列表和取消点赞建议同时具备这些登录态键：

- `sessionid`
- `sessionid_ss`
- `sid_tt`
- `sid_guard`
- `uid_tt`
- `uid_tt_ss`

## 刷新 Cookie

当点赞列表意外返回 `0` 条时，先刷新登录态：

```bash
./.venv/bin/python tools/cookie_fetcher.py --config config.yml --include-all --profile-dir config/playwright-like-cleanup-profile
```

浏览器打开后登录抖音，回到终端按 Enter，程序会把完整 Cookie 写回 `config.yml`。

如果是 AI 在代你操作，必须先等你明确确认“已登录”后再按 Enter，不能只因为浏览器已经打开就继续下一步。

## 执行主流程

```bash
./.venv/bin/douyin-dl -c config.yml
```

运行时会依次经历：

1. 拉取点赞列表
2. 下载作品
3. 对下载成功的作品执行取消点赞

如果取消点赞阶段再次弹出登录页，也应该先完成登录并确认成功，再继续执行。

## 只补跑取消点赞

如果作品之前已经下载好了，只想补跑取消点赞：

```bash
./.venv/bin/python tools/cancel_downloaded_likes.py -c config.yml
```

常用参数：

- `--limit 20`
- `--source db`
- `--source manifest`
- `--aweme-id <id>`

## 常见问题

### 点赞列表还是 0 条

按这个顺序检查：

1. `config.yml` 里 `mode` 是否为 `like`
2. 当前登录账号是否正确
3. `sessionid` / `sid_tt` 等登录态 Cookie 是否存在
4. 当前账号是否真的有可见点赞作品

### 取消点赞阶段卡住

按这个顺序检查：

1. `like_cleanup.headless` 是否为 `false`
2. 浏览器里是否出现登录或验证页面
3. `profile_dir` 是否稳定复用同一个目录
