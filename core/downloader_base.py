import json
import re
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple, Union
from urllib.parse import urlparse

from auth import CookieManager
from config import ConfigLoader
from control import QueueManager, RateLimiter, RetryHandler
from core.api_client import DouyinAPIClient
from core.transcript_manager import TranscriptManager
from storage import Database, FileManager, MetadataHandler
from utils.logger import setup_logger
from utils.media_muxer import MediaMuxer
from utils.validators import sanitize_filename

logger = setup_logger("BaseDownloader")


class ProgressReporter(Protocol):
    def update_step(self, step: str, detail: str = "") -> None:
        ...

    def set_item_total(self, total: int, detail: str = "") -> None:
        ...

    def advance_item(self, status: str, detail: str = "") -> None:
        ...


class DownloadResult:
    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.skipped = 0
        self.success_aweme_ids: List[str] = []

    def __str__(self):
        return f"Total: {self.total}, Success: {self.success}, Failed: {self.failed}, Skipped: {self.skipped}"


class BaseDownloader(ABC):
    def __init__(
        self,
        config: ConfigLoader,
        api_client: DouyinAPIClient,
        file_manager: FileManager,
        cookie_manager: CookieManager,
        database: Optional[Database] = None,
        rate_limiter: Optional[RateLimiter] = None,
        retry_handler: Optional[RetryHandler] = None,
        queue_manager: Optional[QueueManager] = None,
        progress_reporter: Optional[ProgressReporter] = None,
    ):
        self.config = config
        self.api_client = api_client
        self.file_manager = file_manager
        self.cookie_manager = cookie_manager
        self.database = database
        self.rate_limiter = rate_limiter or RateLimiter()
        self.retry_handler = retry_handler or RetryHandler()
        thread_count = int(self.config.get("thread", 5) or 5)
        self.queue_manager = queue_manager or QueueManager(max_workers=thread_count)
        self.progress_reporter = progress_reporter
        self.metadata_handler = MetadataHandler()
        self.transcript_manager = TranscriptManager(
            self.config, self.file_manager, self.database
        )
        self.media_muxer = MediaMuxer()
        self._local_aweme_ids: Optional[set[str]] = None
        self._aweme_id_pattern = re.compile(r"(?<!\d)(\d{15,20})(?!\d)")
        self._local_media_suffixes = {
            ".mp4",
            ".jpg",
            ".jpeg",
            ".png",
            ".webp",
            ".gif",
            ".mp3",
            ".m4a",
        }
        # 控制终端错误日志量，避免进度条被大量日志打断后出现重复重绘。
        self._download_error_log_count = 0
        self._download_error_log_limit = 5

    def _progress_update_step(self, step: str, detail: str = "") -> None:
        if not self.progress_reporter:
            return
        try:
            self.progress_reporter.update_step(step, detail)
        except Exception as exc:
            logger.debug("Progress update_step failed: %s", exc)

    def _progress_set_item_total(self, total: int, detail: str = "") -> None:
        if not self.progress_reporter:
            return
        try:
            self.progress_reporter.set_item_total(total, detail)
        except Exception as exc:
            logger.debug("Progress set_item_total failed: %s", exc)

    def _progress_advance_item(self, status: str, detail: str = "") -> None:
        if not self.progress_reporter:
            return
        try:
            self.progress_reporter.advance_item(status, detail)
        except Exception as exc:
            logger.debug("Progress advance_item failed: %s", exc)

    def _log_download_error(self, log_fn, message: str) -> None:
        if self._download_error_log_count < self._download_error_log_limit:
            log_fn(message)
        elif self._download_error_log_count == self._download_error_log_limit:
            logger.error(
                "Too many download errors, suppressing further per-file logs..."
            )
        self._download_error_log_count += 1

    def _download_headers(self, user_agent: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "Referer": f"{self.api_client.BASE_URL}/",
            "Origin": self.api_client.BASE_URL,
            "Accept": "*/*",
        }

        headers["User-Agent"] = user_agent or self.api_client.headers.get(
            "User-Agent", ""
        )
        return headers

    @abstractmethod
    async def download(self, parsed_url: Dict[str, Any]) -> DownloadResult:
        pass

    async def _should_download(self, aweme_id: str) -> bool:
        in_local = self._is_locally_downloaded(aweme_id)
        in_db = False
        if self.database:
            in_db = await self.database.is_downloaded(aweme_id)

        if in_db and in_local:
            return False

        if in_db and not in_local:
            logger.info(
                "Aweme %s exists in database but media file not found locally, retry download",
                aweme_id,
            )
            return True

        if in_local:
            logger.info("Aweme %s already exists locally, skipping", aweme_id)
            return False

        return True

    def _is_locally_downloaded(self, aweme_id: str) -> bool:
        if not aweme_id:
            return False

        if self._local_aweme_ids is None:
            self._build_local_aweme_index()

        if self._local_aweme_ids is None:
            return False
        return aweme_id in self._local_aweme_ids

    def _build_local_aweme_index(self):
        base_path = self.file_manager.base_path
        aweme_ids: set[str] = set()

        if base_path.exists():
            for path in base_path.rglob("*"):
                if not path.is_file():
                    continue
                if path.suffix.lower() not in self._local_media_suffixes:
                    continue
                try:
                    if path.stat().st_size <= 0:
                        continue
                except OSError:
                    continue
                for match in self._aweme_id_pattern.finditer(path.name):
                    aweme_ids.add(match.group(1))

        self._local_aweme_ids = aweme_ids

    def _mark_local_aweme_downloaded(self, aweme_id: str):
        if not aweme_id:
            return

        if self._local_aweme_ids is None:
            self._local_aweme_ids = set()
        self._local_aweme_ids.add(aweme_id)

    def _filter_by_time(self, aweme_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        start_time = self.config.get("start_time")
        end_time = self.config.get("end_time")

        if not start_time and not end_time:
            return aweme_list

        start_ts = (
            int(datetime.strptime(start_time, "%Y-%m-%d").timestamp())
            if start_time
            else None
        )
        end_ts = (
            int(datetime.strptime(end_time, "%Y-%m-%d").timestamp())
            if end_time
            else None
        )

        filtered: List[Dict[str, Any]] = []
        for aweme in aweme_list:
            create_time = aweme.get("create_time", 0)
            if start_ts is not None and create_time < start_ts:
                continue
            if end_ts is not None and create_time > end_ts:
                continue
            filtered.append(aweme)

        return filtered

    def _limit_count(
        self, aweme_list: List[Dict[str, Any]], mode: str
    ) -> List[Dict[str, Any]]:
        number_config = self.config.get("number", {})
        limit = number_config.get(mode, 0)

        if limit > 0:
            return aweme_list[:limit]
        return aweme_list

    async def _download_aweme_assets(
        self,
        aweme_data: Dict[str, Any],
        author_name: str,
        mode: Optional[str] = None,
    ) -> bool:
        aweme_id = aweme_data.get("aweme_id")
        if not aweme_id:
            logger.error("Missing aweme_id in aweme data")
            return False

        desc = (aweme_data.get("desc", "no_title") or "").strip() or "no_title"
        publish_ts, publish_date = self._resolve_publish_time(
            aweme_data.get("create_time")
        )
        if not publish_date:
            publish_date = datetime.now().strftime("%Y-%m-%d")
            logger.warning(
                "Aweme %s missing/invalid create_time, fallback to current date %s",
                aweme_id,
                publish_date,
            )
        file_stem = sanitize_filename(f"{publish_date}_{desc}_{aweme_id}")

        save_dir = self.file_manager.get_save_path(
            author_name=author_name,
            mode=mode,
            aweme_title=desc,
            aweme_id=aweme_id,
            folderstyle=self.config.get("folderstyle", True),
            download_date=publish_date,
        )
        downloaded_files: List[Path] = []

        def _cleanup_partial_downloads() -> None:
            for path in reversed(downloaded_files):
                try:
                    path.unlink(missing_ok=True)
                except OSError as exc:
                    logger.warning(
                        "Failed removing partial download %s for aweme %s: %s",
                        path,
                        aweme_id,
                        exc,
                    )
            if (
                not self.config.get("folderstyle", True)
                or not save_dir.exists()
                or not save_dir.is_dir()
            ):
                return
            try:
                if not any(save_dir.iterdir()):
                    save_dir.rmdir()
            except OSError:
                return

        session = await self.api_client.get_session()
        video_path: Optional[Path] = None
        video_variant: Optional[str] = None

        media_type = self._detect_media_type(aweme_data)
        if media_type == "video":
            original_request = self._build_default_original_source_request(aweme_data)
            video_plan = self._build_video_download_plan(aweme_data)
            if not original_request and not video_plan:
                logger.error("No playable video URL found for aweme %s", aweme_id)
                return False

            video_path = save_dir / f"{file_stem}.mp4"
            if original_request:
                if await self._download_with_retry(
                    original_request[0],
                    video_path,
                    session,
                    headers=original_request[1],
                ):
                    video_variant = "play_default_original"
                else:
                    logger.warning(
                        "Default original source download failed for aweme %s, fallback to DASH/progressive",
                        aweme_id,
                    )

            if video_variant is None and video_plan is not None:
                if video_plan.get("kind") == "dash":
                    if not await self._download_dash_video(
                        video_plan, video_path, session, str(aweme_id)
                    ):
                        logger.warning(
                            "Original DASH download failed for aweme %s, fallback to progressive MP4",
                            aweme_id,
                        )
                        fallback_request = self._build_no_watermark_url(aweme_data)
                        if not fallback_request:
                            _cleanup_partial_downloads()
                            return False
                        if not await self._download_with_retry(
                            fallback_request[0],
                            video_path,
                            session,
                            headers=fallback_request[1],
                        ):
                            _cleanup_partial_downloads()
                            return False
                        video_variant = "progressive_mp4"
                    else:
                        video_variant = "dash_original"
                else:
                    video_url = str(video_plan.get("video_url") or "")
                    video_headers = video_plan.get("video_headers")
                    if not await self._download_with_retry(
                        video_url, video_path, session, headers=video_headers
                    ):
                        _cleanup_partial_downloads()
                        return False
                    video_variant = "progressive_mp4"

            downloaded_files.append(video_path)

            if self.config.get("cover"):
                cover_url = self._extract_first_url(
                    aweme_data.get("video", {}).get("cover")
                )
                if cover_url:
                    cover_path = save_dir / f"{file_stem}_cover.jpg"
                    if await self._download_with_retry(
                        cover_url,
                        cover_path,
                        session,
                        headers=self._download_headers(),
                        optional=True,
                    ):
                        downloaded_files.append(cover_path)

            if self.config.get("music"):
                music_url = self._extract_first_url(
                    aweme_data.get("music", {}).get("play_url")
                )
                if music_url:
                    music_path = save_dir / f"{file_stem}_music.mp3"
                    if await self._download_with_retry(
                        music_url,
                        music_path,
                        session,
                        headers=self._download_headers(),
                        optional=True,
                    ):
                        downloaded_files.append(music_path)

        elif media_type == "gallery":
            image_url_candidates = self._collect_image_url_candidates(aweme_data)
            image_live_urls = self._collect_image_live_urls(aweme_data)
            logger.info(
                "Gallery aweme %s: %d image(s), %d live photo(s)",
                aweme_id,
                len(image_url_candidates),
                len(image_live_urls),
            )
            if not image_url_candidates and not image_live_urls:
                logger.error(
                    "No gallery assets found for aweme %s (aweme_type=%s, "
                    "has image_post_info=%s, has images=%s)",
                    aweme_id,
                    aweme_data.get("aweme_type"),
                    "image_post_info" in aweme_data,
                    "images" in aweme_data,
                )
                return False

            for index, image_candidates in enumerate(image_url_candidates, start=1):
                suffix = self._infer_image_extension(image_candidates[0])
                image_path = save_dir / f"{file_stem}_{index}{suffix}"
                download_result = await self._download_from_url_candidates(
                    image_candidates,
                    image_path,
                    session,
                    headers=self._download_headers(),
                    prefer_response_content_type=True,
                    return_saved_path=True,
                )
                if not download_result:
                    logger.error(
                        f"Failed downloading image {index} for aweme {aweme_id}"
                    )
                    _cleanup_partial_downloads()
                    return False
                downloaded_files.append(
                    download_result if isinstance(download_result, Path) else image_path
                )

            for index, live_url in enumerate(image_live_urls, start=1):
                suffix = Path(urlparse(live_url).path).suffix or ".mp4"
                live_path = save_dir / f"{file_stem}_live_{index}{suffix}"
                success = await self._download_with_retry(
                    live_url,
                    live_path,
                    session,
                    headers=self._download_headers(),
                )
                if not success:
                    logger.error(
                        f"Failed downloading live image {index} for aweme {aweme_id}"
                    )
                    _cleanup_partial_downloads()
                    return False
                downloaded_files.append(live_path)
        else:
            logger.error("Unsupported media type for aweme %s: %s", aweme_id, media_type)
            return False

        if self.config.get("avatar"):
            author = aweme_data.get("author", {})
            avatar_url = self._extract_first_url(author.get("avatar_larger"))
            if avatar_url:
                avatar_path = save_dir / f"{file_stem}_avatar.jpg"
                if await self._download_with_retry(
                    avatar_url,
                    avatar_path,
                    session,
                    headers=self._download_headers(),
                    optional=True,
                ):
                    downloaded_files.append(avatar_path)

        if self.config.get("json"):
            json_path = save_dir / f"{file_stem}_data.json"
            if await self.metadata_handler.save_metadata(aweme_data, json_path):
                downloaded_files.append(json_path)

        author = aweme_data.get("author", {})
        if self.database:
            metadata_json = json.dumps(aweme_data, ensure_ascii=False)
            await self.database.add_aweme(
                {
                    "aweme_id": aweme_id,
                    "aweme_type": media_type,
                    "title": desc,
                    "author_id": author.get("uid"),
                    "author_name": author.get("nickname", author_name),
                    "create_time": aweme_data.get("create_time"),
                    "file_path": str(save_dir),
                    "metadata": metadata_json,
                }
            )

        manifest_record = {
            "date": publish_date,
            "aweme_id": aweme_id,
            "author_name": author.get("nickname", author_name),
            "desc": desc,
            "media_type": media_type,
            "tags": self._extract_tags(aweme_data),
            "file_names": [path.name for path in downloaded_files],
            "file_paths": [self._to_manifest_path(path) for path in downloaded_files],
        }
        if video_variant:
            manifest_record["download_variant"] = video_variant
        if publish_ts:
            manifest_record["publish_timestamp"] = publish_ts
        await self.metadata_handler.append_download_manifest(
            self.file_manager.base_path, manifest_record
        )

        if media_type == "video" and video_path is not None:
            transcript_result = await self.transcript_manager.process_video(
                video_path, aweme_id=aweme_id
            )
            transcript_status = transcript_result.get("status")
            if transcript_status == "skipped":
                logger.info(
                    "Transcript skipped for aweme %s: %s",
                    aweme_id,
                    transcript_result.get("reason", "unknown"),
                )
            elif transcript_status == "failed":
                logger.warning(
                    "Transcript failed for aweme %s: %s",
                    aweme_id,
                    transcript_result.get("error", "unknown"),
                )

        self._mark_local_aweme_downloaded(aweme_id)
        logger.info("Downloaded %s: %s (%s)", media_type, desc, aweme_id)
        return True

    async def _download_with_retry(
        self,
        url: str,
        save_path: Path,
        session,
        *,
        headers: Optional[Dict[str, str]] = None,
        optional: bool = False,
        prefer_response_content_type: bool = False,
        return_saved_path: bool = False,
    ) -> Union[bool, Path]:
        async def _task():
            download_result = await self.file_manager.download_file(
                url,
                save_path,
                session,
                headers=headers,
                proxy=getattr(self.api_client, "proxy", None),
                prefer_response_content_type=prefer_response_content_type,
                return_saved_path=return_saved_path,
            )
            if not download_result:
                raise RuntimeError(f"Download failed for {url}")
            return download_result

        try:
            return await self.retry_handler.execute_with_retry(_task)
        except Exception as error:
            log_fn = logger.warning if optional else logger.error
            self._log_download_error(
                log_fn,
                f"Download error for {save_path.name}: {error}",
            )
            return False

    async def _download_from_url_candidates(
        self,
        url_candidates: List[str],
        save_path: Path,
        session,
        *,
        headers: Optional[Dict[str, str]] = None,
        optional: bool = False,
        prefer_response_content_type: bool = False,
        return_saved_path: bool = False,
    ) -> Union[bool, Path]:
        normalized_candidates: List[str] = []
        seen: set[str] = set()
        for candidate in url_candidates or []:
            candidate_str = str(candidate or "").strip()
            if not candidate_str or candidate_str in seen:
                continue
            seen.add(candidate_str)
            normalized_candidates.append(candidate_str)

        if not normalized_candidates:
            return False

        log_fn = logger.warning if optional else logger.error
        total_candidates = len(normalized_candidates)
        last_candidate = normalized_candidates[-1]
        for index, candidate in enumerate(normalized_candidates, start=1):
            download_result = await self.file_manager.download_file(
                candidate,
                save_path,
                session,
                headers=headers,
                proxy=getattr(self.api_client, "proxy", None),
                prefer_response_content_type=prefer_response_content_type,
                return_saved_path=return_saved_path,
            )
            if download_result:
                return download_result
            if index < total_candidates:
                logger.warning(
                    "Download candidate failed for %s, trying next source (%s/%s)",
                    save_path.name,
                    index,
                    total_candidates,
                )

        self._log_download_error(
            log_fn,
            (
                f"Download error for {save_path.name}: all "
                f"{total_candidates} candidate URLs failed; last={last_candidate}"
            ),
        )
        return False

    async def _download_dash_video(
        self,
        video_plan: Dict[str, Any],
        video_path: Path,
        session,
        aweme_id: str,
    ) -> bool:
        temp_video_path = video_path.with_name(f"{video_path.stem}.__dash_video__.mp4")
        temp_audio_path = video_path.with_name(f"{video_path.stem}.__dash_audio__.m4a")

        for temp_path in (temp_video_path, temp_audio_path):
            temp_path.unlink(missing_ok=True)

        try:
            if not await self._download_with_retry(
                str(video_plan.get("video_url") or ""),
                temp_video_path,
                session,
                headers=video_plan.get("video_headers"),
            ):
                return False

            if not await self._download_with_retry(
                str(video_plan.get("audio_url") or ""),
                temp_audio_path,
                session,
                headers=video_plan.get("audio_headers"),
            ):
                return False

            if not await self.media_muxer.mux_mp4(
                temp_video_path, temp_audio_path, video_path
            ):
                logger.error("Failed to mux original DASH video for aweme %s", aweme_id)
                return False

            logger.info("Downloaded original DASH video for aweme %s", aweme_id)
            return True
        finally:
            temp_video_path.unlink(missing_ok=True)
            temp_audio_path.unlink(missing_ok=True)

    # aweme_type codes that indicate image/note content
    _GALLERY_AWEME_TYPES = {2, 68, 150}

    def _detect_media_type(self, aweme_data: Dict[str, Any]) -> str:
        if (
            aweme_data.get("image_post_info")
            or aweme_data.get("images")
            or aweme_data.get("image_list")
        ):
            return "gallery"
        aweme_type = aweme_data.get("aweme_type")
        if isinstance(aweme_type, int) and aweme_type in self._GALLERY_AWEME_TYPES:
            logger.info(
                "Detected gallery via aweme_type=%s for aweme %s",
                aweme_type,
                aweme_data.get("aweme_id"),
            )
            return "gallery"
        return "video"

    def _build_no_watermark_url(
        self, aweme_data: Dict[str, Any]
    ) -> Optional[Tuple[str, Dict[str, str]]]:
        video = aweme_data.get("video", {})
        play_addr = self._select_best_video_play_addr(video)
        request = self._prepare_download_request(play_addr.get("url_list") or [])
        if request:
            return request

        uri = (
            play_addr.get("uri")
            or video.get("vid")
            or video.get("download_addr", {}).get("uri")
        )
        if uri:
            params = {
                "video_id": uri,
                "ratio": self._infer_video_ratio(play_addr),
                "line": "0",
                "is_play_url": "1",
                "watermark": "0",
                "source": "PackSourceEnum_PUBLISH",
            }
            signed_url, ua = self.api_client.build_signed_path(
                "/aweme/v1/play/", params
            )
            return signed_url, self._download_headers(user_agent=ua)

        return None

    def _build_default_original_source_request(
        self, aweme_data: Dict[str, Any]
    ) -> Optional[Tuple[str, Dict[str, str]]]:
        video = aweme_data.get("video", {})
        if not isinstance(video, dict):
            return None

        uri = self._extract_preferred_video_uri(video)
        if not uri:
            return None

        params = {
            "video_id": uri,
            "ratio": "default",
            "line": "0",
            "is_play_url": "1",
            "watermark": "0",
            "source": "PackSourceEnum_PUBLISH",
        }
        signed_url, ua = self.api_client.build_signed_path("/aweme/v1/play/", params)
        return signed_url, self._download_headers(user_agent=ua)

    @staticmethod
    def _coerce_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _build_video_download_plan(
        self, aweme_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        video = aweme_data.get("video", {})
        if isinstance(video, dict) and self.media_muxer.is_available():
            dash_bundle = self._select_best_dash_video_bundle(video)
            if dash_bundle:
                video_request = self._prepare_direct_download_request(
                    dash_bundle.get("video_urls") or []
                )
                audio_request = self._prepare_direct_download_request(
                    dash_bundle.get("audio_urls") or []
                )
                if video_request and audio_request:
                    return {
                        "kind": "dash",
                        "video_url": video_request[0],
                        "video_headers": video_request[1],
                        "audio_url": audio_request[0],
                        "audio_headers": audio_request[1],
                    }

        direct_request = self._build_no_watermark_url(aweme_data)
        if not direct_request:
            return None

        return {
            "kind": "direct",
            "video_url": direct_request[0],
            "video_headers": direct_request[1],
        }

    def _extract_preferred_video_uri(self, video: Dict[str, Any]) -> str:
        play_addr = video.get("play_addr")
        play_addr_265 = video.get("play_addr_265")
        play_addr_h264 = video.get("play_addr_h264")
        best_play_addr = self._select_best_video_play_addr(video)

        for candidate in (
            play_addr.get("uri") if isinstance(play_addr, dict) else None,
            play_addr_265.get("uri") if isinstance(play_addr_265, dict) else None,
            play_addr_h264.get("uri") if isinstance(play_addr_h264, dict) else None,
            best_play_addr.get("uri") if isinstance(best_play_addr, dict) else None,
            video.get("vid"),
            (video.get("download_addr") or {}).get("uri")
            if isinstance(video.get("download_addr"), dict)
            else None,
        ):
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return ""

    def _prepare_download_request(
        self, url_candidates: List[str]
    ) -> Optional[Tuple[str, Dict[str, str]]]:
        normalized_candidates = [c for c in url_candidates if c]
        normalized_candidates.sort(key=lambda u: 0 if "watermark=0" in u else 1)

        fallback_candidate: Optional[Tuple[str, Dict[str, str]]] = None

        for candidate in normalized_candidates:
            parsed = urlparse(candidate)
            headers = self._download_headers()

            if parsed.netloc.endswith("douyin.com"):
                if "X-Bogus=" not in candidate:
                    signed_url, ua = self.api_client.sign_url(candidate)
                    headers = self._download_headers(user_agent=ua)
                    return signed_url, headers
                return candidate, headers

            if fallback_candidate is None:
                fallback_candidate = (candidate, headers)

        return fallback_candidate

    def _prepare_direct_download_request(
        self, url_candidates: List[str]
    ) -> Optional[Tuple[str, Dict[str, str]]]:
        normalized_candidates = [c for c in url_candidates if c]
        normalized_candidates.sort(key=lambda u: 0 if "watermark=0" in u else 1)

        fallback_candidate: Optional[Tuple[str, Dict[str, str]]] = None

        for candidate in normalized_candidates:
            parsed = urlparse(candidate)
            headers = self._download_headers()

            if not parsed.netloc.endswith("douyin.com"):
                return candidate, headers

            if fallback_candidate is None:
                if "X-Bogus=" not in candidate:
                    signed_url, ua = self.api_client.sign_url(candidate)
                    headers = self._download_headers(user_agent=ua)
                    fallback_candidate = (signed_url, headers)
                else:
                    fallback_candidate = (candidate, headers)

        return fallback_candidate

    def _select_best_dash_video_bundle(
        self, video: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        audio_variants = self._collect_dash_audio_variants(video)
        if not audio_variants:
            return None

        candidates: List[Tuple[Tuple[int, int, int, int, int, int], Dict[str, Any]]] = []

        for variant in video.get("bit_rate") or []:
            if not isinstance(variant, dict):
                continue
            if str(variant.get("format") or "").lower() != "dash":
                continue

            play_addr = variant.get("play_addr")
            if not isinstance(play_addr, dict):
                continue

            video_urls = self._extract_url_candidates(play_addr)
            if not video_urls:
                continue

            matched_audio = self._match_dash_audio_variant(variant, audio_variants)
            if not matched_audio:
                continue

            candidates.append(
                (
                    self._score_video_variant(variant, play_addr),
                    {
                        "variant": variant,
                        "play_addr": play_addr,
                        "video_urls": video_urls,
                        "audio_urls": matched_audio["audio_urls"],
                    },
                )
            )

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _collect_dash_audio_variants(
        self, video: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        variants: List[Dict[str, Any]] = []

        for item in video.get("bit_rate_audio") or []:
            if not isinstance(item, dict):
                continue

            audio_meta = item.get("audio_meta")
            if not isinstance(audio_meta, dict):
                continue

            audio_urls = self._extract_url_candidates(
                audio_meta.get("url_list"), audio_meta
            )
            if not audio_urls:
                continue

            variants.append(
                {
                    "item": item,
                    "audio_meta": audio_meta,
                    "audio_urls": audio_urls,
                }
            )

        variants.sort(key=self._score_audio_variant, reverse=True)
        return variants

    def _match_dash_audio_variant(
        self, variant: Dict[str, Any], audio_variants: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        video_extra = self._parse_json_dict(variant.get("video_extra"))
        target_audio_file_id = str(video_extra.get("audio_file_id") or "").strip()

        if target_audio_file_id:
            for audio_variant in audio_variants:
                audio_meta = audio_variant.get("audio_meta") or {}
                if str(audio_meta.get("file_id") or "").strip() == target_audio_file_id:
                    return audio_variant

        return audio_variants[0] if audio_variants else None

    def _score_audio_variant(self, entry: Dict[str, Any]) -> Tuple[int, int, int]:
        audio_meta = entry.get("audio_meta") or {}
        item = entry.get("item") or {}
        return (
            self._coerce_int(audio_meta.get("bitrate")),
            self._coerce_int(audio_meta.get("size")),
            self._coerce_int(item.get("audio_quality")),
        )

    @staticmethod
    def _parse_json_dict(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if not isinstance(value, str) or not value.strip():
            return {}
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _select_best_video_play_addr(self, video: Dict[str, Any]) -> Dict[str, Any]:
        candidates: List[Tuple[Tuple[int, int, int, int, int, int], Dict[str, Any]]] = []

        for variant in video.get("bit_rate") or []:
            if not isinstance(variant, dict):
                continue
            if str(variant.get("format") or "").lower() == "dash":
                # dash 流通常需要额外音轨拼接；这里优先选择可直接下载成单个 mp4 的最高质量版本。
                continue
            play_addr = variant.get("play_addr")
            if not isinstance(play_addr, dict):
                continue
            if not (play_addr.get("url_list") or []):
                continue
            candidates.append((self._score_video_variant(variant, play_addr), play_addr))

        if candidates:
            candidates.sort(key=lambda item: item[0], reverse=True)
            return candidates[0][1]

        play_addr = video.get("play_addr", {})
        return play_addr if isinstance(play_addr, dict) else {}

    def _score_video_variant(
        self, variant: Dict[str, Any], play_addr: Dict[str, Any]
    ) -> Tuple[int, int, int, int, int, int]:
        return (
            self._coerce_int(play_addr.get("height")),
            self._coerce_int(play_addr.get("width")),
            self._coerce_int(variant.get("bit_rate")),
            self._coerce_int(play_addr.get("data_size")),
            self._coerce_int(variant.get("quality_type")),
            1 if variant.get("is_h265") else 0,
        )

    def _infer_video_ratio(self, play_addr: Dict[str, Any]) -> str:
        height = self._coerce_int(play_addr.get("height"))
        if height >= 2160:
            return "2160p"
        if height >= 1440:
            return "1440p"
        if height >= 1080:
            return "1080p"
        if height >= 720:
            return "720p"
        return "540p"

    def _collect_image_urls(self, aweme_data: Dict[str, Any]) -> List[str]:
        image_url_candidates = self._collect_image_url_candidates(aweme_data)
        image_urls = [candidates[0] for candidates in image_url_candidates if candidates]
        if not image_urls:
            logger.warning(
                "No image URLs extracted for aweme %s; gallery items count=%d",
                aweme_data.get("aweme_id"),
                len(self._iter_gallery_items(aweme_data)),
            )
        return self._deduplicate_urls(image_urls)

    def _collect_image_url_candidates(self, aweme_data: Dict[str, Any]) -> List[List[str]]:
        gallery_items = self._iter_gallery_items(aweme_data)
        image_candidates: List[List[str]] = []
        for item in gallery_items:
            if not isinstance(item, dict):
                continue
            candidates = self._collect_gallery_item_image_candidates(item)
            if candidates:
                image_candidates.append(candidates)
        return image_candidates

    def _collect_gallery_item_image_candidates(self, item: Dict[str, Any]) -> List[str]:
        candidates: List[str] = []
        seen: set[str] = set()
        for source in (
            item.get("download_url"),
            item.get("download_addr"),
            item.get("download_url_list"),
            item,
            item.get("display_image"),
            item.get("owner_watermark_image"),
        ):
            for candidate in self._extract_url_candidates(source):
                if not candidate or candidate in seen:
                    continue
                seen.add(candidate)
                candidates.append(candidate)
        return candidates

    def _collect_image_live_urls(self, aweme_data: Dict[str, Any]) -> List[str]:
        live_urls: List[str] = []
        for item in self._iter_gallery_items(aweme_data):
            if not isinstance(item, dict):
                continue
            video = item.get("video") if isinstance(item.get("video"), dict) else {}
            live_url = self._pick_first_media_url(
                video.get("play_addr"),
                video.get("download_addr"),
                item.get("video_play_addr"),
                item.get("video_download_addr"),
            )
            if live_url:
                live_urls.append(live_url)
        return self._deduplicate_urls(live_urls)

    @staticmethod
    def _iter_gallery_items(aweme_data: Dict[str, Any]) -> List[Any]:
        image_post = aweme_data.get("image_post_info")
        if isinstance(image_post, dict):
            for key in ("images", "image_list"):
                candidate = image_post.get(key)
                if isinstance(candidate, list) and candidate:
                    return candidate
        images = aweme_data.get("images") or aweme_data.get("image_list") or []
        if isinstance(images, list):
            return images
        return []

    @staticmethod
    def _deduplicate_urls(urls: List[str]) -> List[str]:
        deduped: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not url or url in seen:
                continue
            seen.add(url)
            deduped.append(url)
        return deduped

    @staticmethod
    def _pick_first_media_url(*sources: Any) -> Optional[str]:
        for source in sources:
            candidate = BaseDownloader._extract_first_url(source)
            if candidate:
                return candidate
        return None

    @staticmethod
    def _extract_first_url(source: Any) -> Optional[str]:
        candidates = BaseDownloader._extract_url_candidates(source)
        return candidates[0] if candidates else None

    @staticmethod
    def _extract_url_candidates(*sources: Any) -> List[str]:
        candidates: List[str] = []
        seen: set[str] = set()

        def _append(url: Any) -> None:
            if not isinstance(url, str) or not url:
                return
            if url in seen:
                return
            seen.add(url)
            candidates.append(url)

        def _collect(source: Any) -> None:
            if isinstance(source, dict):
                url_list = source.get("url_list")
                if isinstance(url_list, list):
                    for item in url_list:
                        _collect(item)
                elif isinstance(url_list, dict):
                    for key in ("main_url", "backup_url", "fallback_url"):
                        _collect(url_list.get(key))

                for key in ("main_url", "backup_url", "fallback_url", "url"):
                    _append(source.get(key))
                return

            if isinstance(source, list):
                for item in source:
                    _collect(item)
                return

            _append(source)

        for source in sources:
            _collect(source)

        return candidates

    @staticmethod
    def _infer_image_extension(image_url: str) -> str:
        allowed_exts = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
        if not image_url:
            return ".jpg"

        image_path = (urlparse(image_url).path or "").lower()
        raw_suffix = Path(image_path).suffix.lower()
        if raw_suffix in allowed_exts:
            return raw_suffix

        matches = re.findall(r"\.(?:jpe?g|png|webp|gif)(?=[^a-z0-9]|$)", image_path)
        if matches:
            return matches[-1].lower()

        return ".jpg"

    @staticmethod
    def _resolve_publish_time(create_time: Any) -> Tuple[Optional[int], str]:
        if create_time in (None, ""):
            return None, ""

        try:
            publish_ts = int(create_time)
            if publish_ts <= 0:
                return None, ""
            return publish_ts, datetime.fromtimestamp(publish_ts).strftime("%Y-%m-%d")
        except (TypeError, ValueError, OSError, OverflowError):
            return None, ""

    @staticmethod
    def _extract_tags(aweme_data: Dict[str, Any]) -> List[str]:
        tags: List[str] = []

        def _append_tag(raw_tag: Any):
            if not raw_tag:
                return
            normalized_tag = str(raw_tag).strip().lstrip("#")
            if normalized_tag and normalized_tag not in tags:
                tags.append(normalized_tag)

        for item in aweme_data.get("text_extra") or []:
            if not isinstance(item, dict):
                continue
            _append_tag(item.get("hashtag_name"))
            _append_tag(item.get("tag_name"))

        for item in aweme_data.get("cha_list") or []:
            if not isinstance(item, dict):
                continue
            _append_tag(item.get("cha_name"))
            _append_tag(item.get("name"))

        desc = aweme_data.get("desc") or ""
        for hashtag in re.findall(r"#([^\s#]+)", desc):
            _append_tag(hashtag)

        return tags

    def _to_manifest_path(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.file_manager.base_path))
        except ValueError:
            return str(path)
