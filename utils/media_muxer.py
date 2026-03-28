import asyncio
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from utils.logger import setup_logger

logger = setup_logger("MediaMuxer")

try:
    import av
except Exception as exc:  # pragma: no cover - optional dependency fallback
    av = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


class MediaMuxer:
    def __init__(self):
        self._ffmpeg_path = shutil.which("ffmpeg")
        if self._ffmpeg_path is None:
            for candidate in ("/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg"):
                if Path(candidate).exists():
                    self._ffmpeg_path = candidate
                    break

    def is_available(self) -> bool:
        return self._ffmpeg_path is not None or av is not None

    def availability_error(self) -> Optional[str]:
        if self._ffmpeg_path is not None:
            return None
        return str(_IMPORT_ERROR) if _IMPORT_ERROR else None

    async def mux_mp4(
        self, video_path: Path, audio_path: Path, output_path: Path
    ) -> bool:
        if self._ffmpeg_path is not None:
            try:
                return await asyncio.to_thread(
                    self._mux_mp4_with_ffmpeg,
                    self._ffmpeg_path,
                    video_path,
                    audio_path,
                    output_path,
                )
            except Exception as exc:
                logger.error(
                    "ffmpeg stream-copy mux failed for %s with %s: %s",
                    video_path,
                    audio_path,
                    exc,
                )
                return False

        if av is None:
            logger.error("PyAV is not available: %s", self.availability_error())
            return False

        try:
            return await asyncio.to_thread(
                self._mux_mp4_sync, video_path, audio_path, output_path
            )
        except Exception as exc:
            logger.error("Failed to mux %s with %s: %s", video_path, audio_path, exc)
            return False

    @staticmethod
    def _tmp_output_path(output_path: Path) -> Path:
        output_suffix = output_path.suffix or ".mp4"
        return output_path.with_name(f"{output_path.stem}.mux.tmp{output_suffix}")

    @staticmethod
    def _mux_mp4_with_ffmpeg(
        ffmpeg_path: str, video_path: Path, audio_path: Path, output_path: Path
    ) -> bool:
        tmp_output_path = MediaMuxer._tmp_output_path(output_path)
        tmp_output_path.unlink(missing_ok=True)
        tmp_output_path.parent.mkdir(parents=True, exist_ok=True)

        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(video_path),
            "-i",
            str(audio_path),
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(tmp_output_path),
        ]

        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            os.replace(str(tmp_output_path), str(output_path))
            return True
        except Exception:
            tmp_output_path.unlink(missing_ok=True)
            raise

    @staticmethod
    def _mux_mp4_sync(video_path: Path, audio_path: Path, output_path: Path) -> bool:
        tmp_output_path = MediaMuxer._tmp_output_path(output_path)
        tmp_output_path.unlink(missing_ok=True)
        tmp_output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with av.open(str(video_path)) as video_input:
                with av.open(str(audio_path)) as audio_input:
                    # PyAV cannot infer the mux target from a trailing `.tmp` suffix.
                    # Keep an `.mp4` extension and set the container explicitly so
                    # DASH "original video" merges do not fail and fall back.
                    with av.open(str(tmp_output_path), mode="w", format="mp4") as output:
                        video_stream_in = next(
                            (stream for stream in video_input.streams if stream.type == "video"),
                            None,
                        )
                        audio_stream_in = next(
                            (stream for stream in audio_input.streams if stream.type == "audio"),
                            None,
                        )

                        if video_stream_in is None:
                            raise RuntimeError(f"No video stream found in {video_path}")
                        if audio_stream_in is None:
                            raise RuntimeError(f"No audio stream found in {audio_path}")

                        if hasattr(output, "add_stream_from_template"):
                            video_stream_out = output.add_stream_from_template(
                                video_stream_in
                            )
                            audio_stream_out = output.add_stream_from_template(
                                audio_stream_in
                            )
                        else:
                            video_stream_out = output.add_stream(template=video_stream_in)
                            audio_stream_out = output.add_stream(template=audio_stream_in)

                        for packet in video_input.demux(video_stream_in):
                            if packet.dts is None and packet.pts is None:
                                continue
                            packet.stream = video_stream_out
                            output.mux(packet)

                        for packet in audio_input.demux(audio_stream_in):
                            if packet.dts is None and packet.pts is None:
                                continue
                            packet.stream = audio_stream_out
                            output.mux(packet)

            os.replace(str(tmp_output_path), str(output_path))
            return True
        except Exception:
            tmp_output_path.unlink(missing_ok=True)
            raise
