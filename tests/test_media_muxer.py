from pathlib import Path
from typing import Optional

from utils.media_muxer import MediaMuxer
import utils.media_muxer as media_muxer_module


class _FakeStream:
    def __init__(self, stream_type: str):
        self.type = stream_type


class _FakePacket:
    def __init__(self, pts=0, dts=0):
        self.pts = pts
        self.dts = dts
        self.stream = None


class _FakeInputContainer:
    def __init__(self, stream_type: str):
        self.streams = [_FakeStream(stream_type)]
        self._packet = _FakePacket()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def demux(self, _stream):
        return [self._packet, _FakePacket(pts=None, dts=None)]


class _FakeOutputContainer:
    def __init__(
        self, path: str, mode: Optional[str], container_format: Optional[str]
    ):
        self.path = Path(path)
        self.mode = mode
        self.container_format = container_format
        self.muxed_stream_types = []

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(b"muxed")
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def add_stream(self, template):
        return _FakeStream(template.type)

    def add_stream_from_template(self, template):
        return self.add_stream(template)

    def mux(self, packet):
        self.muxed_stream_types.append(packet.stream.type)


class _FakeAVModule:
    def __init__(self):
        self.open_calls = []
        self.output_container = None

    def open(
        self, path: str, mode: Optional[str] = None, format: Optional[str] = None
    ):
        self.open_calls.append((path, mode, format))
        if mode == "w":
            self.output_container = _FakeOutputContainer(path, mode, format)
            return self.output_container
        if path.endswith(".m4a"):
            return _FakeInputContainer("audio")
        return _FakeInputContainer("video")


def test_mux_mp4_with_ffmpeg_uses_stream_copy(tmp_path, monkeypatch):
    video_path = tmp_path / "video.mp4"
    audio_path = tmp_path / "audio.m4a"
    output_path = tmp_path / "merged.mp4"
    video_path.write_bytes(b"video")
    audio_path.write_bytes(b"audio")

    captured = {}

    def _fake_run(command, check, capture_output, text):
        captured["command"] = command
        captured["check"] = check
        captured["capture_output"] = capture_output
        captured["text"] = text
        tmp_output_path = Path(command[-1])
        tmp_output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_output_path.write_bytes(b"ffmpeg-muxed")

    monkeypatch.setattr(media_muxer_module.subprocess, "run", _fake_run)

    assert (
        MediaMuxer._mux_mp4_with_ffmpeg(
            "/opt/homebrew/bin/ffmpeg", video_path, audio_path, output_path
        )
        is True
    )
    assert output_path.exists()
    assert output_path.read_bytes() == b"ffmpeg-muxed"
    assert captured["command"] == [
        "/opt/homebrew/bin/ffmpeg",
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
        str(tmp_path / "merged.mux.tmp.mp4"),
    ]
    assert captured["check"] is True
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert not (tmp_path / "merged.mux.tmp.mp4").exists()


def test_mux_mp4_sync_uses_mp4_container_for_temp_output(tmp_path, monkeypatch):
    video_path = tmp_path / "video.mp4"
    audio_path = tmp_path / "audio.m4a"
    output_path = tmp_path / "merged.mp4"
    video_path.write_bytes(b"video")
    audio_path.write_bytes(b"audio")

    fake_av = _FakeAVModule()
    monkeypatch.setattr(media_muxer_module, "av", fake_av)

    assert MediaMuxer._mux_mp4_sync(video_path, audio_path, output_path) is True
    assert output_path.exists()
    assert output_path.read_bytes() == b"muxed"

    temp_output_calls = [call for call in fake_av.open_calls if call[1] == "w"]
    assert temp_output_calls == [
        (str(tmp_path / "merged.mux.tmp.mp4"), "w", "mp4")
    ]
    assert fake_av.output_container is not None
    assert fake_av.output_container.muxed_stream_types == ["video", "audio"]
    assert not (tmp_path / "merged.mux.tmp.mp4").exists()
