"""Utility functions for compressing images and videos."""

import asyncio
import logging
import platform
import tempfile
from pathlib import Path

from PIL import Image

from utils import get_file_size, remove_file

logger = logging.getLogger(__name__)

MIN_VIDEO_BITRATE_KBPS = 10
MIN_AUDIO_BITRATE_KBPS = 8


async def get_video_duration(file_path: Path) -> float | None:
    """Retrieve video duration in seconds using ffprobe."""
    # fmt: off
    cmd = [
        "ffprobe",
        "-v","error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.error(
            "ffprobe failed for %s (return code %d): %s",
            file_path,
            proc.returncode,
            stderr.decode().strip(),
        )
        return None
    try:
        return float(stdout.strip())
    except ValueError:
        logger.exception("Invalid duration for %s", file_path)
        return None


def allocate_bitrates(target_size: int, duration: float) -> tuple[float, float]:
    """Automatically allocate bitrates for video and audio to meet the target file size.

    :param target_size: Target file size in bytes.
    :param duration: Video duration in seconds.
    :return: Tuple of (video_bitrate_bps, audio_bitrate_bps)
    """
    target_size_bits = target_size * 8
    total_bitrate_total_bps = target_size_bits / duration

    audio_standard_bps = 128_000  # 128kbps
    audio_min_bps = 8_000  # 8kbps for very low quality
    video_min_bps = 10_000  # 10kbps for very low quality

    if total_bitrate_total_bps >= video_min_bps + audio_standard_bps:
        audio_bps = audio_standard_bps
        video_bps = total_bitrate_total_bps - audio_bps
    elif total_bitrate_total_bps >= video_min_bps + audio_min_bps:
        video_bps = video_min_bps
        audio_bps = total_bitrate_total_bps - video_bps
    else:
        total_min_bps = video_min_bps + audio_min_bps
        if total_min_bps > 0:
            proportion_video = video_min_bps / total_min_bps
            proportion_audio = audio_min_bps / total_min_bps
            video_bps = total_bitrate_total_bps * proportion_video
            audio_bps = total_bitrate_total_bps * proportion_audio
        else:
            logger.warning(
                "Minimum bitrate sum to zero, cannot allocate proportionally.",
            )
            video_bps = total_bitrate_total_bps * 0.8
            audio_bps = total_bitrate_total_bps * 0.2

    video_bps = max(video_bps, 0)
    audio_bps = max(audio_bps, 0)

    return video_bps, audio_bps


async def compress_video(
    original_path: Path,
    target_size: int,
    output_container: str | None = None,
    video_codec: str | None = None,
    audio_codec: str | None = None,
) -> Path:
    """Compress a video to a target size using two-pass encoding.

    Args:
        original_path: Path to the original video file.
        target_size: Target file size in bytes.
        output_container: Desired output container (e.g., "mp4", "webm").
                          If None, chosen based on codecs or defaults.
        video_codec: Desired video codec (e.g., "libx264", "libvpx-vp9").
                     Defaults to "libx264".
        audio_codec: Desired audio codec (e.g., "aac", "libopus").
                     Defaults to "aac".

    Returns:
        Path to the compressed video file.

    Raises:
        ValueError: If video duration cannot be obtained or is invalid.
        RuntimeError: If FFmpeg processing fails.

    """
    """Compress a video to a target size using two-pass encoding."""
    duration = await get_video_duration(original_path)
    target_size_mb = target_size / (1024 * 1024)
    target_size = int(
        target_size * 0.95,
    )  # Reduce target size by 5% to be less than target
    if duration is None or duration < 0:
        msg = f"Could not get duration for {original_path}"
        raise ValueError(msg)

    # Allocate bitrates
    video_bps, audio_bps = allocate_bitrates(target_size, duration)
    video_bitrate_kbps = max(int(video_bps / 1000), MIN_VIDEO_BITRATE_KBPS)
    audio_bitrate_kbps = max(int(audio_bps / 1000), MIN_AUDIO_BITRATE_KBPS)
    effective_vcodec = video_codec or "libx264"
    effective_acodec = audio_codec or "aac"
    effective_pix_fmt = None
    if output_container:
        effective_container_format = output_container.lower()
    elif effective_vcodec in ("libvpx-vp9", "vp9", "libaom-av1", "av1", "vp8"):
        effective_container_format = "webm"
    else:  # Default to mp4 for H.264/AAC and other cases
        effective_container_format = "mp4"
    if effective_container_format == "webm":
        if effective_vcodec not in ("libvpx-vp9", "vp9", "libaom-av1", "av1", "vp8"):
            logger.warning(
                "Output container is WebM. Changing video codec from '%s' to 'libvpx-vp9'.",
                effective_vcodec,
            )
            effective_vcodec = "libvpx-vp9"
        if effective_acodec not in ("libopus", "opus", "vorbis"):
            logger.warning(
                "Output container is WebM. Changing audio codec from '%s' to 'libopus'.",
                effective_acodec,
            )
            effective_acodec = "libopus"
    elif effective_container_format == "mp4":
        if effective_vcodec not in ("libx264", "libx265", "h264", "h265"):
            if (
                effective_vcodec in ("libvpx-vp9", "vp9", "libaom-av1", "av1", "vp8")
                and video_codec is not None
            ):
                logger.info(
                    "Using %s in MP4 container as specified/derived.",
                    effective_vcodec,
                )
            else:
                logger.warning(
                    "Output container is MP4. Changing video codec from '%s' to 'libx264' for compatibility.",
                    effective_vcodec,
                )
                effective_vcodec = "libx264"

        if effective_acodec not in ("aac",):
            if (
                effective_acodec in ("libopus", "opus", "vorbis")
                and audio_codec is not None
            ):
                logger.info(
                    "Using %s in MP4 container as specified/derived.",
                    effective_acodec,
                )
            else:
                logger.warning(
                    "Output container is MP4. Changing audio codec from '%s' to 'aac' for compatibility.",
                    effective_acodec,
                )
                effective_acodec = "aac"
    if effective_vcodec in ("libx264", "libvpx-vp9"):
        effective_pix_fmt = "yuv420p"
    final_output_extension = f".{effective_container_format}"
    compressed_path = original_path.with_name(
        f"{original_path.stem}_compressed{final_output_extension}",
    )
    logger.debug(
        "Targeting: Container=%s, VCodec=%s, ACodec=%s, PixFmt=%s",
        effective_container_format,
        effective_vcodec,
        effective_acodec,
        effective_pix_fmt,
    )
    logger.debug(
        "Input: %s (%.2fs, %.2fMB)",
        original_path.name,
        duration,
        (await get_file_size(original_path) / (1024 * 1024))
        if original_path.exists()
        else 0,
    )
    logger.debug(
        "Calculated bitrates: Video=%dkbps, Audio=%dkbps",
        video_bitrate_kbps,
        audio_bitrate_kbps,
    )
    logger.debug("Output path: %s", compressed_path)
    # Use temporary directory for passlogfile
    with tempfile.TemporaryDirectory() as temp_dir:
        passlogfile = Path(temp_dir) / f"{original_path.name}_ffmpeg2pass"
        null_device = "NUL" if platform.system() == "Windows" else "/dev/null"

        # fmt: off
        cmd1 = [
            "ffmpeg", "-y",
            "-i", str(original_path),
            "-c:v", effective_vcodec,
            "-b:v", f"{video_bitrate_kbps}k",
            "-pass", "1",
            "-an",
            "-fps_mode", "cfr",
            "-preset", "medium",
        ]
        if effective_pix_fmt:
            cmd1.extend(["-pix_fmt", effective_pix_fmt])

        cmd1.extend([
            "-passlogfile", str(passlogfile),
            "-f", "null",
            null_device,
        ])
        logger.debug("FFmpeg Pass 1 command: %s", " ".join(cmd1))
        proc1 = await asyncio.create_subprocess_exec(
            *cmd1,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout1, stderr1 = await proc1.communicate()
        if proc1.returncode != 0:
            msg = (
                f"First pass failed for {original_path} (rc: {proc1.returncode}):\n"
                f"STDOUT: {stdout1.decode(errors='ignore').strip()}\n"
                f"STDERR: {stderr1.decode(errors='ignore').strip()}"
            )
            raise RuntimeError(msg)

        # fmt: off
        cmd2 = [
            "ffmpeg", "-y",
            "-i", str(original_path),
            "-c:v", effective_vcodec,
            "-b:v", f"{video_bitrate_kbps}k",
            "-pass", "2",
            "-c:a", effective_acodec,
            "-b:a", f"{audio_bitrate_kbps}k",
            "-fps_mode", "cfr",
            "-preset", "medium",
        ]
        if effective_pix_fmt:
            cmd2.extend(["-pix_fmt", effective_pix_fmt])
        cmd2.extend([
            "-passlogfile", str(passlogfile),
            "-f", effective_container_format, # Explicitly set output container format
            str(compressed_path),
        ])
        proc2 = await asyncio.create_subprocess_exec(
            *cmd2,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.debug("FFmpeg Pass 2 command: %s", " ".join(cmd2))
        stdout2, stderr2  = await proc2.communicate()
        if proc2.returncode != 0:
            await remove_file(compressed_path)
            msg = (
                f"Second pass failed for {original_path} (rc: {proc2.returncode}):\n"
                f"STDOUT: {stdout2.decode(errors='ignore').strip()}\n"
                f"STDERR: {stderr2.decode(errors='ignore').strip()}"
            )
            logger.error(msg)
            raise RuntimeError(msg)

    # Verify output size
    final_size_mb = await get_file_size(compressed_path) / (1024 * 1024)
    logger.info(
        "Compression for %s complete. Output: %s (%.2fMB, target: ~%.2fMB from original target %.2fMB)",
        original_path.name,
        compressed_path.name,
        final_size_mb,
        target_size / (1024 * 1024),
        target_size_mb,
    )
    if final_size_mb > target_size_mb:
        logger.warning(
            "Compressed file %s (%.2fMB), exceeds target %.2fMB",
            compressed_path.name,
            final_size_mb,
            target_size_mb,
        )

    return compressed_path


async def compress_image(input_path: Path, target_size: int) -> Path:
    """Compress an image file based on its type, aiming for a target size.

    :param input_path: Path to the input image file.
    :param target_size: Target file size in bytes.
    :return: Path to the compressed image file.
    """
    suffix = input_path.suffix.lower()
    if suffix == ".gif":
        return await compress_gif(input_path, target_size)
    if suffix in [".jpg", ".jpeg"]:
        return await compress_jpeg(input_path, target_size)
    if suffix == ".png":
        return await compress_png(input_path, target_size)
    msg = f"Unsupported file type: {suffix}"
    raise ValueError(msg)


async def compress_gif(input_path: Path, target_size: int) -> Path:
    """Compress a GIF file to a target size using gifsicle.

    :param Path input_path: Path to the input GIF file.
    :param int target_size: Target file size in bytes.
    :raises RuntimeError: If gifsicle fails to compress the GIF.
    :return Path: Path to the compressed GIF file.
    """
    output_path = input_path.with_name(
        f"{input_path.stem}_compressed{input_path.suffix}",
    )
    temp_path = Path("temp.gif")

    async def run_gifsicle(colors: int, lossy: int, out_path: Path) -> int:
        cmd = [
            "gifsicle",
            "--optimize",
            f"--colors={colors}",
            f"--lossy={lossy}",
            "-i",
            str(input_path),
            "-o",
            str(out_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            msg = "Gifsicle failed: %s"
            raise RuntimeError(msg, stderr.decode())
        return await get_file_size(out_path)

    # fmt: off
    compression_levels = [
        (256, 0), (256, 20), (256, 40), (256, 60), (256, 80), (256, 100),
        (128, 0), (128, 20), (128, 40), (128, 60), (128, 80), (128, 100),
        (64, 0), (64, 20), (64, 40), (64, 60), (64, 80), (64, 100),
        (32, 0), (32, 20), (32, 40), (32, 60), (32, 80), (32, 100),
        (16, 0), (16, 20), (16, 40), (16, 60), (16, 80), (16, 100),
    ]
    low, high = 0, len(compression_levels) - 1
    best_colors, best_lossy = compression_levels[0]

    while low <= high:
        mid = (low + high) // 2
        colors, lossy = compression_levels[mid]
        with tempfile.NamedTemporaryFile() as out_file:
            out_file.close()
            out_path = Path(out_file.name)
            size = await run_gifsicle(colors, lossy, out_path)

        if size <= target_size:
            best_colors, best_lossy = colors, lossy
            high = mid - 1
        else:
            low = mid + 1
        logger.debug(
            "Trying %d colors with lossy %d: %.2fKB",
            colors,
            lossy,
            size / 1024,
        )
    await run_gifsicle(best_colors, best_lossy, output_path)
    final_size = await get_file_size(output_path)
    logger.debug(
        "Compressed GIF to %.2fKB with %d colors and lossy %d",
        final_size / 1024,
        best_colors,
        best_lossy,
    )
    if temp_path.exists():
        await remove_file(temp_path)

    return output_path


async def compress_jpeg(input_path: Path, target_size: int) -> Path:
    """Compress a JPEG file using Pillow, aiming for a target size.

    :param input_path: Path to the input JPEG file.
    :param target_size: Target file size in bytes.
    :return: Path to the compressed JPEG file.
    """
    output_path = input_path.with_name(
        f"{input_path.stem}_compressed{input_path.suffix}",
    )
    quality = 75
    min_quality = 10

    img = Image.open(input_path)
    while quality >= min_quality:
        await asyncio.to_thread(
            img.save,
            output_path,
            "JPEG",
            quality=quality,
            optimize=True,
        )
        compressed_size = await get_file_size(output_path)
        if compressed_size <= target_size or quality == min_quality:
            logger.debug(
                "Compressed JPEG to %.2fKB with quality %d",
                compressed_size / 1024,
                quality,
            )
            return output_path
        quality -= 10  # Reduce quality

    logger.warning("Could not compress %s to %.2fKB", input_path, target_size / 1024)
    return output_path


async def compress_png(input_path: Path, target_size: int) -> Path:
    """Compress a PNG file using Pillow by converting to paletted mode.

    :param input_path: Path to the input PNG file.
    :param target_size: Target file size in bytes.
    :return: Path to the compressed PNG file.
    """
    output_path = input_path.with_name(
        f"{input_path.stem}_compressed{input_path.suffix}",
    )
    colors = 256  # Start with 256 colors
    min_colors = 16

    img = Image.open(input_path)
    while colors >= min_colors:
        # Convert and save in a thread
        img_paletted = img.convert("P", palette=Image.Palette.ADAPTIVE, colors=colors)
        await asyncio.to_thread(
            img_paletted.save,
            output_path,
            "PNG",
            optimize=True,
        )
        compressed_size = await get_file_size(output_path)
        if compressed_size <= target_size or colors == min_colors:
            logger.debug(
                "Compressed PNG to %.2fKB with %d colors",
                compressed_size / 1024,
                colors,
            )
            return output_path
        colors = max(colors // 2, min_colors)  # Reduce colors

    logger.warning("Could not compress %s to %.2fKB", input_path, target_size / 1024)
    return output_path
