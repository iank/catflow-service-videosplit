from typing import Any, List, Tuple
import signal
import asyncio
from catflow_worker import Worker
from catflow_worker.types import VideoFileSchema, RawFrameSchema, RawFrame
from concurrent.futures import ThreadPoolExecutor
import aiofiles
import io
import os
import cv2
from uuid import uuid4

import logging


def extract_frames(video_path):
    logging.debug(f"Opening VideoCapture({video_path})")
    cap = cv2.VideoCapture(video_path)
    frameCount = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    logging.debug(f"cv2.CAP_PROP_FRAME_COUNT = {frameCount}")

    fc = 0
    ret = True
    while fc < frameCount and ret:
        ret, frame = cap.read()
        logging.debug(f"fc: {fc}, cap.read() -> {ret}")
        if not ret:
            continue

        ret, buf = cv2.imencode(".png", frame)
        yield buf.tobytes()
        fc += 1

    cap.release()


async def videosplit_handler(
    msg: str, key: str, s3: Any, bucket: str
) -> Tuple[bool, List[Tuple[str, Any]]]:
    """Split the incoming video files into frames

    If the routing key matches detect.*, keep them batched."""
    # Load message
    assert len(msg) == 1
    video = VideoFileSchema().load(msg[0])
    logging.info(f"[*] Message received ({key}): {video}")

    # Download S3 object to a temporary file
    async with aiofiles.tempfile.NamedTemporaryFile("wb", delete=False) as f:
        await s3.download_fileobj(bucket, video.key, f)

    # Wrap this with try/finally to ensure the temporary file gets deleted
    frames_created = []
    try:
        # OpenCV frame generator
        loop = asyncio.get_event_loop()
        frame_gen = extract_frames(f.name)

        def next_frame(gen):
            try:
                return next(gen)
            except StopIteration:
                return None

        with ThreadPoolExecutor() as executor:
            while True:
                frame_data = await loop.run_in_executor(executor, next_frame, frame_gen)
                if frame_data is None:
                    break

                logging.debug("Got a frame")
                frame_file = io.BytesIO(frame_data)

                # Upload frames to S3
                frame_filename = str(uuid4()) + ".png"
                await s3.upload_fileobj(frame_file, bucket, frame_filename)

                # Store object name so we can pass it on
                frame = RawFrame(key=frame_filename, source=video)
                logging.info(f"Created frame: {frame}")
                frames_created.append(frame)
    finally:
        os.remove(f.name)

    # Empty response if no frames created
    if len(frames_created) == 0:
        return True, []

    # Dump frames to serializable form
    frames = RawFrameSchema(many=True).dump(frames_created)

    responses = []
    pipeline, _ = key.split(".")
    if pipeline == "ingest":
        # We do not have to batch these, create many responses
        for frame in frames:
            responses.append(("ingest.rawframes", [frame]))
    elif pipeline == "detect":
        # We need to keep these together for the detector to consider the
        # motion event as a whole. Create one response
        responses.append(("detect.rawframes", frames))
    else:
        raise ValueError(f"Unexpected pipeline name {pipeline}")

    nFrames = len(frames)
    nMsgs = len(responses)
    logging.info(f"[-] {nFrames} frames -> {pipeline}.rawframes ({nMsgs} msgs)")
    return True, responses


async def shutdown(worker, task):
    await worker.shutdown()
    task.cancel()
    try:
        await task
    except asyncio.exceptions.CancelledError:
        pass


async def startup(queue: str, topic_key: str):
    worker = await Worker.create(videosplit_handler, queue, topic_key)
    task = asyncio.create_task(worker.work())

    def handle_sigint(sig, frame):
        print("^ SIGINT received, shutting down...")
        asyncio.create_task(shutdown(worker, task))

    signal.signal(signal.SIGINT, handle_sigint)

    try:
        if not await task:
            print("[!] Exited with error")
            return False
    except asyncio.exceptions.CancelledError:
        return True


def main() -> bool:
    topic_key = "*.video"
    queue_name = "catflow-service-videosplit"
    logging.basicConfig(level=logging.INFO)
    return asyncio.run(startup(queue_name, topic_key))
