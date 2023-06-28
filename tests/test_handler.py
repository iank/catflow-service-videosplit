import pytest
from catflow_service_videosplit.worker import extract_frames, videosplit_handler
from catflow_worker.types import VideoFile, VideoFileSchema, RawFrame, RawFrameSchema
from moto import mock_s3
import uuid
from pathlib import Path
import inspect
import boto3
import io

S3_ENDPOINT_URL = "http://localhost:5002"
AWS_BUCKET_NAME = "catflow-test"
AWS_ACCESS_KEY_ID = "catflow-video-test-key"
AWS_SECRET_ACCESS_KEY = "catflow-video-secret-key"

FIXTURE_DIR = Path(__file__).parent.resolve() / "test_files"


def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


class AsyncS3Wrapper:
    """Just fake it so I can still mock with moto

    The alternative is setting up a mock server and connecting to it, as in
    catflow-worker's tests"""

    def __init__(self):
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        self.client = session.client("s3", region_name="us-east-1")

    async def download_fileobj(self, bucket, key, f):
        buf = io.BytesIO()
        self.client.download_fileobj(bucket, key, buf)

        # Ensure we're reading from the start of the file
        buf.seek(0)
        data = buf.read()

        await f.write(data)

        return f

    async def upload_fileobj(self, f, bucket, key):
        return self.client.upload_fileobj(f, bucket, key)


@pytest.fixture
def s3_client():
    with mock_s3():
        s3 = AsyncS3Wrapper()
        yield s3


@pytest.mark.datafiles(
    FIXTURE_DIR / "car.mp4",
)
def test_extract_frames(datafiles):
    """Verify that extract_frames can open a video and return a list of frames"""
    # Get data file
    video = str(next(datafiles.iterdir()))

    # Create generator
    framegen = extract_frames(video)
    assert inspect.isgenerator(framegen)

    # Get frames
    frames = list(framegen)
    assert len(frames) == 11


@pytest.mark.datafiles(
    FIXTURE_DIR / "car.mp4",
)
@pytest.mark.asyncio
async def test_worker(s3_client, datafiles):
    # Get data file
    video = str(next(datafiles.iterdir()))

    # Push it to mock S3 so our worker can retrieve it
    s3_client.client.create_bucket(Bucket=AWS_BUCKET_NAME)
    with open(video, "rb") as f:
        await s3_client.upload_fileobj(f, AWS_BUCKET_NAME, "test1.mp4")

    # Input
    video_file = VideoFile(key="test1.mp4")
    video_schema = VideoFileSchema()
    msg_in = video_schema.dump(video_file)

    # Output schema
    frame_schema = RawFrameSchema(many=True)

    # Run the worker twice. The difference between 'detect' and 'ingest'
    # behavior is just that the detect pipeline batches the response

    # Test worker's behavior in the 'detect' pipeline
    status, responses = await videosplit_handler(
        [msg_in], "detect.video", s3_client, AWS_BUCKET_NAME
    )

    assert status is True
    assert len(responses) == 1
    routing_key, frames = responses[0]
    assert routing_key == "detect.rawframes"

    frames = frame_schema.load(frames)
    assert len(frames) == 11
    for frame in frames:
        assert isinstance(frame, RawFrame)
        uu, ext = frame.key.split(".")
        assert ext == "png"
        assert is_valid_uuid(uu)
        assert frame.source.key == video_file.key

    # Test worker's behavior in the 'ingest' pipeline
    status, responses = await videosplit_handler(
        [msg_in], "ingest.video", s3_client, AWS_BUCKET_NAME
    )

    assert status is True
    assert len(responses) == 11
    for response in responses:
        routing_key, frames = response
        assert routing_key == "ingest.rawframes"
        assert len(frames) == 1
        frame = frame_schema.load(frames)[0]
        uu, ext = frame.key.split(".")
        assert ext == "png"
        assert is_valid_uuid(uu)
        assert frame.source.key == video_file.key
