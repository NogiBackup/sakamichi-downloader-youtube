from http import HTTPStatus
from io import StringIO
import json
import os
import shutil
import time

import boto3
from botocore.vendored.six import StringIO
import requests
import youtube_dl

JOB_INTERVAL = os.environ('JOB_INTERNAL', 300)
S3_BUCKET = os.environ['S3_BUCKET_NAME']
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
SQS_QUEUE_NAME = os.environ['SQS_QUEUE_NAME']
YOUTUBE_CHANNELS = (
    'UCfvohDfHt1v5N8l3BzPRsWQ',  # 乃木坂配信中
    'UCUApURCve0BEqv6Sa9x_OJg',  # my channel【白石麻衣 公式】
)


def _parse_message(message: dict) -> dict:
    body = json.loads(message['Body'])
    body.update(dict(source=message['MessageAttributes']['Source']['StringValue']))
    return body


def create_workspace(root_path: str = '.output/'):
    working_dir = os.path.abspath(root_path)
    if os.path.isdir(working_dir):
        shutil.rmtree(working_dir)
    os.makedirs(working_dir, exist_ok=True)
    return working_dir


def download_video(output_dir: str, object_id: str) -> None:
    config = {
        'outtmpl': '{}/%(title)s.%(ext)s'.format(output_dir),
    }
    with youtube_dl.YoutubeDL(config) as ydl:
        urls = ['https://www.youtube.com/watch?v={}'.format(object_id)]
        ydl.download(urls)


def download_thumbnail(output_dir: str, url: str):
    response = requests.get(url)
    if response.status_code == HTTPStatus.OK:
        with open(os.path.join(output_dir, 'thumbnail.jpg'), 'wb') as writer:
            writer.write(response.content)


def touch_metadata(output_dir: str, job_detail: dict) -> None:
    with open(os.path.join(output_dir, 'metadata.json'), 'w') as writer:
        writer.write(json.dumps(job_detail, ensure_ascii=False, sort_keys=True))


def upload_to_s3(output_dir: str, job_detail: dict) -> dict:
    objects_map = dict()
    s3 = boto3.client('s3', region_name='ap-northeast-1')
    for obj in os.listdir(output_dir):
        obj_key = '{source}/{channel_id}/{episode_id}/{object_key}'.format(
            source=job_detail['source'],
            channel_id=job_detail['channel_id'],
            episode_id=job_detail['id'],
            object_key=obj
        )
        s3.upload_file(os.path.join(output_dir, obj), S3_BUCKET, obj_key)
        objects_map['obj'] = object_key
    complete_file = '{source}/{channel_id}/{episode_id}/{object_key}'.format(
        source=job_detail['source'],
        channel_id=job_detail['channel_id'],
        episode_id=job_detail['id'],
        object_key='__COMPLETED__'
    )
    s3.upload_fileobj(StringIO(""), S3_BUCKET, complete_file)
    return objects_map


def update_database(job: dict):
    pass


if __name__ == "__main__":
    queue = boto3.client('sqs', region_name='ap-northeast-1')
    messages = queue.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        AttributeNames=['All'],
        MessageAttributeNames=['Source', 'ChannelId', 'ObjectId'],
        MaxNumberOfMessages=1
    ).get("Messages")

    while messages:
        job = _parse_message(messages[0])
        # Create Workspace
        workspace = create_workspace(root_path=os.path.abspath(os.path.join('.output/{}'.format(job['id']))))

        # Download
        download_video(workspace, job['id'])
        download_thumbnail(workspace, job['thumbnails']['maxres']['url'])
        touch_metadata(workspace, job)

        # Upload
        object_key = upload_to_s3(workspace, job)

        # Post DB Update
        update_database(job)

        # Housekeep
        shutil.rmtree(workspace)

        # Wait, Next
        time.sleep(JOB_INTERVAL)
        messages = queue.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['All'],
            MessageAttributeNames=['Source', 'ChannelId', 'ObjectId'],
            MaxNumberOfMessages=1
        ).get("Messages")
