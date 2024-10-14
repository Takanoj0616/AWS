最近作成したlambdaです
#!/usr/bin/python
"""zipファイルに含まれるログファイルを、ログ種別に応じたdata streamsに連携する"""

import base64
from zipfile import ZipFile, BadZipFile
import hashlib
import pathlib
import io
import os
from logging import getLogger, INFO
import uuid
from typing import List, Sequence

import boto3

logger = getLogger(__name__)
logger.setLevel(INFO)

CHK_STREAMS = os.environ["CHK_STREAMS"]
OFF_STREAMS = os.environ["OFF_STREAMS"]
AUWIFI_STREAMS = os.environ["_STREAMS"]

valid_file_types = List[dict[str, Sequence[object]]]


def lambda_handler(event, context):

    base64_data = event["body"]
    request_body = base64.b64decode(base64_data)
    zip_data = io.BytesIO(request_body)

    try:
        with ZipFile(zip_data, "r") as zip_obj:
            valid_files: valid_file_types = []

            # ハッシュチェック
            try:
                valid_files = _check_hash(zip_obj)
            except Exception as e:
                logger.error(f"ハッシュチェック時にエラーが発生しました。 error: {e}")

                return {
                    "statusCode": 400,
                    "body": "400 Bad Request"
                }

            # kinesis data streamsへの送信
            try:
                _put_to_kinesis(valid_files)
            except Exception as e:
                logger.error(f"kinesisへの送信時にエラーが発生しました。 error: {e}")

                return {
                    "statusCode": 503,
                    "body": "503 Service Unavailable"
                }

            return {
                "statusCode": 200,
                "body": "OK"
            }

    except BadZipFile as e:
        logger.error(f"zipファイルが開けませんでした。 error: {e}")

        return {
            "statusCode": 415,
            "body": "415 Unsupported Media Type"
        }
    except Exception as e:
        logger.error(f"想定外のエラーが発生しました。 error: {e}")

        return {
            "statusCode": 503,
            "body": "503 Service Unavailable"
        }


def _check_hash(zip_obj: ZipFile) -> valid_file_types:
    """ファイル名に含まれるハッシュ値と実際のデータのハッシュ値を比較する"""

    # ハッシュが一致したデータ
    valid_files = []

    # ハッシュが1ファイルでも一致しなければFalse
    is_valid_zip_file = True

    for file_name in zip_obj.namelist():

        # ディレクトリの場合は後続処理をスキップ
        if file_name.split("/")[-1] == "":
            continue

        with zip_obj.open(file_name) as zip_file:

            """
            file_nameの例: 
            202310190345511_chk_54_KYG01_9370b94ad7d3f0b9656bfd5a1cbb3887.csv
            """
            expect_hash = pathlib.Path(file_name).stem.split("_")[4]
            data = zip_file.read()
            md5 = hashlib.md5(data)
            actual_hash = md5.hexdigest()

            if expect_hash == actual_hash:
                logger.info(
                    f"hash check: file_name: {file_name} "
                    f"expect hash: {expect_hash} actual hash: {actual_hash}"
                )
                valid_files.append({
                    "file_name": file_name,
                    "data": data + b"\n"  # S3出力時に改行が付与されないため改行を付与
                })
            else:
                logger.error(
                    f"hash check error: file_name: {file_name} "
                    f"expect hash: {expect_hash} actual hash: {actual_hash}"
                )
                is_valid_zip_file = False

    if not is_valid_zip_file:
        raise Exception("破損しているファイルがあるため処理を終了します。")

    return valid_files


def _put_to_kinesis(valid_files: valid_file_types) -> None:
    """ログファイル名に対応したdata streamsにputする"""

    for valid_file in valid_files:
        file_name = valid_file["file_name"]
        stream_name = ""

        if "_chk_" in file_name:
            stream_name = CHK_STREAMS
        elif "_OFF_" in file_name:
            stream_name = OFF_STREAMS
        elif "_auWiFi_" in file_name:
            stream_name = AUWIFI_STREAMS

        logger.info(f"data streamsへ送信します。file_name: {file_name}")
        response = boto3.client("kinesis").put_record(
            StreamName=stream_name,
            Data=valid_file["data"],
            PartitionKey=str(uuid.uuid4()),
        )
        logger.info(
            f"送信完了しました。file_name: {file_name} response: {str(response)}"
        )



