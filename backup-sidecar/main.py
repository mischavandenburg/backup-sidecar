import os
import time
import yaml
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def backup_to_blob():
    with open(os.environ.get("CONFIG_PATH", "/config/backup-config.yaml")) as f:
        config = yaml.safe_load(f)

    storage_account = os.environ["STORAGE_ACCOUNT_NAME"]
    container_name = os.environ["CONTAINER_NAME"]

    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob_service = BlobServiceClient(
        account_url, credential=os.environ["BLOB_SAS_TOKEN"]
    )
    container_client = blob_service.get_container_client(container_name)

    for backup_config in config["backups"]:
        directory = backup_config["path"]
        prefix = backup_config.get("prefix", "")

        if not os.path.exists(directory):
            logger.warning(f"Directory {directory} does not exist")
            continue

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.path.basename(directory.rstrip("/"))
        backup_name = (
            f"{prefix}_{base_dir}_{timestamp}.tar.gz"
            if prefix
            else f"{base_dir}_{timestamp}.tar.gz"
        )

        try:
            archive_path = f"/tmp/{backup_name}"
            exit_code = os.system(
                f"tar -czf {archive_path} -C {os.path.dirname(directory)} {base_dir}"
            )

            if exit_code != 0:
                raise Exception(f"tar command failed with exit code {exit_code}")

            with open(archive_path, "rb") as data:
                blob_client = container_client.get_blob_client(backup_name)
                blob_client.upload_blob(data)

            logger.info(f"Successfully backed up {directory} to {backup_name}")
            os.remove(archive_path)

        except Exception as e:
            logger.error(f"Backup failed for {directory}: {str(e)}")


if __name__ == "__main__":
    interval = int(os.environ.get("BACKUP_INTERVAL_SECONDS", 43200))  # 12 hours default
    while True:
        backup_to_blob()
        time.sleep(interval)
