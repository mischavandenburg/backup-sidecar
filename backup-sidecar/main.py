import os
import yaml
import logging
import schedule
import time
from datetime import datetime
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def backup_to_blob():
    with open(os.getenv("CONFIG_PATH", "/config/backup-config.yaml")) as f:
        config = yaml.safe_load(f)

    storage_account = os.getenv("STORAGE_ACCOUNT_NAME")
    container_name = os.getenv("CONTAINER_NAME", "my_container_name")
    blob_sas_token = os.getenv("BLOB_SAS_TOKEN")

    if not all([storage_account, container_name, blob_sas_token]):
        logger.error("Missing required environment variables")
        return

    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob_service = BlobServiceClient(account_url, credential=blob_sas_token)
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
    backup_time = os.getenv("BACKUP_TIME", "02:00")
    timezone = os.getenv("TIMEZONE", "Europe/Amsterdam")
    immediate = os.getenv("IMMEDIATE", False)

    if immediate:
        logger.info("Making immediate backup.")
        backup_to_blob()

    schedule.every().day.at(backup_time, timezone).do(backup_to_blob)
    logger.info(f"Backup scheduled for {backup_time} {timezone}")

    while True:
        schedule.run_pending()
        time.sleep(60)
