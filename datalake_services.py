import os
from dotenv import load_dotenv
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
from io import BytesIO
import logging

# Load environment variables from .env file
load_dotenv()

# Use environment variable for connection string
STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
logging.info(f"AZURE_STORAGE_CONNECTION_STRING: {STORAGE_CONNECTION_STRING}")
if not STORAGE_CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")

PROCESSED_EVENTS_CONTAINER = "processed-events"

async def initialize_marker_container():
    """
    Initializes the marker container by creating it if it doesn't exist.
    """
    try:
        async with BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING) as blob_service_client:
            container_client = blob_service_client.get_container_client(PROCESSED_EVENTS_CONTAINER)
            await container_client.create_container()
            logging.info(f"Container '{PROCESSED_EVENTS_CONTAINER}' created successfully.")
    except Exception as e:
        if "ContainerAlreadyExists" in str(e):
            logging.info(f"Container '{PROCESSED_EVENTS_CONTAINER}' already exists.")
        else:
            logging.error(f"Error creating container '{PROCESSED_EVENTS_CONTAINER}': {e}")
            raise e

async def has_event_been_processed(event_id: str) -> bool:
    """
    Checks if a marker blob exists for the given event_id.
    """
    try:
        async with BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING) as blob_service_client:
            blob_client = blob_service_client.get_blob_client(container=PROCESSED_EVENTS_CONTAINER, blob=event_id)
            await blob_client.get_blob_properties()
            return True
    except Exception as e:
        if "BlobNotFound" in str(e):
            return False
        else:
            logging.error(f"Error checking event ID {event_id}: {e}")
            return False

async def mark_event_as_processed(event_id: str):
    """
    Creates a marker blob to indicate that the event has been processed.
    """
    try:
        async with BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING) as blob_service_client:
            blob_client = blob_service_client.get_blob_client(container=PROCESSED_EVENTS_CONTAINER, blob=event_id)
            await blob_client.upload_blob(data=b'Processed', overwrite=False)
            logging.info(f"Event ID {event_id} marked as processed.")
    except Exception as e:
        if "BlobAlreadyExists" in str(e):
            logging.warning(f"Marker for Event ID {event_id} already exists.")
        else:
            logging.error(f"Error marking Event ID {event_id} as processed: {e}")

async def get_today_files_dict(lake_key, container_name, folder_name, storage_account_name):
    """
    List and download all files added to the specified Azure Data Lake directory today.
    Returns a dictionary {filename: file_content}.
    """
    from datetime import timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_prefix = f"{folder_name}/{today}/" if folder_name else f"{today}/"
    files_dict = {}
    async with BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING) as blob_service_client:
        container_client = blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs(name_starts_with=today_prefix)
        async for blob in blobs:
            filename = os.path.basename(blob.name)
            stream = await download_blob(lake_key, container_name, storage_account_name, folder_name, filename)
            if stream:
                try:
                    content = stream.read()
                    files_dict[filename] = content
                except Exception as e:
                    logging.error(f"Error reading {filename}: {e}")
    return files_dict

async def download_blob(storage_account_key, container_name, storage_account_name, folder_name, file_name):
    """
    Download a file from Azure Data Lake Storage Gen2 given the container name, folder name, and file name.
    Returns a BytesIO stream of the blob's content.
    """
    try:
        account_url = f"https://{storage_account_name}.blob.core.windows.net"
        async with BlobServiceClient(account_url=account_url, credential=storage_account_key) as blob_service_client:
            blob_path = f"{folder_name}/{file_name}" if folder_name else file_name
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
            download_stream = await blob_client.download_blob()
            stream = BytesIO()
            await download_stream.readinto(stream)
            stream.seek(0)
            return stream
    except Exception as e:
        logging.error(f"An error occurred while downloading the blob '{file_name}': {e}")
        return None

def generate_sas_url(storage_account_name, storage_account_key, container_name, blob_name, expiry_minutes=60):
    """
    Generate a SAS URL for a file in Azure Data Lake Gen2.
    """
    try:
        sas_token = generate_blob_sas(
            account_name=storage_account_name,
            container_name=container_name,
            blob_name=blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(minutes=expiry_minutes),
            account_key=storage_account_key
        )
        sas_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        return sas_url
    except Exception as e:
        logging.error(f"An error occurred while generating SAS URL for blob '{blob_name}': {e}")
        return None

async def upload_file_stream(storage_account_name, storage_account_key, container_name, folder_name, file_name, stream):
    """
    Upload a file stream to Azure Data Lake Storage Gen2.
    """
    try:
        account_url = f"https://{storage_account_name}.blob.core.windows.net"
        async with BlobServiceClient(account_url=account_url, credential=storage_account_key) as blob_service_client:
            container_client = blob_service_client.get_container_client(container_name)
            blob_path = f"{folder_name}/{file_name}" if folder_name else file_name
            blob_client = container_client.get_blob_client(blob_path)
            await blob_client.upload_blob(stream, overwrite=True)
            logging.info(f"Uploaded {file_name} to {container_name}/{blob_path}")
            return blob_client.url
    except Exception as e:
        logging.error(f"An error occurred while uploading the blob '{file_name}': {e}")
        return None