import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import BytesIO
import matplotlib.pyplot as plt
import logging 
from app.schemas.logger import logger
load_dotenv()

plots = r"app\core\analysis\report_generation_submodules\output\plots"
def create_matplotlib(self, sentiment_data_agg: list, name: str, num_max_articles: int):
    months = [item["month"] for item in sentiment_data_agg]
    negatives = [item["negative"] for item in sentiment_data_agg]

    # Create the plot
    plt.figure(figsize=(12, 5))
    plt.bar(months, negatives, color='lightcoral')

    # Add labels and title
    plt.xlabel('Month')
    plt.ylabel('Negative News')
    plt.title(name)
    plt.axhline(0, color='black', linewidth=0.8) 
    plt.yticks(range(0, num_max_articles))
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot as a PNG image
    plot_image_path = os.path.join(plots, f"{name}_plot.png")
    plt.savefig(plot_image_path, format='png', dpi=300)
    plt.close()  # Close the plot to free up memory

    return plot_image_path

from azure.storage.blob import BlobServiceClient
import logging
from io import BytesIO
def upload_to_azure_blob(file_buffer: BytesIO, file_name: str, session_id):
    """
    Uploads a file to Azure Blob Storage. Creates the container if it doesn't exist.

    :param file_buffer: The buffer containing the file to be uploaded.
    :param file_name: The name of the file to be saved in blob storage.
    :param session_id: The container name (used as session identifier).
    :return: True if upload is successful, False otherwise.
    """

    connection_string = os.getenv("BLOB_STORAGE__CONNECTION_STRING")
    container_name = session_id

    try:
        # Initialize the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Create the container if it does not exist
        if not container_client.exists():
            container_client.create_container()
            logger.info(f"Created container: {container_name}")

        # Get a blob client
        blob_client = container_client.get_blob_client(file_name)

        # Upload the file buffer, seek to the start before uploading
        file_buffer.seek(0)  # Ensure we're at the start of the buffer
        
        # Upload the buffer directly to blob storage
        blob_client.upload_blob(file_buffer, overwrite=True)
        logger.info(f"Successfully uploaded {file_name} to Azure Blob Storage.")
        return True

    except Exception as e:
        logger.error(f"Failed to upload {file_name} to Azure Blob Storage. Error: {e}")
        return False
    
def clear_output_folder(output_folder):
    """
    Explicitly clear all files in the output folder.
    """
    if os.path.exists(output_folder):
        for file in os.listdir(output_folder):
            file_path = os.path.join(output_folder, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                logger.info(f"Removed file: {file}")

        logger.info("Output folder cleared: All files removed.")
    else:
        logger.warning("Output folder does not exist. Nothing to clear.")