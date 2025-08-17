# Serverless Data Pipeline with Live BI

![Dashboard Screenshot](dashboard-screenshot.png)

### Project Overview

This project demonstrates a complete, end-to-end, real-time data pipeline built to analyze Netflix's content catalog. The pipeline simulates a live data feed, streams the data into a cloud data warehouse, and visualizes the insights on a live Power BI dashboard.

The project showcases skills in data engineering, cloud architecture, and business intelligence. It was designed to be robust and scalable, using modern, enterprise-grade tools.

---

### Architecture

The data flows through a modern, cloud-native architecture:

**Python Script ➡️ Google BigQuery (Streaming Inserts) ➡️ Power BI (DirectQuery Mode)**

1.  **Data Source & Simulation:** A Python script reads an enriched CSV file containing Netflix titles and their associated metadata (ratings, popularity, etc.). It then acts as a producer, streaming this data row by row to simulate a live feed.
2.  **Cloud Data Warehouse:** The script sends data directly to **Google BigQuery** using its high-throughput streaming insert API. BigQuery acts as our scalable, serverless data warehouse, storing the data as it arrives.
3.  **Live Dashboard:** **Power BI** connects to the BigQuery table in **DirectQuery mode**. This ensures that the dashboard is always showing the most up-to-date data from the warehouse without importing or copying it. A refresh in Power BI queries BigQuery live.

---

### How to Run This Project

To recreate this project, follow these steps:

**1. Prerequisites:**
* A Google Cloud Platform account with a new project created.
* Power BI Desktop installed.
* Python 3 installed.

**2. Download the Dataset:**
* The enriched dataset is required. You can download it from Kaggle: [Netflix Movies and TV Shows with TMDb Scores](https://www.kaggle.com/datasets/satpreetmakhija/netflix-movies-and-tv-shows-with-tmdb-scores)
* Save the downloaded file as `netflix_enriched_final.csv` in the project folder.

**3. Setup Your Environment:**
* Clone this repository or download the files.
* Install the required Python libraries by running the following command in your terminal:
    ```bash
    pip install -r requirements.txt
    ```
* Authenticate with Google Cloud by running these commands in your terminal:
    ```bash
    gcloud auth login
    gcloud auth application-default login
    ```

**4. Configure and Run the Stream:**
* Open the `stream_to_bigquery.py` script.
* Update the `PROJECT_ID` variable with your own Google Cloud Project ID.
* Run the script from your terminal:
    ```bash
    python stream_to_bigquery.py
    ```
    This will create the table in BigQuery and begin streaming the data.

**5. Connect Power BI:**
* Open Power BI Desktop and connect to Google BigQuery.
* Choose **DirectQuery** mode.
* Navigate to your project and dataset to find the `enriched_titles` table.
* Load the table and begin building your dashboard.

---

### Challenges & Learnings

This project involved significant problem-solving. Initial plans to use a local Kafka/Spark stack were pivoted due to local machine limitations (Windows Home). Subsequent attempts to use cloud Kafka providers were blocked by service outages and signup barriers.

This led to the final, more robust architecture using Google BigQuery's native streaming capabilities, demonstrating adaptability and a deep understanding of modern cloud data warehousing solutions.
