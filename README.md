# Weather ETL Project (Airflow + Astro)

This project implements a daily **Extract, Transform, Load (ETL)** pipeline using **Apache Airflow** managed with **Astronomer (Astro CLI)**.  
It fetches current weather data for Phnom Penh, Cambodia from the **OpenWeather API**, processes it, and uploads the results as a CSV file to a **Google Cloud Storage (GCS)** bucket.

---

## 🌟 Features

- **Automated Data Extraction:** Uses `HttpSensor` to check API readiness and `SimpleHttpOperator` to retrieve weather data.  
- **Data Transformation:** Transforms JSON data, converts temperatures from Kelvin to Celsius, and handles timezone-aware timestamps for sunrise and sunset.  
- **Cloud Storage Integration:** Uploads processed CSVs to GCS using Airflow's `GCSHook`.  
- **Modern Airflow Syntax:** DAG uses decorator-based syntax.  
- **Concurrency Control:** Only one active DAG run at a time (`max_active_runs=1`) to avoid overlapping jobs.  

---

## ⚡ Prerequisites

Before running the project, ensure you have:

1. **Docker** installed  
2. **Astro CLI** installed  
3. A **Google Cloud Project** with a GCS bucket  
4. **OpenWeather API key**  
5. Python dependencies installed (`pandas`, `requests`, `apache-airflow`, etc.)  

---

## 📂 Project Structure

your-project/ <br>
├── dags/ <br>
│ └── weather_etl.py # Airflow DAG file<br>
├── include/ # Service account JSON key and other files <br>
├── requirements.txt # Python dependencies <br>
├── .env.example # Example environment variables <br>
└── README.md # This file <br>

---

## 📝 Setup Instructions

### 1. Install Astro CLI

Open PowerShell as Administrator:

```powershell
winget install -e --id Astronomer.Astro
astro version  # Verify installation
```
### 2. Configure Connections

**GCP Connection**

Set via Airflow UI or environment variable:
<code>
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://?extra__google_cloud_platform__key_path=/usr/local/airflow/include/<service-account-key>.json&extra__google_cloud_platform__scope=https://www.googleapis.com/auth/cloud-platform'
</code>

**OpenWeather API Connection**
<code>
AIRFLOW_CONN_OPEN_WEATHER_API_CONN=http://api.openweathermap.org
</code>

**Environment Variables**

<code>
OPEN_WEATHER_API_KEY=your-api-key <br>
GCS_BUCKET_NAME=your-gcs-bucket-name
</code>

### 🔄 DAG Workflow

The DAG consists of three sequential tasks:

1. **`is_weather_api_ready`** – Waits for the OpenWeather API to be reachable and return a valid response.  
2. **`extract_weather_data`** – Fetches the raw weather data in JSON format from the API.  
3. **`transform_and_load_weather_data`** – Processes the raw JSON, converts it to a pandas DataFrame, and uploads it as a CSV file to GCS.

### 📤 Output

Daily, a new CSV file is uploaded to your GCS bucket in the folder:

pp_daily_weather_report/weather_data_YYYYMMDDHHMMSS.csv

### 🚀 Running the Project

Start and manage Airflow with Astro CLI:

```bash
astro dev start    # Start Airflow locally
astro dev stop     # Stop Airflow
astro dev restart  # Restart Airflow
```
Access the Airflow Web UI: http://localhost:8080

---

### 🔹 Customization

```markdown
### 🧩 Customization

- Change `lat` and `lon` in the DAG for a different location.  
- Update `GCS_BUCKET_NAME` in `.env` to point to a different bucket.  
- Update `OPEN_WEATHER_API_KEY` in `.env` for your OpenWeather key.  

### 📚 References

- [OpenWeather API](https://openweathermap.org/api)  
- [Airflow GCSHook](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/hooks/gcs.html)  
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/)  

### 🙏 Acknowledgements

- **OpenWeather API** – Weather data provider.  
- **Astronomer (Astro CLI)** – Managed Airflow environment.
