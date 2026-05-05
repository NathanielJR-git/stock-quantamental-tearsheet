<div align="center"> 
  <h1> Automated Stock Tearsheet Generator </h1>
  <h3> Using AWS, Spark and Airflow </h3>

![Demo GIF](/docs/pipeline.png)

</div>

---

## Project Description

Tearsheet Factory is an end-to-end financial data processing platform that integrates large-scale batch processing with artificial intelligence (LLM) to generate comprehensive stock insights. The system is designed using a Modular Monorepo architecture that separates the Data Pipeline (Airflow/Spark) and Presentation Layer (FastAPI/Streamlit).

---

## Tech Stack

### Data Engineering & Orchestration
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=flat-square&logo=amazons3&logoColor=white)
![Parquet](https://img.shields.io/badge/Apache%20Parquet-50C878?style=flat-square&logoColor=white)
![Groq](https://img.shields.io/badge/Groq%20API-FF6B35?style=flat-square&logoColor=white)

- **Orchestrator**: Apache Airflow (v3.2.1 SDK)
- **Processing Engine**: Apache Spark (v3.5.1)
- **LLM Integration**: Groq API (Llama 3)
- **Data Lake**: Amazon S3 with Apache Parquet

### Data Discovery & Serving
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900?style=flat-square&logo=amazon&logoColor=white)
![Amazon Athena](https://img.shields.io/badge/Amazon%20Athena-FF9900?style=flat-square&logo=amazon&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![PyAthena](https://img.shields.io/badge/PyAthena-3776AB?style=flat-square&logo=python&logoColor=white)

- **Data Catalog**: AWS Glue
- **Query Engine**: Amazon Athena (Serverless SQL)
- **Backend API**: FastAPI
- **Cloud SDK**: PyAthena

### Frontend & Visualization
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-3F4F75?style=flat-square&logo=plotly&logoColor=white)

- **Dashboard Framework**: Streamlit
- **Charting Library**: Plotly (Candlestick & Technical Indicators)

### Infrastructure & DevOps
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

- **Containerization**: Docker & Docker Compose