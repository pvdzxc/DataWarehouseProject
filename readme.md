## Data Warehouse Project

This project demonstrates a basic data engineering ETL pipeline data using Cloud Service: Amazon Web Service (S3, Lambda) and Snowflake. It's ideal for learning or demonstrating how to integrate datalake in AWS with Snowflake. 
The Cloud Service free trial has expired (1 month for Snowflake trial) hence no implement prototype

🎥 **Demo Video:** [Watch on Google Drive](https://drive.google.com/file/d/1zwRdB9LI7ZbzNgfERtLWB1b5r7XDozSX/view?usp=sharing)

---

## 📄 File Descriptions

### 1. `Top100BillBoardLambdaFunction.py`

This script was implemented in Lambda Function Service in AWS, which collect data from top 100 BillBoard and store them in S3 Service.

---

### 2. `Top100BillBoardPipeline.sql`

This SQL script in Snowflake create top 100 BillBoard table and pipeline to ingest data from S3.

**Key Features:**
- A Stage to recreate S3 datalake in Snowflake.
- Including Tasks and Procedures to ingest data automatically.
- Add table to transfer and store the data, including Staging table, Historical table, Latest table.

---

### 3. `TrackArtistPipeline.sql`

This SQL script in Snowflake create artist and track table and pipeline to ingest data from S3.

---

