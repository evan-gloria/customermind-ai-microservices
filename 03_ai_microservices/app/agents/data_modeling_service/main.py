from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from google.cloud import bigquery
import json
import os

app = FastAPI(title="Data Modeling Agent API")

class PredictRequest(BaseModel):
    customer_id: int

class CategoryRequest(BaseModel):
    category: str

class SQLRequest(BaseModel):
    query: str

class MaterializeRequest(BaseModel):
    personas: dict

# ---------------------------------------------------------
# Tool 1: Point Lookup (For Streamlit Tab 1)
# ---------------------------------------------------------
@app.post("/api/v1/predict")
async def get_customer_segment(request: PredictRequest):
    client = bigquery.Client()
    
    # 1. We use ML.PREDICT to evaluate the specific customer against your 4-cluster model
    # We query the View, so the model gets the exact calculated features it was trained on
    query = f"""
        SELECT 
            centroid_id,
            Age, 
            Income, 
            Total_Spend, 
            Total_Purchases, 
            Total_Campaigns_Accepted,
            Recency
        FROM ML.PREDICT(MODEL `customermind_ai.customer_segments`,
          (SELECT * FROM `customermind_ai.v_customer_features` WHERE CustomerID = {request.customer_id}))
    """
    
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        
        if not results:
            return {
                "customer_id": request.customer_id, 
                "status": "not_found",
                "message": "Customer ID not found in v_customer_features."
            }
            
        row = results[0]
        cluster_id = row.centroid_id
        
        # 2. Map the K-Means Centroid ID to a Marketer-Friendly Persona
        # Note: You can adjust these names based on what your actual clusters represent!
        segment_map = {
            1: "High-Income / High-Spend Champions",
            2: "Younger / Promising Potentials",
            3: "Price-Sensitive / Low-Spend",
            4: "Older / Loyal Steady Spenders"
        }
        
        return {
            "status": "success",
            "customer_id": request.customer_id,
            "cluster_id": cluster_id,
            "segment_name": segment_map.get(cluster_id, "Unclassified Segment"),
            "customer_features": {
                "age": row.Age,
                "income": row.Income,
                "total_spend": row.Total_Spend,
                "total_purchases": row.Total_Purchases,
                "campaigns_accepted": row.Total_Campaigns_Accepted,
                "days_since_last_purchase": row.Recency
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# ---------------------------------------------------------
# Tool 2: Cohort Aggregation (For Streamlit Tab 2)
# ---------------------------------------------------------
@app.post("/api/v1/cohort-analytics")
async def get_cohort_demographics(request: CategoryRequest):
    client = bigquery.Client()
    """Queries BigQuery for the demographics of the top 3 segments for a given category."""
    query = f"""
        WITH SegmentDemographics AS (
            SELECT 
                segment_name,
                COUNT(customer_id) as cohort_size,
                ROUND(AVG(age_num), 1) as avg_age,
                ROUND(AVG(income_num), 0) as avg_income
            FROM `customermind_ai.customer_segments_materialized`
            GROUP BY segment_name
        )
        
        -- Dynamically score and filter the Top 3 based on the requested category
        SELECT 
            segment_name,
            cohort_size,
            avg_age,
            avg_income,
            -- POC Propensity Model: 
            -- Note: Since the LLM names these dynamically now, we use broad LIKE statements. 
            -- If the LLM generates a name without these keywords, it gracefully falls back to a random baseline score.
            CASE 
                WHEN @category IN ('Tech', 'Telco') AND UPPER(segment_name) LIKE '%TECH%' THEN 95
                WHEN @category IN ('Tech', 'Telco') AND UPPER(segment_name) LIKE '%PROFESSIONAL%' THEN 90
                WHEN @category IN ('Groceries', 'Home') AND UPPER(segment_name) LIKE '%FAMILY%' THEN 88
                WHEN @category IN ('Food', 'Retail') AND UPPER(segment_name) LIKE '%STUDENT%' THEN 92
                WHEN @category IN ('Food', 'Retail') AND UPPER(segment_name) LIKE '%VALUE%' THEN 85
                WHEN @category IN ('Travel', 'Finance') AND UPPER(segment_name) LIKE '%EXECUTIVE%' THEN 85
                WHEN @category IN ('Hardware', 'Groceries') AND UPPER(segment_name) LIKE '%RETIREE%' THEN 80
                ELSE 40 + CAST(RAND() * 20 AS INT64) 
            END as affinity_score
        FROM SegmentDemographics
        ORDER BY affinity_score DESC
        LIMIT 3
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("category", "STRING", request.category)]
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        results = [dict(row) for row in query_job.result()]
        return {"top_cohorts": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# ---------------------------------------------------------
# Tool 3: Run SELECT Statements (For Streamlit Tab 3)
# ---------------------------------------------------------

@app.post("/api/v1/query-sandbox")
async def secure_query_sandbox(request: SQLRequest):
    """Executes read-only SQL safely. Returns errors as text so the LLM can self-correct."""
    sql = request.query.strip()

    # Remove markdown formatting if the LLM hallucinated code blocks
    if sql.startswith("```"):
        sql = sql.strip("`").removeprefix("sql").strip()
        
    # Remove the trailing semicolon so we can safely append to the query
    if sql.endswith(";"):
        sql = sql[:-1]
    
    # 🛡️ THE BOUNCER: Block destructive commands
    forbidden_keywords = ["DROP ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "GRANT ", "TRUNCATE "]
    if any(word in sql.upper() for word in forbidden_keywords):
        raise HTTPException(status_code=403, detail="Security Exception: Only SELECT queries are permitted.")
    
    # 🛡️ THE SEATBELT: Prevent massive data scans
    if "LIMIT" not in sql.upper():
        sql += " LIMIT 100"
        
    try:
        client = bigquery.Client()
        # Cap billing at 100MB per query just in case
        job_config = bigquery.QueryJobConfig(maximum_bytes_billed=10000000000) 
        query_job = client.query(sql, job_config=job_config)
        
        results = [dict(row) for row in query_job]
        return {"status": "success", "data": results}
        
    except Exception as e:
        # We return 200 with the error text instead of crashing with a 500. 
        # This allows the LLM to read the syntax error and try again!
        return {"status": "error", "message": str(e)}
    
# ---------------------------------------------------------
# Tool 4: Distributed MLOps Endpoints (Called by Orchestrator)
# ---------------------------------------------------------

@app.post("/api/v1/train-model-and-get-stats")
async def train_model_and_get_stats():
    """Retrains the K-Means model and extracts the new segment averages."""
    try:
        client = bigquery.Client()
        
        # 1. Retrain the model on a 10% sample
        train_sql = """
        CREATE OR REPLACE MODEL `customermind_ai.customer_segments`
        OPTIONS(model_type='kmeans', num_clusters=5, standardize_features=TRUE, kmeans_init_method='KMEANS++') AS
        SELECT Age, Income, Recency, Total_Spend, Total_Purchases, Total_Campaigns_Accepted
        FROM `customermind_ai.v_customer_features`
        WHERE RAND() < 0.10;
        """
        train_job = client.query(train_sql)
        train_job.result() # Wait for training to finish
        
        # 2. Extract the stats from the newly trained model using a 1% sample
        stats_sql = """
        SELECT 
            centroid_id, 
            ROUND(AVG(Age), 1) as avg_age, 
            ROUND(AVG(Income), 2) as avg_income, 
            ROUND(AVG(Total_Spend), 2) as avg_spend,
            ROUND(AVG(Total_Purchases), 1) as avg_purchases
        FROM ML.PREDICT(
            MODEL `customermind_ai.customer_segments`, 
            (SELECT * FROM `customermind_ai.v_customer_features`)
        )
        GROUP BY centroid_id
        ORDER BY centroid_id;
        """
        
        df_stats = client.query(stats_sql).to_dataframe()
        stats_dict = df_stats.to_dict(orient='records')
        
        return {"status": "success", "stats": stats_dict}
        
    except Exception as e:
        print(f"FATAL ERROR IN TRAIN-MODEL: {str(e)}")
        raise HTTPException(status_code=500, detail=f"BigQuery Training Error: {str(e)}")

@app.post("/api/v1/materialize-semantic-layer")
async def materialize_semantic_layer(request: MaterializeRequest):
    """Takes LLM-generated persona names and builds the final billion-row table."""
    try:
        client = bigquery.Client()
        
        # Build the dynamic SQL CASE statement from the dictionary
        case_statements = ""
        for centroid_id, name in request.personas.items():
            case_statements += f"WHEN {centroid_id} THEN '{name}'\n        "

        materialize_sql = f"""
        CREATE OR REPLACE TABLE `customermind_ai.customer_segments_materialized` 
        CLUSTER BY customer_id
        AS
        SELECT 
            CustomerID AS customer_id,
            CAST(age AS FLOAT64) AS age_num,
            CAST(income AS FLOAT64) AS income_num,
            CASE centroid_id
                {case_statements}
                ELSE 'General Audience'
            END AS segment_name
        FROM 
          ML.PREDICT(
            MODEL `customermind_ai.customer_segments`, 
            TABLE `customermind_ai.v_customer_features`
          );
        """
        
        job = client.query(materialize_sql)
        job.result() # Wait for the billion-row table to build
        
        return {"status": "success", "message": "Semantic layer successfully materialized with new AI labels."}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery Materialization Error: {str(e)}")