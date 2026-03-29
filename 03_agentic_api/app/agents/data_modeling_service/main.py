from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
import os

app = FastAPI(title="Data Modeling Agent API")

class PredictRequest(BaseModel):
    customer_id: int

class CategoryRequest(BaseModel):
    category: str

# ---------------------------------------------------------
# PATH 1: Point Lookup (For Streamlit Tab 1)
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
# PATH 2: Cohort Aggregation (For Streamlit Tab 2)
# ---------------------------------------------------------
@app.post("/api/v1/cohort-analytics")
async def get_cohort_demographics(request: CategoryRequest):
    client = bigquery.Client()
    """Queries BigQuery for the demographics of the top 3 segments for a given category."""
    query = f"""
        -- STEP 1: Run real-time inference using your BQML Model against the feature view
        WITH PredictedCustomers AS (
            SELECT 
                CustomerID AS customer_id,
                CAST(age AS FLOAT64) AS age_num,
                CAST(income AS FLOAT64) AS income_num,
                CASE Centroid_ID
                    WHEN 1 THEN 'High-Value Tech Professional'
                    WHEN 2 THEN 'Conservative Retiree'
                    WHEN 3 THEN 'Young Family / Upsizer'
                    WHEN 4 THEN 'Small Business Owner'
                    WHEN 5 THEN 'Student / Deal Hunter'
                    ELSE 'General Audience'
                END AS segment_name
            FROM ML.PREDICT(MODEL `customermind_ai.customer_segments`, 
                            (SELECT * FROM `customermind_ai.v_customer_features`))
        ),

        SegmentDemographics AS (
            SELECT 
                segment_name,
                COUNT(customer_id) as cohort_size,
                ROUND(AVG(age_num), 1) as avg_age,
                ROUND(AVG(income_num), 0) as avg_income
            FROM PredictedCustomers
            GROUP BY segment_name
        )
        
        -- STEP 3: Dynamically score and filter the Top 3 based on the requested category
        SELECT 
            segment_name,
            cohort_size,
            avg_age,
            avg_income,
            -- For this POC, we use a heuristic CASE statement to simulate a Propensity Model.
            -- It maps the incoming @category to the logical segment affinities.
            CASE 
                WHEN @category IN ('Tech', 'Telco') AND segment_name LIKE '%Tech%' THEN 95
                WHEN @category IN ('Groceries', 'Home') AND segment_name LIKE '%Family%' THEN 88
                WHEN @category IN ('Food', 'Retail') AND segment_name LIKE '%Student%' THEN 92
                WHEN @category IN ('Travel', 'Finance') AND segment_name LIKE '%Tech%' THEN 85
                WHEN @category IN ('Hardware', 'Groceries') AND segment_name LIKE '%Retiree%' THEN 80
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