import json
import httpx
import os
import requests
from fastapi import FastAPI, HTTPException, Security, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from typing import List, Dict, Any

app = FastAPI(title="Campaign Orchestrator API Gateway")


# Security
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)
# ---------------------------------------------------------
# 1. LOAD THE KEYS FROM THE ENVIRONMENT (Injected by GCP)
# ---------------------------------------------------------
EXPECTED_API_KEY = os.getenv("API_KEY")
EXPECTED_ADMIN_KEY = os.getenv("REFRESH_ADMIN_KEY") # This will hold your ai-segment-refresher-api-key

# ---------------------------------------------------------
# 2. STANDARD VERIFY FUNCTION (For Chat / Agents)
# ---------------------------------------------------------
def verify_api_key(api_key: str = Security(api_key_header)):
    if EXPECTED_API_KEY and api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    return api_key

# ---------------------------------------------------------
# 3. NEW ADMIN VERIFY FUNCTION (For MLOps only)
# ---------------------------------------------------------
def verify_admin_key(api_key: str = Security(api_key_header)):
    # Block the request if the key doesn't match the Admin Secret
    if not EXPECTED_ADMIN_KEY or api_key != EXPECTED_ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden: Invalid Admin Key")
    return api_key

class ChatPrompt(BaseModel):
    prompt: str
    history: List[Dict[str, Any]] = [] # 🌟 NEW

class CampaignRequest(BaseModel):
    customer_id: int

class OfferAnalysisRequest(BaseModel):
    offer_title: str
    category: str


# In production, these would be Environment Variables. 
SERVICES = {
    "modeler": os.getenv("URL_MODELER", "https://data-modeling-service-xxx.a.run.app/api/v1/predict"),
    "profiler": os.getenv("URL_PROFILER", "https://profiler-service-xxx.a.run.app/api/v1/profile"),
    "strategist": os.getenv("URL_STRATEGIST", "https://strategist-service-xxx.a.run.app/api/v1/strategize"),
    "reviewer": os.getenv("URL_REVIEWER", "https://reviewer-service-xxx.a.run.app/api/v1/review"),
    "analytics": os.getenv("URL_ANALYTICS", "https://data-modeling-service-xxx.a.run.app/api/v1/cohort-analytics"),
}

@app.post("/api/v1/generate-campaign")
async def run_campaign_pipeline(request: CampaignRequest, api_key: str = Depends(verify_api_key)):
    headers = {"Content-Type": "application/json", "X-API-Key": EXPECTED_API_KEY}
    
    # NEW: The Async Generator for Streaming
    async def event_stream():
        # We put the client inside the generator so it stays open while streaming
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                # --- STEP 1: Data Modeler ---
                yield json.dumps({"status": "update", "message": "🟢 Gateway: Extracting segment data via Modeler..."}) + "\n"
                res_model = await client.post(SERVICES["modeler"], json={"customer_id": request.customer_id}, headers=headers)
                res_model.raise_for_status()
                model_data = res_model.json()

                # --- STEP 2: Profiler ---
                yield json.dumps({"status": "update", "message": "🧠 Gateway: Profiler Agent (Gemini) building persona..."}) + "\n"
                res_prof = await client.post(SERVICES["profiler"], json={"customer_id": request.customer_id, "model_context": model_data}, headers=headers)
                res_prof.raise_for_status()
                persona_brief = res_prof.json().get("persona_brief")

                # --- STEP 3: Strategist ---
                yield json.dumps({"status": "update", "message": "✍️ Gateway: Strategist Agent (Gemini) drafting campaign..."}) + "\n"
                res_strat = await client.post(SERVICES["strategist"], json={"persona_brief": persona_brief}, headers=headers)
                res_strat.raise_for_status()
                strategy = res_strat.json().get("strategy")

                # --- STEP 4: Reviewer ---
                yield json.dumps({"status": "update", "message": "⚖️ Gateway: Reviewer Agent (Llama 3.3 via Groq) auditing strategy..."}) + "\n"
                res_rev = await client.post(SERVICES["reviewer"], json={"persona_brief": persona_brief, "strategy": strategy}, headers=headers)
                res_rev.raise_for_status()
                audit_results = res_rev.json().get("audit_results")

                # --- FINAL PAYLOAD ASSEMBLY ---
                final_payload = {
                    "status": "complete",
                    "pipeline_results": {
                        "customer_id": request.customer_id,
                        "segment_data": model_data,
                        "persona_brief": persona_brief,
                        "executable_strategy": strategy,
                        "audit_results": audit_results
                    }
                }
                yield json.dumps(final_payload) + "\n"

            # NEW: Graceful error streaming
            except httpx.HTTPStatusError as e:
                error_msg = {"status": "error", "message": f"Pipeline Failed at {e.request.url}: {e.response.text}"}
                yield json.dumps(error_msg) + "\n"
            except Exception as e:
                error_msg = {"status": "error", "message": f"Internal Orchestration Error: {str(e)}"}
                yield json.dumps(error_msg) + "\n"

    # Return the stream with the correct NDJSON media type
    return StreamingResponse(event_stream(), media_type="application/x-ndjson")

@app.post("/api/v1/analyze-offer")
async def analyze_offer_viability(request: OfferAnalysisRequest, api_key: str = Depends(verify_api_key)):
    """Routes category to BigQuery, then routes the results to the Strategist LLM."""
    headers = {"Content-Type": "application/json", "X-API-Key": EXPECTED_API_KEY}
    
    async def event_stream():
        async with httpx.AsyncClient(timeout=45.0) as client:
            try:
                # --- STEP 1: Modeler (BigQuery Extraction) ---
                yield json.dumps({"status": "update", "message": f"🔍 Gateway: Querying BigQuery for '{request.category}' cohorts..."}) + "\n"
                
                
                res_bq = await client.post(SERVICES["analytics"], json={"category": request.category}, headers=headers)
                res_bq.raise_for_status()
                cohort_data = res_bq.json().get("top_cohorts", [])
                
                # --- STEP 2: Strategist (LLM Insights) ---
                yield json.dumps({"status": "update", "message": "🧠 Gateway: Strategist Agent analyzing cohort viability..."}) + "\n"
                
                llm_payload = {
                    "offer_title": request.offer_title,
                    "offer_category": request.category,
                    "cohort_data": cohort_data
                }
                res_strat = await client.post(SERVICES["strategist"].replace("/strategize", "/offer-insights"), json=llm_payload, headers=headers)
                res_strat.raise_for_status()
                strategic_insight = res_strat.json().get("strategic_insight")
                
                # --- FINAL PAYLOAD ---
                final_payload = {
                    "status": "complete",
                    "data": {
                        "top_cohorts": cohort_data,
                        "strategic_insight": strategic_insight
                    }
                }
                yield json.dumps(final_payload) + "\n"
                
            except httpx.HTTPStatusError as e:
                yield json.dumps({"status": "error", "message": f"Service Error: {e.response.text}"}) + "\n"
            except Exception as e:
                yield json.dumps({"status": "error", "message": f"Internal Gateway Error: {str(e)}"}) + "\n"

    return StreamingResponse(event_stream(), media_type="application/x-ndjson")

@app.get("/api/v1/live-offers")
async def fetch_live_offers(api_key: str = Depends(verify_api_key)):
    """Routes the UI request to the Strategist service to get live deals."""
    headers = {"Content-Type": "application/json", "X-API-Key": EXPECTED_API_KEY}
    
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            # Dynamically target the new /live-deals endpoint on the Strategist
            strat_url = SERVICES["strategist"].replace("/strategize", "/live-deals")
            
            response = await client.get(strat_url, headers=headers)
            response.raise_for_status()
            
            return {
                "status": "success",
                "offers": response.json().get("offers", [])
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch live offers: {str(e)}")
        

@app.post("/api/v1/chat")
async def chat_with_data_agent(request: ChatPrompt, api_key: str = Depends(verify_api_key)):
    """Routes chat requests to the Multi-Tool Strategist."""
    try:
        # Extract the base URLs dynamically
        modeler_base = SERVICES["analytics"].split("/api/v1")[0]
        strat_url = SERVICES["strategist"].split("/api/v1")[0] + "/api/v1/data-agent"
        
        payload = {
            "prompt": request.prompt,
            "modeler_url": modeler_base,
            "history": request.history
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            res = await client.post(strat_url, json=payload, headers={"X-API-Key": EXPECTED_API_KEY})
            res.raise_for_status()
            return res.json()
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent Gateway Error: {str(e)}")
    
def execute_refresh_workflow():
    """Background task executed by the Orchestrator"""
    
    # Define the service URLs
    data_query_service = SERVICES["modeler"].replace("/predict", "/train-model-and-get-stats")
    customer_profile_service = SERVICES["profiler"].replace("/profile", "/generate-personas")
    data_saved_service = SERVICES["modeler"].replace("/predict", "/materialize-semantic-layer")
    
    internal_headers = {"Content-Type": "application/json", "X-API-Key": EXPECTED_API_KEY}
    
    try:
        print("1. Telling Data Modeler to train K-Means and fetch stats...")
        response_1 = requests.post(data_query_service, headers=internal_headers)
        response_1.raise_for_status()
        raw_stats = response_1.json().get("stats") 
        
        print("2. Sending raw stats to Profiler Service for Gemini to name...")
        response_2 = requests.post(customer_profile_service, json={"stats": raw_stats}, headers=internal_headers)
        response_2.raise_for_status()
        persona_mapping = response_2.json().get("personas") 
        
        print("3. Sending Gemini's names back to Data Modeler to update BigQuery...")
        response_3 = requests.post(data_saved_service, json={"personas": persona_mapping}, headers=internal_headers)
        response_3.raise_for_status()
        
        print("Workflow Complete!")
    except Exception as e:
        print(f"Background Workflow Failed: {str(e)}")

@app.post("/tools/refresh-segments")
async def trigger_refresh_tool(background_tasks: BackgroundTasks, api_key: str = Depends(verify_admin_key)):
    """
    The tool endpoint that your main LLM agent calls when 
    the user asks to refresh the data.
    """
    # Run the heavy workflow in the background
    background_tasks.add_task(execute_refresh_workflow)
    
    return {
        "status": "success",
        "message": "I have initiated the background workflow. The Data Modeler is currently retraining the database, and the Profiler is standing by to evaluate the new segments."
    }