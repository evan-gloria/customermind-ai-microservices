from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
import feedparser
from vertexai.generative_models import GenerativeModel, Tool, FunctionDeclaration, Part
import os
import httpx
from typing import List, Dict, Any

app = FastAPI(title="Strategist Agent API")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)
EXPECTED_API_KEY = os.getenv("API_KEY")

def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    return api_key

class StrategistRequest(BaseModel):
    persona_brief: str

class CohortInsightRequest(BaseModel):
    offer_title: str
    offer_category: str
    cohort_data: list

class DataAgentRequest(BaseModel):
    prompt: str
    modeler_url: str  # The Orchestrator will tell the Agent where the database sandbox is
    history: List[Dict[str, Any]] = []

# 1. Define the Tool
def fetch_ozbargain_deals() -> str:
    feed = feedparser.parse("https://www.ozbargain.com.au/feed")
    deals = [f"- {entry.title}\n  Link: {entry.link}" for entry in feed.entries[:15]]
    return "\n".join(deals) if deals else "No live deals found."

oz_func = FunctionDeclaration(
    name="fetch_ozbargain_deals",
    description="Retrieves active deals trending on OzBargain Australia.",
    parameters={"type": "object", "properties": {}}
)
deal_hunter_tool = Tool(function_declarations=[oz_func])


# 2. Define the BigQuery Tool
bq_func = FunctionDeclaration(
    name="query_database",
    description="Executes a read-only Google Standard SQL query against the customer database.",
    parameters={
        "type": "object",
        "properties": {
            "sql_query": {"type": "string", "description": "The exact SQL SELECT query to execute."}
        },
        "required": ["sql_query"]
    }
)

# 3. Combine both tools into one Toolbelt! (Assuming oz_func is still defined at the top of this file)
agent_toolbelt = Tool(function_declarations=[oz_func, bq_func])

@app.post("/api/v1/strategize")
async def generate_strategy(request: StrategistRequest, api_key: str = Depends(verify_api_key)):
    try:
        model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")
        instruction = (
            "You are a Campaign Strategist. Read the provided 'Target Persona Brief'. "
            "You MUST use the `fetch_ozbargain_deals` tool to find 1-2 real-world deals matching the brief. "
            "Write the final Next-Best-Action marketing strategy incorporating those deals."
        )
        agent = GenerativeModel(model_name, tools=[deal_hunter_tool], system_instruction=instruction)
        
        chat = agent.start_chat()
        response = chat.send_message(f"Target Persona Brief:\n\n{request.persona_brief}")
        
        # THE FIX: Check the first candidate for function calls
        if response.candidates and response.candidates[0].function_calls:
            for call in response.candidates[0].function_calls:
                if call.name == "fetch_ozbargain_deals":
                    deals_text = fetch_ozbargain_deals()
                    response = chat.send_message(
                        Part.from_function_response(name="fetch_ozbargain_deals", response={"content": deals_text})
                    )
                    
        return {"status": "success", "strategy": response.text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/offer-insights")
async def generate_offer_insights(request: CohortInsightRequest, api_key: str = Depends(verify_api_key)):
    """Generates strategic marketing insights based on BigQuery cohort data."""
    try:
        # Format the BigQuery data into a readable string for the LLM
        data_summary = ""
        for cohort in request.cohort_data:
            data_summary += f"- {cohort['segment_name']}: {cohort['cohort_size']} users, Avg Age {cohort['avg_age']}, Avg Income ${cohort['avg_income']}\n"

        prompt = f"""
        You are a Chief Marketing Officer. Analyze this live campaign viability data.
        
        OFFER DETAILS:
        - Title: {request.offer_title}
        - Category: {request.offer_category}
        
        ELIGIBLE AUDIENCE (From BigQuery):
        {data_summary}
        
        TASK:
        Provide a brief, punchy strategic insight (max 3 paragraphs). 
        1. Why is the top cohort a good match for this specific offer?
        2. What specific marketing channel (e.g., TikTok, Email, LinkedIn) should we use to target them based on their age and income?
        3. What is the potential risk or blind spot of this campaign?
        """
        
        model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")
        model = GenerativeModel(model_name)
        response = model.generate_content(prompt)
        
        return {"strategic_insight": response.text}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Strategist Error: {str(e)}")
    

def map_ozbargain_category(title: str) -> str:
    """Heuristic router to map live deal titles to BigQuery categories."""
    title_lower = title.lower()
    if any(word in title_lower for word in ['laptop', 'pc', 'monitor', 'tv', 'phone', 'apple', 'samsung', 'ssd', 'tech']): return 'Tech'
    if any(word in title_lower for word in ['mobile', 'sim', 'nbn', 'telstra', 'optus', 'vodafone']): return 'Telco'
    if any(word in title_lower for word in ['flight', 'hotel', 'qantas', 'virgin', 'travel']): return 'Travel'
    if any(word in title_lower for word in ['woolworths', 'coles', 'aldi', 'groceries']): return 'Groceries'
    if any(word in title_lower for word in ['kfc', 'mcdonalds', 'beer', 'wine', 'food', 'pizza']): return 'Food'
    if any(word in title_lower for word in ['bunnings', 'tool', 'drill', 'hardware']): return 'Hardware'
    if any(word in title_lower for word in ['bank', 'card', 'cashback', 'loan', 'finance']): return 'Finance'
    return 'Retail'

@app.get("/api/v1/live-deals")
async def get_live_deals():
    """Fetches and categorizes the latest OzBargain deals for the UI."""
    try:
        feed = feedparser.parse("https://www.ozbargain.com.au/feed")
        deals = []
        for entry in feed.entries[:15]: # Grab the top 15
            deals.append({
                "title": entry.title,
                "category": map_ozbargain_category(entry.title),
                "link": entry.link
            })
        return {"offers": deals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/data-agent")
async def run_data_agent(request: DataAgentRequest, api_key: str = Depends(verify_api_key)):
    try:
        system_instruction = """
        You are an elite Data & Marketing Agent. You have two tools:
        1. `fetch_ozbargain_deals`: Fetches live market offers.
        2. `query_database`: Runs SQL on the `customermind_ai.v_agent_semantic_layer` table.
        
        DATABASE SCHEMA:
        - CustomerID (STRING)
        - Age (INT64)
        - Income (INT64)
        - NumWebVisitsMonth (INT64)
        - Total_Spend (FLOAT64)
        - segment_name (STRING) - E.g., 'High-Value Tech Professional'

        RULES:
        - NEVER guess data. ALWAYS use `query_database` to answer data questions.
        - You are strictly querying a flat view. Do NOT attempt to write JOIN statements.
        - If your SQL query returns an error, read the error and rewrite your SQL to fix it.

        # DATA GOVERNANCE & PII STRICT RULES
        You are operating in an enterprise environment with strict data privacy laws. 
        1. You must NEVER output Personally Identifiable Information (PII) to the user.
        2. PII includes, but is not limited to: CustomerIDs, names, email addresses, physical addresses, and phone numbers.
        3. If a user explicitly asks for specific Customer IDs or raw user data, you must politely refuse and offer to provide aggregate, anonymized statistics instead.
        4. If your internal SQL tool returns Customer IDs to you, you must aggregate or summarize the data in your final response without displaying the raw IDs.
        """
        model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")
        agent = GenerativeModel(model_name, tools=[agent_toolbelt], system_instruction=system_instruction)
        chat = agent.start_chat()

        memory_context = ""
        if request.history:
            memory_context = "PREVIOUS CONVERSATION CONTEXT:\n"
            for msg in request.history:
                role = "User" if msg["role"] == "user" else "Assistant"
                memory_context += f"{role}: {msg['content']}\n"
            memory_context += "\nCURRENT PROMPT TO ANSWER:\n"

        # Combine them
        full_prompt = f"{memory_context}{request.prompt}"
        
        response = chat.send_message(full_prompt)
        
        # 3. The Autonomous Agent Loop
        MAX_LOOPS = 4
        loop_count = 0
        
        while response.candidates and response.candidates[0].function_calls and loop_count < MAX_LOOPS:
            loop_count += 1
            tool_responses = []
            
            for call in response.candidates[0].function_calls:
                if call.name == "fetch_ozbargain_deals":
                    result = fetch_ozbargain_deals()
                    tool_responses.append(Part.from_function_response(name="fetch_ozbargain_deals", response={"content": result}))
                    
                elif call.name == "query_database":
                    sql = call.args.get("sql_query")
                    # Make a secure internal HTTP call to the Modeler Service
                    with httpx.Client() as client:
                        res = client.post(f"{request.modeler_url}/api/v1/query-sandbox", json={"query": sql}, timeout=20.0)
                        db_result = res.json()
                    tool_responses.append(Part.from_function_response(name="query_database", response=db_result))
                    
            # Send the tool outputs back to the LLM so it can continue reasoning
            response = chat.send_message(tool_responses)
            
        return {"response": response.text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))