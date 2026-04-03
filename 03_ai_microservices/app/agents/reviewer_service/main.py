import os
from openai import OpenAI
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

app = FastAPI(title="Reviewer Agent (Llama 3.3 via Groq)")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)
EXPECTED_API_KEY = os.getenv("API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MODEL_NAME = os.getenv("REVIEWER_MODEL_NAME")

def verify_api_key(api_key: str = Security(api_key_header)):
    if EXPECTED_API_KEY and api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    return api_key

class ReviewRequest(BaseModel):
    persona_brief: str
    strategy: str

@app.post("/api/v1/review")
async def review_strategy(request: ReviewRequest, api_key: str = Depends(verify_api_key)):
    try:
        # Pointing the standard OpenAI library to Groq's high-speed API
        client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=GROQ_API_KEY
        )
        
        system_prompt = (
            "You are a Senior Marketing Auditor. Grade the strategy against the persona.\n"
            "Check for: Budget alignment and Problem-Solution fit.\n"
            "Output: A 'PASS/FAIL' grade and a concise 1-sentence critique."
        )
        
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"PERSONA: {request.persona_brief}\nSTRATEGY: {request.strategy}"}
            ],
            temperature=0.2
        )
        
        return {"audit_results": response.choices[0].message.content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))