import streamlit as st
import requests
import time
import json
import pandas as pd
import os
from dotenv import load_dotenv

# Load the .env file if running locally
load_dotenv()

@st.cache_data(show_spinner=False, ttl=600) # Cache for 10 minutes max
def fetch_live_offers_from_gateway(url, key):
    try:
        res = requests.get(f"{url}/api/v1/live-offers", headers={"X-API-Key": key}, timeout=15)
        if res.status_code == 200:
            return res.json().get("offers", [])
    except Exception as e:
        st.error(f"Failed to connect to API Gateway for live deals.")
    return []

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Agentic AI Portfolio", page_icon="🧠", layout="wide")

# --- MOCK DATA (For Portfolio Demo) ---
live_offers = [
    {"id": "ozb-001", "title": "Woolworths Everyday Mobile: 50% Off First 3 Months", "category": "Telco"},
    {"id": "ozb-002", "title": "Qantas: Double Status Credits on Flights to Tokyo", "category": "Travel"},
    {"id": "ozb-003", "title": "Amazon AU: $10 Bonus Credit on $50 Gift Cards", "category": "Retail"},
    {"id": "ozb-004", "title": "Dell: 40% off XPS 15 Laptops", "category": "Tech"},
    {"id": "ozb-005", "title": "Coles: 10,000 Flybuys points on $100 spend", "category": "Groceries"}
]

# --- CSS ANIMATION ---
st.markdown("""
<style>
.loading-dots::after {
  content: '.';
  animation: dots 1.5s steps(5, end) infinite;
}
@keyframes dots {
  0%, 20% { color: rgba(0,0,0,0); text-shadow: .25em 0 0 rgba(0,0,0,0), .5em 0 0 rgba(0,0,0,0); }
  40% { color: inherit; text-shadow: .25em 0 0 rgba(0,0,0,0), .5em 0 0 rgba(0,0,0,0); }
  60% { text-shadow: .25em 0 0 inherit, .5em 0 0 rgba(0,0,0,0); }
  80%, 100% { text-shadow: .25em 0 0 inherit, .5em 0 0 inherit; }
}
</style>
""", unsafe_allow_html=True)

# --- HEADER ---
st.title("🧠 Enterprise Multi-Agent Orchestration")
st.markdown("**Distributed Microservices Architecture** | *API Gateway Pattern*")
st.divider()

# --- SIDEBAR (Configuration) ---
with st.sidebar:
    st.header("⚙️ System Configuration")
    st.info("Point this UI to your central Orchestrator API Gateway.")
    
    orchestrator_url = os.getenv("URL_ORCHESTRATOR", "http://127.0.0.1:8000")
    
    st.divider()
    api_key = st.text_input("Master API Key", type="password", help="Enter the secure API key to authenticate.")
    
    st.markdown("---")
    st.markdown("### 🛠️ Microservice Architecture")
    st.caption("**UI → API Gateway → Distributed Agents**")
    st.markdown("""
    <div style="font-size: 0.85em; color: #a3a8b8;">
    <b>1. 🟢 Modeler:</b> BigQuery Data Extraction <br>
    <b>2. 🧠 Profiler:</b> Gemini 2.5 Flash Persona <br>
    <b>3. ✍️ Strategist:</b> Gemini 2.5 Flash Campaign <br>
    <b>4. ⚖️ Reviewer:</b> Llama 3.3 Strict Auditing
    </div>
    """, unsafe_allow_html=True)

# --- TABS INITIALIZATION ---
if not api_key:
    st.warning("👈 Please enter your Master API Key in the sidebar to unlock the dashboard.")
else:
    tab1, tab2 = st.tabs(["👤 Audience-Led (Pipeline)", "🛒 Offer-Led (BigQuery Analytics)"])

    # ==========================================
    # TAB 1: THE PIPELINE ENGINE
    # ==========================================
    with tab1:
        st.markdown("### 🏁 Execute Campaign Pipeline")
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            customer_id = st.number_input("Enter Customer ID", min_value=1, value=4141, step=1)
        with col2:
            st.write("") 
            st.write("") 
            execute_btn = st.button("Run AI Factory 🚀", type="primary", use_container_width=True)

        if execute_btn:
            headers = {"Content-Type": "application/json", "X-API-Key": api_key}
            payload = {"customer_id": int(customer_id)}
            
            start_time = time.time()
            final_data = {} 
            
            with st.status("🚀 Orchestrator API Gateway Connected...", expanded=True) as status:
                ui_placeholder = st.empty()
                tracked_steps = []
                
                try:
                    with requests.post(f"{orchestrator_url}/api/v1/generate-campaign", json=payload, headers=headers, stream=True) as response:
                        if response.status_code != 200:
                            st.error(f"Gateway Error {response.status_code}")
                            st.stop()
                            
                        for line in response.iter_lines():
                            if line:
                                stream_data = json.loads(line.decode('utf-8'))
                                
                                if stream_data.get("status") == "update":
                                    tracked_steps.append(stream_data.get("message"))
                                    display_text = ""
                                    for i, step in enumerate(tracked_steps):
                                        parts = step.split(" ", 1)
                                        clean_text = parts[1] if len(parts) > 1 else step
                                        if i == len(tracked_steps) - 1:
                                            display_text += f"⏳ <i>{clean_text}<span class='loading-dots'></span></i><br><br>" 
                                        else:
                                            display_text += f"✅ **{clean_text}**\n\n"
                                    ui_placeholder.markdown(display_text, unsafe_allow_html=True)
                                    
                                elif stream_data.get("status") == "complete":
                                    display_text = ""
                                    for step in tracked_steps:
                                        parts = step.split(" ", 1)
                                        clean_text = parts[1] if len(parts) > 1 else step
                                        display_text += f"✅ **{clean_text}**\n\n"
                                    ui_placeholder.markdown(display_text, unsafe_allow_html=True)
                                    
                                    final_data = stream_data.get("pipeline_results", {})
                                    status.update(label="✅ Pipeline Execution Complete!", state="complete", expanded=False)
                                    
                except Exception as e:
                    st.error(f"Connection Error: {e}")
                    status.update(label="❌ Connection Failed.", state="error")
                    st.stop()

            duration = round(time.time() - start_time, 2)
            st.success(f"End-to-end pipeline executed successfully in {duration} seconds.")
            
            segment_data = final_data.get("segment_data", {})
            persona_brief = final_data.get("persona_brief", "No brief generated.")
            strategy = final_data.get("executable_strategy", "No strategy generated.")
            audit = final_data.get("audit_results", "No audit results.")

            st.markdown(f"### 📊 Customer Insights: {segment_data.get('segment_name', 'Unknown Segment')}")
            
            res_col1, res_col2 = st.columns(2)
            with res_col1:
                st.subheader("🕵️ Persona Brief")
                st.info(persona_brief)
                st.subheader("⚖️ Reviewer Audit Result")
                st.warning(audit)
                
            with res_col2:
                st.subheader("🎯 Final Executable Strategy")
                st.markdown(strategy)

    # ==========================================
    # TAB 2: THE NEW OZBARGAIN ENGINE
    # ==========================================
    with tab2:
        header_col, btn_col = st.columns([4, 1])
        with header_col:
            st.markdown("### 🛒 Campaign Viability Engine")
            st.write("Scan the database to find the eligible audience for a live market offer.")
        with btn_col:
            st.write("") # Spacing alignment
            if st.button("🔄 Refresh Deals", use_container_width=True):
                fetch_live_offers_from_gateway.clear() # 🌟 This instantly wipes the cache!
                st.rerun() # Forces the UI to reload and fetch fresh data
        
        # Fetch the data (will use cache unless the button was just clicked)
        live_offers = fetch_live_offers_from_gateway(orchestrator_url, api_key)
        
        if not live_offers:
            st.warning("⚠️ Waiting for live deals to populate. Please check your Gateway connection.")
        else:
            selected_offer_title = st.selectbox(
                "Select Live OzBargain Offer", 
                options=[offer["title"] for offer in live_offers]
            )
            
            selected_category = next((offer["category"] for offer in live_offers if offer["title"] == selected_offer_title), "General")
            st.caption(f"**System Router:** Mapped this deal to the **'{selected_category}'** category for BigQuery inference.")
            
            analyze_btn = st.button("Query BigQuery & Calculate Reach 📊", type="secondary")
            
            if analyze_btn:
                cohort_data = None
                strategic_insight = None  # 🌟 NEW: Initialize the LLM insight variable
                
                with st.status("🔍 Running Campaign Viability Engine...", expanded=True) as status:
                    ui_placeholder = st.empty()
                    tracked_steps = []
                    
                    try:
                        # 🌟 NEW: Create the JSON payload for the POST request
                        payload = {"offer_title": selected_offer_title, "category": selected_category}
                        
                        # 🌟 NEW: Change to requests.post and use the new /analyze-offer endpoint
                        with requests.post(
                            f"{orchestrator_url}/api/v1/analyze-offer", 
                            json=payload,
                            headers={"X-API-Key": api_key},
                            stream=True
                        ) as res:
                            
                            if res.status_code != 200:
                                st.error(f"Gateway Error {res.status_code}")
                                st.stop()
                                
                            for line in res.iter_lines():
                                if line:
                                    stream_data = json.loads(line.decode('utf-8'))
                                    
                                    if stream_data.get("status") == "update":
                                        tracked_steps.append(stream_data.get("message"))
                                        display_text = ""
                                        for i, step in enumerate(tracked_steps):
                                            parts = step.split(" ", 1)
                                            clean_text = parts[1] if len(parts) > 1 else step
                                            if i == len(tracked_steps) - 1:
                                                display_text += f"⏳ <i>{clean_text} <span class='loading-dots'></span></i><br><br>" 
                                            else:
                                                display_text += f"✅ **{clean_text}**\n\n"
                                        ui_placeholder.markdown(display_text, unsafe_allow_html=True)
                                        
                                    elif stream_data.get("status") == "complete":
                                        cohort_data = stream_data.get("data", {}).get("top_cohorts", [])
                                        # 🌟 NEW: Extract the strategic insight from the backend payload
                                        strategic_insight = stream_data.get("data", {}).get("strategic_insight", "No insight generated.")
                                        
                                        display_text = ""
                                        for step in tracked_steps:
                                            parts = step.split(" ", 1)
                                            clean_text = parts[1] if len(parts) > 1 else step
                                            display_text += f"✅ **{clean_text}**\n\n"
                                        ui_placeholder.markdown(display_text, unsafe_allow_html=True)
                                        status.update(label="✅ Viability Scan Complete", state="complete", expanded=False)
                                    
                                    # 🌟 RESTORED: Catching backend errors so it doesn't fail silently
                                    elif stream_data.get("status") == "error":
                                        st.error(f"Backend Error: {stream_data.get('message')}")
                                        status.update(label="❌ Pipeline Failed", state="error", expanded=True)
                                        st.stop()
                                        
                    except Exception as e:
                        st.error(f"Connection Error: {e}")
                        status.update(label="❌ Query Failed", state="error")
                        st.stop()
                
                if cohort_data:
                    st.success(f"**Campaign Viability:** High propensity match found for **{selected_category}**.")
                    
                    df = pd.DataFrame(cohort_data)
                    df.set_index("segment_name", inplace=True)
                        
                    top_segment = df.index[0]
                    total_reach = df["cohort_size"].sum()
                    avg_income_top = df.loc[top_segment, "avg_income"]
                        
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Top Recommended Segment", top_segment)
                    m2.metric("Total Eligible Reach", f"{total_reach:,} Users")
                    m3.metric(f"Avg Income ({top_segment})", f"${avg_income_top:,.0f}")
                        
                    st.markdown("#### Demographic Distribution (Top 3 Cohorts)")
                        
                    colA, colB = st.columns(2)
                    with colA:
                        st.write("**Cohort Size by Segment**")
                        st.bar_chart(df["cohort_size"], color="#1E88E5")
                    with colB:
                        st.write("**Average Age by Segment**")
                        st.bar_chart(df["avg_age"], color="#FFC107")
                    
                    # 🌟 NEW: Display the LLM Strategist Insight below the charts
                    if strategic_insight:
                        st.divider()
                        st.markdown("### 🧠 AI Strategist Insight")
                        st.info(strategic_insight)
                    else:
                        st.info(f"💡 **Data Note:** This data was aggregated in real-time from BigQuery using `ML.PREDICT`. The **{top_segment}** cohort represents the largest immediate revenue opportunity.")
                        
                else:
                    st.warning("No significant cohort matches found for this category.")