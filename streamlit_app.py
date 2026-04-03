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
st.set_page_config(page_title="Enterprise AI Portfolio", page_icon="🧠", layout="wide")

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


# -----------------------------------------------------------------------------
# 🔒 Security: Secret Management & Environment Variables
# -----------------------------------------------------------------------------

try:
    # 1. Try Streamlit Cloud Secrets
    CONF_API_KEY = st.secrets["API_KEY"]
    CONF_ORCHESTRATOR_URL = st.secrets["URL_ORCHESTRATOR"]
except (FileNotFoundError, KeyError):
    # 2. Fallback to Local/Docker OS Variables
    CONF_API_KEY = os.getenv("API_KEY")
    CONF_ORCHESTRATOR_URL = os.getenv("URL_ORCHESTRATOR", "http://127.0.0.1:8000")


# --- HEADER ---
st.title("🧠 Customer Intelligence Platform")
st.markdown("**Enterprise AI Marketing Hub** | *Powered by AI Microservices & BigQuery*")
st.divider()


# --- SIDEBAR (Configuration & Guide) ---
with st.sidebar:
    
    # Platform Access
    api_key=CONF_API_KEY
    orchestrator_url = CONF_ORCHESTRATOR_URL

    st.markdown("### 🗺️ Platform Navigation")
    st.markdown("""
    <div style="font-size: 0.9em; margin-bottom: 15px;">
    <b>🎯 Persona Strategy Builder</b><br>
    Generate hyper-personalized, 1-to-1 marketing campaigns for individual users.<br><br>
    <b>📊 Campaign Viability Engine</b><br>
    Forecast audience reach and demographic fit for live market offers.<br><br>
    <b>🧠 AI Marketing Copilot</b><br>
    Chat directly with the data warehouse using natural language.
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # 🌟 NEW: Microservice System Status UI
    st.markdown("### 🟢 System Status")
    st.markdown("""
    <div style="font-size: 0.85em; color: #a3a8b8; margin-bottom: 15px;">
    ✅ Orchestrator Service (Gateway)<br>
    ✅ Data Modeler Service (BigQuery)<br>
    ✅ Profiler Service (Gemini Flash)<br>
    ✅ Strategic Service (Gemini Flash)<br>
    ✅ Reviewer Service (Llama 3)
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("### ⚙️ Engine Capabilities")
    st.caption("**The AI Microservices powering this platform:**")
    st.markdown("""
    <div style="font-size: 0.85em; color: #a3a8b8;">
    <b>1. 🟢 Data Engine:</b> Securely extracts and scores enterprise BigQuery profiles.<br><br>
    <b>2. 🧠 Persona AI:</b> Translates raw demographics into deep psychological profiles.<br><br>
    <b>3. ✍️ Campaign AI:</b> Scans live web feeds to draft targeted, high-conversion copy.<br><br>
    <b>4. ⚖️ Compliance AI:</b> Strictly audits all outputs for brand safety and tone.
    </div>
    """, unsafe_allow_html=True)
    
    # ---------------------------------------------------------
    # 🔒 SECURE MLOPS ADMIN PANEL (Environment Feature Flag)
    # ---------------------------------------------------------
    try:
        refresh_admin_key = st.secrets.get("REFRESH_ADMIN_KEY")
    except FileNotFoundError:
        refresh_admin_key = os.getenv("REFRESH_ADMIN_KEY")
        
    if refresh_admin_key:
        st.markdown("---")
        st.markdown(
            """<span style="font-size: 0.85em; color: gray;">
            <b>🛠️ LOCAL ADMIN MODE</b><br>
            Trigger the MLOps pipeline to retrain the K-Means model on fresh BigQuery data.
            </span>""", 
            unsafe_allow_html=True
        )
        
        if st.button("🔄 Retrain AI Segments", type="primary", use_container_width=True):
            with st.spinner("Initiating Orchestrated MLOps Pipeline..."):
                try:
                    headers = {"X-API-Key": refresh_admin_key} 
                    response = requests.post(f"{orchestrator_url}/tools/refresh-segments", headers=headers)
                    
                    if response.status_code == 200:
                        st.toast("✅ Pipeline triggered successfully!", icon="🚀")
                    else:
                        st.error(f"Failed to trigger pipeline: {response.text}")
                        
                except Exception as e:
                    st.error(f"Connection Error: {str(e)}")
    
    st.markdown("---")
    
    # 👨‍💻 Author & GitHub Link
    st.markdown(
        """
        <div style="text-align: center; padding-top: 10px;">
            <p style="font-size: 0.85em; color: #a3a8b8; margin-bottom: 5px;">
                Architected & Engineered by
            </p>
            <p style="font-size: 1em; font-weight: 600; margin-bottom: 10px;">
                Evan Gloria </p>
            <a href="https://github.com/evan-gloria/agentic-customermind-gcp" target="_blank" style="text-decoration: none;">
                <button style="background-color: #2e3138; color: white; border: none; padding: 8px 15px; border-radius: 5px; cursor: pointer; font-size: 0.85em; width: 100%;">
                    🐙 View Architecture on GitHub
                </button>
            </a>
        </div>
        """, 
        unsafe_allow_html=True
    )

    # 📝 Feedback Link
    st.markdown(
        """
        <div style="text-align: center; margin-top: 15px;">
            <a href="https://forms.gle/FHiJpDt8NhPd2rzG8" target="_blank" style="text-decoration: none; font-size: 0.9em; color: #4CAF50; font-weight: 600;">
                📝 Leave App Feedback
            </a>
        </div>
        """, 
        unsafe_allow_html=True
    )

# --- TABS INITIALIZATION ---
if not api_key:
    st.error("🚨 Security Error: API_KEY is missing from the environment. Please check your deployment secrets.")
    st.stop()
else:
    tab1, tab2, tab3 = st.tabs([
        "🎯 Persona Strategy Builder", 
        "📊 Campaign Viability Engine", 
        "🧠 AI Marketing Copilot"
    ])

    # ==========================================
    # TAB 1: THE PIPELINE ENGINE (Persona Strategy Builder)
    # ==========================================
    with tab1:
        st.markdown("### 🎯 Persona Strategy Builder")
        st.info("**The Goal:** Transform raw customer data into a ready-to-send, highly personalized marketing campaign.\n\n**How it works:** Enter a Customer ID to trigger a deterministic microservice pipeline. The Orchestrator extracts their data, builds a psychological profile, scouts live market deals, and drafts a targeted email with auditing capability for brand safety.")
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            customer_id = st.text_input("Enter Customer ID", value="5917880785599854719", max_chars=20)
        with col2:
            st.write("") 
            st.write("") 
            execute_btn = st.button("Customer Analysis 💡", type="primary", use_container_width=True)

        if customer_id and not customer_id.isdigit():
            st.error("Invalid Input: Customer ID must contain only numbers.")
            st.stop()

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
    # TAB 2: THE NEW OZBARGAIN ENGINE (Campaign Viability Engine)
    # ==========================================
    with tab2:
        header_col, btn_col = st.columns([4, 1])
        with header_col:
            st.markdown("### 📊 Campaign Viability Engine")
            st.info("**The Goal:** Validate whether a specific market offer has a large enough audience to be profitable.\n\n**How it works:** Select a live market deal to run a real-time Machine Learning model (`ML.PREDICT`) across the customer database. The engine instantly calculates the exact size, demographics, and expected revenue of the most viable target audience.")
        with btn_col:
            st.write("") 
            if st.button("🔄 Refresh Deals", use_container_width=True):
                fetch_live_offers_from_gateway.clear() 
                st.rerun() 
        
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
                strategic_insight = None 
                
                with st.status("🔍 Running Campaign Viability Engine...", expanded=True) as status:
                    ui_placeholder = st.empty()
                    tracked_steps = []
                    
                    try:
                        payload = {"offer_title": selected_offer_title, "category": selected_category}
                        
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
                                                display_text += f"⏳ <i>{clean_text}<span class='loading-dots'></span></i><br><br>" 
                                            else:
                                                display_text += f"✅ **{clean_text}**\n\n"
                                        ui_placeholder.markdown(display_text, unsafe_allow_html=True)
                                        
                                    elif stream_data.get("status") == "complete":
                                        cohort_data = stream_data.get("data", {}).get("top_cohorts", [])
                                        strategic_insight = stream_data.get("data", {}).get("strategic_insight", "No insight generated.")
                                        
                                        display_text = ""
                                        for step in tracked_steps:
                                            parts = step.split(" ", 1)
                                            clean_text = parts[1] if len(parts) > 1 else step
                                            display_text += f"✅ **{clean_text}**\n\n"
                                        ui_placeholder.markdown(display_text, unsafe_allow_html=True)
                                        status.update(label="✅ Viability Scan Complete", state="complete", expanded=False)
                                    
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
                    
                    if strategic_insight:
                        st.divider()
                        st.markdown("### 🧠 AI Strategist Insight")
                        st.info(strategic_insight)
                    else:
                        st.info(f"💡 **Data Note:** This data was aggregated in real-time from BigQuery using `ML.PREDICT`. The **{top_segment}** cohort represents the largest immediate revenue opportunity.")
                        
                else:
                    st.warning("No significant cohort matches found for this category.")

    # ==========================================
    # TAB 3: SELF-SERVICE DATA AGENT (AI Marketing Copilot)
    # ==========================================
    with tab3:
        st.markdown("### 💬 AI Marketing Copilot")
        st.info("**The Goal:** Bypass SQL and Jira tickets. Get instant answers to complex strategic questions.\n\n**How it works:** Use natural language to ask questions. The AI Copilot will securely write SQL, query the BigQuery data warehouse, and fetch live web data to give you instant, strategic answers.")
        st.write("Ask the AI to query the customer database or check live market offers for you.")
    
        if "messages" not in st.session_state:
            st.session_state.messages = []
            
        chat_container = st.container()
                
        with chat_container:
            for msg in st.session_state.messages:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

        prompt = None 
        
        if len(st.session_state.messages) == 0:
            st.write("") 
            st.caption("💡 **Suggested Queries:**")
            col1, col2, col3 = st.columns(3)
            
            if col1.button("📊 Database Math", use_container_width=True):
                prompt = "What is the average income of our most loyal customers?"
            if col2.button("🛒 Live Market Scan", use_container_width=True):
                prompt = "Are there any live travel deals on OzBargain right now?"
            if col3.button("🧠 Multi-Tool Strategy", use_container_width=True):
                prompt = "Find a live Tech deal on OzBargain, and tell me how many customers we have making over $100,000 to target it to."

        user_input = st.chat_input("E.g., What is the average income of the Tech segment?")
        if user_input:
            prompt = user_input
                
        # Trigger the Copilot if a prompt was fired
        if prompt:
            st.session_state.messages.append({"role": "user", "content": prompt})
            with chat_container:
                with st.chat_message("user"):
                    st.markdown(prompt)
                    
                # Trigger the Copilot and show spinner inside the container
                with st.chat_message("assistant"):
                    with st.spinner("🤖 AI Copilot is reasoning, querying tools, and writing SQL..."):
                        try:
                            recent_history = st.session_state.messages[:-1][-4:]
                            
                            payload = {
                                "prompt": prompt,
                                "history": recent_history
                            }
                            
                            res = requests.post(
                                f"{orchestrator_url}/api/v1/chat",
                                json=payload,
                                headers={"X-API-Key": api_key}
                            )
                            
                            if res.status_code == 200:
                                reply = res.json().get("response", "No response generated.")
                                st.markdown(reply)
                                
                                st.session_state.messages.append({"role": "assistant", "content": reply})
                                
                                st.rerun()
                                
                            else:
                                st.error(f"Backend Error: {res.text}")
                                
                        except Exception as e:
                            st.error(f"Connection Error: {e}")