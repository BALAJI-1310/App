"""
Azure AI Foundry Flask Chat Interface
(Clean UI + Managed Identity + Session-Based Chat + Web UI)
"""

import os
import logging
from flask import Flask, render_template_string, request, session, jsonify, redirect, url_for
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage
from azure.identity import DefaultAzureCredential

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Flask Setup ---
app = Flask(__name__)
app.secret_key = os.urandom(24)

# --- Configuration (Set your inference models endpoint here) ---
INFERENCE_ENDPOINT = "https://<your-resource>.services.ai.azure.com/models"
MODEL_DEPLOYMENT_NAME = "<your-model-deployment-name>"

if not INFERENCE_ENDPOINT or not MODEL_DEPLOYMENT_NAME:
    logger.error("INFERENCE_ENDPOINT or MODEL_DEPLOYMENT_NAME is not set. Please update the values in the script.")
else:
    logger.info(f"Using INFERENCE_ENDPOINT: {INFERENCE_ENDPOINT}")
    logger.info(f"Using MODEL_DEPLOYMENT_NAME: {MODEL_DEPLOYMENT_NAME}")

# --- Global Azure AI Client ---
client = None

class ScopedDefaultAzureCredential:
    """
    Wrap DefaultAzureCredential to request token with Cognitive Services scope
    """
    def __init__(self):
        self._credential = DefaultAzureCredential()
        self._scope = "https://cognitiveservices.azure.com/.default"

    def get_token(self, *args, **kwargs):
        return self._credential.get_token(self._scope, **kwargs)

def init_ai_client():
    """Initialize the Azure AI Foundry client with Managed Identity and correct token scope"""
    global client
    try:
        logger.info("Initializing Azure AI Foundry ChatCompletionsClient with Cognitive Services token scope...")
        credential = ScopedDefaultAzureCredential()
        client = ChatCompletionsClient(INFERENCE_ENDPOINT, credential)
        logger.info("Client initialized successfully using Managed Identity and Cognitive Services scope.")
    except Exception as e:
        logger.error(f"Failed to initialize Azure AI Client: {e}")
        client = None

# Initialize client on import
init_ai_client()

# --- HTML Template ---
HTML = """<Your existing HTML template unchanged>"""

# --- Routes ---
@app.route("/", methods=["GET"])
def index():
    if "chat" not in session:
        session["chat"] = []
    return render_template_string(HTML, chat=session.get("chat", []))


@app.route("/ask", methods=["POST"])
def ask():
    if client is None:
        logger.warning("AI Foundry client not initialized.")
        return jsonify({"answer": "AI Foundry client is not initialized. Please restart the app."})

    data = request.get_json()
    question = data.get("question", "").strip()
    if not question:
        return jsonify({"answer": "Please provide a valid question."})

    chat = session.get("chat", [])
    chat.append({"role": "user", "text": question})

    try:
        logger.info("Sending question to Azure AI Foundry...")

        messages = [
            SystemMessage(content="You are a helpful assistant."),
            UserMessage(content=question)
        ]

        response = client.complete(
            model=MODEL_DEPLOYMENT_NAME,
            messages=messages,
            temperature=0.7,
            max_tokens=500
        )

        answer = response.choices[0].message.content
        chat.append({"role": "agent", "text": answer})
        session["chat"] = chat
        return jsonify({"answer": answer})
    except Exception as e:
        logger.error(f"Error querying AI Foundry: {e}")
        error_msg = f"Error: {e}"
        chat.append({"role": "agent", "text": error_msg})
        session["chat"] = chat
        return jsonify({"answer": error_msg})


@app.route("/clear", methods=["POST"])
def clear_chat():
    session.pop("chat", None)
    return redirect(url_for("index"))
