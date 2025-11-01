import logging
from flask import Flask, request, session, jsonify, redirect, url_for, render_template_string
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.agents.models import ListSortOrder

# --- Flask Setup ---
app = Flask(__name__)
app.secret_key = "your-secret-key"  # Replace with a secure key in production

# --- Azure AI SDK Configuration ---
ENDPOINT = "https://eastus.api.azureml.ms/api/projects/ridersquery"  # Directly provided
AGENT_ID = "asst_GCfvXZLd1uuBt6MH0ZFxt9t"  # Directly provided

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Initialize Azure AI Project Client ---
try:
    project_client = AIProjectClient(
        endpoint=ENDPOINT,
        credential=DefaultAzureCredential()
    )
    logger.info(" Connected to Azure AI Project successfully.")
except Exception as e:
    logger.error(f" Failed to initialize AIProjectClient: {e}")

# --- HTML Placeholder ---
HTML = """{{ your HTML goes here }}"""

# --- Routes ---
@app.route("/", methods=["GET"])
def index():
    if "chat" not in session:
        session["chat"] = []
    return render_template_string(HTML, chat=session.get("chat", []))


@app.route("/ask", methods=["POST"])
def ask():
    data = request.get_json()
    question = data.get("question", "").strip()
    if not question:
        return jsonify({"answer": "Please provide a valid question."})

    chat = session.get("chat", [])
    chat.append({"role": "user", "text": question})

    try:
        logger.info(" Creating thread and sending message...")

        # Create thread if not already created
        if "thread_id" not in session:
            thread = project_client.agents.threads.create()
            session["thread_id"] = thread.id
        else:
            thread = project_client.agents.threads.get(session["thread_id"])

        # Send user message
        project_client.agents.messages.create(
            thread_id=thread.id,
            role="user",
            content=question
        )

        # Run the agent
        run = project_client.agents.runs.create_and_process(
            thread_id=thread.id,
            agent_id=AGENT_ID
        )

        # Check run status
        if run.status == "failed":
            error_msg = f"Run failed: {run.last_error}"
            logger.error(error_msg)
            chat.append({"role": "agent", "text": error_msg})
            session["chat"] = chat
            return jsonify({"answer": error_msg})

        # Fetch messages and extract assistant reply using text_messages[-1].text.value
        messages = project_client.agents.messages.list(
            thread_id=thread.id,
            order=ListSortOrder.ASCENDING
        )

        agent_reply = next(
            (msg.text_messages[-1].text.value for msg in reversed(list(messages))
             if msg.role.lower() == "assistant" and msg.text_messages),
            "No response from agent."
        )

        chat.append({"role": "agent", "text": agent_reply})
        session["chat"] = chat
        return jsonify({"answer": agent_reply})

    except Exception as e:
        logger.error(f" Error communicating with Azure AI Agent: {e}")
        error_msg = f"Error: {e}"
        chat.append({"role": "agent", "text": error_msg})
        session["chat"] = chat
        return jsonify({"answer": error_msg})


@app.route("/clear", methods=["POST"])
def clear_chat():
    session.pop("chat", None)
    session.pop("thread_id", None)
    return redirect(url_for("index"))
