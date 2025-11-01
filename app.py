import os
import logging
from flask import Flask, render_template_string, request, session, jsonify, redirect, url_for
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import ListOrder

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Flask Setup ---
app = Flask(__name__)
app.secret_key = os.urandom(24)

# --- Azure AI SDK Configuration ---
ENDPOINT = "https://eastus.api.azureml.ms/api/projects/ridersquery"  # Replace with your actual endpoint
AGENT_ID = "asst_GCfvXZLd1uuBt6MH0ZFxt9t"  # Replace with your actual agent ID

# Initialize Azure AI Project client
try:
    project_client = AIProjectClient(
        endpoint=ENDPOINT,
        credential=DefaultAzureCredential()
    )
    logger.info("✅ Connected to Azure AI Project successfully.")
except Exception as e:
    logger.error(f"❌ Failed to initialize AIProjectClient: {e}")

# --- HTML Template ---
HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Azure AI Chat</title>
</head>
<body>
    <h1>Chat with Azure AI Agent</h1>
    <div id="chat">
        {% for message in chat %}
            <p><strong>{{ message.role }}:</strong> {{ message.text }}</p>
        {% endfor %}
    </div>
    <form id="chatForm" method="post" action="/ask">
        <input type="text" name="question" placeholder="Ask something..." required>
        <button type="submit">Send</button>
    </form>
    <form method="post" action="/clear">
        <button type="submit">Clear Chat</button>
    </form>
</body>
</html>
"""

# --- Routes ---
@app.route("/", methods=["GET"])
def index():
    if "chat" not in session:
        session["chat"] = []
    return render_template_string(HTML, chat=session.get("chat", []))


@app.route("/ask", methods=["POST"])
def ask():
    question = request.form.get("question", "").strip()
    if not question:
        return jsonify({"answer": "Please provide a valid question."})

    chat = session.get("chat", [])
    chat.append({"role": "user", "text": question})

    try:
        logger.info("Sending message to Azure AI Agent...")

        # Get agent
        agent = project_client.agents.get_agent(AGENT_ID)

        # Create thread if not exists
        if "thread_id" not in session:
            thread = project_client.agents.create_thread(
                agent_id=agent.id,
                role="user",
                content=question
            )
            session["thread_id"] = thread.id
        else:
            thread = project_client.agents.get_thread(session["thread_id"])

            # Send message
            project_client.agents.messages.create(
                thread_id=thread.id,
                role="user",
                content=question
            )

        # Run agent
        run = project_client.agents.runs.create_and_process(thread_id=thread.id)

        if run.status == "failed":
            error_msg = f"Run failed: {run.last_error}"
            logger.error(error_msg)
            chat.append({"role": "agent", "text": error_msg})
        else:
            # Get messages
            messages = project_client.agents.messages.list(
                thread_id=thread.id,
                order=ListOrder.ASCENDING
            )
            agent_reply = next(
                (msg.text.message for msg in reversed(messages) if msg.role.lower() == "assistant"),
                "No response from agent."
            )
            chat.append({"role": "agent", "text": agent_reply})

        session["chat"] = chat
        return redirect(url_for("index"))

    except Exception as e:
        logger.error(f"Error communicating with Azure AI Agent: {e}")
        error_msg = f"Error: {e}"
        chat.append({"role": "agent", "text": error_msg})
        session["chat"] = chat
        return redirect(url_for("index"))


@app.route("/clear", methods=["POST"])
def clear_chat():
    session.pop("chat", None)
    session.pop("thread_id", None)
    return redirect(url_for("index"))
