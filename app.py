import os
import time
import logging
from flask import Flask, render_template_string, request, session, jsonify, redirect, url_for
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Flask Setup ---
app = Flask(__name__)
app.secret_key = os.urandom(24)

# --- Azure AI SDK Configuration ---
ENDPOINT = "https://eastus.api.azureml.ms/api/projects/ridersquery"  # 🔁 Replace with your endpoint
AGENT_ID = "asst_GCfvXZLd1uuBt6MH0ZFxt9t"  # 🔁 Replace with your agent ID

# --- Initialize Azure AI Project client ---
try:
    project_client = AIProjectClient(
        endpoint=ENDPOINT,
        credential=DefaultAzureCredential()
    )
    logger.info("✅ Connected to Azure AI Project successfully.")
except Exception as e:
    logger.error(f"❌ Failed to initialize AIProjectClient: {e}")
    project_client = None


# --- HTML Template (basic chat UI) ---
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Azure AI Agent Chat</title>
    <style>
        body { font-family: Arial; margin: 40px; background: #fafafa; }
        .chat-box { border: 1px solid #ccc; padding: 20px; background: #fff; width: 600px; margin: 0 auto; }
        .message { margin: 10px 0; }
        .user { color: blue; }
        .agent { color: green; }
        textarea { width: 100%; height: 80px; }
    </style>
</head>
<body>
    <div class="chat-box" id="chat-box">
        {% for msg in chat %}
            <div class="message {{ msg.role }}">
                <strong>{{ msg.role.capitalize() }}:</strong> {{ msg.text }}
            </div>
        {% endfor %}
    </div>

    <form id="chat-form">
        <textarea id="question" placeholder="Ask something..."></textarea><br>
        <button type="submit">Send</button>
    </form>

    <form method="POST" action="{{ url_for('clear_chat') }}">
        <button type="submit">Clear Chat</button>
    </form>

    <script>
        document.getElementById("chat-form").addEventListener("submit", async (e) => {
            e.preventDefault();
            const question = document.getElementById("question").value.trim();
            if (!question) return;

            const res = await fetch("/ask", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({question})
            });

            const data = await res.json();
            window.location.reload();
        });
    </script>
</body>
</html>
"""


# --- Flask Routes ---
@app.route("/", methods=["GET"])
def index():
    if "chat" not in session:
        session["chat"] = []
    return render_template_string(HTML, chat=session.get("chat", []))


@app.route("/ask", methods=["POST"])
def ask():
    if not project_client:
        return jsonify({"answer": "Server not connected to Azure AI Project."}), 500

    data = request.get_json()
    question = data.get("question", "").strip()
    if not question:
        return jsonify({"answer": "Please provide a valid question."})

    chat = session.get("chat", [])
    chat.append({"role": "user", "text": question})

    try:
        logger.info("🔁 Creating thread and running agent...")

        # 1️⃣ Create thread and run
        run = project_client.agents.create_thread_and_run(
            agent_id=AGENT_ID,
            thread={"messages": [{"role": "user", "content": question}]}
        )

        thread_id = run.thread_id
        run_id = run.id
        session["thread_id"] = thread_id

        logger.info(f"📘 Thread created: {thread_id}")
        logger.info(f"🏃 Run started: {run_id} (status={run.status})")

        # 2️⃣ Wait until run completes
        while True:
            run_status = project_client.threads.runs.get(thread_id=thread_id, run_id=run_id)
            if run_status.status in ["completed", "failed", "cancelled"]:
                break
            logger.info(f"⏳ Run status: {run_status.status}... waiting...")
            time.sleep(1)

        logger.info(f"✅ Run completed with status: {run_status.status}")
        logger.info("🧾 Full run object:")
        logger.info(run_status)

        # 3️⃣ Fetch all messages in thread
        messages = project_client.threads.messages.list(thread_id=thread_id)

        logger.info("💬 Full message history:")
        for msg in messages:
            for content in msg.content:
                if content.type == "text":
                    logger.info(f"[{msg.role}] {content.text}")

        # 4️⃣ Extract the latest assistant reply
        agent_reply = next(
            (
                content.text
                for msg in reversed(messages)
                if msg.role.lower() == "assistant"
                for content in msg.content
                if content.type == "text"
            ),
            "No response from agent."
        )

        logger.info(f"🤖 Final Agent Reply: {agent_reply}")

        # 5️⃣ Add to session chat
        chat.append({"role": "agent", "text": agent_reply})
        session["chat"] = chat

        return jsonify({"answer": agent_reply})

    except Exception as e:
        logger.error(f"❌ Error communicating with Azure AI Agent: {e}")
        error_msg = f"Error: {e}"
        chat.append({"role": "agent", "text": error_msg})
        session["chat"] = chat
        return jsonify({"answer": error_msg}), 500


@app.route("/clear", methods=["POST"])
def clear_chat():
    session.pop("chat", None)
    session.pop("thread_id", None)
    return redirect(url_for("index"))
