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

-=----------------------------------------------------------------
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from fabric_data_agent_client import FabricDataAgentClient
import os
import webbrowser
from threading import Timer

app = Flask(__name__)
app.secret_key = "supersecretkey"

# ===== Configuration =====
TENANT_ID = os.getenv("TENANT_ID", "")
DATA_AGENT_URL = os.getenv("DATA_AGENT_URL", "")

# ===== Initialize Fabric Data Agent Client =====
client = FabricDataAgentClient(
    tenant_id=TENANT_ID,
    data_agent_url=DATA_AGENT_URL
)


@app.route("/")
def index():
    chat = session.get("chat", [])
    return render_template("index.html", chat=chat)


@app.route("/ask", methods=["POST"])
def ask():
    data = request.get_json()
    question = data.get("question", "").strip()

    if not question:
        return jsonify({"answer": "⚠️ Please enter a question."})

    try:
        # Call the Fabric Data Agent
        run_details = client.get_run_details(question)

        # Extract the assistant's latest natural language response
        messages = run_details.get("messages", {}).get("data", [])
        assistant_messages = [msg for msg in messages if msg.get("role") == "assistant"]

        answer_text = "⚠️ No response from the agent."

        if assistant_messages:
            latest_message = assistant_messages[-1]
            content = latest_message.get("content", [])
            if content:
                first_item = content[0]
                if hasattr(first_item, "text"):  # object with .text.value
                    answer_text = first_item.text.value
                elif isinstance(first_item, dict) and "text" in first_item:
                    text_obj = first_item["text"]
                    if isinstance(text_obj, dict) and "value" in text_obj:
                        answer_text = text_obj["value"]
                    else:
                        answer_text = text_obj
                else:
                    answer_text = str(first_item)

        # Save chat in session
        chat = session.get("chat", [])
        chat.append({"role": "user", "text": question})
        chat.append({"role": "agent", "text": answer_text})
        session["chat"] = chat

        return jsonify({"answer": answer_text})

    except Exception as e:
        print(f"❌ Error in ask(): {e}")
        return jsonify({"answer": f"❌ Error: {str(e)}"})


@app.route("/clear_chat", methods=["POST"])
def clear_chat():
    session.pop("chat", None)
    return redirect(url_for("index"))


# ===== Automatically open the browser =====
def open_browser():
    webbrowser.open_new("http://127.0.0.1:5000")


if __name__ == "__main__":
    Timer(1, open_browser).start()
    app.run(host="0.0.0.0", port=5000, debug=True)
