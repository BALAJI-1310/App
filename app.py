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
from azure.core.credentials import AzureKeyCredential

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Flask Setup ---
app = Flask(__name__)
app.secret_key = os.urandom(24)

# --- Configuration ---
INFERENCE_ENDPOINT = "https://<your-resource>.services.ai.azure.com/models"
MODEL_DEPLOYMENT_NAME = "<your-model-deployment-name>"

if not INFERENCE_ENDPOINT or not MODEL_DEPLOYMENT_NAME:
    logger.error("INFERENCE_ENDPOINT or MODEL_DEPLOYMENT_NAME is not set. Please update the values in the script.")
else:
    logger.info(f"Using INFERENCE_ENDPOINT: {INFERENCE_ENDPOINT}")
    logger.info(f"Using MODEL_DEPLOYMENT_NAME: {MODEL_DEPLOYMENT_NAME}")

# --- Global Azure AI Client ---
client = None

def init_ai_client():
    """Initialize the Azure AI Foundry client with Managed Identity and explicit token"""
    global client
    try:
        logger.info("Initializing Azure AI Foundry ChatCompletionsClient with explicit token...")

        credential = DefaultAzureCredential()
        token = credential.get_token("https://cognitiveservices.azure.com/.default")

        client = ChatCompletionsClient(
            endpoint=INFERENCE_ENDPOINT,
            credential=AzureKeyCredential(token.token)
        )
        logger.info("Client initialized successfully using Managed Identity with explicit token.")
    except Exception as e:
        logger.error(f"Failed to initialize Azure AI Client: {e}")
        client = None

# Initialize client
init_ai_client()

# --- Full HTML Template ---
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Azure AI Foundry Chat</title>
<style>
body {
    font-family: Arial, sans-serif;
    background: #f3f3f3;
    margin: 0;
    height: 100vh;
    display: flex;
    flex-direction: column;
}
header {
    display: flex;
    align-items: center;
    padding: 8px 16px;
    background: #222;
    color: white;
    height: 50px;
}
header h1 {
    font-size: 20px;
    margin: 0;
}
.chat-container {
    max-width: 1100px;
    margin: 20px auto;
    background: white;
    border-radius: 5px;
    padding: 20px;
    flex: 1;
    display: flex;
    flex-direction: column;
    width: 100%;
}
.chat-box {
    max-height: 600px;
    overflow-y: auto;
    flex: 1;
    padding-bottom: 80px;
}
.message {
    margin-bottom: 10px;
    display: flex;
}
.message.user {
    justify-content: flex-end;
}
.message.agent {
    justify-content: flex-start;
}
.bubble {
    padding: 12px 18px;
    border-radius: 15px;
    display: inline-block;
    max-width: 80%;
    word-wrap: break-word;
}
.message.user .bubble {
    background: #007bff;
    color: white;
    border-bottom-right-radius: 0;
}
.message.agent .bubble {
    background: #e5e5ea;
    color: black;
    border-bottom-left-radius: 0;
}
.input-bar {
    position: fixed;
    bottom: 30px;
    left: 50%;
    transform: translateX(-50%);
    display: flex;
    justify-content: center;
    align-items: center;
    width: 100%;
    max-width: 1200px;
    padding: 0 20px;
}
textarea {
    flex: 1;
    max-width: 1000px;
    min-height: 38px;
    max-height: 50px;
    resize: none;
    padding: 10px 14px;
    border-radius: 25px;
    border: 1px solid #ccc;
    font-size: 16px;
    overflow-y: auto;
    line-height: 1.4;
    box-sizing: border-box;
    transition: all 0.2s ease;
}
.send-btn {
    background: #007bff;
    border: none;
    color: white;
    padding: 10px 20px;
    border-radius: 25px;
    cursor: pointer;
    margin-left: 10px;
    font-size: 16px;
}
.send-btn:disabled {
    background: #5a9bf9;
    cursor: not-allowed;
}
.typing .bubble {
    font-style: italic;
    color: gray;
    background: #f0f0f0;
}
.clear-btn {
    background-color: transparent;
    border: 1px solid #ccc;
    color: #fff;
    padding: 6px 12px;
    border-radius: 20px;
    font-size: 14px;
    cursor: pointer;
    transition: all 0.3s ease;
}
.clear-btn:hover {
    background-color: #555;
    border-color: #888;
}
</style>
</head>
<body>
<header>
<h1>Azure AI Foundry Chat</h1>
<div style="margin-left:auto;">
<form action="{{ url_for('clear_chat') }}" method="post">
<button type="submit" class="clear-btn">Clear Chat</button>
</form>
</div>
</header>
<div class="chat-container">
<div class="chat-box" id="chatBox">
{% if chat %}
    {% for msg in chat %}
        <div class="message {{ msg.role }}">
            <div class="bubble">{{ msg.text|safe }}</div>
        </div>
    {% endfor %}
{% else %}
    <div class="message agent">
        <div class="bubble">Hello! I’m your Azure AI Foundry assistant. Ask me something to begin.</div>
    </div>
{% endif %}
</div>
</div>
<form id="chatForm" class="input-bar">
<textarea name="question" placeholder="Type your question here..." required></textarea>
<button type="submit" class="send-btn" id="sendBtn">Send</button>
</form>

<script>
const chatBox = document.getElementById('chatBox');
const chatForm = document.getElementById('chatForm');
const sendBtn = document.getElementById('sendBtn');
const textarea = chatForm.querySelector('textarea');

function scrollToBottom() { chatBox.scrollTop = chatBox.scrollHeight; }
scrollToBottom();

chatForm.addEventListener('submit', function(event) {
    event.preventDefault();
    const question = textarea.value.trim();
    if (!question) return;
    sendBtn.disabled = true;

    const userMsgDiv = document.createElement('div');
    userMsgDiv.className = 'message user';
    userMsgDiv.innerHTML = `<div class="bubble">${escapeHtml(question)}</div>`;
    chatBox.appendChild(userMsgDiv);
    scrollToBottom();

    const typingDiv = document.createElement('div');
    typingDiv.className = 'message agent typing-message';
    typingDiv.innerHTML = '<div class="bubble typing" id="typingBubble">.</div>';
    chatBox.appendChild(typingDiv);
    scrollToBottom();

    let dotCount = 1;
    const typingInterval = setInterval(() => {
        dotCount = (dotCount % 3) + 1;
        document.getElementById('typingBubble').textContent = '.'.repeat(dotCount);
    }, 500);

    fetch('{{ url_for("ask") }}', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: question })
    })
    .then(response => response.json())
    .then(data => {
        clearInterval(typingInterval);
        chatBox.removeChild(typingDiv);

        const agentMsgDiv = document.createElement('div');
        agentMsgDiv.className = 'message agent';
        agentMsgDiv.innerHTML = `<div class="bubble">${escapeHtml(data.answer)}</div>`;
        chatBox.appendChild(agentMsgDiv);
        scrollToBottom();

        sendBtn.disabled = false;
        textarea.value = '';
        textarea.style.height = 'auto';
        textarea.focus();
    })
    .catch(error => {
        clearInterval(typingInterval);
        chatBox.removeChild(typingDiv);
        const errorDiv = document.createElement('div');
        errorDiv.className = 'message agent';
        errorDiv.innerHTML = `<div class="bubble">Error: Unable to get response from the server.</div>`;
        chatBox.appendChild(errorDiv);
        scrollToBottom();
        sendBtn.disabled = false;
    });
});

textarea.addEventListener('input', function () {
    this.style.height = 'auto';
    this.style.height = (this.scrollHeight) + 'px';
});

function escapeHtml(text) {
    const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
    return text.replace(/[&<>"']/g, function(m) { return map[m]; });
}
</script>
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
