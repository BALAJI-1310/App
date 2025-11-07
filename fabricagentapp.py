from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from fabric_data_agent_client import FabricDataAgentClient
import os

app = Flask(__name__)
app.secret_key = "supersecretkey"

# Initialize Fabric Data Agent Client
TENANT_ID = os.getenv("TENANT_ID", "")
DATA_AGENT_URL = os.getenv("DATA_AGENT_URL", "")

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
        # Get run details from Fabric
        run_details = client.get_run_details(question)

        sql_queries = run_details.get("sql_queries", [])
        if sql_queries:
            sql_query = sql_queries[0]
            # Clean up formatting (remove markdown code fences)
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
            answer_text = sql_query
        elif "data_retrieval_query" in run_details and run_details["data_retrieval_query"]:
            sql_query = run_details["data_retrieval_query"].strip()
            sql_query = sql_query.replace("```sql", "").replace("```", "")
            answer_text = sql_query
        else:
            answer_text = "⚠️ No SQL query found for this question."

        # Save chat history
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

