import os
import logging
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from controller.services import fetch_tcg_ids, publish_batch

load_dotenv()
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

@app.route("/start", methods=["POST"])
def start_workflow():
    try:
        data = request.json
        provided_api_key = data.get('key')
        if provided_api_key != os.getenv("API_KEY"):
            return jsonify({"error": "Unauthorized"}), 403
        
        tcg_ids = data.get('tcg_ids', fetch_tcg_ids())
        if not isinstance(tcg_ids, list):
            tcg_ids = tcg_ids.split(',')
        
        publish_batch(tcg_ids, data.get('url'))
        return jsonify({"status": "Workflow initiated", "tcg_ids_count": len(tcg_ids)})
    except Exception as e:
        logging.error(f"Error in start_workflow: {e}")
        return jsonify({"error": "Internal Server Error"}), 500


@app.route("/resend", methods=["POST"])
def retry_workflow():
    try:
        data = request.json
        failed_ids = data.get('failed_ids')
    
        if not failed_ids:
            return jsonify({"error": "No failed IDs provided"}), 400
        
        tcg_ids = failed_ids
    publish_batch(failed_ids, data.get('url'))
    
    return jsonify({"status": "Retry initiated for failed IDs", "count": len(failed_ids)})
if __name__ == "__main__":
    app.run(debug=True)
