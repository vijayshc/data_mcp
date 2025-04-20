#!/usr/bin/env python
"""
Flask application for monitoring MCP client, server, and logs.
"""
import os
import time
import json
import subprocess
import threading
import psutil
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='mcp_monitor.log'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mcp-monitor-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variables
mcp_server_process = None
mcp_client_process = None
logs_buffer = []
MAX_LOGS = 1000

def stream_process_output(process, process_type):
    """Stream output from a process to the logs buffer and through websockets."""
    while process and process.poll() is None:
        line = process.stdout.readline()
        if line:
            line = line.decode('utf-8').strip()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_entry = {
                'timestamp': timestamp,
                'type': process_type,
                'message': line
            }
            logs_buffer.append(log_entry)
            if len(logs_buffer) > MAX_LOGS:
                logs_buffer.pop(0)
            socketio.emit('log_update', log_entry)
            logger.info(f"{process_type}: {line}")
        else:
            time.sleep(0.1)

@app.route('/')
def index():
    """Render the main monitoring page."""
    return render_template('index.html')

@app.route('/api/status')
def status():
    """Get the status of MCP server and client."""
    global mcp_server_process, mcp_client_process
    
    server_status = "stopped"
    if mcp_server_process and mcp_server_process.poll() is None:
        server_status = "running"
    
    client_status = "stopped"
    if mcp_client_process and mcp_client_process.poll() is None:
        client_status = "running"
    
    return jsonify({
        "server": server_status,
        "client": client_status,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

@app.route('/api/logs')
def get_logs():
    """Get the current logs."""
    log_type = request.args.get('type', None)
    count = int(request.args.get('count', 100))
    
    filtered_logs = logs_buffer
    if log_type:
        filtered_logs = [log for log in logs_buffer if log['type'] == log_type]
    
    return jsonify({
        "logs": filtered_logs[-count:],
        "total": len(filtered_logs)
    })

@app.route('/api/start_server', methods=['POST'])
def start_server():
    """Start the MCP server."""
    global mcp_server_process
    
    if mcp_server_process and mcp_server_process.poll() is None:
        return jsonify({"status": "already_running", "message": "MCP server is already running"})
    
    try:
        # Start the MCP server process
        mcp_server_process = subprocess.Popen(
            ["python", "-m", "mcp.server"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            bufsize=1
        )
        
        # Start a thread to stream the output
        threading.Thread(
            target=stream_process_output,
            args=(mcp_server_process, "server"),
            daemon=True
        ).start()
        
        logger.info("MCP server started")
        return jsonify({"status": "started", "message": "MCP server started successfully"})
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        return jsonify({"status": "error", "message": f"Failed to start MCP server: {e}"})

@app.route('/api/stop_server', methods=['POST'])
def stop_server():
    """Stop the MCP server."""
    global mcp_server_process
    
    if not mcp_server_process or mcp_server_process.poll() is not None:
        return jsonify({"status": "not_running", "message": "MCP server is not running"})
    
    try:
        # Kill the process and its children
        parent = psutil.Process(mcp_server_process.pid)
        for child in parent.children(recursive=True):
            child.terminate()
        parent.terminate()
        
        logger.info("MCP server stopped")
        return jsonify({"status": "stopped", "message": "MCP server stopped successfully"})
    except Exception as e:
        logger.error(f"Failed to stop MCP server: {e}")
        return jsonify({"status": "error", "message": f"Failed to stop MCP server: {e}"})

@app.route('/api/start_client', methods=['POST'])
def start_client():
    """Start the MCP client."""
    global mcp_client_process
    
    if mcp_client_process and mcp_client_process.poll() is None:
        return jsonify({"status": "already_running", "message": "MCP client is already running"})
    
    try:
        # Start the MCP client process
        mcp_client_process = subprocess.Popen(
            ["python", "mcp_client.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            bufsize=1
        )
        
        # Start a thread to stream the output
        threading.Thread(
            target=stream_process_output,
            args=(mcp_client_process, "client"),
            daemon=True
        ).start()
        
        logger.info("MCP client started")
        return jsonify({"status": "started", "message": "MCP client started successfully"})
    except Exception as e:
        logger.error(f"Failed to start MCP client: {e}")
        return jsonify({"status": "error", "message": f"Failed to start MCP client: {e}"})

@app.route('/api/stop_client', methods=['POST'])
def stop_client():
    """Stop the MCP client."""
    global mcp_client_process
    
    if not mcp_client_process or mcp_client_process.poll() is not None:
        return jsonify({"status": "not_running", "message": "MCP client is not running"})
    
    try:
        # Kill the process and its children
        parent = psutil.Process(mcp_client_process.pid)
        for child in parent.children(recursive=True):
            child.terminate()
        parent.terminate()
        
        logger.info("MCP client stopped")
        return jsonify({"status": "stopped", "message": "MCP client stopped successfully"})
    except Exception as e:
        logger.error(f"Failed to stop MCP client: {e}")
        return jsonify({"status": "error", "message": f"Failed to stop MCP client: {e}"})

@app.route('/api/send_message', methods=['POST'])
def send_message():
    """Send a message to the MCP client."""
    data = request.json
    message = data.get('message', '')
    
    # In a real implementation, you would send the message to the MCP client
    # This is a placeholder that logs the message
    log_entry = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'type': 'user_message',
        'message': message
    }
    logs_buffer.append(log_entry)
    socketio.emit('log_update', log_entry)
    
    return jsonify({"status": "sent", "message": "Message sent to MCP client"})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
