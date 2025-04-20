// MCP Monitor main JavaScript
document.addEventListener('DOMContentLoaded', function() {
    // Connect to WebSocket server
    const socket = io();
    let currentLogFilter = 'all';
    
    // DOM elements
    const serverStatus = document.getElementById('server-status');
    const clientStatus = document.getElementById('client-status');
    const startServerBtn = document.getElementById('start-server');
    const stopServerBtn = document.getElementById('stop-server');
    const startClientBtn = document.getElementById('start-client');
    const stopClientBtn = document.getElementById('stop-client');
    const messageInput = document.getElementById('message-input');
    const sendMessageBtn = document.getElementById('send-message');
    const logEntries = document.getElementById('log-entries');
    const logsContainer = document.getElementById('logs-container');
    
    // Log filter buttons
    const showAllLogsBtn = document.getElementById('show-all-logs');
    const showServerLogsBtn = document.getElementById('show-server-logs');
    const showClientLogsBtn = document.getElementById('show-client-logs');
    
    // Update status periodically
    function updateStatus() {
        fetch('/api/status')
            .then(response => response.json())
            .then(data => {
                serverStatus.textContent = data.server;
                serverStatus.className = data.server === 'running' ? 'badge status-running' : 'badge status-stopped';
                
                clientStatus.textContent = data.client;
                clientStatus.className = data.client === 'running' ? 'badge status-running' : 'badge status-stopped';
            })
            .catch(error => console.error('Error fetching status:', error));
    }
    
    // Initial status update and set interval
    updateStatus();
    setInterval(updateStatus, 5000);
    
    // Fetch initial logs
    function fetchLogs() {
        fetch('/api/logs?type=' + (currentLogFilter === 'all' ? '' : currentLogFilter) + '&count=100')
            .then(response => response.json())
            .then(data => {
                logEntries.innerHTML = '';
                data.logs.forEach(log => {
                    addLogEntry(log);
                });
                // Scroll to bottom
                logsContainer.scrollTop = logsContainer.scrollHeight;
            })
            .catch(error => console.error('Error fetching logs:', error));
    }
    
    fetchLogs();
    
    // Add a log entry to the UI
    function addLogEntry(log) {
        const logEntry = document.createElement('div');
        logEntry.className = 'log-entry log-' + log.type;
        
        const timestamp = document.createElement('span');
        timestamp.className = 'log-timestamp';
        timestamp.textContent = log.timestamp;
        
        const message = document.createElement('span');
        message.textContent = ` [${log.type}] ${log.message}`;
        
        logEntry.appendChild(timestamp);
        logEntry.appendChild(message);
        logEntries.appendChild(logEntry);
        
        // Auto-scroll to bottom
        logsContainer.scrollTop = logsContainer.scrollHeight;
    }
    
    // WebSocket event handlers
    socket.on('connect', () => {
        console.log('Connected to WebSocket server');
    });
    
    socket.on('log_update', (log) => {
        if (currentLogFilter === 'all' || currentLogFilter === log.type) {
            addLogEntry(log);
        }
    });
    
    // Button event handlers
    startServerBtn.addEventListener('click', () => {
        fetch('/api/start_server', { 
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(data => {
            updateStatus();
            console.log(data.message);
        })
        .catch(error => console.error('Error starting server:', error));
    });
    
    stopServerBtn.addEventListener('click', () => {
        fetch('/api/stop_server', { 
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(data => {
            updateStatus();
            console.log(data.message);
        })
        .catch(error => console.error('Error stopping server:', error));
    });
    
    startClientBtn.addEventListener('click', () => {
        fetch('/api/start_client', { 
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(data => {
            updateStatus();
            console.log(data.message);
        })
        .catch(error => console.error('Error starting client:', error));
    });
    
    stopClientBtn.addEventListener('click', () => {
        fetch('/api/stop_client', { 
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(data => {
            updateStatus();
            console.log(data.message);
        })
        .catch(error => console.error('Error stopping client:', error));
    });
    
    sendMessageBtn.addEventListener('click', () => {
        const message = messageInput.value.trim();
        if (message) {
            fetch('/api/send_message', { 
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ message: message })
            })
            .then(response => response.json())
            .then(data => {
                console.log(data.message);
                messageInput.value = '';
            })
            .catch(error => console.error('Error sending message:', error));
        }
    });
    
    // Log filter event handlers
    showAllLogsBtn.addEventListener('click', () => {
        currentLogFilter = 'all';
        updateActiveFilterButton(showAllLogsBtn);
        fetchLogs();
    });
    
    showServerLogsBtn.addEventListener('click', () => {
        currentLogFilter = 'server';
        updateActiveFilterButton(showServerLogsBtn);
        fetchLogs();
    });
    
    showClientLogsBtn.addEventListener('click', () => {
        currentLogFilter = 'client';
        updateActiveFilterButton(showClientLogsBtn);
        fetchLogs();
    });
    
    function updateActiveFilterButton(activeButton) {
        [showAllLogsBtn, showServerLogsBtn, showClientLogsBtn].forEach(btn => {
            btn.classList.remove('active');
        });
        activeButton.classList.add('active');
    }
});
