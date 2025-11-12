// WebSocket connection
const socket = io();

// Global state
let selectedRoutes = {};
let selectedDates = {};
let isRunning = false;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    loadRoutes();
    loadDataFiles();
    updateStatus();
    
    // Set up periodic status updates
    setInterval(updateStatus, 2000);
    setInterval(loadDataFiles, 5000);
});

// Socket.IO event handlers
socket.on('connect', function() {
    console.log('Connected to server');
    addLogEntry('Connected to server', 'info');
});

socket.on('log_update', function(log) {
    addLogEntry(log.message, log.level, log.timestamp);
});

socket.on('progress_update', function(data) {
    updateProgressBar(data.progress, data.current, data.total, data.task);
});

// Load available routes and dates
async function loadRoutes() {
    try {
        const response = await fetch('/api/routes');
        const routeData = await response.json();
        
        const container = document.getElementById('route-selector');
        container.innerHTML = '';
        
        routeData.forEach((routeSet, index) => {
            const groupDiv = document.createElement('div');
            groupDiv.className = 'route-group';
            
            // Group title
            const title = document.createElement('div');
            title.className = 'route-group-title';
            title.innerHTML = `Route Set ${index + 1}`;
            groupDiv.appendChild(title);
            
            // Routes section
            const routesDiv = document.createElement('div');
            routesDiv.className = 'mb-3';
            routesDiv.innerHTML = '<strong>Routes:</strong>';
            
            selectedRoutes[index] = [];
            routeSet.routes.forEach(route => {
                const checkboxDiv = document.createElement('div');
                checkboxDiv.className = 'form-check route-checkbox';
                checkboxDiv.innerHTML = `
                    <input class="form-check-input" type="checkbox" 
                           id="route-${index}-${route}" 
                           value="${route}"
                           onchange="toggleRoute(${index}, '${route}')">
                    <label class="form-check-label checkbox-label" for="route-${index}-${route}">
                        ${route}
                    </label>
                `;
                routesDiv.appendChild(checkboxDiv);
            });
            groupDiv.appendChild(routesDiv);
            
            // Dates section
            const datesDiv = document.createElement('div');
            datesDiv.className = 'mb-2';
            datesDiv.innerHTML = `
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <strong>Dates:</strong>
                    <button class="btn btn-sm btn-outline-primary select-all-btn" 
                            onclick="selectAllDates(${index}, true)">Select All</button>
                </div>
            `;
            
            const datesContainer = document.createElement('div');
            datesContainer.className = 'row';
            
            selectedDates[index] = [];
            routeSet.dates.forEach(date => {
                const colDiv = document.createElement('div');
                colDiv.className = 'col-4 col-md-3';
                colDiv.innerHTML = `
                    <div class="form-check date-checkbox">
                        <input class="form-check-input" type="checkbox" 
                               id="date-${index}-${date}" 
                               value="${date}"
                               onchange="toggleDate(${index}, '${date}')">
                        <label class="form-check-label checkbox-label" for="date-${index}-${date}">
                            ${date}
                        </label>
                    </div>
                `;
                datesContainer.appendChild(colDiv);
            });
            
            datesDiv.appendChild(datesContainer);
            groupDiv.appendChild(datesDiv);
            
            container.appendChild(groupDiv);
        });
    } catch (error) {
        console.error('Error loading routes:', error);
        addLogEntry('Failed to load routes', 'error');
    }
}

// Toggle route selection
function toggleRoute(index, route) {
    const checkbox = document.getElementById(`route-${index}-${route}`);
    if (checkbox.checked) {
        if (!selectedRoutes[index].includes(route)) {
            selectedRoutes[index].push(route);
        }
    } else {
        selectedRoutes[index] = selectedRoutes[index].filter(r => r !== route);
    }
    console.log('Selected routes:', selectedRoutes);
}

// Toggle date selection
function toggleDate(index, date) {
    const checkbox = document.getElementById(`date-${index}-${date}`);
    if (checkbox.checked) {
        if (!selectedDates[index].includes(date)) {
            selectedDates[index].push(date);
        }
    } else {
        selectedDates[index] = selectedDates[index].filter(d => d !== date);
    }
    console.log('Selected dates:', selectedDates);
}

// Select all dates for a route set
function selectAllDates(index, select) {
    const checkboxes = document.querySelectorAll(`input[id^="date-${index}-"]`);
    checkboxes.forEach(checkbox => {
        checkbox.checked = select;
        const date = checkbox.value;
        if (select && !selectedDates[index].includes(date)) {
            selectedDates[index].push(date);
        }
    });
    if (!select) {
        selectedDates[index] = [];
    }
}

// Start crawling
async function startCrawling() {
    // Validate selection
    let hasSelection = false;
    for (let index in selectedRoutes) {
        if (selectedRoutes[index].length > 0 && selectedDates[index].length > 0) {
            hasSelection = true;
            break;
        }
    }
    
    if (!hasSelection) {
        alert('Please select at least one route and one date!');
        return;
    }
    
    try {
        const response = await fetch('/api/start', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                routes: selectedRoutes,
                dates: selectedDates
            })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            addLogEntry('Crawling started successfully', 'success');
            document.getElementById('start-btn').disabled = true;
            document.getElementById('stop-btn').disabled = false;
            isRunning = true;
            updateStatusBadge('running');
        } else {
            addLogEntry(data.error || 'Failed to start crawling', 'error');
        }
    } catch (error) {
        console.error('Error starting crawling:', error);
        addLogEntry('Failed to start crawling', 'error');
    }
}

// Stop crawling
async function stopCrawling() {
    try {
        const response = await fetch('/api/stop', {
            method: 'POST'
        });
        
        const data = await response.json();
        
        if (response.ok) {
            addLogEntry('Stop request sent', 'warning');
        } else {
            addLogEntry(data.error || 'Failed to stop crawling', 'error');
        }
    } catch (error) {
        console.error('Error stopping crawling:', error);
        addLogEntry('Failed to stop crawling', 'error');
    }
}

// Update status from server
async function updateStatus() {
    try {
        const response = await fetch('/api/status');
        const status = await response.json();
        
        // Update stats
        document.getElementById('stat-successful').textContent = status.stats.successful;
        document.getElementById('stat-failed').textContent = status.stats.failed;
        document.getElementById('stat-total').textContent = status.stats.total_scraped;
        
        // Update duration
        if (status.stats.start_time) {
            const start = new Date(status.stats.start_time);
            const end = status.stats.end_time ? new Date(status.stats.end_time) : new Date();
            const duration = Math.floor((end - start) / 1000);
            const minutes = Math.floor(duration / 60);
            const seconds = duration % 60;
            document.getElementById('stat-duration').textContent = `${minutes}m ${seconds}s`;
        }
        
        // Update UI state
        if (status.is_running !== isRunning) {
            isRunning = status.is_running;
            document.getElementById('start-btn').disabled = isRunning;
            document.getElementById('stop-btn').disabled = !isRunning;
            updateStatusBadge(isRunning ? 'running' : 'ready');
        }
        
    } catch (error) {
        console.error('Error updating status:', error);
    }
}

// Update progress bar
function updateProgressBar(percentage, current, total, task) {
    const progressBar = document.getElementById('progress-bar');
    progressBar.style.width = percentage + '%';
    progressBar.textContent = percentage + '%';
    
    document.getElementById('current-task').textContent = task || '-';
    document.getElementById('progress-text').textContent = 
        `${current} of ${total} tasks completed`;
}

// Add log entry to display
function addLogEntry(message, level = 'info', timestamp = null) {
    const logsContainer = document.getElementById('logs-container');
    
    const time = timestamp ? timestamp.split(' ')[1] : new Date().toLocaleTimeString();
    
    const logEntry = document.createElement('div');
    logEntry.className = 'log-entry';
    logEntry.innerHTML = `
        <span class="log-time">[${time}]</span>
        <span class="log-level log-${level}">[${level.toUpperCase()}]</span>
        <span class="log-message">${message}</span>
    `;
    
    logsContainer.appendChild(logEntry);
    logsContainer.scrollTop = logsContainer.scrollHeight;
}

// Clear logs
function clearLogs() {
    const logsContainer = document.getElementById('logs-container');
    logsContainer.innerHTML = '';
    addLogEntry('Logs cleared', 'info');
}

// Update status badge
function updateStatusBadge(status) {
    const badge = document.getElementById('status-badge');
    badge.className = 'badge bg-light text-dark me-2';
    
    if (status === 'running') {
        badge.classList.add('status-running');
        badge.textContent = 'Running';
        badge.classList.remove('bg-light');
        badge.classList.add('bg-success', 'text-white');
    } else if (status === 'stopped') {
        badge.classList.add('status-stopped');
        badge.textContent = 'Stopped';
        badge.classList.remove('bg-light');
        badge.classList.add('bg-danger', 'text-white');
    } else {
        badge.classList.add('status-ready');
        badge.textContent = 'Ready';
    }
}

// Load data files list
async function loadDataFiles() {
    try {
        const response = await fetch('/api/data');
        const files = await response.json();
        
        const container = document.getElementById('data-files-list');
        
        if (files.length === 0) {
            container.innerHTML = `
                <div class="list-group-item text-center text-muted">
                    <i class="bi bi-inbox"></i> No files yet
                </div>
            `;
            return;
        }
        
        container.innerHTML = '';
        
        files.forEach(file => {
            const item = document.createElement('a');
            item.href = '#';
            item.className = 'list-group-item list-group-item-action data-file-item';
            item.onclick = (e) => {
                e.preventDefault();
                viewFileData(file.filename);
            };
            
            const sizeKB = (file.size / 1024).toFixed(2);
            
            item.innerHTML = `
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <div class="file-name">${file.filename}</div>
                        <div class="file-meta">
                            <i class="bi bi-table"></i> ${file.rows} rows | 
                            <i class="bi bi-hdd"></i> ${sizeKB} KB
                        </div>
                    </div>
                    <button class="btn btn-sm btn-outline-primary" 
                            onclick="downloadFile('${file.filename}'); event.stopPropagation();">
                        <i class="bi bi-download"></i>
                    </button>
                </div>
            `;
            
            container.appendChild(item);
        });
    } catch (error) {
        console.error('Error loading data files:', error);
    }
}

// View file data
async function viewFileData(filename) {
    try {
        const response = await fetch(`/api/data/${filename}`);
        const data = await response.json();
        
        // Highlight selected file
        document.querySelectorAll('.data-file-item').forEach(item => {
            item.classList.remove('active');
        });
        event.target.closest('.data-file-item').classList.add('active');
        
        const previewDiv = document.getElementById('data-preview');
        
        let html = `
            <h6 class="mb-3">${filename}</h6>
            <div class="row mb-3">
                <div class="col-md-3">
                    <div class="stat-box">
                        <div class="stat-value text-primary">${data.rows}</div>
                        <div class="stat-label">Total Rows</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <div class="stat-value text-success">${data.stats.unique_buses}</div>
                        <div class="stat-label">Unique Buses</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <div class="stat-value text-info">Rp ${Math.round(data.stats.avg_price).toLocaleString()}</div>
                        <div class="stat-label">Avg Price</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <div class="stat-value text-warning">${data.stats.min_price.toLocaleString()} - ${data.stats.max_price.toLocaleString()}</div>
                        <div class="stat-label">Price Range</div>
                    </div>
                </div>
            </div>
            <div class="table-responsive">
                <table class="table table-striped table-hover preview-table">
                    <thead>
                        <tr>
        `;
        
        data.columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        
        html += `
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        data.preview.forEach(row => {
            html += '<tr>';
            data.columns.forEach(col => {
                html += `<td>${row[col] || '-'}</td>`;
            });
            html += '</tr>';
        });
        
        html += `
                    </tbody>
                </table>
            </div>
            <p class="text-muted text-center mt-2">
                <small>Showing first 10 rows of ${data.rows}</small>
            </p>
        `;
        
        previewDiv.innerHTML = html;
        
    } catch (error) {
        console.error('Error viewing file data:', error);
        addLogEntry(`Failed to load preview for ${filename}`, 'error');
    }
}

// Download file
function downloadFile(filename) {
    window.location.href = `/api/download/${filename}`;
    addLogEntry(`Downloading ${filename}`, 'info');
}
