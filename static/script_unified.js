// Socket.IO connection
const socket = io();

// State
let currentDataFilter = 'all';
let currentLogFilter = 'all';
let travelokaRouteInfo = [];
let redbusRouteInfo = null;
let unifiedRoutes = ['Jakarta-Semarang', 'Jakarta-Surabaya', 'Jakarta-Malang', 'Jakarta-Lampung'];

// Time tracking
let timeTracking = {
    traveloka: {
        startTime: null,
        lastUpdate: null,
        elapsedSeconds: 0,
        intervalId: null
    },
    redbus: {
        startTime: null,
        lastUpdate: null,
        elapsedSeconds: 0,
        intervalId: null
    }
};

// Worker colors for Redbus
const workerColors = ['#0d6efd', '#198754', '#ffc107', '#dc3545', '#6f42c1'];

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    loadUnifiedRoutes();
    refreshData();
    loadAvailableRoutes();  // Load routes for prediction
    initDataViewControls();
    
    // Auto-refresh status every 2 seconds
    setInterval(updateStatus, 2000);
});

// Socket.IO events
socket.on('connected', function(data) {
    console.log('Connected:', data.message);
});

socket.on('log_update', function(log) {
    addLogEntry(log);
});

socket.on('progress_update', function(data) {
    updateProgress(data);
});

socket.on('training_progress', function(data) {
    const progressBar = document.getElementById('training-progress-bar');
    const stepText = document.getElementById('training-step');
    
    if (progressBar) {
        progressBar.style.width = data.progress + '%';
    }
    if (stepText) {
        stepText.textContent = data.step;
    }
});

socket.on('task_start', function(data) {
    if (data.platform === 'redbus') {
        updateRedbusWorkers();
    }
});

// Load Traveloka routes
async function loadTravelokaRoutes() {
    try {
        const response = await fetch('/api/routes/traveloka');
        travelokaRouteInfo = await response.json();
        
        const container = document.getElementById('traveloka-routes');
        container.innerHTML = '';
        
        travelokaRouteInfo.forEach((routeSet, idx) => {
            const card = document.createElement('div');
            card.className = 'card mb-2';
            card.innerHTML = `
                <div class="card-body">
                    <h6 class="card-title">Route Set ${idx + 1}</h6>
                    <div class="mb-2">
                        <strong>Routes:</strong>
                        <div class="d-flex gap-2 mb-1">
                            <button class="btn btn-sm btn-outline-secondary" onclick="selectAllRoutes(${idx}, true)">All</button>
                            <button class="btn btn-sm btn-outline-secondary" onclick="selectAllRoutes(${idx}, false)">None</button>
                        </div>
                        ${routeSet.routes.map(route => `
                            <div class="form-check route-checkbox">
                                <input class="form-check-input traveloka-route" type="checkbox" 
                                       id="tr-${idx}-${route}" data-set="${idx}" value="${route}">
                                <label class="form-check-label" for="tr-${idx}-${route}">${route}</label>
                            </div>
                        `).join('')}
                    </div>
                    <div>
                        <strong>Date Range:</strong>
                        <div class="row g-2 mt-1">
                            <div class="col-md-6">
                                <label class="form-label small">Start Date:</label>
                                <input type="date" class="form-control form-control-sm traveloka-date-start" 
                                       data-set="${idx}" id="tds-${idx}">
                            </div>
                            <div class="col-md-6">
                                <label class="form-label small">End Date:</label>
                                <input type="date" class="form-control form-control-sm traveloka-date-end" 
                                       data-set="${idx}" id="tde-${idx}">
                            </div>
                        </div>
                        <div class="mt-2">
                            <small class="text-muted">
                                <i class="bi bi-info-circle"></i> Leave empty to use all available dates
                            </small>
                        </div>
                    </div>
                </div>
            `;
            container.appendChild(card);
        });
    } catch (error) {
        console.error('Error loading Traveloka routes:', error);
    }
}

// Load Redbus routes
async function loadRedbusRoutes() {
    try {
        const response = await fetch('/api/routes/redbus');
        redbusRouteInfo = await response.json();
        
        const routesContainer = document.getElementById('redbus-routes');
        routesContainer.innerHTML = redbusRouteInfo.routes.map(route => `
            <div class="form-check route-checkbox">
                <input class="form-check-input redbus-route" type="checkbox" 
                       id="rr-${route}" value="${route}">
                <label class="form-check-label" for="rr-${route}">${route}</label>
            </div>
        `).join('');
        
        const datesContainer = document.getElementById('redbus-dates');
        datesContainer.innerHTML = `
            <div class="row g-2">
                <div class="col-md-6">
                    <label class="form-label small">Start Date:</label>
                    <input type="date" class="form-control form-control-sm" id="redbus-date-start">
                </div>
                <div class="col-md-6">
                    <label class="form-label small">End Date:</label>
                    <input type="date" class="form-control form-control-sm" id="redbus-date-end">
                </div>
            </div>
            <div class="mt-2">
                <small class="text-muted">
                    <i class="bi bi-info-circle"></i> Leave empty to use all available dates
                </small>
            </div>
        `;
    } catch (error) {
        console.error('Error loading Redbus routes:', error);
    }
}

// Load Unified Routes
function loadUnifiedRoutes() {
    const container = document.getElementById('unified-routes');
    container.innerHTML = unifiedRoutes.map(route => `
        <div class="form-check route-checkbox">
            <input class="form-check-input unified-route" type="checkbox" 
                   id="ur-${route}" value="${route}" checked>
            <label class="form-check-label" for="ur-${route}">
                <i class="bi bi-geo-alt-fill"></i> ${route}
            </label>
        </div>
    `).join('');
}

// Select all unified routes
function selectAllUnifiedRoutes(select) {
    document.querySelectorAll('.unified-route').forEach(cb => {
        cb.checked = select;
    });
}

// Start Unified Crawling
async function startUnifiedCrawling() {
    // Get selected platforms
    const travelokaChecked = document.getElementById('platform-traveloka').checked;
    const redbusChecked = document.getElementById('platform-redbus').checked;
    
    if (!travelokaChecked && !redbusChecked) {
        alert('Please select at least one platform!');
        return;
    }
    
    // Get selected routes
    const selectedRoutes = Array.from(document.querySelectorAll('.unified-route:checked'))
        .map(cb => cb.value);
    
    if (selectedRoutes.length === 0) {
        alert('Please select at least one route!');
        return;
    }
    
    // Get date range
    const startDate = document.getElementById('unified-date-start').value;
    const endDate = document.getElementById('unified-date-end').value;
    
    let selectedDates = [];
    if (startDate && endDate) {
        selectedDates = generateDateRange(startDate, endDate);
    } else {
        // Use default dates (December 15-31, 2025)
        const defaultStart = '2025-12-15';
        const defaultEnd = '2025-12-31';
        selectedDates = generateDateRange(defaultStart, defaultEnd);
    } 
    
    // Show progress section
    document.getElementById('unified-progress-section').style.display = 'block';
    document.getElementById('unified-start-btn').style.display = 'none';
    document.getElementById('unified-stop-btn').style.display = 'block';
    
    // Reset time tracking for selected platforms
    if (travelokaChecked) {
        resetTimeTracking('traveloka');
    }
    if (redbusChecked) {
        resetTimeTracking('redbus');
    }
    
    // Start crawling for selected platforms
    const promises = [];
    
    if (travelokaChecked) {
        document.getElementById('traveloka-progress-section').style.display = 'block';
        promises.push(startTravelokaCrawling(selectedRoutes, selectedDates));
    }
    
    if (redbusChecked) {
        document.getElementById('redbus-progress-section').style.display = 'block';
        promises.push(startRedbusCrawling(selectedRoutes, selectedDates));
    }
    
    // Wait for all to start
    try {
        await Promise.all(promises);
        console.log('All selected crawlers started successfully');
    } catch (error) {
        console.error('Error starting crawlers:', error);
        alert('Error starting crawlers: ' + error.message);
    }
}

// Start Traveloka Crawling
async function startTravelokaCrawling(routes, dates) {
    // For Traveloka, we need to use route set 0 (December 2025)
    const routesData = { '0': routes };
    const datesData = { '0': dates };
    
    const data = {
        routes: routesData,
        dates: datesData
    };
    
    try {
        const response = await fetch('/api/start/traveloka', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Failed to start Traveloka crawler');
        }
        
        const result = await response.json();
        console.log('Traveloka started:', result);
    } catch (error) {
        console.error('Traveloka error:', error);
        throw error;
    }
}

// Start Redbus Crawling
async function startRedbusCrawling(routes, dates) {
    const data = {
        routes: routes,
        dates: dates
    };
    
    try {
        const response = await fetch('/api/start/redbus', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Failed to start Redbus crawler');
        }
        
        const result = await response.json();
        console.log('Redbus started:', result);
    } catch (error) {
        console.error('Redbus error:', error);
        throw error;
    }
}

// Stop Unified Crawling
async function stopUnifiedCrawling() {
    const promises = [];
    
    // Stop both platforms
    if (document.getElementById('traveloka-progress-section').style.display !== 'none') {
        promises.push(fetch('/api/stop/traveloka', { method: 'POST' }));
        // Stop timer
        if (timeTracking.traveloka.intervalId) {
            clearInterval(timeTracking.traveloka.intervalId);
            timeTracking.traveloka.intervalId = null;
        }
    }
    
    if (document.getElementById('redbus-progress-section').style.display !== 'none') {
        promises.push(fetch('/api/stop/redbus', { method: 'POST' }));
        // Stop timer
        if (timeTracking.redbus.intervalId) {
            clearInterval(timeTracking.redbus.intervalId);
            timeTracking.redbus.intervalId = null;
        }
    }
    
    try {
        await Promise.all(promises);
        console.log('All crawlers stopped');
    } catch (error) {
        console.error('Error stopping crawlers:', error);
    }
    
    // Hide buttons
    document.getElementById('unified-start-btn').style.display = 'block';
    document.getElementById('unified-stop-btn').style.display = 'none';
}

// Select all helpers
function selectAllRoutes(setIdx, select) {
    document.querySelectorAll(`.traveloka-route[data-set="${setIdx}"]`).forEach(cb => {
        cb.checked = select;
    });
}

function selectAll(platform, type, select) {
    const className = `${platform}-${type}`;
    document.querySelectorAll(`.${className}`).forEach(cb => {
        cb.checked = select;
    });
}

// Helper to generate date range
function generateDateRange(startDate, endDate) {
    if (!startDate || !endDate) return [];
    
    const dates = [];
    const current = new Date(startDate);
    const end = new Date(endDate);
    
    while (current <= end) {
        // Format as YYYY-MM-DD (full date)
        const year = current.getFullYear();
        const month = (current.getMonth() + 1).toString().padStart(2, '0');
        const day = current.getDate().toString().padStart(2, '0');
        const fullDate = `${year}-${month}-${day}`;
        dates.push(fullDate);
        current.setDate(current.getDate() + 1);
    }
    
    return dates;
}

// Start crawling
async function startCrawling(platform) {
    let data = {};
    
    if (platform === 'traveloka') {
        // Collect selected routes and dates
        const routesData = {};
        const datesData = {};
        
        travelokaRouteInfo.forEach((routeSet, idx) => {
            const selectedRoutes = Array.from(document.querySelectorAll(`.traveloka-route[data-set="${idx}"]:checked`))
                .map(cb => cb.value);
            
            // Get date range
            const startDate = document.querySelector(`.traveloka-date-start[data-set="${idx}"]`).value;
            const endDate = document.querySelector(`.traveloka-date-end[data-set="${idx}"]`).value;
            
            let selectedDates = [];
            if (startDate && endDate) {
                selectedDates = generateDateRange(startDate, endDate);
            } else {
                // Use all available dates from routeSet if no date range specified
                selectedDates = routeSet.dates;
            }
            
            if (selectedRoutes.length > 0 && selectedDates.length > 0) {
                routesData[idx] = selectedRoutes;
                datesData[idx] = selectedDates;
            }
        });
        
        if (Object.keys(routesData).length === 0) {
            alert('Please select at least one route and specify dates!');
            return;
        }
        
        data = { routes: routesData, dates: datesData };
    } else if (platform === 'redbus') {
        const selectedRoutes = Array.from(document.querySelectorAll('.redbus-route:checked'))
            .map(cb => cb.value);
        
        // Get date range
        const startDate = document.getElementById('redbus-date-start').value;
        const endDate = document.getElementById('redbus-date-end').value;
        
        let selectedDates = [];
        if (startDate && endDate) {
            selectedDates = generateDateRange(startDate, endDate);
        } else {
            // Use all available dates if no date range specified
            selectedDates = redbusRouteInfo.dates;
        }
        
        if (selectedRoutes.length === 0 || selectedDates.length === 0) {
            alert('Please select at least one route and specify dates!');
            return;
        }
        
        data = {
            routes: selectedRoutes,
            dates: selectedDates
        };
    }
    
    try {
        const response = await fetch(`/api/start/${platform}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        
        const result = await response.json();
        
        if (response.ok) {
            document.getElementById(`${platform}-start-btn`).style.display = 'none';
            document.getElementById(`${platform}-stop-btn`).style.display = 'block';
            document.getElementById(`${platform}-progress-container`).style.display = 'block';
            
            addLogEntry({
                platform: platform,
                level: 'info',
                message: `Started with ${result.total_tasks} tasks`,
                timestamp: new Date().toISOString()
            });
        } else {
            alert(result.error || 'Failed to start crawler');
        }
    } catch (error) {
        console.error('Error starting crawler:', error);
        alert('Error starting crawler');
    }
}

// Stop crawling
async function stopCrawling(platform) {
    try {
        const response = await fetch(`/api/stop/${platform}`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            addLogEntry({
                platform: platform,
                level: 'warning',
                message: 'Stop requested',
                timestamp: new Date().toISOString()
            });
        } else {
            alert(result.error || 'Failed to stop crawler');
        }
    } catch (error) {
        console.error('Error stopping crawler:', error);
    }
}

// Update status
async function updateStatus() {
    try {
        const response = await fetch('/api/status');
        const status = await response.json();
        
        // Update Traveloka
        updatePlatformStatus('traveloka', status.traveloka);
        
        // Update Redbus
        updatePlatformStatus('redbus', status.redbus);
        
    } catch (error) {
        console.error('Error updating status:', error);
    }
}

function updatePlatformStatus(platform, status) {
    // Update stats
    document.getElementById(`${platform}-total`).textContent = status.stats.total_scraped;
    document.getElementById(`${platform}-success`).textContent = status.stats.successful;
    document.getElementById(`${platform}-failed`).textContent = status.stats.failed;
    
    // Check if both platforms are stopped
    if (!status.is_running) {
        // Check if unified UI is visible
        const progressSection = document.getElementById('unified-progress-section');
        if (progressSection && progressSection.style.display !== 'none') {
            // Check if other platform is also stopped
            fetch('/api/status')
                .then(r => r.json())
                .then(allStatus => {
                    const travelokaStopped = !allStatus.traveloka.is_running;
                    const redbusStopped = !allStatus.redbus.is_running;
                    
                    if (travelokaStopped && redbusStopped) {
                        // Both stopped, show start button
                        document.getElementById('unified-start-btn').style.display = 'block';
                        document.getElementById('unified-stop-btn').style.display = 'none';
                    }
                });
        }
    }
}

function updateProgress(data) {
    const platform = data.platform;
    const progressBar = document.getElementById(`${platform}-progress-bar`);
    const progressText = document.getElementById(`${platform}-progress-text`);
    
    progressBar.style.width = `${data.progress}%`;
    progressBar.textContent = `${data.progress}%`;
    progressText.textContent = `${data.completed}/${data.total} (${data.progress}%)`;
    
    // Initialize time tracking when first task starts
    if (data.completed === 1 && !timeTracking[platform].startTime) {
        timeTracking[platform].startTime = new Date();
        timeTracking[platform].lastUpdate = new Date();
        timeTracking[platform].elapsedSeconds = 0;
        
        // Start elapsed time counter
        if (timeTracking[platform].intervalId) {
            clearInterval(timeTracking[platform].intervalId);
        }
        timeTracking[platform].intervalId = setInterval(() => {
            updateElapsedTime(platform);
        }, 1000);
    }
    
    // Calculate and display time estimates
    if (timeTracking[platform].startTime && data.completed > 0) {
        const now = new Date();
        const elapsedMs = now - timeTracking[platform].startTime;
        const elapsedMinutes = elapsedMs / 1000 / 60;
        const tasksRemaining = data.total - data.completed;
        
        // Calculate speed (tasks per minute)
        const speed = data.completed / elapsedMinutes;
        const speedElement = document.getElementById(`${platform}-speed`);
        if (speedElement) {
            speedElement.textContent = speed.toFixed(2);
        }
        
        // Calculate ETA
        if (tasksRemaining > 0 && speed > 0) {
            const etaMinutes = tasksRemaining / speed;
            const etaElement = document.getElementById(`${platform}-eta`);
            if (etaElement) {
                etaElement.textContent = `ETA: ${formatTime(etaMinutes * 60)}`;
                etaElement.className = 'fw-bold text-info';
            }
        } else if (data.progress === 100) {
            const etaElement = document.getElementById(`${platform}-eta`);
            if (etaElement) {
                etaElement.textContent = 'Completed!';
                etaElement.className = 'fw-bold text-success';
            }
            // Stop the timer
            if (timeTracking[platform].intervalId) {
                clearInterval(timeTracking[platform].intervalId);
                timeTracking[platform].intervalId = null;
            }
        }
    }
    
    // Update current task display
    const taskContainer = document.getElementById(`${platform}-current-task`);
    if (taskContainer && data.current_task) {
        taskContainer.textContent = `Current: ${data.current_task}`;
    }
}

// Helper function to update elapsed time display
function updateElapsedTime(platform) {
    if (!timeTracking[platform].startTime) return;
    
    const now = new Date();
    const elapsedMs = now - timeTracking[platform].startTime;
    const elapsedSeconds = Math.floor(elapsedMs / 1000);
    
    const elapsedElement = document.getElementById(`${platform}-elapsed`);
    if (elapsedElement) {
        elapsedElement.textContent = formatTime(elapsedSeconds);
    }
}

// Helper function to format seconds to HH:MM:SS
function formatTime(seconds) {
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    return `${hrs.toString().padStart(2, '0')}:${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
}

// Helper function to reset time tracking
function resetTimeTracking(platform) {
    // Clear existing interval if any
    if (timeTracking[platform].intervalId) {
        clearInterval(timeTracking[platform].intervalId);
        timeTracking[platform].intervalId = null;
    }
    
    // Reset values
    timeTracking[platform].startTime = null;
    timeTracking[platform].lastUpdate = null;
    timeTracking[platform].elapsedSeconds = 0;
    
    // Reset UI elements
    const elapsedElement = document.getElementById(`${platform}-elapsed`);
    if (elapsedElement) {
        elapsedElement.textContent = '00:00:00';
    }
    
    const speedElement = document.getElementById(`${platform}-speed`);
    if (speedElement) {
        speedElement.textContent = '0';
    }
    
    const etaElement = document.getElementById(`${platform}-eta`);
    if (etaElement) {
        etaElement.textContent = '';
    }
}

// Logs
function addLogEntry(log) {
    if (currentLogFilter !== 'all' && log.platform !== currentLogFilter) {
        return;
    }
    
    const container = document.getElementById('log-container');
    
    // Clear placeholder
    if (container.querySelector('.text-muted.text-center')) {
        container.innerHTML = '';
    }
    
    const logEntry = document.createElement('div');
    logEntry.className = `log-entry log-${log.level}`;
    logEntry.innerHTML = `
        <span class="log-timestamp">[${log.timestamp}]</span>
        <span class="log-platform ${log.platform}">${log.platform.toUpperCase()}</span>
        ${log.message}
    `;
    
    container.appendChild(logEntry);
    container.scrollTop = container.scrollHeight;
    
    // Also handle training logs if applicable
    if (log.platform === 'training') {
        const trainingLogsDiv = document.getElementById('training-logs');
        if (trainingLogsDiv) {
            const entry = document.createElement('div');
            entry.className = 'log-entry';
            entry.innerHTML = `
                <span class="log-timestamp">[${log.timestamp}]</span>
                <span class="log-platform training">TRAINING</span>
                <span class="log-${log.level}">${log.message}</span>
            `;
            trainingLogsDiv.appendChild(entry);
            trainingLogsDiv.scrollTop = trainingLogsDiv.scrollHeight;
            
            // Keep only last 50 entries
            while (trainingLogsDiv.children.length > 50) {
                trainingLogsDiv.removeChild(trainingLogsDiv.firstChild);
            }
        }
    }
}

function filterLogs(filter) {
    currentLogFilter = filter;
    const container = document.getElementById('log-container');
    const entries = container.querySelectorAll('.log-entry');
    
    entries.forEach(entry => {
        if (filter === 'all') {
            entry.style.display = '';
        } else {
            const platform = entry.querySelector('.log-platform').textContent.toLowerCase();
            entry.style.display = platform === filter ? '' : 'none';
        }
    });
}

function clearLogs() {
    const container = document.getElementById('log-container');
    container.innerHTML = '<div class="text-muted text-center">Logs cleared</div>';
}

// Data files
async function refreshData() {
    await loadDataFiles(currentDataFilter);
}

async function loadDataFiles(filter = 'all') {
    currentDataFilter = filter;
    
    try {
        const response = await fetch(`/api/data/${filter}`);
        const files = await response.json();
        
        const container = document.getElementById('data-files-list');
        
        if (files.length === 0) {
            container.innerHTML = '<div class="alert alert-info">No data files found</div>';
            return;
        }
        
        container.innerHTML = files.map(file => `
            <div class="file-item">
                <div class="row align-items-center">
                    <div class="col-md-6">
                        <h6 class="mb-1">
                            <span class="platform-badge badge-${file.platform}">${file.platform.toUpperCase()}</span>
                            ${file.filename}
                        </h6>
                        <small class="text-muted">
                            <i class="bi bi-calendar"></i> ${new Date(file.modified).toLocaleString()} | 
                            <i class="bi bi-file-earmark"></i> ${(file.size / 1024).toFixed(2)} KB | 
                            <i class="bi bi-list-ol"></i> ${file.rows} rows
                        </small>
                    </div>
                    <div class="col-md-6 text-end">
                        <button class="btn btn-sm btn-primary" onclick="previewFile('${file.filename}')">
                            <i class="bi bi-eye"></i> Preview
                        </button>
                        <button class="btn btn-sm btn-success" onclick="downloadFile('${file.filename}')">
                            <i class="bi bi-download"></i> Download
                        </button>
                    </div>
                </div>
            </div>
        `).join('');
    } catch (error) {
        console.error('Error loading data files:', error);
    }
}

function filterData(platform) {
    loadDataFiles(platform);
}

async function previewFile(filename) {
    try {
        const response = await fetch(`/api/data/preview/${filename}`);
        const data = await response.json();
        
        if (!response.ok) {
            alert(data.error || 'Error loading preview');
            return;
        }
        
        // Update modal title
        document.getElementById('previewModalLabel').textContent = `Preview: ${filename}`;
        
        // Update stats
        const statsHtml = `
            <div class="row">
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stats-number">${data.rows}</div>
                        <div class="text-muted small">Total Rows</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stats-number">${data.stats.unique_buses || 0}</div>
                        <div class="text-muted small">Unique Buses</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stats-number">${data.stats.unique_types || 0}</div>
                        <div class="text-muted small">Bus Types</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stats-number">
                            ${data.stats.avg_price ? 'Rp ' + data.stats.avg_price.toFixed(0) : 'N/A'}
                        </div>
                        <div class="text-muted small">Avg Price</div>
                    </div>
                </div>
            </div>
        `;
        document.getElementById('preview-stats').innerHTML = statsHtml;
        
        // Update table
        const table = document.getElementById('preview-table');
        const thead = table.querySelector('thead');
        const tbody = table.querySelector('tbody');
        
        // Headers
        thead.innerHTML = `<tr>${data.columns.map(col => `<th>${col}</th>`).join('')}</tr>`;
        
        // Rows (first 20)
        tbody.innerHTML = data.preview.map(row => 
            `<tr>${data.columns.map(col => `<td>${row[col] || ''}</td>`).join('')}</tr>`
        ).join('');
        
        // Show modal
        const modal = new bootstrap.Modal(document.getElementById('previewModal'));
        modal.show();
        
    } catch (error) {
        console.error('Error previewing file:', error);
        alert('Error loading preview');
    }
}

function downloadFile(filename) {
    window.location.href = `/api/download/${filename}`;
}

// --- Database view controls & helpers (Files <-> Database toggle) ---
function initDataViewControls() {
    try {
        const viewFilesBtn = document.getElementById('view-files-btn');
        const viewDbBtn = document.getElementById('view-db-btn');
        const dataFilesControls = document.getElementById('data-files-controls');
        const dataDbControls = document.getElementById('data-db-controls');
        const dataDbSearch = document.getElementById('data-db-search');

        if (viewFilesBtn) {
            viewFilesBtn.addEventListener('click', function () {
                dataFilesControls.style.display = 'block';
                dataDbControls.style.display = 'none';
                // Load file listing
                loadDataFiles(currentDataFilter);
            });
        }

        if (viewDbBtn) {
            viewDbBtn.addEventListener('click', function () {
                dataFilesControls.style.display = 'none';
                dataDbControls.style.display = 'block';
                // Populate route select if not yet populated
                populateDataRouteSelect();
            });
        }

        if (dataDbSearch) {
            dataDbSearch.addEventListener('click', function () {
                loadDatabaseResults();
            });
        }
    } catch (err) {
        console.warn('initDataViewControls failed', err);
    }
}

async function populateDataRouteSelect() {
    try {
        const select = document.getElementById('data-route-select');
        if (!select) return;
        // If already populated with options > 1, skip
        if (select.options && select.options.length > 1) return;

        const res = await fetch('/api/routes/available');
        if (!res.ok) return;
        const routes = await res.json();
        routes.forEach(r => {
            const opt = document.createElement('option');
            opt.value = r;
            opt.textContent = r;
            select.appendChild(opt);
        });
    } catch (err) {
        console.warn('populateDataRouteSelect failed', err);
    }
}

// Load DB results and render into the existing data-files-list container
async function loadDatabaseResults() {
    try {
        const platform = document.getElementById('data-platform-select')?.value || '';
        const route = document.getElementById('data-route-select')?.value || '';
        const date = document.getElementById('data-filter-date')?.value || '';
        const bus = document.getElementById('data-filter-bus')?.value || '';

        // Limit to avoid huge responses; change as needed
        const limit = 1000;

        const params = new URLSearchParams();
        if (platform) params.append('platform', platform);
        if (route) params.append('route_name', route);
        if (date) params.append('date', date);
        if (bus) params.append('bus_name', bus);
        params.append('limit', String(limit));

        const url = '/api/data/db?' + params.toString();
        const res = await fetch(url);
        const data = await res.json();

        const container = document.getElementById('data-files-list');
        if (!container) return;

        // Keep last DB results for preview
        window.__lastDbResults = Array.isArray(data) ? data : [];

        if (!Array.isArray(data) || data.length === 0) {
            container.innerHTML = '<div class="alert alert-info">No database rows found</div>';
            return;
        }

        // Render a simple table with preview buttons
        const rowsHtml = data.map((row, idx) => {
            const dateStr = row.route_date || row.crawl_timestamp || row.created_at || '';
            const routeName = row.route_name || row.route || '';
            const company = row.bus_name || row.company || '';
            const platformBadge = row.platform ? `<span class="badge bg-${row.platform === 'traveloka' ? 'primary' : 'danger'}">${row.platform}</span>` : '';
            return `
                <div class="file-item d-flex align-items-center justify-content-between">
                    <div>
                        <h6 class="mb-1">${platformBadge} ${routeName}</h6>
                        <small class="text-muted">${dateStr} &nbsp; ${company}</small>
                    </div>
                    <div>
                        <button class="btn btn-sm btn-outline-primary me-2" onclick="previewDbRow(${idx})">
                            <i class="bi bi-eye"></i> Preview
                        </button>
                    </div>
                </div>
            `;
        }).join('');

        container.innerHTML = rowsHtml;
    } catch (err) {
        console.error('loadDatabaseResults failed', err);
        const container = document.getElementById('data-files-list');
        if (container) container.innerHTML = `<div class="alert alert-danger">Error loading DB results: ${err.message}</div>`;
    }
}

// Preview a single DB row using the existing preview modal
function previewDbRow(idx) {
    try {
        const results = window.__lastDbResults || [];
        const row = results[idx];
        if (!row) return alert('Row not found');

        document.getElementById('previewModalLabel').textContent = `DB Row Preview`;

        // Stats: show single row count
        const statsHtml = `
            <div class="row">
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stats-number">1</div>
                        <div class="text-muted small">Row</div>
                    </div>
                </div>
                <div class="col-md-9">
                    <div class="text-muted small">Previewing row from database</div>
                </div>
            </div>
        `;
        document.getElementById('preview-stats').innerHTML = statsHtml;

        // Build key/value table
        const table = document.getElementById('preview-table');
        const thead = table.querySelector('thead');
        const tbody = table.querySelector('tbody');

        thead.innerHTML = `<tr><th>Field</th><th>Value</th></tr>`;
        const keys = Object.keys(row);
        tbody.innerHTML = keys.map(k => `
            <tr>
                <td style="min-width: 250px"><strong>${k}</strong></td>
                <td>${row[k] === null || row[k] === undefined ? '' : String(row[k])}</td>
            </tr>
        `).join('');

        const modal = new bootstrap.Modal(document.getElementById('previewModal'));
        modal.show();
    } catch (err) {
        console.error('previewDbRow failed', err);
        alert('Failed to preview row');
    }
}

// Comparison
async function loadComparison() {
    try {
        const response = await fetch('/api/compare');
        const comparison = await response.json();
        
        if (!response.ok) {
            alert(comparison.error || 'Error generating comparison');
            return;
        }
        
        const container = document.getElementById('comparison-content');
        
        let html = `
            <div class="row mb-4">
                <div class="col-md-6">
                    <div class="comparison-card">
                        <h4><span class="platform-badge badge-traveloka">TRAVELOKA</span></h4>
                        <div class="row mt-3">
                            <div class="col-6">
                                <div class="stats-card traveloka">
                                    <div class="stats-number">${comparison.traveloka.total_files}</div>
                                    <div class="text-muted small">Total Files</div>
                                </div>
                            </div>
                            <div class="col-6">
                                <div class="stats-card traveloka">
                                    <div class="stats-number">${comparison.traveloka.total_records}</div>
                                    <div class="text-muted small">Total Records</div>
                                </div>
                            </div>
                            <div class="col-12 mt-2">
                                <div class="stats-card traveloka">
                                    <div class="stats-number">Rp ${comparison.traveloka.avg_price.toFixed(0)}</div>
                                    <div class="text-muted small">Average Price</div>
                                </div>
                            </div>
                        </div>
                        <div class="mt-3">
                            <strong>Date Coverage:</strong>
                            <div class="mt-2">
                                ${comparison.traveloka.date_coverage.map(d => 
                                    `<span class="badge bg-secondary m-1">${d}</span>`
                                ).join('')}
                            </div>
                        </div>
                    </div>
                </div>
                
                <div class="col-md-6">
                    <div class="comparison-card">
                        <h4><span class="platform-badge badge-redbus">REDBUS</span></h4>
                        <div class="row mt-3">
                            <div class="col-6">
                                <div class="stats-card redbus">
                                    <div class="stats-number">${comparison.redbus.total_files}</div>
                                    <div class="text-muted small">Total Files</div>
                                </div>
                            </div>
                            <div class="col-6">
                                <div class="stats-card redbus">
                                    <div class="stats-number">${comparison.redbus.total_records}</div>
                                    <div class="text-muted small">Total Records</div>
                                </div>
                            </div>
                            <div class="col-12 mt-2">
                                <div class="stats-card redbus">
                                    <div class="stats-number">Rp ${comparison.redbus.avg_price.toFixed(0)}</div>
                                    <div class="text-muted small">Average Price</div>
                                </div>
                            </div>
                        </div>
                        <div class="mt-3">
                            <strong>Date Coverage:</strong>
                            <div class="mt-2">
                                ${comparison.redbus.date_coverage.map(d => 
                                    `<span class="badge bg-secondary m-1">${d}</span>`
                                ).join('')}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="comparison-card">
                <h4>Route-by-Route Comparison</h4>
                <div class="table-responsive">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Route</th>
                                <th class="text-center">Traveloka Records</th>
                                <th class="text-center">Redbus Records</th>
                                <th class="text-center">Difference</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${comparison.comparison.map(item => {
                                const diff = item.traveloka_records - item.redbus_records;
                                const diffClass = diff > 0 ? 'text-primary' : diff < 0 ? 'text-danger' : 'text-muted';
                                return `
                                    <tr>
                                        <td>${item.route}</td>
                                        <td class="text-center">${item.traveloka_records}</td>
                                        <td class="text-center">${item.redbus_records}</td>
                                        <td class="text-center ${diffClass}">${diff > 0 ? '+' : ''}${diff}</td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        `;
        
        container.innerHTML = html;
        
    } catch (error) {
        console.error('Error loading comparison:', error);
        alert('Error generating comparison');
    }
}

// Analytics
async function generateAnalytics() {
    const platform = document.getElementById('analytics-platform').value;
    const route = document.getElementById('analytics-route').value;
    const dateInput = document.getElementById('analytics-date').value;
    
    if (!dateInput) {
        alert('Please select a date');
        return;
    }
    
    // Use full date (YYYY-MM-DD format)
    const dateStr = dateInput;
    
    try {
        const response = await fetch(`/api/analytics?platform=${platform}&route=${route}&date=${dateStr}`);
        
        if (!response.ok) {
            const error = await response.json();
            alert(error.error || 'No data found');
            return;
        }
        
        const analytics = await response.json();
        displayAnalytics(analytics);
        
    } catch (error) {
        console.error('Error generating analytics:', error);
        alert('Error generating analytics report');
    }
}

function displayAnalytics(analytics) {
    const container = document.getElementById('analytics-results');
    
    // Get all companies and bus types
    const companies = Object.keys(analytics.summary.bus_companies).sort();
    const allBusTypes = new Set();
    
    Object.values(analytics.summary.bus_types_by_company).forEach(types => {
        Object.keys(types).forEach(type => allBusTypes.add(type));
    });
    
    const busTypes = Array.from(allBusTypes).sort();
    
    // Group similar bus types
    const typeGroups = {
        'VIP': busTypes.filter(t => t.toLowerCase().includes('vip')),
        'Executive': busTypes.filter(t => t.toLowerCase().includes('executive') || t.toLowerCase().includes('eks')),
        'Economy': busTypes.filter(t => t.toLowerCase().includes('economy') || t.toLowerCase().includes('eco')),
        'Other': busTypes.filter(t => 
            !t.toLowerCase().includes('vip') && 
            !t.toLowerCase().includes('executive') && 
            !t.toLowerCase().includes('eks') &&
            !t.toLowerCase().includes('economy') &&
            !t.toLowerCase().includes('eco')
        )
    };
    
    let html = `
        <div class="card mb-3">
            <div class="card-header bg-primary text-white">
                <h5 class="mb-0">
                    <i class="bi bi-info-circle"></i> Analytics Summary
                </h5>
            </div>
            <div class="card-body">
                <div class="row">
                    <div class="col-md-3">
                        <strong>Platform:</strong> ${analytics.platform.toUpperCase()}
                    </div>
                    <div class="col-md-4">
                        <strong>Route:</strong> ${analytics.route}
                    </div>
                    <div class="col-md-2">
                        <strong>Date:</strong> ${analytics.date}
                    </div>
                    <div class="col-md-3">
                        <strong>Total Crawls:</strong> ${analytics.total_crawls}x
                    </div>
                </div>
                <div class="row mt-2">
                    <div class="col-md-12">
                        <strong>Crawl Times:</strong> ${analytics.summary.crawl_times.join(', ')}
                    </div>
                </div>
            </div>
        </div>
        
        <div class="card mb-3">
            <div class="card-header bg-success text-white">
                <h5 class="mb-0">
                    <i class="bi bi-building"></i> Bus Distribution by Company
                </h5>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-bordered table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>Category</th>
                                ${companies.map(company => `<th class="text-center">${company}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            <tr class="table-primary">
                                <td><strong>BUS TOTAL</strong></td>
                                ${companies.map(company => {
                                    const count = analytics.summary.bus_companies[company] || 0;
                                    return `<td class="text-center"><strong>${count}</strong></td>`;
                                }).join('')}
                            </tr>
    `;
    
    // Add rows for each bus type group
    Object.entries(typeGroups).forEach(([groupName, types]) => {
        if (types.length > 0) {
            html += `
                <tr class="table-info">
                    <td><strong>${groupName}</strong><br><small class="text-muted">(${types.join(', ')})</small></td>
                    ${companies.map(company => {
                        let total = 0;
                        const companyTypes = analytics.summary.bus_types_by_company[company] || {};
                        types.forEach(type => {
                            total += companyTypes[type] || 0;
                        });
                        return `<td class="text-center">${total}</td>`;
                    }).join('')}
                </tr>
            `;
        }
    });
    
    html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <div class="card mb-3">
            <div class="card-header bg-info text-white">
                <h5 class="mb-0">
                    <i class="bi bi-clock-history"></i> Crawl Sessions Detail
                </h5>
            </div>
            <div class="card-body">
    `;
    
    analytics.crawl_sessions.forEach(session => {
        html += `
            <div class="card mb-2">
                <div class="card-header">
                    <strong>Crawl #${session.crawl_number}</strong> - 
                    <span class="badge bg-primary">${session.crawl_time}</span> - 
                    <small class="text-muted">${session.filename}</small> - 
                    <span class="badge bg-success">${session.total_buses} buses</span>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <h6>Companies:</h6>
                            <ul class="list-unstyled">
                                ${Object.entries(session.companies).map(([company, count]) => 
                                    `<li><span class="badge bg-secondary">${count}</span> ${company}</li>`
                                ).join('')}
                            </ul>
                        </div>
                        <div class="col-md-6">
                            <h6>Bus Types:</h6>
                            <ul class="list-unstyled">
                                ${Object.entries(session.bus_types).map(([type, count]) => 
                                    `<li><span class="badge bg-info">${count}</span> ${type}</li>`
                                ).join('')}
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });
    
    html += `
            </div>
        </div>
        
        <div class="card">
            <div class="card-header bg-warning">
                <h5 class="mb-0">
                    <i class="bi bi-bar-chart"></i> Overall Statistics
                </h5>
            </div>
            <div class="card-body">
                <div class="row">
                    <div class="col-md-4">
                        <div class="stats-card">
                            <div class="stats-number text-primary">${analytics.summary.total_unique_buses}</div>
                            <div class="text-muted small">Unique Bus Companies</div>
                        </div>
                    </div>
                    <div class="col-md-4">
                        <div class="stats-card">
                            <div class="stats-number text-success">${analytics.summary.total_unique_types}</div>
                            <div class="text-muted small">Unique Bus Types</div>
                        </div>
                    </div>
                    <div class="col-md-4">
                        <div class="stats-card">
                            <div class="stats-number text-info">${Object.values(analytics.summary.bus_companies).reduce((a, b) => a + b, 0)}</div>
                            <div class="text-muted small">Total Buses (All Crawls)</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

// ============ ML Training & Prediction Functions ============

// Start model training
async function startTraining() {
    const daysBack = parseInt(document.getElementById('training-days').value);
    const btn = document.getElementById('train-btn');
    const progressDiv = document.getElementById('training-progress');
    const logsDiv = document.getElementById('training-logs');
    
    btn.disabled = true;
    progressDiv.style.display = 'block';
    logsDiv.innerHTML = '<div class="text-muted">Starting training...</div>';
    
    try {
        const response = await fetch('/api/train/start', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({days_back: daysBack})
        });
        
        const data = await response.json();
        
        if (response.ok) {
            // Monitor training progress
            const checkInterval = setInterval(async () => {
                const statusRes = await fetch('/api/train/status');
                const status = await statusRes.json();
                
                // Update progress bar
                document.getElementById('training-progress-bar').style.width = status.progress + '%';
                document.getElementById('training-step').textContent = status.current_step;
                
                // Check if training is complete
                if (!status.is_running && status.results) {
                    clearInterval(checkInterval);
                    btn.disabled = false;
                    
                    // Display results
                    if (status.results.error) {
                        alert('Training error: ' + status.results.error);
                    } else {
                        displayTrainingResults(status.results);
                    }
                }
            }, 1000);
        } else {
            alert('Error: ' + data.error);
            btn.disabled = false;
            progressDiv.style.display = 'none';
        }
    } catch (error) {
        alert('Request failed: ' + error.message);
        btn.disabled = false;
        progressDiv.style.display = 'none';
    }
}

// Display training results
function displayTrainingResults(results) {
    const resultsDiv = document.getElementById('training-results');
    resultsDiv.style.display = 'block';
    
    document.getElementById('result-mae').textContent = results.metrics.mae.toFixed(2);
    document.getElementById('result-rmse').textContent = results.metrics.rmse.toFixed(2);
    document.getElementById('result-r2').textContent = results.metrics.r2.toFixed(4);
    document.getElementById('result-datapoints').textContent = results.data_points.toLocaleString();
    
    const timestamp = new Date(results.timestamp);
    document.getElementById('result-timestamp').textContent = 
        'Completed: ' + timestamp.toLocaleString();
}

// Generate predictions
// Toggle prediction type (days vs date range)
function togglePredictionType() {
    const predictionType = document.getElementById('prediction-type').value;
    const daysOption = document.getElementById('days-option');
    const daterangeOption = document.getElementById('daterange-option');
    
    if (predictionType === 'daterange') {
        daysOption.style.display = 'none';
        daterangeOption.style.display = 'block';
        
        // Set default dates (tomorrow to 7 days from now)
        const tomorrow = new Date();
        tomorrow.setDate(tomorrow.getDate() + 1);
        const nextWeek = new Date();
        nextWeek.setDate(nextWeek.getDate() + 7);
        
        document.getElementById('start-date').value = tomorrow.toISOString().split('T')[0];
        document.getElementById('end-date').value = nextWeek.toISOString().split('T')[0];
    } else {
        daysOption.style.display = 'block';
        daterangeOption.style.display = 'none';
    }
}

// Toggle custom days input
function toggleCustomDays() {
    const daysSelect = document.getElementById('prediction-days');
    const customInput = document.getElementById('custom-days-input');
    
    if (daysSelect.value === 'custom') {
        customInput.style.display = 'block';
    } else {
        customInput.style.display = 'none';
    }
}

// Load available routes for prediction
async function loadAvailableRoutes() {
    try {
        const response = await fetch('/api/routes/available');
        const routes = await response.json();
        
        const routeSelect = document.getElementById('prediction-route');
        routeSelect.innerHTML = '<option value="">All Routes</option>';
        
        routes.forEach(route => {
            routeSelect.innerHTML += `<option value="${route}">${route}</option>`;
        });
    } catch (error) {
        console.error('Failed to load routes:', error);
    }
}

async function generatePredictions() {
    const predictionType = document.getElementById('prediction-type').value;
    const routeSelect = document.getElementById('prediction-route');
    const btn = document.getElementById('predict-btn');
    const resultsDiv = document.getElementById('prediction-results');
    
    let requestData = {
        route: routeSelect.value || null
    };
    
    // Determine prediction parameters based on type
    if (predictionType === 'daterange') {
        // Date range prediction
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        
        if (!startDate || !endDate) {
            alert('Please select both start and end dates');
            return;
        }
        
        if (new Date(startDate) > new Date(endDate)) {
            alert('Start date must be before or equal to end date');
            return;
        }
        
        requestData.start_date = startDate;
        requestData.end_date = endDate;
    } else {
        // Days from today prediction
        const daysSelect = document.getElementById('prediction-days');
        const customDaysInput = document.getElementById('custom-days-value');
        
        let days;
        if (daysSelect.value === 'custom') {
            days = parseInt(customDaysInput.value);
            if (isNaN(days) || days < 1 || days > 365) {
                alert('Please enter a valid number of days between 1 and 365');
                return;
            }
        } else {
            days = parseInt(daysSelect.value);
        }
        
        requestData.days = days;
    }
    
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Generating...';
    
    try {
        const response = await fetch('/api/predict', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(requestData)
        });
        
        const data = await response.json();
        
        if (response.ok) {
            // Display session ID and total
            document.getElementById('session-id').textContent = data.session_id;
            document.getElementById('total-predictions').textContent = data.total_predictions;
            
            // Display predictions in table
            const tbody = document.getElementById('predictions-table');
            tbody.innerHTML = '';
            
            if (data.predictions.length === 0) {
                tbody.innerHTML = '<tr><td colspan="12" class="text-center text-muted">No predictions available</td></tr>';
            } else {
                data.predictions.forEach(pred => {
                    const row = `
                        <tr>
                            <td>${pred.date}</td>
                            <td><span class="badge bg-${pred.is_weekend === 'Weekend' ? 'warning' : 'info'}">${pred.day_name}</span></td>
                            <td>${pred.route_name}</td>
                            <td><span class="badge bg-${pred.platform === 'traveloka' ? 'primary' : 'danger'}">${pred.platform}</span></td>
                            <td>${pred.bus_name}</td>
                            <td><strong>${pred.predicted_total}</strong></td>
                            <td>${pred.predicted_vip}</td>
                            <td>${pred.predicted_executive}</td>
                            <td>${pred.predicted_other}</td>
                            <td><span class="text-primary">${pred.predicted_departing_time || 'N/A'}</span></td>
                            <td><span class="text-success">${pred.predicted_reaching_time || 'N/A'}</span></td>
                            <td><span class="text-danger">Rp ${(pred.predicted_price || 0).toLocaleString()}</span></td>
                        </tr>
                    `;
                    tbody.innerHTML += row;
                });
            }
            
            resultsDiv.style.display = 'block';
            
            // Scroll to results
            resultsDiv.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        alert('Request failed: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-lightning-fill"></i> Generate Predictions';
    }
}

// Load database statistics
async function loadDatabaseStats() {
    const statsDiv = document.getElementById('db-stats-predictions');
    
    try {
        const response = await fetch('/api/database/stats');
        const stats = await response.json();
        
        console.log('Database stats:', stats);
        let html = `
            <div class="mb-2">
                <small class="text-muted">Total Records</small>
                <h5>${stats.total_records?.toLocaleString() || 0}</h5>
            </div>
            <div class="mb-2">
                <small class="text-muted">Routes</small>
                <h5>${Object.keys(stats.top_routes || {}).length || 0}</h5>
            </div>
            <div class="mb-2">
                <small class="text-muted">Platforms</small>
                <h5>${Object.keys(stats.by_platform || {}).join(', ') || 'N/A'}</h5>
            </div>
            <div class="mb-2">
                <small class="text-muted">Latest Crawl</small>
                <p class="mb-0"><small>${stats.latest_crawl || 'N/A'}</small></p>
            </div>
        `;
        statsDiv.innerHTML = html;
    } catch (error) {
        statsDiv.innerHTML = '<p class="text-danger">Failed to load stats</p>';
    }
}

// Load prediction history
async function loadPredictionHistory() {
    const container = document.getElementById('history-content');
    container.innerHTML = '<div class="text-center"><div class="spinner-border"></div><p>Loading...</p></div>';
    
    try {
        const response = await fetch('/api/predictions/history');
        const sessions = await response.json();
        
        if (sessions.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-5">
                    <i class="bi bi-inbox" style="font-size: 3rem;"></i>
                    <p class="mt-3">No prediction history found</p>
                </div>
            `;
            return;
        }
        
        let html = '<div class="row">';
        sessions.forEach(session => {
            const date = new Date(session.created_at);
            const period = session.prediction_period || 'Unknown';
            const sessionId = session.id;
            html += `
                <div class="col-md-6 mb-3">
                    <div class="card">
                        <div class="card-body">
                            <h6 class="card-title">
                                <i class="bi bi-calendar-event"></i> ${period}
                            </h6>
                            <p class="mb-1"><small class="text-muted">Session ID: ${sessionId}</small></p>
                            <p class="mb-1"><small class="text-muted">Period: ${session.prediction_start_date || 'N/A'} to ${session.prediction_end_date || 'N/A'}</small></p>
                            <p class="mb-2"><small class="text-muted">Created: ${date.toLocaleString()}</small></p>
                            <button class="btn btn-sm btn-outline-primary" onclick="viewSession(${sessionId})">
                                <i class="bi bi-eye"></i> View Details
                            </button>
                        </div>
                    </div>
                </div>
            `;
        });
        html += '</div>';
        
        container.innerHTML = html;
    } catch (error) {
        container.innerHTML = `<div class="alert alert-danger">Error loading history: ${error.message}</div>`;
    }
}

// View specific prediction session
async function viewSession(sessionId) {
    try {
        const response = await fetch(`/api/predictions/session/${sessionId}`);
        const predictions = await response.json();
        
        if (predictions.length === 0) {
            alert('No predictions found for this session');
            return;
        }
        
        // Create modal to display predictions
        const modal = `
            <div class="modal fade" id="sessionModal" tabindex="-1">
                <div class="modal-dialog modal-xl">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">Session ${sessionId} - ${predictions.length} Predictions</h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            <div class="table-responsive" style="max-height: 500px;">
                                <table id="session-predictions-table" class="table table-sm table-hover display" style="width:100%">
                                    <thead class="table-light sticky-top">
                                        <tr>
                                            <th>Date</th>
                                            <th>Day</th>
                                            <th>Route</th>
                                            <th>Platform</th>
                                            <th>Bus Company</th>
                                            <th>Total</th>
                                            <th>VIP</th>
                                            <th>Executive</th>
                                            <th>Other</th>
                                            <th>Depart</th>
                                            <th>Arrive</th>
                                            <th>Price</th>
                                        </tr>
                                        <!-- Filter row: will be filled by DataTables or fallback JS -->
                                        <tr class="table-filter-row">
                                            <th style="min-width:160px">
                                                <input type="date" id="session-filter-date-start" class="form-control form-control-sm" title="Start date">
                                                <input type="date" id="session-filter-date-end" class="form-control form-control-sm" title="End date">
                                            </th>
                                            <th style="min-width:160px">
                                            </th>
                                            <th><input type="search" class="form-control form-control-sm column-filter" data-col="2" placeholder="Filter route"></th>
                                            <th><select class="form-select form-select-sm column-filter" data-col="3"><option value="">All platforms</option></select></th>
                                            <th><input type="search" class="form-control form-control-sm column-filter" data-col="4" placeholder="Filter company"></th>
                                            <th></th>
                                            <th></th>
                                            <th></th>
                                            <th></th>
                                            <th></th>
                                            <th></th>
                                            <th></th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${predictions.map(p => {
                                            const isWeekend = p.is_weekend === 1 || p.is_weekend === true;
                                            const dayNames = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
                                            const dayName = dayNames[p.day_of_week] || '';
                                            return `
                                            <tr>
                                                <td>${p.prediction_date}</td>
                                                <td><span class="badge bg-${isWeekend ? 'warning' : 'info'}">${dayName}</span></td>
                                                <td>${p.route_name}</td>
                                                <td><span class="badge bg-${p.platform === 'traveloka' ? 'primary' : 'danger'}">${p.platform}</span></td>
                                                <td>${p.bus_name}</td>
                                                <td><strong>${p.predicted_total}</strong></td>
                                                <td>${p.predicted_vip}</td>
                                                <td>${p.predicted_executive}</td>
                                                <td>${p.predicted_other}</td>
                                                <td><span class="text-primary"><strong>${p.predicted_departing_time || 'N/A'}</strong></span></td>
                                                <td><span class="text-success"><strong>${p.predicted_reaching_time || 'N/A'}</strong></span></td>
                                                <td><span class="text-danger"><strong>Rp ${(p.predicted_price || 0).toLocaleString()}</strong></span></td>
                                            </tr>
                                        `}).join('')}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // Remove existing modal if any
        const existingModal = document.getElementById('sessionModal');
        if (existingModal) existingModal.remove();
        
        // Add modal to page
        document.body.insertAdjacentHTML('beforeend', modal);
        
        // Show modal
        const modalInstance = new bootstrap.Modal(document.getElementById('sessionModal'));
        modalInstance.show();

        // Initialize DataTables if available, otherwise fallback to simple filtering
        (function initSessionTable() {
            const tableEl = document.getElementById('session-predictions-table');

            // Helper to populate platform select with unique platforms
            function populatePlatformSelect() {
                const select = tableEl.querySelector('select[data-col="3"]');
                const platforms = Array.from(new Set(predictions.map(p => p.platform))).sort();
                platforms.forEach(pl => {
                    const opt = document.createElement('option');
                    opt.value = pl;
                    opt.textContent = pl;
                    select.appendChild(opt);
                });
            }

            populatePlatformSelect();

            const dateStartInput = tableEl.querySelector('#session-filter-date-start');
            const dateEndInput = tableEl.querySelector('#session-filter-date-end');

            // If DataTables (jQuery plugin) is present, initialize it
            try {
                if (window.jQuery && jQuery.fn && jQuery.fn.DataTable) {
                    const $table = jQuery(tableEl);
                    const dt = $table.DataTable({
                        pageLength: 25,
                        lengthMenu: [10, 25, 50, 100],
                        order: [[0, 'asc']],
                        responsive: true,
                        dom: "<'d-flex justify-content-between mb-2'<'dt-left'f><'dt-right'l>>rtip",
                        initComplete: function () {
                            const api = this.api();

                            // Column filters for Route (2), Platform (3), Bus Company (4)
                            api.columns([2,3,4]).every(function () {
                                const col = this;
                                const colIndex = col.index();

                                if (colIndex === 3) {
                                    // platform select: already populated in DOM
                                    const sel = jQuery(tableEl).find('select[data-col="3"]');
                                    sel.on('change', function () {
                                        const val = jQuery(this).val();
                                        col.search(val ? '^' + jQuery.fn.dataTable.util.escapeRegex(val) + '$' : '', true, false).draw();
                                    });
                                } else {
                                    // text input
                                    const input = jQuery(tableEl).find('input[data-col="' + colIndex + '"]');
                                    input.on('keyup change clear', function () {
                                        if (col.search() !== this.value) {
                                            col.search(this.value).draw();
                                        }
                                    });
                                }
                            });

                            // Date range filter using DataTables custom search
                            const customDateFilter = function (settings, searchData, index, rowData, counter) {
                                // searchData[0] corresponds to Date column text
                                const rowDateStr = searchData[0] || '';
                                if (!rowDateStr) return true;

                                const startVal = dateStartInput && dateStartInput.value ? new Date(dateStartInput.value) : null;
                                const endVal = dateEndInput && dateEndInput.value ? new Date(dateEndInput.value) : null;

                                // Parse row date (expect YYYY-MM-DD or ISO)
                                const rowDate = new Date(rowDateStr);
                                if (isNaN(rowDate)) return true;

                                if (startVal && rowDate < startVal) return false;
                                if (endVal && rowDate > endVal) return false;
                                return true;
                            };

                            // Register filter
                            jQuery.fn.dataTable.ext.search.push(customDateFilter);

                            // Re-draw on date changes
                            if (dateStartInput) jQuery(dateStartInput).on('change', function () { api.draw(); });
                            if (dateEndInput) jQuery(dateEndInput).on('change', function () { api.draw(); });

                            // When modal is hidden remove the custom filter
                            document.getElementById('sessionModal').addEventListener('hidden.bs.modal', function () {
                                try {
                                    const idx = jQuery.fn.dataTable.ext.search.indexOf(customDateFilter);
                                    if (idx !== -1) jQuery.fn.dataTable.ext.search.splice(idx, 1);
                                } catch (e) { }
                            });
                        }
                    });

                    // When modal is hidden, destroy DataTable to avoid reinit issues
                    document.getElementById('sessionModal').addEventListener('hidden.bs.modal', function () {
                        try { dt.destroy(true); } catch (e) { }
                    });

                    return;
                }
            } catch (err) {
                console.warn('DataTables init failed, falling back to simple filtering', err);
            }

            // Fallback: simple client-side filtering (no jQuery/DataTables)
            try {
                const inputs = tableEl.querySelectorAll('.column-filter');
                const dateStart = tableEl.querySelector('#session-filter-date-start');
                const dateEnd = tableEl.querySelector('#session-filter-date-end');

                function parseDateString(s) {
                    if (!s) return null;
                    // Accept YYYY-MM-DD or ISO-like strings
                    const d = new Date(s);
                    return isNaN(d) ? null : d;
                }

                function applyFilters() {
                    const filters = {};
                    inputs.forEach(inp => {
                        const col = parseInt(inp.getAttribute('data-col'), 10);
                        const val = inp.value && inp.value.toString().trim().toLowerCase();
                        if (val) filters[col] = val;
                    });

                    const startVal = parseDateString(dateStart ? dateStart.value : null);
                    const endVal = parseDateString(dateEnd ? dateEnd.value : null);

                    const rows = tableEl.querySelectorAll('tbody tr');
                    rows.forEach(row => {
                        const cells = row.querySelectorAll('td');
                        let show = true;

                        // Date range check (column 0)
                        if (startVal || endVal) {
                            const rowDateStr = cells[0] ? cells[0].textContent.trim() : '';
                            const rowDate = parseDateString(rowDateStr);
                            if (!rowDate) {
                                show = false;
                            } else {
                                if (startVal && rowDate < startVal) show = false;
                                if (endVal && rowDate > endVal) show = false;
                            }
                        }

                        if (!show) {
                            row.style.display = 'none';
                            return;
                        }

                        for (const [colIdx, term] of Object.entries(filters)) {
                            const c = cells[colIdx] ? cells[colIdx].textContent.trim().toLowerCase() : '';
                            if (!c.includes(term)) { show = false; break; }
                        }
                        row.style.display = show ? '' : 'none';
                    });
                }

                inputs.forEach(inp => {
                    inp.addEventListener('input', applyFilters);
                    inp.addEventListener('change', applyFilters);
                });

                if (dateStart) dateStart.addEventListener('change', applyFilters);
                if (dateEnd) dateEnd.addEventListener('change', applyFilters);

                // For platform select, populate options already done above
            } catch (err) {
                console.warn('Fallback table filtering setup failed', err);
            }
        })();
        
    } catch (error) {
        alert('Error loading session: ' + error.message);
        console.log(error.message) 
    }
}

// Load stats when predictions tab is shown
document.addEventListener('shown.bs.tab', function(e) {
    if (e.target.id === 'predictions-tab') {
        loadDatabaseStats();
    }
});

