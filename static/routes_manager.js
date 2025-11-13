/**
 * Routes Manager JavaScript
 * Handles the web interface for route management
 */

let currentRoutes = [];
let currentRouteId = null;

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
    loadRoutes();
    setupEventListeners();
    loadRoutesForTesting();
    
    // Set default test date to tomorrow
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    document.getElementById('test-date').value = tomorrow.toISOString().split('T')[0];
});

function setupEventListeners() {
    // Search functionality
    document.getElementById('search-routes').addEventListener('input', filterRoutes);
    document.getElementById('filter-status').addEventListener('change', filterRoutes);
    document.getElementById('filter-platform').addEventListener('change', filterRoutes);
}

// ============ Routes Loading ============

async function loadRoutes() {
    try {
        showLoading('Loading routes...');
        
        const response = await fetch('/api/routes-manager/routes');
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const routes = await response.json();
        currentRoutes = routes;
        
        displayRoutes(routes);
        updateStats(routes);
        hideLoading();
        
        console.log(`Loaded ${routes.length} routes`);
    } catch (error) {
        console.error('Error loading routes:', error);
        showAlert('error', `Failed to load routes: ${error.message}`);
        hideLoading();
    }
}

function displayRoutes(routes) {
    const container = document.getElementById('routes-container');
    
    if (!routes || routes.length === 0) {
        container.innerHTML = `
            <div class="col-12">
                <div class="alert alert-info text-center">
                    <i class="bi bi-info-circle"></i> No routes found. 
                    <a href="#" onclick="showAddRouteModal()">Add your first route</a>
                </div>
            </div>
        `;
        return;
    }
    
    container.innerHTML = routes.map(route => createRouteCard(route)).join('');
}

function createRouteCard(route) {
    const statusBadge = route.active 
        ? '<span class="badge bg-success">Active</span>'
        : '<span class="badge bg-secondary">Inactive</span>';
    
    const platformBadges = [];
    if (route.platforms.redbus) {
        platformBadges.push('<span class="badge bg-info platform-badge"><i class="bi bi-bus-front"></i> Redbus</span>');
    }
    if (route.platforms.traveloka) {
        platformBadges.push('<span class="badge bg-warning platform-badge"><i class="bi bi-airplane"></i> Traveloka</span>');
    }
    
    return `
        <div class="col-md-6 col-lg-4 mb-3">
            <div class="card route-card h-100">
                <div class="card-body">
                    <div class="d-flex justify-content-between align-items-start mb-2">
                        <h6 class="card-title mb-0">${route.name}</h6>
                        ${statusBadge}
                    </div>
                    
                    <p class="card-text text-muted mb-2">
                        <i class="bi bi-geo-alt"></i> ${route.origin} → ${route.destination}
                    </p>
                    
                    <div class="mb-3">
                        ${platformBadges.join(' ')}
                        ${platformBadges.length === 0 ? '<span class="text-muted">No URLs configured</span>' : ''}
                    </div>
                    
                    <div class="d-flex gap-1">
                        <button class="btn btn-sm btn-outline-primary" onclick="editRoute('${route.id}')">
                            <i class="bi bi-pencil"></i> Edit
                        </button>
                        <button class="btn btn-sm btn-outline-info" onclick="manageUrls('${route.id}', '${route.name}')">
                            <i class="bi bi-link"></i> URLs
                        </button>
                        <button class="btn btn-sm btn-outline-secondary" onclick="duplicateRoute('${route.id}')">
                            <i class="bi bi-files"></i> Copy
                        </button>
                        <button class="btn btn-sm btn-outline-danger" onclick="deleteRoute('${route.id}', '${route.name}')">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>
                </div>
                <div class="card-footer text-muted small">
                    ID: ${route.id} | Category: ${route.category || 'intercity'}
                </div>
            </div>
        </div>
    `;
}

function updateStats(routes) {
    const totalRoutes = routes.length;
    const activeRoutes = routes.filter(r => r.active).length;
    const redbusRoutes = routes.filter(r => r.platforms.redbus).length;
    const travelokaRoutes = routes.filter(r => r.platforms.traveloka).length;
    
    document.getElementById('total-routes').textContent = totalRoutes;
    document.getElementById('active-routes').textContent = activeRoutes;
    document.getElementById('redbus-routes').textContent = redbusRoutes;
    document.getElementById('traveloka-routes').textContent = travelokaRoutes;
}

// ============ Filtering ============

function filterRoutes() {
    const searchTerm = document.getElementById('search-routes').value.toLowerCase();
    const statusFilter = document.getElementById('filter-status').value;
    const platformFilter = document.getElementById('filter-platform').value;
    
    let filteredRoutes = currentRoutes.filter(route => {
        // Text search
        const matchesSearch = !searchTerm || 
            route.name.toLowerCase().includes(searchTerm) ||
            route.origin.toLowerCase().includes(searchTerm) ||
            route.destination.toLowerCase().includes(searchTerm);
        
        // Status filter
        const matchesStatus = statusFilter === 'all' ||
            (statusFilter === 'active' && route.active) ||
            (statusFilter === 'inactive' && !route.active);
        
        // Platform filter
        let matchesPlatform = true;
        if (platformFilter === 'redbus') {
            matchesPlatform = route.platforms.redbus;
        } else if (platformFilter === 'traveloka') {
            matchesPlatform = route.platforms.traveloka;
        } else if (platformFilter === 'both') {
            matchesPlatform = route.platforms.redbus && route.platforms.traveloka;
        }
        
        return matchesSearch && matchesStatus && matchesPlatform;
    });
    
    displayRoutes(filteredRoutes);
}

// ============ Route Management ============

function showAddRouteModal() {
    // Clear form
    document.getElementById('addRouteForm').reset();
    document.getElementById('route-active').checked = true;
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('addRouteModal'));
    modal.show();
}

async function addRoute() {
    try {
        const formData = {
            name: document.getElementById('route-name').value.trim(),
            origin: document.getElementById('route-origin').value.trim(),
            destination: document.getElementById('route-destination').value.trim(),
            category: document.getElementById('route-category').value,
            active: document.getElementById('route-active').checked
        };
        
        // Validation
        if (!formData.name || !formData.origin || !formData.destination) {
            showAlert('error', 'Please fill in all required fields');
            return;
        }
        
        const response = await fetch('/api/routes-manager/routes', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showAlert('success', 'Route added successfully');
            bootstrap.Modal.getInstance(document.getElementById('addRouteModal')).hide();
            await loadRoutes();
            await loadRoutesForTesting();
        } else {
            throw new Error(result.error || 'Failed to add route');
        }
        
    } catch (error) {
        console.error('Error adding route:', error);
        showAlert('error', `Failed to add route: ${error.message}`);
    }
}

async function editRoute(routeId) {
    // For now, just show the basic info. In a full implementation,
    // you'd want an edit modal similar to the add modal
    const route = currentRoutes.find(r => r.id === routeId);
    if (!route) {
        showAlert('error', 'Route not found');
        return;
    }
    
    const newName = prompt('Enter new route name:', route.name);
    if (newName && newName !== route.name) {
        try {
            const response = await fetch(`/api/routes/master/${routeId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ name: newName })
            });
            
            const result = await response.json();
            
            if (response.ok) {
                showAlert('success', result.message);
                await loadRoutes();
                await loadRoutesForTesting();
            } else {
                throw new Error(result.error || 'Failed to update route');
            }
        } catch (error) {
            console.error('Error updating route:', error);
            showAlert('error', `Failed to update route: ${error.message}`);
        }
    }
}

async function deleteRoute(routeId, routeName) {
    if (!confirm(`Are you sure you want to delete route "${routeName}"?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/routes/master/${routeId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showAlert('success', result.message);
            await loadRoutes();
            await loadRoutesForTesting();
        } else {
            throw new Error(result.error || 'Failed to delete route');
        }
        
    } catch (error) {
        console.error('Error deleting route:', error);
        showAlert('error', `Failed to delete route: ${error.message}`);
    }
}

async function duplicateRoute(routeId) {
    const route = currentRoutes.find(r => r.id === routeId);
    if (!route) {
        showAlert('error', 'Route not found');
        return;
    }
    
    const newName = prompt('Enter name for the duplicated route:', route.name + ' - Copy');
    if (!newName) return;
    
    try {
        const response = await fetch('/api/routes/master', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                name: newName,
                origin: route.origin,
                destination: route.destination,
                category: route.category,
                active: route.active
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showAlert('success', 'Route duplicated successfully');
            await loadRoutes();
            await loadRoutesForTesting();
        } else {
            throw new Error(result.error || 'Failed to duplicate route');
        }
        
    } catch (error) {
        console.error('Error duplicating route:', error);
        showAlert('error', `Failed to duplicate route: ${error.message}`);
    }
}

// ============ URL Management ============

async function manageUrls(routeId, routeName) {
    currentRouteId = routeId;
    document.getElementById('url-route-name').textContent = routeName;
    
    try {
        // Load existing URLs
        const response = await fetch(`/api/routes/${routeId}/urls`);
        const urls = await response.json();
        
        document.getElementById('redbus-url').value = urls.redbus || '';
        document.getElementById('traveloka-url').value = urls.traveloka || '';
        
        // Show modal
        const modal = new bootstrap.Modal(document.getElementById('urlModal'));
        modal.show();
        
    } catch (error) {
        console.error('Error loading URLs:', error);
        showAlert('error', 'Failed to load URLs');
    }
}

async function saveUrl(platform) {
    if (!currentRouteId) return;
    
    try {
        const url = document.getElementById(`${platform}-url`).value.trim();
        
        if (!url) {
            showAlert('error', 'URL cannot be empty');
            return;
        }
        
        const response = await fetch(`/api/routes/${currentRouteId}/urls/${platform}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ url: url })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showAlert('success', result.message);
            await loadRoutes(); // Refresh to update platform badges
        } else {
            throw new Error(result.error || 'Failed to save URL');
        }
        
    } catch (error) {
        console.error('Error saving URL:', error);
        showAlert('error', `Failed to save URL: ${error.message}`);
    }
}

async function deleteUrl(platform) {
    if (!currentRouteId) return;
    
    if (!confirm(`Are you sure you want to delete the ${platform} URL?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/routes/${currentRouteId}/urls/${platform}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showAlert('success', result.message);
            document.getElementById(`${platform}-url`).value = '';
            await loadRoutes(); // Refresh to update platform badges
        } else {
            throw new Error(result.error || 'Failed to delete URL');
        }
        
    } catch (error) {
        console.error('Error deleting URL:', error);
        showAlert('error', `Failed to delete URL: ${error.message}`);
    }
}

// ============ URL Testing ============

async function loadRoutesForTesting() {
    try {
        const routes = currentRoutes.filter(r => r.active);
        const select = document.getElementById('test-route');
        
        select.innerHTML = '<option value="">Select Route</option>';
        
        routes.forEach(route => {
            const option = document.createElement('option');
            option.value = route.id;
            option.textContent = route.name;
            select.appendChild(option);
        });
        
    } catch (error) {
        console.error('Error loading routes for testing:', error);
    }
}

async function testUrlFormat() {
    try {
        const routeId = document.getElementById('test-route').value;
        const platform = document.getElementById('test-platform').value;
        const date = document.getElementById('test-date').value;
        
        if (!routeId || !platform || !date) {
            showAlert('error', 'Please select route, platform and date');
            return;
        }
        
        const response = await fetch(`/api/routes/format-url?route_id=${routeId}&platform=${platform}&date=${date}`);
        const result = await response.json();
        
        if (response.ok) {
            document.getElementById('test-result').innerHTML = `
                <strong>Success!</strong><br>
                <a href="${result.formatted_url}" target="_blank" class="text-decoration-none">
                    ${result.formatted_url}
                </a>
            `;
        } else {
            throw new Error(result.error || 'Failed to format URL');
        }
        
    } catch (error) {
        console.error('Error testing URL:', error);
        document.getElementById('test-result').innerHTML = `
            <strong class="text-danger">Error:</strong> ${error.message}
        `;
    }
}

// ============ Utility Functions ============

function refreshRoutes() {
    loadRoutes();
    loadRoutesForTesting();
}

function showAlert(type, message) {
    // Remove existing alerts
    const existingAlerts = document.querySelectorAll('.alert-dismissible');
    existingAlerts.forEach(alert => alert.remove());
    
    const alertClass = type === 'error' ? 'alert-danger' : `alert-${type}`;
    const icon = type === 'error' ? 'exclamation-triangle' : 'check-circle';
    
    const alert = document.createElement('div');
    alert.className = `alert ${alertClass} alert-dismissible fade show position-fixed`;
    alert.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
    alert.innerHTML = `
        <i class="bi bi-${icon}"></i> ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    document.body.appendChild(alert);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        if (alert.parentNode) {
            alert.remove();
        }
    }, 5000);
}

function showLoading(message = 'Loading...') {
    // Simple loading implementation
    const container = document.getElementById('routes-container');
    container.innerHTML = `
        <div class="col-12 text-center">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">${message}</span>
            </div>
            <p class="mt-2">${message}</p>
        </div>
    `;
}

function hideLoading() {
    // Loading is hidden when routes are displayed
}