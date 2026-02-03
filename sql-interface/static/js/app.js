// Variables globales
let currentResults = null;
let currentConfig = null;

// Charger la configuration au démarrage
document.addEventListener('DOMContentLoaded', () => {
    loadConfig();
});

// Toggle Configuration Panel
function toggleConfig() {
    const panel = document.getElementById('configPanel');
    if (panel.style.display === 'none') {
        panel.style.display = 'block';
        loadConfig();
    } else {
        panel.style.display = 'none';
    }
}

// Charger la configuration actuelle
async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();
        currentConfig = data;
        
        document.getElementById('endpoint').value = data.endpoint || '';
        document.getElementById('useSSL').checked = data.s3_use_ssl === 'true';
    } catch (error) {
        console.error('Erreur lors du chargement de la configuration:', error);
    }
}

// Sauvegarder la configuration
async function saveConfig() {
    const endpoint = document.getElementById('endpoint').value;
    const accessKey = document.getElementById('accessKey').value;
    const secretKey = document.getElementById('secretKey').value;
    const useSSL = document.getElementById('useSSL').checked;
    
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                endpoint: endpoint,
                s3_access_key_id: accessKey,
                s3_secret_access_key: secretKey,
                s3_use_ssl: useSSL ? 'true' : 'false'
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('Configuration sauvegardée avec succès!', 'success');
            toggleConfig();
        } else {
            showNotification('Erreur: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Erreur lors de la sauvegarde: ' + error.message, 'error');
    }
}

// Exécuter une requête SQL
async function executeQuery() {
    const query = document.getElementById('queryEditor').value.trim();
    
    if (!query) {
        showNotification('Veuillez entrer une requête SQL', 'warning');
        return;
    }
    
    // Afficher le loading overlay
    document.getElementById('loadingOverlay').style.display = 'flex';
    
    // Désactiver les boutons d'export
    document.getElementById('exportCsvBtn').disabled = true;
    document.getElementById('exportJsonBtn').disabled = true;
    
    try {
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query: query })
        });
        
        const data = await response.json();
        
        if (data.success) {
            currentResults = data;
            displayResults(data);
            
            // Activer les boutons d'export
            document.getElementById('exportCsvBtn').disabled = false;
            document.getElementById('exportJsonBtn').disabled = false;
            
            // Afficher les infos d'exécution
            document.getElementById('executionInfo').innerHTML = `
                <i class="fas fa-check-circle" style="color: var(--success-color);"></i>
                Exécuté en ${data.executionTime}s
            `;
        } else {
            displayError(data.error, data.stackTrace);
            document.getElementById('executionInfo').innerHTML = '';
        }
    } catch (error) {
        displayError('Erreur de connexion: ' + error.message);
        document.getElementById('executionInfo').innerHTML = '';
    } finally {
        document.getElementById('loadingOverlay').style.display = 'none';
    }
}

// Afficher les résultats
function displayResults(data) {
    const container = document.getElementById('resultsContainer');
    
    if (data.data.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-inbox"></i>
                <p>Aucun résultat trouvé</p>
            </div>
        `;
        return;
    }
    
    // Créer l'info bar
    const infoBar = `
        <div class="results-info">
            <div class="results-info-item">
                <i class="fas fa-list"></i>
                <strong>${data.rowCount}</strong> ligne(s)
            </div>
            <div class="results-info-item">
                <i class="fas fa-columns"></i>
                <strong>${data.columns.length}</strong> colonne(s)
            </div>
            <div class="results-info-item">
                <i class="fas fa-clock"></i>
                <strong>${data.executionTime}s</strong>
            </div>
        </div>
    `;
    
    // Créer le tableau
    let tableHTML = `
        ${infoBar}
        <div class="results-table-container">
            <table class="results-table">
                <thead>
                    <tr>
    `;
    
    // En-têtes
    data.columns.forEach(col => {
        tableHTML += `<th>${escapeHtml(col)}</th>`;
    });
    
    tableHTML += `
                    </tr>
                </thead>
                <tbody>
    `;
    
    // Données
    data.data.forEach(row => {
        tableHTML += '<tr>';
        data.columns.forEach(col => {
            const value = row[col];
            tableHTML += `<td>${value !== null && value !== undefined ? escapeHtml(String(value)) : '<em style="color: #999;">NULL</em>'}</td>`;
        });
        tableHTML += '</tr>';
    });
    
    tableHTML += `
                </tbody>
            </table>
        </div>
    `;
    
    container.innerHTML = tableHTML;
}

// Afficher une erreur
function displayError(error, stackTrace) {
    const container = document.getElementById('resultsContainer');
    
    let errorHTML = `
        <div class="error-message">
            <strong><i class="fas fa-exclamation-triangle"></i> Erreur SQL</strong>
            <p>${escapeHtml(error)}</p>
    `;
    
    if (stackTrace) {
        errorHTML += `
            <details>
                <summary>Détails techniques</summary>
                <div class="error-details">${escapeHtml(stackTrace)}</div>
            </details>
        `;
    }
    
    errorHTML += '</div>';
    
    container.innerHTML = errorHTML;
}

// Charger les exemples
async function loadExamples() {
    const dropdown = document.getElementById('examplesDropdown');
    
    if (dropdown.style.display === 'block') {
        dropdown.style.display = 'none';
        return;
    }
    
    try {
        const response = await fetch('/api/examples');
        const data = await response.json();
        
        let examplesHTML = '';
        data.examples.forEach(example => {
            examplesHTML += `
                <div class="example-item" onclick="selectExample(\`${escapeHtml(example.query)}\`)">
                    <h4>${escapeHtml(example.name)}</h4>
                    <code>${escapeHtml(example.query)}</code>
                </div>
            `;
        });
        
        document.getElementById('examplesList').innerHTML = examplesHTML;
        dropdown.style.display = 'block';
    } catch (error) {
        showNotification('Erreur lors du chargement des exemples', 'error');
    }
}

// Sélectionner un exemple
function selectExample(query) {
    document.getElementById('queryEditor').value = query;
    document.getElementById('examplesDropdown').style.display = 'none';
}

// Effacer la requête
function clearQuery() {
    if (confirm('Êtes-vous sûr de vouloir effacer la requête ?')) {
        document.getElementById('queryEditor').value = '';
        document.getElementById('executionInfo').innerHTML = '';
    }
}

// Exporter les résultats
function exportResults(format) {
    if (!currentResults || !currentResults.data) {
        showNotification('Aucun résultat à exporter', 'warning');
        return;
    }
    
    let content, filename, mimeType;
    
    if (format === 'csv') {
        content = convertToCSV(currentResults);
        filename = 'results.csv';
        mimeType = 'text/csv';
    } else if (format === 'json') {
        content = JSON.stringify(currentResults.data, null, 2);
        filename = 'results.json';
        mimeType = 'application/json';
    }
    
    // Télécharger le fichier
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification(`Résultats exportés en ${format.toUpperCase()}`, 'success');
}

// Convertir en CSV
function convertToCSV(data) {
    const columns = data.columns;
    const rows = data.data;
    
    // En-têtes
    let csv = columns.map(col => `"${col}"`).join(',') + '\n';
    
    // Données
    rows.forEach(row => {
        const values = columns.map(col => {
            const value = row[col];
            if (value === null || value === undefined) return '';
            return `"${String(value).replace(/"/g, '""')}"`;
        });
        csv += values.join(',') + '\n';
    });
    
    return csv;
}

// Afficher une notification
function showNotification(message, type = 'info') {
    // Créer l'élément de notification
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 25px;
        background: ${type === 'success' ? '#16a34a' : type === 'error' ? '#dc2626' : type === 'warning' ? '#f59e0b' : '#2563eb'};
        color: white;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        z-index: 10000;
        animation: slideIn 0.3s ease;
        font-weight: 600;
    `;
    
    notification.innerHTML = `
        <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : type === 'warning' ? 'exclamation-triangle' : 'info-circle'}"></i>
        ${message}
    `;
    
    document.body.appendChild(notification);
    
    // Supprimer après 3 secondes
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => {
            document.body.removeChild(notification);
        }, 300);
    }, 3000);
}

// Échapper le HTML
function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;',
        '`': '&#96;'
    };
    return text.replace(/[&<>"'`]/g, m => map[m]);
}

// Raccourcis clavier
document.addEventListener('keydown', (e) => {
    // Ctrl+Enter ou Cmd+Enter pour exécuter
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        executeQuery();
    }
    
    // Échap pour fermer les dropdowns
    if (e.key === 'Escape') {
        document.getElementById('examplesDropdown').style.display = 'none';
    }
});

// Fermer le dropdown des exemples en cliquant ailleurs
document.addEventListener('click', (e) => {
    const dropdown = document.getElementById('examplesDropdown');
    const button = e.target.closest('button');
    
    if (dropdown.style.display === 'block' && (!button || !button.textContent.includes('Exemples'))) {
        dropdown.style.display = 'none';
    }
});

// Ajouter les animations CSS dynamiquement
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(100%);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);
