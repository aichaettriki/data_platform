// Variables globales
let currentResults = null;
let currentConfig = null;
let dataTableInstance = null;

document.addEventListener('DOMContentLoaded', () => {
    loadConfig();
});

function toggleConfig() {
    const panel = document.getElementById('configPanel');
    if (panel.style.display === 'none') {
        panel.style.display = 'block';
        loadConfig();
    } else {
        panel.style.display = 'none';
    }
}

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

async function saveConfig() {
    const endpoint  = document.getElementById('endpoint').value;
    const accessKey = document.getElementById('accessKey').value;
    const secretKey = document.getElementById('secretKey').value;
    const useSSL    = document.getElementById('useSSL').checked;

    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                endpoint: endpoint,
                s3_access_key_id: accessKey,
                s3_secret_access_key: secretKey,
                s3_use_ssl: useSSL ? 'true' : 'false'
            })
        });
        const data = await response.json();
        if (data.success) {
            showNotification('Configuration sauvegardee avec succes!', 'success');
            toggleConfig();
        } else {
            showNotification('Erreur: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Erreur lors de la sauvegarde: ' + error.message, 'error');
    }
}

async function executeQuery() {
    const query = document.getElementById('queryEditor').value.trim();

    if (!query) {
        showNotification('Veuillez entrer une requete SQL', 'warning');
        return;
    }

    document.getElementById('loadingOverlay').style.display = 'flex';
    document.getElementById('exportCsvBtn').disabled = true;
    document.getElementById('exportJsonBtn').disabled = true;

    try {
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: query })
        });

        const data = await response.json();

        if (data.success) {
            currentResults = data;
            displayResults(data);

            document.getElementById('exportCsvBtn').disabled = false;
            document.getElementById('exportJsonBtn').disabled = false;

            document.getElementById('executionInfo').innerHTML = `
                <i class="fas fa-check-circle" style="color: var(--success-color);"></i>
                Execute en ${data.executionTime}s
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

function displayResults(data) {
    const container = document.getElementById('resultsContainer');

    if (data.data.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-inbox"></i>
                <p>Aucun resultat trouve</p>
            </div>
        `;
        return;
    }

    if (dataTableInstance) {
        dataTableInstance.destroy();
        dataTableInstance = null;
    }

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

    const tableHTML = `
        ${infoBar}
        <div class="results-table-wrapper">
            <table id="resultsTable" class="display nowrap" style="width:100%">
                <thead>
                    <tr>
                        ${data.columns.map(col => '<th>' + escapeHtml(col) + '</th>').join('')}
                    </tr>
                    <tr>
                        ${data.columns.map(() => '<th></th>').join('')}
                    </tr>
                </thead>
                <tbody></tbody>
            </table>
        </div>
    `;

    container.innerHTML = tableHTML;

    // Préparer les données pour DataTables (tableau de tableaux)
    const dtData = data.data.map(function(row) {
        return data.columns.map(function(col) {
            const v = row[col];
            return (v !== null && v !== undefined) ? String(v) : '';
        });
    });

    dataTableInstance = new DataTable('#resultsTable', {
        orderCellsTop: true,
        fixedHeader: true,
        scrollX: true,
        scrollY: '420px',
        scrollCollapse: true,
        pageLength: 25,
        lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, 'Tout']],
        data: dtData,
        columns: data.columns.map(function(col) {
            return {
                title: escapeHtml(col),
                defaultContent: '<em style="color:#999;">NULL</em>'
            };
        }),
        language: {
            url: 'https://cdn.datatables.net/plug-ins/2.0.8/i18n/fr-FR.json'
        },
        layout: {
            topStart: 'pageLength',
            topEnd: 'search',
            bottomStart: 'info',
            bottomEnd: 'paging'
        },
        initComplete: function() {
            this.api().columns().every(function() {
                const column     = this;
                const headerCell = $(column.header(1));

                // Valeurs uniques de la colonne
                const uniqueValues = [...new Set(
                    column.data().toArray().map(function(v) {
                        return (v === '' || v === null || v === undefined) ? 'NULL' : String(v);
                    })
                )].sort(function(a, b) {
                    return a.localeCompare(b, undefined, { numeric: true });
                });

                const wrapper = $('<div class="col-filter-wrapper"></div>');

                // ── Select valeurs uniques ────────────────────
                if (uniqueValues.length <= 200) {
                    const select = $('<select><option value="">Tous</option></select>');
                    uniqueValues.forEach(function(val) {
                        select.append('<option value="' + escapeHtml(val) + '">' + escapeHtml(val) + '</option>');
                    });
                    wrapper.append(select);
                } else {
                    wrapper.append('<span class="filter-na" title="Trop de valeurs uniques">-</span>');
                }

                // ── Input texte libre ─────────────────────────
                const input = $('<input type="text" class="col-filter-input" placeholder="Rechercher...">');
                wrapper.append(input);
                headerCell.empty().append(wrapper);

                // ── Filtre combiné ────────────────────────────
                function applyColumnFilter() {
                    const selectVal = wrapper.find('select').val() || '';
                    const inputVal  = wrapper.find('input').val().trim();

                    if (selectVal && inputVal) {
                        // Les deux actifs : lookahead regex
                        column.search(
                            '(?=.*^' + $.fn.dataTable.util.escapeRegex(selectVal) + '$)(?=.*' + $.fn.dataTable.util.escapeRegex(inputVal) + ')',
                            true, false
                        ).draw();
                    } else if (selectVal) {
                        column.search('^' + $.fn.dataTable.util.escapeRegex(selectVal) + '$', true, false).draw();
                    } else if (inputVal) {
                        column.search($.fn.dataTable.util.escapeRegex(inputVal), true, false).draw();
                    } else {
                        column.search('', true, false).draw();
                    }
                }

                wrapper.find('select').on('change', applyColumnFilter);
                wrapper.find('input').on('input', applyColumnFilter);
            });
        }
    });
}

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
                <summary>Details techniques</summary>
                <div class="error-details">${escapeHtml(stackTrace)}</div>
            </details>
        `;
    }

    errorHTML += '</div>';
    container.innerHTML = errorHTML;
}

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
        data.examples.forEach(function(example) {
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

function selectExample(query) {
    document.getElementById('queryEditor').value = query;
    document.getElementById('examplesDropdown').style.display = 'none';
}

function clearQuery() {
    if (confirm('Etes-vous sur de vouloir effacer la requete ?')) {
        document.getElementById('queryEditor').value = '';
        document.getElementById('executionInfo').innerHTML = '';
    }
}

function exportResults(format) {
    if (!currentResults || !currentResults.data) {
        showNotification('Aucun resultat a exporter', 'warning');
        return;
    }

    let content, filename, mimeType;

    if (format === 'csv') {
        content  = convertToCSV(currentResults);
        filename = 'results.csv';
        mimeType = 'text/csv';
    } else if (format === 'json') {
        content  = JSON.stringify(currentResults.data, null, 2);
        filename = 'results.json';
        mimeType = 'application/json';
    }

    const blob = new Blob([content], { type: mimeType });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    showNotification('Resultats exportes en ' + format.toUpperCase(), 'success');
}

function convertToCSV(data) {
    const columns = data.columns;
    const rows    = data.data;

    let csv = columns.map(function(col) { return '"' + col + '"'; }).join(',') + '\n';

    rows.forEach(function(row) {
        const values = columns.map(function(col) {
            const value = row[col];
            if (value === null || value === undefined) return '';
            return '"' + String(value).replace(/"/g, '""') + '"';
        });
        csv += values.join(',') + '\n';
    });

    return csv;
}

function showNotification(message, type) {
    type = type || 'info';
    const notification = document.createElement('div');
    const bg = type === 'success' ? '#16a34a' : type === 'error' ? '#dc2626' : type === 'warning' ? '#f59e0b' : '#2563eb';
    const icon = type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : type === 'warning' ? 'exclamation-triangle' : 'info-circle';

    notification.style.cssText = [
        'position:fixed', 'top:20px', 'right:20px',
        'padding:15px 25px', 'background:' + bg,
        'color:white', 'border-radius:8px',
        'box-shadow:0 4px 6px rgba(0,0,0,0.1)',
        'z-index:10000', 'animation:slideIn 0.3s ease',
        'font-weight:600'
    ].join(';');

    notification.innerHTML = '<i class="fas fa-' + icon + '"></i> ' + message;
    document.body.appendChild(notification);

    setTimeout(function() {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(function() {
            if (document.body.contains(notification)) {
                document.body.removeChild(notification);
            }
        }, 300);
    }, 3000);
}

function escapeHtml(text) {
    const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;', '`': '&#96;' };
    return String(text).replace(/[&<>"'`]/g, function(m) { return map[m]; });
}

document.addEventListener('keydown', function(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        executeQuery();
    }
    if (e.key === 'Escape') {
        document.getElementById('examplesDropdown').style.display = 'none';
    }
});

document.addEventListener('click', function(e) {
    const dropdown = document.getElementById('examplesDropdown');
    const button   = e.target.closest('button');
    if (dropdown.style.display === 'block' && (!button || !button.textContent.includes('Exemples'))) {
        dropdown.style.display = 'none';
    }
});

// Animations CSS
const style = document.createElement('style');
style.textContent = [
    '@keyframes slideIn { from { transform:translateX(100%); opacity:0; } to { transform:translateX(0); opacity:1; } }',
    '@keyframes slideOut { from { transform:translateX(0); opacity:1; } to { transform:translateX(100%); opacity:0; } }'
].join(' ');
document.head.appendChild(style);