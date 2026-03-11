// document.addEventListener('DOMContentLoaded', () => {
//     const dropZone = document.getElementById('drop-zone');
//     const fileInput = document.getElementById('file-input');
//     const uploadSection = document.getElementById('upload-section');
//     const loadingState = document.getElementById('loading-state');
//     const errorMsgDiv = document.getElementById('error-message');

//     const resultsSection = document.getElementById('results-section');
//     const itemSelect = document.getElementById('item-select');
//     const downloadCsvBtn = document.getElementById('download-csv-btn');
//     const uploadNewBtn = document.getElementById('upload-new-btn');

//     const tableDisplay = document.getElementById('table-display');
//     const chartDisplay = document.getElementById('chart-display');
//     const chartImage = document.getElementById('chart-image');
//     const chartDescription = document.getElementById('chart-description');
//     const chartDataSection = document.getElementById('chart-data-section');
//     const chartTableContainer = document.getElementById('chart-table-container');

//     let allItems = []; // unified list: {type:'table'|'chart', data:{...}}
//     let currentItem = null;

//     // ── Drag & Drop ──────────────────────────────────────────────────────────
//     ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(e => {
//         dropZone.addEventListener(e, ev => { ev.preventDefault(); ev.stopPropagation(); }, false);
//     });
//     ['dragenter', 'dragover'].forEach(e => {
//         dropZone.addEventListener(e, () => dropZone.classList.add('drag-over'), false);
//     });
//     ['dragleave', 'drop'].forEach(e => {
//         dropZone.addEventListener(e, () => dropZone.classList.remove('drag-over'), false);
//     });
//     dropZone.addEventListener('drop', e => handleFiles(e.dataTransfer.files), false);
//     dropZone.addEventListener('click', () => fileInput.click());
//     fileInput.addEventListener('change', function () {
//         if (this.files.length > 0) handleFiles(this.files);
//     });

//     function handleFiles(files) {
//         const file = files[0];
//         if (file && file.type === 'application/pdf') {
//             uploadFile(file);
//         } else {
//             showError('Please upload a valid PDF file.');
//         }
//     }

//     uploadNewBtn.addEventListener('click', () => {
//         resultsSection.classList.add('hidden');
//         uploadSection.classList.remove('hidden');
//         dropZone.style.display = 'block';
//         loadingState.classList.add('hidden');
//         errorMsgDiv.classList.add('hidden');
//         fileInput.value = '';
//         allItems = [];
//         currentItem = null;
//         downloadCsvBtn.disabled = true;
//     });

//     function showError(msg) {
//         errorMsgDiv.textContent = msg;
//         errorMsgDiv.classList.remove('hidden');
//         dropZone.style.display = 'block';
//         loadingState.classList.add('hidden');
//     }

//     // ══════════════════════════════════════════════════════════════════════════
//     // UPLOAD
//     // ══════════════════════════════════════════════════════════════════════════

//     async function uploadFile(file) {
//         dropZone.style.display = 'none';
//         errorMsgDiv.classList.add('hidden');
//         loadingState.classList.remove('hidden');

//         const formData = new FormData();
//         formData.append('file', file);

//         try {
//             const response = await fetch('/upload', { method: 'POST', body: formData });
//             const data = await response.json();

//             if (!response.ok || !data.success) {
//                 showError(data.error || 'An error occurred.');
//                 return;
//             }

//             const tables = data.tables || [];
//             const charts = data.charts || [];

//             if (tables.length === 0 && charts.length === 0) {
//                 showError('No tables or charts found in this PDF.');
//                 return;
//             }

//             // Build unified items list
//             allItems = [];
//             tables.forEach(t => allItems.push({ type: 'table', data: t }));
//             charts.forEach(c => allItems.push({ type: 'chart', data: c }));

//             showResults();

//         } catch (error) {
//             showError('Network error. Please make sure the server is running.');
//             console.error(error);
//         }
//     }

//     // ══════════════════════════════════════════════════════════════════════════
//     // RESULTS
//     // ══════════════════════════════════════════════════════════════════════════

//     function showResults() {
//         uploadSection.classList.add('hidden');
//         resultsSection.classList.remove('hidden');
//         tableDisplay.classList.add('hidden');
//         chartDisplay.classList.add('hidden');

//         // Populate dropdown
//         itemSelect.innerHTML = '<option value="" disabled selected>Choose a table or chart...</option>';

//         // Group: Tables
//         if (allItems.some(i => i.type === 'table')) {
//             const grp = document.createElement('optgroup');
//             grp.label = 'Tables';
//             allItems.forEach((item, idx) => {
//                 if (item.type === 'table') {
//                     const opt = document.createElement('option');
//                     opt.value = idx;
//                     opt.textContent = `Page ${item.data.page} - ${item.data.title}`;
//                     grp.appendChild(opt);
//                 }
//             });
//             itemSelect.appendChild(grp);
//         }

//         // Group: Charts
//         if (allItems.some(i => i.type === 'chart')) {
//             const grp = document.createElement('optgroup');
//             grp.label = 'Charts';
//             allItems.forEach((item, idx) => {
//                 if (item.type === 'chart') {
//                     const opt = document.createElement('option');
//                     opt.value = idx;
//                     opt.textContent = `Page ${item.data.page} - ${item.data.title}`;
//                     grp.appendChild(opt);
//                 }
//             });
//             itemSelect.appendChild(grp);
//         }

//         downloadCsvBtn.disabled = true;
//     }

//     // ── Selection ────────────────────────────────────────────────────────────
//     itemSelect.addEventListener('change', function () {
//         const idx = parseInt(this.value);
//         if (isNaN(idx)) return;

//         currentItem = allItems[idx];
//         downloadCsvBtn.disabled = false;

//         if (currentItem.type === 'table') {
//             chartDisplay.classList.add('hidden');
//             tableDisplay.classList.remove('hidden');
//             renderTable(currentItem.data);
//         } else {
//             tableDisplay.classList.add('hidden');
//             chartDisplay.classList.remove('hidden');
//             renderChart(currentItem.data);
//         }
//     });

//     // ══════════════════════════════════════════════════════════════════════════
//     // TABLE RENDERING
//     // ══════════════════════════════════════════════════════════════════════════

//     function renderTable(tableData) {
//         if (!tableData || !tableData.rows || tableData.rows.length === 0) {
//             tableDisplay.innerHTML = '<div class="empty-state"><p>No data available.</p></div>';
//             return;
//         }
//         const table = document.createElement('table');
//         const tbody = document.createElement('tbody');
//         let maxCols = 0;
//         tableData.rows.forEach(row => { if (row.length > maxCols) maxCols = row.length; });

//         tableData.rows.forEach((rowData, rowIndex) => {
//             const tr = document.createElement('tr');
//             for (let i = 0; i < maxCols; i++) {
//                 const tag = rowIndex === 0 ? 'th' : 'td';
//                 const cell = document.createElement(tag);
//                 cell.textContent = rowData[i] !== undefined ? rowData[i] : '';
//                 tr.appendChild(cell);
//             }
//             tbody.appendChild(tr);
//         });
//         table.appendChild(tbody);
//         tableDisplay.innerHTML = '';
//         tableDisplay.appendChild(table);
//     }

//     // ══════════════════════════════════════════════════════════════════════════
//     // CHART RENDERING (image + AI description + data table)
//     // ══════════════════════════════════════════════════════════════════════════

//     function renderChart(chartData) {
//         // Show image
//         chartImage.src = `data:image/png;base64,${chartData.image_base64}`;

//         // Show AI description
//         if (chartData.description) {
//             chartDescription.classList.remove('hidden');
//             chartDescription.innerHTML = `
//                 <h4><i class="fa-solid fa-lightbulb"></i> AI Interpretation</h4>
//                 <p>${chartData.description}</p>`;
//         } else {
//             chartDescription.classList.add('hidden');
//         }

//         // Show extracted data table
//         if (chartData.csv_rows && chartData.csv_rows.length > 0) {
//             chartDataSection.classList.remove('hidden');
//             chartTableContainer.innerHTML = buildTable(chartData.csv_rows);
//         } else {
//             chartDataSection.classList.add('hidden');
//         }
//     }

//     function buildTable(csvRows) {
//         let html = '<table class="ai-csv-table">';
//         csvRows.forEach((row, idx) => {
//             html += '<tr>';
//             row.forEach(cell => {
//                 const tag = idx === 0 ? 'th' : 'td';
//                 html += `<${tag}>${cell}</${tag}>`;
//             });
//             html += '</tr>';
//         });
//         return html + '</table>';
//     }

//     // ══════════════════════════════════════════════════════════════════════════
//     // DOWNLOAD CSV
//     // ══════════════════════════════════════════════════════════════════════════

//     downloadCsvBtn.addEventListener('click', () => {
//         if (!currentItem) return;

//         let csvContent, fileName;
//         const BOM = '\uFEFF';
//         const page = currentItem.data.page;
//         const title = currentItem.data.title;
//         const safeName = title.replace(/[^a-zA-Z0-9 \-_]/g, '_').trim().slice(0, 50);

//         if (currentItem.type === 'table') {
//             csvContent = currentItem.data.rows
//                 .map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(';'))
//                 .join('\n');
//             csvContent = `"TITRE: ${title}"\n\n` + csvContent;
//             fileName = `table_p${page}_${safeName}.csv`;
//         } else {
//             // Chart: use the AI-generated CSV
//             csvContent = currentItem.data.csv_data || '';
//             fileName = `chart_ai_p${page}_${safeName}.csv`;
//         }

//         const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' });
//         const url = URL.createObjectURL(blob);
//         const a = document.createElement('a');
//         a.href = url;
//         a.style.display = 'none';
//         a.download = fileName;
//         document.body.appendChild(a);
//         a.click();
//         document.body.removeChild(a);
//         URL.revokeObjectURL(url);
//     });
// });


document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const uploadSection = document.getElementById('upload-section');
    const loadingState = document.getElementById('loading-state');
    const errorMsgDiv = document.getElementById('error-message');

    const resultsSection = document.getElementById('results-section');
    const customMultiselect = document.getElementById('custom-multiselect');
    const multiselectHeader = document.getElementById('multiselect-header');
    const multiselectHeaderText = document.getElementById('multiselect-header-text');
    const multiselectOptions = document.getElementById('multiselect-options');
    const downloadCsvBtn = document.getElementById('download-csv-btn');
    const uploadNewBtn = document.getElementById('upload-new-btn');

    const pageStartInput = document.getElementById('page-start');
    const pageEndInput = document.getElementById('page-end');

    const filterPageStartInput = document.getElementById('filter-page-start');
    const filterPageEndInput = document.getElementById('filter-page-end');
    const applyFilterBtn = document.getElementById('apply-filter-btn');

    const resultsContainer = document.getElementById('results-container');
    const emptySelectionState = document.getElementById('empty-selection-state');

    let allItems = []; // unified list: {type:'table'|'chart', data:{...}}
    let currentItem = null;

    // ── Drag & Drop ──────────────────────────────────────────────────────────
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(e => {
        dropZone.addEventListener(e, ev => { ev.preventDefault(); ev.stopPropagation(); }, false);
    });
    ['dragenter', 'dragover'].forEach(e => {
        dropZone.addEventListener(e, () => dropZone.classList.add('drag-over'), false);
    });
    ['dragleave', 'drop'].forEach(e => {
        dropZone.addEventListener(e, () => dropZone.classList.remove('drag-over'), false);
    });
    dropZone.addEventListener('drop', e => handleFiles(e.dataTransfer.files), false);
    dropZone.addEventListener('click', () => fileInput.click());
    fileInput.addEventListener('change', function () {
        if (this.files.length > 0) handleFiles(this.files);
    });

    function handleFiles(files) {
        const file = files[0];
        if (file && file.type === 'application/pdf') {
            uploadFile(file);
        } else {
            showError('Please upload a valid PDF file.');
        }
    }

    uploadNewBtn.addEventListener('click', () => {
        resultsSection.classList.add('hidden');
        uploadSection.classList.remove('hidden');
        dropZone.style.display = 'block';
        loadingState.classList.add('hidden');
        errorMsgDiv.classList.add('hidden');
        fileInput.value = '';
        allItems = [];
        currentItem = null;
        downloadCsvBtn.disabled = true;
        customTitleInput.value = '';
    });

    function showError(msg) {
        errorMsgDiv.textContent = msg;
        errorMsgDiv.classList.remove('hidden');
        dropZone.style.display = 'block';
        loadingState.classList.add('hidden');
    }

    // ══════════════════════════════════════════════════════════════════════════
    // UPLOAD
    // ══════════════════════════════════════════════════════════════════════════

    async function uploadFile(file) {
        dropZone.style.display = 'none';
        errorMsgDiv.classList.add('hidden');
        loadingState.classList.remove('hidden');

        const formData = new FormData();
        formData.append('file', file);
        if (pageStartInput.value) formData.append('page_start', pageStartInput.value);
        if (pageEndInput.value) formData.append('page_end', pageEndInput.value);

        try {
            const response = await fetch('/upload', { method: 'POST', body: formData });
            const data = await response.json();

            if (!response.ok || !data.success) {
                showError(data.error || 'An error occurred.');
                return;
            }

            const tables = data.tables || [];
            const charts = data.charts || [];

            if (tables.length === 0 && charts.length === 0) {
                showError('No tables or charts found in this PDF.');
                return;
            }

            // Build unified items list
            allItems = [];
            tables.forEach(t => allItems.push({ type: 'table', data: t }));
            charts.forEach(c => allItems.push({ type: 'chart', data: c }));

            showResults();

        } catch (error) {
            showError('Network error. Please make sure the server is running.');
            console.error(error);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // RESULTS
    // ══════════════════════════════════════════════════════════════════════════

    let selectedItemsList = []; // Array of { originalIndex, item, inputId }

    if (customMultiselect) {
        multiselectHeader.addEventListener('click', () => {
            multiselectOptions.classList.toggle('hidden');
        });

        document.addEventListener('click', (e) => {
            if (!customMultiselect.contains(e.target)) {
                multiselectOptions.classList.add('hidden');
            }
        });
    }

    function checkEmptyState() {
        if (selectedItemsList.length === 0) {
            emptySelectionState.classList.remove('hidden');
            downloadCsvBtn.disabled = true;
        } else {
            emptySelectionState.classList.add('hidden');
            downloadCsvBtn.disabled = false;
        }
    }

    function updateHeaderText() {
        const count = selectedItemsList.length;
        if (count === 0) {
            multiselectHeaderText.textContent = 'Choose tables/charts...';
        } else if (count === 1) {
            multiselectHeaderText.textContent = '1 item selected';
        } else {
            multiselectHeaderText.textContent = `${count} items selected`;
        }
    }

    function showResults(filterStart = null, filterEnd = null) {
        uploadSection.classList.add('hidden');
        resultsSection.classList.remove('hidden');

        // Clear results container (except the empty state)
        Array.from(resultsContainer.children).forEach(child => {
            if (child.id !== 'empty-selection-state') {
                child.remove();
            }
        });
        emptySelectionState.classList.remove('hidden');
        selectedItemsList = [];
        updateHeaderText();

        // Populate custom dropdown
        multiselectOptions.innerHTML = '';

        let filteredItems = allItems.map((item, idx) => ({ item, idx }));

        if (filterStart !== null && filterEnd !== null) {
            filteredItems = filteredItems.filter(obj => obj.item.data.page >= filterStart && obj.item.data.page <= filterEnd);
        }

        // Helper to add options
        const addOption = (obj) => {
            const optContainer = document.createElement('label');
            optContainer.style.display = 'block';
            optContainer.style.padding = '0.4rem 0.8rem';
            optContainer.style.cursor = 'pointer';
            optContainer.style.borderBottom = '1px solid rgba(255,255,255,0.05)';
            optContainer.className = 'multiselect-option-label';

            optContainer.innerHTML = `
                <input type="checkbox" value="${obj.idx}" class="multiselect-checkbox" style="margin-right: 0.5rem;" />
                Page ${obj.item.data.page} - ${obj.item.data.title}
            `;

            optContainer.addEventListener('mouseenter', () => optContainer.style.backgroundColor = 'rgba(255,255,255,0.1)');
            optContainer.addEventListener('mouseleave', () => optContainer.style.backgroundColor = 'transparent');

            const cb = optContainer.querySelector('input');
            cb.addEventListener('change', (e) => {
                handleSelectionToggle(obj.idx, e.target.checked, cb);
            });

            multiselectOptions.appendChild(optContainer);
        };

        // Group: Tables
        if (filteredItems.some(obj => obj.item.type === 'table')) {
            const grpTitle = document.createElement('div');
            grpTitle.style.padding = '0.4rem 0.8rem';
            grpTitle.style.fontWeight = 'bold';
            grpTitle.style.backgroundColor = 'rgba(0,0,0,0.3)';
            grpTitle.textContent = 'Tables';
            multiselectOptions.appendChild(grpTitle);

            filteredItems.forEach(obj => {
                if (obj.item.type === 'table') addOption(obj);
            });
        }

        // Group: Charts
        if (filteredItems.some(obj => obj.item.type === 'chart')) {
            const grpTitle = document.createElement('div');
            grpTitle.style.padding = '0.4rem 0.8rem';
            grpTitle.style.fontWeight = 'bold';
            grpTitle.style.backgroundColor = 'rgba(0,0,0,0.3)';
            grpTitle.textContent = 'Charts';
            multiselectOptions.appendChild(grpTitle);

            filteredItems.forEach(obj => {
                if (obj.item.type === 'chart') addOption(obj);
            });
        }

        downloadCsvBtn.disabled = true;
    }

    // ── Apply Result Filter ────────────────────────────────────────────────────────
    if (applyFilterBtn) {
        applyFilterBtn.addEventListener('click', () => {
            let start = parseInt(filterPageStartInput.value);
            let end = parseInt(filterPageEndInput.value);
            if (isNaN(start) || isNaN(end)) {
                showResults(); // Reset to show all
            } else {
                showResults(start, end);
            }
        });
    }

    // ── Selection ────────────────────────────────────────────────────────────

    function handleSelectionToggle(idx, isChecked, checkboxEl) {
        if (isChecked) {
            // Check if already added
            if (selectedItemsList.some(obj => obj.originalIndex === idx)) {
                return;
            }

            const item = allItems[idx];
            const uniqueId = `custom-title-${idx}-${Date.now()}`;

            const itemObj = {
                originalIndex: idx,
                item: item,
                inputId: uniqueId
            };
            selectedItemsList.push(itemObj);

            checkEmptyState();
            updateHeaderText();

            // Build the card
            const card = document.createElement('div');
            card.id = `result-card-${uniqueId}`;
            card.className = 'glass-panel result-card';
            card.style.position = 'relative';
            card.style.marginBottom = '2rem';
            card.style.padding = '1.5rem';

            // Add remove button
            const removeBtn = document.createElement('button');
            removeBtn.innerHTML = '<i class="fa-solid fa-xmark"></i>';
            removeBtn.style.position = 'absolute';
            removeBtn.style.top = '1.5rem';
            removeBtn.style.right = '1.5rem';
            removeBtn.style.background = 'none';
            removeBtn.style.border = 'none';
            removeBtn.style.color = 'var(--text-muted)';
            removeBtn.style.cursor = 'pointer';
            removeBtn.style.fontSize = '1.2rem';
            removeBtn.onmouseenter = () => removeBtn.style.color = '#ff4444';
            removeBtn.onmouseleave = () => removeBtn.style.color = 'var(--text-muted)';
            removeBtn.onclick = () => {
                card.remove();
                selectedItemsList = selectedItemsList.filter(obj => obj !== itemObj);
                checkboxEl.checked = false; // Sync checkbox state
                checkEmptyState();
                updateHeaderText();
            };
            card.appendChild(removeBtn);

            // Header with title input
            const header = document.createElement('div');
            header.style.marginBottom = '1rem';
            header.style.paddingRight = '2rem'; // make room for remove custom X button
            header.innerHTML = `
                <label for="${uniqueId}" style="font-size: 0.85rem; font-weight: 600; display: block; margin-bottom: 0.4rem;">
                    Title (<span style="color: var(--primary);">Page ${item.data.page}</span>):
                </label>
                <input type="text" id="${uniqueId}" value="${(item.data.title || '').replace(/"/g, '&quot;')}"
                    style="padding: 0.6rem; border-radius: 6px; border: 1px solid rgba(255,255,255,0.2); background: rgba(0,0,0,0.2); color: white; width: 100%; box-sizing: border-box; font-family: inherit;">
            `;
            card.appendChild(header);

            // Content
            const contentDiv = document.createElement('div');
            if (item.type === 'table') {
                contentDiv.appendChild(buildTableDOM(item.data));
            } else {
                contentDiv.appendChild(buildChartDOM(item.data));
            }
            card.appendChild(contentDiv);

            resultsContainer.appendChild(card);
        } else {
            // Unchecked, remove it
            const itemObj = selectedItemsList.find(obj => obj.originalIndex === idx);
            if (itemObj) {
                const card = document.getElementById(`result-card-${itemObj.inputId}`);
                if (card) card.remove();
                selectedItemsList = selectedItemsList.filter(obj => obj !== itemObj);
                checkEmptyState();
                updateHeaderText();
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DATA RENDERING
    // ══════════════════════════════════════════════════════════════════════════

    function buildTableDOM(tableData) {
        const wrapper = document.createElement('div');
        wrapper.className = 'table-scroll-wrapper';
        wrapper.style.display = 'block'; // Ensure visibility

        if (!tableData || !tableData.rows || tableData.rows.length === 0) {
            wrapper.innerHTML = '<div class="empty-state" style="position: relative; padding: 2rem;"><p>No data available.</p></div>';
            return wrapper;
        }
        const table = document.createElement('table');
        const tbody = document.createElement('tbody');
        let maxCols = 0;
        tableData.rows.forEach(row => { if (row.length > maxCols) maxCols = row.length; });

        tableData.rows.forEach((rowData, rowIndex) => {
            const tr = document.createElement('tr');
            for (let i = 0; i < maxCols; i++) {
                const tag = rowIndex === 0 ? 'th' : 'td';
                const cell = document.createElement(tag);
                cell.textContent = rowData[i] !== undefined ? rowData[i] : '';
                tr.appendChild(cell);
            }
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        wrapper.appendChild(table);
        return wrapper;
    }

    function buildChartDOM(chartData) {
        const wrapper = document.createElement('div');
        wrapper.className = 'chart-display';
        wrapper.style.display = 'block'; // Ensure visibility

        // Show image
        const imgWrapper = document.createElement('div');
        imgWrapper.className = 'chart-image-wrapper';
        const img = document.createElement('img');
        img.src = `data:image/png;base64,${chartData.image_base64}`;
        img.alt = 'Chart';
        imgWrapper.appendChild(img);
        wrapper.appendChild(imgWrapper);

        // Show AI description
        if (chartData.description) {
            const desc = document.createElement('div');
            desc.className = 'ai-description';
            desc.innerHTML = `
                <h4><i class="fa-solid fa-lightbulb"></i> AI Interpretation</h4>
                <p>${chartData.description}</p>`;
            wrapper.appendChild(desc);
        }

        // Show extracted data table
        if (chartData.csv_rows && chartData.csv_rows.length > 0) {
            const dataSec = document.createElement('div');
            dataSec.className = 'chart-data-section';
            dataSec.style.display = 'block'; // Ensure visibility
            dataSec.innerHTML = '<h4><i class="fa-solid fa-table-cells"></i> AI-Extracted Data</h4>';

            const tableCont = document.createElement('div');
            tableCont.className = 'ai-table-wrapper';
            tableCont.innerHTML = buildHtmlTable(chartData.csv_rows);
            dataSec.appendChild(tableCont);

            wrapper.appendChild(dataSec);
        }

        return wrapper;
    }

    function buildHtmlTable(csvRows) {
        let html = '<table class="ai-csv-table">';
        csvRows.forEach((row, idx) => {
            html += '<tr>';
            row.forEach(cell => {
                const tag = idx === 0 ? 'th' : 'td';
                html += `<${tag}>${cell}</${tag}>`;
            });
            html += '</tr>';
        });
        return html + '</table>';
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DOWNLOAD CSV
    // ══════════════════════════════════════════════════════════════════════════

    downloadCsvBtn.addEventListener('click', async () => {
        if (selectedItemsList.length === 0) return;

        const BOM = '\uFEFF';

        for (const sel of selectedItemsList) {
            const item = sel.item;
            const customTitleEl = document.getElementById(sel.inputId);

            let csvContent = '';
            const page = item.data.page;
            const originalTitle = item.data.title;
            const finalTitle = customTitleEl ? (customTitleEl.value.trim() || originalTitle) : originalTitle;
            const safeName = finalTitle.replace(/[^a-zA-Z0-9 \-_]/g, '_').trim().slice(0, 50);

            if (item.type === 'table') {
                csvContent = item.data.rows
                    .map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(';'))
                    .join('\n');
                csvContent = `"${finalTitle}"\n"Page: ${page}"\n\n` + csvContent;
            } else {
                // Chart: use the AI-generated CSV
                csvContent = item.data.csv_data || '';
                csvContent = csvContent.replace(/^"TITRE:.*?"\s*/, '');
                csvContent = `"${finalTitle}"\n"Page: ${page}"\n\n` + csvContent;
            }

            const fileName = item.type === 'table'
                ? `table_p${page}_${safeName}.csv`
                : `chart_p${page}_${safeName}.csv`;

            const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.style.display = 'none';
            a.download = fileName;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            // Small delay between downloads so the browser doesn't block them
            await new Promise(resolve => setTimeout(resolve, 200));
        }
    });
});
