




// document.addEventListener('DOMContentLoaded', () => {
//     const dropZone = document.getElementById('drop-zone');
//     const fileInput = document.getElementById('file-input');
//     const uploadSection = document.getElementById('upload-section');
//     const loadingState = document.getElementById('loading-state');
//     const errorMsgDiv = document.getElementById('error-message');

//     const resultsSection = document.getElementById('results-section');
//     const customMultiselect = document.getElementById('custom-multiselect');
//     const multiselectHeader = document.getElementById('multiselect-header');
//     const multiselectHeaderText = document.getElementById('multiselect-header-text');
//     const multiselectOptions = document.getElementById('multiselect-options');
//     const downloadCsvBtn = document.getElementById('download-csv-btn');
//     const uploadNewBtn = document.getElementById('upload-new-btn');

//     const pageStartInput = document.getElementById('page-start');
//     const pageEndInput = document.getElementById('page-end');

//     const filterPageStartInput = document.getElementById('filter-page-start');
//     const filterPageEndInput = document.getElementById('filter-page-end');
//     const applyFilterBtn = document.getElementById('apply-filter-btn');

//     const resultsContainer = document.getElementById('results-container');
//     const emptySelectionState = document.getElementById('empty-selection-state');

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
//         customTitleInput.value = '';
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
//         if (pageStartInput.value) formData.append('page_start', pageStartInput.value);
//         if (pageEndInput.value) formData.append('page_end', pageEndInput.value);

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

//     let selectedItemsList = []; // Array of { originalIndex, item, inputId }

//     if (customMultiselect) {
//         multiselectHeader.addEventListener('click', () => {
//             multiselectOptions.classList.toggle('hidden');
//         });

//         document.addEventListener('click', (e) => {
//             if (!customMultiselect.contains(e.target)) {
//                 multiselectOptions.classList.add('hidden');
//             }
//         });
//     }

//     function checkEmptyState() {
//         if (selectedItemsList.length === 0) {
//             emptySelectionState.classList.remove('hidden');
//             downloadCsvBtn.disabled = true;
//         } else {
//             emptySelectionState.classList.add('hidden');
//             downloadCsvBtn.disabled = false;
//         }
//     }

//     function updateHeaderText() {
//         const count = selectedItemsList.length;
//         if (count === 0) {
//             multiselectHeaderText.textContent = 'Choose tables/charts...';
//         } else if (count === 1) {
//             multiselectHeaderText.textContent = '1 item selected';
//         } else {
//             multiselectHeaderText.textContent = `${count} items selected`;
//         }
//     }

//     function showResults(filterStart = null, filterEnd = null) {
//         uploadSection.classList.add('hidden');
//         resultsSection.classList.remove('hidden');

//         // Clear results container (except the empty state)
//         Array.from(resultsContainer.children).forEach(child => {
//             if (child.id !== 'empty-selection-state') {
//                 child.remove();
//             }
//         });
//         emptySelectionState.classList.remove('hidden');
//         selectedItemsList = [];
//         updateHeaderText();

//         // Populate custom dropdown
//         multiselectOptions.innerHTML = '';

//         let filteredItems = allItems.map((item, idx) => ({ item, idx }));

//         if (filterStart !== null && filterEnd !== null) {
//             filteredItems = filteredItems.filter(obj => obj.item.data.page >= filterStart && obj.item.data.page <= filterEnd);
//         }

//         // ── Select All / Deselect All toggle ──────────────────────────────────
//         const selectAllContainer = document.createElement('label');
//         selectAllContainer.style.display = 'block';
//         selectAllContainer.style.padding = '0.4rem 0.8rem';
//         selectAllContainer.style.cursor = 'pointer';
//         selectAllContainer.style.borderBottom = '2px solid rgba(255,255,255,0.15)';
//         selectAllContainer.style.backgroundColor = 'rgba(255,255,255,0.05)';
//         selectAllContainer.style.fontWeight = '600';
//         selectAllContainer.className = 'multiselect-option-label';
//         selectAllContainer.innerHTML = `
//             <input type="checkbox" id="select-all-checkbox" style="margin-right: 0.5rem;" />
//             Select All
//         `;
//         selectAllContainer.addEventListener('mouseenter', () => selectAllContainer.style.backgroundColor = 'rgba(255,255,255,0.12)');
//         selectAllContainer.addEventListener('mouseleave', () => selectAllContainer.style.backgroundColor = 'rgba(255,255,255,0.05)');
//         multiselectOptions.appendChild(selectAllContainer);

//         const selectAllCb = selectAllContainer.querySelector('#select-all-checkbox');
//         selectAllCb.addEventListener('change', (e) => {
//             const allCheckboxes = multiselectOptions.querySelectorAll('.multiselect-checkbox');
//             allCheckboxes.forEach(cb => {
//                 if (cb.checked !== e.target.checked) {
//                     cb.checked = e.target.checked;
//                     cb.dispatchEvent(new Event('change'));
//                 }
//             });
//         });

//         // Helper to add options
//         const addOption = (obj) => {
//             const optContainer = document.createElement('label');
//             optContainer.style.display = 'block';
//             optContainer.style.padding = '0.4rem 0.8rem';
//             optContainer.style.cursor = 'pointer';
//             optContainer.style.borderBottom = '1px solid rgba(255,255,255,0.05)';
//             optContainer.className = 'multiselect-option-label';

//             optContainer.innerHTML = `
//                 <input type="checkbox" value="${obj.idx}" class="multiselect-checkbox" style="margin-right: 0.5rem;" />
//                 Page ${obj.item.data.page} - ${obj.item.data.title}
//             `;

//             optContainer.addEventListener('mouseenter', () => optContainer.style.backgroundColor = 'rgba(255,255,255,0.1)');
//             optContainer.addEventListener('mouseleave', () => optContainer.style.backgroundColor = 'transparent');

//             const cb = optContainer.querySelector('input');
//             cb.addEventListener('change', (e) => {
//                 handleSelectionToggle(obj.idx, e.target.checked, cb);
//                 // Sync the Select All checkbox state
//                 const allCbs = multiselectOptions.querySelectorAll('.multiselect-checkbox');
//                 const allChecked = Array.from(allCbs).every(c => c.checked);
//                 const noneChecked = Array.from(allCbs).every(c => !c.checked);
//                 selectAllCb.checked = allChecked;
//                 selectAllCb.indeterminate = !allChecked && !noneChecked;
//             });

//             multiselectOptions.appendChild(optContainer);
//         };

//         // Group: Tables
//         if (filteredItems.some(obj => obj.item.type === 'table')) {
//             const grpTitle = document.createElement('div');
//             grpTitle.style.padding = '0.4rem 0.8rem';
//             grpTitle.style.fontWeight = 'bold';
//             grpTitle.style.backgroundColor = 'rgba(0,0,0,0.3)';
//             grpTitle.textContent = 'Tables';
//             multiselectOptions.appendChild(grpTitle);

//             filteredItems.forEach(obj => {
//                 if (obj.item.type === 'table') addOption(obj);
//             });
//         }

//         // Group: Charts
//         if (filteredItems.some(obj => obj.item.type === 'chart')) {
//             const grpTitle = document.createElement('div');
//             grpTitle.style.padding = '0.4rem 0.8rem';
//             grpTitle.style.fontWeight = 'bold';
//             grpTitle.style.backgroundColor = 'rgba(0,0,0,0.3)';
//             grpTitle.textContent = 'Charts';
//             multiselectOptions.appendChild(grpTitle);

//             filteredItems.forEach(obj => {
//                 if (obj.item.type === 'chart') addOption(obj);
//             });
//         }

//         downloadCsvBtn.disabled = true;
//     }

//     // ── Apply Result Filter ────────────────────────────────────────────────────────
//     if (applyFilterBtn) {
//         applyFilterBtn.addEventListener('click', () => {
//             let start = parseInt(filterPageStartInput.value);
//             let end = parseInt(filterPageEndInput.value);
//             if (isNaN(start) || isNaN(end)) {
//                 showResults(); // Reset to show all
//             } else {
//                 showResults(start, end);
//             }
//         });
//     }

//     // ── Selection ────────────────────────────────────────────────────────────

//     function handleSelectionToggle(idx, isChecked, checkboxEl) {
//         if (isChecked) {
//             // Check if already added
//             if (selectedItemsList.some(obj => obj.originalIndex === idx)) {
//                 return;
//             }

//             const item = allItems[idx];
//             const uniqueId = `custom-title-${idx}-${Date.now()}`;

//             const itemObj = {
//                 originalIndex: idx,
//                 item: item,
//                 inputId: uniqueId
//             };
//             selectedItemsList.push(itemObj);

//             checkEmptyState();
//             updateHeaderText();

//             // Build the card
//             const card = document.createElement('div');
//             card.id = `result-card-${uniqueId}`;
//             card.className = 'glass-panel result-card';
//             card.style.position = 'relative';
//             card.style.marginBottom = '2rem';
//             card.style.padding = '1.5rem';

//             // Add remove button
//             const removeBtn = document.createElement('button');
//             removeBtn.innerHTML = '<i class="fa-solid fa-xmark"></i>';
//             removeBtn.style.position = 'absolute';
//             removeBtn.style.top = '1.5rem';
//             removeBtn.style.right = '1.5rem';
//             removeBtn.style.background = 'none';
//             removeBtn.style.border = 'none';
//             removeBtn.style.color = 'var(--text-muted)';
//             removeBtn.style.cursor = 'pointer';
//             removeBtn.style.fontSize = '1.2rem';
//             removeBtn.onmouseenter = () => removeBtn.style.color = '#ff4444';
//             removeBtn.onmouseleave = () => removeBtn.style.color = 'var(--text-muted)';
//             removeBtn.onclick = () => {
//                 card.remove();
//                 selectedItemsList = selectedItemsList.filter(obj => obj !== itemObj);
//                 checkboxEl.checked = false; // Sync checkbox state
//                 checkEmptyState();
//                 updateHeaderText();
//             };
//             card.appendChild(removeBtn);

//             // Header with title input
//             const header = document.createElement('div');
//             header.style.marginBottom = '1rem';
//             header.style.paddingRight = '2rem'; // make room for remove custom X button
//             header.innerHTML = `
//                 <label for="${uniqueId}" style="font-size: 0.85rem; font-weight: 600; display: block; margin-bottom: 0.4rem;">
//                     Title (<span style="color: var(--primary);">Page ${item.data.page}</span>):
//                 </label>
//                 <input type="text" id="${uniqueId}" value="${(item.data.title || '').replace(/"/g, '&quot;')}"
//                     style="padding: 0.6rem; border-radius: 6px; border: 1px solid rgba(255,255,255,0.2); background: rgba(0,0,0,0.2); color: white; width: 100%; box-sizing: border-box; font-family: inherit;">
//             `;
//             card.appendChild(header);

//             // Content
//             const contentDiv = document.createElement('div');
//             if (item.type === 'table') {
//                 contentDiv.appendChild(buildTableDOM(item.data, uniqueId));
//             } else {
//                 contentDiv.appendChild(buildChartDOM(item.data));
//             }
//             card.appendChild(contentDiv);

//             resultsContainer.appendChild(card);
//         } else {
//             // Unchecked, remove it
//             const itemObj = selectedItemsList.find(obj => obj.originalIndex === idx);
//             if (itemObj) {
//                 const card = document.getElementById(`result-card-${itemObj.inputId}`);
//                 if (card) card.remove();
//                 selectedItemsList = selectedItemsList.filter(obj => obj !== itemObj);
//                 checkEmptyState();
//                 updateHeaderText();
//             }
//         }
//     }

//     // ══════════════════════════════════════════════════════════════════════════
//     // DATA RENDERING
//     // ══════════════════════════════════════════════════════════════════════════

//     function buildTableDOM(tableData, cardId) {
//         const wrapper = document.createElement('div');
//         wrapper.className = 'table-scroll-wrapper';
//         wrapper.style.display = 'block';

//         if (!tableData || !tableData.rows || tableData.rows.length === 0) {
//             wrapper.innerHTML = '<div class="empty-state" style="position: relative; padding: 2rem;"><p>No data available.</p></div>';
//             return wrapper;
//         }
//         const table = document.createElement('table');
//         const tbody = document.createElement('tbody');
//         let maxCols = 0;
//         tableData.rows.forEach(row => { if (row.length > maxCols) maxCols = row.length; });

//         tableData.rows.forEach((rowData, rowIndex) => {
//             const tr = document.createElement('tr');
//             for (let i = 0; i < maxCols; i++) {
//                 if (rowIndex === 0) {
//                     // Editable column header
//                     const th = document.createElement('th');
//                     th.style.padding = '4px';
//                     const input = document.createElement('input');
//                     input.type = 'text';
//                     input.value = rowData[i] !== undefined ? rowData[i] : '';
//                     input.className = 'col-header-input';
//                     input.dataset.colIndex = i;
//                     input.title = 'Click to rename this column';
//                     input.style.cssText = [
//                         'background: rgba(255,255,255,0.1)',
//                         'border: 1px solid rgba(255,255,255,0.35)',
//                         'border-radius: 4px',
//                         'padding: 3px 6px',
//                         'color: white',
//                         'font-weight: 700',
//                         'width: 100%',
//                         'min-width: 60px',
//                         'font-family: inherit',
//                         'font-size: inherit',
//                         'box-sizing: border-box',
//                         'transition: border-color 0.2s',
//                     ].join(';');
//                     input.addEventListener('focus', () => input.style.borderColor = 'var(--primary, #7c6fff)');
//                     input.addEventListener('blur', () => input.style.borderColor = 'rgba(255,255,255,0.35)');
//                     th.appendChild(input);
//                     tr.appendChild(th);
//                 } else {
//                     const td = document.createElement('td');
//                     td.textContent = rowData[i] !== undefined ? rowData[i] : '';
//                     tr.appendChild(td);
//                 }
//             }
//             tbody.appendChild(tr);
//         });
//         table.appendChild(tbody);
//         wrapper.appendChild(table);
//         return wrapper;
//     }

//     function buildChartDOM(chartData) {
//         const wrapper = document.createElement('div');
//         wrapper.className = 'chart-display';
//         wrapper.style.display = 'block'; // Ensure visibility

//         // Show image
//         const imgWrapper = document.createElement('div');
//         imgWrapper.className = 'chart-image-wrapper';
//         const img = document.createElement('img');
//         img.src = `data:image/png;base64,${chartData.image_base64}`;
//         img.alt = 'Chart';
//         imgWrapper.appendChild(img);
//         wrapper.appendChild(imgWrapper);

//         // Show AI description
//         if (chartData.description) {
//             const desc = document.createElement('div');
//             desc.className = 'ai-description';
//             desc.innerHTML = `
//                 <h4><i class="fa-solid fa-lightbulb"></i> AI Interpretation</h4>
//                 <p>${chartData.description}</p>`;
//             wrapper.appendChild(desc);
//         }

//         // Show extracted data table
//         if (chartData.csv_rows && chartData.csv_rows.length > 0) {
//             const dataSec = document.createElement('div');
//             dataSec.className = 'chart-data-section';
//             dataSec.style.display = 'block'; // Ensure visibility
//             dataSec.innerHTML = '<h4><i class="fa-solid fa-table-cells"></i> AI-Extracted Data</h4>';

//             const tableCont = document.createElement('div');
//             tableCont.className = 'ai-table-wrapper';
//             tableCont.innerHTML = buildHtmlTable(chartData.csv_rows);
//             dataSec.appendChild(tableCont);

//             wrapper.appendChild(dataSec);
//         }

//         return wrapper;
//     }

//     function buildHtmlTable(csvRows) {
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

//     downloadCsvBtn.addEventListener('click', async () => {
//         if (selectedItemsList.length === 0) return;

//         const BOM = '\uFEFF';

//         for (const sel of selectedItemsList) {
//             const item = sel.item;
//             const customTitleEl = document.getElementById(sel.inputId);

//             let csvContent = '';
//             const page = item.data.page;
//             const originalTitle = item.data.title;
//             const finalTitle = customTitleEl ? (customTitleEl.value.trim() || originalTitle) : originalTitle;
//             const safeName = finalTitle.replace(/[^a-zA-Z0-9 \-_]/g, '_').trim().slice(0, 50);

//             if (item.type === 'table') {
//                 // Read edited column headers from the DOM inputs
//                 const card = document.getElementById(`result-card-${sel.inputId}`);
//                 const headerInputs = card ? card.querySelectorAll('.col-header-input') : [];
//                 const headerValues = Array.from(headerInputs).map(inp => inp.value.trim() || inp.dataset.colIndex);

//                 const headerRow = headerValues
//                     .map(h => `"${String(h).replace(/"/g, '""')}"`)
//                     .join(';');

//                 // rows[0] is the original header — skip it; rows[1..] are data
//                 const dataRows = item.data.rows.slice(1)
//                     .map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(';'))
//                     .join('\n');

//                 csvContent = `"${finalTitle}"\n` + headerRow + '\n' + dataRows;
//             } else {
//                 // Chart: use the AI-generated CSV with title as header
//                 csvContent = item.data.csv_data || '';
//                 csvContent = csvContent.replace(/^"TITRE:.*?"\s*/, '');
//                 csvContent = `"${finalTitle}"\n` + csvContent;
//             }

//             const fileName = item.type === 'table'
//                 ? `table_p${page}_${safeName}.csv`
//                 : `chart_p${page}_${safeName}.csv`;

//             const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' });
//             const url = URL.createObjectURL(blob);
//             const a = document.createElement('a');
//             a.href = url;
//             a.style.display = 'none';
//             a.download = fileName;
//             document.body.appendChild(a);
//             a.click();
//             document.body.removeChild(a);
//             URL.revokeObjectURL(url);

//             // Small delay between downloads so the browser doesn't block them
//             await new Promise(resolve => setTimeout(resolve, 200));
//         }
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

    const pageInput = document.getElementById('page-input');
    const filterPageInput = document.getElementById('filter-page-input');
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
        if (pageInput.value) formData.append('pages', pageInput.value);

        try {
            const response = await fetch('/upload', { method: 'POST', body: formData });
            const data = await response.json();

            if (!response.ok || !data.success) {
                showError(data.error || 'An error occurred.');
                return;
            }

            const tables = data.tables || [];

            if (tables.length === 0) {
                showError('No tables found in this PDF.');
                return;
            }

            // Build items list
            allItems = [];
            tables.forEach(t => allItems.push({ type: 'table', data: t }));

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
            multiselectHeaderText.textContent = 'Choose tables...';
        } else if (count === 1) {
            multiselectHeaderText.textContent = '1 item selected';
        } else {
            multiselectHeaderText.textContent = `${count} items selected`;
        }
    }

    function showResults(filterPagesSet = null) {
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

        if (filterPagesSet !== null) {
            filteredItems = filteredItems.filter(obj => filterPagesSet.has(obj.item.data.page));
        }

        // ── Select All / Deselect All toggle ──────────────────────────────────
        const selectAllContainer = document.createElement('label');
        selectAllContainer.style.display = 'block';
        selectAllContainer.style.padding = '0.4rem 0.8rem';
        selectAllContainer.style.cursor = 'pointer';
        selectAllContainer.style.borderBottom = '2px solid rgba(255,255,255,0.15)';
        selectAllContainer.style.backgroundColor = 'rgba(255,255,255,0.05)';
        selectAllContainer.style.fontWeight = '600';
        selectAllContainer.className = 'multiselect-option-label';
        selectAllContainer.innerHTML = `
            <input type="checkbox" id="select-all-checkbox" style="margin-right: 0.5rem;" />
            Select All
        `;
        selectAllContainer.addEventListener('mouseenter', () => selectAllContainer.style.backgroundColor = 'rgba(255,255,255,0.12)');
        selectAllContainer.addEventListener('mouseleave', () => selectAllContainer.style.backgroundColor = 'rgba(255,255,255,0.05)');
        multiselectOptions.appendChild(selectAllContainer);

        const selectAllCb = selectAllContainer.querySelector('#select-all-checkbox');
        selectAllCb.addEventListener('change', (e) => {
            const isChecked = e.target.checked;
            const allCheckboxes = multiselectOptions.querySelectorAll('.multiselect-checkbox');

            allCheckboxes.forEach(cb => {
                cb.checked = isChecked;
                const itemIdx = parseInt(cb.value);
                const item = allItems[itemIdx];
                const existingIndex = selectedItemsList.findIndex(obj => obj.originalIndex === itemIdx);

                if (isChecked && existingIndex === -1) {
                    selectedItemsList.push({
                        item: item,
                        originalIndex: itemIdx,
                        inputId: `custom-title-${itemIdx}`
                    });
                } else if (!isChecked && existingIndex !== -1) {
                    selectedItemsList.splice(existingIndex, 1);
                }
            });

            checkEmptyState();
            updateHeaderText();

            // Re-render UI: clear all cards and re-add them based on selectedItemsList
            Array.from(resultsContainer.children).forEach(child => {
                if (child.id !== 'empty-selection-state') {
                    child.remove();
                }
            });

            selectedItemsList.forEach(sel => {
                const uniqueId = sel.inputId;
                const item = sel.item;

                const card = document.createElement('div');
                card.id = `result-card-${uniqueId}`;
                card.className = 'glass-panel result-card';
                card.style.position = 'relative';
                card.style.marginBottom = '2rem';
                card.style.padding = '1.5rem';

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
                    selectedItemsList = selectedItemsList.filter(obj => obj !== sel);
                    const cbToUncheck = Array.from(multiselectOptions.querySelectorAll('.multiselect-checkbox')).find(c => parseInt(c.value) === sel.originalIndex);
                    if (cbToUncheck) cbToUncheck.checked = false;

                    const allCbs = multiselectOptions.querySelectorAll('.multiselect-checkbox');
                    const allChecked = Array.from(allCbs).every(c => c.checked);
                    const noneChecked = Array.from(allCbs).every(c => !c.checked);
                    selectAllCb.checked = allChecked;
                    selectAllCb.indeterminate = !allChecked && !noneChecked;

                    checkEmptyState();
                    updateHeaderText();
                };
                card.appendChild(removeBtn);

                const header = document.createElement('div');
                header.style.marginBottom = '1rem';
                header.style.paddingRight = '2rem';
                header.innerHTML = `
                    <label for="${uniqueId}" style="font-size: 0.85rem; font-weight: 600; display: block; margin-bottom: 0.4rem;">
                        Title (<span style="color: var(--primary);">Page ${item.data.page}</span>):
                    </label>
                    <input type="text" id="${uniqueId}" value="${(item.data.title || '').replace(/"/g, '&quot;')}"
                        style="padding: 0.6rem; border-radius: 6px; border: 1px solid rgba(255,255,255,0.2); background: rgba(0,0,0,0.2); color: white; width: 100%; box-sizing: border-box; font-family: inherit;">
                `;
                card.appendChild(header);

                const contentDiv = document.createElement('div');
                contentDiv.appendChild(buildTableDOM(item.data, uniqueId));
                card.appendChild(contentDiv);

                resultsContainer.appendChild(card);
            });
        });

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
                // Sync the Select All checkbox state
                const allCbs = multiselectOptions.querySelectorAll('.multiselect-checkbox');
                const allChecked = Array.from(allCbs).every(c => c.checked);
                const noneChecked = Array.from(allCbs).every(c => !c.checked);
                selectAllCb.checked = allChecked;
                selectAllCb.indeterminate = !allChecked && !noneChecked;
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



        downloadCsvBtn.disabled = true;
    }

    // ── Apply Result Filter ────────────────────────────────────────────────────────

    function parsePageInterval(inputStr) {
        if (!inputStr || !inputStr.trim()) return null;
        const pageSet = new Set();
        const parts = inputStr.split(',');
        for (const p of parts) {
            const rangeParts = p.trim().split('-');
            if (rangeParts.length === 1) {
                const num = parseInt(rangeParts[0]);
                if (!isNaN(num)) pageSet.add(num);
            } else if (rangeParts.length === 2) {
                const start = parseInt(rangeParts[0]);
                const end = parseInt(rangeParts[1]);
                if (!isNaN(start) && !isNaN(end) && start <= end) {
                    for (let i = start; i <= end; i++) pageSet.add(i);
                }
            }
        }
        return pageSet.size > 0 ? pageSet : null;
    }

    if (applyFilterBtn) {
        applyFilterBtn.addEventListener('click', () => {
            const parsedSet = parsePageInterval(filterPageInput.value);
            showResults(parsedSet);
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

                // Sync the Select All checkbox state
                const allCbs = multiselectOptions.querySelectorAll('.multiselect-checkbox');
                const allChecked = Array.from(allCbs).every(c => c.checked);
                const noneChecked = Array.from(allCbs).every(c => !c.checked);
                selectAllCb.checked = allChecked;
                selectAllCb.indeterminate = !allChecked && !noneChecked;

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
            contentDiv.appendChild(buildTableDOM(item.data, uniqueId));
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

    function buildTableDOM(tableData, cardId) {
        const wrapper = document.createElement('div');
        wrapper.className = 'table-scroll-wrapper';
        wrapper.style.display = 'block';

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
                if (rowIndex === 0) {
                    // Editable column header
                    const th = document.createElement('th');
                    th.style.padding = '4px';
                    const input = document.createElement('input');
                    input.type = 'text';
                    input.value = rowData[i] !== undefined ? rowData[i] : '';
                    input.className = 'col-header-input';
                    input.dataset.colIndex = i;
                    input.title = 'Click to rename this column';
                    input.style.cssText = [
                        'background: rgba(255,255,255,0.1)',
                        'border: 1px solid rgba(255,255,255,0.35)',
                        'border-radius: 4px',
                        'padding: 3px 6px',
                        'color: white',
                        'font-weight: 700',
                        'width: 100%',
                        'min-width: 60px',
                        'font-family: inherit',
                        'font-size: inherit',
                        'box-sizing: border-box',
                        'transition: border-color 0.2s',
                    ].join(';');
                    input.addEventListener('focus', () => input.style.borderColor = 'var(--primary, #7c6fff)');
                    input.addEventListener('blur', () => input.style.borderColor = 'rgba(255,255,255,0.35)');
                    th.appendChild(input);
                    tr.appendChild(th);
                } else {
                    const td = document.createElement('td');
                    td.textContent = rowData[i] !== undefined ? rowData[i] : '';
                    tr.appendChild(td);
                }
            }
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        wrapper.appendChild(table);
        return wrapper;
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

            // Read edited column headers from the DOM inputs
            const card = document.getElementById(`result-card-${sel.inputId}`);
            const headerInputs = card ? card.querySelectorAll('.col-header-input') : [];
            const headerValues = Array.from(headerInputs).map(inp => inp.value.trim() || inp.dataset.colIndex);

            const headerRow = headerValues
                .map(h => `"${String(h).replace(/"/g, '""')}"`)
                .join(';');

            // rows[0] is the original header — skip it; rows[1..] are data
            const dataRows = item.data.rows.slice(1)
                .map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(';'))
                .join('\n');

            csvContent = `"${finalTitle}"\n` + headerRow + '\n' + dataRows;

            const fileName = `table_p${page}_${safeName}.csv`;

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
