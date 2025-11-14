// js/ui.js

const ui = {
    // --- State & References ---
    _chartInstances: {}, // Store chart instances to manage them

    // --- Core UI Management ---
    /**
     * Switches the main view of the application.
     * @param {string} viewId - The ID of the view to show.
     */
    switchView(viewId) {
        const views = document.querySelectorAll('[id$="-view"]');
        views.forEach(view => view.classList.add('hidden'));
        const targetView = document.getElementById(viewId);
        if (targetView) {
            targetView.classList.remove('hidden');
        } else {
            console.error(`View with id '${viewId}' not found.`);
        }
    },

    /**
     * Toggles the visibility of the side menu.
     */
    toggleMenu() {
        const sideMenu = document.getElementById('side-menu');
        const menuOverlay = document.getElementById('menu-overlay');
        sideMenu.classList.toggle('open');
        menuOverlay.classList.toggle('open');
    },

    /**
     * Updates the text of a loader element.
     * @param {string} text - The text to display on the loader.
     */
    updateLoaderText(text) {
        const loaderText = document.getElementById('loader-text');
        if (loaderText) loaderText.textContent = text;
    },

    /**
     * Shows or hides the main application loader.
     * @param {boolean} show - True to show, false to hide.
     */
    toggleAppLoader(show) {
        const loader = document.getElementById('loader');
        if (loader) {
            loader.classList.toggle('hidden', !show);
        }
    },

    // --- Generic Component Population ---

    /**
     * Populates a <select> dropdown element with options.
     * @param {HTMLElement} selectElement - The <select> element to populate.
     * @param {Array<object>} data - The array of data objects.
     * @param {string} valueField - The property name for the option value.
     * @param {string} textField - The property name for the option text.
     * @param {string} [defaultOptionText="Todos"] - Text for the default "all" option.
     */
    populateDropdown(selectElement, data, valueField, textField, defaultOptionText = "Todos") {
        if (!selectElement) return;
        selectElement.innerHTML = `<option value="">${defaultOptionText}</option>`;
        data.forEach(item => {
            const option = document.createElement('option');
            option.value = item[valueField];
            option.textContent = item[textField];
            selectElement.appendChild(option);
        });
    },

    // --- Chart Management ---

    /**
     * Creates or updates a Chart.js chart.
     * @param {string} chartId - The ID for the chart instance (e.g., 'salesByPersonChart').
     * @param {string} canvasContainerId - The ID of the container element for the canvas.
     * @param {object} chartConfig - The Chart.js configuration object.
     */
    createOrUpdateChart(chartId, canvasContainerId, chartConfig) {
        const container = document.getElementById(canvasContainerId);
        if (!container) {
            console.error(`Chart container '${canvasContainerId}' not found.`);
            return;
        }

        // Destroy existing chart instance if it exists
        if (ui._chartInstances[chartId]) {
            ui._chartInstances[chartId].destroy();
        }

        // Ensure canvas exists and is clean
        container.innerHTML = `<canvas id="${chartId}-canvas"></canvas>`;
        const ctx = document.getElementById(`${chartId}-canvas`).getContext('2d');

        ui._chartInstances[chartId] = new Chart(ctx, chartConfig);
    },

    // --- Menu Population ---
    /**
     * Populates the side navigation menu.
     * @param {HTMLElement} navContainer - The container element for the navigation buttons.
     */
    populateSideMenu(navContainer) {
        const menuStructure = [
            {
                section: 'Dashboards',
                items: [
                    { label: 'Dashboard Principal', viewId: 'dashboard-view', default: true },
                    { label: 'Pedidos Detalhados', viewId: 'orders-view' },
                    { label: 'Análise de Cidades', viewId: 'city-view' },
                    { label: 'Acompanhamento Semanal', viewId: 'weekly-view' },
                ]
            },
            {
                section: 'Análises',
                items: [
                    { label: 'Comparativo entre Meses', viewId: 'comparison-view' },
                    { label: 'Análise de Estoque', viewId: 'stock-view' },
                    { label: 'Análise de Inovações', viewId: 'innovations-view' },
                    { label: 'Cobertura (Estoque x PDVs)', viewId: 'coverage-view' },
                ]
            },
            {
                section: 'Ferramentas',
                items: [
                    { label: 'Uploader de Dados', viewId: 'admin-uploader-modal' },
                ]
            }
        ];

        navContainer.innerHTML = '';
        menuStructure.forEach(section => {
            const sectionHeader = document.createElement('h3');
            sectionHeader.textContent = section.section;
            navContainer.appendChild(sectionHeader);

            section.items.forEach(item => {
                const button = document.createElement('button');
                button.className = 'nav-btn';
                button.textContent = item.label;
                button.dataset.viewId = item.viewId;
                if (item.default) {
                    button.classList.add('active');
                }
                navContainer.appendChild(button);
            });
        });
    },
};
