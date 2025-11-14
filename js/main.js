// js/main.js

// --- Global State Variables ---
let supabase;
let chartInstances = {}; // Centralized chart management

// Cached data for filters
let g_supervisors = [];
let g_vendedores = [];
let g_fornecedores = [];
let g_tiposVenda = [];
let g_redes = [];
let g_allClientsData = [];
let g_productDetails = [];
let g_innovationsCategories = [];
let g_lastSaleDate = null;

// --- DOM Element Cache ---
const elements = {};

/**
 * Caches frequently accessed DOM elements to improve performance.
 */
function cacheDOMElements() {
    const ids = [
        // Loaders & Screens
        'loader', 'dashboard-view', 'orders-view', 'city-view', 'weekly-view',
        'comparison-view', 'stock-view', 'innovations-view', 'coverage-view',
        // Main Dashboard KPIs
        'total-peso', 'total-vendas', 'kpi-sku-pdv', 'kpi-positivacao', 'kpi-positivacao-percent',
        // Main Dashboard Filters
        'supervisor-filter', 'vendedor-filter-dropdown', 'vendedor-filter-text',
        // Main Dashboard Containers
        'fornecedor-toggle-container', 'generation-date',
        'salesByPersonChartContainer', 'faturamentoPorFornecedorChartContainer', 'salesByProductBarChartContainer',
        'trendChartContainer',
        // Side Menu
        'side-menu', 'menu-overlay', 'menu-toggle-btn', 'main-nav'
    ];
    ids.forEach(id => {
        elements[id] = document.getElementById(id);
    });
}

/**
 * The main entry point for the dashboard application.
 * Called from auth.js once the user is authenticated and approved.
 * @param {SupabaseClient} supabaseClient - The initialized Supabase client.
 */
async function initializeNewDashboard(supabaseClient) {
    supabase = supabaseClient;
    cacheDOMElements();
    setupTheme(); // Initialize theme from theme.js

    ui.toggleAppLoader(true);
    ui.updateLoaderText('Carregando dados iniciais...');

    try {
        // Função auxiliar para executar uma consulta e retornar dados ou um padrão em caso de erro
        const fetchData = async (promise, defaultValue = []) => {
            try {
                const { data, error } = await promise;
                if (error) {
                    // Log o erro mas não impede o progresso
                    console.warn(`Erro não fatal na busca de dados: ${error.message}`);
                    return defaultValue;
                }
                return data;
            } catch (error) {
                console.warn(`Exceção não fatal na busca de dados: ${error.message}`);
                return defaultValue;
            }
        };

        const fetchRpc = async (promise, defaultValue = []) => {
             try {
                const result = await promise;
                // RPCs podem não seguir o padrão {data, error}, então verificamos o resultado diretamente
                if (!result) { // ou uma verificação mais específica se a API retornar um objeto de erro
                    console.warn(`Erro não fatal na chamada RPC.`);
                    return defaultValue;
                }
                return result;
            } catch (error) {
                console.warn(`Exceção não fatal na chamada RPC: ${error.message}`);
                return defaultValue;
            }
        };

        // Usa a função auxiliar para cada busca de dados
        const [
            clientsData,
            productDetailsData,
            innovationsData,
            supervisorsData,
            fornecedoresData,
            tiposVendaData,
            redesData,
            metadata
        ] = await Promise.all([
            fetchData(supabase.from('data_clients').select('*')),
            fetchData(supabase.from('data_product_details').select('code,descricao,codfor,fornecedor,dtcadastro')),
            fetchData(supabase.from('data_innovations').select('code,description,category')), // Esta irá falhar graciosamente
            fetchRpc(api.getDistinctSupervisors(supabase)),
            fetchRpc(api.getDistinctFornecedores(supabase)),
            fetchRpc(api.getDistinctTiposVenda(supabase)),
            fetchRpc(api.getDistinctRedes(supabase)),
            fetchData(supabase.from('data_metadata').select('key,value').eq('key', 'last_sale_date').single(), null)
        ]);

        // Atribui os dados às variáveis globais
        g_allClientsData = clientsData;
        g_productDetails = productDetailsData;
        g_innovationsCategories = innovationsData;
        g_supervisors = supervisorsData;
        g_fornecedores = fornecedoresData;
        g_tiposVenda = tiposVendaData;
        g_redes = redesData;
        g_lastSaleDate = metadata ? metadata.value : new Date().toISOString().split('T')[0];

        if (elements['generation-date']) {
            elements['generation-date'].textContent = `Dados atualizados em: ${new Date(g_lastSaleDate).toLocaleDateString('pt-BR', { timeZone: 'UTC' })}`;
        }

        // Populate UI components
        populateAllFilterDropdowns();
        ui.populateSideMenu(elements['main-nav']);

        // Load the initial view
        await updateDashboardView();

        // Setup event listeners after everything is populated
        setupEventListeners();

        // Initialize the uploader functionality
        setupUploader();

    } catch (error) {
        console.error("Erro fatal durante a inicialização:", error);
        ui.updateLoaderText('Erro ao carregar dados. Tente recarregar a página.');
        // Do not hide loader on fatal error
        return;
    }

    // Always ensure the dashboard view is shown and loader is hidden after initialization attempt
    elements['dashboard-view'].classList.remove('hidden');
    ui.toggleAppLoader(false);
}

/**
 * Populates all filter dropdowns across all application views.
 */
function populateAllFilterDropdowns() {
    // Example for one view - this would be expanded for all views
    ui.populateDropdown(elements['supervisor-filter'], g_supervisors, 'superv', 'superv', 'Todos Supervisores');
    // ... populate other filters for the dashboard view
    // ... populate filters for the orders view
    // ... and so on for all other views
}

/**
 * Fetches data and updates all visuals on the main dashboard.
 */
async function updateDashboardView() {
    ui.toggleAppLoader(true);
    ui.updateLoaderText('Atualizando dashboard...');

    try {
        const filters = getFiltersForRPC('dashboard-view');
        const groupBy = filters.p_supervisor ? 'vendedor' : 'supervisor';

        const [kpiData, salesByPersonData, salesByCategoryData, topProductsData] = await Promise.all([
            api.getMainKpis(supabase, filters),
            api.getSalesByGroup(supabase, { ...filters, p_group_by: groupBy }),
            api.getSalesByGroup(supabase, { ...filters, p_group_by: 'categoria' }),
            api.getTopProducts(supabase, { ...filters, p_metric: 'faturamento' })
        ]);

        // --- 1. Update KPIs ---
        if (kpiData) {
            elements['total-vendas'].textContent = formatCurrency(kpiData.total_faturamento);
            elements['total-peso'].textContent = formatNumber(kpiData.total_peso / 1000, 2) + ' Ton';
            const skuPdv = kpiData.total_pdvs_positivados > 0 ? kpiData.total_skus / kpiData.total_pdvs_positivados : 0;
            elements['kpi-sku-pdv'].textContent = formatNumber(skuPdv, 1);
            const positivacao = kpiData.base_clientes_filtro > 0 ? kpiData.total_pdvs_positivados / kpiData.base_clientes_filtro : 0;
            elements['kpi-positivacao'].textContent = formatPercentage(positivacao);
            elements['kpi-positivacao-percent'].textContent = `${kpiData.total_pdvs_positivados} de ${kpiData.base_clientes_filtro} PDVs`;
        }

        // --- 2. Update Sales by Person Chart ---
        if (salesByPersonData) {
            ui.createOrUpdateChart('salesByPersonChart', 'salesByPersonChartContainer', {
                type: 'bar',
                data: {
                    labels: salesByPersonData.map(d => d.group_name),
                    datasets: [{
                        label: `Vendas por ${groupBy}`,
                        data: salesByPersonData.map(d => d.total_faturamento),
                        backgroundColor: professionalPalette,
                    }]
                },
                options: { responsive: true, maintainAspectRatio: false }
            });
        }

        // --- 3. Update Sales by Category Chart ---
        if (salesByCategoryData) {
            ui.createOrUpdateChart('faturamentoPorFornecedorChart', 'faturamentoPorFornecedorChartContainer', {
                type: 'doughnut',
                data: {
                    labels: salesByCategoryData.map(d => d.group_name),
                    datasets: [{
                        data: salesByCategoryData.map(d => d.total_faturamento),
                        backgroundColor: professionalPalette,
                    }]
                },
                options: { responsive: true, maintainAspectRatio: false }
            });
        }

        // --- 4. Update Top Products Chart ---
        if (topProductsData) {
             ui.createOrUpdateChart('salesByProductBarChart', 'salesByProductBarChartContainer', {
                type: 'bar',
                data: {
                    labels: topProductsData.map(p => p.descricao_produto),
                    datasets: [{
                        label: 'Faturamento por Produto',
                        data: topProductsData.map(p => p.valor_metrica),
                        backgroundColor: professionalPalette[4],
                    }]
                },
                options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y' }
            });
        }

    } catch (error) {
        console.error("Erro ao atualizar a dashboard:", error);
        alert("Não foi possível carregar os dados da dashboard.");
    } finally {
        ui.toggleAppLoader(false);
    }
}

// --- View-specific State for Orders ---
const ordersTableState = {
    currentPage: 1,
    itemsPerPage: 15,
    totalItems: 0,
    totalPages: 1,
};

/**
 * Renders the orders table and pagination controls.
 * @param {Array} ordersData - Data for the orders to be rendered.
 */
function renderOrdersTable(ordersData) {
    const tableBody = document.getElementById('orders-table-body');
    const pageInfo = document.getElementById('orders-page-info-text');
    const prevBtn = document.getElementById('orders-prev-page-btn');
    const nextBtn = document.getElementById('orders-next-page-btn');
    const paginationControls = document.getElementById('orders-pagination-controls');

    if (!tableBody || !paginationControls) return;

    tableBody.innerHTML = '';
    if (!ordersData || ordersData.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="9" class="text-center py-8">Nenhum pedido encontrado com os filtros atuais.</td></tr>';
        paginationControls.classList.add('hidden');
        return;
    }

    ordersData.forEach(order => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td class="px-4 py-3">${order.pedido}</td>
            <td class="px-4 py-3">${order.codcli}</td>
            <td class="px-4 py-3">${order.nome}</td>
            <td class="px-4 py-3">${order.fornecedores_list.join(', ')}</td>
            <td class="px-4 py-3">${new Date(order.dtped).toLocaleDateString('pt-BR')}</td>
            <td class="px-4 py-3">${order.dtfat ? new Date(order.dtfat).toLocaleDateString('pt-BR') : 'N/A'}</td>
            <td class="px-4 py-3 text-right">${formatNumber(order.totpesoliq, 2)} Kg</td>
            <td class="px-4 py-3 text-right">${formatCurrency(order.vltotal)}</td>
            <td class="px-4 py-3 text-center">${order.posicao}</td>
        `;
        tableBody.appendChild(row);
    });

    // Update pagination state and controls
    ordersTableState.totalPages = Math.ceil(ordersTableState.totalItems / ordersTableState.itemsPerPage);
    pageInfo.textContent = `Página ${ordersTableState.currentPage} de ${ordersTableState.totalPages}`;
    prevBtn.disabled = ordersTableState.currentPage === 1;
    nextBtn.disabled = ordersTableState.currentPage >= ordersTableState.totalPages;
    paginationControls.classList.remove('hidden');
}

/**
 * Fetches data and updates the detailed orders view.
 */
async function updateOrdersView() {
    ui.toggleAppLoader(true);
    ui.updateLoaderText('Carregando pedidos...');
    try {
        const filters = getFiltersForRPC('orders-view');

        const countResult = await api.getOrdersCount(supabase, filters);
        ordersTableState.totalItems = countResult.count || 0;

        const ordersData = await api.getPaginatedOrders(supabase, {
            ...filters,
            p_page_number: ordersTableState.currentPage,
            p_page_size: ordersTableState.itemsPerPage
        });

        renderOrdersTable(ordersData);

    } catch (error) {
        console.error("Erro ao carregar pedidos:", error);
        alert("Não foi possível carregar os pedidos.");
        document.getElementById('orders-table-body').innerHTML = '<tr><td colspan="9" class="text-center py-8 text-red-500">Erro ao carregar dados.</td></tr>';
    } finally {
        ui.toggleAppLoader(false);
    }
}

/**
 * Fetches data and updates the city analysis view.
 */
async function updateCityView() {
    ui.toggleAppLoader(true);
    ui.updateLoaderText('Analisando cidades...');
    try {
        const filters = getFiltersForRPC('city-view');
        const analysisData = await api.getCityAnalysis(supabase, filters);

        if (!analysisData) throw new Error("A API não retornou dados.");

        // Separate data for charts and tables
        const chartData = analysisData.filter(d => d.tipo_analise === 'chart');
        const clientList = analysisData.filter(d => d.tipo_analise === 'client_list');

        // Render Top 10 chart
        ui.createOrUpdateChart('salesByClientInCityChart', 'salesByClientInCityChartContainer', {
            type: 'bar',
            data: {
                labels: chartData.map(d => d.group_name),
                datasets: [{
                    label: 'Faturamento',
                    data: chartData.map(d => d.total_faturamento),
                    backgroundColor: professionalPalette,
                }]
            },
            options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y' }
        });

        // --- Render Customer Status Chart ---
        const statusCounts = clientList.reduce((acc, client) => {
            acc[client.status_cliente] = (acc[client.status_cliente] || 0) + 1;
            return acc;
        }, { ativo: 0, inativo: 0, novo: 0 });

        ui.createOrUpdateChart('customerStatusChart', 'customerStatusChartContainer', {
            type: 'doughnut',
            data: {
                labels: ['Ativos', 'Inativos', 'Novos'],
                datasets: [{
                    data: [statusCounts.ativo, statusCounts.inativo, statusCounts.novo],
                    backgroundColor: [professionalPalette[1], professionalPalette[4], professionalPalette[2]],
                }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });

        // --- Render Client Tables ---
        const renderClientTable = (tableId, clients) => {
            const tableBody = document.getElementById(tableId);
            if (!tableBody) return;
            tableBody.innerHTML = '';
            clients.forEach(client => {
                const row = document.createElement('tr');
                // Customize columns based on table
                if (tableId === 'city-active-detail-table-body') {
                    row.innerHTML = `
                        <td>${client.codigo_cliente}</td>
                        <td>${client.fantasia}</td>
                        <td class="text-right">${formatCurrency(client.total_faturamento)}</td>
                        <td>${client.cidade}</td>
                        <td>${client.bairro}</td>
                        <td>${client.rca1}</td>
                    `;
                } else { // inactive
                    row.innerHTML = `
                        <td>${client.codigo_cliente}</td>
                        <td>${client.fantasia}</td>
                        <td>${client.cidade}</td>
                        <td>${client.bairro}</td>
                        <td class="text-center">${client.ultimacompra ? new Date(client.ultimacompra).toLocaleDateString('pt-BR') : 'N/A'}</td>
                        <td>${client.rca1}</td>
                    `;
                }
                tableBody.appendChild(row);
            });
        };

        renderClientTable('city-active-detail-table-body', clientList.filter(c => c.status_cliente === 'ativo'));
        renderClientTable('city-inactive-detail-table-body', clientList.filter(c => c.status_cliente !== 'ativo'));

    } catch (error) {
        console.error("Erro ao carregar análise de cidades:", error);
        alert("Não foi possível carregar a análise de cidades.");
    } finally {
        ui.toggleAppLoader(false);
    }
}

/**
 * Fetches data and updates the weekly analysis view.
 */
async function updateWeeklyView() {
    ui.toggleAppLoader(true);
    ui.updateLoaderText('Carregando análise semanal...');
    try {
        // For this view, filters are simplified in this implementation
        const filters = getFiltersForRPC('weekly-view');
        const rankingData = await api.getWeeklySalesAndRankings(supabase, {
            p_pasta: filters.p_pasta,
            p_supervisores: filters.p_supervisor ? [filters.p_supervisor] : null
        });

        if (!rankingData) throw new Error("A API não retornou dados.");

        // Separate the data based on 'tipo_dado'
        const weeklySales = rankingData.filter(d => d.tipo_dado === 'venda_semanal');
        const positivacao = rankingData.filter(d => d.tipo_dado === 'rank_positivacao');
        const topSellers = rankingData.filter(d => d.tipo_dado === 'rank_topsellers');
        const mix = rankingData.filter(d => d.tipo_dado === 'rank_mix');

        // Chart 1: Weekly Sales by Day of Week
        // This chart is more complex and requires data transformation not shown here for brevity
        // Placeholder for weekly sales chart rendering

        // Chart 2: Positivação Ranking
        ui.createOrUpdateChart('positivacaoChart', 'positivacaoChartContainer', {
            type: 'bar',
            data: {
                labels: positivacao.map(d => d.group_name),
                datasets: [{
                    label: 'PDVs Positivados',
                    data: positivacao.map(d => d.total_valor),
                    backgroundColor: professionalPalette[2],
                }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });

        // Chart 3: Top Sellers Ranking
        ui.createOrUpdateChart('topSellersChart', 'topSellersChartContainer', {
            type: 'bar',
            data: {
                labels: topSellers.map(d => d.group_name),
                datasets: [{
                    label: 'Faturamento',
                    data: topSellers.map(d => d.total_valor),
                    backgroundColor: professionalPalette[3],
                }]
            },
            options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y' }
        });

        // Chart 4: Mix Ranking
        ui.createOrUpdateChart('mixChart', 'mixChartContainer', {
            type: 'bar',
            data: {
                labels: mix.map(d => d.group_name),
                datasets: [{
                    label: 'Média de SKUs por PDV',
                    data: mix.map(d => d.total_valor),
                    backgroundColor: professionalPalette[5],
                }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });

    } catch (error) {
        console.error("Erro ao carregar análise semanal:", error);
        alert("Não foi possível carregar a análise semanal.");
    } finally {
        ui.toggleAppLoader(false);
    }
}

/**
 * Fetches data and updates the comparison view.
 */
async function updateComparisonView() {
    ui.toggleAppLoader(true);
    ui.updateLoaderText('Carregando dados comparativos...');
    try {
        const filters = getFiltersForRPC('comparison-view');
        const comparisonData = await api.getComparisonData(supabase, filters);

        if (!comparisonData) throw new Error("A API não retornou dados.");

        const currentSales = comparisonData.filter(d => d.origem === 'current');
        const historySales = comparisonData.filter(d => d.origem === 'history');

        // Placeholder for rendering logic
        console.log('Current Sales:', currentSales.length, 'History Sales:', historySales.length);

        // Logic to render KPIs, charts, and tables using the two datasets will be added here.

    } catch (error) {
        console.error("Erro ao carregar dados comparativos:", error);
        alert("Não foi possível carregar os dados comparativos.");
    } finally {
        ui.toggleAppLoader(false);
    }
}

/**
 * Fetches data and updates the stock analysis view.
 */
async function updateStockView() {
    ui.toggleAppLoader(true);
    ui.updateLoaderText('Carregando análise de estoque...');
    try {
        const filters = getFiltersForRPC('stock-view');
        const stockData = await api.getStockAnalysisData(supabase, filters);
        console.log('Stock Data:', stockData);
        // Placeholder for rendering logic
    } catch (error) {
        console.error("Erro ao carregar análise de estoque:", error);
        alert("Não foi possível carregar a análise de estoque.");
    } finally {
        ui.toggleAppLoader(false);
    }
}

/**
 * Fetches data and updates the innovations/coverage view.
 */
async function updateInnovationsView() {
    ui.toggleAppLoader(true);
    ui.updateLoaderText('Carregando análise de inovações...');
    try {
        const filters = getFiltersForRPC('innovations-view');
        // This view requires product codes, which would come from a multi-select dropdown
        // For now, we'll use a placeholder
        filters.p_product_codes = []; // Needs real implementation

        if (filters.p_product_codes.length > 0) {
            const coverageData = await api.getCoverageAnalysis(supabase, filters);
            console.log('Coverage Data:', coverageData);
            // Placeholder for rendering logic
        } else {
            // Handle state where no products are selected
            document.getElementById('innovations-table-body').innerHTML = '<tr><td colspan="5" class="text-center py-8">Selecione um ou mais produtos para analisar.</td></tr>';
        }

    } catch (error) {
        console.error("Erro ao carregar análise de inovações:", error);
        alert("Não foi possível carregar a análise de inovações.");
    } finally {
        ui.toggleAppLoader(false);
    }
}


/**
 * Collects the current state of filters for a specific view.
 * @param {string} viewId - The ID of the view (e.g., 'dashboard-view').
 * @returns {object} An object containing parameters formatted for RPC calls.
 */
function getFiltersForRPC(viewId) {
    const filters = {};

    // Global filters (apply to most views)
    const supervisorEl = document.getElementById(`${viewId.split('-')[0]}-supervisor-filter`);
    if (supervisorEl) filters.p_supervisor = supervisorEl.value || null;

    // This is a simplified example. In a real scenario, you'd get selected sellers,
    // redes, etc., from your custom dropdown components.
    filters.p_vendedor_nomes = null; // Placeholder
    filters.p_rede_group = null; // Placeholder
    filters.p_redes = null; // Placeholder

    // View-specific filters
    if (viewId === 'dashboard-view') {
        const codcliEl = document.getElementById('codcli-filter');
        if (codcliEl) filters.p_codcli = codcliEl.value || null;

        const posicaoEl = document.getElementById('posicao-filter');
        if (posicaoEl) filters.p_posicao = posicaoEl.value || null;

        // This would read from the Pepsico/Multimarcas toggle
        filters.p_pasta = document.querySelector('#fornecedor-toggle-container .fornecedor-btn.active')?.dataset.fornecedor || null;
    }

    // Add other view-specific filter logic here...
    // if (viewId === 'orders-view') { ... }

    // Default filters that might apply to many RPCs
    filters.p_tipos_venda = ['1', '9']; // Default to always include these types
    filters.p_filial = 'ambas'; // Default value

    return filters;
}


/**
 * Sets up all global event listeners for the application.
 */
function setupEventListeners() {
    // Menu toggling
    elements['menu-toggle-btn'].addEventListener('click', ui.toggleMenu);
    elements['menu-overlay'].addEventListener('click', ui.toggleMenu);

    // Side navigation
    elements['main-nav'].addEventListener('click', async (e) => {
        const navBtn = e.target.closest('.nav-btn');
        if (navBtn && navBtn.dataset.viewId) {
            const viewId = navBtn.dataset.viewId;

            // Handle special case for uploader modal
            if (viewId === 'admin-uploader-modal') {
                const modal = document.getElementById(viewId);
                if(modal) modal.classList.remove('hidden');
                return;
            }

            ui.switchView(viewId);
            ui.toggleMenu(); // Close menu on navigation

            // Update active button state
            document.querySelectorAll('#main-nav .nav-btn').forEach(btn => btn.classList.remove('active'));
            navBtn.classList.add('active');

            // Call the corresponding update function for the new view
            switch (viewId) {
                case 'dashboard-view':
                    await updateDashboardView();
                    break;
                case 'orders-view':
                    ordersTableState.currentPage = 1; // Reset to first page
                    await updateOrdersView();
                    break;
                case 'city-view':
                    await updateCityView();
                    break;
                case 'weekly-view':
                    await updateWeeklyView();
                    break;
                case 'comparison-view':
                    await updateComparisonView();
                    break;
                case 'stock-view':
                    await updateStockView();
                    break;
                case 'innovations-view':
                    await updateInnovationsView();
                    break;
                case 'coverage-view':
                    // This can share the same function as innovations for now
                    await updateInnovationsView();
                    break;
            }
        }
    });

    // --- Orders View Pagination ---
    const prevBtn = document.getElementById('orders-prev-page-btn');
    const nextBtn = document.getElementById('orders-next-page-btn');

    if (prevBtn) {
        prevBtn.addEventListener('click', async () => {
            if (ordersTableState.currentPage > 1) {
                ordersTableState.currentPage--;
                await updateOrdersView();
            }
        });
    }

    if (nextBtn) {
        nextBtn.addEventListener('click', async () => {
            if (ordersTableState.currentPage < ordersTableState.totalPages) {
                ordersTableState.currentPage++;
                await updateOrdersView();
            }
        });
    }

    // Example of a filter event listener
    elements['supervisor-filter'].addEventListener('change', async () => {
        // When a filter changes, we need to update the view
        await updateDashboardView();
    });
}
