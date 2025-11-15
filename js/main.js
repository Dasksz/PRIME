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
let g_lastSaleDate = null;

// --- DOM Element Cache ---
const elements = {};

/**
 * Collects all active filter values from the UI for a specific view.
 * @param {string} viewPrefix - The prefix for the view's filter elements (e.g., 'main', 'orders').
 * @returns {object} An object containing the filter parameters for the API calls.
 */
function getAppliedFilters(viewPrefix) {
    const filters = {};

    // Helper to get value from a simple element (select, input)
    const getElementValue = (id) => {
        const element = document.getElementById(id);
        return element ? (element.value.trim() || null) : null;
    };

    // Helper to get value from a button group (like 'Todos', 'Com Rede', 'Sem Rede')
    const getActiveButtonValue = (containerId) => {
        const container = document.getElementById(containerId);
        if (!container) return null;
        const activeButton = container.querySelector('.active');
        return activeButton ? (activeButton.dataset.group || activeButton.dataset.fornecedor || null) : null;
    };

    // Helper for our custom multi-select dropdowns
    const getMultiSelectValues = (dropdownId) => {
        const dropdown = document.getElementById(dropdownId);
        if (!dropdown) return null;
        const selectedItems = dropdown.querySelectorAll('input[type="checkbox"]:checked');
        if (selectedItems.length === 0) return null;
        return Array.from(selectedItems).map(item => item.value);
    };

    // --- Map UI elements to API parameters using the prefix ---
    filters.p_supervisor = getElementValue(`${viewPrefix}-supervisor-filter`);
    filters.p_codcli = getElementValue(`${viewPrefix}-codcli-filter`);
    filters.p_posicao = getElementValue(`${viewPrefix}-posicao-filter`);
    filters.p_codfor = getElementValue(`${viewPrefix}-fornecedor-filter`);
    filters.p_cidade = getElementValue(`${viewPrefix}-city-filter`) || getElementValue(`${viewPrefix}-name-filter`);
    filters.p_filial = getElementValue(`${viewPrefix}-filial-filter`) || 'ambas';

    // Button groups
    filters.p_pasta = getActiveButtonValue(`${viewPrefix}-fornecedor-toggle-container`) || getActiveButtonValue('fornecedor-toggle-container');
    filters.p_rede_group = getActiveButtonValue(`${viewPrefix}-rede-group-container`);

    // Multi-select dropdowns
    filters.p_vendedor_nomes = getMultiSelectValues(`${viewPrefix}-vendedor-filter-dropdown`);
    filters.p_tipos_venda = getMultiSelectValues(`${viewPrefix}-tipo-venda-filter-dropdown`);
    filters.p_redes = getMultiSelectValues(`${viewPrefix}-rede-filter-dropdown`);
    filters.p_fornecedores = getMultiSelectValues(`${viewPrefix}-supplier-filter-dropdown`);
    filters.p_produtos = getMultiSelectValues(`${viewPrefix}-product-list`);

    // --- Clean up and set defaults ---
    Object.keys(filters).forEach(key => {
        const value = filters[key];
        if (value === '' || (Array.isArray(value) && value.length === 0)) {
            filters[key] = null;
        }
    });

    if (!filters.p_tipos_venda) {
        filters.p_tipos_venda = ['1', '9'];
    }

    return filters;
}

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

    // **CORREÇÃO:** Wrapper para chamadas de API que previne que o Promise.all falhe.
    const safeApiCall = async (promise, defaultValue = null) => {
        try {
            const result = await promise;
            // O helper _callRpc já trata o {data, error}, então podemos retornar diretamente.
            return result;
        } catch (error) {
            console.warn(`Uma chamada de API falhou, mas foi tratada: ${error.message}`);
            return defaultValue; // Retorna um valor padrão em caso de erro.
        }
    };

    try {
        const [
            clientsData,
            productDetailsData,
            supervisorsData,
            fornecedoresData,
            tiposVendaData,
            redesData,
            metadataResult
        ] = await Promise.all([
            safeApiCall(supabase.from('data_clients').select('*'), []),
            safeApiCall(supabase.from('data_product_details').select('code,descricao,codfor,fornecedor,dtcadastro'), []),
            safeApiCall(api.getDistinctSupervisors(supabase), []),
            safeApiCall(api.getDistinctFornecedores(supabase), []),
            safeApiCall(api.getDistinctTiposVenda(supabase), []),
            safeApiCall(api.getDistinctRedes(supabase), []),
            safeApiCall(supabase.from('data_metadata').select('key,value').eq('key', 'last_sale_date'))
        ]);
        
        const metadata = Array.isArray(metadataResult) && metadataResult.length > 0 ? metadataResult[0] : null;

        g_allClientsData = clientsData || [];
        g_productDetails = productDetailsData || [];
        g_supervisors = supervisorsData || [];
        g_fornecedores = fornecedoresData || [];
        g_tiposVenda = tiposVendaData || [];
        g_redes = redesData || [];
        g_lastSaleDate = metadata ? metadata.value : new Date().toISOString().split('T')[0];

        if (elements['generation-date']) {
            elements['generation-date'].textContent = `Dados atualizados em: ${new Date(g_lastSaleDate).toLocaleDateString('pt-BR', { timeZone: 'UTC' })}`;
        }

        populateAllFilterDropdowns();
        ui.populateSideMenu(elements['main-nav']);

        await updateDashboardView();

        setupEventListeners();
        setupUploader();

    } catch (error) {
        // Este bloco catch agora só será atingido por erros inesperados, não por falhas de API.
        console.error("Erro fatal e inesperado durante a inicialização:", error);
        ui.updateLoaderText('Ocorreu um erro crítico. Tente recarregar a página.');
        return; // Mantém o loader visível em caso de erro realmente fatal.
    }

    // Este código agora será executado mesmo se as chamadas de API falharem.
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
        const allFilters = getAppliedFilters('main');
        const groupBy = allFilters.p_supervisor ? 'vendedor' : 'supervisor';

        const mainParams = {
            p_pasta: allFilters.p_pasta,
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_codcli: allFilters.p_codcli,
            p_posicao: allFilters.p_posicao,
            p_codfor: allFilters.p_codfor,
            p_tipos_venda: allFilters.p_tipos_venda,
            p_rede_group: allFilters.p_rede_group,
            p_redes: allFilters.p_redes,
            p_cidade: allFilters.p_cidade,
            p_filial: allFilters.p_filial
        };

        const topProductsParams = {
            ...mainParams,
            p_metric: 'faturamento'
        };

        // **CORREÇÃO:** Aplica o mesmo wrapper de segurança aqui.
        const safeApiCall = async (promise, defaultValue = null) => {
            try {
                return await promise;
            } catch (error) {
                console.warn(`Falha na API da dashboard, continuando com dados vazios: ${error.message}`);
                return defaultValue;
            }
        };

        const [kpiData, salesByPersonData, salesByCategoryData, topProductsData] = await Promise.all([
            safeApiCall(api.getMainKpis(supabase, mainParams), null),
            safeApiCall(api.getSalesByGroup(supabase, { ...mainParams, p_group_by: groupBy }), []),
            safeApiCall(api.getSalesByGroup(supabase, { ...mainParams, p_group_by: 'categoria' }), []),
            safeApiCall(api.getTopProducts(supabase, topProductsParams), [])
        ]);

        // --- 1. Update KPIs ---
        // Adicionada verificação para kpiData para evitar erros se a chamada falhar
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
        if (salesByPersonData && salesByPersonData.length > 0) {
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
        if (salesByCategoryData && salesByCategoryData.length > 0) {
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
        if (topProductsData && topProductsData.length > 0) {
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
        // Não mostra um alerta para não interromper o usuário, apenas loga o erro.
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
        const allFilters = getAppliedFilters('orders');

        const rpcParams = {
            p_pasta: allFilters.p_pasta,
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_codcli: allFilters.p_codcli,
            p_posicao: allFilters.p_posicao,
            p_codfor: allFilters.p_codfor,
            p_tipos_venda: allFilters.p_tipos_venda,
            p_rede_group: allFilters.p_rede_group,
            p_redes: allFilters.p_redes,
            p_cidade: allFilters.p_cidade,
            p_filial: allFilters.p_filial
        };

        const countResult = await api.getOrdersCount(supabase, rpcParams);
        ordersTableState.totalItems = countResult && countResult[0] ? countResult[0].total_count : 0;

        const ordersData = await api.getPaginatedOrders(supabase, {
            ...rpcParams,
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
        const allFilters = getAppliedFilters('city');

        const rpcParams = {
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_rede_group: allFilters.p_rede_group,
            p_redes: allFilters.p_redes,
            p_cidade: allFilters.p_cidade,
            p_codcli: allFilters.p_codcli
        };

        const analysisData = await api.getCityAnalysis(supabase, rpcParams);

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
        const allFilters = getAppliedFilters('weekly');

        const rpcParams = {
            p_pasta: allFilters.p_pasta,
            // The RPC function expects p_supervisores as an array
            p_supervisores: allFilters.p_supervisor ? [allFilters.p_supervisor] : null
        };

        const rankingData = await api.getWeeklySalesAndRankings(supabase, rpcParams);

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
        const allFilters = getAppliedFilters('comparison');

        const rpcParams = {
            p_pasta: allFilters.p_pasta,
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_codcli: allFilters.p_codcli,
            p_fornecedores: allFilters.p_fornecedores,
            p_produtos: allFilters.p_produtos,
            p_rede_group: allFilters.p_rede_group,
            p_redes: allFilters.p_redes,
            p_cidade: allFilters.p_cidade,
            p_filial: allFilters.p_filial
        };

        const comparisonData = await api.getComparisonData(supabase, rpcParams);

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
        const allFilters = getAppliedFilters('stock');

        const rpcParams = {
            p_pasta: allFilters.p_pasta,
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_fornecedores: allFilters.p_fornecedores,
            p_produtos: allFilters.p_produtos,
            p_rede_group: allFilters.p_rede_group,
            p_redes: allFilters.p_redes,
            p_cidade: allFilters.p_cidade,
            p_filial: allFilters.p_filial
        };

        const stockData = await api.getStockAnalysisData(supabase, rpcParams);
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
        const allFilters = getAppliedFilters('innovations');

        // Add the include_bonus checkbox value
        const includeBonus = document.getElementById('innovations-include-bonus');

        const rpcParams = {
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_fornecedores: allFilters.p_fornecedores,
            p_rede_group: allFilters.p_rede_group,
            p_redes: allFilters.p_redes,
            p_cidade: allFilters.p_cidade,
            p_filial: allFilters.p_filial,
            p_product_codes: allFilters.p_produtos || [],
            p_include_bonus: includeBonus ? includeBonus.checked : true
        };

        if (rpcParams.p_product_codes.length > 0) {
            const coverageData = await api.getCoverageAnalysis(supabase, rpcParams);
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
