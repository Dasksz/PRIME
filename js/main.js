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
    const viewPrefixes = ['main', 'orders', 'city', 'comparison', 'stock', 'innovations', 'innovations-month', 'coverage'];

    viewPrefixes.forEach(prefix => {
        // Padrão: Popula dropdowns <select>
        ui.populateDropdown(document.getElementById(`${prefix}-supervisor-filter`), g_supervisors, 'superv', 'superv', 'Todos Supervisores');
        ui.populateDropdown(document.getElementById(`${prefix}-fornecedor-filter`), g_fornecedores, 'codfor', 'fornecedor', 'Todos Fornecedores');

        // Padrão: Popula dropdowns customizados de multi-seleção
        ui.populateMultiSelectDropdown(`${prefix}-vendedor-filter-dropdown`, g_vendedores, 'nome', 'nome');
        ui.populateMultiSelectDropdown(`${prefix}-tipo-venda-filter-dropdown`, g_tiposVenda, 'tipovenda', 'tipovenda');
        ui.populateMultiSelectDropdown(`${prefix}-rede-filter-dropdown`, g_redes, 'rede', 'rede');
        ui.populateMultiSelectDropdown(`${prefix}-supplier-filter-dropdown`, g_fornecedores, 'fornecedor', 'fornecedor');

        // Dropdown de produtos é especial pois pode ser grande
        ui.populateMultiSelectDropdown(`${prefix}-product-list`, g_productDetails, 'code', 'descricao');
    });

    // Lógicas específicas
    // O filtro de supervisor da tela 'weekly' é diferente (checkboxes)
    const weeklySupervisorContainer = document.getElementById('weekly-supervisor-filter');
    if (weeklySupervisorContainer) {
        weeklySupervisorContainer.innerHTML = '';
        g_supervisors.forEach(s => {
            const div = document.createElement('div');
            div.innerHTML = `
                <label class="flex items-center space-x-2 cursor-pointer">
                    <input type="checkbox" class="form-checkbox h-4 w-4 text-teal-600 rounded weekly-supervisor-checkbox" value="${s.superv}">
                    <span class="text-sm">${s.superv}</span>
                </label>
            `;
            weeklySupervisorContainer.appendChild(div);
        });
    }
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
    const tableBody = document.getElementById('orders-table-body');
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
        ordersTableState.totalItems = countResult?.[0]?.total_count ?? 0;

        const ordersData = await api.getPaginatedOrders(supabase, {
            ...rpcParams,
            p_page_number: ordersTableState.currentPage,
            p_page_size: ordersTableState.itemsPerPage
        });

        renderOrdersTable(ordersData);

    } catch (error) {
        console.error("Erro ao carregar pedidos:", error);
        if (tableBody) {
            tableBody.innerHTML = `<tr><td colspan="9" class="text-center py-8 text-red-500">Erro ao carregar pedidos: ${error.message}</td></tr>`;
        }
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
    const cityViewContent = document.getElementById('city-view-content');
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

        if (!analysisData) {
            document.getElementById('city-active-detail-table-body').innerHTML = '<tr><td colspan="6" class="text-center py-8">A API não retornou dados.</td></tr>';
            document.getElementById('city-inactive-detail-table-body').innerHTML = '<tr><td colspan="6" class="text-center py-8">A API não retornou dados.</td></tr>';
            return;
        };

        const chartData = analysisData.filter(d => d.tipo_analise === 'chart');
        const clientList = analysisData.filter(d => d.tipo_analise === 'client_list');

        ui.createOrUpdateChart('salesByClientInCityChart', 'salesByClientInCityChartContainer', { type: 'bar', data: { labels: chartData.map(d => d.group_name), datasets: [{ label: 'Faturamento', data: chartData.map(d => d.total_faturamento), backgroundColor: professionalPalette, }] }, options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y' } });

        const statusCounts = clientList.reduce((acc, client) => { acc[client.status_cliente] = (acc[client.status_cliente] || 0) + 1; return acc; }, { ativo: 0, inativo: 0, novo: 0 });
        ui.createOrUpdateChart('customerStatusChart', 'customerStatusChartContainer', { type: 'doughnut', data: { labels: ['Ativos', 'Inativos', 'Novos'], datasets: [{ data: [statusCounts.ativo, statusCounts.inativo, statusCounts.novo], backgroundColor: [professionalPalette[1], professionalPalette[4], professionalPalette[2]], }] }, options: { responsive: true, maintainAspectRatio: false } });

        const renderClientTable = (tableId, clients) => {
            const tableBody = document.getElementById(tableId);
            if (!tableBody) return;
            tableBody.innerHTML = '';
            clients.forEach(client => {
                const row = document.createElement('tr');
                if (tableId === 'city-active-detail-table-body') {
                    row.innerHTML = `<td>${client.codigo_cliente}</td><td>${client.fantasia}</td><td class="text-right">${formatCurrency(client.total_faturamento)}</td><td>${client.cidade}</td><td>${client.bairro}</td><td>${client.rca1}</td>`;
                } else {
                    row.innerHTML = `<td>${client.codigo_cliente}</td><td>${client.fantasia}</td><td>${client.cidade}</td><td>${client.bairro}</td><td class="text-center">${client.ultimacompra ? new Date(client.ultimacompra).toLocaleDateString('pt-BR') : 'N/A'}</td><td>${client.rca1}</td>`;
                }
                tableBody.appendChild(row);
            });
        };
        renderClientTable('city-active-detail-table-body', clientList.filter(c => c.status_cliente === 'ativo'));
        renderClientTable('city-inactive-detail-table-body', clientList.filter(c => c.status_cliente !== 'ativo'));

    } catch (error) {
        console.error("Erro ao carregar análise de cidades:", error);
        if (cityViewContent) cityViewContent.innerHTML = `<p class="text-center text-red-500 py-8">Erro ao carregar dados: ${error.message}</p>`;
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
    const weeklyViewContainer = document.getElementById('weekly-view');
    try {
        const allFilters = getAppliedFilters('weekly');
        const rpcParams = {
            p_pasta: allFilters.p_pasta,
            p_supervisores: allFilters.p_supervisor ? [allFilters.p_supervisor] : g_supervisors.map(s => s.superv) // Passa todos se nenhum for selecionado
        };

        const rankingData = await api.getWeeklySalesAndRankings(supabase, rpcParams);

        if (!rankingData) {
            // Limpa os gráficos se não houver dados
            ['positivacaoChart', 'topSellersChart', 'mixChart'].forEach(chartId => ui.createOrUpdateChart(chartId, `${chartId}Container`, { type: 'bar', data: { labels: [], datasets: [] } }));
            return;
        }

        const positivacao = rankingData.filter(d => d.tipo_dado === 'rank_positivacao');
        const topSellers = rankingData.filter(d => d.tipo_dado === 'rank_topsellers');
        const mix = rankingData.filter(d => d.tipo_dado === 'rank_mix');

        ui.createOrUpdateChart('positivacaoChart', 'positivacaoChartContainer', { type: 'bar', data: { labels: positivacao.map(d => d.group_name), datasets: [{ label: 'PDVs Positivados', data: positivacao.map(d => d.total_valor), backgroundColor: professionalPalette[2], }] }, options: { responsive: true, maintainAspectRatio: false } });
        ui.createOrUpdateChart('topSellersChart', 'topSellersChartContainer', { type: 'bar', data: { labels: topSellers.map(d => d.group_name), datasets: [{ label: 'Faturamento', data: topSellers.map(d => d.total_valor), backgroundColor: professionalPalette[3], }] }, options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y' } });
        ui.createOrUpdateChart('mixChart', 'mixChartContainer', { type: 'bar', data: { labels: mix.map(d => d.group_name), datasets: [{ label: 'Média de SKUs por PDV', data: mix.map(d => d.total_valor), backgroundColor: professionalPalette[5], }] }, options: { responsive: true, maintainAspectRatio: false } });

    } catch (error) {
        console.error("Erro ao carregar análise semanal:", error);
        if (weeklyViewContainer) weeklyViewContainer.innerHTML = `<p class="text-center text-red-500 py-8">Erro ao carregar dados: ${error.message}</p>`;
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
    const kpiContainer = document.getElementById('comparison-kpi-container');
    const supervisorTableBody = document.getElementById('supervisorComparisonTableBody');

    try {
        const allFilters = getAppliedFilters('comparison');
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

        const comparisonData = await api.getComparisonData(supabase, rpcParams);
        kpiContainer.innerHTML = '';
        supervisorTableBody.innerHTML = '';

        if (!comparisonData || comparisonData.length === 0) {
            kpiContainer.innerHTML = '<p class="text-center col-span-full">Nenhum dado encontrado para o período.</p>';
            return;
        }

        const processKpis = (data, monthName, isCurrent) => {
            const totalFaturamento = data.reduce((sum, item) => sum + item.total_faturamento, 0);
            const totalPeso = data.reduce((sum, item) => sum + item.total_peso, 0);
            const pdvsPositivados = new Set(data.map(item => item.codcli)).size;
            return { monthName, totalFaturamento, totalPeso, pdvsPositivados };
        };

        const kpisAtual = processKpis(comparisonData.filter(d => d.origem === 'current'), 'Mês Atual', true);
        const kpisHist1 = processKpis(comparisonData.filter(d => d.mes_historico === 1), 'Mês 1');
        const kpisHist2 = processKpis(comparisonData.filter(d => d.mes_historico === 2), 'Mês 2');
        const kpisHist3 = processKpis(comparisonData.filter(d => d.mes_historico === 3), 'Mês 3');

        const mediaFaturamentoHist = (kpisHist1.totalFaturamento + kpisHist2.totalFaturamento + kpisHist3.totalFaturamento) / 3;
        const mediaPesoHist = (kpisHist1.totalPeso + kpisHist2.totalPeso + kpisHist3.totalPeso) / 3;
        const mediaPdvsHist = (kpisHist1.pdvsPositivados + kpisHist2.pdvsPositivados + kpisHist3.pdvsPositivados) / 3;

        const kpisMedia = { monthName: 'Média Trim.', totalFaturamento: mediaFaturamentoHist, totalPeso: mediaPesoHist, pdvsPositivados: mediaPdvsHist };

        const renderKpiCard = (kpi) => {
            const card = document.createElement('div');
            card.className = 'comparison-kpi-card';
            card.innerHTML = `
                <h3 class="text-sm font-semibold text-secondary uppercase">${kpi.monthName}</h3>
                <p class="text-xl font-bold text-primary mt-1">${formatCurrency(kpi.totalFaturamento)}</p>
                <p class="text-xs text-secondary">${formatNumber(kpi.totalPeso / 1000, 2)} Ton</p>
                <p class="text-xs text-secondary">${formatNumber(kpi.pdvsPositivados, 0)} PDVs</p>
            `;
            kpiContainer.appendChild(card);
        };

        renderKpiCard(kpisMedia);
        renderKpiCard(kpisAtual);
        [kpisHist3, kpisHist2, kpisHist1].forEach(renderKpiCard);

        // Renderiza tabela de supervisores
        const supervisorData = Array.from(new Set(comparisonData.map(d => d.supervisor))).map(supervisor => {
            const current = comparisonData.filter(d => d.origem === 'current' && d.supervisor === supervisor).reduce((sum, i) => sum + i.total_faturamento, 0);
            const hist1 = comparisonData.filter(d => d.mes_historico === 1 && d.supervisor === supervisor).reduce((sum, i) => sum + i.total_faturamento, 0);
            const hist2 = comparisonData.filter(d => d.mes_historico === 2 && d.supervisor === supervisor).reduce((sum, i) => sum + i.total_faturamento, 0);
            const hist3 = comparisonData.filter(d => d.mes_historico === 3 && d.supervisor === supervisor).reduce((sum, i) => sum + i.total_faturamento, 0);
            const media = (hist1 + hist2 + hist3) / 3;
            const variacao = media > 0 ? (current / media) - 1 : current > 0 ? 1 : 0;
            return { supervisor, media, current, variacao };
        }).sort((a, b) => b.current - a.current);

        supervisorData.forEach(item => {
            const row = document.createElement('tr');
            const variacaoClass = item.variacao > 0 ? 'text-green-500' : item.variacao < 0 ? 'text-red-500' : 'text-gray-500';
            const variacaoSign = item.variacao > 0 ? '+' : '';
            row.innerHTML = `
                <td class="px-4 py-3">${item.supervisor || 'N/A'}</td>
                <td class="px-4 py-3 text-right">${formatCurrency(item.media)}</td>
                <td class="px-4 py-3 text-right">${formatCurrency(item.current)}</td>
                <td class="px-4 py-3 text-right ${variacaoClass}">${variacaoSign}${formatPercentage(item.variacao)}</td>
            `;
            supervisorTableBody.appendChild(row);
        });

    } catch (error) {
        console.error("Erro ao carregar dados comparativos:", error);
        kpiContainer.innerHTML = `<p class="text-center col-span-full text-red-500">Erro ao carregar dados: ${error.message}</p>`;
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
    const mainTableBody = document.getElementById('stock-analysis-table-body');
    const growthTableBody = document.getElementById('growth-table-body');
    const declineTableBody = document.getElementById('decline-table-body');
    const newProductsTableBody = document.getElementById('new-products-table-body');
    const lostProductsTableBody = document.getElementById('lost-products-table-body');

    try {
        const allFilters = getAppliedFilters('stock');
        const rpcParams = {
            p_pasta: allFilters.p_pasta,
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_fornecedores: allFilters.p_fornecedores,
            p_produtos: allFilters.p_produtos,
            p_cidade: allFilters.p_cidade,
            p_filial: allFilters.p_filial
        };

        const stockData = await api.getStockAnalysisData(supabase, rpcParams);

        // Limpa todas as tabelas
        [mainTableBody, growthTableBody, declineTableBody, newProductsTableBody, lostProductsTableBody].forEach(tbody => {
            if (tbody) tbody.innerHTML = '';
        });

        if (!stockData || stockData.length === 0) {
            mainTableBody.innerHTML = '<tr><td colspan="6" class="text-center py-8">Nenhum dado de estoque encontrado.</td></tr>';
            return;
        }

        // Renderiza a tabela principal
        stockData.forEach(item => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td class="px-4 py-3">${item.produto_descricao}</td>
                <td class="px-4 py-3">${item.fornecedor}</td>
                <td class="px-4 py-3 text-right">${formatNumber(item.estoque_atual_cx, 0)}</td>
                <td class="px-4 py-3 text-right">${formatNumber(item.venda_media_mensal_cx, 1)}</td>
                <td class="px-4 py-3 text-right">${formatNumber(item.media_diaria_cx, 2)}</td>
                <td class="px-4 py-3 text-right">${formatNumber(item.tendencia_dias, 1)}</td>
            `;
            mainTableBody.appendChild(row);
        });

        // Filtra e renderiza as tabelas secundárias
        const renderProductChangeTable = (tbody, data, type) => {
            if (!tbody || !data || data.length === 0) return;
            data.forEach(item => {
                const row = document.createElement('tr');
                const variacao = item.venda_atual_cx - item.media_trimestre_cx;
                const variacaoClass = variacao > 0 ? 'text-green-500' : 'text-red-500';

                let variacaoHtml = type === 'new' || type === 'lost'
                    ? ''
                    : `<td class="px-2 py-2 text-right ${variacaoClass}">${formatPercentage(item.variacao)}</td>`;

                row.innerHTML = `
                    <td class="px-2 py-2 text-left">${item.produto_descricao}</td>
                    <td class="px-2 py-2 text-right">${formatNumber(item.venda_atual_cx, 1)}</td>
                    <td class="px-2 py-2 text-right">${formatNumber(item.media_trimestre_cx, 1)}</td>
                    ${variacaoHtml}
                    <td class="px-2 py-2 text-right">${formatNumber(item.estoque_atual_cx, 0)}</td>
                `;
                tbody.appendChild(row);
            });
        };

        renderProductChangeTable(growthTableBody, stockData.filter(p => p.status_produto === 'crescimento'), 'growth');
        renderProductChangeTable(declineTableBody, stockData.filter(p => p.status_produto === 'queda'), 'decline');
        renderProductChangeTable(newProductsTableBody, stockData.filter(p => p.status_produto === 'novo'), 'new');
        renderProductChangeTable(lostProductsTableBody, stockData.filter(p => p.status_produto === 'perdido'), 'lost');

    } catch (error) {
        console.error("Erro ao carregar análise de estoque:", error);
        mainTableBody.innerHTML = '<tr><td colspan="6" class="text-center py-8 text-red-500">Erro ao carregar dados.</td></tr>';
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
    const tableBody = document.getElementById('innovations-table-body');
    const resultsContainer = document.getElementById('innovations-results');
    const placeholder = document.getElementById('innovations-placeholder');

    try {
        const allFilters = getAppliedFilters('innovations');
        const includeBonus = document.getElementById('innovations-include-bonus')?.checked ?? true;

        const rpcParams = {
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_fornecedores: allFilters.p_fornecedores,
            p_cidade: allFilters.p_cidade,
            p_filial: allFilters.p_filial,
            p_product_codes: allFilters.p_produtos || [],
            p_include_bonus: includeBonus
        };

        if (rpcParams.p_product_codes.length === 0) {
            resultsContainer.classList.add('hidden');
            placeholder.classList.remove('hidden');
            tableBody.innerHTML = '';
            return;
        }

        const innovationsData = await api.getCoverageAnalysis(supabase, rpcParams);

        placeholder.classList.add('hidden');
        resultsContainer.classList.remove('hidden');
        tableBody.innerHTML = ''; // Limpa a tabela

        if (!innovationsData || innovationsData.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="5" class="text-center py-8">Nenhum dado encontrado para os produtos selecionados.</td></tr>';
            return;
        }

        innovationsData.forEach(item => {
            const row = document.createElement('tr');
            const variacao = item.cobertura_atual - item.cobertura_anterior;
            const variacaoClass = variacao > 0 ? 'text-green-500' : variacao < 0 ? 'text-red-500' : 'text-gray-500';
            const variacaoSign = variacao > 0 ? '+' : '';

            row.innerHTML = `
                <td class="px-2 py-2">${item.produto_descricao}</td>
                <td class="px-2 py-2 text-right">${formatNumber(item.estoque_cx, 0)}</td>
                <td class="px-2 py-2 text-right">${formatPercentage(item.cobertura_anterior)}</td>
                <td class="px-2 py-2 text-right">${formatPercentage(item.cobertura_atual)}</td>
                <td class="px-2 py-2 text-right ${variacaoClass}">${variacaoSign}${formatPercentage(variacao)}</td>
            `;
            tableBody.appendChild(row);
        });

    } catch (error) {
        console.error("Erro ao carregar análise de inovações:", error);
        tableBody.innerHTML = '<tr><td colspan="5" class="text-center py-8 text-red-500">Erro ao carregar dados.</td></tr>';
    } finally {
        ui.toggleAppLoader(false);
    }
}

/**
 * Fetches data and updates the coverage analysis view.
 */
async function updateCoverageView() {
    ui.toggleAppLoader(true);
    ui.updateLoaderText('Carregando análise de cobertura...');
    const tableBody = document.getElementById('coverage-table-body');
    try {
        const allFilters = getAppliedFilters('coverage');
        const includeBonus = document.getElementById('coverage-include-bonus')?.checked ?? true;

        const rpcParams = {
            p_supervisor: allFilters.p_supervisor,
            p_vendedor_nomes: allFilters.p_vendedor_nomes,
            p_fornecedores: allFilters.p_fornecedores,
            p_cidade: allFilters.p_cidade,
            p_filial: allFilters.p_filial,
            p_product_codes: allFilters.p_produtos || [],
            p_include_bonus: includeBonus
        };

        // A API pode ser a mesma, mas a renderização é diferente
        const coverageData = await api.getCoverageAnalysis(supabase, rpcParams);

        tableBody.innerHTML = '';
        if (!coverageData || coverageData.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="6" class="text-center py-8">Nenhum dado encontrado. Selecione filtros para ver os resultados.</td></tr>';
            return;
        }

        coverageData.forEach(item => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td class="px-2 py-2">${item.produto_descricao}</td>
                <td class="px-2 py-2 text-right">${formatNumber(item.estoque_cx, 0)}</td>
                <td class="px-2 py-2 text-right">${formatNumber(item.cobertura_estoque_dias, 1)}</td>
                <td class="px-2 py-2 text-right">${item.pdvs_mes_anterior}</td>
                <td class="px-2 py-2 text-right">${item.pdvs_mes_atual}</td>
                <td class="px-2 py-2 text-right">${formatPercentage(item.cobertura_pdvs)}</td>
            `;
            tableBody.appendChild(row);
        });

    } catch (error) {
        console.error("Erro ao carregar análise de cobertura:", error);
        tableBody.innerHTML = '<tr><td colspan="6" class="text-center py-8 text-red-500">Erro ao carregar dados.</td></tr>';
    } finally {
        ui.toggleAppLoader(false);
    }
}

/**
 * Helper to register event listeners for a view's filters.
 * @param {string} viewPrefix - The prefix for the view's filter elements.
 * @param {Function} updateFunction - The function to call when a filter changes.
 */
function setupFilterEventListeners(viewPrefix, updateFunction) {
    const filterContainer = document.getElementById(`${viewPrefix}-filters`) || document.getElementById(`${viewPrefix}-view`);
    if (!filterContainer) return;

    const debouncedUpdate = debounce(updateFunction, 300);

    // Lógica de filtro dependente: Supervisor -> Vendedor
    const supervisorSelect = filterContainer.querySelector(`select[id$="supervisor-filter"]`);
    if (supervisorSelect) {
        supervisorSelect.addEventListener('change', async () => {
            const selectedSupervisor = supervisorSelect.value;
            const vendedorDropdownId = `${viewPrefix}-vendedor-filter-dropdown`;

            // Busca novos vendedores baseados no supervisor
            const vendedores = selectedSupervisor
                ? await api.getDistinctVendedores(supabase, selectedSupervisor)
                : g_vendedores; // Usa a lista global se nenhum supervisor for selecionado

            // Repopula o dropdown de vendedores
            ui.populateMultiSelectDropdown(vendedorDropdownId, vendedores, 'nome', 'nome');

            // Dispara a atualização da view principal
            debouncedUpdate();
        });
    }

    // Event listeners para outros <select> e <input>
    filterContainer.querySelectorAll('select:not([id$="supervisor-filter"]), input[type="text"], input[type="number"]').forEach(el => {
        el.addEventListener('change', debouncedUpdate);
    });

    filterContainer.querySelectorAll('.group-btn, .fornecedor-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const group = e.target.closest('.inline-flex, .flex');
            group.querySelectorAll('.group-btn, .fornecedor-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            debouncedUpdate();
        });
    });

    // Lógica para dropdowns de multi-seleção
    filterContainer.querySelectorAll('[id$="-filter-btn"]').forEach(button => {
        const dropdownId = button.id.replace('-btn', '-dropdown');
        const dropdown = document.getElementById(dropdownId);
        if (!dropdown) return;

        button.addEventListener('click', () => dropdown.classList.toggle('hidden'));

        dropdown.addEventListener('change', (e) => {
            if (e.target.classList.contains('item-checkbox') || e.target.classList.contains('select-all-checkbox')) {
                debouncedUpdate();
            }
        });

        const clearButton = dropdown.querySelector('.clear-multiselect-btn');
        if (clearButton) {
            clearButton.addEventListener('click', () => {
                dropdown.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
                debouncedUpdate();
            });
        }
    });

    // Botão para limpar todos os filtros da view
    const clearBtn = document.getElementById(`clear-${viewPrefix}-filters-btn`);
    if (clearBtn) {
        clearBtn.addEventListener('click', () => {
            filterContainer.querySelectorAll('select').forEach(s => s.selectedIndex = 0);
            filterContainer.querySelectorAll('input[type="text"], input[type="number"]').forEach(i => i.value = '');
            filterContainer.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
            // Reseta botões de grupo para o padrão 'Todos'
            filterContainer.querySelectorAll('.group-btn, .fornecedor-btn').forEach(btn => {
                const group = btn.closest('.inline-flex, .flex');
                group.querySelectorAll('.active').forEach(b => b.classList.remove('active'));
                const defaultBtn = group.querySelector('[data-group=""], [data-fornecedor="PEPSICO"]'); // Adapte conforme necessário
                if(defaultBtn) defaultBtn.classList.add('active');
            });
            updateFunction(); // Chama a atualização imediatamente
        });
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

            if (viewId === 'admin-uploader-modal') {
                document.getElementById(viewId)?.classList.remove('hidden');
                return;
            }

            ui.switchView(viewId);
            ui.toggleMenu();

            document.querySelectorAll('#main-nav .nav-btn.active').forEach(btn => btn.classList.remove('active'));
            navBtn.classList.add('active');

            const viewUpdateMap = {
                'dashboard-view': updateDashboardView,
                'orders-view': () => { ordersTableState.currentPage = 1; updateOrdersView(); },
                'city-view': updateCityView,
                'weekly-view': updateWeeklyView,
                'comparison-view': updateComparisonView,
                'stock-view': updateStockView,
                'innovations-view': updateInnovationsView,
                'coverage-view': updateCoverageView // CORRIGIDO
            };

            if (viewUpdateMap[viewId]) {
                await viewUpdateMap[viewId]();
            }
        }
    });

    // Setup filter listeners for each view
    setupFilterEventListeners('main', updateDashboardView);
    setupFilterEventListeners('orders', () => { ordersTableState.currentPage = 1; updateOrdersView(); });
    setupFilterEventListeners('city', updateCityView);
    setupFilterEventListeners('weekly', updateWeeklyView);
    setupFilterEventListeners('comparison', updateComparisonView);
    setupFilterEventListeners('stock', updateStockView);
    setupFilterEventListeners('innovations', updateInnovationsView);
    setupFilterEventListeners('coverage', updateCoverageView);

    // --- Orders View Pagination ---
    document.getElementById('orders-prev-page-btn')?.addEventListener('click', async () => {
        if (ordersTableState.currentPage > 1) {
            ordersTableState.currentPage--;
            await updateOrdersView();
        }
    });

    document.getElementById('orders-next-page-btn')?.addEventListener('click', async () => {
        if (ordersTableState.currentPage < ordersTableState.totalPages) {
            ordersTableState.currentPage++;
            await updateOrdersView();
        }
    });

    // Adiciona listener para fechar dropdowns se clicar fora
    document.addEventListener('click', (e) => {
        document.querySelectorAll('[id$="-filter-dropdown"]').forEach(dropdown => {
            const buttonId = dropdown.id.replace('-dropdown', '-btn');
            const button = document.getElementById(buttonId);
            if (!dropdown.contains(e.target) && !button?.contains(e.target)) {
                dropdown.classList.add('hidden');
            }
        });
    });
}
