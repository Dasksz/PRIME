 
    const SUPABASE_URL = 'https://dhozwhfmrwiumwpcqabi.supabase.co';
    const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRob3p3aGZtcndpdW13cGNxYWJpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNjMzNjAsImV4cCI6MjA3NzgzOTM2MH0.syWqcBCbfH5Ey5AB4NGrsF2-ZuBw4W3NZAPIAZb6Bq4';

    // Helper to UPPERCASE keys
    function mapKeysToUpper(data, type) {
        if (!data || data.length === 0) return [];
        return data.map(item => {
            const newItem = {};
            for (const key in item) {
                // Mapeamentos específicos se necessário, ou apenas Upper
                let newKey = key.toUpperCase();
                // Ajustes finos para corresponder exatamente ao que o script espera
                if (type === 'clients') {
                    if (newKey === 'CODIGO_CLIENTE') newKey = 'Código'; // Special case for Clients
                    if (newKey === 'RCA1') newKey = 'RCA 1';
                    if (newKey === 'RCA2') newKey = 'RCA 2';
                    if (newKey === 'NOMECLIENTE') newKey = 'Cliente';
                    if (newKey === 'RAZAOSOCIAL') newKey = 'razaoSocial'; // Fix: Separate key
                    if (newKey === 'ULTIMACOMPRA') newKey = 'Data da Última Compra';
                    if (newKey === 'DATACADASTRO') newKey = 'Data e Hora de Cadastro';
                    if (newKey === 'INSCRICAOESTADUAL') newKey = 'Insc. Est. / Produtor';
                    if (newKey === 'CNPJ_CPF') newKey = 'CNPJ/CPF';
                    if (newKey === 'ENDERECO') newKey = 'Endereço Comercial';
                    if (newKey === 'TELEFONE') newKey = 'Telefone Comercial';
                    if (newKey === 'DESCRICAO') newKey = 'Descricao'; // For Client
                }

                // For Sales/History (Already match mostly, just need verify)
                if (newKey === 'CLIENTE_NOME') newKey = 'CLIENTE_NOME'; // Keep as is if script uses it

                // Validação de tipos
                if (item[key] !== null) {
                    if (newKey === 'DTPED' || newKey === 'DTSAIDA' || newKey === 'Data da Última Compra' || newKey === 'Data e Hora de Cadastro') {
                         // Ensure it's a valid date string or timestamp
                         newItem[newKey] = item[key];
                    } else if (newKey === 'QTVENDA' || newKey === 'VLVENDA' || newKey === 'VLBONIFIC' || newKey === 'TOTPESOLIQ' || newKey === 'ESTOQUECX' || newKey === 'ESTOQUEUNIT' || newKey === 'PEDIDO') {
                         // Force numeric conversion for sales metrics
                         const val = Number(item[key]);
                         newItem[newKey] = isNaN(val) ? 0 : val;
                    } else if (newKey === 'FILIAL') {
                         newItem[newKey] = String(item[key]);
                    } else {
                         newItem[newKey] = item[key];
                    }
                } else {
                     newItem[newKey] = item[key];
                }
            }
            return newItem;
        });
    }

    async function carregarDadosDoSupabase(supabaseClient) {
        isAppReady = true;
        const loader = document.getElementById('loader');
        const loaderText = document.getElementById('loader-text');
        const dashboardView = document.getElementById('main-dashboard');

        try {
            loader.classList.remove('hidden');
            loaderText.textContent = 'Verificando dados...';

            // --- IndexedDB Cache Logic ---
            const DB_NAME = 'PrimeDashboardDB';
            const STORE_NAME = 'data_store';
            const DB_VERSION = 1;

            const initDB = () => {
                return idb.openDB(DB_NAME, DB_VERSION, {
                    upgrade(db) {
                        if (!db.objectStoreNames.contains(STORE_NAME)) {
                            db.createObjectStore(STORE_NAME);
                        }
                    },
                });
            };

            const getFromCache = async (key) => {
                try {
                    const db = await initDB();
                    return await db.get(STORE_NAME, key);
                } catch (e) {
                    console.warn('Erro ao ler cache:', e);
                    return null;
                }
            };

            const saveToCache = async (key, value) => {
                try {
                    const db = await initDB();
                    await db.put(STORE_NAME, value, key);
                } catch (e) {
                    console.warn('Erro ao salvar cache:', e);
                }
            };

            // 1. Fetch Metadata from Supabase first (lightweight)
            let metadataRemote = null;
            try {
                const { data, error } = await supabaseClient.from('data_metadata').select('*');
                if (!error && data && data.length > 0) {
                    metadataRemote = {};
                    data.forEach(item => metadataRemote[item.key] = item.value);
                }
            } catch (e) {
                console.warn('Erro ao buscar metadados:', e);
            }

            // 2. Check Cache
            let cachedData = await getFromCache('dashboardData');
            let useCache = false;

            if (cachedData && metadataRemote) {
                // Check if remote last_update is same as cached
                const remoteDate = new Date(metadataRemote.last_update).getTime();
                const cachedDate = new Date(cachedData.metadata ? cachedData.metadata.find(m=>m.key==='last_update')?.value : 0).getTime();

                // Also check if working days matches, just in case
                // If remote date is valid and same as cached, use cache
                if (!isNaN(remoteDate) && remoteDate <= cachedDate) {
                    console.log("Usando cache do IndexedDB (Versão atualizada)");
                    useCache = true;
                } else {
                    console.log("Cache desatualizado. Baixando novos dados...");
                }
            }

            if (useCache) {
                 const { detailed, history, clients, products, activeProds, stock, innovations, metadata, orders } = cachedData;
                 // Proceed with cached data
                 // Need to reconstruct Helper Maps if they are not in cache or reconstructable

                 // Reconstruct objects from cached data for processing...
                 // Skipping fetchAll steps

                 loaderText.textContent = 'Processando cache...';
            }

            loaderText.textContent = 'Buscando dados...';

            // Shared client map for both parsers
            const clientMap = {
                'CODIGO_CLIENTE': 'Código',
                'RCA1': 'RCA 1',
                'RCA2': 'RCA 2',
                'NOMECLIENTE': 'Cliente',
                'RAZAOSOCIAL': 'razaoSocial',
                'ULTIMACOMPRA': 'Data da Última Compra',
                'DATACADASTRO': 'Data e Hora de Cadastro',
                'INSCRICAOESTADUAL': 'Insc. Est. / Produtor',
                'CNPJ_CPF': 'CNPJ/CPF',
                'ENDERECO': 'Endereço Comercial',
                'TELEFONE': 'Telefone Comercial',
                'RCAS': 'rcas'
            };

            const parseCSVToObjects = (text, type) => {
                const result = [];
                let headers = null;
                let currentVal = '';
                let currentLine = [];
                let inQuote = false;

                const pushLine = (lineValues) => {
                    if (!headers) {
                        headers = lineValues;
                        return;
                    }
                    if (lineValues.length !== headers.length) return;

                    const obj = {};
                    for (let j = 0; j < headers.length; j++) {
                        let header = headers[j].trim().toUpperCase();
                        let val = lineValues[j];

                        if (type === 'clients' && clientMap[header]) header = clientMap[header];
                        if (type === 'orders' && ['VLVENDA', 'TOTPESOLIQ', 'VLBONIFIC', 'QTVENDA'].includes(header)) val = val === '' ? 0 : Number(val);

                        if (val && typeof val === 'string' && val.startsWith('{') && val.endsWith('}')) {
                            val = val.slice(1, -1).split(',').map(s => s.replace(/^"|"$/g, ''));
                        }

                        obj[header] = val;
                    }
                    result.push(obj);
                };

                for (let i = 0; i < text.length; i++) {
                    const char = text[i];
                    if (inQuote) {
                        if (char === '"') {
                            if (i + 1 < text.length && text[i + 1] === '"') { currentVal += '"'; i++; }
                            else { inQuote = false; }
                        } else { currentVal += char; }
                    } else {
                        if (char === '"') { inQuote = true; }
                        else if (char === ',') { currentLine.push(currentVal); currentVal = ''; }
                        else if (char === '\n' || char === '\r') {
                            if (char === '\r' && i + 1 < text.length && text[i+1] === '\n') i++;
                            currentLine.push(currentVal); currentVal = '';
                            pushLine(currentLine); currentLine = [];
                        } else { currentVal += char; }
                    }
                }
                if (currentLine.length > 0 || currentVal !== '') { currentLine.push(currentVal); pushLine(currentLine); }
                return result;
            };

            const parseCSVToColumnar = (text, type, existingColumnar = null) => {
                const columnar = existingColumnar || { columns: [], values: {}, length: 0 };
                const hasExistingColumns = columnar.columns.length > 0;
                let headers = hasExistingColumns ? columnar.columns : null;
                
                let currentVal = '';
                let currentLine = [];
                let inQuote = false;
                
                // If we already have columns, the first line of this chunk is a repeated header, so we skip it.
                // If we don't, the first line IS the header.
                let skipFirstLine = hasExistingColumns;
                let isFirstLine = true;

                const pushLine = (lineValues) => {
                    if (lineValues.length === 0 || (lineValues.length === 1 && lineValues[0] === '')) return;

                    if (isFirstLine) {
                        isFirstLine = false;
                        if (skipFirstLine) return; // Skip repeated header

                        headers = lineValues.map(h => {
                            let header = h.trim().toUpperCase();
                            if (type === 'clients' && clientMap[header]) header = clientMap[header];
                            return header;
                        });
                        columnar.columns = headers;
                        headers.forEach(h => { if (!columnar.values[h]) columnar.values[h] = []; });
                        return;
                    }

                    if (headers && lineValues.length === headers.length) {
                        for (let j = 0; j < headers.length; j++) {
                            const header = headers[j];
                            let val = lineValues[j];

                            if (type === 'sales' || type === 'history') {
                                if (['QTVENDA', 'VLVENDA', 'VLBONIFIC', 'TOTPESOLIQ', 'ESTOQUECX', 'ESTOQUEUNIT', 'QTVENDA_EMBALAGEM_MASTER', 'PEDIDO'].includes(header)) {
                                    val = val === '' ? 0 : Number(val);
                                }
                            }
                            if (type === 'stock' && header === 'STOCK_QTY') val = val === '' ? 0 : Number(val);
                            if (type === 'clients' && header === 'rcas') {
                                if (typeof val === 'string') {
                                    val = val.trim();
                                    if (val.startsWith('{')) {
                                        val = val.slice(1, -1).split(',').map(s => s.replace(/^"|"$/g, ''));
                                    } else if (val.startsWith('[')) {
                                        try { val = JSON.parse(val); } catch(e) { val = [val]; }
                                    } else if (val === '') {
                                        val = [];
                                    } else {
                                        val = [val];
                                    }
                                } else if (!val) {
                                    val = [];
                                } else if (!Array.isArray(val)) {
                                    val = [val];
                                }
                            }

                            columnar.values[header].push(val);
                        }
                        columnar.length++;
                    }
                };

                for (let i = 0; i < text.length; i++) {
                    const char = text[i];
                    if (inQuote) {
                        if (char === '"') {
                            if (i + 1 < text.length && text[i + 1] === '"') { currentVal += '"'; i++; }
                            else { inQuote = false; }
                        } else { currentVal += char; }
                    } else {
                        if (char === '"') { inQuote = true; }
                        else if (char === ',') { currentLine.push(currentVal); currentVal = ''; }
                        else if (char === '\n' || char === '\r') {
                            if (char === '\r' && i + 1 < text.length && text[i+1] === '\n') i++;
                            currentLine.push(currentVal); currentVal = '';
                            pushLine(currentLine); currentLine = [];
                        } else { currentVal += char; }
                    }
                }
                if (currentLine.length > 0 || currentVal !== '') { currentLine.push(currentVal); pushLine(currentLine); }
                return columnar;
            };

            const fetchAll = async (table, columns = null, type = null, format = 'object') => {
                // Config
                // Reduce CSV page size to 2500 to prevent 500 errors (timeout/memory)
                const pageSize = 20000;
                const CONCURRENCY_LIMIT = 4; // Reduced from 3 to improve stability

                // Initial Count (Only for UI progress estimation, not for termination)
                // We use 'estimated' which is fast but can be inaccurate.
                let estimatedTotal = 100000;
                // Removed HEAD request to prevent 500 errors and delays on large tables.
                // The progress bar will just rely on chunks downloaded.

                // Queue State
                let pageIndex = 0;
                let activeRequests = 0;
                let hasMore = true;

                // Reorder Buffer State
                let nextExpectedPage = 0;
                const bufferedPages = new Map(); // Map<pageIndex, data>
                
                let result = format === 'columnar' ? { columns: [], values: {}, length: 0 } : [];

                // Track progress for UI
                const reportProgress = () => {
                    const fetched = format === 'columnar' ? result.length : result.length;
                    console.log(`[${table}] Fetched rows: ${fetched}`);
                };

                return new Promise((resolve, reject) => {
                    const processQueue = () => {
                        // If no more pages to fetch, no active requests, and buffer empty, we are done.
                        if (!hasMore && activeRequests === 0 && bufferedPages.size === 0) {
                            resolve(result);
                            return;
                        }

                        // Launch workers until concurrency limit or stop signal
                        while (hasMore && activeRequests < CONCURRENCY_LIMIT) {
                            const currentPage = pageIndex++;
                            activeRequests++;

                            const from = currentPage * pageSize;
                            const to = (currentPage + 1) * pageSize - 1;

                            const fetchWithRetry = async (attempt = 1) => {
                                try {
                                    const query = supabaseClient.from(table).select(columns || '*');
                                    const promise = columns ? query.range(from, to).csv() : query.range(from, to);

                                    // Timeout wrapper (30 seconds)
                                    const timeoutPromise = new Promise((_, reject) =>
                                        setTimeout(() => reject(new Error('Request timed out')), 30000)
                                    );

                                    const response = await Promise.race([promise, timeoutPromise]);

                                    if (response.error) throw response.error;
                                    return response.data;
                                } catch (err) {
                                    if (attempt < 4) { // Retry up to 3 times (1+3=4 total attempts)
                                        const delay = 1000 * Math.pow(2, attempt - 1); // 1s, 2s, 4s
                                        console.warn(`Retrying page ${currentPage} of ${table} (Attempt ${attempt})... Error: ${err.message}`);
                                        await new Promise(r => setTimeout(r, delay));
                                        return fetchWithRetry(attempt + 1);
                                    }
                                    throw err;
                                }
                            };

                            fetchWithRetry().then((data) => {
                                activeRequests--;

                                // Determine if page is empty
                                let isEmpty = false;
                                let chunkLength = 0;

                                if (columns) {
                                    if (!data || data.length < 5) isEmpty = true;
                                } else {
                                    if (!data || data.length === 0) isEmpty = true;
                                    chunkLength = data ? data.length : 0;
                                }

                                if (isEmpty) {
                                    hasMore = false;
                                    bufferedPages.set(currentPage, null);
                                } else {
                                    bufferedPages.set(currentPage, data);
                                    if (!columns && chunkLength < pageSize) hasMore = false;
                                }

                                // Process Buffer Sequentially
                                while (bufferedPages.has(nextExpectedPage)) {
                                    const chunkData = bufferedPages.get(nextExpectedPage);
                                    bufferedPages.delete(nextExpectedPage);
                                    nextExpectedPage++;

                                    if (chunkData) {
                                        if (columns) {
                                            if (format === 'columnar') {
                                                const preLen = result.length;
                                                result = parseCSVToColumnar(chunkData, type, result);
                                                const added = result.length - preLen;
                                                if (added === 0) hasMore = false;
                                            } else {
                                                const objects = parseCSVToObjects(chunkData, type);
                                                result = result.concat(objects);
                                                if (objects.length === 0) hasMore = false;
                                            }
                                        } else {
                                            result = result.concat(chunkData);
                                        }
                                    }
                                }

                                // Continue processing queue
                                processQueue();

                            }).catch(err => {
                                console.error(`Failed to fetch page ${currentPage} of ${table} after retries:`, err);
                                activeRequests--;
                                hasMore = false;
                                // Unblock sequence
                                bufferedPages.set(currentPage, null);
                                processQueue();
                            });
                        }
                    };

                    // Start
                    processQueue();
                });
            };

            let detailed, history, clients, products, activeProds, stock, innovations, metadata, orders;

            if (useCache) {
                // Se usamos cache, os dados já foram carregados do IndexedDB na variável cachedData
                // Precisamos extraí-los para as variáveis que o resto do script espera
                detailed = cachedData.detailed;
                history = cachedData.history;
                clients = cachedData.clients;
                products = cachedData.products;
                activeProds = cachedData.activeProds;
                stock = cachedData.stock;
                innovations = cachedData.innovations;
                metadata = cachedData.metadata;
                orders = cachedData.orders;
            } else {
                const colsDetailed = 'pedido,codcli,nome,superv,codsupervisor,produto,descricao,fornecedor,observacaofor,codfor,codusur,qtvenda,vlvenda,vlbonific,totpesoliq,dtped,dtsaida,posicao,estoqueunit,tipovenda,filial,qtvenda_embalagem_master';
                const colsClients = 'codigo_cliente,rca1,rca2,rcas,cidade,nomecliente,bairro,razaosocial,fantasia,cnpj_cpf,endereco,numero,cep,telefone,email,ramo,ultimacompra,datacadastro,bloqueio,inscricaoestadual';
                const colsStock = 'product_code,filial,stock_qty';
                const colsOrders = 'pedido,codcli,cliente_nome,cidade,nome,superv,fornecedores_str,dtped,dtsaida,posicao,vlvenda,totpesoliq,filial,tipovenda,fornecedores_list,codfors_list';

                const [detailedUpper, historyUpper, clientsUpper, productsFetched, activeProdsFetched, stockFetched, innovationsFetched, metadataFetched, ordersUpper] = await Promise.all([
                    fetchAll('data_detailed', colsDetailed, 'sales', 'columnar'),
                    fetchAll('data_history', colsDetailed, 'history', 'columnar'),
                    fetchAll('data_clients', colsClients, 'clients', 'columnar'),
                    fetchAll('data_product_details'),
                    fetchAll('data_active_products'),
                    fetchAll('data_stock', colsStock, 'stock', 'columnar'),
                    fetchAll('data_innovations'),
                    fetchAll('data_metadata'),
                    fetchAll('data_orders', colsOrders, 'orders', 'object')
                ]);

                detailed = detailedUpper;
                history = historyUpper;
                clients = clientsUpper;
                products = productsFetched;
                activeProds = activeProdsFetched;
                stock = stockFetched;
                innovations = innovationsFetched;
                metadata = metadataFetched;
                orders = ordersUpper;

                // Salvar no Cache
                const dataToCache = {
                    detailed, history, clients, products, activeProds, stock, innovations, metadata, orders
                };

                // Salvar de forma assíncrona sem travar UI
                saveToCache('dashboardData', dataToCache).then(() => console.log('Dados salvos no cache IndexedDB.'));
            }

            loaderText.textContent = 'Processando...';

            // Reconstruct Helper Maps
            const productDetailsMap = {};
            if (products && products.length) {
                products.forEach(p => {
                    productDetailsMap[p.code] = {
                        descricao: p.descricao,
                        fornecedor: p.fornecedor,
                        codfor: p.codfor,
                        dtCadastro: p.dtcadastro ? new Date(p.dtcadastro).getTime() : null,
                        ...p
                    };
                });
            }

            const activeProductCodes = activeProds.map(p => p.code);

            const stockMap05 = {};
            const stockMap08 = {};
            if (stock && stock.values) {
                const pCodes = stock.values['PRODUCT_CODE'];
                const filials = stock.values['FILIAL'];
                const qtys = stock.values['STOCK_QTY'];
                const len = stock.length;
                for(let i = 0; i < len; i++) {
                    const code = pCodes[i];
                    const fil = filials[i];
                    const qty = qtys[i];
                    if (fil === '05') stockMap05[code] = qty;
                    if (fil === '08') stockMap08[code] = qty;
                }
            }

            // Calculate Last Sale Date (Columnar optimized)
            let lastSale = 0;
            if (detailed.values && detailed.values['DTPED']) {
                const dtpeds = detailed.values['DTPED'];
                const len = dtpeds.length;
                for(let i=0; i<len; i++) {
                    if (dtpeds[i] > lastSale) lastSale = dtpeds[i];
                }
            } else if (Array.isArray(detailed) && detailed.length > 0) {
                 lastSale = detailed.reduce((max, p) => p.DTPED > max ? p.DTPED : max, 0);
            }
            if (lastSale === 0) lastSale = Date.now();

            const embeddedData = {
                detailed: detailed,
                history: history,
                clients: clients,
                byOrder: orders,
                stockMap05: stockMap05,
                stockMap08: stockMap08,
                innovationsMonth: innovations,
                activeProductCodes: activeProductCodes,
                productDetails: productDetailsMap,
                metadata: metadata,
                passedWorkingDaysCurrentMonth: 1,
                isColumnar: true
            };

            // Update Generation Date UI
            const lastUpdateText = document.getElementById('last-update-text');
            if (lastUpdateText) {
                let displayDate = lastSale;

                // Try to get from metadata first (Actual Upload Time)
                if (embeddedData.metadata && Array.isArray(embeddedData.metadata)) {
                    const metaUpdate = embeddedData.metadata.find(m => m.key === 'last_update');
                    if (metaUpdate && metaUpdate.value) {
                        displayDate = metaUpdate.value;
                    }
                }

                const dateObj = new Date(displayDate);
                if (!isNaN(dateObj.getTime())) {
                    const formattedDate = dateObj.toLocaleString('pt-BR');
                    lastUpdateText.textContent = `Dados atualizados em: ${formattedDate}`;
                }
            }

            window.embeddedData = embeddedData;
            window.isDataLoaded = true;

            // Inject Logic
            const scriptEl = document.createElement('script');
            scriptEl.src = 'app.js';
            scriptEl.onload = () => {
                loader.classList.add('hidden');
                document.getElementById('content-wrapper').classList.remove('hidden');
            };
            document.body.appendChild(scriptEl);

        } catch (e) {
            console.error(e);
            loaderText.textContent = 'Erro: ' + e.message;
        }
    }
    document.addEventListener('DOMContentLoaded', () => {
        const { createClient } = supabase;
        const supabaseClient = window.supabaseClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

        // Gatekeeper Logic
        const loginButton = document.getElementById('login-button');
        const telaLogin = document.getElementById('tela-login');
        const telaLoading = document.getElementById('tela-loading');
        const telaPendente = document.getElementById('tela-pendente');

        // Logout Button Logic for Pending Screen
        const logoutButtonPendente = document.getElementById('logout-button-pendente');
        if (logoutButtonPendente) {
            logoutButtonPendente.addEventListener('click', async () => {
                const { error } = await supabaseClient.auth.signOut();
                if (error) console.error('Erro ao sair:', error);
                window.location.reload();
            });
        }

        loginButton.addEventListener('click', async () => {
            await supabaseClient.auth.signInWithOAuth({
                provider: 'google',
                options: { redirectTo: window.location.origin + window.location.pathname }
            });
        });

        let isCheckingProfile = false;
        let isAppReady = false;

        async function verifyUserProfile(session) {
            if (window.isDataLoaded) {
                const telaLoading = document.getElementById('tela-loading');
                const telaLogin = document.getElementById('tela-login');
                if (telaLoading) telaLoading.classList.add('hidden');
                if (telaLogin) telaLogin.classList.add('hidden');
                return;
            }

            if (isCheckingProfile || !session) return;
            isCheckingProfile = true;

            // Only Reset UI to Loading State if App is NOT Ready (Initial Load)
            if (!isAppReady) {
                telaLogin.classList.add('hidden');
                telaPendente.classList.add('hidden');
                const card = document.getElementById('loading-card-content');
                if (card) {
                    card.innerHTML = `
                        <h2 style="margin-top: 0; font-size: 1.5rem; font-weight: 600;">Carregando...</h2>
                        <p style="color: #a0aec0;">Verificando credenciais.</p>
                        <p style="color: #4a5568; font-size: 0.75rem; margin-top: 1rem;">v5.2.1</p>
                    `;
                }
                telaLoading.classList.remove('hidden');
            }

            try {
                // Check Profile with Timeout - 15s
                const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('Tempo limite de conexão excedido. Verifique sua internet.')), 15000));
                const profilePromise = supabaseClient.from('profiles').select('*').eq('id', session.user.id).single();

                const { data: profile, error } = await Promise.race([profilePromise, timeout]);

                if (error) {
                    if (error.code !== 'PGRST116') {
                        throw error;
                    }
                }

                if (profile && profile.status === 'aprovado') {
                    if (!isAppReady) {
                        telaLoading.classList.add('hidden');
                        carregarDadosDoSupabase(supabaseClient);
                    }
                    // If App is Ready, we do nothing - keep dashboard active.
                } else {
                    // Profile not approved - Enforce Block
                    telaLoading.classList.add('hidden');
                    telaPendente.classList.remove('hidden');

                    // Update Pending Message based on specific status
                    const statusMsg = document.getElementById('pendente-status-msg');
                    if (statusMsg) {
                        if (profile && profile.status === 'bloqueado') {
                            statusMsg.textContent = "Acesso Bloqueado pelo Administrador";
                            statusMsg.style.color = "#e53e3e"; // Red
                        } else {
                            statusMsg.textContent = "Aguardando Liberação";
                            statusMsg.style.color = "#FF9933"; // Orange
                        }
                    }

                    // Hide dashboard content just in case
                    const contentWrapper = document.getElementById('content-wrapper');
                    if(contentWrapper) contentWrapper.classList.add('hidden');
                }
            } catch (err) {
                console.error("Error checking profile:", err);

                // If App is Ready (Silent Check), suppress error screen.
                if (isAppReady) {
                    console.warn("Background profile check failed. Keeping session active.");
                    // Optionally show a toast here in future.
                } else {
                    // Initial Load Failed - Show Error Screen
                    const card = document.getElementById('loading-card-content');
                    if (card) {
                        card.innerHTML = `
                            <h2 style="margin-top: 0; font-size: 1.5rem; font-weight: 600; color: #fc8181;">Erro de Conexão</h2>
                            <p style="color: #a0aec0; margin-bottom: 1.5rem;">${err.message || 'Não foi possível verificar suas credenciais.'}</p>
                            <button id="retry-connection-btn" class="gatekeeper-btn" style="background-color: #2d3748; border-color: #4a5568;">
                                Tentar Novamente
                            </button>
                            <p style="color: #4a5568; font-size: 0.75rem; margin-top: 1rem;">v5.2.1</p>
                        `;
                        // Re-bind retry button
                        const retryBtn = document.getElementById('retry-connection-btn');
                        if(retryBtn) {
                            retryBtn.addEventListener('click', () => {
                                isCheckingProfile = false; // Reset flag to allow retry
                                verifyUserProfile(session);
                            });
                        }
                    } else {
                        alert("Erro de conexão: " + err.message);
                        telaLoading.classList.add('hidden');
                        telaPendente.classList.remove('hidden');
                    }
                }
            } finally {
                isCheckingProfile = false;
            }
        }

        // Visibility Change Listener for Reconnection
        document.addEventListener('visibilitychange', async () => {
            if (document.visibilityState === 'visible') {
                const errorCard = document.getElementById('loading-card-content');
                // Check if we are showing the connection error screen
                if (errorCard && errorCard.innerHTML.includes('Erro de Conexão')) {
                    console.log('Tab became visible and error screen detected. Attempting seamless reconnection...');

                    const { data } = await supabaseClient.auth.getSession();
                    if (data && data.session) {
                        // Force retry without reload
                        isCheckingProfile = false; // Reset flag to allow auto-retry
                        verifyUserProfile(data.session);
                    } else {
                        // No session? Reload to login
                        window.location.reload();
                    }
                }
            }
        });

        // Auth State Listener
        supabaseClient.auth.onAuthStateChange(async (event, session) => {
            if (session) {
                verifyUserProfile(session);
            } else {
                telaLogin.classList.remove('hidden');
            }
        });


        // Theme Switcher Logic
        const themeCheckbox = document.getElementById('checkbox-theme');
        const htmlElement = document.documentElement;

        // Load saved theme
        const savedTheme = localStorage.getItem('theme') || 'dark';
        if (savedTheme === 'light') {
            htmlElement.classList.add('light');
            if(themeCheckbox) themeCheckbox.checked = true;
        }

        if(themeCheckbox) {
            themeCheckbox.addEventListener('change', function() {
                if(this.checked) {
                    htmlElement.classList.add('light');
                    localStorage.setItem('theme', 'light');
                } else {
                    htmlElement.classList.remove('light');
                    localStorage.setItem('theme', 'dark');
                }

                // Force Chart.js update if charts exist (optional but good practice)
                if (typeof charts !== 'undefined') {
                    if (typeof updateDashboard === 'function') {
                        updateDashboard();
                    } else {
                        // Tenta atualizar os gráficos diretamente se updateDashboard não estiver acessível
                        try {
                            Object.values(charts).forEach(chart => chart.update());
                        } catch (e) {
                            console.warn("Could not update charts on theme change", e);
                        }
                    }
                }
            });
        }


        // Admin Modal Logic
        const adminBtn = document.getElementById('open-admin-btn');
        const adminModal = document.getElementById('admin-uploader-modal');
        if (adminBtn && adminModal) {
            adminBtn.addEventListener('click', () => {
                adminModal.classList.remove('hidden');
                // Close sidebar on mobile if open
                document.getElementById('side-menu').classList.remove('translate-x-0');
                document.getElementById('sidebar-overlay').classList.add('hidden');
            });
        }

        // Save Goals Logic
        const saveBtn = document.getElementById('save-goals-btn');
        const clearBtn = document.getElementById('clear-goals-btn');
        const masterKeyModal = document.getElementById('master-key-modal');
        const masterKeyInput = document.getElementById('master-key-input');
        const masterKeyConfirm = document.getElementById('master-key-confirm');
        const masterKeyCancel = document.getElementById('master-key-cancel');

        // State to know if we are saving or clearing
        let pendingAction = null; // 'save' or 'clear'

        if (saveBtn) {
            saveBtn.addEventListener('click', () => {
                pendingAction = 'save';
                masterKeyModal.classList.remove('hidden');
            });
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                pendingAction = 'clear';
                masterKeyModal.classList.remove('hidden');
            });
        }

        masterKeyCancel.addEventListener('click', () => {
            masterKeyModal.classList.add('hidden');
            masterKeyInput.value = '';
        });

        masterKeyConfirm.addEventListener('click', async () => {
            const keyInput = masterKeyInput.value.trim();

            // Tentamos obter a sessão atual do usuário
            const { data: { session } } = await supabaseClient.auth.getSession();

            // Prioridade: Chave fornecida manualmente (pode ser a service_role) OU Token da sessão
            const authToken = keyInput || session?.access_token;

            if (!authToken) {
                alert('Por favor, faça login ou insira a Chave Secreta (service_role).');
                return;
            }

            // Validação básica da chave se fornecida
            if (keyInput) {
                if (keyInput.startsWith('sb_publishable')) {
                    alert("Você inseriu a chave PÚBLICA (sb_publishable). Esta chave não tem permissão de escrita. Por favor, insira a chave SECRETA (service_role) que começa com 'ey...'.");
                    return;
                }
            }

            masterKeyModal.classList.add('hidden');
            masterKeyInput.value = '';

            // Dispatch based on pendingAction
            if (pendingAction === 'save') {
                const statusText = document.getElementById('save-goals-btn');
                const originalText = statusText.innerHTML;
                statusText.disabled = true;
                statusText.innerHTML = 'Salvando...';

                try {
                    await saveGoalsToSupabase(authToken);
                } catch (error) {
                    console.error(error);
                    alert('Erro ao salvar metas: ' + error.message);
                } finally {
                    statusText.disabled = false;
                    statusText.innerHTML = originalText;
                }
            } else if (pendingAction === 'clear') {
                // Call clearGoalsFromSupabase
                // Note: clearGoalsFromSupabase handles its own button state
                try {
                    await clearGoalsFromSupabase(authToken);
                } catch (error) {
                    console.error(error);
                    alert('Erro ao limpar metas: ' + error.message);
                }
            }

            pendingAction = null;
        });

        async function saveGoalsToSupabase(authToken) {
            // Collect Goals Data
            if (typeof globalClientGoals === 'undefined') {
                throw new Error('Dados de metas não disponíveis (globalClientGoals undefined).');
            }

            const monthKey = new Date().toISOString().slice(0, 7); // YYYY-MM

            // Convert Map to JSON-friendly format
            const goalsObj = {};
            globalClientGoals.forEach((val, key) => {
                goalsObj[key] = Object.fromEntries(val);
            });

            const payload = {
                month_key: monthKey,
                supplier: 'ALL', // Global snapshot
                brand: 'GENERAL', // Fix for Unique Constraint
                goals_data: {
                    clients: goalsObj,
                    targets: goalsTargets
                },
                updated_at: new Date().toISOString()
            };

            // Use FETCH directly to allow service_role key without "Forbidden" error in browser
            const response = await fetch(`${SUPABASE_URL}/rest/v1/goals_distribution`, {
                method: 'POST',
                headers: {
                    'apikey': SUPABASE_ANON_KEY,
                    'Authorization': `Bearer ${authToken}`,
                    'Content-Type': 'application/json',
                    'Prefer': 'resolution=merge-duplicates' // upsert behavior
                },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Erro Supabase (${response.status}): ${errorText}`);
            }

            alert('Metas salvas com sucesso!');
        }

        async function clearGoalsFromSupabase(authToken) {
            const btn = document.getElementById('clear-goals-btn');
            const originalText = btn.innerHTML;
            btn.innerHTML = 'Limpando...';
            btn.disabled = true;

            const monthKey = new Date().toISOString().slice(0, 7);

            try {
                // DELETE via REST
                const response = await fetch(`${SUPABASE_URL}/rest/v1/goals_distribution?month_key=eq.${monthKey}&supplier=eq.ALL&brand=eq.GENERAL`, {
                    method: 'DELETE',
                    headers: {
                        'apikey': SUPABASE_ANON_KEY,
                        'Authorization': `Bearer ${authToken}`,
                        'Content-Type': 'application/json'
                    }
                });

                if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`Erro ao limpar metas (${response.status}): ${errorText}`);
                }

                // Limpar estado local
                if(typeof globalClientGoals !== 'undefined') globalClientGoals.clear();
                if(typeof goalsTargets !== 'undefined') {
                    for(let k in goalsTargets) goalsTargets[k] = { fat: 0, vol: 0 };
                }

                // Atualizar UI Input fields
                const elFat = document.getElementById('goal-global-fat');
                const elVol = document.getElementById('goal-global-vol');
                const elMix = document.getElementById('goal-global-mix');
                const elMixSalty = document.getElementById('goal-global-mix-salty');
                const elMixFoods = document.getElementById('goal-global-mix-foods');

                if(elFat) elFat.value = '0,00';
                if(elVol) elVol.value = '0,000';
                if(elMix) elMix.value = '0';
                if(elMixSalty) elMixSalty.value = '0';
                if(elMixFoods) elMixFoods.value = '0';

                // Re-calcular métricas e atualizar tabela (que usará os defaults ou zero)
                // Precisamos chamar updateGoals(), mas ele está dentro do scopo do module script principal
                // ou acessível via window se exposto.
                // VERIFICAÇÃO: updateGoals está definido dentro do scopo 'DOMContentLoaded'.
                // Precisamos expor ou disparar um evento.
                // Vou disparar um evento customizado 'goalsCleared' e ouvir no scopo principal.
                document.dispatchEvent(new CustomEvent('goalsCleared'));

                alert('Metas limpas com sucesso!');
            } catch (err) {
                console.error('Erro ao limpar metas:', err);
                alert('Erro ao limpar metas: ' + err.message);
            } finally {
                btn.innerHTML = originalText;
                btn.disabled = false;
            }
        }

        // Attach listener for Clear Button - This needs to be inside the DOMContentLoaded where authentication happens or keys are available
        // But `clearGoalsFromSupabase` needs access to URL/KEY.
        // Wait, `saveGoalsToSupabase` uses `SUPABASE_URL` which is defined in the script scope above.
        // I will hook the button click in the authentication handler block (where save-goals-btn is handled).
    });
