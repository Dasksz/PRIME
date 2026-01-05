        const embeddedData = window.embeddedData;

        // --- OPTIMIZATION: Lazy Columnar Accessor with Write-Back Support ---
        class ColumnarDataset {
            constructor(columnarData) {
                this.columns = columnarData.columns;
                this._data = columnarData.values; // Renamed to avoid shadowing values() method
                this.length = columnarData.length;
                this._overrides = new Map(); // Stores mutations: Map<index, Object>
            }

            get(index) {
                if (index < 0 || index >= this.length) return undefined;

                const overrides = this._overrides;
                const values = this._data;
                const columns = this.columns;

                // Return a Lazy Proxy that constructs properties only on access
                // and supports write-back for mutations (e.g. seller remapping)
                return new Proxy({}, {
                    get(target, prop) {
                        if (prop === 'toJSON') return () => "ColumnarRowProxy"; // Debug help

                        // 1. Check overrides first (mutations)
                        const ov = overrides.get(index);
                        if (ov && prop in ov) {
                            return ov[prop];
                        }

                        // 2. Check columnar data (lazy read)
                        // Note: values[prop] is the array for that column
                        if (values && values[prop]) {
                            return values[prop][index];
                        }

                        return target[prop]; // Fallback (e.g. prototype methods)
                    },

                    set(target, prop, value) {
                        let ov = overrides.get(index);
                        if (!ov) {
                            ov = {};
                            overrides.set(index, ov);
                        }
                        ov[prop] = value;
                        return true;
                    },

                    ownKeys(target) {
                        // Return all original columns plus any new keys added via mutation
                        const ov = overrides.get(index);
                        if (ov) {
                            // Create a set of keys to ensure uniqueness
                            const keys = new Set(columns);
                            Object.keys(ov).forEach(k => keys.add(k));
                            return Array.from(keys);
                        }
                        return columns;
                    },

                    getOwnPropertyDescriptor(target, prop) {
                        // Check overrides or values to confirm existence
                        const ov = overrides.get(index);
                        if ((ov && prop in ov) || (values && values[prop])) {
                            return { enumerable: true, configurable: true, writable: true };
                        }
                        return undefined;
                    },

                    has(target, prop) {
                        const ov = overrides.get(index);
                        return (ov && prop in ov) || (values && prop in values);
                    }
                });
            }

            // Implement basic Array methods to behave like an array
            map(callback) {
                const result = new Array(this.length);
                for (let i = 0; i < this.length; i++) {
                    result[i] = callback(this.get(i), i, this);
                }
                return result;
            }

            filter(callback) {
                const result = [];
                for (let i = 0; i < this.length; i++) {
                    const item = this.get(i);
                    if (callback(item, i, this)) {
                        result.push(item);
                    }
                }
                return result;
            }

            forEach(callback) {
                for (let i = 0; i < this.length; i++) {
                    callback(this.get(i), i, this);
                }
            }

            reduce(callback, initialValue) {
                let accumulator = initialValue;
                for (let i = 0; i < this.length; i++) {
                    if (i === 0 && initialValue === undefined) {
                        accumulator = this.get(i);
                    } else {
                        accumulator = callback(accumulator, this.get(i), i, this);
                    }
                }
                return accumulator;
            }

            values() {
                // Returns all items as Proxies (expensive if iterated fully, but needed for 'no filter' cases)
                const result = new Array(this.length);
                for (let i = 0; i < this.length; i++) {
                    result[i] = this.get(i);
                }
                return result;
            }

            some(callback) {
                for (let i = 0; i < this.length; i++) {
                    if (callback(this.get(i), i)) return true;
                }
                return false;
            }

            every(callback) {
                for (let i = 0; i < this.length; i++) {
                    if (!callback(this.get(i), i)) return false;
                }
                return true;
            }

            find(callback) {
                for (let i = 0; i < this.length; i++) {
                    const item = this.get(i);
                    if (callback(item, i)) return item;
                }
                return undefined;
            }

            [Symbol.iterator]() {
                let index = 0;
                return {
                    next: () => {
                        if (index < this.length) {
                            return { value: this.get(index++), done: false };
                        } else {
                            return { done: true };
                        }
                    }
                };
            }
        }

        // Custom Map implementation for Index-based storage
        class IndexMap {
            constructor(dataSource) {
                this._indices = new Map();
                this._source = dataSource;
            }

            set(key, index) {
                this._indices.set(key, index);
            }

            get(key) {
                const index = this._indices.get(key);
                if (index === undefined) return undefined;
                return this._source.get(index);
            }

            getIndex(key) {
                return this._indices.get(key);
            }

            has(key) {
                return this._indices.has(key);
            }

            values() {
                // Warning: Heavy operation
                const objects = [];
                for (const index of this._indices.values()) {
                    objects.push(this._source.get(index));
                }
                return objects;
            }

            forEach(callback) {
                this._indices.forEach((index, key) => {
                    callback(this._source.get(index), key);
                });
            }
        }

        function parseDate(dateString) {
            if (!dateString) return null;

            // Se já for um objeto Date, retorna diretamente
            if (dateString instanceof Date) {
                return !isNaN(dateString.getTime()) ? dateString : null;
            }

            // Se for um número (formato Excel ou Timestamp)
            if (typeof dateString === 'number') {
                // Excel Serial Date (approx < 50000 for current dates, Timestamp is > 1000000000000)
                if (dateString < 100000) return new Date(Math.round((dateString - 25569) * 86400 * 1000));
                // Timestamp
                return new Date(dateString);
            }

            if (typeof dateString !== 'string') return null;

            // Tentativa de parse para 'YYYY-MM-DDTHH:mm:ss.sssZ' ou 'YYYY-MM-DD'
            // O construtor do Date já lida bem com isso, mas vamos garantir o UTC.
            if (dateString.includes('T') || dateString.includes('-')) {
                 // Adiciona 'Z' se não tiver informação de fuso horário para forçar UTC
                const isoString = dateString.endsWith('Z') ? dateString : dateString + 'Z';
                const isoDate = new Date(isoString);
                if (!isNaN(isoDate.getTime())) {
                    return isoDate;
                }
            }

            // Tentativa de parse para 'DD/MM/YYYY'
            if (dateString.length === 10 && dateString.charAt(2) === '/' && dateString.charAt(5) === '/') {
                const [day, month, year] = dateString.split('/');
                if (year && month && day && year.length === 4) {
                    // Cria a data em UTC para evitar problemas de fuso horário
                    const utcDate = new Date(Date.UTC(parseInt(year), parseInt(month) - 1, parseInt(day)));
                    if (!isNaN(utcDate.getTime())) {
                        return utcDate;
                    }
                }
            }

            // Fallback para outros formatos que o `new Date()` consegue interpretar
            const genericDate = new Date(dateString);
            if (!isNaN(genericDate.getTime())) {
                return genericDate;
            }

            return null; // Retorna nulo se nenhum formato corresponder
        }

        // --- OPTIMIZATION: Chunked Processor to prevent UI Freeze ---
        function runAsyncChunked(items, processItemFn, onComplete, isCancelled) {
            let index = 0;
            const total = items.length;

            // Check if items is a ColumnarDataset and access by index directly to avoid overhead
            const isColumnar = items instanceof ColumnarDataset;

            function nextChunk() {
                if (isCancelled && isCancelled()) return;

                const start = performance.now();
                while (index < total) {
                    const item = isColumnar ? items.get(index) : items[index];
                    processItemFn(item, index);
                    index++;

                    if (index % 5 === 0 && performance.now() - start >= 12) { // Check budget frequently to avoid long tasks
                        break;
                    }
                }

                if (index < total) {
                    requestAnimationFrame(nextChunk); // Yield to main thread
                } else {
                    if(onComplete) onComplete();
                }
            }

            requestAnimationFrame(nextChunk);
        }

        const FORBIDDEN_KEYS = ['SUPERV', 'CODUSUR', 'CODSUPERVISOR', 'NOME', 'CODCLI', 'PRODUTO', 'DESCRICAO', 'FORNECEDOR', 'OBSERVACAOFOR', 'CODFOR', 'QTVENDA', 'VLVENDA', 'VLBONIFIC', 'TOTPESOLIQ', 'ESTOQUEUNIT', 'TIPOVENDA', 'FILIAL', 'ESTOQUECX', 'SUPERVISOR', 'PASTA', 'RAMO', 'ATIVIDADE', 'CIDADE', 'MUNICIPIO', 'BAIRRO'];
        let allSalesData, allHistoryData, allClientsData;

        function sanitizeData(data) {
            if (!data) return [];
            const forbidden = ['SUPERV', 'CODUSUR', 'CODSUPERVISOR', 'NOME', 'CODCLI', 'PRODUTO', 'DESCRICAO', 'FORNECEDOR', 'OBSERVACAOFOR', 'CODFOR', 'QTVENDA', 'VLVENDA', 'VLBONIFIC', 'TOTPESOLIQ', 'ESTOQUEUNIT', 'TIPOVENDA', 'FILIAL', 'ESTOQUECX', 'SUPERVISOR'];

            // Check if it's a ColumnarDataset proxy or regular array.
            // If it's a ColumnarDataset, we can't easily filter in-place without rebuilding.
            // However, ColumnarDataset usually proxies access.
            // Since we're trying to fix garbage, it's safer to check if we can filter.

            if (Array.isArray(data)) {
                return data.filter(item => {
                    const superv = String(item.SUPERV || '').trim().toUpperCase();
                    const nome = String(item.NOME || '').trim().toUpperCase();
                    const codUsur = String(item.CODUSUR || '').trim().toUpperCase();
                    // Check against headers
                    if (forbidden.includes(superv) || forbidden.includes(nome) || forbidden.includes(codUsur)) return false;
                    return true;
                });
            }
            // If Columnar, we assume the worker already did a good job, OR we might need to implement filtering for ColumnarDataset.
            // But since 'fromColumnar' was removed/replaced by 'ColumnarDataset' class usage?
            // Wait, looking at the code above: 'allSalesData = new ColumnarDataset(...)'.
            // Filtering a ColumnarDataset is hard.
            // But wait! The worker ALREADY sanitizes via 'processSalesData' logic I added.

            return data;
        }

        if (embeddedData.isColumnar) {
            allSalesData = new ColumnarDataset(embeddedData.detailed);
            allHistoryData = new ColumnarDataset(embeddedData.history);
            allClientsData = new ColumnarDataset(embeddedData.clients);
        } else {
            allSalesData = sanitizeData(embeddedData.detailed);
            allHistoryData = sanitizeData(embeddedData.history);
            allClientsData = embeddedData.clients;
        }

        let aggregatedOrders = embeddedData.byOrder;
        const stockData05 = new Map(Object.entries(embeddedData.stockMap05 || {}));
        const stockData08 = new Map(Object.entries(embeddedData.stockMap08 || {}));
        const innovationsMonthData = embeddedData.innovationsMonth;
        const clientMapForKPIs = new Map();
        // Init Client Map manually since map() might be slower on Columnar
        for(let i=0; i<allClientsData.length; i++) {
            const c = allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i];
            clientMapForKPIs.set(String(c['Código'] || c['codigo_cliente']), c);
        }

        const activeProductCodesFromCadastro = new Set(embeddedData.activeProductCodes || []);
        const productDetailsMap = new Map(Object.entries(embeddedData.productDetails || {}));
        const passedWorkingDaysCurrentMonth = embeddedData.passedWorkingDaysCurrentMonth || 1;

        if (embeddedData.lastSaleDate) {
            const ts = parseInt(embeddedData.lastSaleDate);
            if (!isNaN(ts) && ts > 0) {
                lastSaleDate = new Date(ts);
            }
        }

        const clientsWithSalesThisMonth = new Set();
        // Populate set
        for(let i=0; i<allSalesData.length; i++) {
            const s = allSalesData instanceof ColumnarDataset ? allSalesData.get(i) : allSalesData[i];
            clientsWithSalesThisMonth.add(s.CODCLI);
        }

        const optimizedData = {
            salesById: allSalesData, // Use dataset directly to avoid empty IndexMap issues
            historyById: allHistoryData, // Use dataset directly
            indices: {
                current: {
                    bySupervisor: new Map(),
                    byRca: new Map(),
                    byPasta: new Map(),
                    bySupplier: new Map(),
                    byClient: new Map(),
                    byPosition: new Map(),
                    byRede: new Map(),
                    byTipoVenda: new Map(),
                    byProduct: new Map(),
                    byCity: new Map(),
                    byFilial: new Map()
                },
                history: {
                    bySupervisor: new Map(),
                    byRca: new Map(),
                    byPasta: new Map(),
                    bySupplier: new Map(),
                    byClient: new Map(),
                    byPosition: new Map(),
                    byRede: new Map(),
                    byTipoVenda: new Map(),
                    byProduct: new Map(),
                    byCity: new Map(),
                    byFilial: new Map()
                }
            },
            searchIndices: {
                clients: [], // [{ code, nameLower, cityLower }]
                products: [] // [{ code, descLower }]
            }
        };
        let clientLastBranch = new Map();
        let clientRamoMap = new Map();

        const QUARTERLY_DIVISOR = 3;

        // Optimized lastSaleDate calculation to avoid mapping huge array
        let maxDateTs = 0;
        for(let i=0; i<allSalesData.length; i++) {
            const s = allSalesData instanceof ColumnarDataset ? allSalesData.get(i) : allSalesData[i];
            let ts = 0;
            if (typeof s.DTPED === 'number' && s.DTPED > 1000000) {
                 ts = s.DTPED;
            } else {
                 const d = parseDate(s.DTPED);
                 if(d && !isNaN(d)) ts = d.getTime();
            }

            if(ts > maxDateTs) maxDateTs = ts;
        }
        const lastSaleDate = maxDateTs > 0 ? new Date(maxDateTs) : new Date();
        lastSaleDate.setUTCHours(0,0,0,0);
        let maxWorkingDaysStock = 0;
        let sortedWorkingDays = [];
        let customWorkingDaysStock = 0;

        // --- Geolocation Logic (Leaflet + Heatmap + Nominatim) ---
        let leafletMap = null;
        let heatLayer = null;
        let clientMarkersLayer = null;
        let clientCoordinatesMap = new Map(); // Map<ClientCode, {lat, lng, address}>
        let nominatimQueue = [];
        let isProcessingQueue = false;
        let currentFilteredClients = [];
        let currentFilteredSalesMap = new Map();
        let areMarkersGenerated = false;

        // Load cached coordinates from embeddedData
        if (embeddedData.clientCoordinates) {
            // Robust check: Handle both Array and Object (if keys are used)
            const coords = Array.isArray(embeddedData.clientCoordinates) ? embeddedData.clientCoordinates : Object.values(embeddedData.clientCoordinates);
            coords.forEach(c => {
                clientCoordinatesMap.set(String(c.client_code), {
                    lat: parseFloat(c.lat),
                    lng: parseFloat(c.lng),
                    address: c.address
                });
            });
        }

        function initLeafletMap() {
            if (leafletMap) return;
            const mapContainer = document.getElementById('leaflet-map');
            if (!mapContainer) return;

            // Default center (Bahia/Salvador approx)
            const defaultCenter = [-12.9714, -38.5014];

            leafletMap = L.map(mapContainer).setView(defaultCenter, 7);

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            }).addTo(leafletMap);

            // Initialize empty heat layer with increased transparency
            heatLayer = L.heatLayer([], {
                radius: 25,
                blur: 15,
                maxZoom: 10,
                minOpacity: 0.2, // More transparent
                max: 2.0, // Reduce max intensity to allow seeing text underneath
                gradient: {0.2: 'blue', 0.5: 'lime', 1: 'red'}
            }).addTo(leafletMap);

            // Initialize Markers Layer (Hidden by default, shown on zoom)
            clientMarkersLayer = L.layerGroup();

            leafletMap.on('zoomend', () => {
                updateMarkersVisibility();
            });
        }

        async function saveCoordinateToSupabase(clientCode, lat, lng, address) {
            if (window.userRole !== 'adm') return;

            try {
                const { error } = await window.supabaseClient
                    .from('data_client_coordinates')
                    .upsert({
                        client_code: String(clientCode),
                        lat: lat,
                        lng: lng,
                        address: address
                    });

                if (error) console.error("Error saving coordinate:", error);
                else {
                    clientCoordinatesMap.set(String(clientCode), { lat, lng, address });
                }
            } catch (e) {
                console.error("Error saving coordinate:", e);
            }
        }

        // Rate-limited Queue Processor for Nominatim (1 req/1.2s)
        async function processNominatimQueue() {
            if (isProcessingQueue || nominatimQueue.length === 0) return;
            isProcessingQueue = true;

            // Removed Blocking Overlay logic for Background Processing
            const loadingText = document.getElementById('map-loading-text');

            const processNext = async () => {
                if (nominatimQueue.length === 0) {
                    isProcessingQueue = false;
                    console.log("[GeoSync] Fila de download finalizada.");
                    return;
                }

                const { client, address } = nominatimQueue.shift();
                console.log(`[GeoSync] Baixando coordenadas: ${client.nomeCliente} (${nominatimQueue.length} restantes)...`);

                try {
                    const result = await geocodeAddressNominatim(address);
                    if (result) {
                        console.log(`[GeoSync] Sucesso: ${client.nomeCliente} -> Salvo.`);
                        const codCli = String(client['Código'] || client['codigo_cliente']);
                        await saveCoordinateToSupabase(codCli, result.lat, result.lng, result.formatted_address);

                        // Add point directly to current heatmap data if visible
                        if (heatLayer) {
                            heatLayer.addLatLng([result.lat, result.lng, 1]); // intensity 1
                        }
                    } else {
                        console.log(`[GeoSync] Falha (Não encontrado): ${client.nomeCliente}`);
                    }
                } catch (e) {
                    console.error(`[GeoSync] Erro API: ${client.nomeCliente}`, e);
                }

                // Respect Rate Limit: 1200ms
                setTimeout(processNext, 1200);
            };

            processNext();
        }

        async function syncGlobalCoordinates() {
            if (window.userRole !== 'adm') {
                console.log("[GeoSync] Sincronização em segundo plano ignorada (Requer permissão 'adm').");
                return;
            }

            console.log("[GeoSync] Iniciando verificação de coordenadas em segundo plano...");

            const activeClientsList = getActiveClientsData();
            const activeClientCodes = new Set(activeClientsList.map(c => String(c['Código'] || c['codigo_cliente'])));

            // 1. Cleanup Orphans (Clients in DB but NOT in active list)
            const orphanedCodes = [];
            for (const [code, coord] of clientCoordinatesMap) {
                if (!activeClientCodes.has(code)) {
                    orphanedCodes.push(code);
                }
            }

            if (orphanedCodes.length > 0) {
                console.log(`Cleaning up ${orphanedCodes.length} orphaned coordinates...`);
                // Batch delete from Supabase
                const { error } = await window.supabaseClient
                    .from('data_client_coordinates')
                    .delete()
                    .in('client_code', orphanedCodes);

                if (!error) {
                    orphanedCodes.forEach(c => clientCoordinatesMap.delete(c));
                }
            }

            // 2. Queue All Missing (Active clients without coordinates)
            let queuedCount = 0;
            // Iterate all ACTIVE clients
            activeClientsList.forEach(client => {
                const code = String(client['Código'] || client['codigo_cliente']);
                if (clientCoordinatesMap.has(code)) return; // Already has coord

                // Check address validity
                const addressParts = [
                    client.endereco || client.ENDERECO,
                    client.numero || client.NUMERO,
                    client.bairro || client.BAIRRO,
                    client.cidade || client.CIDADE,
                    "Bahia",
                    "Brasil"
                ].filter(p => p && p !== 'N/A').join(', ');

                if (addressParts.length > 15) {
                    // Avoid duplicates in queue
                    if (!nominatimQueue.some(item => String(item.client['Código'] || item.client['codigo_cliente']) === code)) {
                        nominatimQueue.push({ client, address: addressParts });
                        queuedCount++;
                    }
                }
            });

            if (queuedCount > 0) {
                console.log(`[GeoSync] Identificados ${queuedCount} clientes sem coordenadas. Iniciando download...`);
                processNominatimQueue();
            } else {
                console.log("[GeoSync] Todos os clientes ativos já possuem coordenadas.");
            }
        }

        async function geocodeAddressNominatim(address) {
            if (!address) return null;
            const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(address)}&limit=1`;

            try {
                const response = await fetch(url, {
                    headers: { 'User-Agent': 'PrimeDashboardApp/1.0' }
                });
                if (!response.ok) return null;
                const data = await response.json();
                if (data && data.length > 0) {
                    return {
                        lat: parseFloat(data[0].lat),
                        lng: parseFloat(data[0].lon),
                        formatted_address: data[0].display_name
                    };
                }
            } catch (e) {
                console.warn("Nominatim fetch failed:", e);
            }
            return null;
        }

        async function updateCityMap() {
            const cityMapContainer = document.getElementById('city-map-container');
            if (!leafletMap || (cityMapContainer && cityMapContainer.classList.contains('hidden'))) return;

            const { clients, sales } = getCityFilteredData();
            if (!clients || clients.length === 0) return;

            // Cache for Async Marker Generation
            currentFilteredClients = clients;
            areMarkersGenerated = false;
            if (clientMarkersLayer) clientMarkersLayer.clearLayers();

            // Cache Sales
            currentFilteredSalesMap.clear();
            if (sales) {
                sales.forEach(s => {
                    const cod = s.CODCLI;
                    const val = Number(s.VLVENDA) || 0;
                    currentFilteredSalesMap.set(cod, (currentFilteredSalesMap.get(cod) || 0) + val);
                });
            }

            const heatData = [];
            const missingCoordsClients = [];
            const validBounds = [];

            // Heatmap Loop (Sync - Fast)
            clients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const coords = clientCoordinatesMap.get(codCli);

                if (coords) {
                    heatData.push([coords.lat, coords.lng, 1.0]);
                    validBounds.push([coords.lat, coords.lng]);
                } else {
                    missingCoordsClients.push(client);
                }
            });

            // Update Heatmap
            if (heatLayer) {
                heatLayer.setLatLngs(heatData);
            }

            // Fit Bounds
            if (validBounds.length > 0) {
                leafletMap.fitBounds(validBounds);
            }

            // Trigger Marker Logic
            updateMarkersVisibility();
        }

        function updateMarkersVisibility() {
            if (!leafletMap || !clientMarkersLayer) return;
            const zoom = leafletMap.getZoom();

            if (zoom >= 14) {
                if (!areMarkersGenerated) {
                    generateMarkersAsync();
                } else {
                    if (!leafletMap.hasLayer(clientMarkersLayer)) leafletMap.addLayer(clientMarkersLayer);
                }
            } else {
                if (leafletMap.hasLayer(clientMarkersLayer)) leafletMap.removeLayer(clientMarkersLayer);
            }
        }

        function generateMarkersAsync() {
            if (areMarkersGenerated) return;

            // Use local reference to avoid race conditions if filter changes mid-process
            const clientsToProcess = currentFilteredClients;

            runAsyncChunked(clientsToProcess, (client) => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const coords = clientCoordinatesMap.get(codCli);

                if (coords) {
                    const val = currentFilteredSalesMap.get(codCli) || 0;
                    const formattedVal = val.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

                    const rcaCode = client.rca1 || 'N/A';
                    const rcaName = (optimizedData.rcaNameByCode && optimizedData.rcaNameByCode.get(rcaCode)) || rcaCode;

                    const tooltipContent = `
                        <div class="text-xs">
                            <b>${codCli} - ${client.nomeCliente || 'Cliente'}</b><br>
                            <span class="text-blue-500 font-semibold">RCA: ${rcaName}</span><br>
                            <span class="text-green-600 font-bold">Venda: ${formattedVal}</span><br>
                            ${client.bairro || ''}, ${client.cidade || ''}
                        </div>
                    `;

                    const marker = L.circleMarker([coords.lat, coords.lng], {
                        radius: 10,
                        color: 'transparent',
                        fillColor: 'transparent',
                        fillOpacity: 0,
                        opacity: 0
                    });

                    marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, -5] });
                    clientMarkersLayer.addLayer(marker);
                }
            }, () => {
                areMarkersGenerated = true;
                updateMarkersVisibility();
            });
        }

        function initializeOptimizedDataStructures() {
            const sellerDetailsMap = new Map();
            const sellerLastSaleDateMap = new Map(); // Track latest date per seller
            const clientToCurrentSellerMap = new Map();
            let americanasCodCli = null;

            // Use ONLY History Data for identifying Supervisor (User Request)
            // Identify Supervisor for each Seller based on the *Latest* sale in History
            const historyData = allHistoryData; // Using variable for clarity
            for (let i = 0; i < historyData.length; i++) {
                const s = historyData instanceof ColumnarDataset ? historyData.get(i) : historyData[i];
                const codUsur = s.CODUSUR;
                // Ignorar 'INATIVOS' e 'AMERICANAS' para evitar poluição do mapa de supervisores com lógica de fallback
                if (codUsur && s.NOME !== 'INATIVOS' && s.NOME !== 'AMERICANAS') {
                    const dt = parseDate(s.DTPED);
                    const ts = dt ? dt.getTime() : 0;
                    const lastTs = sellerLastSaleDateMap.get(codUsur) || 0;

                    if (ts >= lastTs || !sellerDetailsMap.has(codUsur)) {
                        sellerLastSaleDateMap.set(codUsur, ts);
                        sellerDetailsMap.set(codUsur, { name: s.NOME, supervisor: s.SUPERV });
                    }
                }
            }

            optimizedData.clientsByRca = new Map();
            optimizedData.searchIndices.clients = new Array(allClientsData.length);

            // Access via accessor method for potential ColumnarDataset
            const getClient = (i) => allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i];

            for (let i = 0; i < allClientsData.length; i++) {
                const client = getClient(i); // Hydrate object for processing
                const codCli = String(client['Código'] || client['codigo_cliente']);

                // Sanitize: Skip header rows if present
                if (!codCli || codCli === 'Código' || codCli === 'codigo_cliente' || codCli === 'CODCLI' || codCli === 'CODIGO') continue;

                // Normalize keys from Supabase (Upper) or Local/Legacy (Lower/Camel)
                // mapKeysToUpper might have transformed 'cidade' -> 'CIDADE', 'ramo' -> 'RAMO', etc.
                client.cidade = client.cidade || client.CIDADE || 'N/A';
                client.bairro = client.bairro || client.BAIRRO || 'N/A';
                client.ramo = client.ramo || client.RAMO || 'N/A';

                // Name Normalization
                // mapKeysToUpper maps 'NOMECLIENTE' -> 'Cliente'. Local/Worker might produce 'nomeCliente'.
                // Fix: Include razaoSocial and RAZAOSOCIAL in naming priority
                client.nomeCliente = client.nomeCliente || client.razaoSocial || client.RAZAOSOCIAL || client.Cliente || client.CLIENTE || client.NOMECLIENTE || 'N/A';

                // RCA Handling
                // mapKeysToUpper maps 'RCA1' -> 'RCA 1'. Local might use 'rca1'.
                const rca1 = client.rca1 || client['RCA 1'] || client.RCA1;
                // Normalize access for rest of the code
                client.rca1 = rca1;

                const razaoSocial = client.razaoSocial || client.RAZAOSOCIAL || client.Cliente || ''; // Fallback

                if (razaoSocial.toUpperCase().includes('AMERICANAS')) {
                    client.rca1 = '1001';
                    client.rcas = ['1001'];
                    americanasCodCli = codCli;
                }
                // Removed INATIVOS logic as per request

                if (client.rca1) clientToCurrentSellerMap.set(codCli, String(client.rca1));
                clientRamoMap.set(codCli, client.ramo);

                // Handle RCAS array (could be 'rcas' or 'RCAS')
                let rcas = client.rcas || client.RCAS;
                
                // Sanitize RCAS: Filter out invalid values like "rcas" (header leak)
                if (Array.isArray(rcas)) {
                    rcas = rcas.filter(r => r && String(r).toLowerCase() !== 'rcas');
                } else if (typeof rcas === 'string' && rcas.toLowerCase() === 'rcas') {
                    rcas = [];
                }

                client.rcas = rcas; // Normalize for later use if needed

                if (rcas) {
                    for (let j = 0; j < rcas.length; j++) {
                        const rca = rcas[j];
                        if (rca) {
                            if (!optimizedData.clientsByRca.has(rca)) optimizedData.clientsByRca.set(rca, []);
                            optimizedData.clientsByRca.get(rca).push(client);
                        }
                    }
                }

                optimizedData.searchIndices.clients[i] = { code: codCli, nameLower: (client.nomeCliente || '').toLowerCase(), cityLower: (client.cidade || '').toLowerCase() };
            }

            optimizedData.rcasBySupervisor = new Map();
            optimizedData.productsBySupplier = new Map();
            optimizedData.salesByProduct = { current: new Map(), history: new Map() };
            optimizedData.rcaCodeByName = new Map();
            optimizedData.rcaNameByCode = new Map();
            optimizedData.supervisorCodeByName = new Map();
            optimizedData.productPastaMap = new Map();
            const supervisorToRcaMap = new Map();
            const workingDaysSet = new Set();

            const processDatasetForIndices = (data, indexSet, dataMap, isHistory) => {
                const { bySupervisor, byRca, byPasta, bySupplier, byClient, byPosition, byRede, byTipoVenda, byProduct, byCity, byFilial } = indexSet;

                const isColumnar = data instanceof ColumnarDataset;
                // Use _data because .values is now a method
                const colValues = isColumnar ? data._data : null;

                // Optimization: Helper to read values without creating Proxy
                const getVal = (i, prop) => {
                    if (isColumnar && colValues && colValues[prop]) {
                        return colValues[prop][i];
                    }
                    // Fallback for non-columnar or missing columns
                    if (isColumnar) {
                         // If column missing in _data, try safe access via get() but this creates proxy
                         // Better: assume if not in _data, it's not there or handled by overrides (which are rare here)
                         // But if we MUST fallback:
                         const item = data.get(i);
                         return item ? item[prop] : undefined;
                    }
                    return data[i] ? data[i][prop] : undefined;
                };

                for (let i = 0; i < data.length; i++) {
                    // Optimized: Use Integer Index as ID
                    const id = i;

                    // Note: dataMap is now the dataset itself, we don't need to set anything into it.
                    // We just index the position 'i'.

                    const supervisor = getVal(i, 'SUPERV') || 'N/A';
                    const rca = getVal(i, 'NOME') || 'N/A';

                    // --- FIX: Derive PASTA if empty directly inside indexing loop ---
                    let pasta = getVal(i, 'OBSERVACAOFOR');
                    if (!pasta || pasta === '0' || pasta === '00' || pasta === 'N/A') {
                        const rawFornecedor = String(getVal(i, 'FORNECEDOR') || '').toUpperCase();
                        pasta = rawFornecedor.includes('PEPSICO') ? 'PEPSICO' : 'MULTIMARCAS';
                    }
                    // ---------------------------------------------------------------

                    const supplier = getVal(i, 'CODFOR');
                    const client = getVal(i, 'CODCLI');
                    const position = getVal(i, 'POSICAO') || 'N/A';
                    const rede = clientRamoMap.get(client) || 'N/A';
                    const tipoVenda = getVal(i, 'TIPOVENDA');
                    const product = getVal(i, 'PRODUTO');
                    // Optimized: Lookup City from Client Map (Removed from Sales Data to save space)
                    const clientObj = clientMapForKPIs.get(String(client));
                    const city = (clientObj ? (clientObj.cidade || clientObj['Nome da Cidade']) : 'N/A').toLowerCase();
                    const filial = getVal(i, 'FILIAL');
                    const codUsur = getVal(i, 'CODUSUR');
                    const codSupervisor = getVal(i, 'CODSUPERVISOR');

                    if (!bySupervisor.has(supervisor)) bySupervisor.set(supervisor, new Set()); bySupervisor.get(supervisor).add(id);
                    if (!byRca.has(rca)) byRca.set(rca, new Set()); byRca.get(rca).add(id);
                    if (!byPasta.has(pasta)) byPasta.set(pasta, new Set()); byPasta.get(pasta).add(id);
                    if (supplier) { if (!bySupplier.has(supplier)) bySupplier.set(supplier, new Set()); bySupplier.get(supplier).add(id); }
                    if (client) { if (!byClient.has(client)) byClient.set(client, new Set()); byClient.get(client).add(id); }
                    if (tipoVenda) { if (!byTipoVenda.has(tipoVenda)) byTipoVenda.set(tipoVenda, new Set()); byTipoVenda.get(tipoVenda).add(id); }
                    if (position) { if (!byPosition.has(position)) byPosition.set(position, new Set()); byPosition.get(position).add(id); }
                    if (rede) { if (!byRede.has(rede)) byRede.set(rede, new Set()); byRede.get(rede).add(id); }
                    if (product) { if (!byProduct.has(product)) byProduct.set(product, new Set()); byProduct.get(product).add(id); }
                    if (city) { if (!byCity.has(city)) byCity.set(city, new Set()); byCity.get(city).add(id); }
                    if (filial) { if (!byFilial.has(filial)) byFilial.set(filial, new Set()); byFilial.get(filial).add(id); }

                    if (codUsur && supervisor) { if (!supervisorToRcaMap.has(supervisor)) supervisorToRcaMap.set(supervisor, new Set()); supervisorToRcaMap.get(supervisor).add(codUsur); }
                    if (supplier && product) { if (!optimizedData.productsBySupplier.has(supplier)) optimizedData.productsBySupplier.set(supplier, new Set()); optimizedData.productsBySupplier.get(supplier).add(product); }
                    if (rca && codUsur) { optimizedData.rcaCodeByName.set(rca, codUsur); optimizedData.rcaNameByCode.set(codUsur, rca); }
                    if (supervisor && codSupervisor) { optimizedData.supervisorCodeByName.set(supervisor, codSupervisor); }
                    if (client && filial) { clientLastBranch.set(client, filial); }
                    if (product && pasta && !optimizedData.productPastaMap.has(product)) { optimizedData.productPastaMap.set(product, pasta); }

                    const dtPed = getVal(i, 'DTPED');
                    if (dtPed) {
                        // dtPed is likely a number (timestamp).
                        // If it's a number, new Date(dtPed) works.
                        // If it's a string, parseDate(dtPed) (from local function or global?)
                        // Global parseDate handles numbers too.
                        const dateObj = (typeof dtPed === 'number') ? new Date(dtPed) : parseDate(dtPed);

                        if(dateObj && !isNaN(dateObj.getTime())) {
                            const dayOfWeek = dateObj.getUTCDay();
                            if (dayOfWeek >= 1 && dayOfWeek <= 5) workingDaysSet.add(dateObj.toISOString().split('T')[0]);
                        }
                    }

                    if (product) {
                        const targetMap = isHistory ? optimizedData.salesByProduct.history : optimizedData.salesByProduct.current;
                        if (!targetMap.has(product)) targetMap.set(product, []);
                        // Here we still push the item object because consumers expect it.
                        // Ideally we would store indices, but that requires larger refactor.
                        // Since this is subset by product, it might be acceptable, or we create Proxy on demand.
                        targetMap.get(product).push(isColumnar ? data.get(i) : data[i]);
                    }
                }
            };

            processDatasetForIndices(allSalesData, optimizedData.indices.current, optimizedData.salesById, false);
            processDatasetForIndices(allHistoryData, optimizedData.indices.history, optimizedData.historyById, true);

            // --- POPULATE MISSING PASTA FOR UNSOLD PRODUCTS ---
            // Build a map of CODFOR -> PASTA using sold products
            const codforToPastaMap = new Map();
            optimizedData.productPastaMap.forEach((pasta, productCode) => {
                const details = productDetailsMap.get(productCode);
                if (details && details.codfor) {
                    if (!codforToPastaMap.has(details.codfor)) {
                        codforToPastaMap.set(details.codfor, pasta);
                    }
                }
            });

            // Backfill Pasta for products that have no sales (and thus no entry in productPastaMap yet)
            productDetailsMap.forEach((details, productCode) => {
                if (!optimizedData.productPastaMap.has(productCode) && details.codfor) {
                    const inferredPasta = codforToPastaMap.get(details.codfor);
                    if (inferredPasta) {
                        optimizedData.productPastaMap.set(productCode, inferredPasta);
                    }
                }
            });
            // --- END BACKFILL ---

            supervisorToRcaMap.forEach((rcas, supervisor) => {
                optimizedData.rcasBySupervisor.set(supervisor, Array.from(rcas));
            });

            // Process Aggregated Orders (Remap only)
            for(let i = 0; i < aggregatedOrders.length; i++) {
                const sale = aggregatedOrders[i];
                // Convert to Date if number
                if (sale.DTPED && !(sale.DTPED instanceof Date)) sale.DTPED = new Date(sale.DTPED);
                if (sale.DTSAIDA && !(sale.DTSAIDA instanceof Date)) sale.DTSAIDA = new Date(sale.DTSAIDA);

                if (sale.CODCLI !== americanasCodCli) {
                    const currentSellerCode = clientToCurrentSellerMap.get(sale.CODCLI);
                    if (currentSellerCode) {
                        const sellerDetails = sellerDetailsMap.get(currentSellerCode);
                        if (sellerDetails) {
                            sale.CODUSUR = currentSellerCode;
                            sale.NOME = sellerDetails.name;
                            sale.SUPERV = sellerDetails.supervisor;
                        }
                    }
                }
            }

            sortedWorkingDays = Array.from(workingDaysSet).sort((a, b) => new Date(a) - new Date(b));
            maxWorkingDaysStock = workingDaysSet.size > 0 ? workingDaysSet.size : 1;
            customWorkingDaysStock = maxWorkingDaysStock;

            setTimeout(() => {
                const maxDaysLabel = document.getElementById('max-working-days-label');
                if (maxDaysLabel) maxDaysLabel.textContent = `(Máx: ${maxWorkingDaysStock})`;
                const daysInput = document.getElementById('stock-working-days-input');
                if(daysInput) daysInput.value = customWorkingDaysStock;
            }, 0);
        }

        aggregatedOrders.sort((a, b) => {
            const dateA = parseDate(a.DTPED);
            const dateB = parseDate(b.DTPED);
            if (!dateA) return 1;
            if (!dateB) return -1;
            return dateB - dateA;
        });

        Chart.register(ChartDataLabels);

        const mainDashboard = document.getElementById('main-dashboard');
        const cityView = document.getElementById('city-view');
        const weeklyView = document.getElementById('weekly-view');
        const comparisonView = document.getElementById('comparison-view');
        const stockView = document.getElementById('stock-view');

        const showWeeklyBtn = document.getElementById('show-weekly-btn');
        const showCityBtn = document.getElementById('show-city-btn');
        const backToMainFromCityBtn = document.getElementById('back-to-main-from-city-btn');
        const backToMainFromWeeklyBtn = document.getElementById('back-to-main-from-weekly-btn');
        const backToMainFromComparisonBtn = document.getElementById('back-to-main-from-comparison-btn');
        const backToMainFromStockBtn = document.getElementById('back-to-main-from-stock-btn');

        const totalVendasEl = document.getElementById('total-vendas');
        const totalPesoEl = document.getElementById('total-peso');
        const kpiSkuPdVEl = document.getElementById('kpi-sku-pdv');
        const kpiPositivacaoEl = document.getElementById('kpi-positivacao');
        const kpiPositivacaoPercentEl = document.getElementById('kpi-positivacao-percent');


        const viewChartBtn = document.getElementById('viewChartBtn');
        const viewTableBtn = document.getElementById('viewTableBtn');
        const viewComparisonBtn = document.getElementById('viewComparisonBtn');
        const viewStockBtn = document.getElementById('viewStockBtn');
        const chartView = document.getElementById('chartView');
        const tableView = document.getElementById('tableView');
        const faturamentoBtn = document.getElementById('faturamentoBtn');
        const pesoBtn = document.getElementById('pesoBtn');

        const supervisorFilter = document.getElementById('supervisor-filter');
        const fornecedorFilter = document.getElementById('fornecedor-filter');
        const vendedorFilterBtn = document.getElementById('vendedor-filter-btn');
        const vendedorFilterText = document.getElementById('vendedor-filter-text');
        const vendedorFilterDropdown = document.getElementById('vendedor-filter-dropdown');

        const tipoVendaFilterBtn = document.getElementById('tipo-venda-filter-btn');
        const tipoVendaFilterText = document.getElementById('tipo-venda-filter-text');
        const tipoVendaFilterDropdown = document.getElementById('tipo-venda-filter-dropdown');

        const mainRedeGroupContainer = document.getElementById('main-rede-group-container');
        const mainComRedeBtn = document.getElementById('main-com-rede-btn');
        const mainComRedeBtnText = document.getElementById('main-com-rede-btn-text');
        const mainRedeFilterDropdown = document.getElementById('main-rede-filter-dropdown');

        const posicaoFilter = document.getElementById('posicao-filter');
        const codcliFilter = document.getElementById('codcli-filter');
        const clearFiltersBtn = document.getElementById('clear-filters-btn');
        const salesByPersonTitle = document.getElementById('sales-by-person-title');
        const fornecedorToggleContainerEl = document.getElementById('fornecedor-toggle-container');

        const citySupervisorFilter = document.getElementById('city-supervisor-filter');
        const cityVendedorFilterBtn = document.getElementById('city-vendedor-filter-btn');
        const cityVendedorFilterText = document.getElementById('city-vendedor-filter-text');
        const cityVendedorFilterDropdown = document.getElementById('city-vendedor-filter-dropdown');
        const citySupplierFilterBtn = document.getElementById('city-supplier-filter-btn');
        const citySupplierFilterText = document.getElementById('city-supplier-filter-text');
        const citySupplierFilterDropdown = document.getElementById('city-supplier-filter-dropdown');
        const cityNameFilter = document.getElementById('city-name-filter');
        function getActiveClientsData() {
            return allClientsData.filter(c => {
                const codcli = String(c['Código'] || c['codigo_cliente']);
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                
                // Logic identical to 'updateCoverageView' active clients KPI
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(codcli));
            });
        }
        const cityCodCliFilter = document.getElementById('city-codcli-filter');
        const citySuggestions = document.getElementById('city-suggestions');
        const clearCityFiltersBtn = document.getElementById('clear-city-filters-btn');
        const totalFaturamentoCidadeEl = document.getElementById('total-faturamento-cidade');
        const totalClientesCidadeEl = document.getElementById('total-clientes-cidade');
        const cityActiveDetailTableBody = document.getElementById('city-active-detail-table-body');
        const cityInactiveDetailTableBody = document.getElementById('city-inactive-detail-table-body');

        const cityRedeGroupContainer = document.getElementById('city-rede-group-container');
        const cityComRedeBtn = document.getElementById('city-com-rede-btn');
        const cityComRedeBtnText = document.getElementById('city-com-rede-btn-text');
        const cityRedeFilterDropdown = document.getElementById('city-rede-filter-dropdown');

        const cityTipoVendaFilterBtn = document.getElementById('city-tipo-venda-filter-btn');
        const cityTipoVendaFilterText = document.getElementById('city-tipo-venda-filter-text');
        const cityTipoVendaFilterDropdown = document.getElementById('city-tipo-venda-filter-dropdown');

        const weeklySupervisorFilterBtn = document.getElementById('weekly-supervisor-filter-btn');
        const weeklySupervisorFilterText = document.getElementById('weekly-supervisor-filter-text');
        const weeklySupervisorFilterDropdown = document.getElementById('weekly-supervisor-filter-dropdown');
        const weeklyVendedorFilterBtn = document.getElementById('weekly-vendedor-filter-btn');
        const weeklyVendedorFilterText = document.getElementById('weekly-vendedor-filter-text');
        const weeklyVendedorFilterDropdown = document.getElementById('weekly-vendedor-filter-dropdown');
        const clearWeeklyFiltersBtn = document.getElementById('clear-weekly-filters-btn');
        const totalMesSemanalEl = document.getElementById('total-mes-semanal');
        const weeklyFornecedorToggleContainer = document.getElementById('weekly-fornecedor-toggle-container');

        const comparisonSupervisorFilter = document.getElementById('comparison-supervisor-filter');
        const comparisonVendedorFilterBtn = document.getElementById('comparison-vendedor-filter-btn');
        const comparisonVendedorFilterText = document.getElementById('comparison-vendedor-filter-text');
        const comparisonVendedorFilterDropdown = document.getElementById('comparison-vendedor-filter-dropdown');
        const comparisonFornecedorToggleContainer = document.getElementById('comparison-fornecedor-toggle-container');
        const comparisonFilialFilter = document.getElementById('comparison-filial-filter');

        const comparisonSupplierFilterBtn = document.getElementById('comparison-supplier-filter-btn');
        const comparisonSupplierFilterText = document.getElementById('comparison-supplier-filter-text');
        const comparisonSupplierFilterDropdown = document.getElementById('comparison-supplier-filter-dropdown');

        const comparisonCityFilter = document.getElementById('comparison-city-filter');
        const comparisonCitySuggestions = document.getElementById('comparison-city-suggestions');
        const comparisonProductFilterBtn = document.getElementById('comparison-product-filter-btn');
        const comparisonProductFilterText = document.getElementById('comparison-product-filter-text');
        const comparisonProductFilterDropdown = document.getElementById('comparison-product-filter-dropdown');

        const comparisonRedeGroupContainer = document.getElementById('comparison-rede-group-container');
        const comparisonComRedeBtn = document.getElementById('comparison-com-rede-btn');
        const comparisonComRedeBtnText = document.getElementById('comparison-com-rede-btn-text');
        const comparisonRedeFilterDropdown = document.getElementById('comparison-rede-filter-dropdown');

        const comparisonTipoVendaFilterBtn = document.getElementById('comparison-tipo-venda-filter-btn');
        const comparisonTipoVendaFilterText = document.getElementById('comparison-tipo-venda-filter-text');
        const comparisonTipoVendaFilterDropdown = document.getElementById('comparison-tipo-venda-filter-dropdown');

        const clearComparisonFiltersBtn = document.getElementById('clear-comparison-filters-btn');
        const comparisonTendencyToggle = document.getElementById('comparison-tendency-toggle');

        const comparisonChartTitle = document.getElementById('comparison-chart-title');
        const toggleWeeklyBtn = document.getElementById('toggle-weekly-btn');
        const toggleMonthlyBtn = document.getElementById('toggle-monthly-btn');
        const weeklyComparisonChartContainer = document.getElementById('weeklyComparisonChartContainer');
        const monthlyComparisonChartContainer = document.getElementById('monthlyComparisonChartContainer');

        const stockFilialFilter = document.getElementById('stock-filial-filter');
        const stockRedeGroupContainer = document.getElementById('stock-rede-group-container');
        const stockComRedeBtn = document.getElementById('stock-com-rede-btn');
        const stockComRedeBtnText = document.getElementById('stock-com-rede-btn-text');
        const stockRedeFilterDropdown = document.getElementById('stock-rede-filter-dropdown');
        const stockFornecedorToggleContainer = document.getElementById('stock-fornecedor-toggle-container');
        const stockSupervisorFilter = document.getElementById('stock-supervisor-filter');
        const stockVendedorFilterBtn = document.getElementById('stock-vendedor-filter-btn');
        const stockVendedorFilterText = document.getElementById('stock-vendedor-filter-text');
        const stockVendedorFilterDropdown = document.getElementById('stock-vendedor-filter-dropdown');
        const stockSupplierFilterBtn = document.getElementById('stock-supplier-filter-btn');
        const stockSupplierFilterText = document.getElementById('stock-supplier-filter-text');
        const stockSupplierFilterDropdown = document.getElementById('stock-supplier-filter-dropdown');
        const stockCityFilter = document.getElementById('stock-city-filter');
        const stockCitySuggestions = document.getElementById('stock-city-suggestions');
        const stockProductFilterBtn = document.getElementById('stock-product-filter-btn');
        const stockProductFilterText = document.getElementById('stock-product-filter-text');
        const stockProductFilterDropdown = document.getElementById('stock-product-filter-dropdown');
        const stockTipoVendaFilterBtn = document.getElementById('stock-tipo-venda-filter-btn');
        const stockTipoVendaFilterText = document.getElementById('stock-tipo-venda-filter-text');
        const stockTipoVendaFilterDropdown = document.getElementById('stock-tipo-venda-filter-dropdown');
        const clearStockFiltersBtn = document.getElementById('clear-stock-filters-btn');
        const stockAnalysisTableBody = document.getElementById('stock-analysis-table-body');
        const growthTableBody = document.getElementById('growth-table-body');
        const declineTableBody = document.getElementById('decline-table-body');
        const newProductsTableBody = document.getElementById('new-products-table-body');
        const lostProductsTableBody = document.getElementById('lost-products-table-body');


        const innovationsMonthView = document.getElementById('innovations-month-view');
        const innovationsMonthChartContainer = document.getElementById('innovations-month-chartContainer');
        const innovationsMonthTableBody = document.getElementById('innovations-month-table-body');
        const innovationsMonthCategoryFilter = document.getElementById('innovations-month-category-filter');
        const innovationsMonthSupervisorFilter = document.getElementById('innovations-month-supervisor-filter');
        const innovationsMonthVendedorFilterBtn = document.getElementById('innovations-month-vendedor-filter-btn');
        const innovationsMonthVendedorFilterText = document.getElementById('innovations-month-vendedor-filter-text');
        const innovationsMonthVendedorFilterDropdown = document.getElementById('innovations-month-vendedor-filter-dropdown');
        const innovationsMonthCityFilter = document.getElementById('innovations-month-city-filter');
        const innovationsMonthCitySuggestions = document.getElementById('innovations-month-city-suggestions');
        const clearInnovationsMonthFiltersBtn = document.getElementById('clear-innovations-month-filters-btn');
        const innovationsMonthFilialFilter = document.getElementById('innovations-month-filial-filter');
        const innovationsMonthActiveClientsKpi = document.getElementById('innovations-month-active-clients-kpi');
        const innovationsMonthTopCoverageKpi = document.getElementById('innovations-month-top-coverage-kpi');
        const innovationsMonthTopCoverageValueKpi = document.getElementById('innovations-month-top-coverage-value-kpi');
        const innovationsMonthTopCoverageCountKpi = document.getElementById('innovations-month-top-coverage-count-kpi');
        const innovationsMonthSelectionCoverageValueKpi = document.getElementById('innovations-month-selection-coverage-value-kpi');
        const innovationsMonthSelectionCoverageCountKpi = document.getElementById('innovations-month-selection-coverage-count-kpi');
        const innovationsMonthSelectionCoverageValueKpiPrevious = document.getElementById('innovations-month-selection-coverage-value-kpi-previous');
        const innovationsMonthSelectionCoverageCountKpiPrevious = document.getElementById('innovations-month-selection-coverage-count-kpi-previous');
        const innovationsMonthBonusCoverageValueKpi = document.getElementById('innovations-month-bonus-coverage-value-kpi');
        const innovationsMonthBonusCoverageCountKpi = document.getElementById('innovations-month-bonus-coverage-count-kpi');
        const innovationsMonthBonusCoverageValueKpiPrevious = document.getElementById('innovations-month-bonus-coverage-value-kpi-previous');
        const innovationsMonthBonusCoverageCountKpiPrevious = document.getElementById('innovations-month-bonus-coverage-count-kpi-previous');
        const exportInnovationsMonthPdfBtn = document.getElementById('export-innovations-month-pdf-btn');
        const innovationsMonthTipoVendaFilterBtn = document.getElementById('innovations-month-tipo-venda-filter-btn');
        const innovationsMonthTipoVendaFilterText = document.getElementById('innovations-month-tipo-venda-filter-text');
        const innovationsMonthTipoVendaFilterDropdown = document.getElementById('innovations-month-tipo-venda-filter-dropdown');

        const coverageView = document.getElementById('coverage-view');
        const viewCoverageBtn = document.getElementById('viewCoverageBtn');
        const backToMainFromCoverageBtn = document.getElementById('back-to-main-from-coverage-btn');
        const coverageSupervisorFilter = document.getElementById('coverage-supervisor-filter');
        const coverageVendedorFilterBtn = document.getElementById('coverage-vendedor-filter-btn');
        const coverageVendedorFilterText = document.getElementById('coverage-vendedor-filter-text');
        const coverageVendedorFilterDropdown = document.getElementById('coverage-vendedor-filter-dropdown');
        const coverageSupplierFilterBtn = document.getElementById('coverage-supplier-filter-btn');
        const coverageSupplierFilterText = document.getElementById('coverage-supplier-filter-text');
        const coverageSupplierFilterDropdown = document.getElementById('coverage-supplier-filter-dropdown');
        const coverageCityFilter = document.getElementById('coverage-city-filter');
        const coverageCitySuggestions = document.getElementById('coverage-city-suggestions');
        const coverageProductFilterBtn = document.getElementById('coverage-product-filter-btn');
        const coverageProductFilterText = document.getElementById('coverage-product-filter-text');
        const coverageProductFilterDropdown = document.getElementById('coverage-product-filter-dropdown');
        const clearCoverageFiltersBtn = document.getElementById('clear-coverage-filters-btn');
        const coverageFilialFilter = document.getElementById('coverage-filial-filter');
        const coverageIncludeBonusCheckbox = document.getElementById('coverage-include-bonus');

        const coverageActiveClientsKpi = document.getElementById('coverage-active-clients-kpi');
        const coverageSelectionCoverageValueKpiPrevious = document.getElementById('coverage-selection-coverage-value-kpi-previous');
        const coverageSelectionCoverageCountKpiPrevious = document.getElementById('coverage-selection-coverage-count-kpi-previous');
        const coverageSelectionCoverageValueKpi = document.getElementById('coverage-selection-coverage-value-kpi');
        const coverageSelectionCoverageCountKpi = document.getElementById('coverage-selection-coverage-count-kpi');
        const coverageTopCoverageValueKpi = document.getElementById('coverage-top-coverage-value-kpi');
        const coverageTopCoverageProductKpi = document.getElementById('coverage-top-coverage-product-kpi');
        const coverageTotalBoxesEl = document.getElementById('coverage-total-boxes');

        const coverageTableBody = document.getElementById('coverage-table-body');

        // --- Goals View Elements ---
        const goalsView = document.getElementById('goals-view');
        const goalsGvContent = document.getElementById('goals-gv-content');
        const goalsSvContent = document.getElementById('goals-sv-content');
        const goalsGvTableBody = document.getElementById('goals-gv-table-body');
        const goalsGvTotalValueEl = document.getElementById('goals-gv-total-value');

        const goalsGvSupervisorFilterBtn = document.getElementById('goals-gv-supervisor-filter-btn');
        const goalsGvSupervisorFilterText = document.getElementById('goals-gv-supervisor-filter-text');
        const goalsGvSupervisorFilterDropdown = document.getElementById('goals-gv-supervisor-filter-dropdown');

        const goalsGvSellerFilterBtn = document.getElementById('goals-gv-seller-filter-btn');
        const goalsGvSellerFilterText = document.getElementById('goals-gv-seller-filter-text');
        const goalsGvSellerFilterDropdown = document.getElementById('goals-gv-seller-filter-dropdown');

        const goalsGvCodcliFilter = document.getElementById('goals-gv-codcli-filter');
        const clearGoalsGvFiltersBtn = document.getElementById('clear-goals-gv-filters-btn');

        const goalsSvSupervisorFilterBtn = document.getElementById('goals-sv-supervisor-filter-btn');
        const goalsSvSupervisorFilterText = document.getElementById('goals-sv-supervisor-filter-text');
        const goalsSvSupervisorFilterDropdown = document.getElementById('goals-sv-supervisor-filter-dropdown');


        const modal = document.getElementById('order-details-modal');
        const modalCloseBtn = document.getElementById('modal-close-btn');
        const modalPedidoId = document.getElementById('modal-pedido-id');
        const modalHeaderInfo = document.getElementById('modal-header-info');
        const modalTableBody = document.getElementById('modal-table-body');
        const modalFooterTotal = document.getElementById('modal-footer-total');

        const clientModal = document.getElementById('client-details-modal');
        const clientModalCloseBtn = document.getElementById('client-modal-close-btn');
        const clientModalContent = document.getElementById('client-modal-content');

        const holidayModal = document.getElementById('holiday-modal');
        const holidayModalCloseBtn = document.getElementById('holiday-modal-close-btn');
        const holidayModalDoneBtn = document.getElementById('holiday-modal-done-btn');
        const mainHolidayPickerBtn = document.getElementById('main-holiday-picker-btn');
        const comparisonHolidayPickerBtn = document.getElementById('comparison-holiday-picker-btn');
        const calendarContainer = document.getElementById('calendar-container');

        const tablePaginationControls = document.getElementById('table-pagination-controls');
        const prevPageBtn = document.getElementById('prev-page-btn');
        const nextPageBtn = document.getElementById('next-page-btn');
        const pageInfoText = document.getElementById('page-info-text');

        // --- View State Management ---
        const viewState = {
            dashboard: { dirty: true },
            pedidos: { dirty: true },
            comparativo: { dirty: true },
            estoque: { dirty: true },
            cobertura: { dirty: true },
            cidades: { dirty: true },
            semanal: { dirty: true },
            inovacoes: { dirty: true, cache: null, lastTypesKey: '' },
            mix: { dirty: true },
            goals: { dirty: true }
        };

        // Render IDs for Race Condition Guard
        let mixRenderId = 0;
        let coverageRenderId = 0;
        let cityRenderId = 0;
        let stockRenderId = 0;
        let comparisonRenderId = 0;
        let goalsRenderId = 0;
        let goalsSvRenderId = 0;

        let charts = {};
        let currentProductMetric = 'faturamento';
        let currentFornecedor = '';
        let currentWeeklyFornecedor = '';
        let currentComparisonFornecedor = '';
        let currentStockFornecedor = '';
        let useTendencyComparison = false;
        let comparisonChartType = 'weekly';
        let comparisonMonthlyMetric = 'faturamento';
        let activeClientsForExport = [];
        let inactiveClientsForExport = [];
        let selectedSellers = [];
        let selectedMainSupervisors = [];
        let selectedMainSuppliers = [];
        let selectedTiposVenda = [];
        var selectedCitySellers = [];
        var selectedCitySuppliers = [];
        var selectedCitySupervisors = [];
        let selectedComparisonSellers = [];
        let selectedComparisonSupervisors = [];
        let selectedStockSellers = [];
        let selectedStockSupervisors = [];
        let selectedComparisonSuppliers = [];
        let selectedComparisonProducts = [];
        let selectedStockSuppliers = [];
        let selectedStockProducts = [];
        let selectedStockTiposVenda = [];
        let selectedCoverageTiposVenda = [];
        let selectedComparisonTiposVenda = [];
        let selectedCityTiposVenda = [];
        let historicalBests = {};
        let selectedHolidays = [];
        let stockTrendFilter = 'all';

        let selectedMainRedes = [];
        let selectedCityRedes = [];
        let selectedWeeklySupervisors = [];
        let selectedWeeklySellers = [];
        let selectedComparisonRedes = [];
        let selectedStockRedes = [];

        let mainRedeGroupFilter = '';
        let cityRedeGroupFilter = '';
        let comparisonRedeGroupFilter = '';
        let stockRedeGroupFilter = '';

        let selectedInnovationsMonthSellers = [];
        let selectedInnovationsSupervisors = [];
        let selectedInnovationsMonthTiposVenda = [];

        let selectedMixSupervisors = [];
        let selectedMixSellers = [];
        let selectedMixRedes = [];
        let mixRedeGroupFilter = '';
        let selectedMixTiposVenda = [];
        let mixTableDataForExport = [];
        let mixKpiMode = 'total'; // 'total' ou 'atendidos'

        let currentGoalsSupplier = 'PEPSICO_ALL';
        let currentGoalsBrand = null;
        let currentGoalsSvSupplier = '707';
        let currentGoalsSvBrand = null;
        let currentGoalsSvData = [];
        let goalsTableState = {
            currentPage: 1,
            itemsPerPage: 100,
            filteredData: [],
            totalPages: 1
        };
        let goalsTargets = {
            '707': { fat: 0, vol: 0 },
            '708': { fat: 0, vol: 0 },
            '752': { fat: 0, vol: 0 },
            '1119_TODDYNHO': { fat: 0, vol: 0 },
            '1119_TODDY': { fat: 0, vol: 0 },
            '1119_QUAKER_KEROCOCO': { fat: 0, vol: 0 }
        };
        let globalGoalsMetrics = {};
        let globalGoalsTotalsCache = {};
        let globalClientGoals = new Map();
        let goalsPosAdjustments = { 'ELMA_ALL': new Map(), 'FOODS_ALL': new Map(), 'PEPSICO_ALL': new Map(), '707': new Map(), '708': new Map(), '752': new Map(), '1119_TODDYNHO': new Map(), '1119_TODDY': new Map(), '1119_QUAKER_KEROCOCO': new Map() }; // Map<CodCli, Map<Key, {fat: 0, vol: 0}>>
        let goalsMixSaltyAdjustments = { 'PEPSICO_ALL': new Map(), 'ELMA_ALL': new Map(), 'FOODS_ALL': new Map() }; // Map<SellerName, adjustment>
        let goalsMixFoodsAdjustments = { 'PEPSICO_ALL': new Map(), 'ELMA_ALL': new Map(), 'FOODS_ALL': new Map() }; // Map<SellerName, adjustment>
        let quarterMonths = [];

        function identifyQuarterMonths() {
            const months = new Set();
            allHistoryData.forEach(s => {
                const d = parseDate(s.DTPED);
                if(d) {
                    months.add(`${d.getUTCFullYear()}-${d.getUTCMonth()}`);
                }
            });
            const sorted = Array.from(months).sort((a, b) => {
                const [y1, m1] = a.split('-').map(Number);
                const [y2, m2] = b.split('-').map(Number);
                return (y1 * 12 + m1) - (y2 * 12 + m2);
            });
            // Take last 3
            const last3 = sorted.slice(-3);

            const monthNames = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"];

            quarterMonths = last3.map(k => {
                const [y, m] = k.split('-');
                return { key: k, label: monthNames[parseInt(m)] };
            });
        }

        function calculateGoalsMetrics() {
            if (quarterMonths.length === 0) identifyQuarterMonths();

            // Helper to init metrics structure
            const createMetric = () => ({
                fat: 0, vol: 0, prevFat: 0, prevVol: 0,
                prevClientsSet: new Set(),
                quarterlyPosClientsSet: new Set(), // New Set for Quarter Active
                monthlyClientsSets: new Map() // Map<MonthKey, Set<CodCli>>
            });

            globalGoalsMetrics = {
                '707': createMetric(),
                '708': createMetric(),
                '752': createMetric(),
                '1119_TODDYNHO': createMetric(),
                '1119_TODDY': createMetric(),
                '1119_QUAKER_KEROCOCO': createMetric(),
                'ELMA_ALL': createMetric(),
                'FOODS_ALL': createMetric(),
                'PEPSICO_ALL': createMetric()
            };

            const currentDate = lastSaleDate;
            const prevMonthDate = new Date(Date.UTC(currentDate.getUTCFullYear(), currentDate.getUTCMonth() - 1, 1));
            const prevMonthIndex = prevMonthDate.getUTCMonth();
            const prevMonthYear = prevMonthDate.getUTCFullYear();

            // Filter clients to match the "Active Structure" definition (Same as Coverage/Goals Table)
            const activeClients = allClientsData.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                if (isAmericanas) return true;
                // STRICT FILTER: Exclude RCA 53 (Balcão) and INATIVOS (Empty RCA1)
                if (rca1 === '53') return false;
                if (rca1 === '') return false; // Exclude INATIVOS
                return true;
            });

            // Optimization: Detect if history is columnar and IndexMap is available
            const isHistoryColumnar = optimizedData.historyById instanceof IndexMap && optimizedData.historyById._source.values;
            const historyValues = isHistoryColumnar ? optimizedData.historyById._source.values : null;

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const clientHistoryIds = optimizedData.indices.history.byClient.get(codCli);

                // Temp accumulation for this client to ensure Positive Balance check
                const clientTotals = {}; // key -> { prevFat: 0, monthlyFat: Map<MonthKey, val> }

                if (clientHistoryIds) {
                    if (isHistoryColumnar) {
                        // Optimized Path: Use indices
                        clientHistoryIds.forEach(id => {
                            const idx = optimizedData.historyById.getIndex(id);
                            if (idx === undefined) return;

                            const codUsur = historyValues['CODUSUR'][idx];
                             // EXCEPTION: Exclude Balcão (53) sales for Client 9569 from Summary Metrics
                            if (String(codCli).trim() === '9569' && (String(codUsur).trim() === '53' || String(codUsur).trim() === '053')) return;

                            let key = null;
                            const codFor = String(historyValues['CODFOR'][idx]);

                            if (codFor === '707') key = '707';
                            else if (codFor === '708') key = '708';
                            else if (codFor === '752') key = '752';
                            else if (codFor === '1119') {
                                const desc = normalize(historyValues['DESCRICAO'][idx] || '');
                                if (desc.includes('TODDYNHO')) key = '1119_TODDYNHO';
                                else if (desc.includes('TODDY')) key = '1119_TODDY';
                                else if (desc.includes('QUAKER') || desc.includes('KEROCOCO')) key = '1119_QUAKER_KEROCOCO';
                            }

                            if (key && globalGoalsMetrics[key]) {
                                const dtPed = historyValues['DTPED'][idx];
                                const d = typeof dtPed === 'number' ? new Date(dtPed) : parseDate(dtPed);
                                const isPrevMonth = d && d.getUTCMonth() === prevMonthIndex && d.getUTCFullYear() === prevMonthYear;

                                // 1. Revenue/Volume metrics (Types 1 & 9) - Global Sums
                                const tipoVenda = historyValues['TIPOVENDA'][idx];
                                if (tipoVenda === '1' || tipoVenda === '9') {
                                    const vlVenda = Number(historyValues['VLVENDA'][idx]) || 0;
                                    const totPeso = Number(historyValues['TOTPESOLIQ'][idx]) || 0;

                                    globalGoalsMetrics[key].fat += vlVenda;
                                    globalGoalsMetrics[key].vol += totPeso;

                                    if (isPrevMonth) {
                                        globalGoalsMetrics[key].prevFat += vlVenda;
                                        globalGoalsMetrics[key].prevVol += totPeso;

                                        // Initialize Client Goal with Prev Month Value
                                        if (!globalClientGoals.has(codCli)) globalClientGoals.set(codCli, new Map());
                                        const cGoals = globalClientGoals.get(codCli);
                                        if (!cGoals.has(key)) cGoals.set(key, { fat: 0, vol: 0 });
                                        const g = cGoals.get(key);
                                        g.fat += vlVenda;
                                        g.vol += totPeso; // Kg
                                    }

                                    // 2. Accumulate for Client Count Check (Balance per period)
                                    if (d) {
                                    if (!clientTotals[key]) clientTotals[key] = { prevFat: 0, monthlyFat: new Map() };

                                    if (isPrevMonth) clientTotals[key].prevFat += vlVenda;

                                    const monthKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
                                    const currentMVal = clientTotals[key].monthlyFat.get(monthKey) || 0;
                                    clientTotals[key].monthlyFat.set(monthKey, currentMVal + vlVenda);
                                    }
                                }
                            }
                        });
                    } else {
                        // Fallback: Original Logic
                        clientHistoryIds.forEach(id => {
                            const sale = optimizedData.historyById.get(id);
                            // EXCEPTION: Exclude Balcão (53) sales for Client 9569 from Summary Metrics
                            if (String(codCli).trim() === '9569' && (String(sale.CODUSUR).trim() === '53' || String(sale.CODUSUR).trim() === '053')) return;

                            let key = null;
                            const codFor = String(sale.CODFOR);

                            if (codFor === '707') key = '707';
                            else if (codFor === '708') key = '708';
                            else if (codFor === '752') key = '752';
                            else if (codFor === '1119') {
                                const desc = normalize(sale.DESCRICAO || '');
                                if (desc.includes('TODDYNHO')) key = '1119_TODDYNHO';
                                else if (desc.includes('TODDY')) key = '1119_TODDY';
                                else if (desc.includes('QUAKER') || desc.includes('KEROCOCO')) key = '1119_QUAKER_KEROCOCO';
                            }

                            if (key && globalGoalsMetrics[key]) {
                                const d = parseDate(sale.DTPED);
                                const isPrevMonth = d && d.getUTCMonth() === prevMonthIndex && d.getUTCFullYear() === prevMonthYear;

                                // 1. Revenue/Volume metrics (Types 1 & 9) - Global Sums
                                if (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') {
                                    globalGoalsMetrics[key].fat += sale.VLVENDA;
                                    globalGoalsMetrics[key].vol += sale.TOTPESOLIQ;

                                    if (isPrevMonth) {
                                        globalGoalsMetrics[key].prevFat += sale.VLVENDA;
                                        globalGoalsMetrics[key].prevVol += sale.TOTPESOLIQ;

                                        // Initialize Client Goal with Prev Month Value
                                        if (!globalClientGoals.has(codCli)) globalClientGoals.set(codCli, new Map());
                                        const cGoals = globalClientGoals.get(codCli);
                                        if (!cGoals.has(key)) cGoals.set(key, { fat: 0, vol: 0 });
                                        const g = cGoals.get(key);
                                        g.fat += sale.VLVENDA;
                                        g.vol += sale.TOTPESOLIQ; // Kg
                                    }

                                    // 2. Accumulate for Client Count Check (Balance per period)
                                    if (d) {
                                    if (!clientTotals[key]) clientTotals[key] = { prevFat: 0, monthlyFat: new Map() };

                                    if (isPrevMonth) clientTotals[key].prevFat += sale.VLVENDA;

                                    const monthKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
                                    const currentMVal = clientTotals[key].monthlyFat.get(monthKey) || 0;
                                    clientTotals[key].monthlyFat.set(monthKey, currentMVal + sale.VLVENDA);
                                    }
                                }
                            }
                        });
                    }
                }

                // Check thresholds for this client
                for (const key in clientTotals) {
                    const t = clientTotals[key];
                    if (t.prevFat >= 1) {
                        globalGoalsMetrics[key].prevClientsSet.add(codCli);
                    }
                    t.monthlyFat.forEach((val, mKey) => {
                        if (val >= 1) {
                            if (!globalGoalsMetrics[key].monthlyClientsSets.has(mKey)) {
                                globalGoalsMetrics[key].monthlyClientsSets.set(mKey, new Set());
                            }
                            globalGoalsMetrics[key].monthlyClientsSets.get(mKey).add(codCli);
                        }
                    });
                }
            });

            // Calculate Averages and Finalize
            // First calculate basic metrics for leaf keys
            const leafKeys = ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];

            // Helper for aggregation
            const aggregateToAll = (targetKey, sourceKeys) => {
                const target = globalGoalsMetrics[targetKey];
                sourceKeys.forEach(key => {
                    const source = globalGoalsMetrics[key];
                    target.fat += source.fat;
                    target.vol += source.vol;
                    target.prevFat += source.prevFat;
                    target.prevVol += source.prevVol; // Already raw, keep raw for now

                    source.prevClientsSet.forEach(c => target.prevClientsSet.add(c));
                    source.quarterlyPosClientsSet.forEach(c => target.quarterlyPosClientsSet.add(c));

                    source.monthlyClientsSets.forEach((set, monthKey) => {
                        if (!target.monthlyClientsSets.has(monthKey)) {
                            target.monthlyClientsSets.set(monthKey, new Set());
                        }
                        const targetSet = target.monthlyClientsSets.get(monthKey);
                        set.forEach(c => targetSet.add(c));
                    });
                });
            };

            aggregateToAll('ELMA_ALL', ['707', '708', '752']);
            aggregateToAll('FOODS_ALL', ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO']);
            aggregateToAll('PEPSICO_ALL', ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO']);

            // Finalize calculations for ALL keys
            for (const key in globalGoalsMetrics) {
                const m = globalGoalsMetrics[key];

                m.avgFat = m.fat / QUARTERLY_DIVISOR;
                m.avgVol = m.vol / QUARTERLY_DIVISOR; // Kg (No / 1000)
                m.prevVol = m.prevVol; // Kg (No / 1000)

                m.prevClients = m.prevClientsSet.size;

                let sumClients = 0;
                m.monthlyClientsSets.forEach(set => sumClients += set.size);
                m.avgClients = sumClients / QUARTERLY_DIVISOR;
            }
        }
        let selectedGoalsGvSupervisors = [];
        let selectedGoalsGvSellers = [];
        let selectedGoalsSvSupervisors = [];
        let selectedGoalsSummarySupervisors = [];

        // let innovationsIncludeBonus = true; // REMOVED
        // let innovationsMonthIncludeBonus = true; // REMOVED

        let innovationsMonthTableDataForExport = [];
        let innovationsByClientForExport = [];
        let categoryLegendForExport = [];
        let chartLabels = [];
        let globalInnovationCategories = null;
        let globalProductToCategoryMap = null;

        let calendarState = { year: lastSaleDate.getUTCFullYear(), month: lastSaleDate.getUTCMonth() };

        let selectedCoverageSellers = [];
        let selectedCoverageSuppliers = [];
        let selectedCoverageSupervisors = [];
        let selectedCoverageProducts = [];
        let coverageUnitPriceFilter = null;
        let customWorkingDaysCoverage = 0;
        let coverageTrendFilter = 'all';
        let coverageTableDataForExport = [];
        let currentCoverageChartMode = 'city';

        const coverageTipoVendaFilterBtn = document.getElementById('coverage-tipo-venda-filter-btn');
        const coverageTipoVendaFilterText = document.getElementById('coverage-tipo-venda-filter-text');
        const coverageTipoVendaFilterDropdown = document.getElementById('coverage-tipo-venda-filter-dropdown');

        let mainTableState = {
            currentPage: 1,
            itemsPerPage: 50,
            filteredData: [],
            totalPages: 1
        };

        let mixTableState = {
            currentPage: 1,
            itemsPerPage: 100,
            filteredData: [],
            totalPages: 1
        };

        const getFirstName = (fullName) => (fullName || '').split(' ')[0];

        function formatDate(date) {
            if (!date) return '';
            const d = parseDate(date);
            if (!d || isNaN(d.getTime())) return '';
            const userTimezoneOffset = d.getTimezoneOffset() * 60000;
            return new Date(d.getTime() + userTimezoneOffset).toLocaleDateString('pt-BR');
        }

        function buildInnovationSalesMaps(salesData, mainTypes, bonusTypes) {
            const mainMap = new Map(); // Map<CODCLI, Map<PRODUTO, Set<CODUSUR>>>
            const bonusMap = new Map();
            const mainSet = new Set(mainTypes);
            const bonusSet = new Set(bonusTypes);

            salesData.forEach(sale => {
                const isMain = mainSet.has(sale.TIPOVENDA);
                const isBonus = bonusSet.has(sale.TIPOVENDA);

                if (!isMain && !isBonus) return;

                const codCli = sale.CODCLI;
                const prod = sale.PRODUTO;
                const rca = sale.CODUSUR;

                if (isMain) {
                    if (!mainMap.has(codCli)) mainMap.set(codCli, new Map());
                    const clientMap = mainMap.get(codCli);
                    if (!clientMap.has(prod)) clientMap.set(prod, new Set());
                    clientMap.get(prod).add(rca);
                }

                if (isBonus) {
                    if (!bonusMap.has(codCli)) bonusMap.set(codCli, new Map());
                    const clientMap = bonusMap.get(codCli);
                    if (!clientMap.has(prod)) clientMap.set(prod, new Set());
                    clientMap.get(prod).add(rca);
                }
            });
            return { mainMap, bonusMap };
        }

        // --- MIX VIEW LOGIC ---
        const MIX_SALTY_CATEGORIES = ['CHEETOS', 'DORITOS', 'FANDANGOS', 'RUFFLES', 'TORCIDA'];
        const MIX_FOODS_CATEGORIES = ['TODDYNHO', 'TODDY ', 'QUAKER', 'KEROCOCO'];

        function getMixFilteredData(options = {}) {
            const { excludeFilter = null } = options;

            const sellersSet = new Set(selectedMixSellers);
            const supervisorsSet = new Set(selectedMixSupervisors);
            const tiposVendaSet = new Set(selectedMixTiposVenda);
            const city = document.getElementById('mix-city-filter').value.trim().toLowerCase();
            const filial = document.getElementById('mix-filial-filter').value;

            let clients = allClientsData;

            if (excludeFilter !== 'rede') {
                 if (mixRedeGroupFilter === 'com_rede') {
                    clients = clients.filter(c => c.ramo && c.ramo !== 'N/A');
                    if (selectedMixRedes.length > 0) {
                        const redeSet = new Set(selectedMixRedes);
                        clients = clients.filter(c => redeSet.has(c.ramo));
                    }
                } else if (mixRedeGroupFilter === 'sem_rede') {
                    clients = clients.filter(c => !c.ramo || c.ramo === 'N/A');
                }
            }

            if (filial !== 'ambas') {
                clients = clients.filter(c => clientLastBranch.get(c['Código']) === filial);
            }

            if (excludeFilter !== 'supervisor' && selectedMixSupervisors.length > 0) {
                const rcasSet = new Set();
                selectedMixSupervisors.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => rcasSet.add(rca));
                });
                clients = clients.filter(c => c.rcas.some(r => rcasSet.has(r)));
            }

            if (excludeFilter !== 'seller' && sellersSet.size > 0) {
                const rcasSet = new Set();
                sellersSet.forEach(name => {
                    const code = optimizedData.rcaCodeByName.get(name);
                    if(code) rcasSet.add(code);
                });
                clients = clients.filter(c => c.rcas.some(r => rcasSet.has(r)));
            }

            if (excludeFilter !== 'supplier' && selectedCitySuppliers.length > 0) {
                 // No filtering of clients list based on supplier for now.
            }

            if (excludeFilter !== 'city' && city) {
                clients = clients.filter(c => c.cidade && c.cidade.toLowerCase() === city);
            }

            // Include only active or Americanas or not RCA 53
            clients = clients.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(c['Código']));
            });

            const clientCodes = new Set(clients.map(c => c['Código']));

            const filters = {
                supervisor: supervisorsSet,
                seller: sellersSet,
                city: city,
                filial: filial,
                tipoVenda: tiposVendaSet,
                clientCodes: clientCodes
            };

            const sales = getFilteredDataFromIndices(optimizedData.indices.current, optimizedData.salesById, filters, excludeFilter);

            return { clients, sales };
        }

        function updateAllMixFilters(options = {}) {
            const { skipFilter = null } = options;

            if (skipFilter !== 'supervisor') {
                const { sales } = getMixFilteredData({ excludeFilter: 'supervisor' });
                selectedMixSupervisors = updateSupervisorFilter(document.getElementById('mix-supervisor-filter-dropdown'), document.getElementById('mix-supervisor-filter-text'), selectedMixSupervisors, sales);
            }

            const { sales: salesSeller } = getMixFilteredData({ excludeFilter: 'seller' });
            selectedMixSellers = updateSellerFilter(selectedMixSupervisors, document.getElementById('mix-vendedor-filter-dropdown'), document.getElementById('mix-vendedor-filter-text'), selectedMixSellers, salesSeller, skipFilter === 'seller');

            const { sales: salesTV } = getMixFilteredData({ excludeFilter: 'tipoVenda' });
            selectedMixTiposVenda = updateTipoVendaFilter(document.getElementById('mix-tipo-venda-filter-dropdown'), document.getElementById('mix-tipo-venda-filter-text'), selectedMixTiposVenda, salesTV, skipFilter === 'tipoVenda');

            if (skipFilter !== 'rede') {
                 const { clients: clientsRede } = getMixFilteredData({ excludeFilter: 'rede' });
                 if (mixRedeGroupFilter === 'com_rede') {
                     selectedMixRedes = updateRedeFilter(document.getElementById('mix-rede-filter-dropdown'), document.getElementById('mix-com-rede-btn-text'), selectedMixRedes, clientsRede);
                 }
            }
        }

        function handleMixFilterChange(options = {}) {
            if (window.mixUpdateTimeout) clearTimeout(window.mixUpdateTimeout);
            window.mixUpdateTimeout = setTimeout(() => {
                updateAllMixFilters(options);
                updateMixView();
            }, 10);
        }

        function resetMixFilters() {
            selectedMixSupervisors = [];
            document.getElementById('mix-city-filter').value = '';
            document.getElementById('mix-filial-filter').value = 'ambas';
            selectedMixSellers = [];
            selectedMixTiposVenda = [];
            selectedMixRedes = [];
            mixRedeGroupFilter = '';

            const redeGroupContainer = document.getElementById('mix-rede-group-container');
            redeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            redeGroupContainer.querySelector('button[data-group=""]').classList.add('active');
            document.getElementById('mix-rede-filter-dropdown').classList.add('hidden');

            updateAllMixFilters();
            updateMixView();
        }

        function escapeHtml(text) {
            if (text === null || text === undefined) return '';
            return String(text)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        function updateMixView() {
            mixRenderId++;
            const currentRenderId = mixRenderId;

            const { clients, sales } = getMixFilteredData();
            // const activeClientCodes = new Set(clients.map(c => c['Código'])); // Not used if iterating clients array

            // Show Loading
            document.getElementById('mix-table-body').innerHTML = '<tr><td colspan="13" class="text-center p-8"><svg class="animate-spin h-8 w-8 text-teal-500 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg></td></tr>';

            // 1. Agregar Valor Líquido por Produto por Cliente (Sync - O(Sales))
            const clientProductNetValues = new Map(); // Map<CODCLI, Map<PRODUTO, NetValue>>
            const clientProductDesc = new Map(); // Map<PRODUTO, Descricao> (Cache)

            sales.forEach(s => {
                if (!s.CODCLI || !s.PRODUTO) return;

                if (!clientProductNetValues.has(s.CODCLI)) {
                    clientProductNetValues.set(s.CODCLI, new Map());
                }
                const clientMap = clientProductNetValues.get(s.CODCLI);
                const currentVal = clientMap.get(s.PRODUTO) || 0;
                clientMap.set(s.PRODUTO, currentVal + (Number(s.VLVENDA) || 0));

                if (!clientProductDesc.has(s.PRODUTO)) {
                    clientProductDesc.set(s.PRODUTO, s.DESCRICAO);
                }
            });

            // 2. Determinar Categorias Positivadas por Cliente
            // Uma categoria é positivada se o cliente comprou Pelo MENOS UM produto dela com valor líquido > 1
            const clientPositivatedCategories = new Map(); // Map<CODCLI, Set<CategoryName>>

            // Sync Loop for Map aggregation is fast enough
            clientProductNetValues.forEach((productsMap, codCli) => {
                const positivatedCats = new Set();

                productsMap.forEach((netValue, prodCode) => {
                    if (netValue >= 1) {
                        const desc = normalize(clientProductDesc.get(prodCode) || '');

                        // Checar Salty
                        MIX_SALTY_CATEGORIES.forEach(cat => {
                            if (desc.includes(cat)) positivatedCats.add(cat);
                        });
                        // Checar Foods
                        MIX_FOODS_CATEGORIES.forEach(cat => {
                            if (desc.includes(cat)) positivatedCats.add(cat);
                        });
                    }
                });
                clientPositivatedCategories.set(codCli, positivatedCats);
            });

            let positivadosSalty = 0;
            let positivadosFoods = 0;
            let positivadosBoth = 0;

            const tableData = [];

            // ASYNC CHUNKED PROCESSING for Clients
            runAsyncChunked(clients, (client) => {
                const codcli = client['Código'];
                const positivatedCats = clientPositivatedCategories.get(codcli) || new Set();

                // Determine Status based on "Buying ALL" (Strict Positive)
                const hasSalty = MIX_SALTY_CATEGORIES.every(b => positivatedCats.has(b));
                const hasFoods = MIX_FOODS_CATEGORIES.every(b => positivatedCats.has(b));

                if (hasSalty) positivadosSalty++;
                if (hasFoods) positivadosFoods++;
                if (hasSalty && hasFoods) positivadosBoth++;

                const missing = [];
                // Detailed missing analysis for Salty
                MIX_SALTY_CATEGORIES.forEach(b => { if(!positivatedCats.has(b)) missing.push(b); });
                // Detailed missing analysis for Foods
                MIX_FOODS_CATEGORIES.forEach(b => { if(!positivatedCats.has(b)) missing.push(b); });

                const missingText = missing.length > 0 ? missing.join(', ') : '';

                // Resolve Vendor Name
                const rcaCode = (client.rcas && client.rcas.length > 0) ? client.rcas[0] : null;
                let vendorName = 'N/A';
                if (rcaCode) {
                    vendorName = optimizedData.rcaNameByCode.get(rcaCode) || rcaCode;
                } else {
                    vendorName = 'INATIVOS';
                }

                const rowData = {
                    codcli: codcli,
                    name: client.fantasia || client.razaoSocial,
                    city: client.cidade || client.CIDADE || client['Nome da Cidade'] || 'N/A',
                    vendedor: vendorName,
                    hasSalty: hasSalty,
                    hasFoods: hasFoods,
                    brands: positivatedCats,
                    missingText: missingText,
                    score: missing.length
                };
                tableData.push(rowData);
            }, () => {
                // --- ON COMPLETE (Render) ---
                if (currentRenderId !== mixRenderId) return;

                let baseClientCount;
                const kpiTitleEl = document.getElementById('mix-kpi-title');

                if (mixKpiMode === 'atendidos') {
                    baseClientCount = getPositiveClientsWithNewLogic(sales);
                    if (kpiTitleEl) kpiTitleEl.textContent = 'Clientes Atendidos';
                } else {
                    baseClientCount = clients.length;
                    if (kpiTitleEl) kpiTitleEl.textContent = 'Total Clientes (Filtro)';
                }

                const saltyPct = baseClientCount > 0 ? (positivadosSalty / baseClientCount) * 100 : 0;
                const foodsPct = baseClientCount > 0 ? (positivadosFoods / baseClientCount) * 100 : 0;
                const bothPct = baseClientCount > 0 ? (positivadosBoth / baseClientCount) * 100 : 0;

                // Update KPIs
                document.getElementById('mix-total-clients-kpi').textContent = baseClientCount.toLocaleString('pt-BR');
                document.getElementById('mix-salty-kpi').textContent = `${saltyPct.toFixed(1)}%`;
                document.getElementById('mix-salty-count-kpi').textContent = `${positivadosSalty} clientes`;
                document.getElementById('mix-foods-kpi').textContent = `${foodsPct.toFixed(1)}%`;
                document.getElementById('mix-foods-count-kpi').textContent = `${positivadosFoods} clientes`;
                document.getElementById('mix-both-kpi').textContent = `${bothPct.toFixed(1)}%`;
                document.getElementById('mix-both-count-kpi').textContent = `${positivadosBoth} clientes`;

                // Charts
                const distributionData = [
                    positivadosBoth,
                    positivadosSalty - positivadosBoth,
                    positivadosFoods - positivadosBoth,
                    baseClientCount - (positivadosSalty + positivadosFoods - positivadosBoth)
                ];

                createChart('mixDistributionChart', 'doughnut', ['Mix Ideal (Ambos)', 'Só Salty', 'Só Foods', 'Nenhum'], distributionData, {
                    maintainAspectRatio: false, // Fix layout issue
                    backgroundColor: ['#a855f7', '#14b8a6', '#f59e0b', '#475569'],
                    plugins: { legend: { position: 'right' } }
                });

                // Seller Efficiency Chart
                const sellerStats = {};
                tableData.forEach(row => {
                    const seller = row.vendedor;
                    if (!sellerStats[seller]) sellerStats[seller] = { total: 0, both: 0, salty: 0, foods: 0 };
                    sellerStats[seller].total++;
                    if (row.hasSalty && row.hasFoods) sellerStats[seller].both++;
                    if (row.hasSalty) sellerStats[seller].salty++;
                    if (row.hasFoods) sellerStats[seller].foods++;
                });

                const sortedSellers = Object.entries(sellerStats)
                    .sort(([,a], [,b]) => b.both - a.both)
                    .slice(0, 10);

                createChart('mixSellerChart', 'bar', sortedSellers.map(([name]) => getFirstName(name)),
                    [
                        { label: 'Mix Ideal', data: sortedSellers.map(([,s]) => s.both), backgroundColor: '#a855f7' },
                        { label: 'Salty Total', data: sortedSellers.map(([,s]) => s.salty), backgroundColor: '#14b8a6', hidden: true },
                        { label: 'Foods Total', data: sortedSellers.map(([,s]) => s.foods), backgroundColor: '#f59e0b', hidden: true }
                    ],
                    { scales: { x: { stacked: false }, y: { stacked: false } } }
                );

                // Render Table with Detailed Columns
                tableData.sort((a, b) => {
                    // Sort by City (Alphabetical), then by Client Name
                    const cityA = (a.city || '').toLowerCase();
                    const cityB = (b.city || '').toLowerCase();
                    if (cityA < cityB) return -1;
                    if (cityA > cityB) return 1;
                    return (a.name || '').localeCompare(b.name || '');
                });

                mixTableDataForExport = tableData;

                mixTableState.filteredData = tableData;
                mixTableState.totalPages = Math.ceil(tableData.length / mixTableState.itemsPerPage);
                if (mixTableState.currentPage > mixTableState.totalPages && mixTableState.totalPages > 0) {
                    mixTableState.currentPage = mixTableState.totalPages;
                } else if (mixTableState.totalPages === 0) {
                     mixTableState.currentPage = 1;
                }

                const startIndex = (mixTableState.currentPage - 1) * mixTableState.itemsPerPage;
                const endIndex = startIndex + mixTableState.itemsPerPage;
                const pageData = tableData.slice(startIndex, endIndex);

                const checkIcon = `<svg class="w-4 h-4 text-green-400 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>`;
                const dashIcon = `<span class="text-slate-600 text-xs">-</span>`;

                const xIcon = `<svg class="w-3 h-3 text-red-500 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>`;

                let tableHTML = pageData.map(row => {
                    let saltyCols = MIX_SALTY_CATEGORIES.map(b => `<td class="px-1 py-2 text-center border-l border-slate-500">${row.brands.has(b) ? checkIcon : xIcon}</td>`).join('');
                    let foodsCols = MIX_FOODS_CATEGORIES.map(b => `<td class="px-1 py-2 text-center border-l border-slate-500">${row.brands.has(b) ? checkIcon : xIcon}</td>`).join('');

                    return `
                    <tr class="hover:bg-slate-700/50 border-b border-slate-500 last:border-0">
                        <td class="px-2 py-2 font-medium text-slate-300 text-xs">${escapeHtml(row.codcli)}</td>
                        <td class="px-2 py-2 text-xs truncate max-w-[150px]" title="${escapeHtml(row.name)}">${escapeHtml(row.name)}</td>
                        <td class="px-2 py-2 text-xs text-slate-300 truncate max-w-[100px]">${escapeHtml(row.city)}</td>
                        <td class="px-2 py-2 text-xs text-slate-400 truncate max-w-[100px]">${escapeHtml(getFirstName(row.vendedor))}</td>
                        ${saltyCols}
                        ${foodsCols}
                    </tr>
                `}).join('');

                // Append Footer with Totals
                tableHTML += `
                    <tr class="bg-slate-800 font-bold border-t-2 border-slate-500 text-xs sticky bottom-0 z-20">
                        <td colspan="4" class="px-2 py-3 text-right text-white">TOTAL POSITIVADOS:</td>
                        <td colspan="${MIX_SALTY_CATEGORIES.length}" class="px-2 py-3 text-center text-teal-400 text-sm border-l border-slate-500">${positivadosSalty}</td>
                        <td colspan="${MIX_FOODS_CATEGORIES.length}" class="px-2 py-3 text-center text-yellow-400 text-sm border-l border-slate-500">${positivadosFoods}</td>
                    </tr>
                `;

                document.getElementById('mix-table-body').innerHTML = tableHTML;

                const controls = document.getElementById('mix-pagination-controls');
                const infoText = document.getElementById('mix-page-info-text');
                const prevBtn = document.getElementById('mix-prev-page-btn');
                const nextBtn = document.getElementById('mix-next-page-btn');

                if (tableData.length > 0 && mixTableState.totalPages > 1) {
                    infoText.textContent = `Página ${mixTableState.currentPage} de ${mixTableState.totalPages} (Total: ${tableData.length} clientes)`;
                    prevBtn.disabled = mixTableState.currentPage === 1;
                    nextBtn.disabled = mixTableState.currentPage === mixTableState.totalPages;
                    controls.classList.remove('hidden');
                } else {
                    controls.classList.add('hidden');
                }
            }, () => currentRenderId !== mixRenderId);
        }

        async function exportMixPDF() {
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF('landscape');

            const supervisor = document.getElementById('mix-supervisor-filter-text').textContent;
            const vendedor = document.getElementById('mix-vendedor-filter-text').textContent;
            const city = document.getElementById('mix-city-filter').value.trim();
            const generationDate = new Date().toLocaleString('pt-BR');

            doc.setFontSize(18);
            doc.text('Relatório de Detalhado - Mix Salty & Foods', 14, 22);
            doc.setFontSize(10);
            doc.setTextColor(10);
            doc.text(`Data de Emissão: ${generationDate}`, 14, 30);
            doc.text(`Filtros: Supervisor: ${supervisor} | Vendedor: ${vendedor} | Cidade: ${city || 'Todas'}`, 14, 36);

            // Determine dynamic columns
            const saltyCols = MIX_SALTY_CATEGORIES.map(c => c.substring(0, 8)); // Truncate headers
            const foodsCols = MIX_FOODS_CATEGORIES.map(c => c.substring(0, 8));

            const head = [['Cód', 'Cliente', 'Cidade', 'Vendedor', ...saltyCols, ...foodsCols]];

            const body = mixTableDataForExport.map(row => {
                const saltyCells = MIX_SALTY_CATEGORIES.map(b => row.brands.has(b) ? 'OK' : 'X');
                const foodsCells = MIX_FOODS_CATEGORIES.map(b => row.brands.has(b) ? 'OK' : 'X');
                return [
                    row.codcli,
                    row.name,
                    row.city || '',
                    getFirstName(row.vendedor),
                    ...saltyCells,
                    ...foodsCells
                ];
            });

            // Calculate Totals for Footer
            let totalSalty = 0;
            let totalFoods = 0;
            mixTableDataForExport.forEach(row => {
                if(row.hasSalty) totalSalty++;
                if(row.hasFoods) totalFoods++;
            });

            const footerRow = [
                { content: 'TOTAL POSITIVADOS:', colSpan: 4, styles: { halign: 'right', fontStyle: 'bold', fontSize: 12, textColor: [255, 255, 255], fillColor: [50, 50, 50] } },
                { content: String(totalSalty), colSpan: MIX_SALTY_CATEGORIES.length, styles: { halign: 'center', fontStyle: 'bold', fontSize: 12, textColor: [45, 212, 191], fillColor: [50, 50, 50] } }, // Teal-400
                { content: String(totalFoods), colSpan: MIX_FOODS_CATEGORIES.length, styles: { halign: 'center', fontStyle: 'bold', fontSize: 12, textColor: [250, 204, 21], fillColor: [50, 50, 50] } } // Yellow-400
            ];

            body.push(footerRow);

            doc.autoTable({
                head: head,
                body: body,
                startY: 45,
                theme: 'grid',
                styles: { fontSize: 6, cellPadding: 1, textColor: [0, 0, 0], halign: 'center' },
                headStyles: { fillColor: [20, 184, 166], textColor: 255, fontStyle: 'bold', fontSize: 8 },
                columnStyles: {
                    0: { halign: 'left', cellWidth: 15 },
                    1: { halign: 'left', cellWidth: 40 },
                    2: { halign: 'left', cellWidth: 25 },
                    3: { halign: 'left', cellWidth: 20 },
                },
                didParseCell: function(data) {
                    if (data.section === 'body') {
                        // Colorize OK/X cells
                        if (data.cell.raw === 'OK') {
                            data.cell.styles.textColor = [0, 128, 0]; // Stronger Green
                            data.cell.styles.fontStyle = 'bold';
                        }
                        if (data.cell.raw === 'X') {
                            data.cell.styles.textColor = [220, 0, 0]; // Stronger Red
                            data.cell.styles.fontStyle = 'bold';
                        }
                    }
                }
            });

            const pageCount = doc.internal.getNumberOfPages();
            for(let i = 1; i <= pageCount; i++) {
                doc.setPage(i);
                doc.setFontSize(9);
                doc.setTextColor(10);
                doc.text(`Página ${i} de ${pageCount}`, doc.internal.pageSize.width / 2, doc.internal.pageSize.height - 10, { align: 'center' });
            }

            let fileNameParam = 'geral';
            if (selectedMixSellers.length === 1) {
                fileNameParam = getFirstName(selectedMixSellers[0]);
            } else if (city) {
                fileNameParam = city;
            }
            const safeFileNameParam = fileNameParam.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            doc.save(`relatorio_mix_detalhado_${safeFileNameParam}_${new Date().toISOString().slice(0,10)}.pdf`);
        }

        // --- GOALS VIEW LOGIC ---

        function exportGoalsSvXLSX() {
            if (typeof XLSX === 'undefined') {
                alert("Erro: Biblioteca XLSX não carregada. Verifique sua conexão com a internet.");
                return;
            }

            if (!currentGoalsSvData || currentGoalsSvData.length === 0) {
                try { updateGoalsSvView(); } catch (e) { console.error(e); }
                if (!currentGoalsSvData || currentGoalsSvData.length === 0) {
                    alert("Sem dados para exportar.");
                    return;
                }
            }

            const wb = XLSX.utils.book_new();
            const ws_data = [];

            // Estilos
            const headerStyle = { font: { bold: true, color: { rgb: "FFFFFF" } }, fill: { fgColor: { rgb: "0F172A" } }, alignment: { horizontal: "center", vertical: "center" }, border: { bottom: { style: "thin", color: { rgb: "475569" } } } };
            const subHeaderStyle = { font: { bold: true, color: { rgb: "FFFFFF" } }, fill: { fgColor: { rgb: "1E293B" } }, alignment: { horizontal: "center", vertical: "center" }, border: { bottom: { style: "thin", color: { rgb: "334155" } } } };
            const editableStyle = { fill: { fgColor: { rgb: "FEF9C3" } }, border: { top: { style: "thin" }, bottom: { style: "thin" }, left: { style: "thin" }, right: { style: "thin" } } }; // Light Yellow
            const readOnlyStyle = { fill: { fgColor: { rgb: "F1F5F9" } } }; // Light Slate
            const totalRowStyle = { font: { bold: true, color: { rgb: "FFFFFF" } }, fill: { fgColor: { rgb: "334155" } }, border: { top: { style: "thick" } } };
            const grandTotalStyle = { font: { bold: true, color: { rgb: "FFFFFF" } }, fill: { fgColor: { rgb: "0F172A" } }, border: { top: { style: "thick" } } };

            // Format Strings
            const fmtMoney = "\"R$ \"#,##0.00";
            const fmtVol = "0.00 \"Kg\"";
            const fmtInt = "0";
            const fmtDec1 = "0.0";

            // Cores de Grupo
            const colorMap = {
                'total_elma': { fgColor: { rgb: "14B8A6" } }, // Teal
                'total_foods': { fgColor: { rgb: "F59E0B" } }, // Amber/Yellow
                'tonelada_elma': { fgColor: { rgb: "F97316" } }, // Orange
                'tonelada_foods': { fgColor: { rgb: "F97316" } },
                'mix_salty': { fgColor: { rgb: "14B8A6" } },
                'mix_foods': { fgColor: { rgb: "F59E0B" } },
                'geral': { fgColor: { rgb: "3B82F6" } }, // Blue
                'pedev': { fgColor: { rgb: "EC4899" } } // Pink
            };

            const createCell = (v, s = {}, z = null) => {
                const cell = { v, t: 'n' };
                if (z) {
                    cell.z = z;
                    cell.s = { ...s, numFmt: z };
                } else {
                    cell.s = s;
                }
                if (typeof v === 'string') cell.t = 's';
                return cell;
            };

            // --- 1. Headers ---
            const row1 = [createCell("CÓD", headerStyle), createCell("VENDEDOR", headerStyle)];
            const merges = [{ s: { r: 0, c: 0 }, e: { r: 2, c: 0 } }, { s: { r: 0, c: 1 }, e: { r: 2, c: 1 } }];
            let colIdx = 2;

            const svColumns = [
                { id: 'total_elma', label: 'TOTAL ELMA', type: 'standard', isAgg: true },
                { id: '707', label: 'EXTRUSADOS', type: 'standard' },
                { id: '708', label: 'NÃO EXTRUSADOS', type: 'standard' },
                { id: '752', label: 'TORCIDA', type: 'standard' },
                { id: 'tonelada_elma', label: 'KG ELMA', type: 'tonnage', isAgg: true },
                { id: 'mix_salty', label: 'MIX SALTY', type: 'mix', isAgg: true },
                { id: 'total_foods', label: 'TOTAL FOODS', type: 'standard', isAgg: true },
                { id: '1119_TODDYNHO', label: 'TODDYNHO', type: 'standard' },
                { id: '1119_TODDY', label: 'TODDY', type: 'standard' },
                { id: '1119_QUAKER_KEROCOCO', label: 'QUAKER / KEROCOCO', type: 'standard' },
                { id: 'tonelada_foods', label: 'KG FOODS', type: 'tonnage', isAgg: true },
                { id: 'mix_foods', label: 'MIX FOODS', type: 'mix', isAgg: true },
                { id: 'geral', label: 'GERAL', type: 'geral', isAgg: true },
                { id: 'pedev', label: 'AUDITORIA PEDEV', type: 'pedev', isAgg: true }
            ];

            const colMap = {};

            svColumns.forEach(col => {
                colMap[col.id] = colIdx;
                const style = { ...headerStyle };
                if (colorMap[col.id]) style.fill = colorMap[col.id]; // Apply Group Color

                row1.push(createCell(col.label, style));
                let span = 0;
                if (col.type === 'standard') span = 4;
                else if (col.type === 'tonnage' || col.type === 'mix') span = 3;
                else if (col.type === 'geral') span = 4;
                else if (col.type === 'pedev') span = 1;

                merges.push({ s: { r: 0, c: colIdx }, e: { r: 0, c: colIdx + span - 1 } });
                for (let k = 1; k < span; k++) row1.push(createCell("", style));
                colIdx += span;
            });
            ws_data.push(row1);

            // Row 2: Metric Names
            const row2 = [createCell("", headerStyle), createCell("", headerStyle)];
            svColumns.forEach(col => {
                const style = { ...subHeaderStyle, font: { bold: true, color: { rgb: "FFFFFF" } } };
                if (col.type === 'standard') {
                    row2.push(createCell("FATURAMENTO", style), createCell("", style), createCell("POSITIVAÇÃO", style), createCell("", style));
                    merges.push({ s: { r: 1, c: colMap[col.id] }, e: { r: 1, c: colMap[col.id] + 1 } });
                    merges.push({ s: { r: 1, c: colMap[col.id] + 2 }, e: { r: 1, c: colMap[col.id] + 3 } });
                } else if (col.type === 'tonnage') {
                    row2.push(createCell("MÉDIA TRIM.", style), createCell("META KG", style), createCell("", style));
                    merges.push({ s: { r: 1, c: colMap[col.id] + 1 }, e: { r: 1, c: colMap[col.id] + 2 } });
                } else if (col.type === 'mix') {
                    row2.push(createCell("MÉDIA TRIM.", style), createCell("META MIX", style), createCell("", style));
                    merges.push({ s: { r: 1, c: colMap[col.id] + 1 }, e: { r: 1, c: colMap[col.id] + 2 } });
                } else if (col.type === 'geral') {
                    row2.push(createCell("FATURAMENTO", style), createCell("", style), createCell("TONELADA", style), createCell("POSITIVAÇÃO", style));
                    merges.push({ s: { r: 1, c: colMap[col.id] }, e: { r: 1, c: colMap[col.id] + 1 } });
                } else {
                    row2.push(createCell("META", style));
                }
            });
            ws_data.push(row2);

            // Row 3: Subtitles
            const row3 = [createCell("", subHeaderStyle), createCell("", subHeaderStyle)];
            svColumns.forEach(col => {
                if (col.type === 'standard') {
                    row3.push(createCell("Meta", subHeaderStyle), createCell("Ajuste", subHeaderStyle), createCell("Meta", subHeaderStyle), createCell("Ajuste", subHeaderStyle));
                } else if (col.type === 'tonnage') {
                    row3.push(createCell("Volume", subHeaderStyle), createCell("Volume", subHeaderStyle), createCell("Ajuste", subHeaderStyle));
                } else if (col.type === 'mix') {
                    row3.push(createCell("Qtd", subHeaderStyle), createCell("Meta", subHeaderStyle), createCell("Ajuste", subHeaderStyle));
                } else if (col.type === 'geral') {
                    row3.push(createCell("Média Trim.", subHeaderStyle), createCell("Meta", subHeaderStyle), createCell("Meta", subHeaderStyle), createCell("Meta", subHeaderStyle));
                } else {
                    row3.push(createCell("", subHeaderStyle));
                }
            });
            ws_data.push(row3);

            // --- 2. Data Rows ---
            let currentRow = 3;
            const colCellsForGrandTotal = {};
            svColumns.forEach(c => colCellsForGrandTotal[c.id] = { fat: [], pos: [], vol: [], mix: [], avg: [] });

            currentGoalsSvData.forEach(sup => {
                const sellers = sup.sellers;
                const colCellsForSupTotal = {};
                svColumns.forEach(c => colCellsForSupTotal[c.id] = { fat: [], pos: [], vol: [], mix: [], avg: [] });

                sellers.forEach(seller => {
                    const rowData = [createCell(seller.code), createCell(seller.name)];

                    svColumns.forEach(col => {
                        const d = seller.data[col.id] || { metaFat: 0, metaVol: 0, metaPos: 0, avgVol: 0, avgMix: 0, metaMix: 0, avgFat: 0 };
                        const cIdx = colMap[col.id];
                        const excelRow = currentRow + 1;
                        const getColLet = (idx) => XLSX.utils.encode_col(idx);

                        // Highlight Logic
                        const isEditable = !col.isAgg; // Base columns are editable (Yellow)
                        const cellStyle = isEditable ? editableStyle : readOnlyStyle;
                        const aggCellStyle = readOnlyStyle; // Aggregated columns (Light Grey)

                        if (col.type === 'standard') {
                            rowData.push(createCell(d.metaFat, readOnlyStyle, fmtMoney));

                            // Formula for Aggregate Logic
                            if (col.id === 'total_elma' || col.id === 'total_foods') {
                                const ids = col.id === 'total_elma' ? ['707', '708', '752'] : ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                                const compCols = ids.map(id => colMap[id] + 1);
                                const compColsPos = ids.map(id => colMap[id] + 3);
                                const formulaFat = compCols.map(c => `${getColLet(c)}${excelRow}`).join("+");

                                rowData.push({ t: 'n', v: d.metaFat, f: formulaFat, s: { ...aggCellStyle, numFmt: fmtMoney }, z: fmtMoney });
                                rowData.push(createCell(d.metaPos, readOnlyStyle, fmtInt));
                                // Static value for Positivation (Unique Clients), no formula
                                rowData.push(createCell(d.metaPos, aggCellStyle, fmtInt));
                            } else {
                                // Editable Cells
                                rowData.push(createCell(d.metaFat, cellStyle, fmtMoney));
                                rowData.push(createCell(d.metaPos, readOnlyStyle, fmtInt));
                                rowData.push(createCell(d.metaPos, cellStyle, fmtInt));
                            }

                            colCellsForSupTotal[col.id].fat.push(`${getColLet(cIdx + 1)}${excelRow}`);
                            colCellsForSupTotal[col.id].pos.push(`${getColLet(cIdx + 3)}${excelRow}`);

                        } else if (col.type === 'tonnage') {
                            rowData.push(createCell(d.avgVol, readOnlyStyle, fmtVol));
                            rowData.push(createCell(d.metaVol, readOnlyStyle, fmtVol));
                            rowData.push(createCell(d.metaVol, isEditable ? cellStyle : aggCellStyle, fmtVol));
                            colCellsForSupTotal[col.id].vol.push(`${getColLet(cIdx + 2)}${excelRow}`);
                            colCellsForSupTotal[col.id].avg.push(`${getColLet(cIdx)}${excelRow}`);

                        } else if (col.type === 'mix') {
                            rowData.push(createCell(d.avgMix, readOnlyStyle, fmtDec1));
                            rowData.push(createCell(d.metaMix, readOnlyStyle, fmtInt));
                            rowData.push(createCell(d.metaMix, isEditable ? cellStyle : aggCellStyle, fmtInt));
                            colCellsForSupTotal[col.id].mix.push(`${getColLet(cIdx + 2)}${excelRow}`);
                            colCellsForSupTotal[col.id].avg.push(`${getColLet(cIdx)}${excelRow}`);

                        } else if (col.type === 'geral') {
                            const elmaIdx = colMap['total_elma'];
                            const foodsIdx = colMap['total_foods'];
                            const elmaTonIdx = colMap['tonelada_elma'];
                            const foodsTonIdx = colMap['tonelada_foods'];

                            const fFat = `${getColLet(elmaIdx + 1)}${excelRow}+${getColLet(foodsIdx + 1)}${excelRow}`;
                            const fTon = `${getColLet(elmaTonIdx + 2)}${excelRow}+${getColLet(foodsTonIdx + 2)}${excelRow}`;

                            // REMOVED Formula for Positivation. Used static value instead.
                            // const fPos = `${getColLet(elmaIdx + 3)}${excelRow}+${getColLet(foodsIdx + 3)}${excelRow}`;

                            rowData.push(createCell(d.avgFat, readOnlyStyle, fmtMoney));
                            rowData.push({ t: 'n', v: d.metaFat, f: fFat, s: { ...aggCellStyle, numFmt: fmtMoney }, z: fmtMoney });
                            rowData.push({ t: 'n', v: d.metaVol, f: fTon, s: { ...aggCellStyle, numFmt: fmtVol }, z: fmtVol });
                            // Use static adjusted value for Positivation (PEPSICO_ALL)
                            rowData.push(createCell(d.metaPos, aggCellStyle, fmtInt));

                            colCellsForSupTotal[col.id].fat.push(`${getColLet(cIdx + 1)}${excelRow}`);
                            colCellsForSupTotal[col.id].vol.push(`${getColLet(cIdx + 2)}${excelRow}`);
                            colCellsForSupTotal[col.id].pos.push(`${getColLet(cIdx + 3)}${excelRow}`);
                            colCellsForSupTotal[col.id].avg.push(`${getColLet(cIdx)}${excelRow}`);

                        } else if (col.type === 'pedev') {
                            const elmaIdx = colMap['total_elma'];
                            const fPedev = `ROUND(${getColLet(elmaIdx + 3)}${excelRow}*0.9, 0)`;
                            rowData.push({ t: 'n', v: d.metaPos, f: fPedev, s: { ...aggCellStyle, numFmt: fmtInt }, z: fmtInt });
                            colCellsForSupTotal[col.id].pos.push(`${getColLet(cIdx)}${excelRow}`);
                        }
                    });
                    ws_data.push(rowData);
                    currentRow++;
                });

                // Supervisor Total Row
                const supRowData = [createCell(sup.code, totalRowStyle), createCell(sup.name.toUpperCase(), totalRowStyle)];
                const excelSupRow = currentRow + 1;

                svColumns.forEach(col => {
                    const cIdx = colMap[col.id];
                    const getColLet = (idx) => XLSX.utils.encode_col(idx);

                    if (col.type === 'standard') {
                        const rangeFat = colCellsForSupTotal[col.id].fat;
                        const fFatRange = rangeFat.length > 0 ? `SUM(${rangeFat[0]}:${rangeFat[rangeFat.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fFatRange, s: { ...totalRowStyle, numFmt: fmtMoney }, z: fmtMoney });
                        supRowData.push({ t: 'n', v: 0, f: fFatRange, s: { ...totalRowStyle, numFmt: fmtMoney }, z: fmtMoney });

                        const rangePos = colCellsForSupTotal[col.id].pos;
                        const fPosRange = rangePos.length > 0 ? `SUM(${rangePos[0]}:${rangePos[rangePos.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fPosRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });
                        supRowData.push({ t: 'n', v: 0, f: fPosRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });

                        colCellsForGrandTotal[col.id].fat.push(`${getColLet(cIdx+1)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].pos.push(`${getColLet(cIdx+3)}${excelSupRow}`);

                    } else if (col.type === 'tonnage') {
                        const rangeAvg = colCellsForSupTotal[col.id].avg;
                        const fAvgRange = rangeAvg.length > 0 ? `SUM(${rangeAvg[0]}:${rangeAvg[rangeAvg.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fAvgRange, s: { ...totalRowStyle, numFmt: fmtVol }, z: fmtVol });

                        const rangeVol = colCellsForSupTotal[col.id].vol;
                        const fVolRange = rangeVol.length > 0 ? `SUM(${rangeVol[0]}:${rangeVol[rangeVol.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fVolRange, s: { ...totalRowStyle, numFmt: fmtVol }, z: fmtVol });
                        supRowData.push({ t: 'n', v: 0, f: fVolRange, s: { ...totalRowStyle, numFmt: fmtVol }, z: fmtVol });

                        colCellsForGrandTotal[col.id].vol.push(`${getColLet(cIdx+2)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].avg.push(`${getColLet(cIdx)}${excelSupRow}`);

                    } else if (col.type === 'mix') {
                        const rangeAvg = colCellsForSupTotal[col.id].avg;
                        const fAvgRange = rangeAvg.length > 0 ? `SUM(${rangeAvg[0]}:${rangeAvg[rangeAvg.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fAvgRange, s: { ...totalRowStyle, numFmt: fmtDec1 }, z: fmtDec1 });

                        const rangeMix = colCellsForSupTotal[col.id].mix;
                        const fMixRange = rangeMix.length > 0 ? `SUM(${rangeMix[0]}:${rangeMix[rangeMix.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fMixRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });
                        supRowData.push({ t: 'n', v: 0, f: fMixRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });

                        colCellsForGrandTotal[col.id].mix.push(`${getColLet(cIdx+2)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].avg.push(`${getColLet(cIdx)}${excelSupRow}`);

                    } else if (col.type === 'geral') {
                        const rangeAvg = colCellsForSupTotal[col.id].avg;
                        const fAvgRange = rangeAvg.length > 0 ? `SUM(${rangeAvg[0]}:${rangeAvg[rangeAvg.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fAvgRange, s: { ...totalRowStyle, numFmt: fmtMoney }, z: fmtMoney });

                        const rangeFat = colCellsForSupTotal[col.id].fat;
                        const fFatRange = rangeFat.length > 0 ? `SUM(${rangeFat[0]}:${rangeFat[rangeFat.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fFatRange, s: { ...totalRowStyle, numFmt: fmtMoney }, z: fmtMoney });

                        const rangeVol = colCellsForSupTotal[col.id].vol;
                        const fVolRange = rangeVol.length > 0 ? `SUM(${rangeVol[0]}:${rangeVol[rangeVol.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fVolRange, s: { ...totalRowStyle, numFmt: fmtVol }, z: fmtVol });

                        const rangePos = colCellsForSupTotal[col.id].pos;
                        const fPosRange = rangePos.length > 0 ? `SUM(${rangePos[0]}:${rangePos[rangePos.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fPosRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });

                        colCellsForGrandTotal[col.id].fat.push(`${getColLet(cIdx+1)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].vol.push(`${getColLet(cIdx+2)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].pos.push(`${getColLet(cIdx+3)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].avg.push(`${getColLet(cIdx)}${excelSupRow}`);

                    } else if (col.type === 'pedev') {
                        const elmaIdx = colMap['total_elma'];
                        const fPedev = `ROUND(${getColLet(elmaIdx + 3)}${excelSupRow}*0.9, 0)`;
                        supRowData.push({ t: 'n', v: 0, f: fPedev, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });
                        colCellsForGrandTotal[col.id].pos.push(`${getColLet(cIdx)}${excelSupRow}`);
                    }
                });

                ws_data.push(supRowData);
                currentRow++;
            });

            // Grand Total Row
            const grandRowData = [createCell("GV", grandTotalStyle), createCell("GERAL PRIME", grandTotalStyle)];
            svColumns.forEach(col => {
                if (col.type === 'standard') {
                    const rangeFat = colCellsForGrandTotal[col.id].fat;
                    const fFat = rangeFat.length > 0 ? rangeFat.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fFat, s: { ...grandTotalStyle, numFmt: fmtMoney }, z: fmtMoney });
                    grandRowData.push({ t: 'n', v: 0, f: fFat, s: { ...grandTotalStyle, numFmt: fmtMoney }, z: fmtMoney });

                    const rangePos = colCellsForGrandTotal[col.id].pos;
                    const fPos = rangePos.length > 0 ? rangePos.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fPos, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });
                    grandRowData.push({ t: 'n', v: 0, f: fPos, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });

                } else if (col.type === 'tonnage') {
                    const rangeAvg = colCellsForGrandTotal[col.id].avg;
                    const fAvg = rangeAvg.length > 0 ? rangeAvg.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fAvg, s: { ...grandTotalStyle, numFmt: fmtVol }, z: fmtVol });

                    const rangeVol = colCellsForGrandTotal[col.id].vol;
                    const fVol = rangeVol.length > 0 ? rangeVol.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fVol, s: { ...grandTotalStyle, numFmt: fmtVol }, z: fmtVol });
                    grandRowData.push({ t: 'n', v: 0, f: fVol, s: { ...grandTotalStyle, numFmt: fmtVol }, z: fmtVol });

                } else if (col.type === 'mix') {
                    const rangeAvg = colCellsForGrandTotal[col.id].avg;
                    const fAvg = rangeAvg.length > 0 ? rangeAvg.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fAvg, s: { ...grandTotalStyle, numFmt: fmtDec1 }, z: fmtDec1 });

                    const rangeMix = colCellsForGrandTotal[col.id].mix;
                    const fMix = rangeMix.length > 0 ? rangeMix.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fMix, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });
                    grandRowData.push({ t: 'n', v: 0, f: fMix, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });

                } else if (col.type === 'geral') {
                    const rangeAvg = colCellsForGrandTotal[col.id].avg;
                    const fAvg = rangeAvg.length > 0 ? rangeAvg.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fAvg, s: { ...grandTotalStyle, numFmt: fmtMoney }, z: fmtMoney });

                    const rangeFat = colCellsForGrandTotal[col.id].fat;
                    const fFat = rangeFat.length > 0 ? rangeFat.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fFat, s: { ...grandTotalStyle, numFmt: fmtMoney }, z: fmtMoney });

                    const rangeVol = colCellsForGrandTotal[col.id].vol;
                    const fVol = rangeVol.length > 0 ? rangeVol.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fVol, s: { ...grandTotalStyle, numFmt: fmtVol }, z: fmtVol });

                    const rangePos = colCellsForGrandTotal[col.id].pos;
                    const fPos = rangePos.length > 0 ? rangePos.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fPos, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });

                } else if (col.type === 'pedev') {
                    const rangePos = colCellsForGrandTotal[col.id].pos;
                    const fPos = rangePos.length > 0 ? rangePos.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fPos, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });
                }
            });
            ws_data.push(grandRowData);

            // Create Sheet
            const ws = XLSX.utils.aoa_to_sheet(ws_data);
            ws['!merges'] = merges;

            // Auto-width
            const wscols = [{ wch: 10 }, { wch: 20 }];
            for(let i = 2; i < 50; i++) wscols.push({ wch: 12 });
            ws['!cols'] = wscols;

            // Add Sheet to Workbook
            XLSX.utils.book_append_sheet(wb, ws, "Metas SV");
            XLSX.writeFile(wb, "Metas_Fechamento_SV.xlsx");
        }

        function isActiveClient(c) {
            const rca1 = String(c.rca1 || '').trim();
            const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
            if (isAmericanas) return true;
            // STRICT FILTER: Exclude RCA 53 (Balcão) and INATIVOS
            if (rca1 === '53') return false;
            if (rca1 === '') return false; // Exclude INATIVOS
            return true;
        }

        function getGoalsFilteredData() {
            const sellersSet = new Set(selectedGoalsGvSellers);
            const supervisorsSet = new Set(selectedGoalsGvSupervisors);
            const codCli = goalsGvCodcliFilter.value.trim();

            let clients = allClientsData;

            // Apply "Active" Filter logic
            clients = clients.filter(c => isActiveClient(c));

            // Filter by Supervisor
            if (supervisorsSet.size > 0) {
                const rcasSet = new Set();
                supervisorsSet.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => rcasSet.add(rca));
                });
                clients = clients.filter(c => {
                    const clientRcas = (c.rcas && Array.isArray(c.rcas)) ? c.rcas : [];
                    return clientRcas.some(r => rcasSet.has(r));
                });
            }

            // Filter by Seller
            if (sellersSet.size > 0) {
                const rcasSet = new Set();
                sellersSet.forEach(name => {
                    const code = optimizedData.rcaCodeByName.get(name);
                    if(code) rcasSet.add(code);
                });
                clients = clients.filter(c => {
                    const clientRcas = (c.rcas && Array.isArray(c.rcas)) ? c.rcas : [];
                    return clientRcas.some(r => rcasSet.has(r));
                });
            }

            // Filter by Client Code
            if (codCli) {
                clients = clients.filter(c => String(c['Código']) === codCli);
            }

            return clients;
        }

        function parseInputMoney(id) {
            const el = document.getElementById(id);
            if (!el) return 0;
            let val = el.value.replace(/\./g, '').replace(',', '.');
            return parseFloat(val) || 0;
        }

        function getMetricsForSupervisors(supervisorsList) {
            // Helper to init metrics structure
            const createMetric = () => ({
                fat: 0, vol: 0, prevFat: 0, prevVol: 0,
                prevClientsSet: new Set(),
                quarterlyPosClientsSet: new Set(), // New Set for Quarter Active
                monthlyClientsSets: new Map() // Map<MonthKey, Set<CodCli>>
            });

            const metricsMap = {
                '707': createMetric(),
                '708': createMetric(),
                '752': createMetric(),
                '1119_TODDYNHO': createMetric(),
                '1119_TODDY': createMetric(),
                '1119_QUAKER_KEROCOCO': createMetric(),
                'ELMA_ALL': createMetric(),
                'FOODS_ALL': createMetric(),
                'PEPSICO_ALL': createMetric()
            };

            const currentDate = lastSaleDate;
            const prevMonthDate = new Date(Date.UTC(currentDate.getUTCFullYear(), currentDate.getUTCMonth() - 1, 1));
            const prevMonthIndex = prevMonthDate.getUTCMonth();
            const prevMonthYear = prevMonthDate.getUTCFullYear();

            // Filter clients to match the "Active Structure" definition (Same as Coverage/Goals Table)
            let activeClients = allClientsData.filter(c => isActiveClient(c));

            // Filter by Supervisors if provided
            if (supervisorsList && supervisorsList.length > 0) {
                const supervisorsSet = new Set(supervisorsList);
                const rcasSet = new Set();
                supervisorsSet.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => rcasSet.add(rca));
                });
                activeClients = activeClients.filter(c => c.rcas.some(r => rcasSet.has(r)));
            }

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const clientHistoryIds = optimizedData.indices.history.byClient.get(codCli);

                // Temp accumulation for this client to ensure Positive Balance check
                const clientTotals = {}; // key -> { prevFat: 0, monthlyFat: Map<MonthKey, val> }

                if (clientHistoryIds) {
                    clientHistoryIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        // EXCEPTION: Exclude Balcão (53) sales for Client 9569 from Summary Metrics
                        if (String(codCli).trim() === '9569' && (String(sale.CODUSUR).trim() === '53' || String(sale.CODUSUR).trim() === '053')) return;

                        let key = null;
                        const codFor = String(sale.CODFOR);

                        if (codFor === '707') key = '707';
                        else if (codFor === '708') key = '708';
                        else if (codFor === '752') key = '752';
                        else if (codFor === '1119') {
                            const desc = normalize(sale.DESCRICAO || '');
                            if (desc.includes('TODDYNHO')) key = '1119_TODDYNHO';
                            else if (desc.includes('TODDY')) key = '1119_TODDY';
                            else if (desc.includes('QUAKER') || desc.includes('KEROCOCO')) key = '1119_QUAKER_KEROCOCO';
                        }

                        const keysToProcess = [];
                        if (key && metricsMap[key]) keysToProcess.push(key);

                        // Direct calculation for Groups to ensure correct Net Total logic (handling returns across brands)
                        if (['707', '708', '752'].includes(codFor)) keysToProcess.push('ELMA_ALL');
                        if (codFor === '1119') keysToProcess.push('FOODS_ALL');
                        if (['707', '708', '752', '1119'].includes(codFor)) keysToProcess.push('PEPSICO_ALL');

                        keysToProcess.forEach(procKey => {
                            const d = parseDate(sale.DTPED);
                            const isPrevMonth = d && d.getUTCMonth() === prevMonthIndex && d.getUTCFullYear() === prevMonthYear;

                            // 1. Revenue/Volume metrics (Types 1 & 9) - Global Sums
                            if (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') {
                                metricsMap[procKey].fat += sale.VLVENDA;
                                metricsMap[procKey].vol += sale.TOTPESOLIQ;

                                if (isPrevMonth) {
                                    metricsMap[procKey].prevFat += sale.VLVENDA;
                                    metricsMap[procKey].prevVol += sale.TOTPESOLIQ;
                                }

                                // 2. Accumulate for Client Count Check (Balance per period)
                                // Standardized Logic: Track GLOBAL FAT sum regardless of date validity for positivation
                                if (!clientTotals[procKey]) clientTotals[procKey] = { prevFat: 0, monthlyFat: new Map(), globalFat: 0 };
                                clientTotals[procKey].globalFat += sale.VLVENDA;

                                if (d) {
                                    if (isPrevMonth) clientTotals[procKey].prevFat += sale.VLVENDA;

                                    const monthKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
                                    const currentMVal = clientTotals[procKey].monthlyFat.get(monthKey) || 0;
                                    clientTotals[procKey].monthlyFat.set(monthKey, currentMVal + sale.VLVENDA);
                                }
                            }
                        });
                    });
                }

                // Check thresholds for this client
                for (const key in clientTotals) {
                    const t = clientTotals[key];

                    // Calculate Total Quarter Fat for this client/key to determine Meta Pos
                    // MODIFIED: Use globalFat (Sum of ALL sales, including invalid dates) to match PEPSICO Logic
                    if (t.globalFat >= 1) {
                        metricsMap[key].quarterlyPosClientsSet.add(codCli);
                    }

                    if (t.prevFat >= 1) {
                        metricsMap[key].prevClientsSet.add(codCli);
                    }
                    t.monthlyFat.forEach((val, mKey) => {
                        if (val >= 1) {
                            if (!metricsMap[key].monthlyClientsSets.has(mKey)) {
                                metricsMap[key].monthlyClientsSets.set(mKey, new Set());
                            }
                            metricsMap[key].monthlyClientsSets.get(mKey).add(codCli);
                        }
                    });
                }
            });

            // Calculate Averages and Finalize
            for (const key in metricsMap) {
                const m = metricsMap[key];

                m.avgFat = m.fat / QUARTERLY_DIVISOR;
                m.avgVol = m.vol / QUARTERLY_DIVISOR; // Kg
                m.prevVol = m.prevVol; // Kg

                m.prevClients = m.prevClientsSet.size;
                m.quarterlyPos = m.quarterlyPosClientsSet.size; // New Metric

                let sumClients = 0;
                m.monthlyClientsSets.forEach(set => sumClients += set.size);
                m.avgClients = sumClients / QUARTERLY_DIVISOR;
            }
            return metricsMap;
        }

        function updateGoalsSummaryView() {
            const container = document.getElementById('goals-summary-grid');
            if (!container) return;

            // Use the independent summary filter
            const displayMetrics = getMetricsForSupervisors(selectedGoalsSummarySupervisors);

            // Calculate Target Sums for Filtered Subset
            // 1. Identify clients matching the summary filter
            let filteredSummaryClients = allClientsData;

            // Apply "Active" Filter logic (Consistent with other Goal Views)
            filteredSummaryClients = filteredSummaryClients.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                if (isAmericanas) return true;
                // STRICT FILTER: Exclude RCA 53 (Balcão) and INATIVOS
                if (rca1 === '53') return false;
                if (rca1 === '') return false; // Exclude INATIVOS
                return true;
            });

            if (selectedGoalsSummarySupervisors.length > 0) {
                const supervisorsSet = new Set(selectedGoalsSummarySupervisors);
                const rcasSet = new Set();
                supervisorsSet.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => rcasSet.add(rca));
                });
                filteredSummaryClients = filteredSummaryClients.filter(c => c.rcas.some(r => rcasSet.has(r)));
            }

            // 2. Prepare Sets for fast lookup and Sum up goals
            const filteredSummaryClientCodes = new Set();
            const activeSellersInSummary = new Set();

            filteredSummaryClients.forEach(c => {
                filteredSummaryClientCodes.add(c['Código']);
                // Resolve Seller Name for Adjustment Filtering
                const rcaCode = String(c.rca1 || '').trim();
                if (rcaCode) {
                    const name = optimizedData.rcaNameByCode.get(rcaCode);
                    if (name) activeSellersInSummary.add(name);
                    else if (rcaCode === 'INATIVOS') activeSellersInSummary.add('INATIVOS');
                }
            });

            const summaryGoalsSums = {
                '707': { fat: 0, vol: 0 },
                '708': { fat: 0, vol: 0 },
                '752': { fat: 0, vol: 0 },
                '1119_TODDYNHO': { fat: 0, vol: 0 },
                '1119_TODDY': { fat: 0, vol: 0 },
                '1119_QUAKER_KEROCOCO': { fat: 0, vol: 0 }
            };

            // Calculate Base Total for Mix (Use ELMA_ALL metric with exclusion)
            const elmaTargetBase = getElmaTargetBase(displayMetrics, goalsPosAdjustments, activeSellersInSummary);

            filteredSummaryClients.forEach(c => {
                const codCli = c['Código'];
                if (globalClientGoals.has(codCli)) {
                    const cGoals = globalClientGoals.get(codCli);
                    cGoals.forEach((val, key) => {
                        if (summaryGoalsSums[key]) {
                            summaryGoalsSums[key].fat += val.fat;
                            summaryGoalsSums[key].vol += val.vol;
                        }
                    });
                }
            });

            const summaryItems = [
                { title: 'Extrusados', supplier: '707', brand: null, color: 'teal' },
                { title: 'Não Extrusados', supplier: '708', brand: null, color: 'blue' },
                { title: 'Torcida', supplier: '752', brand: null, color: 'purple' },
                { title: 'Toddynho', supplier: '1119', brand: 'TODDYNHO', color: 'orange' },
                { title: 'Toddy', supplier: '1119', brand: 'TODDY', color: 'amber' },
                { title: 'Quaker / Kerococo', supplier: '1119', brand: 'QUAKER_KEROCOCO', color: 'cyan' }
            ];

            let totalFat = 0;
            let totalVol = 0;
            const uniquePosClientsSet = new Set();

            const cardsHTML = summaryItems.map(item => {
                const key = item.supplier + (item.brand ? `_${item.brand}` : '');

                // Use calculated sums if filter is active, otherwise global targets?
                // Actually, if no filter is active, filteredSummaryClients = All Active, so the sum matches the global target.
                // So we can always use summaryGoalsSums.

                const target = summaryGoalsSums[key] || { fat: 0, vol: 0 };
                const metrics = displayMetrics[key] || { avgFat: 0, prevFat: 0 };

                // LOGIC CHANGE: If Distributed Goal is 0, display Previous Month (Suggestion)
                let displayFat = target.fat;
                let displayVol = target.vol;

                if (displayFat < 0.01) displayFat = metrics.prevFat;
                if (displayVol < 0.001) displayVol = metrics.prevVol;

                let subCategoryAdjustment = 0;
                if (goalsPosAdjustments[key]) {
                    // goalsPosAdjustments keys are Seller Names, not Client Codes
                    goalsPosAdjustments[key].forEach((adjVal, sellerName) => {
                        if (activeSellersInSummary.has(sellerName)) {
                            subCategoryAdjustment += adjVal;
                        }
                    });
                }

                totalFat += displayFat;
                totalVol += displayVol;

                if (metrics.quarterlyPosClientsSet) {
                    metrics.quarterlyPosClientsSet.forEach(clientCode => uniquePosClientsSet.add(clientCode));
                }

                // Color mapping for classes
                const colorMap = {
                    teal: 'border-teal-500 text-teal-400 bg-teal-900/10',
                    blue: 'border-blue-500 text-blue-400 bg-blue-900/10',
                    purple: 'border-purple-500 text-purple-400 bg-purple-900/10',
                    orange: 'border-orange-500 text-orange-400 bg-orange-900/10',
                    amber: 'border-amber-500 text-amber-400 bg-amber-900/10',
                    cyan: 'border-cyan-500 text-cyan-400 bg-cyan-900/10'
                };

                const styleClass = colorMap[item.color] || colorMap.teal;
                const textColor = styleClass.split(' ')[1];

                return `
                    <div class="bg-[#1e2a5a] border-l-4 ${styleClass.split(' ')[0]} rounded-r-lg p-4 shadow-md transition hover:-translate-y-1">
                        <h3 class="font-bold text-lg text-white mb-3 border-b border-slate-700 pb-2">${item.title}</h3>
                        <div class="space-y-4">
                            <div>
                                <div class="flex justify-between items-baseline mb-1">
                                    <p class="text-xs text-slate-300 uppercase font-semibold">Meta Faturamento</p>
                                </div>
                                <p class="text-xl font-bold ${textColor} mb-2">
                                    ${displayFat.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}
                                </p>
                                <div class="flex justify-between text-[10px] text-slate-300 border-t border-slate-700/50 pt-1">
                                    <span>Trim: <span class="text-slate-300">${metrics.avgFat.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</span></span>
                                    <span>Ant: <span class="text-slate-300">${metrics.prevFat.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</span></span>
                                </div>
                            </div>

                            <div>
                                <div class="flex justify-between items-baseline mb-1">
                                    <p class="text-xs text-slate-300 uppercase font-semibold">Meta Volume (Kg)</p>
                                </div>
                                <p class="text-xl font-bold ${textColor} mb-2">
                                    ${displayVol.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 })}
                                </p>
                                <div class="flex justify-between text-[10px] text-slate-300 border-t border-slate-700/50 pt-1">
                                    <span>Trim: <span class="text-slate-300">${metrics.avgVol.toLocaleString('pt-BR', { minimumFractionDigits: 3 })}</span></span>
                                    <span>Ant: <span class="text-slate-300">${metrics.prevVol.toLocaleString('pt-BR', { minimumFractionDigits: 3 })}</span></span>
                                </div>
                            </div>

                            <div>
                                <div class="flex justify-between items-baseline mb-1">
                                    <p class="text-xs text-slate-300 uppercase font-semibold">Meta Pos. (Clientes)</p>
                                </div>
                                <p class="text-xl font-bold ${textColor} mb-2">
                                    ${((metrics.quarterlyPos || 0) + subCategoryAdjustment).toLocaleString('pt-BR')}
                                </p>
                                <div class="flex justify-between text-[10px] text-slate-300 border-t border-slate-700/50 pt-1">
                                    <span>Ativos no Trimestre</span>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            }).join('');

            container.innerHTML = cardsHTML;

            // Update Totals
            const totalFatEl = document.getElementById('summary-total-fat');
            const totalVolEl = document.getElementById('summary-total-vol');
            const totalPosEl = document.getElementById('summary-total-pos');
            const mixSaltyEl = document.getElementById('summary-mix-salty');
            const mixFoodsEl = document.getElementById('summary-mix-foods');

            if(totalFatEl) totalFatEl.textContent = totalFat.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
            if(totalVolEl) totalVolEl.textContent = totalVol.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 });

            // Calculate Base Total (Use PEPSICO_ALL metric instead of Union)
            // basePosCount is already defined at the top of the function
            const basePosCount = displayMetrics['PEPSICO_ALL'].quarterlyPos;

            let totalAdjustment = 0;
            // Only PEPSICO adjustments affect the Global/Summary Total Pos
            if (goalsPosAdjustments['PEPSICO_ALL']) {
                goalsPosAdjustments['PEPSICO_ALL'].forEach((val, sellerName) => {
                    // Only include adjustment if seller is active in current summary view
                    if (activeSellersInSummary.has(sellerName)) {
                        totalAdjustment += val;
                    }
                });
            }

            const adjustedTotalPos = basePosCount + totalAdjustment;

            if(totalPosEl) totalPosEl.textContent = adjustedTotalPos.toLocaleString('pt-BR');

            // Calculate base for Mix Goals (Exclude Americanas / Seller 1001)
            let naturalMixBaseCount = 0;
            uniquePosClientsSet.forEach(clientCode => {
                const client = clientMapForKPIs.get(String(clientCode));
                if (client) {
                     const rca1 = String(client.rca1 || '').trim();
                     if (rca1 !== '1001') {
                         naturalMixBaseCount++;
                     }
                }
            });

            // MIX KPIs - Based on ELMA Target (50% Salty / 30% Foods)
            const naturalSaltyTarget = Math.round(elmaTargetBase * 0.50);

            let mixSaltyAdjustment = 0;
            if (goalsMixSaltyAdjustments['PEPSICO_ALL']) {
                 goalsMixSaltyAdjustments['PEPSICO_ALL'].forEach((val, sellerName) => {
                     // Check if seller is in the filtered summary view
                     if (activeSellersInSummary.has(sellerName)) mixSaltyAdjustment += val;
                 });
            }
            if(mixSaltyEl) mixSaltyEl.textContent = (naturalSaltyTarget + mixSaltyAdjustment).toLocaleString('pt-BR');

            // Mix Foods - Based on ELMA Target (30%)
            const naturalFoodsTarget = Math.round(elmaTargetBase * 0.30);
            let mixFoodsAdjustment = 0;
            if (goalsMixFoodsAdjustments['PEPSICO_ALL']) {
                 goalsMixFoodsAdjustments['PEPSICO_ALL'].forEach((val, sellerName) => {
                     if (activeSellersInSummary.has(sellerName)) mixFoodsAdjustment += val;
                 });
            }
            if(mixFoodsEl) mixFoodsEl.textContent = (naturalFoodsTarget + mixFoodsAdjustment).toLocaleString('pt-BR');
        }

        function getElmaTargetBase(displayMetrics, goalsPosAdjustments, activeSellersSet) {
            // MATCH LOGIC WITH "RELATÓRIO" (SV): Base is "Total ELMA" (707, 708, 752)
            // Logic derived from `updateGoalsSvView`:
            // - The Grand Total for Mix Salty/Foods EXCLUDES Americanas (Seller 1001) from the base.
            // - It INCLUDES normal clients.

            // 1. Iterate ALL valid clients (Active Structure)
            // 2. EXCLUDE Americanas (RCA 1001) for this specific KPI base (matches SV footer logic)
            // 3. Exclude Balcão (53) and Inativos
            // 4. Check if Client has > 1 Total Sales in History (Elma: 707, 708, 752)
            // 5. Match Active Sellers

            let naturalCount = 0;

            // Iterate all clients (global allClientsData)
            // Use standard loop for performance
            for (let i = 0; i < allClientsData.length; i++) {
                const client = allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i];
                const codCli = String(client['Código'] || client['codigo_cliente']);

                // 1. Exclusions (Structure)
                const rca1 = String(client.rca1 || '').trim();
                const isAmericanas = (client.razaoSocial || '').toUpperCase().includes('AMERICANAS');

                // Exclude Americanas (Specific Rule for Mix Base)
                if (rca1 === '1001' || isAmericanas) continue;

                // Exclude Balcão (53) and Inativos
                if (rca1 === '53' || rca1 === '') continue;

                // 2. Active Seller Check
                let belongsToActiveSeller = true;
                if (activeSellersSet && activeSellersSet.size > 0) {
                    let sellerName = 'N/A';
                    // In SV, we map rcas[0] to Name.
                    const rcaCode = (client.rcas && client.rcas.length > 0) ? client.rcas[0] : '';
                    if (rcaCode) {
                         sellerName = optimizedData.rcaNameByCode.get(rcaCode) || rcaCode;
                    } else {
                        sellerName = 'INATIVOS';
                    }

                    if (!activeSellersSet.has(sellerName)) belongsToActiveSeller = false;
                }

                if (!belongsToActiveSeller) continue;

                // 3. Check History (Positive ELMA: 707, 708, 752)
                const hIds = optimizedData.indices.history.byClient.get(codCli);
                let totalFat = 0;
                if (hIds) {
                    // hIds is Set<string> (id)
                    for (const id of hIds) {
                        const s = optimizedData.historyById.get(id);
                        const codFor = String(s.CODFOR);
                         if (['707', '708', '752'].includes(codFor)) {
                            if (s.TIPOVENDA === '1' || s.TIPOVENDA === '9') totalFat += s.VLVENDA;
                        }
                    }
                }

                if (totalFat >= 1) {
                    naturalCount++;
                }
            }

            // 2. Adjustments (Meta Pos) - Preserve Logic
            let adjustment = 0;
            const elmaAdj = goalsPosAdjustments['ELMA_ALL'];
            if (elmaAdj) {
                elmaAdj.forEach((val, sellerName) => {
                    // Check if seller is in current view (activeSellersSet)
                    if (!activeSellersSet || activeSellersSet.has(sellerName)) {
                        adjustment += val;
                    }
                });
            }

            return naturalCount + adjustment;
        }

                function calculateDistributedGoals(filteredClients, currentGoalsSupplier, currentGoalsBrand, goalFat, goalVol) {
            const cacheKey = currentGoalsSupplier + (currentGoalsBrand ? `_${currentGoalsBrand}` : '');

            if (quarterMonths.length === 0) identifyQuarterMonths();

            // Determine dates for Previous Month calc
            const currentDate = lastSaleDate;
            const prevMonthDate = new Date(Date.UTC(currentDate.getUTCFullYear(), currentDate.getUTCMonth() - 1, 1));
            const prevMonthIndex = prevMonthDate.getUTCMonth();
            const prevMonthYear = prevMonthDate.getUTCFullYear();

            // --- CÁLCULO DOS TOTAIS GLOBAIS (EMPRESA) PARA O FORNECEDOR/MARCA ATUAL ---
            let globalTotalAvgFat = 0;
            let globalTotalAvgVol = 0;

            const shouldIncludeSale = (sale, supplier, brand) => {
                const codFor = String(sale.CODFOR);
                if (supplier === 'PEPSICO_ALL') {
                    // Includes everything
                    if (!['707', '708', '752', '1119'].includes(codFor)) return false;
                } else if (supplier === 'ELMA_ALL') {
                    if (!['707', '708', '752'].includes(codFor)) return false;
                } else if (supplier === 'FOODS_ALL') {
                    // Include all brands of 1119 that are in sub-tabs
                    if (codFor !== '1119') return false;
                    // No brand filtering here, assuming 1119 contains mostly Foods
                } else {
                    if (codFor !== supplier) return false;
                    if (brand) {
                        const desc = normalize(sale.DESCRICAO || '');
                        if (brand === 'TODDYNHO') {
                            if (!desc.includes('TODDYNHO')) return false;
                        } else if (brand === 'TODDY') {
                            if (!desc.includes('TODDY') || desc.includes('TODDYNHO')) return false;
                        } else if (brand === 'QUAKER_KEROCOCO') {
                            if (!desc.includes('QUAKER') && !desc.includes('KEROCOCO')) return false;
                        }
                    }
                }
                return true;
            };

            if (globalGoalsTotalsCache[cacheKey]) {
                globalTotalAvgFat = globalGoalsTotalsCache[cacheKey].fat;
                globalTotalAvgVol = globalGoalsTotalsCache[cacheKey].vol;
            } else {
                const allActiveClients = allClientsData.filter(c => {
                    const rca1 = String(c.rca1 || '').trim();
                    const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                    if (isAmericanas) return true;
                // STRICT FILTER: Exclude RCA 53 (Balcão) and INATIVOS
                    if (rca1 === '53') return false;
                if (rca1 === '') return false; // Exclude INATIVOS
                    return true;
                });

                allActiveClients.forEach(client => {
                    const codCli = String(client['Código'] || client['codigo_cliente']);
                    const clientHistoryIds = optimizedData.indices.history.byClient.get(codCli);
                    if (clientHistoryIds) {
                        let sumFat = 0;
                        let sumVol = 0;
                        clientHistoryIds.forEach(id => {
                            const sale = optimizedData.historyById.get(id);
                            // EXCEPTION: Exclude Balcão (53) sales for Client 9569 from Global Portfolio Totals
                            if (String(codCli).trim() === '9569' && (String(sale.CODUSUR).trim() === '53' || String(sale.CODUSUR).trim() === '053')) return;

                            if (shouldIncludeSale(sale, currentGoalsSupplier, currentGoalsBrand)) {
                                if (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') {
                                    sumFat += sale.VLVENDA;
                                    sumVol += sale.TOTPESOLIQ;
                                }
                            }
                        });

                        // NEW LOGIC: Simple Average (Sum / 3) regardless of active months
                        globalTotalAvgFat += (sumFat / QUARTERLY_DIVISOR);
                        globalTotalAvgVol += (sumVol / QUARTERLY_DIVISOR); // Kg (No / 1000)
                    }
                });

                globalGoalsTotalsCache[cacheKey] = { fat: globalTotalAvgFat, vol: globalTotalAvgVol };
            }

            const clientMetrics = [];

            filteredClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const clientHistoryIds = optimizedData.indices.history.byClient.get(codCli);

                let sumFat = 0;
                let sumVol = 0;
                let prevFat = 0;
                let prevVol = 0;
                const monthlyActivity = new Map(); // MonthKey -> Fat

                // Initialize monthly values for breakdown
                const monthlyValues = {};
                quarterMonths.forEach(m => monthlyValues[m.key] = 0);

                if (clientHistoryIds) {
                    clientHistoryIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        // EXCEPTION: Exclude Balcão (53) sales for Client 9569 from Portfolio Average
                        if (String(codCli).trim() === '9569' && (String(sale.CODUSUR).trim() === '53' || String(sale.CODUSUR).trim() === '053')) return;

                        if (shouldIncludeSale(sale, currentGoalsSupplier, currentGoalsBrand)) {
                            if (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') {
                                sumFat += sale.VLVENDA;
                                sumVol += sale.TOTPESOLIQ;

                                const d = parseDate(sale.DTPED);
                                if (d) {
                                    // Previous Month Calc
                                    if (d.getUTCMonth() === prevMonthIndex && d.getUTCFullYear() === prevMonthYear) {
                                        prevFat += sale.VLVENDA;
                                        prevVol += sale.TOTPESOLIQ;
                                    }

                                    // Activity per Month Calc
                                    const monthKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
                                    monthlyActivity.set(monthKey, (monthlyActivity.get(monthKey) || 0) + sale.VLVENDA);

                                    if (monthlyValues.hasOwnProperty(monthKey)) {
                                        monthlyValues[monthKey] += sale.VLVENDA;
                                    }
                                }
                            }
                        }
                    });
                }

                // NEW LOGIC: Simple Average (Sum / 3) regardless of active months
                const avgFat = sumFat / QUARTERLY_DIVISOR;
                const avgVol = sumVol / QUARTERLY_DIVISOR; // Kg (No / 1000)

                let activeMonthsCount = 0;
                monthlyActivity.forEach(val => { if(val >= 1) activeMonthsCount++; });

                const isActivePrevMonth = prevFat >= 1 ? 1 : 0;

                let sellerName = 'N/A';
                const rcaCode = client.rcas[0];
                if (rcaCode) sellerName = optimizedData.rcaNameByCode.get(rcaCode) || rcaCode;
                else if (client.rcas.length === 0 || client.rcas[0] === '') sellerName = 'INATIVOS';

                // Retrieve Stored Goal
                let metaFat = 0;
                let metaVol = 0;

                if (currentGoalsSupplier === 'ELMA_ALL' || currentGoalsSupplier === 'FOODS_ALL' || currentGoalsSupplier === 'PEPSICO_ALL') {
                    if (globalClientGoals.has(codCli)) {
                        const cGoals = globalClientGoals.get(codCli);
                        let keysToSum = [];
                        if (currentGoalsSupplier === 'ELMA_ALL') keysToSum = ['707', '708', '752'];
                        else if (currentGoalsSupplier === 'FOODS_ALL') keysToSum = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                        else if (currentGoalsSupplier === 'PEPSICO_ALL') keysToSum = ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];

                        keysToSum.forEach(k => {
                            if (cGoals.has(k)) {
                                const g = cGoals.get(k);
                                metaFat += g.fat;
                                metaVol += g.vol;
                            }
                        });
                    }
                } else {
                    if (globalClientGoals.has(codCli)) {
                        const cGoals = globalClientGoals.get(codCli);
                        if (cGoals.has(cacheKey)) {
                            const g = cGoals.get(cacheKey);
                            metaFat = g.fat;
                            metaVol = g.vol;
                        }
                    }
                }

                const metaPos = (sumFat >= 1 && avgFat > 0) ? 1 : 0; // Positivado se venda >= 1 (threshold padrão)

                clientMetrics.push({
                    cod: codCli,
                    name: client.fantasia || client.razaoSocial,
                    seller: sellerName,
                    avgFat,
                    avgVol, // Now Kg
                    prevFat,
                    prevVol: prevVol, // Now Kg (removed / 1000)
                    activeMonthsCount,
                    isActivePrevMonth,
                    shareFat: (globalTotalAvgFat > 0 && avgFat > 0) ? (avgFat / globalTotalAvgFat) : 0,
                    shareVol: (globalTotalAvgVol > 0 && avgVol > 0) ? (avgVol / globalTotalAvgVol) : 0,
                    metaFat: metaFat,
                    metaVol: metaVol,
                    metaPos: metaPos,
                    monthlyBreakdown: monthlyValues
                });
            });

            // Calculate auto distribution if goal is set but individual goals are zero (first run)
            const totalShareFat = clientMetrics.reduce((sum, c) => sum + c.shareFat, 0);
            const totalShareVol = clientMetrics.reduce((sum, c) => sum + c.shareVol, 0);

            // If we have input goals, we can distribute them proportionally (Visual only, not saved unless clicked)
            // But here we just return the metrics. The view uses these metrics.

            return { clientMetrics, globalTotalAvgFat, globalTotalAvgVol };
        }

        function recalculateTotalGoals() {
            // Reset goalsTargets sums
            for (const key in goalsTargets) {
                goalsTargets[key] = { fat: 0, vol: 0 };
            }

            globalClientGoals.forEach((goalsMap, codCli) => {
                goalsMap.forEach((val, key) => {
                    if (goalsTargets[key]) {
                        goalsTargets[key].fat += val.fat;
                        goalsTargets[key].vol += val.vol;
                    }
                });
            });
        }

        function distributeGoals(type) {
            const inputId = type === 'fat' ? 'goal-global-fat' : 'goal-global-vol';
            const inputValue = parseInputMoney(inputId);

            const filteredClients = getGoalsFilteredData();
            if (filteredClients.length === 0) return;

            let keysToProcess = [];
            if (currentGoalsSupplier === 'PEPSICO_ALL') {
                keysToProcess = ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
            } else if (currentGoalsSupplier === 'ELMA_ALL') {
                keysToProcess = ['707', '708', '752'];
            } else if (currentGoalsSupplier === 'FOODS_ALL') {
                keysToProcess = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
            } else {
                const cacheKey = currentGoalsSupplier + (currentGoalsBrand ? `_${currentGoalsBrand}` : '');
                keysToProcess = [cacheKey];
            }

            // 1. Calculate Total Denominator (Sum of Averages of all target keys for all filtered clients)
            let totalDenominator = 0;
            const distributionMap = new Map(); // Map<ClientCod, Map<Key, AvgValue>>

            filteredClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const clientHistoryIds = optimizedData.indices.history.byClient.get(codCli);

                if (!distributionMap.has(codCli)) distributionMap.set(codCli, new Map());
                const clientMap = distributionMap.get(codCli);

                keysToProcess.forEach(targetKey => {
                    let sumVal = 0;
                    if (clientHistoryIds) {
                        clientHistoryIds.forEach(id => {
                            const sale = optimizedData.historyById.get(id);

                            // Check if sale belongs to targetKey
                            let saleKey = String(sale.CODFOR);
                            const codFor = String(sale.CODFOR);

                            // Special handling for broken down categories (FOODS)
                            if (codFor === '1119') {
                                const desc = normalize(sale.DESCRICAO || '');
                                if (desc.includes('TODDYNHO')) saleKey = '1119_TODDYNHO';
                                else if (desc.includes('TODDY')) saleKey = '1119_TODDY';
                                else if (desc.includes('QUAKER') || desc.includes('KEROCOCO')) saleKey = '1119_QUAKER_KEROCOCO';
                                else if (targetKey.startsWith('1119_')) saleKey = null; // If targeting a sub-brand but this product doesn't match, exclude it
                            }

                            if (saleKey === targetKey) {
                                if (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') {
                                    if (type === 'fat') sumVal += sale.VLVENDA;
                                    else sumVal += sale.TOTPESOLIQ;
                                }
                            }
                        });
                    }

                    // Apply divisor
                    let avg = sumVal / QUARTERLY_DIVISOR;
                    if (type === 'vol') avg = avg / 1000; // Tons

                    clientMap.set(targetKey, avg);
                    totalDenominator += avg;
                });
            });

            // 2. Distribute
            filteredClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const clientMap = distributionMap.get(codCli);

                if (!globalClientGoals.has(codCli)) globalClientGoals.set(codCli, new Map());
                const cGoals = globalClientGoals.get(codCli);

                keysToProcess.forEach(key => {
                    const avg = clientMap.get(key) || 0;
                    let share = totalDenominator > 0 ? (avg / totalDenominator) : 0;

                    if (totalDenominator === 0) {
                         const totalItems = filteredClients.length * keysToProcess.length;
                         if (totalItems > 0) share = 1 / totalItems;
                    }

                    const newGoal = share * inputValue;

                    if (!cGoals.has(key)) cGoals.set(key, { fat: 0, vol: 0 });
                    const g = cGoals.get(key);

                    if (type === 'fat') g.fat = newGoal;
                    else g.vol = newGoal;
                });
            });

            recalculateTotalGoals();
            updateGoalsView();
        }

        function showConfirmationModal(message, onConfirm) {
            const modal = document.getElementById('confirmation-modal');
            const msgEl = document.getElementById('confirmation-message');
            const confirmBtn = document.getElementById('confirmation-confirm-btn');
            const cancelBtn = document.getElementById('confirmation-cancel-btn');

            msgEl.textContent = message;
            modal.classList.remove('hidden');

            // Clean up old listeners to avoid duplicates
            const newConfirmBtn = confirmBtn.cloneNode(true);
            const newCancelBtn = cancelBtn.cloneNode(true);
            confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);
            cancelBtn.parentNode.replaceChild(newCancelBtn, cancelBtn);

            newConfirmBtn.addEventListener('click', () => {
                modal.classList.add('hidden');
                onConfirm();
            });

            newCancelBtn.addEventListener('click', () => {
                modal.classList.add('hidden');
            });
        }

        function getFilterDescription() {
            if (selectedGoalsGvSupervisors.length > 0) {
                return selectedGoalsGvSupervisors.length === 1 ? `Supervisor "${selectedGoalsGvSupervisors[0]}"` : "Supervisores selecionados";
            }
            if (selectedGoalsGvSellers.length > 0) {
                return selectedGoalsGvSellers.length === 1 ? `Vendedor "${getFirstName(selectedGoalsGvSellers[0])}"` : "Vendedores selecionados";
            }
            if (goalsGvCodcliFilter.value) {
                return `Cliente "${goalsGvCodcliFilter.value}"`;
            }

            // Default to Tab Name
            if (currentGoalsSupplier === '707') return 'EXTRUSADOS';
            if (currentGoalsSupplier === '708') return 'NÃO EXTRUSADOS';
            if (currentGoalsSupplier === '752') return 'TORCIDA';
            if (currentGoalsBrand) return currentGoalsBrand;

            return 'filtro atual';
        }

        function saveMixAdjustment(type, value, sellerName) {
            // Find natural base for this seller based on ELMA metrics (excluding Americanas)
            const sellerCode = optimizedData.rcaCodeByName.get(sellerName);

            // Re-use logic for Active Clients counting
            const sellerClients = allClientsData.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                if (!sellerCode) return false;

                // Is client active check (Same as others)
                // Exclude Americanas explicitly from this calculation as per requirement
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                if (isAmericanas || (rca1 === '53' || rca1 === '053' || rca1 === '' || rca1 === 'INATIVOS')) return false;

                // Does client belong to seller? (Current Hierarchy)
                return c.rcas.includes(sellerCode);
            });

            let naturalCount = 0;
            // Count "Meta Pos" (Revenue > 1 in ELMA_ALL: 707, 708, 752) for these clients
            sellerClients.forEach(c => {
                const codCli = c['Código'];
                const hIds = optimizedData.indices.history.byClient.get(codCli);
                let sumFat = 0;
                if (hIds) {
                    hIds.forEach(id => {
                        const s = optimizedData.historyById.get(id);
                        if (['707', '708', '752'].includes(String(s.CODFOR))) {
                            if (s.TIPOVENDA === '1' || s.TIPOVENDA === '9') sumFat += s.VLVENDA;
                        }
                    });
                }
                if (sumFat >= 1) naturalCount++;
            });

            // Check if seller has specific adjustment for ELMA_ALL (Meta Pos)
            let adjustmentPos = 0;
            if (goalsPosAdjustments['ELMA_ALL'] && goalsPosAdjustments['ELMA_ALL'].has(sellerName)) {
                adjustmentPos = goalsPosAdjustments['ELMA_ALL'].get(sellerName);
            }

            // Base = Natural Elma Count + Elma Adjustment
            const totalElmaBase = naturalCount + adjustmentPos;

            // Apply 50% / 30% rule
            const base = type === 'salty' ? Math.round(totalElmaBase * 0.50) : Math.round(totalElmaBase * 0.30);
            const adjustment = value - base;

            // ALWAYS STORE IN PEPSICO_ALL (Unify Inputs)
            if (type === 'salty') {
                if (!goalsMixSaltyAdjustments['PEPSICO_ALL']) goalsMixSaltyAdjustments['PEPSICO_ALL'] = new Map();
                goalsMixSaltyAdjustments['PEPSICO_ALL'].set(sellerName, adjustment);
            } else {
                if (!goalsMixFoodsAdjustments['PEPSICO_ALL']) goalsMixFoodsAdjustments['PEPSICO_ALL'] = new Map();
                goalsMixFoodsAdjustments['PEPSICO_ALL'].set(sellerName, adjustment);
            }

            updateGoalsView();
        }


        function exportGoalsCurrentTabXLSX() {
            const data = goalsTableState.filteredData;
            if (!data || data.length === 0) {
                alert('Sem dados para exportar.');
                return;
            }

            const wb = XLSX.utils.book_new();

            // 1. Headers
            const monthLabels = quarterMonths.map(m => m.label);
            const flatHeaders = [
                'CÓD', 'CLIENTE', 'VENDEDOR',
                ...monthLabels.map(m => `${m} (FAT)`),
                'MÉDIA FAT', '% SHARE FAT', 'META FAT',
                'MÉDIA VOL (KG)', '% SHARE VOL', 'META VOL (KG)', 'META POS'
            ];

            const ws_data_flat = [flatHeaders];
             data.forEach(item => {
                const row = [
                    parseInt(item.cod),
                    item.name,
                    getFirstName(item.seller),
                    ...quarterMonths.map(m => item.monthlyValues[m.key] || 0),
                    item.avgFat,
                    item.shareFat,
                    item.metaFat,
                    item.avgVol,
                    item.shareVol,
                    item.metaVol,
                    item.metaPos
                ];
                ws_data_flat.push(row);
            });

            const ws_flat = XLSX.utils.aoa_to_sheet(ws_data_flat);

             // Style Header
            if (ws_flat['!ref']) {
                const range = XLSX.utils.decode_range(ws_flat['!ref']);
                for (let C = range.s.c; C <= range.e.c; ++C) {
                    const addr = XLSX.utils.encode_cell({ r: 0, c: C });
                    if (!ws_flat[addr]) continue;
                    if (!ws_flat[addr].s) ws_flat[addr].s = {};
                    ws_flat[addr].s.fill = { fgColor: { rgb: "1E293B" } };
                    ws_flat[addr].s.font = { color: { rgb: "FFFFFF" }, bold: true };
                    ws_flat[addr].s.alignment = { horizontal: "center" };
                }

                // Number formats
                for (let R = 1; R <= range.e.r; ++R) {
                     // Month Cols start at 3
                     const monthStart = 3;
                     const monthEnd = 3 + quarterMonths.length - 1;

                     for (let C = monthStart; C <= range.e.c; ++C) {
                          const addr = XLSX.utils.encode_cell({ r: R, c: C });
                          if (!ws_flat[addr]) continue;
                          if (!ws_flat[addr].s) ws_flat[addr].s = {};
                          ws_flat[addr].t = 'n';

                          // Percentages (Indices relative to monthEnd)
                          // Header: [COD, CLI, VEND, M1, M2, M3, AVG, SHARE, META, AVG_V, SHARE, META_V, POS]
                          // M3 is monthEnd.
                          // AVG is monthEnd+1
                          // SHARE is monthEnd+2
                          // META is monthEnd+3
                          // AVG_V is monthEnd+4
                          // SHARE_V is monthEnd+5
                          // META_V is monthEnd+6
                          // POS is monthEnd+7

                          if (C === monthEnd + 2 || C === monthEnd + 5) {
                              ws_flat[addr].z = '0.00%';
                          }
                          // Volumes (TON)
                          else if (C === monthEnd + 4 || C === monthEnd + 6) {
                              ws_flat[addr].z = '#,##0.000';
                          }
                          // Currency/Values
                          else if (C <= monthEnd + 3) {
                              ws_flat[addr].z = '#,##0.00';
                          }
                     }
                }

                ws_flat['!cols'] = [
                    { wch: 8 }, { wch: 35 }, { wch: 15 },
                    ...quarterMonths.map(_ => ({ wch: 15 })),
                    { wch: 15 }, { wch: 10 }, { wch: 15 },
                    { wch: 15 }, { wch: 10 }, { wch: 15 }, { wch: 8 }
                ];
            }

            XLSX.utils.book_append_sheet(wb, ws_flat, "Metas GV");
            XLSX.writeFile(wb, `Metas_GV_${currentGoalsSupplier}_${new Date().toISOString().slice(0,10)}.xlsx`);
        }

        function updateGoalsView() {
            goalsRenderId++;
            const currentRenderId = goalsRenderId;

            // Check if we are in Summary Mode
            if (document.getElementById('goals-summary-content') && !document.getElementById('goals-summary-content').classList.contains('hidden')) {
                updateGoalsSummaryView();
                return;
            }

            if (quarterMonths.length === 0) identifyQuarterMonths();

            // Calculate Metrics for Current View (Supervisor Filter)
            const displayMetrics = getMetricsForSupervisors(selectedGoalsGvSupervisors);

            // Update Header (Dynamic) - Same as before
            const thead = document.querySelector('#goals-table-container table thead');
            if (thead) {
                const monthHeaders = quarterMonths.map(m => `<th class="px-2 py-2 text-right w-20 bg-blue-900/10 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">${m.label}</th>`).join('');
                const monthsCount = quarterMonths.length;
                thead.innerHTML = `<tr><th rowspan="2" class="px-2 py-2 text-center w-16 border-r border-b border-slate-700">CÓD</th><th rowspan="2" class="px-3 py-2 text-left w-48 border-r border-b border-slate-700">CLIENTE</th><th rowspan="2" class="px-3 py-2 text-left w-24 border-r border-b border-slate-700">VENDEDOR</th><th colspan="${3 + monthsCount}" class="px-2 py-1 text-center bg-blue-900/30 text-blue-400 border-r border-slate-700 border-b-0">FATURAMENTO (R$)</th><th colspan="3" class="px-2 py-1 text-center bg-orange-900/30 text-orange-400 border-r border-slate-700 border-b-0">VOLUME (KG)</th><th rowspan="2" class="px-2 py-2 text-center w-16 bg-purple-900/20 text-purple-300 font-bold border-b border-slate-700">META POS.</th></tr><tr>${monthHeaders}<th class="px-2 py-2 text-right w-24 bg-blue-900/20 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">MÉDIA</th><th class="px-2 py-2 text-center w-16 bg-blue-900/20 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">% SHARE</th><th class="px-2 py-2 text-right w-24 bg-blue-900/20 text-blue-100 font-bold border-r border-b border-slate-700 text-[10px]">META AUTO</th><th class="px-2 py-2 text-right w-24 bg-orange-900/20 text-orange-300 border-r border-b border-slate-700/50 text-[10px]">MÉDIA KG</th><th class="px-2 py-2 text-center w-16 bg-orange-900/20 text-orange-300 border-r border-b border-slate-700/50 text-[10px]">% SHARE</th><th class="px-2 py-2 text-right w-24 bg-orange-900/20 text-orange-100 font-bold border-r border-b border-slate-700 text-[10px]">META KG</th></tr>`;
            }

            const filteredClients = getGoalsFilteredData();
            goalsGvTableBody.innerHTML = '<tr><td colspan="15" class="text-center p-8"><svg class="animate-spin h-8 w-8 text-teal-500 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg></td></tr>';

            // Cache Key for Global Totals
            const cacheKey = currentGoalsSupplier + (currentGoalsBrand ? `_${currentGoalsBrand}` : '');

            if (!globalGoalsTotalsCache[cacheKey]) {
                 calculateDistributedGoals([], currentGoalsSupplier, currentGoalsBrand, 0, 0);
            }

            const currentDate = lastSaleDate;
            const prevMonthDate = new Date(Date.UTC(currentDate.getUTCFullYear(), currentDate.getUTCMonth() - 1, 1));
            const prevMonthIndex = prevMonthDate.getUTCMonth();
            const prevMonthYear = prevMonthDate.getUTCFullYear();

            // Helper for inclusion check
            const shouldIncludeSale = (sale, supplier, brand) => {
                const codFor = String(sale.CODFOR);
                if (supplier === 'PEPSICO_ALL') { if (!['707', '708', '752', '1119'].includes(codFor)) return false; }
                else if (supplier === 'ELMA_ALL') { if (!['707', '708', '752'].includes(codFor)) return false; }
                else if (supplier === 'FOODS_ALL') { if (codFor !== '1119') return false; }
                else {
                    if (codFor !== supplier) return false;
                    if (brand) {
                        const desc = normalize(sale.DESCRICAO || '');
                        if (brand === 'TODDYNHO') { if (!desc.includes('TODDYNHO')) return false; }
                        else if (brand === 'TODDY') { if (!desc.includes('TODDY') || desc.includes('TODDYNHO')) return false; }
                        else if (brand === 'QUAKER_KEROCOCO') { if (!desc.includes('QUAKER') && !desc.includes('KEROCOCO')) return false; }
                    }
                }
                return true;
            };

            const globalTotalAvgFat = globalGoalsTotalsCache[cacheKey].fat;
            const globalTotalAvgVol = globalGoalsTotalsCache[cacheKey].vol;

            const clientMetrics = [];
            let sumFat = 0; let sumVol = 0;
            let totalAvgFat = 0; let totalPrevFat = 0; let totalAvgVol = 0; let totalPrevVol = 0; let sumActiveMonths = 0; let totalPrevClients = 0;

            runAsyncChunked(filteredClients, (client) => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const clientHistoryIds = optimizedData.indices.history.byClient.get(codCli);

                let cSumFat = 0; let cSumVol = 0; let cPrevFat = 0; let cPrevVol = 0;
                const monthlyActivity = new Map();
                const monthlyValues = {};
                quarterMonths.forEach(m => monthlyValues[m.key] = 0);

                if (clientHistoryIds) {
                    clientHistoryIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        // EXCEPTION: Exclude Balcão (53) sales for Client 9569 from Portfolio Average
                        if (String(codCli).trim() === '9569' && (String(sale.CODUSUR).trim() === '53' || String(sale.CODUSUR).trim() === '053')) return;

                        if (shouldIncludeSale(sale, currentGoalsSupplier, currentGoalsBrand)) {
                            if (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') {
                                cSumFat += sale.VLVENDA;
                                cSumVol += sale.TOTPESOLIQ;
                                const d = parseDate(sale.DTPED);
                                if (d) {
                                    if (d.getUTCMonth() === prevMonthIndex && d.getUTCFullYear() === prevMonthYear) {
                                        cPrevFat += sale.VLVENDA;
                                        cPrevVol += sale.TOTPESOLIQ;
                                    }
                                    const monthKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
                                    monthlyActivity.set(monthKey, (monthlyActivity.get(monthKey) || 0) + sale.VLVENDA);
                                    if (monthlyValues.hasOwnProperty(monthKey)) monthlyValues[monthKey] += sale.VLVENDA;
                                }
                            }
                        }
                    });
                }

                let activeMonthsCount = 0;
                monthlyActivity.forEach(val => { if(val >= 1) activeMonthsCount++; });
                const divisor = QUARTERLY_DIVISOR;
                const avgFat = cSumFat / divisor;
                const avgVol = cSumVol / divisor;
                const isActivePrevMonth = cPrevFat >= 1 ? 1 : 0;

                let sellerName = 'N/A';
                const rcaCode = client.rcas[0];
                if (rcaCode) sellerName = optimizedData.rcaNameByCode.get(rcaCode) || rcaCode;

                // Retrieve Stored Goal
                let metaFat = 0; let metaVol = 0;
                if (currentGoalsSupplier === 'ELMA_ALL' || currentGoalsSupplier === 'FOODS_ALL' || currentGoalsSupplier === 'PEPSICO_ALL') {
                    if (globalClientGoals.has(codCli)) {
                        const cGoals = globalClientGoals.get(codCli);
                        let keysToSum = [];
                        if (currentGoalsSupplier === 'ELMA_ALL') keysToSum = ['707', '708', '752'];
                        else if (currentGoalsSupplier === 'FOODS_ALL') keysToSum = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                        else if (currentGoalsSupplier === 'PEPSICO_ALL') keysToSum = ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];

                        keysToSum.forEach(k => { if (cGoals.has(k)) { const g = cGoals.get(k); metaFat += g.fat; metaVol += g.vol; } });
                    }
                } else {
                    if (globalClientGoals.has(codCli)) {
                        const cGoals = globalClientGoals.get(codCli);
                        if (cGoals.has(cacheKey)) { const g = cGoals.get(cacheKey); metaFat = g.fat; metaVol = g.vol; }
                    }
                }

                const metaPos = cSumFat >= 1 ? 1 : 0;

                const metric = {
                    cod: codCli, name: client.nomeCliente || client.fantasia || client.razaoSocial || 'Cliente Sem Nome', seller: sellerName,
                    avgFat, avgVol, prevFat: cPrevFat, prevVol: cPrevVol,
                    activeMonthsCount, isActivePrevMonth,
                    shareFat: (globalTotalAvgFat > 0 && avgFat > 0) ? (avgFat / globalTotalAvgFat) : 0,
                    shareVol: (globalTotalAvgVol > 0 && avgVol > 0) ? (avgVol / globalTotalAvgVol) : 0,
                    metaFat, metaVol, metaPos, monthlyValues
                };
                clientMetrics.push(metric);

                // Accumulate totals
                sumFat += metaFat; sumVol += metaVol;
                totalAvgFat += avgFat; totalPrevFat += cPrevFat;
                totalAvgVol += avgVol; totalPrevVol += cPrevVol;
                sumActiveMonths += activeMonthsCount; totalPrevClients += isActivePrevMonth;

            }, () => {
                if (currentRenderId !== goalsRenderId) return;

                // Finalize Render
                const totalAvgClients = sumActiveMonths / QUARTERLY_DIVISOR;

                const fatInput = document.getElementById('goal-global-fat');
                const volInput = document.getElementById('goal-global-vol');
                const btnDistributeFat = document.getElementById('btn-distribute-fat');
                const btnDistributeVol = document.getElementById('btn-distribute-vol');
                const isAggregatedTab = currentGoalsSupplier === 'ELMA_ALL' || currentGoalsSupplier === 'FOODS_ALL' || currentGoalsSupplier === 'PEPSICO_ALL';

                if (fatInput) {
                    if (document.activeElement !== fatInput) {
                        const displayFat = (sumFat === 0 && totalPrevFat > 0) ? totalPrevFat : sumFat;
                        fatInput.value = displayFat.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    }
                    fatInput.readOnly = false; fatInput.classList.remove('opacity-50', 'cursor-not-allowed');
                }
                if (volInput) {
                    if (document.activeElement !== volInput) {
                        const displayVol = (sumVol === 0 && totalPrevVol > 0) ? totalPrevVol : sumVol;
                        volInput.value = displayVol.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 });
                    }
                    volInput.readOnly = false; volInput.classList.remove('opacity-50', 'cursor-not-allowed');
                }
                if (btnDistributeFat) btnDistributeFat.style.display = '';
                if (btnDistributeVol) btnDistributeVol.style.display = '';

                // KPIs
                const refAvgFat = document.getElementById('ref-avg-fat'); if(refAvgFat) refAvgFat.textContent = totalAvgFat.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                const refPrevFat = document.getElementById('ref-prev-fat'); if(refPrevFat) refPrevFat.textContent = totalPrevFat.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                const refAvgVol = document.getElementById('ref-avg-vol'); if(refAvgVol) refAvgVol.textContent = totalAvgVol.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Kg';
                const refPrevVol = document.getElementById('ref-prev-vol'); if(refPrevVol) refPrevVol.textContent = totalPrevVol.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Kg';
                const refAvgClients = document.getElementById('ref-avg-clients'); if(refAvgClients) refAvgClients.textContent = totalAvgClients.toLocaleString('pt-BR', { maximumFractionDigits: 1 });
                const refPrevClients = document.getElementById('ref-prev-clients'); if(refPrevClients) refPrevClients.textContent = totalPrevClients.toLocaleString('pt-BR');

                clientMetrics.sort((a, b) => b.metaFat - a.metaFat);

                const goalMixInput = document.getElementById('goal-global-mix');
                const btnDistributeMix = document.getElementById('btn-distribute-mix');
                const naturalTotalPos = clientMetrics.reduce((sum, item) => sum + item.metaPos, 0);
                const isSingleSeller = selectedGoalsGvSellers.length === 1;

                if (goalMixInput) {
                    const newMixInput = goalMixInput.cloneNode(true);
                    goalMixInput.parentNode.replaceChild(newMixInput, goalMixInput);

                    // Calculate Total Adjustment for Current View Context
                    let contextAdjustment = 0;
                    const adjustmentMap = goalsPosAdjustments[currentGoalsSupplier];

                    if (adjustmentMap) {
                        if (isSingleSeller) {
                            // Specific Seller Context
                            contextAdjustment = adjustmentMap.get(selectedGoalsGvSellers[0]) || 0;
                        } else {
                            const visibleSellers = new Set(clientMetrics.map(c => c.seller));
                            adjustmentMap.forEach((val, seller) => {
                                if (visibleSellers.has(seller)) {
                                    contextAdjustment += val;
                                }
                            });
                        }
                    }

                    const displayPos = naturalTotalPos + contextAdjustment;
                    newMixInput.value = displayPos.toLocaleString('pt-BR');

                    if (isSingleSeller) {
                        newMixInput.readOnly = false;
                        newMixInput.classList.remove('opacity-50', 'cursor-not-allowed');

                        if(btnDistributeMix) {
                            const newBtnDistributeMix = btnDistributeMix.cloneNode(true);
                            btnDistributeMix.parentNode.replaceChild(newBtnDistributeMix, btnDistributeMix);
                            newBtnDistributeMix.style.display = '';

                            newBtnDistributeMix.onclick = () => {
                            const valStr = newMixInput.value;
                            const val = parseFloat(valStr.replace(/\./g, '').replace(',', '.')) || 0;
                            const filterDesc = getFilterDescription();
                            // Validation: Check against PEPSICO Limit
                            const sellerName = selectedGoalsGvSellers[0];
                            let pepsicoNaturalPos = 0;
                            // Calculate Natural PEPSICO Positivação for this seller
                            const len = allClientsData.length;
                            for(let i=0; i<len; i++) {
                                const c = allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i];

                            const rca = c.rcas[0];
                            const sName = optimizedData.rcaNameByCode.get(rca) || rca;
                            if (sName === sellerName) {
                            const historyIds = optimizedData.indices.history.byClient.get(c['Código']);
                            if (historyIds) {
                            for (let id of historyIds) {
                            const sale = optimizedData.historyById.get(id);
                            if ((sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') &&
                            ['707','708','752','1119'].includes(String(sale.CODFOR))) {
                            pepsicoNaturalPos++;
                            break;
                            }
                            }
                            }
                            }

                            }
                const pepsicoAdj = goalsPosAdjustments['PEPSICO_ALL'].get(sellerName) || 0;
                            const pepsicoLimit = pepsicoNaturalPos + pepsicoAdj;
                            if (currentGoalsSupplier !== 'PEPSICO_ALL' && val > pepsicoLimit) {
                            alert(`O valor não pode ultrapassar a Meta de Positivação PEPSICO definida (${pepsicoLimit.toLocaleString('pt-BR')}).\n(Natural: ${pepsicoNaturalPos}, Ajuste PEPSICO: ${pepsicoAdj})`);
                            return;
                            }
                            showConfirmationModal(`Confirmar ajuste de Meta Positivação para ${valStr} (Cliente: ${filterDesc})?`, () => {
                            const newAdjustment = val - naturalTotalPos;
                            if (adjustmentMap) {
                            adjustmentMap.set(selectedGoalsGvSellers[0], newAdjustment);
                            updateGoalsView();
                            }
                            });
                            };
                        }
                    } else {
                        newMixInput.readOnly = true;
                        newMixInput.classList.add('opacity-50', 'cursor-not-allowed');
                        if(btnDistributeMix) btnDistributeMix.style.display = 'none';
                    }
                }

                // --- MIX SALTY & FOODS CARDS LOGIC (PEPSICO ONLY) ---
                const cardMixSalty = document.getElementById('card-mix-salty');
                const cardMixFoods = document.getElementById('card-mix-foods');

                if (currentGoalsSupplier === 'PEPSICO_ALL' || currentGoalsSupplier === 'ELMA_ALL' || currentGoalsSupplier === 'FOODS_ALL') {
                    if(cardMixSalty) cardMixSalty.classList.remove('hidden');
                    if(cardMixFoods) cardMixFoods.classList.remove('hidden');

                    // Logic to populate values and handle edit
                    let naturalMixBase = 0;
                    clientMetrics.forEach(c => {
                        // Check if not seller 1001 (Americanas)
                        const sellerCode = optimizedData.rcaCodeByName.get(c.seller) || '';
                        if (sellerCode !== '1001') {
                            if (c.metaPos > 0) naturalMixBase++; // Count positivations in PEPSICO_ALL (Total Pos)
                        }
                    });

                    const naturalSaltyTarget = Math.round(naturalMixBase * 0.50);
                    const naturalFoodsTarget = Math.round(naturalMixBase * 0.30);

                    const handleMixCard = (type, naturalTarget, adjustmentsMap, inputId, btnId) => {
                        let adj = 0;
                        if (isSingleSeller) {
                            adj = adjustmentsMap.get(selectedGoalsGvSellers[0]) || 0;
                        } else {
                            const visibleSellers = new Set(clientMetrics.map(c => c.seller));
                            adjustmentsMap.forEach((val, seller) => {
                                if (visibleSellers.has(seller)) adj += val;
                            });
                        }

                        const displayVal = naturalTarget + adj;
                        const input = document.getElementById(inputId);
                        const btn = document.getElementById(btnId);

                        if(input) {
                            input.value = displayVal.toLocaleString('pt-BR');

                            if (isSingleSeller) {
                                input.readOnly = false;
                                input.classList.remove('opacity-50', 'cursor-not-allowed');
                                if(btn) btn.style.display = '';
                            } else {
                                input.readOnly = true;
                                input.classList.add('opacity-50', 'cursor-not-allowed');
                                if(btn) btn.style.display = 'none';
                            }
                        }
                    };

                    // FORCE READ FROM PEPSICO_ALL KEY for Mix Cards
                    // Calculate Natural Base using ELMA metrics (excluding Americanas) for consistency

                    // Determine visible sellers set for filtering adjustments in helper
                    let visibleSellersSet = new Set(clientMetrics.map(c => c.seller));

                    // Bugfix: If table is empty (e.g. no active clients) but we have a specific seller filter,
                    // use the filter to prevent getElmaTargetBase from returning global counts (empty set bypass).
                    if (visibleSellersSet.size === 0 && selectedGoalsGvSellers.length > 0) {
                         visibleSellersSet = new Set(selectedGoalsGvSellers);
                    }

                    const elmaTargetBase = getElmaTargetBase(displayMetrics, goalsPosAdjustments, visibleSellersSet);

                    // Card Natural Targets (Based on ELMA: 50% Salty / 30% Foods)
                    const globalNaturalSalty = Math.round(elmaTargetBase * 0.50);
                    const globalNaturalFoods = Math.round(elmaTargetBase * 0.30);

                    if (goalsMixSaltyAdjustments['PEPSICO_ALL']) {
                        handleMixCard('Salty', globalNaturalSalty, goalsMixSaltyAdjustments['PEPSICO_ALL'], 'goal-global-mix-salty', 'btn-distribute-mix-salty');
                    }
                    if (goalsMixFoodsAdjustments['PEPSICO_ALL']) {
                        handleMixCard('Foods', globalNaturalFoods, goalsMixFoodsAdjustments['PEPSICO_ALL'], 'goal-global-mix-foods', 'btn-distribute-mix-foods');
                    }

                } else {
                    if(cardMixSalty) cardMixSalty.classList.add('hidden');
                    if(cardMixFoods) cardMixFoods.classList.add('hidden');
                }

                goalsTableState.filteredData = clientMetrics;
                goalsTableState.totalPages = Math.ceil(clientMetrics.length / goalsTableState.itemsPerPage);
                if (goalsTableState.currentPage > goalsTableState.totalPages && goalsTableState.totalPages > 0) goalsTableState.currentPage = goalsTableState.totalPages;
                else if (goalsTableState.totalPages === 0) goalsTableState.currentPage = 1;

                const startIndex = (goalsTableState.currentPage - 1) * goalsTableState.itemsPerPage;
                const endIndex = startIndex + goalsTableState.itemsPerPage;
                const pageData = clientMetrics.slice(startIndex, endIndex);
                const paginationControls = document.getElementById('goals-pagination-controls');
                const pageInfo = document.getElementById('goals-page-info-text');
                const prevBtn = document.getElementById('goals-prev-page-btn');
                const nextBtn = document.getElementById('goals-next-page-btn');

                if (clientMetrics.length === 0) {
                    goalsGvTableBody.innerHTML = `<tr><td colspan="${12 + quarterMonths.length}" class="text-center p-4 text-slate-500">Nenhum cliente encontrado nos filtros para este fornecedor.</td></tr>`;
                    if (paginationControls) paginationControls.classList.add('hidden');
                } else {
                    const rows = pageData.map(item => {
                        const monthCells = quarterMonths.map(m => `<td class="px-2 py-2 text-right text-slate-400 border-r border-slate-800/50 text-[10px] bg-blue-900/5">${(item.monthlyValues[m.key] || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>`).join('');
                        return `<tr class="hover:bg-slate-800 group transition-colors border-b border-slate-800"><td class="px-2 py-2 text-center border-r border-slate-800 bg-[#151c36] text-xs text-slate-300">${item.cod}</td><td class="px-2 py-2 text-left border-r border-slate-800 bg-[#151c36] text-xs font-bold text-white truncate max-w-[200px]" title="${item.name}">${(item.name || '').substring(0, 30)}</td><td class="px-2 py-2 text-left border-r border-slate-800 bg-[#151c36] text-[10px] text-slate-400 uppercase">${getFirstName(item.seller)}</td>${monthCells}<td class="px-2 py-2 text-right text-slate-300 font-medium bg-blue-900/10 border-r border-slate-800/50 text-xs">${item.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-2 py-2 text-center text-blue-400 text-xs bg-blue-900/10 border-r border-slate-800/50">${(item.shareFat * 100).toFixed(2)}%</td><td class="px-2 py-2 text-right font-bold text-blue-200 bg-blue-900/20 border-r border-slate-800 text-xs">${item.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-2 py-2 text-right text-slate-300 font-medium bg-orange-900/10 border-r border-slate-800/50 text-xs">${item.avgVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-2 py-2 text-center text-orange-400 text-xs bg-orange-900/10 border-r border-slate-800/50">${(item.shareVol * 100).toFixed(2)}%</td><td class="px-2 py-2 text-right font-bold text-orange-200 bg-orange-900/20 border-r border-slate-800 text-xs">${item.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-2 py-2 text-center font-bold text-purple-300 bg-purple-900/10 text-xs">${item.metaPos}</td></tr>`;
                    }).join('');
                    goalsGvTableBody.innerHTML = rows;
                    if (paginationControls) {
                        paginationControls.classList.remove('hidden');

                        let exportBtn = document.getElementById('btn-export-goals-gv');
                        if (!exportBtn) {
                             const btnContainer = document.createElement('div');
                             btnContainer.className = "flex items-center ml-4";
                             btnContainer.innerHTML = `<button id="btn-export-goals-gv" class="flex items-center space-x-1 text-xs font-bold text-green-400 hover:text-green-300 border border-green-500/30 hover:border-green-500/50 bg-green-500/10 hover:bg-green-500/20 px-3 py-1.5 rounded transition-colors" title="Exportar tabela completa (XLSX)">
                                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                                    </svg>
                                    <span>XLSX</span>
                                </button>`;
                             pageInfo.parentNode.insertBefore(btnContainer, pageInfo.nextSibling);
                             document.getElementById('btn-export-goals-gv').addEventListener('click', exportGoalsCurrentTabXLSX);
                        }

                        pageInfo.textContent = `Página ${goalsTableState.currentPage} de ${goalsTableState.totalPages} (Total: ${clientMetrics.length})`;
                        prevBtn.disabled = goalsTableState.currentPage === 1;
                        nextBtn.disabled = goalsTableState.currentPage === goalsTableState.totalPages;
                    }
                }
            }, () => currentRenderId !== goalsRenderId);
        }

        function getGoalsSvFilteredData() {
            const supervisorsSet = new Set(selectedGoalsSvSupervisors);

            let clients = allClientsData;

            clients = clients.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                if (isAmericanas) return true;
                // STRICT FILTER: Exclude RCA 53 (Balcão) and INATIVOS
                if (rca1 === '53') return false;
                if (rca1 === '') return false; // Exclude INATIVOS
                return true;
            });

            if (supervisorsSet.size > 0) {
                const rcasSet = new Set();
                supervisorsSet.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => rcasSet.add(rca));
                });
                clients = clients.filter(c => c.rcas.some(r => rcasSet.has(r)));
            }

            return clients;
        }

        function recalculateGoalsSvTotals(input) {
            const { supId, colId, field, sellerId } = input.dataset;

            // Helper to parse input value
            const parseVal = (str) => {
                let val = parseFloat(str.replace(/\./g, '').replace(',', '.'));
                return isNaN(val) ? 0 : val;
            };

            // Helper to calculate and update column totals (Supervisor and Grand)
            const updateColumnTotals = (cId, fld) => {
                // 1. Supervisor Total
                // For 'geral', the values are in spans (text), for others inputs.
                let supSum = 0;
                if (cId === 'geral') {
                    const supCells = document.querySelectorAll(`.goals-sv-text[data-sup-id="${supId}"][data-col-id="${cId}"][data-field="${fld}"]`);
                    supCells.forEach(el => supSum += parseVal(el.textContent));
                } else {
                    const supInputs = document.querySelectorAll(`.goals-sv-input[data-sup-id="${supId}"][data-col-id="${cId}"][data-field="${fld}"]`);
                    supInputs.forEach(inp => supSum += parseVal(inp.value));
                }

                const supTotalEl = document.getElementById(`total-sup-${supId}-${cId}-${fld}`);
                if (supTotalEl) {
                    if (fld === 'fat') supTotalEl.textContent = supSum.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    else if (fld === 'vol' || fld === 'ton') supTotalEl.textContent = supSum.toLocaleString('pt-BR', {minimumFractionDigits: 3, maximumFractionDigits: 3});
                    else supTotalEl.textContent = supSum;
                }

                // 2. Grand Total
                let grandSum = 0;
                if (cId === 'geral') {
                    const allCells = document.querySelectorAll(`.goals-sv-text[data-col-id="${cId}"][data-field="${fld}"]`);
                    allCells.forEach(el => grandSum += parseVal(el.textContent));
                } else {
                    const allInputs = document.querySelectorAll(`.goals-sv-input[data-col-id="${cId}"][data-field="${fld}"]`);
                    allInputs.forEach(inp => grandSum += parseVal(inp.value));
                }

                const grandTotalEl = document.getElementById(`total-grand-${cId}-${fld}`);
                if (grandTotalEl) {
                    if (fld === 'fat') grandTotalEl.textContent = grandSum.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    else if (fld === 'vol' || fld === 'ton') grandTotalEl.textContent = grandSum.toLocaleString('pt-BR', {minimumFractionDigits: 3, maximumFractionDigits: 3});
                    else grandTotalEl.textContent = grandSum;
                }
            };

            // A. Update Current Column Totals
            updateColumnTotals(colId, field);

            // B. Row Aggregation Logic (Update Total Elma/Foods and Geral)
            const elmaIds = ['707', '708', '752'];
            const foodsIds = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];

            let groupTotalId = null;
            let components = [];

            if (elmaIds.includes(colId)) {
                groupTotalId = 'total_elma';
                components = elmaIds;
            } else if (foodsIds.includes(colId)) {
                groupTotalId = 'total_foods';
                components = foodsIds;
            }

            // Only aggregate if we are editing a base column (not changing mix or tonnage directly if those were editable)
            if (groupTotalId) {
                // 1. Recalculate Group Total (Row)
                let groupSum = 0;
                components.forEach(cId => {
                    const el = document.querySelector(`.goals-sv-input[data-seller-id="${sellerId}"][data-col-id="${cId}"][data-field="${field}"]`);
                    if (el) groupSum += parseVal(el.value);
                });

                // Update Group Total Input (Read-only)
                const groupInput = document.querySelector(`.goals-sv-input[data-seller-id="${sellerId}"][data-col-id="${groupTotalId}"][data-field="${field}"]`);
                if (groupInput) {
                    if (field === 'fat') groupInput.value = groupSum.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    else if (field === 'vol') groupInput.value = groupSum.toLocaleString('pt-BR', {minimumFractionDigits: 3, maximumFractionDigits: 3});
                    else groupInput.value = groupSum; // Pos

                    // Update Column Totals for the Group Column
                    updateColumnTotals(groupTotalId, field);

                    // Special Logic: If updating TOTAL ELMA POS, update PEDEV
                    if (groupTotalId === 'total_elma' && field === 'pos') {
                        const pedevVal = Math.round(groupSum * 0.9);
                        const pedevCell = document.getElementById(`pedev-${sellerId}-pos`);
                        if (pedevCell) {
                            pedevCell.textContent = pedevVal;
                            updateColumnTotals('pedev', 'pos');
                        }
                    }
                }
            }

            // 2. Recalculate GERAL Total (Row) - Only for Fat and Vol/Ton
            // Geral Pos is static (Active Clients), so we don't update it on input change
            if (field === 'fat' || field === 'vol') {
                const elmaInput = document.querySelector(`.goals-sv-input[data-seller-id="${sellerId}"][data-col-id="total_elma"][data-field="${field}"]`);
                const foodsInput = document.querySelector(`.goals-sv-input[data-seller-id="${sellerId}"][data-col-id="total_foods"][data-field="${field}"]`);

                let elmaVal = elmaInput ? parseVal(elmaInput.value) : 0;
                let foodsVal = foodsInput ? parseVal(foodsInput.value) : 0;
                let geralSum = elmaVal + foodsVal;

                // Map field 'vol' to 'ton' for Geral if needed, or keep consistent
                // In column definitions: 'tonelada_elma' is type 'tonnage' (field 'vol'). 'geral' is type 'geral'.
                // Geral uses field 'fat' and 'ton'.
                const geralField = field === 'vol' ? 'ton' : field;

                const geralCell = document.getElementById(`geral-${sellerId}-${geralField}`);
                if (geralCell) {
                    if (geralField === 'fat') geralCell.textContent = geralSum.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    else geralCell.textContent = geralSum.toLocaleString('pt-BR', {minimumFractionDigits: 3, maximumFractionDigits: 3});

                    // Update Column Totals for Geral
                    updateColumnTotals('geral', geralField);
                }
            }
        }

        function updateGoalsSvView() {
            goalsSvRenderId++;
            const currentRenderId = goalsSvRenderId;

            if (quarterMonths.length === 0) identifyQuarterMonths();
            const filteredClients = getGoalsSvFilteredData();

            // Define Column Blocks (Metrics Config)
            const svColumns = [
                { id: 'total_elma', label: 'TOTAL ELMA', type: 'standard', isAgg: true, colorClass: 'text-teal-400', components: ['707', '708', '752'] },
                { id: '707', label: 'EXTRUSADOS', type: 'standard', supplier: '707', brand: null, colorClass: 'text-slate-300' },
                { id: '708', label: 'NÃO EXTRUSADOS', type: 'standard', supplier: '708', brand: null, colorClass: 'text-slate-300' },
                { id: '752', label: 'TORCIDA', type: 'standard', supplier: '752', brand: null, colorClass: 'text-slate-300' },
                { id: 'tonelada_elma', label: 'KG ELMA', type: 'tonnage', isAgg: true, colorClass: 'text-orange-400', components: ['707', '708', '752'] },
                { id: 'mix_salty', label: 'MIX SALTY', type: 'mix', isAgg: true, colorClass: 'text-teal-400', components: [] },
                { id: 'total_foods', label: 'TOTAL FOODS', type: 'standard', isAgg: true, colorClass: 'text-yellow-400', components: ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'] },
                { id: '1119_TODDYNHO', label: 'TODDYNHO', type: 'standard', supplier: '1119', brand: 'TODDYNHO', colorClass: 'text-slate-300' },
                { id: '1119_TODDY', label: 'TODDY', type: 'standard', supplier: '1119', brand: 'TODDY', colorClass: 'text-slate-300' },
                { id: '1119_QUAKER_KEROCOCO', label: 'QUAKER / KEROCOCO', type: 'standard', supplier: '1119', brand: 'QUAKER_KEROCOCO', colorClass: 'text-slate-300' },
                { id: 'tonelada_foods', label: 'KG FOODS', type: 'tonnage', isAgg: true, colorClass: 'text-orange-400', components: ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'] },
                { id: 'mix_foods', label: 'MIX FOODS', type: 'mix', isAgg: true, colorClass: 'text-yellow-400', components: [] },
                { id: 'geral', label: 'GERAL', type: 'geral', isAgg: true, colorClass: 'text-white', components: ['total_elma', 'total_foods'] },
                { id: 'pedev', label: 'AUDITORIA PEDEV', type: 'pedev', isAgg: true, colorClass: 'text-pink-400', components: ['total_elma'] }
            ];

            const baseCategories = svColumns.filter(c => c.type === 'standard' && !c.isAgg);
            const mainTable = document.getElementById('goals-sv-main-table');
            if (mainTable) mainTable.innerHTML = '<tbody><tr><td class="text-center p-8"><svg class="animate-spin h-8 w-8 text-teal-500 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg></td></tr></tbody>';

            // Ensure Global Totals are cached (Sync operation, but fast enough for initialization)
            baseCategories.forEach(cat => {
                const cacheKey = cat.supplier + (cat.brand ? `_${cat.brand}` : '');
                if (!globalGoalsTotalsCache[cacheKey]) calculateDistributedGoals([], cat.supplier, cat.brand, 0, 0);
            });

            // Prepare Aggregation Structures
            const sellerMap = new Map();
            const initSeller = (sellerName) => {
                if (!sellerMap.has(sellerName)) {
                    let sellerCode = optimizedData.rcaCodeByName.get(sellerName) || '';
                    let supervisorName = 'N/A';
                    if (sellerCode) {
                        for (const [sup, rcas] of optimizedData.rcasBySupervisor) {
                            if (rcas.includes(sellerCode)) { supervisorName = sup; break; }
                        }
                    }
                    sellerMap.set(sellerName, { name: sellerName, code: sellerCode, supervisor: supervisorName, data: {}, metaPosTotal: 0, elmaPos: 0, foodsPos: 0 });
                }
                return sellerMap.get(sellerName);
            };

            const currentDate = lastSaleDate;
            const prevMonthDate = new Date(Date.UTC(currentDate.getUTCFullYear(), currentDate.getUTCMonth() - 1, 1));
            const prevMonthIndex = prevMonthDate.getUTCMonth();
            const prevMonthYear = prevMonthDate.getUTCFullYear();

            // Optimization: Normalize functions outside loop
            const norm = (str) => str ? str.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase() : '';

            // ASYNC LOOP
            runAsyncChunked(filteredClients, (client) => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const clientHistoryIds = optimizedData.indices.history.byClient.get(codCli);

                let sellerName = 'N/A';
                const rcaCode = client.rcas[0];
                if (rcaCode) sellerName = optimizedData.rcaNameByCode.get(rcaCode) || rcaCode;
                else if (client.rcas.length === 0 || client.rcas[0] === '') sellerName = 'INATIVOS';
                const sellerObj = initSeller(sellerName);

                // Initialize client totals for each category
                const clientCatTotals = {};
                baseCategories.forEach(c => clientCatTotals[c.id] = { fat: 0, vol: 0, pos: 0, prevFat: 0, monthly: {} });

                // Single Pass over History for this Client
                if (clientHistoryIds) {
                    clientHistoryIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        // EXCEPTION: Exclude Balcão (53) sales for Client 9569 from Portfolio Analysis
                        if (String(codCli).trim() === '9569' && (String(sale.CODUSUR).trim() === '53' || String(sale.CODUSUR).trim() === '053')) return;

                        const isRev = (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9');
                        if (!isRev) return;

                        const codFor = String(sale.CODFOR);
                        let matchedCats = [];

                        // Determine which categories this sale belongs to
                        if (codFor === '707') matchedCats.push('707');
                        else if (codFor === '708') matchedCats.push('708');
                        else if (codFor === '752') matchedCats.push('752');
                        else if (codFor === '1119') {
                            const desc = norm(sale.DESCRICAO || '');
                            if (desc.includes('TODDYNHO')) matchedCats.push('1119_TODDYNHO');
                            else if (desc.includes('TODDY')) matchedCats.push('1119_TODDY');
                            else if (desc.includes('QUAKER') || desc.includes('KEROCOCO')) matchedCats.push('1119_QUAKER_KEROCOCO');
                        }

                        if (matchedCats.length > 0) {
                            const d = parseDate(sale.DTPED);
                            const isPrev = d && d.getUTCMonth() === prevMonthIndex && d.getUTCFullYear() === prevMonthYear;
                            const monthKey = d ? `${d.getUTCFullYear()}-${d.getUTCMonth()}` : null;

                            matchedCats.forEach(catId => {
                                const t = clientCatTotals[catId];
                                t.fat += sale.VLVENDA;
                                t.vol += sale.TOTPESOLIQ;
                                if (isPrev) t.prevFat += sale.VLVENDA;
                                if (monthKey) t.monthly[monthKey] = (t.monthly[monthKey] || 0) + sale.VLVENDA;
                            });
                        }
                    });
                }

                // Aggregate to Seller
                baseCategories.forEach(cat => {
                    const t = clientCatTotals[cat.id];
                    if (!sellerObj.data[cat.id]) sellerObj.data[cat.id] = { metaFat: 0, metaVol: 0, metaPos: 0, avgVol: 0, avgFat: 0, monthlyValues: {} };
                    const sData = sellerObj.data[cat.id];

                    const avgFat = t.fat / QUARTERLY_DIVISOR;
                    const avgVol = t.vol / QUARTERLY_DIVISOR;
                    const metaPos = t.fat >= 1 ? 1 : 0;

                    // Fetch Stored Goal
                    let metaFat = 0; let metaVol = 0;
                    if (globalClientGoals.has(codCli)) {
                        const cacheKey = cat.supplier + (cat.brand ? `_${cat.brand}` : '');
                        const cGoals = globalClientGoals.get(codCli);
                        if (cGoals.has(cacheKey)) { const g = cGoals.get(cacheKey); metaFat = g.fat; metaVol = g.vol; }
                    }

                    sData.metaFat += metaFat;
                    sData.metaVol += metaVol;
                    sData.metaPos += metaPos;
                    sData.avgVol += avgVol;
                    sData.avgFat += avgFat;

                    // Monthly Breakdown
                    quarterMonths.forEach(m => {
                        if (!sData.monthlyValues[m.key]) sData.monthlyValues[m.key] = 0;
                        sData.monthlyValues[m.key] += (t.monthly[m.key] || 0);
                    });
                });

                // Calculate Aggregate Positivation for Client (Unique Client Count)
                let clientElmaFat = (clientCatTotals['707']?.fat || 0) + (clientCatTotals['708']?.fat || 0) + (clientCatTotals['752']?.fat || 0);
                if (clientElmaFat >= 1) sellerObj.elmaPos++;

                let clientFoodsFat = (clientCatTotals['1119_TODDYNHO']?.fat || 0) + (clientCatTotals['1119_TODDY']?.fat || 0) + (clientCatTotals['1119_QUAKER_KEROCOCO']?.fat || 0);
                if (clientFoodsFat >= 1) sellerObj.foodsPos++;

            }, () => {
                if (currentRenderId !== goalsSvRenderId) return;

                // --- FINALIZE AGGREGATION & RENDER ---

                // 1. Calculate Aggregates (Mix, Geral, etc.)
                sellerMap.forEach(sellerObj => {
                    // Component Aggregates
                    svColumns.filter(c => c.isAgg && c.type !== 'mix' && c.type !== 'geral' && c.type !== 'pedev').forEach(aggCol => {
                        let sumFat = 0, sumVol = 0, sumPos = 0, sumAvgVol = 0, sumAvgFat = 0;
                        const monthlySum = {}; quarterMonths.forEach(m => monthlySum[m.key] = 0);
                        aggCol.components.forEach(compId => {
                            if (sellerObj.data[compId]) {
                                sumFat += sellerObj.data[compId].metaFat; sumVol += sellerObj.data[compId].metaVol;
                                sumPos += sellerObj.data[compId].metaPos; sumAvgVol += sellerObj.data[compId].avgVol;
                                sumAvgFat += sellerObj.data[compId].avgFat || 0;
                                quarterMonths.forEach(m => monthlySum[m.key] += (sellerObj.data[compId].monthlyValues[m.key] || 0));
                            }
                        });

                        // Use calculated unique client count for Total Elma/Foods
                        if (aggCol.id === 'total_elma') sumPos = sellerObj.elmaPos || 0;
                        else if (aggCol.id === 'total_foods') sumPos = sellerObj.foodsPos || 0;

                        sellerObj.data[aggCol.id] = { metaFat: sumFat, metaVol: sumVol, metaPos: sumPos, avgVol: sumAvgVol, avgFat: sumAvgFat, monthlyValues: monthlySum };
                    });

                    // Mix Metrics
                    const historyIds = optimizedData.indices.history.byRca.get(sellerObj.name) || [];
                    let activeClientsCount = 0;

                    // Logic for Active Clients (Positivados Geral > 1)
                    // We need to check if ANY sale > 1 for this client in history
                    // Optimized: Reuse indices
                    // We iterate filteredClients to find active ones for this seller?
                    // No, "Meta Pos Total" is defined as unique active clients for the seller in the filtered list.
                    // We can re-iterate filteredClients? No, slow.
                    // We can aggregate during the main loop.
                    // Let's do it simply:
                    const sellerClients = filteredClients.filter(c => {
                        const code = c.rcas[0];
                        const name = optimizedData.rcaNameByCode.get(code) || code;
                        return name === sellerObj.name;
                    });
                    sellerClients.forEach(c => {
                        // Check if active (Total Fat > 1 in history)
                        const hIds = optimizedData.indices.history.byClient.get(c['Código']);
                        let totalFat = 0;
                        if(hIds) {
                            for (const id of hIds) {
                                const s = optimizedData.historyById.get(id);
                                const codFor = String(s.CODFOR);
                                if (['707', '708', '752', '1119'].includes(codFor)) {
                                    if (s.TIPOVENDA === '1' || s.TIPOVENDA === '9') totalFat += s.VLVENDA;
                                }
                            }
                        }
                        if (totalFat >= 1) activeClientsCount++;
                    });

                    // Mix Calc (re-implement or optimize?)
                    // Mix calculation requires detailed product analysis per client.
                    // For speed, let's assume we can do it sync here for 1 seller's history (smaller dataset).
                    const monthlyData = new Map();
                    historyIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        if (sale.TIPOVENDA !== '1' && sale.TIPOVENDA !== '9') return; // Strict Type Check
                        const d = parseDate(sale.DTPED);
                        if (!d) return;
                        const mKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
                        if (!monthlyData.has(mKey)) monthlyData.set(mKey, new Map());
                        const cMap = monthlyData.get(mKey);
                        if (!cMap.has(sale.CODCLI)) cMap.set(sale.CODCLI, { salty: new Set(), foods: new Set() });
                        const cData = cMap.get(sale.CODCLI);
                        if (sale.VLVENDA >= 1) {
                            const desc = norm(sale.DESCRICAO);
                            MIX_SALTY_CATEGORIES.forEach(cat => { if (desc.includes(cat)) cData.salty.add(cat); });
                            MIX_FOODS_CATEGORIES.forEach(cat => { if (desc.includes(cat)) cData.foods.add(cat); });
                        }
                    });
                    let sumSalty = 0; let sumFoods = 0;
                    const months = Array.from(monthlyData.keys()).sort().slice(-3);
                    const divisor = months.length > 0 ? months.length : 1;
                    months.forEach(m => {
                        const cMap = monthlyData.get(m);
                        let mSalty = 0; let mFoods = 0;
                        cMap.forEach(d => {
                            if (d.salty.size >= MIX_SALTY_CATEGORIES.length) mSalty++;
                            if (d.foods.size >= MIX_FOODS_CATEGORIES.length) mFoods++;
                        });
                        sumSalty += mSalty; sumFoods += mFoods;
                    });

                    // Calculate Mix Targets using ELMA Base (Natural + Adjustment) to match GV 'RESUMO' Logic
                    // Base logic: Active Elma Clients (elmaPos) + ELMA Adjustments
                    const elmaAdjForMix = goalsPosAdjustments['ELMA_ALL'] ? (goalsPosAdjustments['ELMA_ALL'].get(sellerObj.name) || 0) : 0;
                    const elmaBaseForMix = (sellerObj.elmaPos || 0) + elmaAdjForMix;

                    let mixSaltyMeta = Math.round(elmaBaseForMix * 0.50);
                    let mixFoodsMeta = Math.round(elmaBaseForMix * 0.30);

                    if (sellerObj.code === '1001') { mixSaltyMeta = 0; mixFoodsMeta = 0; }

                    sellerObj.data['mix_salty'] = { avgMix: sumSalty / divisor, metaMix: mixSaltyMeta };
                    sellerObj.data['mix_foods'] = { avgMix: sumFoods / divisor, metaMix: mixFoodsMeta };

                    // Geral & Pedev
                    const totalElma = sellerObj.data['total_elma'];
                    const totalFoods = sellerObj.data['total_foods'];

                    // Note: 'activeClientsCount' here is the Pepsico Natural Active Count.
                    // The 'geral' column will receive the 'PEPSICO_ALL' adjustment in the loop below.

                    sellerObj.data['geral'] = {
                        avgFat: (totalElma.avgFat || 0) + (totalFoods.avgFat || 0),
                        metaFat: totalElma.metaFat + totalFoods.metaFat,
                        metaVol: totalElma.metaVol + totalFoods.metaVol,
                        metaPos: activeClientsCount
                    };
                    // Pedev uses Total Elma (Natural). We'll update it after adjustment loop to be safe.
                    sellerObj.data['pedev'] = { metaPos: Math.round(totalElma.metaPos * 0.9) };
                });

                // Group Supervisors

                // --- APPLY ADJUSTMENTS TO SELLERS ---
                sellerMap.forEach(seller => {
                    const sellerName = seller.name;

                    // 1. Positivation Adjustments
                    // Map Column ID -> Adjustment Key
                    // IDs: 'total_elma'->'ELMA_ALL', 'total_foods'->'FOODS_ALL', 'geral'->'PEPSICO_ALL'
                    //      '707'->'707', etc.

                    const posKeys = {
                        'total_elma': 'ELMA_ALL',
                        'total_foods': 'FOODS_ALL',
                        'geral': 'PEPSICO_ALL', // GERAL uses PEPSICO_ALL for Positivação
                        '707': '707', '708': '708', '752': '752',
                        '1119_TODDYNHO': '1119_TODDY', // Wait, map keys?
                        '1119_TODDY': '1119_TODDY', // Check keys in globalGoalsMetrics
                        '1119_QUAKER_KEROCOCO': '1119_QUAKER_KEROCOCO'
                    };

                    // Specific Fix for Toddynho Key mismatch if any (1119_TODDYNHO vs 1119_TODDYNHO)
                    // My previous code used '1119_TODDYNHO'.
                    posKeys['1119_TODDYNHO'] = '1119_TODDYNHO';

                    for (const [colId, data] of Object.entries(seller.data)) {
                        // Apply Pos Adjustment
                        const adjKey = posKeys[colId] || colId; // Fallback to ID
                        if (goalsPosAdjustments[adjKey]) {
                            const adj = goalsPosAdjustments[adjKey].get(sellerName) || 0;
                            // Update Meta Pos: Natural (Summed from clients) + Adjustment
                            // Note: 'data.metaPos' currently holds Natural Count from client loop.
                            data.metaPos = data.metaPos + adj;
                        }

                        // Apply Mix Adjustment (Only for Mix Cols)
                        if (colId === 'mix_salty') {
                            const adj = goalsMixSaltyAdjustments['PEPSICO_ALL']?.get(sellerName) || 0;
                            // metaMix was calculated as Math.round(active * 0.5).
                            data.metaMix = data.metaMix + adj;
                        }
                        if (colId === 'mix_foods') {
                            const adj = goalsMixFoodsAdjustments['PEPSICO_ALL']?.get(sellerName) || 0;
                            data.metaMix = data.metaMix + adj;
                        }

                        // Apply Pedev Adjustment? (Calculated as 90% of Total Elma)
                        // This is calculated LATER in the supervisor loop?
                        // "sellerObj.data['pedev'] = { metaPos: Math.round(totalElma.metaPos * 0.9) };"
                        // This line exists inside the client loop (aggregating).
                        // Since we just updated total_elma.metaPos, we should re-calculate pedev here.
                    }

                    // Re-calculate PEDEV based on updated TOTAL ELMA
                    if (seller.data['total_elma'] && seller.data['pedev']) {
                         seller.data['pedev'].metaPos = Math.round(seller.data['total_elma'].metaPos * 0.9);
                    }

                    // Re-calculate GERAL based on updated components?
                    // GERAL components: total_elma, total_foods.
                    // "sellerObj.data['geral'] = { ... metaPos: activeClientsCount }"
                    // The 'activeClientsCount' in the loop was based on (Total Fat > 1).
                    // This is 'PEPSICO NATURAL'.
                    // So 'geral' key maps to PEPSICO_ALL adjustment.
                    // Handled above via posKeys['geral'] = 'PEPSICO_ALL'.
                });

const supervisorGroups = new Map();
                sellerMap.forEach(seller => {
                    const supName = seller.supervisor;
                    if (!supervisorGroups.has(supName)) supervisorGroups.set(supName, { name: supName, id: supName.replace(/[^a-zA-Z0-9]/g, '_'), code: optimizedData.supervisorCodeByName.get(supName) || '', sellers: [], totals: {} });
                    supervisorGroups.get(supName).sellers.push(seller);
                });

                // Aggregate Totals
                supervisorGroups.forEach(group => {
                    svColumns.forEach(col => {
                        if (!group.totals[col.id]) group.totals[col.id] = { metaFat: 0, metaVol: 0, metaPos: 0, avgVol: 0, avgMix: 0, metaMix: 0, avgFat: 0, monthlyValues: {} };
                        quarterMonths.forEach(m => group.totals[col.id].monthlyValues[m.key] = 0);
                        group.sellers.forEach(seller => {
                            if (seller.data[col.id]) {
                                const s = seller.data[col.id]; const t = group.totals[col.id];
                                t.metaFat += s.metaFat || 0; t.metaVol += s.metaVol || 0; t.metaPos += s.metaPos || 0;
                                t.avgVol += s.avgVol || 0; t.avgMix += s.avgMix || 0; t.metaMix += s.metaMix || 0; t.avgFat += s.avgFat || 0;
                                if (s.monthlyValues) quarterMonths.forEach(m => t.monthlyValues[m.key] += s.monthlyValues[m.key]);
                            }
                        });
                    });

                    // Recalculate Mix Targets for Supervisor using Group Aggregates to match Global Logic
                    // 1. Calculate Group Natural Base (Sum of sellers' natural bases)
                    // Note: 'metaPos' in 'geral' is the Natural PEPSICO Base (unique clients per seller)
                    // Must exclude Americanas (Code 1001) from Base Calculation
                    let groupPepsicoNatural = 0;
                    group.sellers.forEach(seller => {
                        if (seller.code !== '1001') {
                            groupPepsicoNatural += (seller.data['geral'] ? seller.data['geral'].metaPos : 0);
                        }
                    });

                    // 2. Calculate Group Adjustments
                    let groupPepsicoAdj = 0;
                    if (goalsPosAdjustments['PEPSICO_ALL']) {
                        group.sellers.forEach(seller => {
                            groupPepsicoAdj += (goalsPosAdjustments['PEPSICO_ALL'].get(seller.name) || 0);
                        });
                    }

                    const groupPepsicoBase = groupPepsicoNatural + groupPepsicoAdj;

                    // 3. Calculate Mix Targets
                    let groupMixSaltyMeta = Math.round(groupPepsicoBase * 0.50);
                    let groupMixFoodsMeta = Math.round(groupPepsicoBase * 0.30);

                    // 4. Add Mix Adjustments
                    let groupMixSaltyAdj = 0;
                    let groupMixFoodsAdj = 0;
                    if (goalsMixSaltyAdjustments['PEPSICO_ALL']) {
                        group.sellers.forEach(seller => groupMixSaltyAdj += (goalsMixSaltyAdjustments['PEPSICO_ALL'].get(seller.name) || 0));
                    }
                    if (goalsMixFoodsAdjustments['PEPSICO_ALL']) {
                        group.sellers.forEach(seller => groupMixFoodsAdj += (goalsMixFoodsAdjustments['PEPSICO_ALL'].get(seller.name) || 0));
                    }

                    // Override summed totals with recalculated totals
                    if (group.totals['mix_salty']) group.totals['mix_salty'].metaMix = groupMixSaltyMeta + groupMixSaltyAdj;
                    if (group.totals['mix_foods']) group.totals['mix_foods'].metaMix = groupMixFoodsMeta + groupMixFoodsAdj;
                });

                // Recalculate Grand Total (Geral PRIME) using Global Aggregates
                // We can sum the recalculated Group totals which are now consistent, or redo Global.
                // Let's redo Global to be absolutely sure "A = B".
                const grandTotalRow = { totals: {} };
                svColumns.forEach(col => grandTotalRow.totals[col.id] = { metaFat: 0, metaVol: 0, metaPos: 0, avgVol: 0, avgMix: 0, metaMix: 0, avgFat: 0 });

                // Sum standard metrics from groups
                supervisorGroups.forEach(group => {
                    svColumns.forEach(col => {
                        const s = group.totals[col.id]; const t = grandTotalRow.totals[col.id];
                        t.metaFat += s.metaFat; t.metaVol += s.metaVol; t.metaPos += s.metaPos;
                        t.avgVol += s.avgVol; t.avgMix += s.avgMix; t.avgFat += s.avgFat;
                        // Don't sum metaMix yet
                    });
                });

                // Recalculate Grand Total Mix
                // Calculate Global Natural Base excluding Americanas (using TOTAL_ELMA base as per RESUMO logic)
                let globalElmaNatural = 0;
                sellerMap.forEach(seller => {
                    if (seller.code !== '1001') {
                        globalElmaNatural += (seller.data['total_elma'] ? seller.data['total_elma'].metaPos : 0);
                    }
                });

                let globalElmaAdj = 0;
                let globalMixSaltyAdj = 0;
                let globalMixFoodsAdj = 0;

                // Iterate ALL sellers to get total adjustments
                sellerMap.forEach(seller => {
                    if (seller.code !== '1001') {
                        globalElmaAdj += (goalsPosAdjustments['ELMA_ALL'] ? (goalsPosAdjustments['ELMA_ALL'].get(seller.name) || 0) : 0);
                        globalMixSaltyAdj += (goalsMixSaltyAdjustments['PEPSICO_ALL'] ? (goalsMixSaltyAdjustments['PEPSICO_ALL'].get(seller.name) || 0) : 0);
                        globalMixFoodsAdj += (goalsMixFoodsAdjustments['PEPSICO_ALL'] ? (goalsMixFoodsAdjustments['PEPSICO_ALL'].get(seller.name) || 0) : 0);
                    }
                });

                const globalElmaBase = globalElmaNatural + globalElmaAdj;
                grandTotalRow.totals['mix_salty'].metaMix = Math.round(globalElmaBase * 0.50) + globalMixSaltyAdj;
                grandTotalRow.totals['mix_foods'].metaMix = Math.round(globalElmaBase * 0.30) + globalMixFoodsAdj;

                // We inject this fake Grand Total row logic into the sortedSupervisors array or handle it in rendering?
                // The rendering logic likely expects sortedSupervisors to contain only supervisors.
                // The "Geral PRIME" row is usually rendered separately in the footer.
                // Let's check the rendering loop below.

                const sortedSupervisors = Array.from(supervisorGroups.values()).sort((a, b) => (b.totals['total_elma']?.metaFat || 0) - (a.totals['total_elma']?.metaFat || 0));
                currentGoalsSvData = sortedSupervisors;

                // Render HTML
                if (!mainTable) return;
                const monthsCount = quarterMonths.length;
                let headerHTML = `<thead class="text-[10px] uppercase sticky top-0 z-20 bg-[#0f172a] text-slate-400"><tr><th rowspan="3" class="px-2 py-2 text-center w-16 border-r border-b border-slate-700">CÓD</th><th rowspan="3" class="px-3 py-2 text-left w-48 border-r border-b border-slate-700">VENDEDOR</th>`;
                svColumns.forEach(col => {
                    let colspan = 2;
                    if (col.type === 'standard') colspan = monthsCount + 1 + 4;
                    if (col.type === 'tonnage') colspan = 3; if (col.type === 'mix') colspan = 3; if (col.type === 'geral') colspan = 4;
                    headerHTML += `<th colspan="${colspan}" class="px-2 py-2 text-center font-bold border-r border-b border-slate-700 ${col.colorClass}">${col.label}</th>`;
                });
                headerHTML += `</tr><tr>`;
                svColumns.forEach(col => {
                    if (col.type === 'standard') headerHTML += `<th colspan="${monthsCount + 1}" class="px-1 py-1 text-center border-r border-slate-700/50 bg-slate-800/50">HISTÓRICO FAT.</th><th colspan="2" class="px-1 py-1 text-center border-r border-slate-700/50 bg-slate-800/50">FATURAMENTO</th><th colspan="2" class="px-1 py-1 text-center border-r border-slate-700 bg-slate-800/50">POSITIVAÇÃO</th>`;
                    else if (col.type === 'tonnage') headerHTML += `<th class="px-1 py-1 text-right border-r border-slate-700/50 bg-slate-800/50">MÉDIA TRIM.</th><th colspan="2" class="px-1 py-1 text-center border-r border-slate-700 bg-slate-800/50">META KG</th>`;
                    else if (col.type === 'mix') headerHTML += `<th class="px-1 py-1 text-right border-r border-slate-700/50 bg-slate-800/50">MÉDIA TRIM.</th><th colspan="2" class="px-1 py-1 text-center border-r border-slate-700 bg-slate-800/50">META MIX</th>`;
                    else if (col.type === 'geral') headerHTML += `<th colspan="2" class="px-1 py-1 text-center border-r border-slate-700/50 bg-slate-800/50">FATURAMENTO</th><th class="px-1 py-1 text-center border-r border-slate-700/50 bg-slate-800/50">KG</th><th class="px-1 py-1 text-center border-r border-slate-700 bg-slate-800/50">POSITIVAÇÃO</th>`;
                    else if (col.type === 'pedev') headerHTML += `<th class="px-1 py-1 text-center border-r border-slate-700/50 bg-slate-800/50">META</th>`;
                });
                headerHTML += `</tr><tr>`;
                const gearIcon = ``; /* Removed Icon as per request to remove editing option, but kept column structure */
                svColumns.forEach(col => {
                    if (col.type === 'standard') {
                        quarterMonths.forEach(m => headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal w-12">${m.label}</th>`);
                        headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Média</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Aj.</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Aj.</th>`;
                    } else if (col.type === 'tonnage') headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Volume</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Volume</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Aj.</th>`;
                    else if (col.type === 'mix') headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Qtd</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Aj.</th>`;
                    else if (col.type === 'geral') headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Média Trim.</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th>`;
                    else if (col.type === 'pedev') headerHTML += `<th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th>`;
                });
                headerHTML += `</tr></thead>`;

                let bodyHTML = `<tbody class="divide-y divide-slate-800 bg-[#151c36]">`;
                // Grand Totals calc
                const grandTotals = {}; svColumns.forEach(col => { grandTotals[col.id] = { metaFat: 0, metaVol: 0, metaPos: 0, avgVol: 0, avgMix: 0, metaMix: 0, avgFat: 0, monthlyValues: {} }; quarterMonths.forEach(m => grandTotals[col.id].monthlyValues[m.key] = 0); });

                sortedSupervisors.forEach((sup, index) => {
                    sup.id = `sup-${index}`;
                    svColumns.forEach(col => {
                        grandTotals[col.id].metaFat += sup.totals[col.id].metaFat; grandTotals[col.id].metaVol += sup.totals[col.id].metaVol; grandTotals[col.id].metaPos += sup.totals[col.id].metaPos;
                        grandTotals[col.id].avgVol += sup.totals[col.id].avgVol; grandTotals[col.id].avgMix += sup.totals[col.id].avgMix; grandTotals[col.id].metaMix += sup.totals[col.id].metaMix;
                        grandTotals[col.id].avgFat += sup.totals[col.id].avgFat;
                        if (sup.totals[col.id].monthlyValues) quarterMonths.forEach(m => grandTotals[col.id].monthlyValues[m.key] += sup.totals[col.id].monthlyValues[m.key]);
                    });

                    sup.sellers.sort((a, b) => (b.data['total_elma']?.metaFat || 0) - (a.data['total_elma']?.metaFat || 0));
                    sup.sellers.forEach(seller => {
                        bodyHTML += `<tr class="hover:bg-slate-800 border-b border-slate-800"><td class="px-2 py-1 text-center text-slate-400 font-mono">${seller.code}</td><td class="px-2 py-1 text-left text-white font-medium truncate max-w-[200px]" title="${seller.name}">${getFirstName(seller.name)}</td>`;
                        svColumns.forEach(col => {
                            const d = seller.data[col.id] || { metaFat: 0, metaVol: 0, metaPos: 0, avgVol: 0, avgMix: 0, metaMix: 0, avgFat: 0, monthlyValues: {} };
                            if (col.type === 'standard') {
                                const isReadOnly = col.isAgg; const inputClass = isReadOnly ? 'text-slate-400 font-bold opacity-70' : 'text-yellow-300'; const readonlyAttr = 'readonly disabled'; const cellBg = isReadOnly ? 'bg-[#151c36]' : 'bg-[#1e293b]';
                                quarterMonths.forEach(m => bodyHTML += `<td class="px-1 py-1 text-right text-slate-400 border-r border-slate-800/50 text-[10px] bg-blue-900/5">${(d.monthlyValues[m.key] || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>`);
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 bg-blue-900/10 font-medium">${d.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right ${col.colorClass} border-r border-slate-800/50 text-xs font-mono">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 ${cellBg} border-r border-slate-800/50"><input type="text" value="${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}" class="goals-sv-input bg-transparent text-right w-full outline-none ${inputClass} text-xs font-mono" ${readonlyAttr}></td><td class="px-1 py-1 text-center text-slate-300 border-r border-slate-800/50">${d.metaPos}</td><td class="px-1 py-1 ${cellBg} border-r border-slate-800/50"><input type="text" value="${d.metaPos}" class="goals-sv-input bg-transparent text-center w-full outline-none ${inputClass} text-xs font-mono" ${readonlyAttr}></td>`;
                            } else if (col.type === 'tonnage') {
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-mono text-xs">${d.avgVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-bold font-mono text-xs">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 bg-[#1e293b] border-r border-slate-800/50"><input type="text" value="${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}" class="goals-sv-input bg-transparent text-right w-full outline-none text-yellow-300 text-xs font-mono" readonly disabled></td>`;
                            } else if (col.type === 'mix') {
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50">${d.avgMix.toLocaleString('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})}</td><td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-bold">${d.metaMix}</td><td class="px-1 py-1 bg-[#1e293b] border-r border-slate-800/50"><input type="text" value="${d.metaMix}" class="goals-sv-input bg-transparent text-right w-full outline-none text-yellow-300 text-xs font-mono" readonly disabled></td>`;
                            } else if (col.type === 'geral') {
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-400 border-r border-slate-800/50 font-mono text-xs">${d.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right text-white font-bold border-r border-slate-800/50 font-mono text-xs goals-sv-text" data-sup-id="${sup.id}" data-col-id="geral" data-field="fat" id="geral-${seller.id || seller.name.replace(/\s+/g,'_')}-fat">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right text-white font-bold border-r border-slate-800/50 font-mono text-xs goals-sv-text" data-sup-id="${sup.id}" data-col-id="geral" data-field="ton" id="geral-${seller.id || seller.name.replace(/\s+/g,'_')}-ton">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 text-center text-white font-bold border-r border-slate-800/50 font-mono text-xs goals-sv-text" data-sup-id="${sup.id}" data-col-id="geral" data-field="pos" id="geral-${seller.id || seller.name.replace(/\s+/g,'_')}-pos">${d.metaPos}</td>`;
                            } else if (col.type === 'pedev') {
                                bodyHTML += `<td class="px-1 py-1 text-center text-pink-400 font-bold border-r border-slate-800/50 font-mono text-xs goals-sv-text" data-sup-id="${sup.id}" data-col-id="pedev" data-field="pos" id="pedev-${seller.id || seller.name.replace(/\s+/g,'_')}-pos">${d.metaPos}</td>`;
                            }
                        });
                        bodyHTML += `</tr>`;
                    });

                    bodyHTML += `<tr class="bg-slate-800 font-bold border-b border-slate-600"><td class="px-2 py-2 text-center text-slate-400 font-mono">${sup.code}</td><td class="px-2 py-2 text-left text-white uppercase tracking-wider">${sup.name}</td>`;
                    svColumns.forEach(col => {
                        const d = sup.totals[col.id]; const color = col.id.includes('total') || col.type === 'tonnage' || col.type === 'mix' ? 'text-white' : 'text-slate-300';
                        if (col.type === 'standard') {
                            quarterMonths.forEach(m => bodyHTML += `<td class="px-1 py-1 text-right text-slate-400 border-r border-slate-700 text-[10px] bg-blue-900/5">${(d.monthlyValues[m.key] || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>`);
                            bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-700 bg-blue-900/10 font-medium">${d.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right ${color} border-r border-slate-700">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right text-yellow-500/70 border-r border-slate-700" id="total-sup-${sup.id}-${col.id}-fat">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-center ${color} border-r border-slate-700">${d.metaPos}</td><td class="px-1 py-1 text-center text-yellow-500/70 border-r border-slate-700" id="total-sup-${sup.id}-${col.id}-pos">${d.metaPos}</td>`;
                        } else if (col.type === 'tonnage') bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-700">${d.avgVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 text-right ${color} border-r border-slate-700">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 text-right text-yellow-500/70 border-r border-slate-700" id="total-sup-${sup.id}-${col.id}-vol">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td>`;
                        else if (col.type === 'mix') bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-700">${d.avgMix.toLocaleString('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})}</td><td class="px-1 py-1 text-right ${color} border-r border-slate-700">${d.metaMix}</td><td class="px-1 py-1 text-right text-yellow-500/70 border-r border-slate-700" id="total-sup-${sup.id}-${col.id}-mix">${d.metaMix}</td>`;
                        else if (col.type === 'geral') bodyHTML += `<td class="px-1 py-1 text-right text-slate-400 border-r border-slate-700">${d.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right text-white border-r border-slate-700" id="total-sup-${sup.id}-geral-fat">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right text-white border-r border-slate-700" id="total-sup-${sup.id}-geral-ton">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 text-center text-white border-r border-slate-700" id="total-sup-${sup.id}-geral-pos">${d.metaPos}</td>`;
                        else if (col.type === 'pedev') bodyHTML += `<td class="px-1 py-1 text-center text-pink-400 border-r border-slate-700" id="total-sup-${sup.id}-pedev-pos">${Math.round(sup.totals['total_elma']?.metaPos * 0.9)}</td>`;
                    });
                    bodyHTML += `</tr>`;
                });

                // Grand Total
                bodyHTML += `<tr class="bg-[#0f172a] font-bold text-white border-t-2 border-slate-500 sticky bottom-0 z-20"><td class="px-2 py-3 text-center uppercase tracking-wider">GV</td><td class="px-2 py-3 text-left uppercase tracking-wider">Geral PRIME</td>`;
                svColumns.forEach(col => {
                    // Use recalculated grandTotalRow.totals instead of summed grandTotals
                    const d = grandTotalRow.totals[col.id] || { metaFat: 0, metaVol: 0, metaPos: 0, avgVol: 0, avgMix: 0, metaMix: 0, avgFat: 0 };
                    // Fallback to monthlyValues from original summation if needed (recalculation didn't handle it, but it's not critical for Meta Mix/Pos)
                    // Actually, let's use grandTotals for monthly/avgs and grandTotalRow for Metas if recalculated.
                    const dOrig = grandTotals[col.id];
                    const monthlyVals = dOrig ? dOrig.monthlyValues : {};
                    const avgFat = dOrig ? dOrig.avgFat : 0;
                    const avgVol = dOrig ? dOrig.avgVol : 0;
                    const avgMix = dOrig ? dOrig.avgMix : 0;

                    if (col.type === 'standard') {
                        quarterMonths.forEach(m => bodyHTML += `<td class="px-1 py-2 text-right text-slate-400 border-r border-slate-700 text-[10px] bg-blue-900/5">${(monthlyVals[m.key] || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>`);
                        bodyHTML += `<td class="px-1 py-2 text-right text-teal-400 border-r border-slate-700 bg-blue-900/10">${avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-2 text-right text-teal-400 border-r border-slate-700">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-2 text-right text-teal-600/70 border-r border-slate-700" id="total-grand-${col.id}-fat">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-2 text-center text-purple-400 border-r border-slate-700">${d.metaPos}</td><td class="px-1 py-2 text-center text-purple-600/70 border-r border-slate-700" id="total-grand-${col.id}-pos">${d.metaPos}</td>`;
                    } else if (col.type === 'tonnage') bodyHTML += `<td class="px-1 py-2 text-right text-slate-400 border-r border-slate-700">${avgVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-2 text-right text-orange-400 border-r border-slate-700">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-2 text-right text-orange-600/70 border-r border-slate-700" id="total-grand-${col.id}-vol">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>`;
                    else if (col.type === 'mix') bodyHTML += `<td class="px-1 py-2 text-right text-slate-400 border-r border-slate-700">${avgMix.toLocaleString('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})}</td><td class="px-1 py-2 text-right text-cyan-400 border-r border-slate-700">${d.metaMix}</td><td class="px-1 py-2 text-right text-cyan-600/70 border-r border-slate-700" id="total-grand-${col.id}-mix">${d.metaMix}</td>`;
                    else if (col.type === 'geral') bodyHTML += `<td class="px-1 py-2 text-right text-slate-500 border-r border-slate-700">${avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-2 text-right text-white border-r border-slate-700" id="total-grand-geral-fat">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-2 text-right text-white border-r border-slate-700" id="total-grand-geral-ton">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-2 text-center text-white border-r border-slate-700" id="total-grand-geral-pos">${d.metaPos}</td>`;
                    else if (col.type === 'pedev') bodyHTML += `<td class="px-1 py-2 text-center text-pink-400 border-r border-slate-700" id="total-grand-pedev-pos">${Math.round(grandTotalRow.totals['total_elma']?.metaPos * 0.9)}</td>`;
                });
                bodyHTML += `</tr></tbody>`;
                mainTable.innerHTML = headerHTML + bodyHTML;
            }, () => currentRenderId !== goalsSvRenderId);
        }

        function handleGoalsFilterChange() {
            // Update Dropdown Lists based on available data?
            // Standard pattern: Update filter lists based on selection of others?
            // For now, simpler: Just update the view.
            if (window.goalsUpdateTimeout) clearTimeout(window.goalsUpdateTimeout);
            window.goalsUpdateTimeout = setTimeout(() => {
                // Update Seller Filter options based on Supervisor
                // Get all clients matching supervisor filter, extract sellers.
                // This is slightly different from sales-based filtering.
                // But we can stick to standard updateSellerFilter using 'allSalesData' or 'allClientsData'?
                // updateSellerFilter uses a sales/data array.
                // Let's use allSalesData for consistency with other views to populate seller lists.

                updateGoalsView();
            }, 50);
        }

        function resetGoalsGvFilters() {
            selectedGoalsGvSupervisors = [];
            selectedGoalsGvSellers = [];
            goalsGvCodcliFilter.value = '';

            selectedGoalsGvSupervisors = updateSupervisorFilter(goalsGvSupervisorFilterDropdown, goalsGvSupervisorFilterText, selectedGoalsGvSupervisors, allSalesData);
            selectedGoalsGvSellers = updateSellerFilter(selectedGoalsGvSupervisors, goalsGvSellerFilterDropdown, goalsGvSellerFilterText, selectedGoalsGvSellers, allSalesData);

            updateGoalsView();
        }

        // <!-- INÍCIO DO CÓDIGO RESTAURADO -->

        function getCoverageFilteredData(options = {}) {
            const { excludeFilter = null } = options;
            const isExcluded = (f) => excludeFilter === f || (Array.isArray(excludeFilter) && excludeFilter.includes(f));

            // Define filter sets from UI state
            const sellersNameSet = new Set(selectedCoverageSellers);
            const supervisorsSet = new Set(selectedCoverageSupervisors);
            const city = coverageCityFilter.value.trim().toLowerCase();
            const filial = coverageFilialFilter.value;
            const suppliersSet = new Set(selectedCoverageSuppliers);
            const productsSet = new Set(selectedCoverageProducts);
            const tiposVendaSet = new Set(selectedCoverageTiposVenda);

            // --- Client Filtering (Universe for KPIs) ---
            // Use active clients data defined by coverage logic
            let clients = getActiveClientsData();

            if (filial !== 'ambas' || supervisorsSet.size > 0 || sellersNameSet.size > 0 || city) {
                const rcasOfSupervisor = new Set();
                if (!isExcluded('supervisor') && supervisorsSet.size > 0) {
                    supervisorsSet.forEach(sup => {
                        (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => rcasOfSupervisor.add(rca));
                    });
                }
                const rcasOfSellers = new Set();
                if (!isExcluded('seller') && sellersNameSet.size > 0) {
                     sellersNameSet.forEach(sellerName => {
                        const rcaCode = optimizedData.rcaCodeByName.get(sellerName);
                        if (rcaCode) rcasOfSellers.add(rcaCode);
                    });
                }
                clients = clients.filter(c => {
                    if (filial !== 'ambas' && clientLastBranch.get(c['Código']) !== filial) return false;
                    const clientRcas = (c.rcas && Array.isArray(c.rcas)) ? c.rcas : [];
                    if (rcasOfSupervisor.size > 0 && !clientRcas.some(rca => rcasOfSupervisor.has(rca))) return false;
                    if (rcasOfSellers.size > 0 && !clientRcas.some(rca => rcasOfSellers.has(rca))) return false;
                    if (!isExcluded('city') && city && (!c.cidade || c.cidade.toLowerCase() !== city)) return false;
                    return true;
                });
            }
            const clientCodes = new Set(clients.map(c => c['Código']));

            // --- Sales Filtering (Optimized via Indices) ---
            const filters = {
                supervisor: supervisorsSet,
                seller: sellersNameSet,
                supplier: suppliersSet,
                product: productsSet,
                tipoVenda: tiposVendaSet,
                city: city,
                filial: filial,
                clientCodes: clientCodes // Pass client universe to correctly scope sales
            };

            let sales = getFilteredDataFromIndices(optimizedData.indices.current, optimizedData.salesById, filters, excludeFilter);
            let history = getFilteredDataFromIndices(optimizedData.indices.history, optimizedData.historyById, filters, excludeFilter);

            // Post-filtering for logic not supported by indices
            const unitPriceInput = document.getElementById('coverage-unit-price-filter');
            const unitPrice = unitPriceInput && unitPriceInput.value ? parseFloat(unitPriceInput.value) : null;
            if (unitPrice !== null) {
                const unitPriceFilter = s => (s.QTVENDA > 0 && Math.abs((s.VLVENDA / s.QTVENDA) - unitPrice) < 0.01);
                sales = sales.filter(unitPriceFilter);
                history = history.filter(unitPriceFilter);
            }

            return { sales, history, clients };
        }

        function updateAllCoverageFilters(options = {}) {
            const { skipFilter = null } = options;

            if (skipFilter !== 'supervisor') {
                const { sales: salesSup, history: historySup } = getCoverageFilteredData({ excludeFilter: 'supervisor' });
                const combinedDataSup = [...salesSup, ...historySup];
                selectedCoverageSupervisors = updateSupervisorFilter(document.getElementById('coverage-supervisor-filter-dropdown'), document.getElementById('coverage-supervisor-filter-text'), selectedCoverageSupervisors, combinedDataSup);
            }

            const { sales: salesSeller, history: historySeller } = getCoverageFilteredData({ excludeFilter: 'seller' });
            selectedCoverageSellers = updateSellerFilter(selectedCoverageSupervisors, coverageVendedorFilterDropdown, coverageVendedorFilterText, selectedCoverageSellers, [...salesSeller, ...historySeller], skipFilter === 'seller');

            const { sales: salesSupplier, history: historySupplier } = getCoverageFilteredData({ excludeFilter: ['supplier', 'product'] });
            selectedCoverageSuppliers = updateSupplierFilter(coverageSupplierFilterDropdown, coverageSupplierFilterText, selectedCoverageSuppliers, [...salesSupplier, ...historySupplier], 'coverage', skipFilter === 'supplier');

            const { sales: salesProd, history: historyProd } = getCoverageFilteredData({ excludeFilter: 'product' });
            selectedCoverageProducts = updateProductFilter(coverageProductFilterDropdown, coverageProductFilterText, selectedCoverageProducts, [...salesProd, ...historyProd], 'coverage', skipFilter === 'product');

            const { sales: salesTV, history: historyTV } = getCoverageFilteredData({ excludeFilter: 'tipoVenda' });
            selectedCoverageTiposVenda = updateTipoVendaFilter(coverageTipoVendaFilterDropdown, coverageTipoVendaFilterText, selectedCoverageTiposVenda, [...salesTV, ...historyTV], skipFilter === 'tipoVenda');
        }

        function handleCoverageFilterChange(options = {}) {
            // Debounce update to prevent UI freezing during rapid selection
            if (window.coverageUpdateTimeout) clearTimeout(window.coverageUpdateTimeout);
            window.coverageUpdateTimeout = setTimeout(() => {
                 updateAllCoverageFilters(options);
                 updateCoverageView();
            }, 10);
        }

        function resetCoverageFilters() {
            selectedCoverageSupervisors = [];
            coverageCityFilter.value = '';
            coverageFilialFilter.value = 'ambas';

            const unitPriceInput = document.getElementById('coverage-unit-price-filter');
            if(unitPriceInput) unitPriceInput.value = '';

            const workingDaysInput = document.getElementById('coverage-working-days-input');
            if(workingDaysInput) workingDaysInput.value = customWorkingDaysCoverage;

            selectedCoverageSellers = [];
            selectedCoverageSuppliers = [];
            selectedCoverageProducts = [];
            selectedCoverageTiposVenda = [];

            updateAllCoverageFilters();
            updateCoverageView();
        }

        function updateCoverageView() {
            coverageRenderId++;
            const currentRenderId = coverageRenderId;

            const { clients, sales, history } = getCoverageFilteredData();
            const productsToAnalyze = [...new Set([...sales.map(s => s.PRODUTO), ...history.map(s => s.PRODUTO)])];

            const sellers = selectedCoverageSellers;
            const sellerRcaCodes = new Set();
            if (sellers.length > 0) {
                sellers.forEach(sellerName => {
                    const rcaCode = optimizedData.rcaCodeByName.get(sellerName);
                    if (rcaCode) sellerRcaCodes.add(rcaCode);
                });
            } else if (selectedCoverageSupervisors.length > 0) {
                selectedCoverageSupervisors.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => sellerRcaCodes.add(rca));
                });
            }

            const activeClientsForCoverage = clients.filter(c => {
                const codcli = c['Código'];
                const rca1 = String(c.rca1 || '').trim();

                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(codcli));
            });
            const activeClientsCount = activeClientsForCoverage.length;
            const activeClientCodes = new Set(activeClientsForCoverage.map(c => c['Código']));

            coverageActiveClientsKpi.textContent = activeClientsCount.toLocaleString('pt-BR');

            // Show Loading State in Table
            coverageTableBody.innerHTML = '<tr><td colspan="8" class="text-center p-8"><div class="flex justify-center items-center"><svg class="animate-spin h-8 w-8 text-teal-500 mr-3" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg><span class="text-slate-400">Calculando cobertura...</span></div></td></tr>';

            if (productsToAnalyze.length === 0) {
                coverageSelectionCoverageValueKpi.textContent = '0%';
                coverageSelectionCoverageCountKpi.textContent = `0 de ${activeClientsCount.toLocaleString('pt-BR')} clientes`;
                coverageSelectionCoverageValueKpiPrevious.textContent = '0%';
                coverageSelectionCoverageCountKpiPrevious.textContent = `0 de ${activeClientsCount.toLocaleString('pt-BR')} clientes`;
                coverageTopCoverageValueKpi.textContent = '0%';
                coverageTopCoverageProductKpi.textContent = '-';
                coverageTableBody.innerHTML = '<tr><td colspan="7" class="text-center p-8 text-slate-500">Nenhum produto selecionado ou encontrado para os filtros.</td></tr>';
                showNoDataMessage('coverageCityChart', 'Sem dados para exibir.');
                return;
            }

            const tableData = [];
            const clientSelectionValueCurrent = new Map(); // Map<CODCLI, Value>
            const clientSelectionValuePrevious = new Map(); // Map<CODCLI, Value>
            let topCoverageItem = { name: '-', coverage: 0, clients: 0 };
            const activeStockMap = getActiveStockMap(coverageFilialFilter.value);

            const currentMonth = lastSaleDate.getUTCMonth();
            const currentYear = lastSaleDate.getUTCFullYear();
            const prevMonthIdx = (currentMonth === 0) ? 11 : currentMonth - 1;
            const prevMonthYear = (currentMonth === 0) ? currentYear - 1 : currentYear;

            // --- CRITICAL OPTIMIZATION: Pre-aggregate everything ---

            // Maps for Box Quantities: Map<PRODUTO, Number>
            const boxesSoldCurrentMap = new Map();
            const boxesSoldPreviousMap = new Map();

            // Index for Trend Calculation: Map<PRODUTO, Array<Sale>>
            // We group all sales (current + history) by product to calculate trend efficiently
            const trendSalesMap = new Map();

            // Process Current Sales (O(N))
            // --- OTIMIZAÇÃO: Mapa invertido para performance O(1) no cálculo de cobertura ---
            const productClientsCurrent = new Map(); // Map<PRODUTO, Map<CODCLI, Value>>
            const productClientsPrevious = new Map(); // Map<PRODUTO, Map<CODCLI, Value>>

            // Use synchronous loops for initial map building as iterating sales (linear) is generally fast enough
            // (e.g. 50k sales ~ 50ms). Splitting this would require complex state management.
            // The bottleneck is the nested Product * Client check loop later.

            sales.forEach(s => {
                // Coverage Map (Inverted for Performance)
                if (!productClientsCurrent.has(s.PRODUTO)) productClientsCurrent.set(s.PRODUTO, new Map());
                const clientMap = productClientsCurrent.get(s.PRODUTO);
                clientMap.set(s.CODCLI, (clientMap.get(s.CODCLI) || 0) + s.VLVENDA);

                // Box Quantity Map
                boxesSoldCurrentMap.set(s.PRODUTO, (boxesSoldCurrentMap.get(s.PRODUTO) || 0) + s.QTVENDA_EMBALAGEM_MASTER);

                // Trend Map
                if (!trendSalesMap.has(s.PRODUTO)) trendSalesMap.set(s.PRODUTO, []);
                trendSalesMap.get(s.PRODUTO).push(s);
            });

            // Process History Sales (O(N))
            history.forEach(s => {
                const d = parseDate(s.DTPED);
                const isPrevMonth = d && d.getUTCMonth() === prevMonthIdx && d.getUTCFullYear() === prevMonthYear;

                // Coverage Map (only if prev month)
                if (isPrevMonth) {
                    // Coverage Map (Inverted for Performance)
                    if (!productClientsPrevious.has(s.PRODUTO)) productClientsPrevious.set(s.PRODUTO, new Map());
                    const clientMap = productClientsPrevious.get(s.PRODUTO);
                    clientMap.set(s.CODCLI, (clientMap.get(s.CODCLI) || 0) + s.VLVENDA);

                    // Box Quantity Map (only if prev month)
                    boxesSoldPreviousMap.set(s.PRODUTO, (boxesSoldPreviousMap.get(s.PRODUTO) || 0) + s.QTVENDA_EMBALAGEM_MASTER);
                }

                // Trend Map (All history)
                if (!trendSalesMap.has(s.PRODUTO)) trendSalesMap.set(s.PRODUTO, []);
                trendSalesMap.get(s.PRODUTO).push(s);
            });

            // Pre-calculate global dates for Trend
            const endDate = parseDate(sortedWorkingDays[sortedWorkingDays.length - 1]);

            // --- ASYNC CHUNKED PROCESSING ---
            runAsyncChunked(productsToAnalyze, (productCode) => {
                const productInfo = productDetailsMap.get(productCode) || { descricao: `Produto ${productCode}`};

                let clientsWhoGotProductCurrent = 0;
                let clientsWhoGotProductPrevious = 0;

                // --- OTIMIZAÇÃO CRÍTICA: Iterar apenas os compradores do produto em vez de todos os clientes ativos ---

                // Check Current
                const buyersCurrentMap = productClientsCurrent.get(productCode);
                if (buyersCurrentMap) {
                    buyersCurrentMap.forEach((val, buyer) => {
                        if (activeClientCodes.has(buyer)) {
                            if (val >= 1) clientsWhoGotProductCurrent++;
                            clientSelectionValueCurrent.set(buyer, (clientSelectionValueCurrent.get(buyer) || 0) + val);
                        }
                    });
                }

                // Check Previous
                const buyersPreviousMap = productClientsPrevious.get(productCode);
                if (buyersPreviousMap) {
                    buyersPreviousMap.forEach((val, buyer) => {
                        if (activeClientCodes.has(buyer)) {
                            if (val >= 1) clientsWhoGotProductPrevious++;
                            clientSelectionValuePrevious.set(buyer, (clientSelectionValuePrevious.get(buyer) || 0) + val);
                        }
                    });
                }

                const coverageCurrent = activeClientsCount > 0 ? (clientsWhoGotProductCurrent / activeClientsCount) * 100 : 0;

                if (coverageCurrent > topCoverageItem.coverage) {
                    topCoverageItem = {
                        name: `(${productCode}) ${productInfo.descricao}`,
                        coverage: coverageCurrent,
                        clients: clientsWhoGotProductCurrent
                    };
                }

                const stockQty = activeStockMap.get(productCode) || 0;

                // Trend Calculation
                const productAllSales = trendSalesMap.get(productCode) || [];

                const productCadastroDate = parseDate(productInfo.dtCadastro);
                let productFirstWorkingDayIndex = 0;
                if (productCadastroDate) {
                    const cadastroDateString = productCadastroDate.toISOString().split('T')[0];
                    productFirstWorkingDayIndex = sortedWorkingDays.findIndex(d => d >= cadastroDateString);
                    if (productFirstWorkingDayIndex === -1) productFirstWorkingDayIndex = sortedWorkingDays.length;
                }
                const productMaxLifeInWorkingDays = sortedWorkingDays.length - productFirstWorkingDayIndex;

                const hasHistory = productAllSales.some(s => {
                    const d = parseDate(s.DTPED);
                    return d && (d.getUTCFullYear() < currentYear || (d.getUTCFullYear() === currentYear && d.getUTCMonth() < currentMonth));
                });
                const soldThisMonth = (boxesSoldCurrentMap.get(productCode) || 0) > 0;
                const isFactuallyNewOrReactivated = (!hasHistory && soldThisMonth);

                const daysFromBox = customWorkingDaysCoverage;
                let effectiveDaysToCalculate;

                if (isFactuallyNewOrReactivated) {
                    const daysToConsider = (daysFromBox > 0) ? daysFromBox : passedWorkingDaysCurrentMonth;
                    effectiveDaysToCalculate = Math.min(passedWorkingDaysCurrentMonth, daysToConsider);
                } else {
                    if (daysFromBox > 0) {
                        effectiveDaysToCalculate = Math.min(daysFromBox, productMaxLifeInWorkingDays);
                    } else {
                        effectiveDaysToCalculate = productMaxLifeInWorkingDays;
                    }
                }

                const daysDivisor = effectiveDaysToCalculate > 0 ? effectiveDaysToCalculate : 1;
                const targetIndex = Math.max(0, sortedWorkingDays.length - daysDivisor);
                const startDate = parseDate(sortedWorkingDays[targetIndex]);

                let totalQtySoldInRange = 0;
                // Optimized loop: only iterating relevant sales for this product
                productAllSales.forEach(sale => {
                    const saleDate = parseDate(sale.DTPED);
                    if (saleDate && saleDate >= startDate && saleDate <= endDate) {
                        totalQtySoldInRange += (sale.QTVENDA_EMBALAGEM_MASTER || 0);
                    }
                });

                const dailyAvgSale = totalQtySoldInRange / daysDivisor;
                const trendDays = dailyAvgSale > 0 ? (stockQty / dailyAvgSale) : (stockQty > 0 ? Infinity : 0);

                // Box Quantities (Pre-calculated)
                const boxesSoldCurrentMonth = boxesSoldCurrentMap.get(productCode) || 0;
                const boxesSoldPreviousMonth = boxesSoldPreviousMap.get(productCode) || 0;

                const boxesVariation = boxesSoldPreviousMonth > 0
                    ? ((boxesSoldCurrentMonth - boxesSoldPreviousMonth) / boxesSoldPreviousMonth) * 100
                    : (boxesSoldCurrentMonth > 0 ? Infinity : 0);

                const pdvVariation = clientsWhoGotProductPrevious > 0
                    ? ((clientsWhoGotProductCurrent - clientsWhoGotProductPrevious) / clientsWhoGotProductPrevious) * 100
                    : (clientsWhoGotProductCurrent > 0 ? Infinity : 0);

                tableData.push({
                    descricao: `(${productCode}) ${productInfo.descricao}`,
                    stockQty: stockQty,
                    boxesSoldCurrentMonth: boxesSoldCurrentMonth,
                    boxesSoldPreviousMonth: boxesSoldPreviousMonth,
                    boxesVariation: boxesVariation,
                    pdvVariation: pdvVariation,
                    trendDays: trendDays,
                    clientsPreviousCount: clientsWhoGotProductPrevious,
                    clientsCurrentCount: clientsWhoGotProductCurrent,
                    coverageCurrent: coverageCurrent
                });
            }, () => {
                // --- ON COMPLETE CALLBACK (Render UI) ---
                if (currentRenderId !== coverageRenderId) return;

                coverageTopCoverageValueKpi.textContent = `${topCoverageItem.coverage.toFixed(2)}%`;
                coverageTopCoverageProductKpi.textContent = topCoverageItem.name;
                coverageTopCoverageProductKpi.title = topCoverageItem.name;

                let selectionCoveredCountCurrent = 0;
                clientSelectionValueCurrent.forEach(val => { if (val >= 1) selectionCoveredCountCurrent++; });
                const selectionCoveragePercentCurrent = activeClientsCount > 0 ? (selectionCoveredCountCurrent / activeClientsCount) * 100 : 0;
                coverageSelectionCoverageValueKpi.textContent = `${selectionCoveragePercentCurrent.toFixed(2)}%`;
                coverageSelectionCoverageCountKpi.textContent = `${selectionCoveredCountCurrent.toLocaleString('pt-BR')} de ${activeClientsCount.toLocaleString('pt-BR')} clientes`;

                let selectionCoveredCountPrevious = 0;
                clientSelectionValuePrevious.forEach(val => { if (val >= 1) selectionCoveredCountPrevious++; });
                const selectionCoveragePercentPrevious = activeClientsCount > 0 ? (selectionCoveredCountPrevious / activeClientsCount) * 100 : 0;
                coverageSelectionCoverageValueKpiPrevious.textContent = `${selectionCoveragePercentPrevious.toFixed(2)}%`;
                coverageSelectionCoverageCountKpiPrevious.textContent = `${selectionCoveredCountPrevious.toLocaleString('pt-BR')} de ${activeClientsCount.toLocaleString('pt-BR')} clientes`;

                tableData.sort((a, b) => {
                    return b.stockQty - a.stockQty;
                });

                let filteredTableData = tableData.filter(item => item.boxesSoldCurrentMonth > 0);

                if (coverageTrendFilter !== 'all') {
                    filteredTableData = filteredTableData.filter(item => {
                        const trend = item.trendDays;
                        if (coverageTrendFilter === 'low') return isFinite(trend) && trend < 15;
                        if (coverageTrendFilter === 'medium') return isFinite(trend) && trend >= 15 && trend < 30;
                        if (coverageTrendFilter === 'good') return isFinite(trend) && trend >= 30;
                        return false;
                    });
                }

                const totalBoxesFiltered = filteredTableData.reduce((sum, item) => sum + item.boxesSoldCurrentMonth, 0);
                if (coverageTotalBoxesEl) {
                    coverageTotalBoxesEl.textContent = totalBoxesFiltered.toLocaleString('pt-BR', { maximumFractionDigits: 0 });
                }

                coverageTableDataForExport = filteredTableData;

                coverageTableBody.innerHTML = filteredTableData.slice(0, 500).map(item => {
                    let boxesVariationContent;
                    if (isFinite(item.boxesVariation)) {
                        const colorClass = item.boxesVariation >= 0 ? 'text-green-400' : 'text-red-400';
                        boxesVariationContent = `<span class="${colorClass}">${item.boxesVariation.toFixed(1)}%</span>`;
                    } else if (item.boxesVariation === Infinity) {
                        boxesVariationContent = `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-purple-500/30 text-purple-300">Novo</span>`;
                    } else {
                        boxesVariationContent = `<span>-</span>`;
                    }

                    let pdvVariationContent;
                    if (isFinite(item.pdvVariation)) {
                        const colorClass = item.pdvVariation >= 0 ? 'text-green-400' : 'text-red-400';
                        pdvVariationContent = `<span class="${colorClass}">${item.pdvVariation.toFixed(1)}%</span>`;
                    } else if (item.pdvVariation === Infinity) {
                        pdvVariationContent = `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-purple-500/30 text-purple-300">Novo</span>`;
                    } else {
                        pdvVariationContent = `<span>-</span>`;
                    }

                    return `
                        <tr class="hover:bg-slate-700/50">
                            <td class="px-2 py-1.5 text-xs">${item.descricao}</td>
                            <td class="px-2 py-1.5 text-xs text-right">${item.stockQty.toLocaleString('pt-BR')}</td>
                            <td class="px-2 py-1.5 text-xs text-right">${item.boxesSoldPreviousMonth.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                            <td class="px-2 py-1.5 text-xs text-right">${item.boxesSoldCurrentMonth.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                            <td class="px-2 py-1.5 text-xs text-right">${boxesVariationContent}</td>
                            <td class="px-2 py-1.5 text-xs text-right">${item.clientsPreviousCount.toLocaleString('pt-BR')}</td>
                            <td class="px-2 py-1.5 text-xs text-right">${item.clientsCurrentCount.toLocaleString('pt-BR')}</td>
                            <td class="px-2 py-1.5 text-xs text-right">${pdvVariationContent}</td>
                        </tr>
                    `;
                }).join('');

                // Render Top 10 Cities Chart
                const salesByCity = {};
                const salesBySeller = {};

                sales.forEach(s => {
                    const client = clientMapForKPIs.get(String(s.CODCLI));
                    const city = client ? (client.cidade || client['Nome da Cidade'] || 'N/A') : 'N/A';
                    salesByCity[city] = (salesByCity[city] || 0) + s.QTVENDA_EMBALAGEM_MASTER;

                    const seller = s.NOME || 'N/A';
                    salesBySeller[seller] = (salesBySeller[seller] || 0) + s.QTVENDA_EMBALAGEM_MASTER;
                });

                // 1. Chart Data for Cities
                const sortedCities = Object.entries(salesByCity)
                    .sort(([,a], [,b]) => b - a)
                    .slice(0, 10);

                // 2. Chart Data for Sellers
                const sortedSellers = Object.entries(salesBySeller)
                    .sort(([,a], [,b]) => b - a)
                    .slice(0, 10);

                const commonChartOptions = {
                    indexAxis: 'x',
                    plugins: {
                        datalabels: {
                            align: 'end',
                            anchor: 'end',
                            color: '#cbd5e1',
                            font: { weight: 'bold', size: 14 },
                            formatter: (value) => value.toLocaleString('pt-BR', { maximumFractionDigits: 1 })
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    let label = context.dataset.label || '';
                                    if (label) {
                                        label += ': ';
                                    }
                                    if (context.parsed.y !== null) {
                                        label += context.parsed.y.toLocaleString('pt-BR', { maximumFractionDigits: 2 }) + ' caixas';
                                    }
                                    return label;
                                }
                            }
                        }
                    }
                };

                if (sortedCities.length > 0) {
                    createChart('coverageCityChart', 'bar', sortedCities.map(([city]) => city), sortedCities.map(([, qty]) => qty), commonChartOptions);
                } else {
                    showNoDataMessage('coverageCityChart', 'Sem dados para exibir.');
                }

                if (sortedSellers.length > 0) {
                    createChart('coverageSellerChart', 'bar', sortedSellers.map(([seller]) => getFirstName(seller)), sortedSellers.map(([, qty]) => qty), commonChartOptions);
                } else {
                    showNoDataMessage('coverageSellerChart', 'Sem dados para exibir.');
                }

                // Visibility Toggle Logic
                const cityContainer = document.getElementById('coverageCityChartContainer');
                const sellerContainer = document.getElementById('coverageSellerChartContainer');
                const toggleBtn = document.getElementById('coverage-chart-toggle-btn');
                const chartTitle = document.getElementById('coverage-chart-title');

                if (currentCoverageChartMode === 'city') {
                    if (cityContainer) cityContainer.classList.remove('hidden');
                    if (sellerContainer) sellerContainer.classList.add('hidden');
                    if (toggleBtn) toggleBtn.textContent = 'Ver Vendedores';
                    if (chartTitle) chartTitle.textContent = 'Top 10 Cidades (Quantidade de Caixas)';
                } else {
                    if (cityContainer) cityContainer.classList.add('hidden');
                    if (sellerContainer) sellerContainer.classList.remove('hidden');
                    if (toggleBtn) toggleBtn.textContent = 'Ver Cidades';
                    if (chartTitle) chartTitle.textContent = 'Top 10 Vendedores (Quantidade de Caixas)';
                }
            }, () => currentRenderId !== coverageRenderId);
        }

        // <!-- FIM DO CÓDIGO RESTAURADO -->

        function getUniqueMonthCount(data) {
            const months = new Set();
            data.forEach(sale => {
                const saleDate = parseDate(sale.DTPED);
                if (saleDate) {
                    const monthKey = `${saleDate.getUTCFullYear()}-${saleDate.getUTCMonth()}`;
                    months.add(monthKey);
                }
            });
            return months.size > 0 ? months.size : 1;
        }

        function calculateSummaryFromData(data, isFiltered, clientBaseForPositivacao) {
            const summary = {
                totalFaturamento: 0, totalPeso: 0, vendasPorVendedor: {}, vendasPorSupervisor: {},
                top10ProdutosFaturamento: [], top10ProdutosPeso: [], faturamentoPorFornecedor: {},
                skuPdv: 0, positivacaoCount: 0, positivacaoPercent: 0
            };
            const salesByProduct = {};
            const faturamentoMap = new Map();

            // --- INÍCIO DA MODIFICAÇÃO: KPIs de Cobertura e SKU ---

            // 1. Lógica de Positivação (Cobertura)
            // Registar clientes que tiveram *qualquer* operação (Venda OU Bonificação)
            const positiveClients = new Set();
            const clientUniqueSkus = new Map(); // Map<CodCli, Set<Produto>>

            // 1. Lógica de Positivação (Cobertura) - Alinhada com Comparativo
            // Agrega valor total por cliente para verificar threshold >= 1
            const clientTotalSales = new Map();

            data.forEach(sale => {
                if (sale.CODCLI) {
                    const currentVal = clientTotalSales.get(sale.CODCLI) || 0;
                    // Considera apenas VLVENDA para consistência com o KPI "Clientes Atendidos" do Comparativo
                    // Se a regra de bonificação mudar lá, deve mudar aqui também.
                    // Atualmente Comparativo usa: (s.TIPOVENDA === '1' || s.TIPOVENDA === '9') -> VLVENDA
                    // Note que 'data' aqui já vem filtrado, mas precisamos checar se o valor agregado passa do threshold
                    clientTotalSales.set(sale.CODCLI, currentVal + (Number(sale.VLVENDA) || 0));

                    // Rastrear SKUs únicos (mantendo lógica existente para SKU/PDV)
                    // Mas apenas se o cliente for considerado "positivo" no final?
                    // Não, SKU/PDV geralmente considera tudo que foi movimentado.
                    // Porém, para consistência, se o cliente não conta como "Atendido", seus SKUs deveriam contar?
                    // Normalmente SKU/PDV é (Total SKUs Movimentados) / (Total Clientes Atendidos).
                    // Vamos manter o rastreamento aqui, mas usar o denominador corrigido.
                    if (!clientUniqueSkus.has(sale.CODCLI)) {
                        clientUniqueSkus.set(sale.CODCLI, new Set());
                    }
                    clientUniqueSkus.get(sale.CODCLI).add(sale.PRODUTO);
                }
            });

            clientTotalSales.forEach((total, codCli) => {
                if (total >= 1) {
                    positiveClients.add(codCli);
                }
            });
            summary.positivacaoCount = positiveClients.size;

            let totalSkus = 0;
            // Somar a quantidade de SKUs ÚNICOS por cliente
            clientUniqueSkus.forEach(skus => {
                totalSkus += skus.size;
            });

            data.forEach(item => {
                const vlVenda = Number(item.VLVENDA) || 0;
                const totPesoLiq = Number(item.TOTPESOLIQ) || 0;

                summary.totalFaturamento += vlVenda;
                summary.totalPeso += totPesoLiq;

                const isForbidden = (str) => !str || FORBIDDEN_KEYS.includes(str.trim().toUpperCase());

                const vendedor = item.NOME || 'N/A';
                if (!isForbidden(vendedor)) {
                    summary.vendasPorVendedor[vendedor] = (summary.vendasPorVendedor[vendedor] || 0) + vlVenda;
                }

                const supervisor = item.SUPERV || 'N/A';
                if (!isForbidden(supervisor)) {
                    summary.vendasPorSupervisor[supervisor] = (summary.vendasPorSupervisor[supervisor] || 0) + vlVenda;
                }

                const produto = item.DESCRICAO || 'N/A';
                const codigo = item.PRODUTO || 'N/A';
                if (!salesByProduct[produto]) salesByProduct[produto] = { faturamento: 0, peso: 0, codigo: codigo };
                salesByProduct[produto].faturamento += vlVenda;
                salesByProduct[produto].peso += totPesoLiq;

                let fornecedorLabel;
                if(isFiltered){
                    const fornecedorNome = item.FORNECEDOR || 'N/A';
                    const codFor = item.CODFOR;
                    fornecedorLabel = `${fornecedorNome} - ${codFor}`;
                } else {
                     fornecedorLabel = item.OBSERVACAOFOR || 'N/A';
                }

                if (!isForbidden(fornecedorLabel)) {
                    const currentTotal = faturamentoMap.get(fornecedorLabel) || 0;
                    faturamentoMap.set(fornecedorLabel, currentTotal + vlVenda);
                }
            });

            const totalRelevantClients = clientBaseForPositivacao.length;
            summary.positivacaoPercent = totalRelevantClients > 0 ? (summary.positivacaoCount / totalRelevantClients) * 100 : 0;
            // O cálculo do SKU/PDV agora usa a nova contagem de SKUs e a nova contagem de positivação
            summary.skuPdv = summary.positivacaoCount > 0 ? totalSkus / summary.positivacaoCount : 0;
            // --- FIM DA MODIFICAÇÃO ---

            summary.faturamentoPorFornecedor = Object.fromEntries(faturamentoMap);
            summary.top10ProdutosFaturamento = Object.entries(salesByProduct).sort(([,a],[,b]) => b.faturamento - a.faturamento).slice(0, 10).map(([p, d]) => ({ produto: p, ...d }));
            summary.top10ProdutosPeso = Object.entries(salesByProduct).sort(([,a],[,b]) => b.peso - a.peso).slice(0, 10).map(([p, d]) => ({ produto: p, ...d }));
            return summary;
        }

        const isObject = obj => obj && typeof obj === 'object' && !Array.isArray(obj);
        const mergeDeep = (...objects) => {
            return objects.reduce((prev, obj) => {
                Object.keys(obj).forEach(key => {
                    const pVal = prev[key];
                    const oVal = obj[key];
                    if (isObject(pVal) && isObject(oVal)) prev[key] = mergeDeep(pVal, oVal);
                    else prev[key] = oVal;
                });
                return prev;
            }, {});
        };

        function createChart(canvasId, type, labels, chartData, optionsOverrides = {}, pluginsToRegister = []) {
            const container = document.getElementById(canvasId + 'Container');
            if (!container) {
                console.error(`Chart container not found for id: ${canvasId}Container`);
                return;
            }

            if (pluginsToRegister.length > 0) {
                try { Chart.register(...pluginsToRegister); } catch (e) {}
            }

            const isLightMode = document.documentElement.classList.contains('light');
            const textColor = isLightMode ? '#1e293b' : '#cbd5e1'; // slate-800 vs slate-300
            const tickColor = isLightMode ? '#475569' : '#94a3b8'; // slate-600 vs slate-400
            const gridColor = isLightMode ? 'rgba(0, 0, 0, 0.05)' : 'rgba(255, 255, 255, 0.05)';

            const professionalPalette = ['#14b8a6', '#6366f1', '#ec4899', '#f97316', '#8b5cf6', '#06b6d4', '#f59e0b', '#ef4444', '#22c55e'];

            let finalDatasets;
            if (Array.isArray(chartData) && chartData.length > 0 && typeof chartData[0] === 'object' && chartData[0].hasOwnProperty('label')) {
                finalDatasets = chartData.map((dataset, index) => ({ ...dataset, backgroundColor: dataset.backgroundColor || professionalPalette[index % professionalPalette.length], borderColor: dataset.borderColor || professionalPalette[index % professionalPalette.length] }));
            } else {
                 finalDatasets = [{ data: chartData || [], backgroundColor: canvasId === 'customerStatusChart' ? ['#2dd4bf', '#f59e0b'] : professionalPalette }];
            }

            let baseOptions = {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: { top: 25 } },
                plugins: {
                    legend: { display: false, labels: {color: textColor} },
                    datalabels: { display: false },
                    tooltip: {
                        backgroundColor: isLightMode ? '#ffffff' : '#1e293b',
                        titleColor: isLightMode ? '#0f172a' : '#f1f5f9',
                        bodyColor: isLightMode ? '#334155' : '#cbd5e1',
                        borderColor: isLightMode ? '#e2e8f0' : '#334155',
                        borderWidth: 1,
                    }
                },
                scales: {
                    y: { beginAtZero: true, grace: '5%', ticks: { color: tickColor }, grid: { color: gridColor} },
                    x: { ticks: { color: tickColor }, grid: { color: gridColor} }
                }
            };

            let typeDefaults = {};
            if (type === 'bar') typeDefaults = { layout: { padding: { right: 30, top: 30 } }, plugins: { datalabels: { display: true, anchor: 'end', align: 'end', offset: -4, color: textColor, font: { size: 10 }, formatter: (v) => (v > 1000 ? (v/1000).toFixed(1) + 'k' : v.toFixed(0)) } } };
            if (type === 'doughnut') typeDefaults = { maintainAspectRatio: true, scales: { y: { display: false }, x: { display: false } }, plugins: { legend: { position: 'top', labels: { color: textColor } }, datalabels: { display: true, color: '#fff', font: { size: 11, weight: 'bold' }, formatter: (v, ctx) => { const total = ctx.chart.data.datasets[0].data.reduce((a, b) => a + b, 0); if(total === 0 || v === 0) return ''; const p = (v / total) * 100; return p > 5 ? p.toFixed(0) + '%' : ''; } } } };

            // 1. Sempre construir um objeto de opções novo e limpo
            const options = mergeDeep({}, baseOptions, typeDefaults, optionsOverrides);

            if (charts[canvasId]) {
                charts[canvasId].data.labels = labels;
                charts[canvasId].data.datasets = finalDatasets;
                // 2. Substituir as opções antigas pelas novas, em vez de tentar um merge
                charts[canvasId].options = options;
                charts[canvasId].update();
                return;
            }

            container.innerHTML = '';
            const newCanvas = document.createElement('canvas');
            newCanvas.id = canvasId;
            container.appendChild(newCanvas);
            container.style.display = ''; container.style.alignItems = ''; container.style.justifyContent = '';
            const ctx = newCanvas.getContext('2d');

            charts[canvasId] = new Chart(ctx, { type, data: { labels, datasets: finalDatasets }, options });
        }

        function showNoDataMessage(canvasId, message) {
            if (charts[canvasId]) {
                charts[canvasId].destroy();
                delete charts[canvasId];
            }
            const container = document.getElementById(canvasId + 'Container');
            if(container) {
                container.style.display = 'flex'; container.style.alignItems = 'center'; container.style.justifyContent = 'center';
                container.innerHTML = `<p class="text-slate-500">${message}</p>`;
            }
        }

        function updateProductBarChart(summary) {
            const metric = currentProductMetric;
            const data = metric === 'faturamento' ? summary.top10ProdutosFaturamento : summary.top10ProdutosPeso;
            const labels = data.map(p => `(${p.codigo}) ${p.produto}`);
            const values = data.map(p => p[metric]);
            createChart('salesByProductBarChart', 'bar', labels, values);
        }

        function renderTable(data) {
            const tableBody = document.getElementById('report-table-body');
            if (!tableBody) return;

            mainTableState.filteredData = data;
            mainTableState.totalPages = Math.ceil(data.length / mainTableState.itemsPerPage);
            if (mainTableState.currentPage > mainTableState.totalPages && mainTableState.totalPages > 0) {
                mainTableState.currentPage = mainTableState.totalPages;
            } else if (mainTableState.totalPages === 0) {
                 mainTableState.currentPage = 1;
            }

            const startIndex = (mainTableState.currentPage - 1) * mainTableState.itemsPerPage;
            const endIndex = startIndex + mainTableState.itemsPerPage;
            const pageData = data.slice(startIndex, endIndex);

            const getPosicaoBadge = (posicao) => {
                if (!posicao) return document.createTextNode('-');
                let classes = '';
                switch (posicao) {
                    case 'L': classes = 'bg-green-500/20 text-green-300'; break;
                    case 'M': classes = 'bg-blue-500/20 text-blue-300'; break;
                    case 'F': classes = 'bg-yellow-500/20 text-yellow-300'; break;
                    default: classes = 'bg-slate-500/20 text-slate-300';
                }

                const span = document.createElement('span');
                span.className = `inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${classes}`;
                span.textContent = posicao;
                return span;
            };

            // Otimização: Usar DocumentFragment para renderização eficiente
            const fragment = document.createDocumentFragment();

            pageData.forEach(row => {
                const tr = document.createElement('tr');
                tr.className = "hover:bg-slate-700 transition-colors";

                const createLink = (text, dataAttr, dataVal) => {
                    const a = document.createElement('a');
                    a.href = "#";
                    a.className = "text-teal-400 hover:underline";
                    a.dataset[dataAttr] = escapeHtml(dataVal);
                    a.textContent = text; // textContent automatically escapes
                    return a;
                };

                const tdPedido = document.createElement('td');
                tdPedido.className = "px-4 py-2";
                tdPedido.appendChild(createLink(row.PEDIDO, 'pedidoId', row.PEDIDO));
                tr.appendChild(tdPedido);

                const tdCodCli = document.createElement('td');
                tdCodCli.className = "px-4 py-2";
                tdCodCli.appendChild(createLink(row.CODCLI, 'codcli', row.CODCLI));
                tr.appendChild(tdCodCli);

                const tdVendedor = document.createElement('td');
                tdVendedor.className = "px-4 py-2";
                tdVendedor.textContent = getFirstName(row.NOME); // textContent escapes
                tr.appendChild(tdVendedor);

                const tdForn = document.createElement('td');
                tdForn.className = "px-4 py-2";
                tdForn.textContent = row.FORNECEDORES_STR || ''; // textContent escapes
                tr.appendChild(tdForn);

                const tdDtPed = document.createElement('td');
                tdDtPed.className = "px-4 py-2";
                tdDtPed.textContent = formatDate(row.DTPED);
                tr.appendChild(tdDtPed);

                const tdDtSaida = document.createElement('td');
                tdDtSaida.className = "px-4 py-2";
                tdDtSaida.textContent = formatDate(row.DTSAIDA);
                tr.appendChild(tdDtSaida);

                const tdPeso = document.createElement('td');
                tdPeso.className = "px-4 py-2 text-right";
                tdPeso.textContent = (Number(row.TOTPESOLIQ) || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 }) + ' Kg';
                tr.appendChild(tdPeso);

                const tdValor = document.createElement('td');
                tdValor.className = "px-4 py-2 text-right";
                tdValor.textContent = (Number(row.VLVENDA) || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                tr.appendChild(tdValor);

                const tdPosicao = document.createElement('td');
                tdPosicao.className = "px-4 py-2 text-center";
                const badge = getPosicaoBadge(row.POSICAO);
                if (typeof badge === 'string') tdPosicao.textContent = badge; // Change innerHTML to textContent if string
                else tdPosicao.appendChild(badge);
                tr.appendChild(tdPosicao);

                fragment.appendChild(tr);
            });

            tableBody.innerHTML = '';
            tableBody.appendChild(fragment);

            if (data.length > 0) {
                pageInfoText.textContent = `Página ${mainTableState.currentPage} de ${mainTableState.totalPages} (Total: ${data.length} pedidos)`;
                prevPageBtn.disabled = mainTableState.currentPage === 1;
                nextPageBtn.disabled = mainTableState.currentPage === mainTableState.totalPages;
                tablePaginationControls.classList.remove('hidden');
            } else {
                pageInfoText.textContent = 'Nenhum pedido encontrado.';
                prevPageBtn.disabled = true;
                nextPageBtn.disabled = true;
                tablePaginationControls.classList.add('hidden');
            }
        }

        function isHoliday(date, holidays) {
            if (!date || !holidays) return false;
            const dateString = date.toISOString().split('T')[0];
            return holidays.includes(dateString);
        }

        function getWorkingDaysInMonth(year, month, holidays) {
            let count = 0;
            const date = new Date(Date.UTC(year, month, 1));
            while (date.getUTCMonth() === month) {
                const dayOfWeek = date.getUTCDay();
                if (dayOfWeek >= 1 && dayOfWeek <= 5 && !isHoliday(date, holidays)) {
                    count++;
                }
                date.setUTCDate(date.getUTCDate() + 1);
            }
            return count;
        }

        function getPassedWorkingDaysInMonth(year, month, holidays, today) {
            let count = 0;
            const date = new Date(Date.UTC(year, month, 1));
            // Ensure today is treated as UTC for comparison
            const todayUTC = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));

            while (date <= todayUTC && date.getUTCMonth() === month) {
                const dayOfWeek = date.getUTCDay();
                if (dayOfWeek >= 1 && dayOfWeek <= 5 && !isHoliday(date, holidays)) {
                    count++;
                }
                date.setUTCDate(date.getUTCDate() + 1);
            }
            return count > 0 ? count : 1;
        }


        function updateTrendChart(currentSales, historicalSales) {
            const currentYear = lastSaleDate.getUTCFullYear();
            const currentMonth = lastSaleDate.getUTCMonth();
            const totalWorkingDays = getWorkingDaysInMonth(currentYear, currentMonth, selectedHolidays);
            const passedWorkingDays = getPassedWorkingDaysInMonth(currentYear, currentMonth, selectedHolidays, lastSaleDate);

            const monthCount = getUniqueMonthCount(historicalSales);

            // Robust calculation: handle potential undefined/NaN in full dataset
            const pastQuarterRevenue = historicalSales.reduce((sum, sale) => sum + (Number(sale.VLVENDA) || 0), 0);
            let averageMonthlyRevenue = pastQuarterRevenue / QUARTERLY_DIVISOR;
            if (isNaN(averageMonthlyRevenue)) averageMonthlyRevenue = 0;

            const currentMonthRevenue = currentSales.reduce((sum, sale) => sum + (Number(sale.VLVENDA) || 0), 0);
            let trend = passedWorkingDays > 0 && totalWorkingDays > 0 ? (currentMonthRevenue / passedWorkingDays) * totalWorkingDays : 0;
            if (isNaN(trend)) trend = 0;

            const lineChartDataset = [{ label: 'Valor', data: [averageMonthlyRevenue, trend], fill: true, borderColor: '#14b8a6', backgroundColor: 'rgba(20, 184, 166, 0.1)', tension: 0.2, pointRadius: 6, pointBackgroundColor: '#14b8a6', pointBorderColor: '#FFF', pointBorderWidth: 2, pointHoverRadius: 8 }];

            const formatLabelValue = (v) => {
                if (typeof v !== 'number' || isNaN(v)) return ''; // Proteção contra valores não numéricos
                return (v >= 1000000 ? (v / 1000000).toFixed(2) + ' M' : (v / 1000).toFixed(0) + 'k');
            };

            createChart('trendChart', 'line', ['Média Faturamento Trimestre', 'Tendência Mês Atual'], lineChartDataset, {
                layout: { padding: { top: 30 } },
                plugins: {
                    legend: { display: false },
                    datalabels: { display: true, anchor: 'end', align: 'top', offset: 8, color: '#cbd5e1', font: { size: 12, weight: 'bold' }, formatter: formatLabelValue }
                },
                scales: {
                    y: { ticks: { callback: function(value) { if (value >= 1000000) return 'R$ ' + (value / 1000000).toFixed(1) + 'M'; return 'R$ ' + (value / 1000) + 'k'; } } },
                    x: { grid: { display: false } }
                }
            });
        }

        function updateAllVisuals() {
            const posicao = posicaoFilter.value;
            const codcli = codcliFilter.value.trim();

            let clientBaseForCoverage = allClientsData.filter(c => {
                const rca1 = String(c.rca1 || '').trim();

                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');

                // Regra de inclusão (Americanas ou RCA 1 diferente de 53)
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(c['Código']));
            });

            if (mainRedeGroupFilter === 'com_rede') {
                clientBaseForCoverage = clientBaseForCoverage.filter(c => c.ramo && c.ramo !== 'N/A');
                if (selectedMainRedes.length > 0) {
                    clientBaseForCoverage = clientBaseForCoverage.filter(c => selectedMainRedes.includes(c.ramo));
                }
            } else if (mainRedeGroupFilter === 'sem_rede') {
                clientBaseForCoverage = clientBaseForCoverage.filter(c => !c.ramo || c.ramo === 'N/A');
            }
            const clientCodesInRede = new Set(clientBaseForCoverage.map(c => c['Código']));

            const intersectSets = (sets) => {
                if (sets.length === 0) return new Set();

                // --- OPTIMIZATION START ---
                // Sort sets by size to intersect the smallest sets first.
                sets.sort((a, b) => a.size - b.size);

                let result = new Set(sets[0]);
                for (let i = 1; i < sets.length; i++) {
                    if (result.size === 0) break; // Stop early if the result is already empty

                    const currentSet = sets[i];
                    for (const id of result) {
                        if (!currentSet.has(id)) {
                            result.delete(id);
                        }
                    }
                }
                // --- OPTIMIZATION END ---
                return result;
            };

            const getFilteredIds = (indices, dataset) => {
                let setsToIntersect = [];
                let hasFilter = false;

                if (codcli) {
                    hasFilter = true;
                    if (indices.byClient.has(codcli)) {
                        setsToIntersect.push(indices.byClient.get(codcli));
                    } else {
                        return [];
                    }
                }
                    if (selectedMainSupervisors.length > 0) {
                        hasFilter = true;
                        const supIds = new Set();
                        selectedMainSupervisors.forEach(sup => {
                            if (indices.bySupervisor.has(sup)) {
                                (indices.bySupervisor.get(sup) || []).forEach(id => supIds.add(id));
                            }
                        });
                        setsToIntersect.push(supIds);
                    }
                    if (selectedSellers.length > 0) {
                        hasFilter = true;
                        const sellerIds = new Set();
                        selectedSellers.forEach(seller => {
                            (indices.byRca.get(seller) || []).forEach(id => sellerIds.add(id));
                        });
                        setsToIntersect.push(sellerIds);
                    }

                if (selectedTiposVenda.length > 0) {
                    hasFilter = true;
                    const tipoVendaIds = new Set();
                    selectedTiposVenda.forEach(tipo => {
                        (indices.byTipoVenda.get(tipo) || []).forEach(id => tipoVendaIds.add(id));
                    });
                    setsToIntersect.push(tipoVendaIds);
                }

                if (currentFornecedor) {
                    hasFilter = true;
                    if (indices.byPasta.has(currentFornecedor)) {
                        setsToIntersect.push(indices.byPasta.get(currentFornecedor));
                    } else {
                        return [];
                    }
                }
                if (selectedMainSuppliers.length > 0) {
                    hasFilter = true;
                    const supplierIds = new Set();
                    selectedMainSuppliers.forEach(sup => {
                        if (indices.bySupplier.has(sup)) {
                            (indices.bySupplier.get(sup) || []).forEach(id => supplierIds.add(id));
                        }
                    });
                    setsToIntersect.push(supplierIds);
                }

                if (indices.byPosition && posicao) {
                    hasFilter = true;
                    if (indices.byPosition.has(posicao)) {
                        setsToIntersect.push(indices.byPosition.get(posicao));
                    } else {
                        return [];
                    }
                }

                if (mainRedeGroupFilter) {
                    hasFilter = true;
                    const redeIds = new Set();
                    clientCodesInRede.forEach(clientCode => {
                         (indices.byClient.get(clientCode) || []).forEach(id => redeIds.add(id));
                    });
                    setsToIntersect.push(redeIds);
                }

                if (setsToIntersect.length === 0 && hasFilter) {
                    return [];
                }

                // Helper to retrieve item from dataset (ColumnarDataset or Array)
                const getItem = (idx) => (dataset.get ? dataset.get(idx) : dataset[idx]);

                if (setsToIntersect.length === 0 && !hasFilter) {
                    // No filters: return all items
                    // Check if it is a ColumnarDataset specifically to avoid calling .values() on native Array (which returns iterator)
                    if (dataset instanceof ColumnarDataset) {
                        return dataset.values();
                    }
                    if (Array.isArray(dataset)) return dataset;

                    // Fallback iteration (safe for array-like objects)
                    const all = [];
                    for(let i=0; i<dataset.length; i++) all.push(getItem(i));
                    return all;
                }

                const finalIds = intersectSets(setsToIntersect);
                // finalIds are indices (integers). Use getItem to retrieve the object/proxy.
                return Array.from(finalIds).map(id => getItem(id));
            };

            const filteredSalesData = getFilteredIds(optimizedData.indices.current, optimizedData.salesById);
            const filteredHistoryData = getFilteredIds(optimizedData.indices.history, optimizedData.historyById);

            const filteredTableData = aggregatedOrders.filter(order => {
                let matches = true;
                if (mainRedeGroupFilter) {
                    matches = matches && clientCodesInRede.has(order.CODCLI);
                }
                if (codcli) matches = matches && order.CODCLI === codcli;
                else {
                    if (selectedMainSupervisors.length > 0) matches = matches && selectedMainSupervisors.includes(order.SUPERV);
                    if (selectedSellers.length > 0) matches = matches && selectedSellers.includes(order.NOME);
                }
                // Robust filtering with existence checks
                if (selectedTiposVenda.length > 0) matches = matches && order.TIPOVENDA && selectedTiposVenda.includes(order.TIPOVENDA);
                if (currentFornecedor) matches = matches && order.FORNECEDORES_LIST && order.FORNECEDORES_LIST.includes(currentFornecedor);
                if (selectedMainSuppliers.length > 0) matches = matches && order.CODFORS_LIST && order.CODFORS_LIST.some(c => selectedMainSuppliers.includes(c));
                if (posicao) matches = matches && order.POSICAO === posicao;
                return matches;
            });

            const isFiltered = selectedMainSupervisors.length > 0 || selectedSellers.length > 0 || !!codcli || !!currentFornecedor || !!mainRedeGroupFilter || selectedMainSuppliers.length > 0 || !!posicao || selectedTiposVenda.length > 0;

            const summary = calculateSummaryFromData(filteredSalesData, isFiltered, clientBaseForCoverage);

            totalVendasEl.textContent = summary.totalFaturamento.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
            totalPesoEl.textContent = `${(summary.totalPeso / 1000).toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 })}`;
            kpiSkuPdVEl.textContent = summary.skuPdv.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            kpiPositivacaoEl.textContent = `${summary.positivacaoPercent.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
            kpiPositivacaoPercentEl.textContent = `${summary.positivacaoCount.toLocaleString('pt-BR')} PDVs`;


            if (!tableView.classList.contains('hidden')) {
                renderTable(filteredTableData);
            }

            if (!chartView.classList.contains('hidden')) {
                updateTrendChart(filteredSalesData, filteredHistoryData);
                const totalForPercentage = selectedMainSupervisors.length > 0 ? Object.values(summary.vendasPorVendedor).reduce((a, b) => a + b, 0) : Object.values(summary.vendasPorSupervisor).reduce((a, b) => a + b, 0);
                const personChartTooltipOptions = { plugins: { tooltip: { callbacks: { label: function(context) { let label = context.dataset.label || ''; if (label) label += ': '; const value = context.parsed.y; if (value !== null) { label += new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value); if (totalForPercentage > 0) { const percentage = ((value / totalForPercentage) * 100).toFixed(2); label += ` (${percentage}%)`; } } return label; } } } } };
                if (selectedMainSupervisors.length > 0) {
                    salesByPersonTitle.textContent = 'Vendas por Vendedor';
                    createChart('salesByPersonChart', 'bar', Object.keys(summary.vendasPorVendedor).map(getFirstName), Object.values(summary.vendasPorVendedor), personChartTooltipOptions);
                } else {
                    salesByPersonTitle.textContent = 'Vendas por Supervisor';
                    createChart('salesByPersonChart', 'bar', Object.keys(summary.vendasPorSupervisor), Object.values(summary.vendasPorSupervisor), personChartTooltipOptions);
                }

                document.getElementById('faturamentoPorFornecedorTitle').textContent = isFiltered ? 'Faturamento por Fornecedor' : 'Faturamento por Categoria';
                const faturamentoPorFornecedorData = summary.faturamentoPorFornecedor;
                const totalFaturamentoFornecedor = Object.values(faturamentoPorFornecedorData).reduce((a, b) => a + b, 0);
                const fornecedorTooltipOptions = { indexAxis: 'y', plugins: { tooltip: { callbacks: { label: function(context) { let label = context.label || ''; if (label) label += ': '; const value = context.parsed.x; if (value !== null) { label += new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value); if (totalFaturamentoFornecedor > 0) { const percentage = ((value / totalFaturamentoFornecedor) * 100).toFixed(2); label += ` (${percentage}%)`; } } return label; } } } } };
                const sortedFornecedores = Object.entries(faturamentoPorFornecedorData).sort(([, a], [, b]) => a - b);
                if (sortedFornecedores.length > 0) createChart('faturamentoPorFornecedorChart', 'bar', sortedFornecedores.map(([name]) => name), sortedFornecedores.map(([, total]) => total), fornecedorTooltipOptions);
                else showNoDataMessage('faturamentoPorFornecedorChart', 'Sem dados de faturamento.');
                updateProductBarChart(summary);
            }
        }

        function updateSupervisorFilter(dropdown, filterText, selectedArray, dataSource, skipRender = false) {
            if (!dropdown || !filterText) return selectedArray;
            const forbidden = ['SUPERV', 'CODUSUR', 'CODSUPERVISOR', 'NOME', 'CODCLI', 'PRODUTO', 'DESCRICAO', 'FORNECEDOR', 'OBSERVACAOFOR', 'CODFOR', 'QTVENDA', 'VLVENDA', 'VLBONIFIC', 'TOTPESOLIQ', 'ESTOQUEUNIT', 'TIPOVENDA', 'FILIAL', 'ESTOQUECX', 'SUPERVISOR'];
            const supervisors = [...new Set(dataSource.map(item => item.SUPERV).filter(s => s && !forbidden.includes(s.toUpperCase())))].sort();

            selectedArray = selectedArray.filter(sup => supervisors.includes(sup));

            if (!skipRender) {
                const htmlParts = [];
                for (let i = 0; i < supervisors.length; i++) {
                    const sup = supervisors[i];
                    const isChecked = selectedArray.includes(sup);
                    htmlParts.push(`<label class="flex items-center p-2 hover:bg-slate-600 cursor-pointer"><input type="checkbox" class="form-checkbox h-4 w-4 bg-slate-800 border-slate-500 rounded text-teal-500 focus:ring-teal-500" value="${sup}" ${isChecked ? 'checked' : ''}><span class="ml-2">${sup}</span></label>`);
                }
                dropdown.innerHTML = htmlParts.join('');
            }

            if (selectedArray.length === 0 || selectedArray.length === supervisors.length) filterText.textContent = 'Todos';
            else if (selectedArray.length === 1) filterText.textContent = selectedArray[0];
            else filterText.textContent = `${selectedArray.length} selecionados`;
            return selectedArray;
        }

        function updateSellerFilter(supervisors, dropdown, filterText, selectedArray, dataSource, skipRender = false) {
            const forbidden = ['NOME', 'VENDEDOR', 'SUPERV', 'CODUSUR', 'CODCLI', 'SUPERVISOR'];
            let sellersToShow;
            if (supervisors && supervisors.length > 0) {
                const supSet = new Set(supervisors);
                sellersToShow = [...new Set(dataSource.filter(s => supSet.has(s.SUPERV)).map(s => s.NOME).filter(n => n && !forbidden.includes(n.toUpperCase())))].sort();
            } else {
                sellersToShow = [...new Set(dataSource.map(item => item.NOME).filter(n => n && !forbidden.includes(n.toUpperCase())))].sort();
            }

            selectedArray = selectedArray.filter(seller => sellersToShow.includes(seller));

            if (!skipRender) {
                const htmlParts = [];
                for (let i = 0; i < sellersToShow.length; i++) {
                    const s = sellersToShow[i];
                    const isChecked = selectedArray.includes(s);
                    htmlParts.push(`<label class="flex items-center p-2 hover:bg-slate-600 cursor-pointer"><input type="checkbox" class="form-checkbox h-4 w-4 bg-slate-800 border-slate-500 rounded text-teal-500 focus:ring-teal-500" value="${s}" ${isChecked ? 'checked' : ''}><span class="ml-2">${s}</span></label>`);
                }
                dropdown.innerHTML = htmlParts.join('');
            }

            if (selectedArray.length === 0 || selectedArray.length === sellersToShow.length) filterText.textContent = 'Todos Vendedores';
            else if (selectedArray.length === 1) filterText.textContent = selectedArray[0];
            else filterText.textContent = `${selectedArray.length} vendedores selecionados`;
            return selectedArray;
        }

        function updateTipoVendaFilter(dropdown, filterText, selectedArray, dataSource, skipRender = false) {
            // Collect unique types from data source
            const forbidden = ['TIPOVENDA', 'TIPO VENDA', 'TIPO', 'CODUSUR', 'CODCLI', 'SUPERV', 'NOME'];
            const uniqueTypes = new Set(dataSource.map(item => item.TIPOVENDA).filter(t => t && !forbidden.includes(t.toUpperCase())));

            // Ensure currently selected items are kept in the list (Safety Net)
            selectedArray.forEach(type => uniqueTypes.add(type));

            const tiposVendaToShow = [...uniqueTypes].sort((a, b) => parseInt(a) - parseInt(b));

            // Re-filter selectedArray ensures we don't have stale data if we wanted strictness,
            // but here we just ensured they are IN the list, so this line effectively does nothing
            // except ordering or removing truly invalid ones if we didn't add them above.
            // Since we added them above, this is redundant but harmless.
            selectedArray = selectedArray.filter(tipo => tiposVendaToShow.includes(tipo));

            if (!skipRender) {
                const htmlParts = [];
                for (let i = 0; i < tiposVendaToShow.length; i++) {
                    const s = tiposVendaToShow[i];
                    const isChecked = selectedArray.includes(s);
                    htmlParts.push(`<label class="flex items-center p-2 hover:bg-slate-600 cursor-pointer"><input type="checkbox" class="form-checkbox h-4 w-4 bg-slate-800 border-slate-500 rounded text-teal-500 focus:ring-teal-500" value="${s}" ${isChecked ? 'checked' : ''}><span class="ml-2">${s}</span></label>`);
                }
                dropdown.innerHTML = htmlParts.join('');
            }

            if (selectedArray.length === 0 || selectedArray.length === tiposVendaToShow.length) filterText.textContent = 'Todos os Tipos';
            else if (selectedArray.length === 1) filterText.textContent = selectedArray[0];
            else filterText.textContent = `${selectedArray.length} tipos selecionados`;
            return selectedArray;
        }

        function updateRedeFilter(dropdown, buttonTextElement, selectedArray, dataSource, baseText = 'Com Rede') {
            const forbidden = ['RAMO', 'RAMO DE ATIVIDADE', 'RAMO_ATIVIDADE', 'DESCRICAO', 'ATIVIDADE'];
            const redesToShow = [...new Set(dataSource.map(item => item.ramo).filter(r => r && r !== 'N/A' && !forbidden.includes(r.toUpperCase())))].sort();
            const validSelected = selectedArray.filter(rede => redesToShow.includes(rede));

            const htmlParts = [];
            for (let i = 0; i < redesToShow.length; i++) {
                const r = redesToShow[i];
                const isChecked = validSelected.includes(r);
                htmlParts.push(`<label class="flex items-center p-2 hover:bg-slate-600 cursor-pointer"><input type="checkbox" class="form-checkbox h-4 w-4 bg-slate-800 border-slate-500 rounded text-teal-500 focus:ring-teal-500" value="${r}" ${isChecked ? 'checked' : ''}><span class="ml-2 text-sm">${r}</span></label>`);
            }
            dropdown.innerHTML = htmlParts.join('');

            if (validSelected.length === 0) {
                buttonTextElement.textContent = baseText;
            } else {
                buttonTextElement.textContent = `${baseText} (${validSelected.length})`;
            }
            return validSelected;
        }

        function resetMainFilters() {
            selectedMainSupervisors = [];
            selectedMainSuppliers = [];
            posicaoFilter.value = '';
            codcliFilter.value = '';
            currentFornecedor = '';
            selectedSellers = [];
            selectedTiposVenda = [];
            mainRedeGroupFilter = '';
            selectedMainRedes = [];
            mainTableState.currentPage = 1;

            mainRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            mainRedeGroupContainer.querySelector('button[data-group=""]').classList.add('active');
            updateRedeFilter(mainRedeFilterDropdown, mainComRedeBtnText, selectedMainRedes, allClientsData);

            document.querySelectorAll('#fornecedor-toggle-container .fornecedor-btn').forEach(b => b.classList.remove('active'));
            selectedMainSupervisors = updateSupervisorFilter(document.getElementById('supervisor-filter-dropdown'), document.getElementById('supervisor-filter-text'), selectedMainSupervisors, allSalesData);
            selectedMainSuppliers = updateSupplierFilter(document.getElementById('fornecedor-filter-dropdown'), document.getElementById('fornecedor-filter-text'), selectedMainSuppliers, [...allSalesData, ...allHistoryData], 'main');
            selectedSellers = updateSellerFilter(selectedMainSupervisors, vendedorFilterDropdown, vendedorFilterText, selectedSellers, allSalesData);
            selectedTiposVenda = updateTipoVendaFilter(tipoVendaFilterDropdown, tipoVendaFilterText, selectedTiposVenda, allSalesData);
            updateAllVisuals();
        }

        function resetCityFilters() {
            selectedCitySupervisors = [];
            cityNameFilter.value = '';
            cityCodCliFilter.value = '';
            selectedCitySellers = [];
            selectedCityRedes = [];
            cityRedeGroupFilter = '';
            selectedCityTiposVenda = [];
            selectedCitySuppliers = [];

            selectedCitySupervisors = updateSupervisorFilter(document.getElementById('city-supervisor-filter-dropdown'), document.getElementById('city-supervisor-filter-text'), selectedCitySupervisors, allSalesData);
            selectedCitySellers = updateSellerFilter(selectedCitySupervisors, cityVendedorFilterDropdown, cityVendedorFilterText, selectedCitySellers, allSalesData);
            selectedCityTiposVenda = updateTipoVendaFilter(cityTipoVendaFilterDropdown, cityTipoVendaFilterText, selectedCityTiposVenda, allSalesData);
            selectedCitySuppliers = updateSupplierFilter(document.getElementById('city-supplier-filter-dropdown'), document.getElementById('city-supplier-filter-text'), selectedCitySuppliers, [...allSalesData, ...allHistoryData], 'city');

            cityRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            cityRedeGroupContainer.querySelector('button[data-group=""]').classList.add('active');
            updateRedeFilter(cityRedeFilterDropdown, cityComRedeBtnText, selectedCityRedes, allClientsData);

            updateCityView();
        }

        function resetWeeklyFilters() {
            selectedWeeklySupervisors = [];
            selectedWeeklySellers = [];
            currentWeeklyFornecedor = '';
            document.querySelectorAll('#weekly-fornecedor-toggle-container .fornecedor-btn').forEach(b => b.classList.remove('active'));

            // Re-populate will handle resetting the dropdown UI text and checkboxes based on empty selected arrays
            populateWeeklyFilters();
            updateWeeklyView();
        }

        function resetComparisonFilters() {
            selectedComparisonSupervisors = [];
            comparisonFilialFilter.value = 'ambas';
            currentComparisonFornecedor = '';
            comparisonCityFilter.value = '';
            selectedComparisonSellers = [];
            selectedComparisonSuppliers = [];
            selectedComparisonProducts = [];
            selectedComparisonTiposVenda = [];
            comparisonRedeGroupFilter = '';
            selectedComparisonRedes = [];

            comparisonRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            comparisonRedeGroupContainer.querySelector('button[data-group=""]').classList.add('active');
            updateRedeFilter(comparisonRedeFilterDropdown, comparisonComRedeBtnText, selectedComparisonRedes, allClientsData);

            document.querySelectorAll('#comparison-fornecedor-toggle-container .fornecedor-btn').forEach(b => b.classList.remove('active'));

            updateAllComparisonFilters();
            updateComparisonView();
        }

        function resetStockFilters() {
            stockFilialFilter.value = 'ambas';
            currentStockFornecedor = '';
            selectedStockSupervisors = [];
            stockCityFilter.value = '';
            selectedStockSellers = [];
            selectedStockSuppliers = [];
            selectedStockProducts = [];
            selectedStockTiposVenda = [];
            stockRedeGroupFilter = '';
            selectedStockRedes = [];
            stockTrendFilter = 'all';

            document.querySelectorAll('.stock-trend-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelector('.stock-trend-btn[data-trend="all"]').classList.add('active');

            stockRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            stockRedeGroupContainer.querySelector('button[data-group=""]').classList.add('active');
            updateRedeFilter(stockRedeFilterDropdown, stockComRedeBtnText, selectedStockRedes, allClientsData, 'Com Rede');

            document.querySelectorAll('#stock-fornecedor-toggle-container .fornecedor-btn').forEach(b => b.classList.remove('active'));

            customWorkingDaysStock = maxWorkingDaysStock;
            const daysInput = document.getElementById('stock-working-days-input');
            if(daysInput) daysInput.value = customWorkingDaysStock;

            handleStockFilterChange();
        }


        function getCityFilteredData(options = {}) {
            const { excludeFilter = null } = options;

            const sellersSet = new Set(selectedCitySellers);
            const cityInput = cityNameFilter.value.trim().toLowerCase();
            const codCli = cityCodCliFilter.value.trim();
            const tiposVendaSet = new Set(selectedCityTiposVenda);

            let clients = allClientsData;

            if (excludeFilter !== 'rede') {
                 if (cityRedeGroupFilter === 'com_rede') {
                    clients = clients.filter(c => c.ramo && c.ramo !== 'N/A');
                    if (selectedCityRedes.length > 0) {
                        const redeSet = new Set(selectedCityRedes);
                        clients = clients.filter(c => redeSet.has(c.ramo));
                    }
                } else if (cityRedeGroupFilter === 'sem_rede') {
                    clients = clients.filter(c => !c.ramo || c.ramo === 'N/A');
                }
            }

            if (excludeFilter !== 'supervisor' && selectedCitySupervisors.length > 0) {
                const rcasSet = new Set();
                selectedCitySupervisors.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => rcasSet.add(rca));
                });
                clients = clients.filter(c => c.rcas.some(r => rcasSet.has(r)));
            }

            if (excludeFilter !== 'seller' && sellersSet.size > 0) {
                const rcasSet = new Set();
                sellersSet.forEach(name => {
                    const code = optimizedData.rcaCodeByName.get(name);
                    if(code) rcasSet.add(code);
                });
                clients = clients.filter(c => c.rcas.some(r => rcasSet.has(r)));
            }

            if (excludeFilter !== 'supplier' && selectedCitySuppliers.length > 0) {
                 // No filtering of clients list based on supplier for now.
            }

            if (excludeFilter !== 'city' && cityInput) {
                clients = clients.filter(c => (c.cidade || c.CIDADE) && (c.cidade || c.CIDADE).toLowerCase() === cityInput);
            }

            if (excludeFilter !== 'codcli' && codCli) {
                 clients = clients.filter(c => String(c['Código']) === codCli);
            }

            const clientCodes = new Set(clients.map(c => c['Código']));

            const supervisorSet = new Set(selectedCitySupervisors);

            const filters = {
                supervisor: supervisorSet,
                seller: sellersSet,
                city: cityInput,
                tipoVenda: tiposVendaSet,
                clientCodes: clientCodes,
                supplier: new Set(selectedCitySuppliers)
            };

            const sales = getFilteredDataFromIndices(optimizedData.indices.current, optimizedData.salesById, filters, excludeFilter);

            return { clients, sales };
        }

        function updateAllCityFilters(options = {}) {
            const { skipFilter = null } = options;

            if (skipFilter !== 'supervisor') {
                const { sales } = getCityFilteredData({ excludeFilter: 'supervisor' });
                selectedCitySupervisors = updateSupervisorFilter(document.getElementById('city-supervisor-filter-dropdown'), document.getElementById('city-supervisor-filter-text'), selectedCitySupervisors, sales);
            }

            const { sales: salesSeller } = getCityFilteredData({ excludeFilter: 'seller' });
            selectedCitySellers = updateSellerFilter(selectedCitySupervisors, cityVendedorFilterDropdown, cityVendedorFilterText, selectedCitySellers, salesSeller, skipFilter === 'seller');

            const { sales: salesTV } = getCityFilteredData({ excludeFilter: 'tipoVenda' });
            selectedCityTiposVenda = updateTipoVendaFilter(cityTipoVendaFilterDropdown, cityTipoVendaFilterText, selectedCityTiposVenda, salesTV, skipFilter === 'tipoVenda');

            const { sales: salesSupplier } = getCityFilteredData({ excludeFilter: 'supplier' });
            selectedCitySuppliers = updateSupplierFilter(document.getElementById('city-supplier-filter-dropdown'), document.getElementById('city-supplier-filter-text'), selectedCitySuppliers, salesSupplier, 'city');

            if (skipFilter !== 'rede') {
                 const { clients: clientsRede } = getCityFilteredData({ excludeFilter: 'rede' });
                 if (cityRedeGroupFilter === 'com_rede') {
                     selectedCityRedes = updateRedeFilter(cityRedeFilterDropdown, cityComRedeBtnText, selectedCityRedes, clientsRede);
                 }
            }
        }

        function handleCityFilterChange(options = {}) {
            if (window.cityUpdateTimeout) clearTimeout(window.cityUpdateTimeout);
            window.cityUpdateTimeout = setTimeout(() => {
                updateAllCityFilters(options);
                updateCityView();
            }, 10);
        }

        function updateCitySuggestions(filterInput, suggestionsContainer, dataSource) {
            const forbidden = ['CIDADE', 'MUNICIPIO', 'CIDADE_CLIENTE', 'NOME DA CIDADE', 'CITY'];
            const inputValue = filterInput.value.toLowerCase();
            
            if (!inputValue) {
                suggestionsContainer.classList.add('hidden');
                return;
            }

            const uniqueCities = new Set();
            const suggestionsFragment = document.createDocumentFragment();
            let count = 0;
            const LIMIT = 50;

            for (let i = 0; i < dataSource.length; i++) {
                if (count >= LIMIT) break;

                const item = dataSource instanceof ColumnarDataset ? dataSource.get(i) : dataSource[i];
                let city = 'N/A';

                if (item.CIDADE) city = item.CIDADE;
                else if (item.cidade || item.CIDADE) city = item.cidade || item.CIDADE;
                else if (item.CODCLI) {
                    const c = clientMapForKPIs.get(String(item.CODCLI));
                    if (c) city = c.cidade || c.CIDADE || c['Nome da Cidade'];
                }

                if (city && city !== 'N/A' && !forbidden.includes(city.toUpperCase()) && city.toLowerCase().includes(inputValue)) {
                    if (!uniqueCities.has(city)) {
                        uniqueCities.add(city);
                        const div = document.createElement('div');
                        div.className = 'p-2 hover:bg-slate-600 cursor-pointer';
                        div.textContent = city;
                        suggestionsFragment.appendChild(div);
                        count++;
                    }
                }
            }

            if (uniqueCities.size > 0 && (document.activeElement === filterInput || !suggestionsContainer.classList.contains('manual-hide'))) {
                suggestionsContainer.innerHTML = '';
                suggestionsContainer.appendChild(suggestionsFragment);
                suggestionsContainer.classList.remove('hidden');
            } else {
                suggestionsContainer.classList.add('hidden');
            }
        }

        function validateCityFilter() {
            const selectedCity = cityNameFilter.value;
            if (!selectedCity) return;
            const { clients } = getCityFilteredData({ excludeFilter: 'city' });
            const availableCities = [...new Set(clients.map(c => c.cidade).filter(Boolean))];
            if (!availableCities.includes(selectedCity)) cityNameFilter.value = '';
        }

        function updateCityView() {
            cityRenderId++;
            const currentRenderId = cityRenderId;

            updateCityMap();

            let { clients: clientsForAnalysis, sales: salesForAnalysis } = getCityFilteredData();
            const cidadeFiltro = cityNameFilter.value.trim();

            const referenceDate = lastSaleDate;
            const currentMonth = referenceDate.getUTCMonth();
            const currentYear = referenceDate.getUTCFullYear();

            const selectedTiposVendaSet = new Set(selectedCityTiposVenda);

            // Pre-aggregate "Sales This Month" for Status Classification
            const clientTotalsThisMonth = new Map();
            // Sync Pre-aggregation (O(N) is fast)
            for(let i=0; i<allSalesData.length; i++) {
                const s = (allSalesData instanceof ColumnarDataset) ? allSalesData.get(i) : allSalesData[i];
                if (selectedTiposVendaSet.size > 0 && !selectedTiposVendaSet.has(s.TIPOVENDA)) continue;

                const d = parseDate(s.DTPED);
                if (d && d.getUTCFullYear() === currentYear && d.getUTCMonth() === currentMonth) {
                    clientTotalsThisMonth.set(s.CODCLI, (clientTotalsThisMonth.get(s.CODCLI) || 0) + s.VLVENDA);
                }
            }

            const detailedDataByClient = new Map(); // Map<CODCLI, { total, pepsico, multimarcas, maxDate }>

            // Pre-aggregate Sales Data for Analysis (Sync)
            salesForAnalysis.forEach(s => {
                const d = parseDate(s.DTPED);
                if (d) {
                    if (!detailedDataByClient.has(s.CODCLI)) {
                        detailedDataByClient.set(s.CODCLI, { total: 0, pepsico: 0, multimarcas: 0, maxDate: 0 });
                    }
                    const entry = detailedDataByClient.get(s.CODCLI);
                    const ts = d.getTime();

                    if (ts > entry.maxDate) entry.maxDate = ts;

                    if (d.getUTCFullYear() === currentYear && d.getUTCMonth() === currentMonth) {
                        entry.total += s.VLVENDA;
                        if (s.OBSERVACAOFOR === 'PEPSICO') entry.pepsico += s.VLVENDA;
                        else if (s.OBSERVACAOFOR === 'MULTIMARCAS') entry.multimarcas += s.VLVENDA;
                    }
                }
            });

            // Filter clients universe
            clientsForAnalysis = clientsForAnalysis.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(c['Código']));
            });

            // Show Loading
            cityActiveDetailTableBody.innerHTML = '<tr><td colspan="6" class="text-center p-8"><svg class="animate-spin h-8 w-8 text-teal-500 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg></td></tr>';
            cityInactiveDetailTableBody.innerHTML = '<tr><td colspan="6" class="text-center p-8"><svg class="animate-spin h-8 w-8 text-teal-500 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg></td></tr>';

            const activeClientsList = [];
            const inactiveClientsList = [];
            const salesByActiveClient = {}; // Map for export/rendering

            // ASYNC CHUNKED PROCESSING
            runAsyncChunked(clientsForAnalysis, (client) => {
                const codcli = String(client['Código']);

                const registrationDate = parseDate(client.dataCadastro);
                client.isNew = registrationDate && registrationDate.getUTCMonth() === currentMonth && registrationDate.getUTCFullYear() === currentYear;

                const totalFaturamentoMes = clientTotalsThisMonth.get(codcli) || 0;

                if (totalFaturamentoMes >= 1) {
                    activeClientsList.push(client);

                    // Detailed Data check
                    const details = detailedDataByClient.get(codcli);
                    if (details && details.total >= 1) {
                        const outrosTotal = details.total - details.pepsico - details.multimarcas;
                        salesByActiveClient[codcli] = {
                            // Explicit copy to avoid Spread issues with Proxy
                            'Código': client['Código'],
                            fantasia: client.fantasia || client.FANTASIA || client['Nome Fantasia'],
                            razaoSocial: client.razaoSocial || client.RAZAOSOCIAL || client.Cliente,
                            cidade: client.cidade || client.CIDADE || client['Nome da Cidade'],
                            bairro: client.bairro || client.BAIRRO || client['Bairro'],
                            ultimaCompra: details.maxDate || client.ultimaCompra || client['Data da Última Compra'] || client.ULTIMACOMPRA,
                            rcas: client.rcas,
                            isNew: client.isNew,
                            // Metrics
                            total: details.total,
                            pepsico: details.pepsico,
                            multimarcas: details.multimarcas,
                            outros: outrosTotal
                        };
                    }
                } else {
                    if (totalFaturamentoMes < 0) {
                        client.isReturn = true;
                    }
                    client.isNewForInactiveLabel = client.isNew && !parseDate(client.ultimaCompra);
                    inactiveClientsList.push(client);
                }
            }, () => {
                // --- ON COMPLETE (Render) ---
                if (currentRenderId !== cityRenderId) return;

                inactiveClientsForExport = inactiveClientsList;

                if (clientsForAnalysis.length > 0) {
                     const statusChartOptions = { maintainAspectRatio: false, animation: { duration: 800, easing: 'easeOutQuart' }, plugins: { legend: { position: 'bottom', labels: { color: '#cbd5e1' } }, tooltip: { callbacks: { label: function(context) { return context.label; } } }, datalabels: { formatter: (value, ctx) => { const total = ctx.chart.data.datasets[0].data.reduce((a, b) => a + b, 0); if (total === 0 || value === 0) return ''; const percentage = (value * 100 / total).toFixed(1) + "%"; return `${value}\n(${percentage})`; }, color: '#fff', backgroundColor: 'rgba(0, 0, 0, 0.6)', borderRadius: 4, padding: 4, font: { weight: 'bold', size: 12 }, textAlign: 'center' } } };
                    createChart('customerStatusChart', 'doughnut', ['Ativos no Mês', 'S/ Vendas no Mês'], [activeClientsList.length, inactiveClientsList.length], statusChartOptions);
                } else showNoDataMessage('customerStatusChart', 'Sem clientes no filtro para exibir o status.');

                const sortedActiveClients = Object.values(salesByActiveClient).sort((a, b) => b.total - a.total);
                activeClientsForExport = sortedActiveClients;

                cityActiveDetailTableBody.innerHTML = sortedActiveClients.slice(0, 500).map(data => {
                    const novoLabel = data.isNew ? `<span class="ml-2 text-xs font-semibold text-purple-400 bg-purple-900/50 px-2 py-0.5 rounded-full">NOVO</span>` : '';
                    let tooltipParts = [];
                    if (data.pepsico > 0) tooltipParts.push(`PEPSICO: ${data.pepsico.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}`);
                    if (data.multimarcas > 0) tooltipParts.push(`MULTIMARCAS: ${data.multimarcas.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}`);
                    if (data.outros > 0.001) tooltipParts.push(`OUTROS: ${data.outros.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}`);
                    const tooltipText = tooltipParts.length > 0 ? tooltipParts.join('<br>') : 'Sem detalhamento de categoria';
                    const rcaVal = (data.rcas && data.rcas.length > 0) ? data.rcas[0] : '-';

                    const fantasia = data.fantasia || data.FANTASIA || data.Fantasia || '';
                    const razao = data.razaoSocial || data.Cliente || data.RAZAOSOCIAL || '';
                    const nome = fantasia || razao || 'N/A';
                    const cidade = data.cidade || data.CIDADE || data['Nome da Cidade'] || 'N/A';
                    const bairro = data.bairro || data.BAIRRO || 'N/A';

                    return `<tr class="hover:bg-slate-700"><td class="px-4 py-2"><a href="#" class="text-teal-400 hover:underline" data-codcli="${data['Código']}">${data['Código']}</a></td><td class="px-4 py-2 flex items-center">${nome}${novoLabel}</td><td class="px-4 py-2 text-right"><div class="tooltip">${data.total.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}<span class="tooltip-text" style="width: max-content; transform: translateX(-50%); margin-left: 0;">${tooltipText}</span></div></td><td class="px-4 py-2">${cidade}</td><td class="px-4 py-2">${bairro}</td><td class="px-4 py-2 text-center">${formatDate(data.ultimaCompra)}</td><td class="px-4 py-2">${rcaVal}</td></tr>`
                }).join('');

                inactiveClientsList.sort((a, b) => {
                    if (a.isReturn && !b.isReturn) return -1;
                    if (!a.isReturn && b.isReturn) return 1;
                    if (a.isNewForInactiveLabel && !b.isNewForInactiveLabel) return -1;
                    if (!a.isNewForInactiveLabel && b.isNewForInactiveLabel) return 1;
                    return (parseDate(b.ultimaCompra) || 0) - (parseDate(a.ultimaCompra) || 0);
                });

                cityInactiveDetailTableBody.innerHTML = inactiveClientsList.slice(0, 500).map(client => {
                    const novoLabel = client.isNewForInactiveLabel ? `<span class="ml-2 text-xs font-semibold text-purple-400 bg-purple-900/50 px-2 py-0.5 rounded-full">NOVO</span>` : '';
                    const devolucaoLabel = client.isReturn ? `<span class="ml-2 text-xs font-semibold text-red-400 bg-red-900/50 px-2 py-0.5 rounded-full">DEVOLUÇÃO</span>` : '';
                    const rcaVal = (client.rcas && client.rcas.length > 0) ? client.rcas[0] : '-';

                    const fantasia = client.fantasia || client.FANTASIA || client.Fantasia || '';
                    const razao = client.razaoSocial || client.Cliente || client.RAZAOSOCIAL || '';
                    const nome = fantasia || razao || 'N/A';
                    const cidade = client.cidade || client.CIDADE || client['Nome da Cidade'] || 'N/A';
                    const bairro = client.bairro || client.BAIRRO || 'N/A';
                    const ultCompra = client.ultimaCompra || client['Data da Última Compra'] || client.ULTIMACOMPRA;

                    return `<tr class="hover:bg-slate-700"><td class="px-4 py-2"><a href="#" class="text-teal-400 hover:underline" data-codcli="${client['Código']}">${client['Código']}</a></td><td class="px-4 py-2 flex items-center">${nome}${novoLabel}${devolucaoLabel}</td><td class="px-4 py-2">${cidade}</td><td class="px-4 py-2">${bairro}</td><td class="px-4 py-2 text-center">${formatDate(ultCompra)}</td><td class="px-4 py-2">${rcaVal}</td></tr>`
                }).join('');

                const cityChartTitleEl = document.getElementById('city-chart-title');
                const cityChartOptions = { indexAxis: 'y', scales: { x: { grace: '15%' } }, plugins: { datalabels: { align: 'end', anchor: 'end', color: '#cbd5e1', font: { size: 14, weight: 'bold' }, formatter: (value) => (value / 1000).toFixed(1) + 'k', offset: 8 } } };
                const totalFaturamentoCidade = salesForAnalysis.reduce((sum, item) => sum + item.VLVENDA, 0);
                totalFaturamentoCidadeEl.textContent = totalFaturamentoCidade.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

                if (totalClientesCidadeEl) {
                    totalClientesCidadeEl.textContent = clientsForAnalysis.length.toLocaleString('pt-BR');
                }

                if (cidadeFiltro) {
                    cityChartTitleEl.textContent = 'Top 10 Bairros';
                    const salesByBairro = {};
                    salesForAnalysis.forEach(sale => {
                        let bairro = sale.BAIRRO;
                        if (!bairro && sale.CODCLI) {
                            const c = clientMapForKPIs.get(String(sale.CODCLI));
                            if (c) bairro = c.bairro || c.BAIRRO || c['Bairro'];
                        }
                        bairro = bairro || 'N/A';
                        salesByBairro[bairro] = (salesByBairro[bairro] || 0) + sale.VLVENDA;
                    });
                    const sortedBairros = Object.entries(salesByBairro).sort(([, a], [, b]) => b - a).slice(0, 10);
                    createChart('salesByClientInCityChart', 'bar', sortedBairros.map(([name]) => name), sortedBairros.map(([, total]) => total), cityChartOptions);
                } else {
                    cityChartTitleEl.textContent = 'Top 10 Cidades';
                    const salesByCity = {};
                    salesForAnalysis.forEach(sale => {
                        let cidade = sale.CIDADE;
                        if (!cidade && sale.CODCLI) {
                            const c = clientMapForKPIs.get(String(sale.CODCLI));
                            if (c) cidade = c.cidade || c.CIDADE || c['Nome da Cidade'];
                        }
                        cidade = cidade || 'N/A';
                        salesByCity[cidade] = (salesByCity[cidade] || 0) + sale.VLVENDA;
                    });
                    const sortedCidades = Object.entries(salesByCity).sort(([, a], [, b]) => b - a).slice(0, 10);
                    createChart('salesByClientInCityChart', 'bar', sortedCidades.map(([name]) => name), sortedCidades.map(([, total]) => total), cityChartOptions);
                }
            }, () => currentRenderId !== cityRenderId);
        }

        function getWeekOfMonth(date) {
            const d = new Date(date);
            const day = d.getUTCDate();
            return Math.ceil(day / 7);
        }

        function calculateHistoricalBests() {
            const salesBySupervisorByDay = {};
            const mostRecentSaleDate = allSalesData.map(s => parseDate(s.DTPED)).filter(Boolean).reduce((a, b) => a > b ? a : b, new Date(0));
            const previousMonthDate = new Date(Date.UTC(mostRecentSaleDate.getUTCFullYear(), mostRecentSaleDate.getUTCMonth() - 1, 1));
            const previousMonth = previousMonthDate.getUTCMonth();
            const previousMonthYear = previousMonthDate.getUTCFullYear();
            const historyLastMonthData = allHistoryData.filter(sale => { const saleDate = parseDate(sale.DTPED); return saleDate && saleDate.getUTCMonth() === previousMonth && saleDate.getUTCFullYear() === previousMonthYear; });
            historyLastMonthData.forEach(sale => {
                if (!sale.SUPERV || sale.SUPERV === 'BALCAO' || !sale.DTPED) return;
                const saleDate = parseDate(sale.DTPED); if (!saleDate) return;
                const supervisor = sale.SUPERV.toUpperCase(); const dateString = saleDate.toISOString().split('T')[0];
                if (!salesBySupervisorByDay[supervisor]) salesBySupervisorByDay[supervisor] = {};
                if (!salesBySupervisorByDay[supervisor][dateString]) salesBySupervisorByDay[supervisor][dateString] = 0;
                salesBySupervisorByDay[supervisor][dateString] += sale.VLVENDA;
            });
            const bestDayByWeekdayBySupervisor = {};
            for (const supervisor in salesBySupervisorByDay) {
                const salesByDay = salesBySupervisorByDay[supervisor];
                const bests = {};
                for (const dateString in salesByDay) {
                    const date = new Date(dateString + 'T00:00:00Z');
                    const dayOfWeek = date.getUTCDay();
                    const total = salesByDay[dateString];
                    if (dayOfWeek >= 1 && dayOfWeek <= 5) { if (!bests[dayOfWeek] || total > bests[dayOfWeek]) bests[dayOfWeek] = total; }
                }
                bestDayByWeekdayBySupervisor[supervisor] = bests;
            }
            historicalBests = bestDayByWeekdayBySupervisor;
        }

        function populateWeeklyFilters() {
            selectedWeeklySupervisors = updateSupervisorFilter(weeklySupervisorFilterDropdown, weeklySupervisorFilterText, selectedWeeklySupervisors, allSalesData);
            selectedWeeklySellers = updateSellerFilter(selectedWeeklySupervisors, weeklyVendedorFilterDropdown, weeklyVendedorFilterText, selectedWeeklySellers, allSalesData);
        }

        function updateWeeklyView() {
            let dataForGeneralCharts;

            // Use the generic filtering helper which supports multiple selections
            const filters = {
                supervisor: selectedWeeklySupervisors.length > 0 ? new Set(selectedWeeklySupervisors) : null,
                seller: selectedWeeklySellers.length > 0 ? new Set(selectedWeeklySellers) : null,
                pasta: currentWeeklyFornecedor || null
            };

            // If we have filters, use optimized lookup
            if (filters.supervisor || filters.seller || filters.pasta) {
                dataForGeneralCharts = getFilteredDataFromIndices(optimizedData.indices.current, optimizedData.salesById, filters);
            } else {
                dataForGeneralCharts = allSalesData;
            }

            const currentMonth = lastSaleDate.getUTCMonth(); const currentYear = lastSaleDate.getUTCFullYear();
            const monthSales = dataForGeneralCharts.filter(d => { if (!d.DTPED) return false; const saleDate = parseDate(d.DTPED); return saleDate && saleDate.getUTCMonth() === currentMonth && saleDate.getUTCFullYear() === currentYear; });
            const totalMes = monthSales.reduce((sum, item) => sum + item.VLVENDA, 0);
            totalMesSemanalEl.textContent = totalMes.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
            const salesByWeekAndDay = {}; const dayNames = ['Domingo', 'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado'];
            monthSales.forEach(sale => { const saleDate = parseDate(sale.DTPED); if (!saleDate) return; const weekNum = getWeekOfMonth(saleDate); const dayName = dayNames[saleDate.getUTCDay()]; if (!salesByWeekAndDay[weekNum]) salesByWeekAndDay[weekNum] = {}; if (!salesByWeekAndDay[weekNum][dayName]) salesByWeekAndDay[weekNum][dayName] = 0; salesByWeekAndDay[weekNum][dayName] += sale.VLVENDA; });
            const weekLabels = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta'];
            const weekNumbers = Object.keys(salesByWeekAndDay).sort((a,b) => a - b);
            const professionalPalette = ['#14b8a6', '#6366f1', '#ec4899', '#f97316', '#8b5cf6'];
            const currentMonthDatasets = weekNumbers.map((weekNum, index) => ({ label: `Semana ${weekNum}`, data: weekLabels.map(day => salesByWeekAndDay[weekNum][day] || 0), backgroundColor: professionalPalette[index % professionalPalette.length] }));
            // Note: Calculate "Melhor Dia Mês Anterior" dynamically based on current filters
            const historicalDataForChart = [0, 0, 0, 0, 0];
            
            // 1. Get Filtered History Data
            let historyDataForCalculation;
            if (filters.supervisor || filters.seller || filters.pasta) {
                historyDataForCalculation = getFilteredDataFromIndices(optimizedData.indices.history, optimizedData.historyById, filters);
            } else {
                historyDataForCalculation = allHistoryData;
            }

            // 2. Determine Previous Month Range
            let prevMonth = currentMonth - 1;
            let prevYear = currentYear;
            if (prevMonth < 0) { prevMonth = 11; prevYear--; }

            // 3. Aggregate Sales by Date for the Previous Month
            const prevMonthSalesByDate = {}; // 'YYYY-MM-DD' -> Total
            
            // Optimize iteration if it's a columnar dataset proxy (although array methods work)
            for (let i = 0; i < historyDataForCalculation.length; i++) {
                const sale = historyDataForCalculation instanceof ColumnarDataset ? historyDataForCalculation.get(i) : historyDataForCalculation[i];
                const d = parseDate(sale.DTPED);
                if (d && d.getUTCMonth() === prevMonth && d.getUTCFullYear() === prevYear) {
                    const dateStr = d.toISOString().split('T')[0];
                    if (!prevMonthSalesByDate[dateStr]) prevMonthSalesByDate[dateStr] = 0;
                    prevMonthSalesByDate[dateStr] += (sale.VLVENDA || 0);
                }
            }

            // 4. Find Best Total for Each Weekday
            const bestsByWeekday = {}; // 1..5 -> maxTotal
            for (const dateStr in prevMonthSalesByDate) {
                const d = new Date(dateStr + 'T00:00:00Z'); // Ensure UTC parsing
                const dayOfWeek = d.getUTCDay();
                if (dayOfWeek >= 1 && dayOfWeek <= 5) {
                    const total = prevMonthSalesByDate[dateStr];
                    if (!bestsByWeekday[dayOfWeek] || total > bestsByWeekday[dayOfWeek]) {
                        bestsByWeekday[dayOfWeek] = total;
                    }
                }
            }

            // 5. Populate Chart Data
            historicalDataForChart[0] = bestsByWeekday[1] || 0; // Mon
            historicalDataForChart[1] = bestsByWeekday[2] || 0; // Tue
            historicalDataForChart[2] = bestsByWeekday[3] || 0; // Wed
            historicalDataForChart[3] = bestsByWeekday[4] || 0; // Thu
            historicalDataForChart[4] = bestsByWeekday[5] || 0; // Fri
            const historicalDataset = { type: 'line', label: 'Melhor Dia Mês Anterior', data: historicalDataForChart, borderColor: '#f59e0b', backgroundColor: 'transparent', pointBackgroundColor: '#f59e0b', pointRadius: 4, tension: 0.1, borderWidth: 2, yAxisID: 'y', datalabels: { display: false } };
            const finalDatasets = [...currentMonthDatasets, historicalDataset];
            const weeklyChartOptions = { plugins: { legend: { display: true, onClick: (e, legendItem, legend) => { const index = legendItem.datasetIndex; const ci = legend.chart; if (ci.isDatasetVisible(index)) { ci.hide(index); legendItem.hidden = true; } else { ci.show(index); legendItem.hidden = false; } let newTotal = 0; ci.data.datasets.forEach((dataset, i) => { if (ci.isDatasetVisible(i) && dataset.type !== 'line') newTotal += dataset.data.reduce((acc, val) => acc + val, 0); }); totalMesSemanalEl.textContent = newTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }); } } } };
            createChart('weeklySalesChart', 'bar', weekLabels, finalDatasets, weeklyChartOptions);
            const weeklySummaryTableBody = document.getElementById('weekly-summary-table-body');
            if (weeklySummaryTableBody) {
                weeklySummaryTableBody.innerHTML = ''; let grandTotal = 0;
                Object.keys(salesByWeekAndDay).sort((a,b) => parseInt(a) - parseInt(b)).forEach(weekNum => { const weekTotal = Object.values(salesByWeekAndDay[weekNum]).reduce((a, b) => a + b, 0); grandTotal += weekTotal; weeklySummaryTableBody.innerHTML += `<tr class="hover:bg-slate-700"><td class="px-4 py-2">Semana ${weekNum}</td><td class="px-4 py-2 text-right">${weekTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</td></tr>`; });
                weeklySummaryTableBody.innerHTML += `<tr class="font-bold bg-slate-700/50"><td class="px-4 py-2">Total do Mês</td><td class="px-4 py-2 text-right">${grandTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</td></tr>`;
            }
            let dataForRankings = dataForGeneralCharts.filter(d => d.SUPERV !== 'BALCAO');

            // --- "AMERICANAS" Exclusion Logic ---
            // If "AMERICANAS" is NOT explicitly selected in the vendor filter, exclude it from ranking charts.
            // Assuming "AMERICANAS" appears as a Vendor name in sales data (d.NOME) or Client name?
            // Usually AMERICANAS is a client, but sometimes mapped as a dummy vendor or huge account.
            // The prompt says "o vendedor 'AMERICANAS'". So we check d.NOME.
            const americanasSelected = selectedWeeklySellers.some(s => s.toUpperCase().includes('AMERICANAS'));
            if (!americanasSelected) {
                dataForRankings = dataForRankings.filter(d => !d.NOME.toUpperCase().includes('AMERICANAS'));
            }
            // ------------------------------------

            const positivacaoMap = new Map(); // Map<Seller, Map<Client, Value>>
            dataForRankings.forEach(d => {
                if (!d.NOME || !d.CODCLI) return;
                if (!positivacaoMap.has(d.NOME)) positivacaoMap.set(d.NOME, new Map());
                const clientMap = positivacaoMap.get(d.NOME);
                clientMap.set(d.CODCLI, (clientMap.get(d.CODCLI) || 0) + d.VLVENDA);
            });

            const positivacaoRank = [];
            positivacaoMap.forEach((clientMap, seller) => {
                let activeCount = 0;
                clientMap.forEach(val => { if (val >= 1) activeCount++; });
                positivacaoRank.push({ vendedor: seller, total: activeCount });
            });
            positivacaoRank.sort((a, b) => b.total - a.total).splice(10); // Keep top 10

            if (positivacaoRank.length > 0) createChart('positivacaoChart', 'bar', positivacaoRank.map(r => getFirstName(r.vendedor)), positivacaoRank.map(r => r.total));
            else showNoDataMessage('positivacaoChart', 'Sem dados para o ranking.');
            const salesBySeller = {}; dataForRankings.forEach(d => { if (!d.NOME) return; salesBySeller[d.NOME] = (salesBySeller[d.NOME] || 0) + d.VLVENDA; });
            const topSellersRank = Object.entries(salesBySeller).sort(([, a], [, b]) => b - a).slice(0, 10);
            if (topSellersRank.length > 0) createChart('topSellersChart', 'bar', topSellersRank.map(r => getFirstName(r[0])), topSellersRank.map(r => r[1]));
            else showNoDataMessage('topSellersChart', 'Sem dados para o ranking.');
            // --- OPTIMIZATION: Mix Rank Calculation ---
            // Calculate mix map: Map<Seller, Map<Client, Map<Description, Value>>>
            const sellerClientMixMap = new Map();
            const targetSuppliers = new Set(['707', '708']); // Aligned with Comparison Page

            dataForRankings.forEach(d => {
                const vendedor = d.NOME;
                if (!vendedor || vendedor === 'VD HIAGO') return;

                // Rule: Aligned with Comparison Page (Strict Pepsico)
                if (!targetSuppliers.has(String(d.CODFOR))) return;

                if (!d.CODCLI || !d.DESCRICAO) return;

                if (!sellerClientMixMap.has(vendedor)) sellerClientMixMap.set(vendedor, new Map());
                const clientMap = sellerClientMixMap.get(vendedor);

                if (!clientMap.has(d.CODCLI)) clientMap.set(d.CODCLI, new Map());
                const prodMap = clientMap.get(d.CODCLI);
                prodMap.set(d.DESCRICAO, (prodMap.get(d.DESCRICAO) || 0) + d.VLVENDA);
            });

            const mixRank = [];
            for (const [vendedor, clientMap] of sellerClientMixMap.entries()) {
                const mixValues = [];
                for (const prodMap of clientMap.values()) {
                    let positiveProducts = 0;
                    prodMap.forEach(val => { if (val >= 1) positiveProducts++; });
                    if (positiveProducts > 0) mixValues.push(positiveProducts);
                }

                if (mixValues.length > 0) {
                    const avgMix = mixValues.reduce((a, b) => a + b, 0) / mixValues.length;
                    mixRank.push({ vendedor, avgMix });
                } else {
                     mixRank.push({ vendedor, avgMix: 0 });
                }
            }

            mixRank.sort((a, b) => b.avgMix - a.avgMix);
            const top10Mix = mixRank.slice(0, 10);

            if(top10Mix.length > 0 && top10Mix.some(r => r.avgMix > 0)) createChart('mixChart', 'bar', top10Mix.map(r => getFirstName(r.vendedor)), top10Mix.map(r => r.avgMix));
            else showNoDataMessage('mixChart', 'Sem dados para o ranking.');
        }

        function updateSupplierFilter(dropdown, filterText, selectedArray, dataSource, filterType = 'comparison', skipRender = false) {
            const forbidden = ['CODFOR', 'FORNECEDOR', 'COD FOR', 'NOME DO FORNECEDOR', 'FORNECEDOR_NOME'];
            const suppliers = new Map();
            dataSource.forEach(s => {
                if(s.CODFOR && s.FORNECEDOR && !forbidden.includes(s.CODFOR.toUpperCase()) && !forbidden.includes(s.FORNECEDOR.toUpperCase())) {
                    suppliers.set(s.CODFOR, s.FORNECEDOR);
                }
            });
            const sortedSuppliers = [...suppliers.entries()].sort((a, b) => a[1].localeCompare(b[1]));

            selectedArray = selectedArray.filter(cod => suppliers.has(cod));

            if (!skipRender) {
                const htmlParts = [];
                for (let i = 0; i < sortedSuppliers.length; i++) {
                    const [cod, name] = sortedSuppliers[i];
                    const isChecked = selectedArray.includes(cod);
                    htmlParts.push(`<label class="flex items-center p-2 hover:bg-slate-600 cursor-pointer"><input type="checkbox" data-filter-type="${filterType}" class="form-checkbox h-4 w-4 bg-slate-800 border-slate-500 rounded text-teal-500 focus:ring-teal-500" value="${cod}" ${isChecked ? 'checked' : ''}><span class="ml-2 text-xs">${cod} - ${name}</span></label>`);
                }
                dropdown.innerHTML = htmlParts.join('');
            }

            if (selectedArray.length === 0 || selectedArray.length === sortedSuppliers.length) {
                filterText.textContent = 'Todos Fornecedores';
            } else if (selectedArray.length === 1) {
                filterText.textContent = suppliers.get(selectedArray[0]) || '1 selecionado';
            } else {
                filterText.textContent = `${selectedArray.length} fornecedores selecionados`;
            }
            return selectedArray;
        }

        function updateComparisonCitySuggestions(dataSource) {
            const forbidden = ['CIDADE', 'MUNICIPIO', 'CIDADE_CLIENTE', 'NOME DA CIDADE', 'CITY'];
            const inputValue = comparisonCityFilter.value.toLowerCase();
            // Optimized Lookup
            const allAvailableCities = [...new Set(dataSource.map(item => {
                if (item.CIDADE) return item.CIDADE;
                if (item.CODCLI) {
                    const c = clientMapForKPIs.get(String(item.CODCLI));
                    if (c) return c.cidade || c['Nome da Cidade'];
                }
                return 'N/A';
            }).filter(c => c && c !== 'N/A' && !forbidden.includes(c.toUpperCase())))].sort();
            const filteredCities = inputValue ? allAvailableCities.filter(c => c.toLowerCase().includes(inputValue)) : allAvailableCities;
            if (filteredCities.length > 0 && document.activeElement === comparisonCityFilter) {
                comparisonCitySuggestions.innerHTML = filteredCities.map(c => `<div class="p-2 hover:bg-slate-600 cursor-pointer">${c}</div>`).join('');
                comparisonCitySuggestions.classList.remove('hidden');
            } else {
                comparisonCitySuggestions.classList.add('hidden');
            }
        }

        function getMonthWeeks(year, month) {
            const weeks = [];
            // Find the first day of the month
            const firstOfMonth = new Date(Date.UTC(year, month, 1));

            // Find the Sunday on or before the 1st
            const dayOfWeek = firstOfMonth.getUTCDay(); // 0 (Sun) to 6 (Sat)
            let currentStart = new Date(firstOfMonth);
            currentStart.setUTCDate(firstOfMonth.getUTCDate() - dayOfWeek);

            // Find the last day of the month
            const lastOfMonth = new Date(Date.UTC(year, month + 1, 0));

            // Iterate weeks until we cover the last day of the month
            while (currentStart <= lastOfMonth) {
                const currentEnd = new Date(currentStart);
                currentEnd.setUTCDate(currentStart.getUTCDate() + 6);
                currentEnd.setUTCHours(23, 59, 59, 999);

                weeks.push({ start: new Date(currentStart), end: currentEnd });

                // Move to next Sunday
                currentStart.setUTCDate(currentStart.getUTCDate() + 7);
            }
            return weeks;
        }

        function normalize(str) {
            return str
                ? str.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/\s+/g, ' ').trim().toUpperCase()
                : '';
        }

        function getPositiveClientsWithNewLogic(salesData) {
            const salesByClient = new Map();
            salesData.forEach(sale => {
                if (!sale.CODCLI) return;
                const clientTotal = salesByClient.get(sale.CODCLI) || 0;
                salesByClient.set(sale.CODCLI, clientTotal + sale.VLVENDA);
            });

            let positiveClients = 0;
            const threshold = 1;

            for (const total of salesByClient.values()) {
                if (total > threshold) {
                    positiveClients++;
                }
            }
            return positiveClients;
        }

        const formatValue = (val, format) => {
            if (format === 'currency') return val.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
            if (format === 'decimal') return val.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 });
            if (format === 'mix') return val.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            return val.toLocaleString('pt-BR', { maximumFractionDigits: 0 });
        };

        const formatAbbreviated = (val, format) => {
            let prefix = format === 'currency' ? 'R$ ' : '';
            if (val >= 1000000) {
                return prefix + (val / 1000000).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mi';
            } else if (val >= 1000) {
                 return prefix + (val / 1000).toLocaleString('pt-BR', { maximumFractionDigits: 0 }) + ' K';
            }
            return null;
        };

        function renderKpiCards(kpis) {
            const container = document.getElementById('comparison-kpi-container');

            container.innerHTML = kpis.map(kpi => {
                const variation = kpi.history > 0 ? ((kpi.current - kpi.history) / kpi.history) * 100 : (kpi.current > 0 ? 100 : 0);
                const colorClass = variation > 0 ? 'text-green-400' : variation < 0 ? 'text-red-400' : 'text-slate-400';

                let displayValue;
                if (kpi.title === 'Faturamento Total') {
                    displayValue = formatAbbreviated(kpi.current, kpi.format) || formatValue(kpi.current, kpi.format);
                } else {
                    displayValue = formatValue(kpi.current, kpi.format);
                }

                let glowClass = 'kpi-glow-blue';
                if (kpi.title.includes('Faturamento')) glowClass = 'kpi-glow-green';
                else if (kpi.title.includes('Volume')) glowClass = 'kpi-glow-blue';
                else if (kpi.title.includes('Positivação') || kpi.title.includes('Cobertura')) glowClass = 'kpi-glow-purple';
                else if (kpi.title.includes('SKU') || kpi.title.includes('Mix')) glowClass = 'kpi-glow-yellow';

                return `<div class="kpi-card p-4 rounded-lg text-center kpi-glow-base ${glowClass} transition transform hover:-translate-y-1 duration-200">
                            <p class="text-slate-300 text-sm">${kpi.title}</p>
                            <p class="text-2xl font-bold text-white my-2">${displayValue}</p>
                            <p class="text-sm ${colorClass}">${variation.toFixed(2)}% vs Média do Trimestre</p>
                            <p class="text-xs text-slate-300">Média Trim.: ${formatValue(kpi.history, kpi.format)}</p>
                        </div>`;
            }).join('');
        }

        function calculateAverageMixComDevolucao(salesData, targetCodfors) {
             if (!salesData || salesData.length === 0 || !targetCodfors || targetCodfors.length === 0) return 0;

            const clientProductNetValue = new Map();

            for (const sale of salesData) {
                if (!targetCodfors.includes(String(sale.CODFOR))) continue;
                if (!sale.CODCLI || !sale.PRODUTO) continue;

                if (!clientProductNetValue.has(sale.CODCLI)) {
                    clientProductNetValue.set(sale.CODCLI, new Map());
                }
                const clientProducts = clientProductNetValue.get(sale.CODCLI);

                const currentValue = clientProducts.get(sale.PRODUTO) || 0;
                clientProducts.set(sale.PRODUTO, currentValue + (Number(sale.VLVENDA) || 0));
            }

            const mixValues = [];
            for (const products of clientProductNetValue.values()) {
                let positiveProductCount = 0;
                for (const netValue of products.values()) {
                    if (netValue >= 1) {
                        positiveProductCount++;
                    }
                }
                if (positiveProductCount > 0) {
                    mixValues.push(positiveProductCount);
                }
            }

            if (mixValues.length === 0) return 0;

            return mixValues.reduce((a, b) => a + b, 0) / mixValues.length;
        }

        function calculatePositivacaoPorCestaComDevolucao(salesData, requiredCategories) {
            if (!salesData || salesData.length === 0 || !requiredCategories || requiredCategories.length === 0) return 0;

            const normalizedCategories = requiredCategories.map(normalize);
            const clientProductNetSales = new Map();

            for (const sale of salesData) {
                if (!sale.CODCLI || !sale.PRODUTO) continue;

                if (!clientProductNetSales.has(sale.CODCLI)) {
                    clientProductNetSales.set(sale.CODCLI, new Map());
                }
                const clientProducts = clientProductNetSales.get(sale.CODCLI);

                if (!clientProducts.has(sale.PRODUTO)) {
                    clientProducts.set(sale.PRODUTO, { netValue: 0, description: sale.DESCRICAO });
                }
                const productData = clientProducts.get(sale.PRODUTO);
                productData.netValue += (Number(sale.VLVENDA) || 0);
            }

            const clientPurchasedCategories = new Map();

            for (const [codcli, products] of clientProductNetSales.entries()) {
                for (const data of products.values()) {
                    if (data.netValue >= 1) {
                        const normalizedDescription = normalize(data.description);
                        for (const category of normalizedCategories) {
                            if (normalizedDescription.includes(category)) {
                                if (!clientPurchasedCategories.has(codcli)) {
                                    clientPurchasedCategories.set(codcli, new Set());
                                }
                                clientPurchasedCategories.get(codcli).add(category);
                                break;
                            }
                        }
                    }
                }
            }

            let positivadosCount = 0;
            const requiredCategoryCount = normalizedCategories.length;
            for (const categoriesPurchased of clientPurchasedCategories.values()) {
                if (categoriesPurchased.size >= requiredCategoryCount) {
                    positivadosCount++;
                }
            }

            return positivadosCount;
        }


        function groupSalesByMonth(salesData) {
            const salesByMonth = {};
            salesData.forEach(sale => {
                const date = parseDate(sale.DTPED);
                if (!date || isNaN(date.getTime())) return;
                const monthKey = date.getUTCFullYear() + '-' + String(date.getUTCMonth() + 1).padStart(2, '0');
                if (!salesByMonth[monthKey]) salesByMonth[monthKey] = [];
                salesByMonth[monthKey].push(sale);
            });
            return salesByMonth;
        }

                        function calculateUnifiedMetrics(currentSales, historySales) {
            // 1. Setup Data Structures
            const currentYear = lastSaleDate.getUTCFullYear();
            const currentMonth = lastSaleDate.getUTCMonth();
            const currentMonthWeeks = getMonthWeeks(currentYear, currentMonth);

            const metrics = {
                current: {
                    fat: 0, peso: 0, clients: 0,
                    mixPepsico: 0, positivacaoSalty: 0, positivacaoFoods: 0
                },
                history: {
                    fat: 0, peso: 0,
                    avgFat: 0, avgPeso: 0, avgClients: 0,
                    avgMixPepsico: 0, avgPositivacaoSalty: 0, avgPositivacaoFoods: 0
                },
                charts: {
                    weeklyCurrent: new Array(currentMonthWeeks.length).fill(0),
                    weeklyHistory: new Array(currentMonthWeeks.length).fill(0),
                    monthlyData: [], // { label, value (fat/clients) }
                    supervisorData: {} // { sup: { current, history } }
                },
                overlapSales: []
            };

            const firstWeekStart = currentMonthWeeks[0].start;
            const firstOfMonth = new Date(Date.UTC(currentYear, currentMonth, 1));
            const hasOverlap = firstWeekStart < firstOfMonth;

            const pepsicoCodfors = new Set(['707', '708']);
            const saltyCategories = ['CHEETOS', 'DORITOS', 'FANDANGOS', 'RUFFLES', 'TORCIDA'];
            const foodsCategories = ['TODDYNHO', 'TODDY ', 'QUAKER', 'KEROCOCO'];

            // Helper to normalize strings
            const norm = (s) => s ? s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase() : '';

            // --- FILTER: Foods Description Logic (Same as Metas) ---
            const isValidFoodsProduct = (codFor, desc) => {
                // Apply strict description check ONLY for Supplier 1119 (Foods)
                if (codFor !== '1119') return true;

                const d = norm(desc || '');
                // Check if it matches ANY of the Foods sub-brands (Toddynho, Toddy, Quaker/Kerococo)
                // Note: Metas logic separates them. Here we just want to know if it belongs to "Foods Group".
                // If it contains NONE of the keywords, it is excluded from "Foods" metrics.
                if (d.includes('TODDYNHO')) return true;
                if (d.includes('TODDY ')) return true; // Note the space
                if (d.includes('QUAKER')) return true;
                if (d.includes('KEROCOCO')) return true;

                return false;
            };

            // --- 2. Process Current Sales (Single Pass) ---
            const currentClientProductMap = new Map(); // Client -> Product -> { val, desc, codfor }
            const currentClientsSet = new Map(); // Client -> Total Value (for Positive check)

            currentSales.forEach(s => {
                // Filter: Only Type 1 and 9 count for Metrics (Fat/Peso/Charts)
                const isValidType = (s.TIPOVENDA === '1' || s.TIPOVENDA === '9');

                // Filter: Strict Foods Definition
                if (!isValidFoodsProduct(String(s.CODFOR), s.DESCRICAO)) return;

                if (isValidType) {
                    metrics.current.fat += s.VLVENDA;
                    metrics.current.peso += s.TOTPESOLIQ;
                }

                if (s.CODCLI) {
                    // Accumulate for Positive Check (using VLVENDA which is 0 for non-1/9 anyway, but keeping consistent)
                    currentClientsSet.set(s.CODCLI, (currentClientsSet.get(s.CODCLI) || 0) + s.VLVENDA);

                    if (!currentClientProductMap.has(s.CODCLI)) currentClientProductMap.set(s.CODCLI, new Map());
                    const cMap = currentClientProductMap.get(s.CODCLI);
                    if (!cMap.has(s.PRODUTO)) cMap.set(s.PRODUTO, { val: 0, desc: s.DESCRICAO, codfor: String(s.CODFOR) });
                    cMap.get(s.PRODUTO).val += s.VLVENDA;
                }

                // Supervisor Data
                if (s.SUPERV && isValidType) {
                    if (!metrics.charts.supervisorData[s.SUPERV]) metrics.charts.supervisorData[s.SUPERV] = { current: 0, history: 0 };
                    metrics.charts.supervisorData[s.SUPERV].current += s.VLVENDA;
                }

                // Weekly Chart (Current)
                const d = parseDate(s.DTPED);
                if (d && isValidType) {
                    const wIdx = currentMonthWeeks.findIndex(w => d >= w.start && d <= w.end);
                    if (wIdx !== -1) metrics.charts.weeklyCurrent[wIdx] += s.VLVENDA;
                }
            });

            // Calculate Current KPIs from Maps
            let currentPositiveClients = 0;
            currentClientsSet.forEach(val => { if (val >= 1) currentPositiveClients++; });
            metrics.current.clients = currentPositiveClients;

            // Mix/Positivacao Current
            let sumMix = 0;
            let countMixClients = 0;
            let countSalty = 0;
            let countFoods = 0;

            currentClientProductMap.forEach((prods, codcli) => {
                // Mix Pepsico
                let pepsicoCount = 0;
                const boughtCatsSalty = new Set();
                const boughtCatsFoods = new Set();

                prods.forEach(pData => {
                    if (pData.val >= 1) {
                        if (pepsicoCodfors.has(pData.codfor)) pepsicoCount++;

                        const desc = norm(pData.desc);
                        saltyCategories.forEach(cat => { if (desc.includes(cat)) boughtCatsSalty.add(cat); });
                        foodsCategories.forEach(cat => { if (desc.includes(cat)) boughtCatsFoods.add(cat); });
                    }
                });

                if (pepsicoCount > 0) {
                    sumMix += pepsicoCount;
                    countMixClients++;
                }
                if (boughtCatsSalty.size >= saltyCategories.length) countSalty++;
                if (boughtCatsFoods.size >= foodsCategories.length) countFoods++;
            });

            metrics.current.mixPepsico = countMixClients > 0 ? sumMix / countMixClients : 0;
            metrics.current.positivacaoSalty = countSalty;
            metrics.current.positivacaoFoods = countFoods;


            // --- 3. Process History Sales (Single Pass) ---
            const historyMonths = new Map(); // MonthKey -> { fat, clientMap, weekSales: [] }

            // Cache week ranges for history months to avoid recalculating
            const monthWeeksCache = new Map();

            historySales.forEach(s => {
                const d = parseDate(s.DTPED);
                if (!d) return;

                const monthKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;

                // Filter: Strict Foods Definition (Apply here too)
                if (!isValidFoodsProduct(String(s.CODFOR), s.DESCRICAO)) return;

                // Filter: Only Type 1 and 9 count for Metrics (Fat/Peso/Charts)
                const isValidType = (s.TIPOVENDA === '1' || s.TIPOVENDA === '9');

                if (isValidType) {
                    metrics.history.fat += s.VLVENDA;
                    metrics.history.peso += s.TOTPESOLIQ;
                }

                if (!historyMonths.has(monthKey)) {
                    historyMonths.set(monthKey, {
                        fat: 0,
                        clients: new Map(), // Client -> Total
                        productMap: new Map() // Client -> Product -> Data
                    });
                }
                const mData = historyMonths.get(monthKey);

                if (isValidType) {
                    mData.fat += s.VLVENDA;
                }

                if (s.CODCLI) {
                    mData.clients.set(s.CODCLI, (mData.clients.get(s.CODCLI) || 0) + s.VLVENDA);

                    if (!mData.productMap.has(s.CODCLI)) mData.productMap.set(s.CODCLI, new Map());
                    const cMap = mData.productMap.get(s.CODCLI);
                    if (!cMap.has(s.PRODUTO)) cMap.set(s.PRODUTO, { val: 0, desc: s.DESCRICAO, codfor: String(s.CODFOR) });
                    cMap.get(s.PRODUTO).val += s.VLVENDA;
                }

                // Supervisor History
                if (s.SUPERV && isValidType) {
                    if (!metrics.charts.supervisorData[s.SUPERV]) metrics.charts.supervisorData[s.SUPERV] = { current: 0, history: 0 };
                    metrics.charts.supervisorData[s.SUPERV].history += s.VLVENDA;
                }

                // Weekly History (Average logic)
                if (!monthWeeksCache.has(monthKey)) {
                    monthWeeksCache.set(monthKey, getMonthWeeks(d.getUTCFullYear(), d.getUTCMonth()));
                }
                const weeks = monthWeeksCache.get(monthKey);
                const wIdx = weeks.findIndex(w => d >= w.start && d <= w.end);

                // Map to Current Month's structure (0..4)
                if (wIdx !== -1 && wIdx < metrics.charts.weeklyHistory.length && isValidType) {
                    metrics.charts.weeklyHistory[wIdx] += s.VLVENDA;
                }

                // Handle Overlap for Current Chart (Single Pass)
                if (hasOverlap && d >= firstWeekStart && d < firstOfMonth && isValidType) {
                    metrics.charts.weeklyCurrent[0] += s.VLVENDA;
                    metrics.overlapSales.push(s);
                }
            });

            // Calculate History Averages
            metrics.history.avgFat = metrics.history.fat / QUARTERLY_DIVISOR;
            metrics.history.avgPeso = metrics.history.peso / QUARTERLY_DIVISOR;
            metrics.charts.weeklyHistory = metrics.charts.weeklyHistory.map(v => v / QUARTERLY_DIVISOR);

            // Normalize Supervisor History
            Object.values(metrics.charts.supervisorData).forEach(d => d.history /= QUARTERLY_DIVISOR);

            // Process Monthly KPIs (Clients, Mix)
            // Sort months to ensure we take the last 3 if there are more
            const sortedMonths = Array.from(historyMonths.keys()).sort();

            // Take last 3 months
            const monthsToProcess = sortedMonths.slice(-3);

            let sumClients = 0;
            let sumMixPep = 0;
            let sumPosSalty = 0;
            let sumPosFoods = 0;

            monthsToProcess.forEach(mKey => {
                const mData = historyMonths.get(mKey);

                // Clients
                let posClients = 0;
                mData.clients.forEach(v => { if(v >= 1) posClients++; });
                sumClients += posClients;

                // Mix
                let mSumMix = 0;
                let mCountMixClients = 0;
                let mCountSalty = 0;
                let mCountFoods = 0;

                mData.productMap.forEach((prods, codcli) => {
                    let pepsicoCount = 0;
                    const boughtCatsSalty = new Set();
                    const boughtCatsFoods = new Set();

                    prods.forEach(pData => {
                        if (pData.val >= 1) {
                            if (pepsicoCodfors.has(pData.codfor)) pepsicoCount++;
                            const desc = norm(pData.desc);
                            saltyCategories.forEach(cat => { if (desc.includes(cat)) boughtCatsSalty.add(cat); });
                            foodsCategories.forEach(cat => { if (desc.includes(cat)) boughtCatsFoods.add(cat); });
                        }
                    });

                    if (pepsicoCount > 0) {
                        mSumMix += pepsicoCount;
                        mCountMixClients++;
                    }
                    if (boughtCatsSalty.size >= saltyCategories.length) mCountSalty++;
                    if (boughtCatsFoods.size >= foodsCategories.length) mCountFoods++;
                });

                sumMixPep += (mCountMixClients > 0 ? mSumMix / mCountMixClients : 0);
                sumPosSalty += mCountSalty;
                sumPosFoods += mCountFoods;

                // For Monthly Chart (Labels and Values)
                const [y, m] = mKey.split('-');
                const monthName = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"][parseInt(m)];

                metrics.charts.monthlyData.push({
                    label: monthName,
                    fat: mData.fat,
                    clients: posClients,
                    key: mKey
                });
            });

            // Finish History Averages
            metrics.history.avgClients = sumClients / QUARTERLY_DIVISOR;
            metrics.history.avgMixPepsico = sumMixPep / QUARTERLY_DIVISOR;
            metrics.history.avgPositivacaoSalty = sumPosSalty / QUARTERLY_DIVISOR;
            metrics.history.avgPositivacaoFoods = sumPosFoods / QUARTERLY_DIVISOR;

            return metrics;
        }


        function monthlyKpiAverage(dataInput, kpiFn, isGrouped = false, ...kpiArgs) {
            let salesByMonth;
            if (isGrouped) {
                salesByMonth = dataInput;
            } else {
                salesByMonth = groupSalesByMonth(dataInput);
            }

            let sortedMonths = Object.keys(salesByMonth).sort();

            if (sortedMonths.length > 0) {
                const firstMonthKey = sortedMonths[0];
                const firstSaleInFirstMonth = salesByMonth[firstMonthKey].reduce((earliest, sale) => {
                    const saleDate = parseDate(sale.DTPED);
                    return (!earliest || (saleDate && saleDate < earliest)) ? saleDate : earliest;
                }, null);

                if (firstSaleInFirstMonth && firstSaleInFirstMonth.getUTCDate() > 20) {
                    sortedMonths.shift();
                }
            }

            const monthsToAverage = sortedMonths.slice(-3);

            const kpiValues = monthsToAverage.map(monthKey => {
                const salesForMonth = salesByMonth[monthKey];
                return kpiFn(salesForMonth, ...kpiArgs);
            });

            if (kpiValues.length === 0) return 0;
            return kpiValues.reduce((a, b) => a + b, 0) / QUARTERLY_DIVISOR;
        }

        function calculateStockMonthlyAverage(historySales) {
            // Function groupSalesByMonth is defined above in the scope
            const salesByMonth = groupSalesByMonth(historySales);
            let sortedMonths = Object.keys(salesByMonth).sort();

            if (sortedMonths.length > 0) {
                const firstMonthKey = sortedMonths[0];
                const firstSaleInFirstMonth = salesByMonth[firstMonthKey].reduce((earliest, sale) => {
                    const saleDate = parseDate(sale.DTPED);
                    return (!earliest || (saleDate && saleDate < earliest)) ? saleDate : earliest;
                }, null);

                if (firstSaleInFirstMonth && firstSaleInFirstMonth.getUTCDate() > 20) {
                    sortedMonths.shift();
                }
            }

            const monthsToAverage = sortedMonths.slice(-3);

            const kpiValues = monthsToAverage.map(monthKey => {
                const salesForMonth = salesByMonth[monthKey];
                return salesForMonth.reduce((sum, s) => sum + (s.QTVENDA_EMBALAGEM_MASTER || 0), 0);
            });

            if (kpiValues.length === 0) return 0;
            return kpiValues.reduce((a, b) => a + b, 0) / kpiValues.length;
        }

        const getFilteredDataFromIndices = (indices, dataset, filters, excludeFilter = null) => {
            const isExcluded = (f) => excludeFilter === f || (Array.isArray(excludeFilter) && excludeFilter.includes(f));
            const setsToIntersect = [];
            let hasFilter = false;

            // Helper to get item
            const getItem = (idx) => (dataset.get ? dataset.get(idx) : dataset[idx]);

            if (filters.filial && filters.filial !== 'ambas') {
                hasFilter = true;
                if (indices.byFilial && indices.byFilial.has(filters.filial)) {
                    setsToIntersect.push(indices.byFilial.get(filters.filial));
                } else {
                    return [];
                }
            }

            if (!isExcluded('supervisor') && filters.supervisor) {
                if (typeof filters.supervisor === 'string') {
                    hasFilter = true;
                    if (indices.bySupervisor && indices.bySupervisor.has(filters.supervisor)) {
                        setsToIntersect.push(indices.bySupervisor.get(filters.supervisor));
                    } else {
                        return [];
                    }
                } else if (filters.supervisor.size > 0) {
                    hasFilter = true;
                    const unionIds = new Set();
                    let foundAny = false;
                    filters.supervisor.forEach(sup => {
                        if (indices.bySupervisor && indices.bySupervisor.has(sup)) {
                            indices.bySupervisor.get(sup).forEach(id => unionIds.add(id));
                            foundAny = true;
                        }
                    });
                    if (foundAny) setsToIntersect.push(unionIds);
                    else return [];
                }
            }

            if (!isExcluded('pasta') && filters.pasta) {
                hasFilter = true;
                if (indices.byPasta && indices.byPasta.has(filters.pasta)) {
                    setsToIntersect.push(indices.byPasta.get(filters.pasta));
                } else {
                    return [];
                }
            }

            if (!isExcluded('tipoVenda') && filters.tipoVenda && filters.tipoVenda.size > 0) {
                hasFilter = true;
                const ids = new Set();
                let foundAny = false;
                filters.tipoVenda.forEach(t => {
                    if (indices.byTipoVenda && indices.byTipoVenda.has(t)) {
                        indices.byTipoVenda.get(t).forEach(id => ids.add(id));
                        foundAny = true;
                    }
                });
                if (foundAny) setsToIntersect.push(ids);
                else return [];
            }

            if (!isExcluded('seller') && filters.seller && filters.seller.size > 0) {
                hasFilter = true;
                const ids = new Set();
                let foundAny = false;
                filters.seller.forEach(s => {
                    if (indices.byRca && indices.byRca.has(s)) {
                        indices.byRca.get(s).forEach(id => ids.add(id));
                        foundAny = true;
                    }
                });
                if (foundAny) setsToIntersect.push(ids);
                else return [];
            }

            if (!isExcluded('supplier') && filters.supplier && filters.supplier.size > 0) {
                hasFilter = true;
                const ids = new Set();
                let foundAny = false;
                filters.supplier.forEach(s => {
                    if (indices.bySupplier && indices.bySupplier.has(s)) {
                        indices.bySupplier.get(s).forEach(id => ids.add(id));
                        foundAny = true;
                    }
                });
                if (foundAny) setsToIntersect.push(ids);
                else return [];
            }

            if (!isExcluded('product') && filters.product && filters.product.size > 0) {
                hasFilter = true;
                const ids = new Set();
                let foundAny = false;
                filters.product.forEach(p => {
                    if (indices.byProduct && indices.byProduct.has(p)) {
                        indices.byProduct.get(p).forEach(id => ids.add(id));
                        foundAny = true;
                    }
                });
                if (foundAny) setsToIntersect.push(ids);
                else return [];
            }

            if (!isExcluded('city') && filters.city) {
                hasFilter = true;
                if (indices.byCity && indices.byCity.has(filters.city)) {
                    setsToIntersect.push(indices.byCity.get(filters.city));
                } else {
                    return [];
                }
            }

            if (setsToIntersect.length === 0 && !hasFilter && !filters.clientCodes) {
                if (dataset.values && typeof dataset.values === 'function') {
                    return dataset.values();
                }
                if (Array.isArray(dataset)) return dataset;

                const all = [];
                for(let i=0; i<dataset.length; i++) all.push(getItem(i));
                return all;
            }

            let resultIds;
            if (setsToIntersect.length > 0) {
                // --- OPTIMIZATION START ---
                // Sort sets by size to intersect the smallest sets first.
                setsToIntersect.sort((a, b) => a.size - b.size);

                // Start with the smallest set.
                resultIds = new Set(setsToIntersect[0]);

                // Intersect with the rest of the sets.
                for (let i = 1; i < setsToIntersect.length; i++) {
                    // Stop early if the result is already empty.
                    if (resultIds.size === 0) break;

                    const currentSet = setsToIntersect[i];
                    for (const id of resultIds) {
                        if (!currentSet.has(id)) {
                            resultIds.delete(id);
                        }
                    }
                }
                // --- OPTIMIZATION END ---
            } else if (filters.clientCodes) {
                 const allData = [];
                 // Use iteration if values() unavailable
                 if (dataset.values && typeof dataset.values === 'function') {
                     const vals = dataset.values();
                     for(let i=0; i<vals.length; i++) if(filters.clientCodes.has(vals[i].CODCLI)) allData.push(vals[i]);
                 } else {
                     for(let i=0; i<dataset.length; i++) {
                         const item = getItem(i);
                         if(filters.clientCodes.has(item.CODCLI)) allData.push(item);
                     }
                 }
                 return allData;
            } else {
                // Should be unreachable due to first check, but safe fallback
                if (dataset.values && typeof dataset.values === 'function') return dataset.values();
                if (Array.isArray(dataset)) return dataset;
                const all = [];
                for(let i=0; i<dataset.length; i++) all.push(getItem(i));
                return all;
            }

            const result = [];
            for (const id of resultIds) {
                const item = getItem(id);
                if (!filters.clientCodes || filters.clientCodes.has(item.CODCLI)) {
                    result.push(item);
                }
            }
            return result;
        };

        function getComparisonFilteredData(options = {}) {
            const { excludeFilter = null } = options;

            const sellersSet = new Set(selectedComparisonSellers);
            const suppliersSet = new Set(selectedComparisonSuppliers);
            const productsSet = new Set(selectedComparisonProducts);
            const tiposVendaSet = new Set(selectedComparisonTiposVenda);
            const redeSet = new Set(selectedComparisonRedes);
            const supervisorSet = new Set(selectedComparisonSupervisors);

            const pasta = currentComparisonFornecedor;
            const city = comparisonCityFilter.value.trim().toLowerCase();
            const filial = comparisonFilialFilter.value;

            let clientCodes = null;
            if (comparisonRedeGroupFilter) {
                let clients = allClientsData;
                if (comparisonRedeGroupFilter === 'com_rede') {
                    clients = clients.filter(c => c.ramo && c.ramo !== 'N/A');
                     if (redeSet.size > 0) {
                        clients = clients.filter(c => redeSet.has(c.ramo));
                    }
                } else if (comparisonRedeGroupFilter === 'sem_rede') {
                    clients = clients.filter(c => !c.ramo || c.ramo === 'N/A');
                }
                clientCodes = new Set(clients.map(c => c['Código']));
            }

            const filters = {
                filial,
                supervisor: supervisorSet,
                pasta,
                tipoVenda: tiposVendaSet,
                seller: sellersSet,
                supplier: suppliersSet,
                product: productsSet,
                city,
                clientCodes
            };

            return {
                currentSales: getFilteredDataFromIndices(optimizedData.indices.current, optimizedData.salesById, filters, excludeFilter),
                historySales: getFilteredDataFromIndices(optimizedData.indices.history, optimizedData.historyById, filters, excludeFilter)
            };
        }


        function updateAllComparisonFilters() {
            const { currentSales: supervisorCurrent, historySales: supervisorHistory } = getComparisonFilteredData({ excludeFilter: 'supervisor' });
            const supervisorOptionsData = [...supervisorCurrent, ...supervisorHistory];
            selectedComparisonSupervisors = updateSupervisorFilter(document.getElementById('comparison-supervisor-filter-dropdown'), document.getElementById('comparison-supervisor-filter-text'), selectedComparisonSupervisors, supervisorOptionsData);

            const { currentSales: sellerCurrent, historySales: sellerHistory } = getComparisonFilteredData({ excludeFilter: 'seller' });
            const sellerOptionsData = [...sellerCurrent, ...sellerHistory];
            selectedComparisonSellers = updateSellerFilter(selectedComparisonSupervisors, comparisonVendedorFilterDropdown, comparisonVendedorFilterText, selectedComparisonSellers, sellerOptionsData);

            const { currentSales: supplierCurrent, historySales: supplierHistory } = getComparisonFilteredData({ excludeFilter: 'supplier' });
            const supplierOptionsData = [...supplierCurrent, ...supplierHistory];
            selectedComparisonSuppliers = updateSupplierFilter(comparisonSupplierFilterDropdown, comparisonSupplierFilterText, selectedComparisonSuppliers, supplierOptionsData, 'comparison');

            const { currentSales: tvCurrent, historySales: tvHistory } = getComparisonFilteredData({ excludeFilter: 'tipoVenda' });
            selectedComparisonTiposVenda = updateTipoVendaFilter(comparisonTipoVendaFilterDropdown, comparisonTipoVendaFilterText, selectedComparisonTiposVenda, [...tvCurrent, ...tvHistory]);

            updateComparisonProductFilter();

            const { currentSales: cityCurrent, historySales: cityHistory } = getComparisonFilteredData({ excludeFilter: 'city' });
            const cityOptionsData = [...cityCurrent, ...cityHistory];
            updateComparisonCitySuggestions(cityOptionsData);

            const { currentSales: pastaCurrent, historySales: pastaHistory } = getComparisonFilteredData({ excludeFilter: 'pasta' });
            const pastaOptionsData = [...pastaCurrent, ...pastaHistory];
            const pepsicoBtn = document.querySelector('#comparison-fornecedor-toggle-container button[data-fornecedor="PEPSICO"]');
            const multimarcasBtn = document.querySelector('#comparison-fornecedor-toggle-container button[data-fornecedor="MULTIMARCAS"]');
            const hasPepsico = pastaOptionsData.some(s => s.OBSERVACAOFOR === 'PEPSICO');
            const hasMultimarcas = pastaOptionsData.some(s => s.OBSERVACAOFOR === 'MULTIMARCAS');
            pepsicoBtn.disabled = !hasPepsico;
            multimarcasBtn.disabled = !hasMultimarcas;
            pepsicoBtn.classList.toggle('opacity-50', !hasPepsico);
            multimarcasBtn.classList.toggle('opacity-50', !hasMultimarcas);
        }

        function updateProductFilter(dropdown, filterText, selectedArray, dataSource, filterType = 'comparison', skipRender = false) {
            const forbidden = ['PRODUTO', 'DESCRICAO', 'CODIGO', 'CÓDIGO', 'DESCRIÇÃO'];
            const searchInput = dropdown.querySelector('input[type="text"]');
            const listContainer = dropdown.querySelector('div[id$="-list"]');
            const searchTerm = searchInput ? searchInput.value.toLowerCase() : '';

            const products = [...new Map(dataSource.map(s => [s.PRODUTO, s.DESCRICAO]))
                .entries()]
                .filter(([code, desc]) => code && desc && !forbidden.includes(code.toUpperCase()) && !forbidden.includes(desc.toUpperCase()))
                .sort((a,b) => a[1].localeCompare(b[1]));

            // Filter selectedArray to keep only items present in the current dataSource
            const availableProductCodes = new Set(products.map(p => p[0]));
            selectedArray = selectedArray.filter(code => availableProductCodes.has(code));

            const filteredProducts = searchTerm.length > 0
                ? products.filter(([code, name]) =>
                    name.toLowerCase().includes(searchTerm) || code.toLowerCase().includes(searchTerm)
                  )
                : products;

            if (!skipRender) {
                const htmlParts = [];
                for (let i = 0; i < filteredProducts.length; i++) {
                    const [code, name] = filteredProducts[i];
                    const isChecked = selectedArray.includes(code);
                    htmlParts.push(`
                        <label class="flex items-center p-2 hover:bg-slate-600 cursor-pointer">
                            <input type="checkbox" data-filter-type="${filterType}" class="form-checkbox h-4 w-4 bg-slate-800 border-slate-500 rounded text-teal-500 focus:ring-teal-500" value="${code}" ${isChecked ? 'checked' : ''}>
                            <span class="ml-2 text-xs">(${code}) ${name}</span>
                        </label>`);
                }
                listContainer.innerHTML = htmlParts.join('');
            }

            if (selectedArray.length === 0) {
                filterText.textContent = 'Todos os Produtos';
            } else if (selectedArray.length === 1) {
                const productsInfo = new Map(products);
                filterText.textContent = productsInfo.get(selectedArray[0]) || '1 selecionado';
            } else {
                filterText.textContent = `${selectedArray.length} produtos selecionados`;
            }
            return selectedArray;
        }

        function updateComparisonProductFilter() {
            const { currentSales, historySales } = getComparisonFilteredData({ excludeFilter: 'product' });
            selectedComparisonProducts = updateProductFilter(comparisonProductFilterDropdown, comparisonProductFilterText, selectedComparisonProducts, [...currentSales, ...historySales], 'comparison');
        }

        function updateStockProductFilter(skipRender = false) {
            const data = getStockFilteredData({excludeFilter: 'product'});
            selectedStockProducts = updateProductFilter(stockProductFilterDropdown, stockProductFilterText, selectedStockProducts, [...data.sales, ...data.history], 'stock', skipRender);
        }

        function updateStockSupplierFilter(skipRender = false) {
            const data = getStockFilteredData({excludeFilter: 'supplier'});
            selectedStockSuppliers = updateSupplierFilter(stockSupplierFilterDropdown, stockSupplierFilterText, selectedStockSuppliers, [...data.sales, ...data.history], 'stock', skipRender);
        }

        function updateStockSellerFilter(skipRender = false) {
            const data = getStockFilteredData({excludeFilter: 'seller'});
            selectedStockSellers = updateSellerFilter(selectedStockSupervisors, stockVendedorFilterDropdown, stockVendedorFilterText, selectedStockSellers, [...data.sales, ...data.history], skipRender);
        }

        function updateStockCitySuggestions(dataSource) {
            const forbidden = ['CIDADE', 'MUNICIPIO', 'CIDADE_CLIENTE', 'NOME DA CIDADE', 'CITY'];
            const inputValue = stockCityFilter.value.toLowerCase();
            // Optimized Lookup
            const allAvailableCities = [...new Set(dataSource.map(item => {
                if (item.CIDADE) return item.CIDADE;
                if (item.CODCLI) {
                    const c = clientMapForKPIs.get(String(item.CODCLI));
                    if (c) return c.cidade || c['Nome da Cidade'];
                }
                return 'N/A';
            }).filter(c => c && c !== 'N/A' && !forbidden.includes(c.toUpperCase())))].sort();
            const filteredCities = inputValue ? allAvailableCities.filter(c => c.toLowerCase().includes(inputValue)) : allAvailableCities;
            if (filteredCities.length > 0 && document.activeElement === stockCityFilter) {
                stockCitySuggestions.innerHTML = filteredCities.map(c => `<div class="p-2 hover:bg-slate-600 cursor-pointer">${c}</div>`).join('');
                stockCitySuggestions.classList.remove('hidden');
            } else {
                stockCitySuggestions.classList.add('hidden');
            }
        }

        function getActiveStockMap(filial) {
            const filterValue = filial || stockFilialFilter.value;
            if (filterValue === '05') {
                return stockData05;
            }
            if (filterValue === '08') {
                return stockData08;
            }
            const combinedStock = new Map(stockData05);
            stockData08.forEach((qty, code) => {
                combinedStock.set(code, (combinedStock.get(code) || 0) + qty);
            });
            return combinedStock;
        }

        function getStockFilteredData(options = {}) {
            const { excludeFilter = null } = options;

            const sellersSet = new Set(selectedStockSellers);
            const suppliersSet = new Set(selectedStockSuppliers);
            const productsSet = new Set(selectedStockProducts);
            const tiposVendaSet = new Set(selectedStockTiposVenda);
            const redeSet = new Set(selectedStockRedes);
            const supervisorSet = new Set(selectedStockSupervisors);

            const pasta = currentStockFornecedor;
            const city = stockCityFilter.value.trim().toLowerCase();
            const filial = stockFilialFilter.value;

            let clientCodes = null;
            if (excludeFilter !== 'rede') {
                if (stockRedeGroupFilter === 'com_rede' || stockRedeGroupFilter === 'sem_rede') {
                    let clients = allClientsData;
                    if (stockRedeGroupFilter === 'com_rede') {
                        clients = clients.filter(c => c.ramo && c.ramo !== 'N/A');
                        if (redeSet.size > 0) {
                            clients = clients.filter(c => redeSet.has(c.ramo));
                        }
                    } else if (stockRedeGroupFilter === 'sem_rede') {
                        clients = clients.filter(c => !c.ramo || c.ramo === 'N/A');
                    }
                    clientCodes = new Set(clients.map(c => c['Código']));
                }
            }

            const filters = {
                filial,
                supervisor: supervisorSet,
                pasta,
                tipoVenda: tiposVendaSet,
                seller: sellersSet,
                supplier: suppliersSet,
                product: productsSet,
                city,
                clientCodes
            };

            return {
                sales: getFilteredDataFromIndices(optimizedData.indices.current, optimizedData.salesById, filters, excludeFilter),
                history: getFilteredDataFromIndices(optimizedData.indices.history, optimizedData.historyById, filters, excludeFilter)
            };
        }

        function handleStockFilterChange(options = {}) {
            const { skipFilter = null } = options;

            // Debounce stock view update
            if (window.stockUpdateTimeout) clearTimeout(window.stockUpdateTimeout);
            window.stockUpdateTimeout = setTimeout(() => {
                if (skipFilter !== 'supervisor') {
                    const supervisorData = getStockFilteredData({ excludeFilter: 'supervisor' });
                    const supervisorOptionsData = [...supervisorData.sales, ...supervisorData.history];
                    selectedStockSupervisors = updateSupervisorFilter(document.getElementById('stock-supervisor-filter-dropdown'), document.getElementById('stock-supervisor-filter-text'), selectedStockSupervisors, supervisorOptionsData);
                }

                updateStockSellerFilter(skipFilter === 'seller');
                updateStockSupplierFilter(skipFilter === 'supplier');
                updateStockProductFilter(skipFilter === 'product');

                const tvData = getStockFilteredData({ excludeFilter: 'tipoVenda' });
                selectedStockTiposVenda = updateTipoVendaFilter(stockTipoVendaFilterDropdown, stockTipoVendaFilterText, selectedStockTiposVenda, [...tvData.sales, ...tvData.history], skipFilter === 'tipoVenda');

                if (skipFilter !== 'city') {
                    const cityData = getStockFilteredData({ excludeFilter: 'city' });
                    updateStockCitySuggestions([...cityData.sales, ...cityData.history]);
                }

                if (skipFilter !== 'pasta') {
                    const pastaData = getStockFilteredData({ excludeFilter: 'pasta' });
                    const pastaOptionsData = [...pastaData.sales, ...pastaData.history];
                    const pepsicoBtn = document.querySelector('#stock-fornecedor-toggle-container button[data-fornecedor="PEPSICO"]');
                    const multimarcasBtn = document.querySelector('#stock-fornecedor-toggle-container button[data-fornecedor="MULTIMARCAS"]');
                    const hasPepsico = pastaOptionsData.some(s => s.OBSERVACAOFOR === 'PEPSICO');
                    const hasMultimarcas = pastaOptionsData.some(s => s.OBSERVACAOFOR === 'MULTIMARCAS');
                    pepsicoBtn.disabled = !hasPepsico;
                    multimarcasBtn.disabled = !hasMultimarcas;
                    pepsicoBtn.classList.toggle('opacity-50', !hasPepsico);
                    multimarcasBtn.classList.toggle('opacity-50', !hasMultimarcas);
                }

                updateStockView();
            }, 10);
        }

        function updateComparisonView() {
            comparisonRenderId++;
            const currentRenderId = comparisonRenderId;
            const { currentSales, historySales } = getComparisonFilteredData();

            // Show Loading State on Charts
            const chartContainers = ['weeklyComparisonChart', 'monthlyComparisonChart', 'dailyWeeklyComparisonChart'];
            chartContainers.forEach(id => {
                if (charts[id]) {
                    charts[id].destroy();
                    delete charts[id];
                }
                const el = document.getElementById(id + 'Container');
                if(el) el.innerHTML = '<div class="flex h-full items-center justify-center"><svg class="animate-spin h-8 w-8 text-teal-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg></div>';
            });

            const currentYear = lastSaleDate.getUTCFullYear();
            const currentMonth = lastSaleDate.getUTCMonth();
            const currentMonthWeeks = getMonthWeeks(currentYear, currentMonth);

            const metrics = {
                current: { fat: 0, peso: 0, clients: 0, mixPepsico: 0, positivacaoSalty: 0, positivacaoFoods: 0 },
                history: { fat: 0, peso: 0, avgFat: 0, avgPeso: 0, avgClients: 0, avgMixPepsico: 0, avgPositivacaoSalty: 0, avgPositivacaoFoods: 0 },
                charts: {
                    weeklyCurrent: new Array(currentMonthWeeks.length).fill(0),
                    weeklyHistory: new Array(currentMonthWeeks.length).fill(0),
                    monthlyData: [],
                    supervisorData: {}
                },
                historicalDayTotals: new Array(7).fill(0), // 0=Sun, 6=Sat
                overlapSales: []
            };

            const firstWeekStart = currentMonthWeeks[0].start;
            const firstOfMonth = new Date(Date.UTC(currentYear, currentMonth, 1));
            const hasOverlap = firstWeekStart < firstOfMonth;
            const pepsicoCodfors = new Set(['707', '708']);
            const saltyCategories = ['CHEETOS', 'DORITOS', 'FANDANGOS', 'RUFFLES', 'TORCIDA'];
            const foodsCategories = ['TODDYNHO', 'TODDY ', 'QUAKER', 'KEROCOCO'];
            const norm = (s) => s ? s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase() : '';

            // Temp structures for Current processing
            const currentClientProductMap = new Map();
            const currentClientsSet = new Map();

            // Temp structures for History processing
            const historyMonths = new Map();
            const monthWeeksCache = new Map();

            // --- Async Pipeline ---

            // 1. Process Current Sales
            runAsyncChunked(currentSales, (s) => {
                const isValidType = (s.TIPOVENDA === '1' || s.TIPOVENDA === '9');
                if (isValidType) {
                    metrics.current.fat += s.VLVENDA;
                    metrics.current.peso += s.TOTPESOLIQ;
                }
                if (s.CODCLI) {
                    currentClientsSet.set(s.CODCLI, (currentClientsSet.get(s.CODCLI) || 0) + s.VLVENDA);
                    if (!currentClientProductMap.has(s.CODCLI)) currentClientProductMap.set(s.CODCLI, new Map());
                    const cMap = currentClientProductMap.get(s.CODCLI);
                    if (!cMap.has(s.PRODUTO)) cMap.set(s.PRODUTO, { val: 0, desc: s.DESCRICAO, codfor: String(s.CODFOR) });
                    cMap.get(s.PRODUTO).val += s.VLVENDA;
                }
                if (s.SUPERV && isValidType) {
                    if (!metrics.charts.supervisorData[s.SUPERV]) metrics.charts.supervisorData[s.SUPERV] = { current: 0, history: 0 };
                    metrics.charts.supervisorData[s.SUPERV].current += s.VLVENDA;
                }
                const d = parseDate(s.DTPED);
                if (d && isValidType) {
                    const wIdx = currentMonthWeeks.findIndex(w => d >= w.start && d <= w.end);
                    if (wIdx !== -1) metrics.charts.weeklyCurrent[wIdx] += s.VLVENDA;
                }
            }, () => {
                // 1.1 Finalize Current KPIs
                let currentPositiveClients = 0;
                currentClientsSet.forEach(val => { if (val >= 1) currentPositiveClients++; });
                metrics.current.clients = currentPositiveClients;

                let sumMix = 0; let countMixClients = 0; let countSalty = 0; let countFoods = 0;
                currentClientProductMap.forEach((prods) => {
                    let pepsicoCount = 0;
                    const boughtCatsSalty = new Set();
                    const boughtCatsFoods = new Set();
                    prods.forEach(pData => {
                        if (pData.val >= 1) {
                            if (pepsicoCodfors.has(pData.codfor)) pepsicoCount++;
                            const desc = norm(pData.desc);
                            saltyCategories.forEach(cat => { if (desc.includes(cat)) boughtCatsSalty.add(cat); });
                            foodsCategories.forEach(cat => { if (desc.includes(cat)) boughtCatsFoods.add(cat); });
                        }
                    });
                    if (pepsicoCount > 0) { sumMix += pepsicoCount; countMixClients++; }
                    if (boughtCatsSalty.size >= saltyCategories.length) countSalty++;
                    if (boughtCatsFoods.size >= foodsCategories.length) countFoods++;
                });
                metrics.current.mixPepsico = countMixClients > 0 ? sumMix / countMixClients : 0;
                metrics.current.positivacaoSalty = countSalty;
                metrics.current.positivacaoFoods = countFoods;

                if (currentRenderId !== comparisonRenderId) return;

                // 2. Process History Sales
                runAsyncChunked(historySales, (s) => {
                    const isValidType = (s.TIPOVENDA === '1' || s.TIPOVENDA === '9');
                    if (isValidType) {
                        metrics.history.fat += s.VLVENDA;
                        metrics.history.peso += s.TOTPESOLIQ;
                    }
                    const d = parseDate(s.DTPED);
                    if (!d) return;

                    const monthKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
                    if (!historyMonths.has(monthKey)) historyMonths.set(monthKey, { fat: 0, clients: new Map(), productMap: new Map() });
                    const mData = historyMonths.get(monthKey);

                    if (isValidType) mData.fat += s.VLVENDA;

                    if (s.CODCLI) {
                        mData.clients.set(s.CODCLI, (mData.clients.get(s.CODCLI) || 0) + s.VLVENDA);
                        if (!mData.productMap.has(s.CODCLI)) mData.productMap.set(s.CODCLI, new Map());
                        const cMap = mData.productMap.get(s.CODCLI);
                        if (!cMap.has(s.PRODUTO)) cMap.set(s.PRODUTO, { val: 0, desc: s.DESCRICAO, codfor: String(s.CODFOR) });
                        cMap.get(s.PRODUTO).val += s.VLVENDA;
                    }

                    if (s.SUPERV && isValidType) {
                        if (!metrics.charts.supervisorData[s.SUPERV]) metrics.charts.supervisorData[s.SUPERV] = { current: 0, history: 0 };
                        metrics.charts.supervisorData[s.SUPERV].history += s.VLVENDA;
                    }

                    // Accumulate Day Totals for Day Weight Calculation
                    if (isValidType) {
                        metrics.historicalDayTotals[d.getUTCDay()] += s.VLVENDA;
                    }

                    if (!monthWeeksCache.has(monthKey)) monthWeeksCache.set(monthKey, getMonthWeeks(d.getUTCFullYear(), d.getUTCMonth()));
                    const weeks = monthWeeksCache.get(monthKey);
                    const wIdx = weeks.findIndex(w => d >= w.start && d <= w.end);
                    if (wIdx !== -1 && wIdx < metrics.charts.weeklyHistory.length && isValidType) {
                        metrics.charts.weeklyHistory[wIdx] += s.VLVENDA;
                    }
                    if (hasOverlap && d >= firstWeekStart && d < firstOfMonth && isValidType) {
                        metrics.charts.weeklyCurrent[0] += s.VLVENDA;
                        metrics.overlapSales.push(s);
                    }
                }, () => {
                    if (currentRenderId !== comparisonRenderId) return;

                    // 2.1 Finalize History Metrics
                    metrics.history.avgFat = metrics.history.fat / QUARTERLY_DIVISOR;
                    metrics.history.avgPeso = metrics.history.peso / QUARTERLY_DIVISOR;
                    metrics.charts.weeklyHistory = metrics.charts.weeklyHistory.map(v => v / QUARTERLY_DIVISOR);
                    Object.values(metrics.charts.supervisorData).forEach(d => d.history /= QUARTERLY_DIVISOR);

                    // Calculate Day Weights
                    const totalHistoryDays = metrics.historicalDayTotals.reduce((a, b) => a + b, 0);
                    metrics.dayWeights = metrics.historicalDayTotals.map(v => totalHistoryDays > 0 ? v / totalHistoryDays : 0);

                    const sortedMonths = Array.from(historyMonths.keys()).sort((a, b) => {
                        const [y1, m1] = a.split('-').map(Number);
                        const [y2, m2] = b.split('-').map(Number);
                        return (y1 * 12 + m1) - (y2 * 12 + m2);
                    }).slice(-3);
                    let sumClients = 0; let sumMixPep = 0; let sumPosSalty = 0; let sumPosFoods = 0;

                    sortedMonths.forEach(mKey => {
                        const mData = historyMonths.get(mKey);
                        let posClients = 0;
                        mData.clients.forEach(v => { if(v >= 1) posClients++; });
                        sumClients += posClients;

                        let mSumMix = 0; let mCountMixClients = 0; let mCountSalty = 0; let mCountFoods = 0;
                        mData.productMap.forEach((prods) => {
                            let pepsicoCount = 0;
                            const boughtCatsSalty = new Set();
                            const boughtCatsFoods = new Set();
                            prods.forEach(pData => {
                                if (pData.val >= 1) {
                                    if (pepsicoCodfors.has(pData.codfor)) pepsicoCount++;
                                    const desc = norm(pData.desc);
                                    saltyCategories.forEach(cat => { if (desc.includes(cat)) boughtCatsSalty.add(cat); });
                                    foodsCategories.forEach(cat => { if (desc.includes(cat)) boughtCatsFoods.add(cat); });
                                }
                            });
                            if (pepsicoCount > 0) { mSumMix += pepsicoCount; mCountMixClients++; }
                            if (boughtCatsSalty.size >= saltyCategories.length) mCountSalty++;
                            if (boughtCatsFoods.size >= foodsCategories.length) mCountFoods++;
                        });
                        sumMixPep += (mCountMixClients > 0 ? mSumMix / mCountMixClients : 0);
                        sumPosSalty += mCountSalty;
                        sumPosFoods += mCountFoods;

                        const [y, m] = mKey.split('-');
                        const label = new Date(Date.UTC(parseInt(y), parseInt(m), 1)).toLocaleDateString('pt-BR', { month: 'short', year: '2-digit', timeZone: 'UTC' });
                        metrics.charts.monthlyData.push({ label, fat: mData.fat, clients: posClients });
                    });

                    metrics.history.avgClients = sumClients / QUARTERLY_DIVISOR;
                    metrics.history.avgMixPepsico = sumMixPep / QUARTERLY_DIVISOR;
                    metrics.history.avgPositivacaoSalty = sumPosSalty / QUARTERLY_DIVISOR;
                    metrics.history.avgPositivacaoFoods = sumPosFoods / QUARTERLY_DIVISOR;

                    // 3. Render Views
                    const m = metrics;
                    renderKpiCards([
                        { title: 'Faturamento Total', current: m.current.fat, history: m.history.avgFat, format: 'currency' },
                        { title: 'Peso Total (Ton)', current: m.current.peso / 1000, history: m.history.avgPeso / 1000, format: 'decimal' },
                        { title: 'Clientes Atendidos', current: m.current.clients, history: m.history.avgClients, format: 'integer' },
                        { title: 'Ticket Médio', current: m.current.clients > 0 ? m.current.fat / m.current.clients : 0, history: m.history.avgClients > 0 ? m.history.avgFat / m.history.avgClients : 0, format: 'currency' },
                        { title: 'Mix por PDV (Pepsico)', current: m.current.mixPepsico, history: m.history.avgMixPepsico, format: 'mix' },
                        { title: 'Mix Salty', current: m.current.positivacaoSalty, history: m.history.avgPositivacaoSalty, format: 'integer' },
                        { title: 'Mix Foods', current: m.current.positivacaoFoods, history: m.history.avgPositivacaoFoods, format: 'integer' }
                    ]);

                    // Weekly Chart Logic with Tendency
                    let weeklyCurrentData = [...m.charts.weeklyCurrent];
                    if (useTendencyComparison) {
                        const today = lastSaleDate;
                        const currentWeekIndex = currentMonthWeeks.findIndex(w => today >= w.start && today <= w.end);
                        const totalWeeks = currentMonthWeeks.length;
                        for (let i = 0; i < totalWeeks; i++) {
                            if (i === currentWeekIndex) {
                                const currentWeek = currentMonthWeeks[i];
                                let workingDaysPassed = 0; let totalWorkingDays = 0;
                                for (let d = new Date(currentWeek.start); d <= currentWeek.end; d.setUTCDate(d.getUTCDate() + 1)) {
                                    const dayOfWeek = d.getUTCDay();
                                    if (dayOfWeek >= 1 && dayOfWeek <= 5 && !isHoliday(d, selectedHolidays)) {
                                        totalWorkingDays++;
                                        if (d <= today) workingDaysPassed++;
                                    }
                                }
                                const salesSoFar = weeklyCurrentData[i];
                                if (workingDaysPassed > 0 && totalWorkingDays > 0) {
                                    weeklyCurrentData[i] = (salesSoFar / workingDaysPassed) * totalWorkingDays;
                                } else {
                                    weeklyCurrentData[i] = m.charts.weeklyHistory[i] || 0;
                                }
                            } else if (i > currentWeekIndex) {
                                weeklyCurrentData[i] = m.charts.weeklyHistory[i] || 0;
                            }
                        }
                    }

                    // Render Charts logic (Reusing existing drawing code)
                    if (comparisonChartType === 'weekly') {
                        monthlyComparisonChartContainer.classList.add('hidden');
                        weeklyComparisonChartContainer.classList.remove('hidden');
                        comparisonChartTitle.textContent = 'Comparativo de Faturamento Semanal';
                        const weekLabels = currentMonthWeeks.map((w, i) => `Semana ${i + 1}`);
                        createChart('weeklyComparisonChart', 'line', weekLabels, [
                            { label: useTendencyComparison ? 'Tendência Semanal' : 'Mês Atual', data: weeklyCurrentData, borderColor: '#14b8a6', tension: 0.2, pointRadius: 5, pointBackgroundColor: '#14b8a6', borderWidth: 2.5 },
                            { label: 'Média Trimestre', data: m.charts.weeklyHistory, borderColor: '#f97316', tension: 0.2, pointRadius: 5, pointBackgroundColor: '#f97316', borderWidth: 2.5 }
                        ], { plugins: { legend: { display: true, position: 'top', align: 'end' } }, layout: { padding: { bottom: 0 } } });
                    } else if (comparisonChartType === 'monthly') {
                        weeklyComparisonChartContainer.classList.add('hidden');
                        monthlyComparisonChartContainer.classList.remove('hidden');
                        const metricToggle = document.getElementById('comparison-monthly-metric-container');
                        if (metricToggle) metricToggle.classList.remove('hidden');
                        const isFat = comparisonMonthlyMetric === 'faturamento';
                        comparisonChartTitle.textContent = isFat ? 'Comparativo de Faturamento Mensal' : 'Comparativo de Clientes Atendidos Mensal';
                        const monthLabels = m.charts.monthlyData.map(d => d.label);
                        const monthValues = m.charts.monthlyData.map(d => isFat ? d.fat : d.clients);
                        let currentMonthLabel = 'Mês Atual';
                        if (currentSales.length > 0) {
                            const firstSaleDate = parseDate(currentSales[0].DTPED) || new Date();
                            currentMonthLabel = firstSaleDate.toLocaleString('pt-BR', { month: 'short', year: '2-digit' });
                        }
                        let currentVal = isFat ? m.current.fat : m.current.clients;
                        if (isFat && useTendencyComparison) {
                            const totalDays = getWorkingDaysInMonth(currentYear, currentMonth, selectedHolidays);
                            const passedDays = getPassedWorkingDaysInMonth(currentYear, currentMonth, selectedHolidays, lastSaleDate);
                            if (totalDays > 0 && passedDays > 0) { currentVal = (currentVal / passedDays) * totalDays; }
                        }
                        monthLabels.push(currentMonthLabel);
                        monthValues.push(currentVal);
                        createChart('monthlyComparisonChart', 'bar', monthLabels, [{ label: isFat ? 'Faturamento' : 'Clientes Atendidos', data: monthValues, backgroundColor: (context) => { const ctx = context.chart.ctx; const gradient = ctx.createLinearGradient(0, 0, 0, 400); gradient.addColorStop(0, 'rgba(20, 184, 166, 0.8)'); gradient.addColorStop(1, 'rgba(126, 34, 206, 0.8)'); return gradient; }, borderColor: (context) => { const ctx = context.chart.ctx; const gradient = ctx.createLinearGradient(0, 0, 0, 400); gradient.addColorStop(0, '#14b8a6'); gradient.addColorStop(1, '#7e22ce'); return gradient; }, borderWidth: 2 }], { layout: { padding: { top: 20 } }, plugins: { legend: { display: false }, datalabels: { color: '#ffffff', anchor: 'end', align: 'top', offset: 4, font: { weight: 'bold' }, formatter: (value) => { if (isFat) { if (value >= 1000000) return 'R$ ' + (value / 1000000).toFixed(2) + ' M'; return 'R$ ' + (value / 1000).toFixed(0) + 'k'; } else { return value.toLocaleString('pt-BR'); } } }, tooltip: { callbacks: { label: function(context) { let label = context.dataset.label || ''; if (label) label += ': '; if (context.parsed.y !== null) { if (isFat) label += new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(context.parsed.y); else label += context.parsed.y.toLocaleString('pt-BR') + ' clientes'; } return label; } } } } });
                    }

                    // Daily Chart (Simplified re-calc for now, or could optimize further)
                    const salesByWeekAndDay = {};
                    currentMonthWeeks.forEach((w, i) => { salesByWeekAndDay[i + 1] = new Array(7).fill(0); });
                    currentSales.forEach(s => { const d = parseDate(s.DTPED); if(d) { const wIdx = currentMonthWeeks.findIndex(w => d >= w.start && d <= w.end); if(wIdx !== -1) salesByWeekAndDay[wIdx+1][d.getUTCDay()] += s.VLVENDA; } });
                    if (m.overlapSales && m.overlapSales.length > 0) { m.overlapSales.forEach(s => { const d = parseDate(s.DTPED); if (d) salesByWeekAndDay[1][d.getUTCDay()] += s.VLVENDA; }); }

                    // --- INICIO DA MODIFICAÇÃO: Tendência no Gráfico Diário ---
                    if (useTendencyComparison) {
                        const today = lastSaleDate;
                        const currentWeekIndex = currentMonthWeeks.findIndex(w => today >= w.start && today <= w.end);

                        // 1. Project Current Week
                        if (currentWeekIndex !== -1) {
                            const currentWeek = currentMonthWeeks[currentWeekIndex];
                            let workingDaysPassed = 0; let totalWorkingDays = 0;
                            const remainingDaysIndices = [];

                            for (let d = new Date(currentWeek.start); d <= currentWeek.end; d.setUTCDate(d.getUTCDate() + 1)) {
                                const dayOfWeek = d.getUTCDay();
                                if (dayOfWeek >= 1 && dayOfWeek <= 5 && !isHoliday(d, selectedHolidays)) {
                                    totalWorkingDays++;
                                    if (d <= today) workingDaysPassed++;
                                    else remainingDaysIndices.push(dayOfWeek);
                                }
                            }

                            if (workingDaysPassed > 0 && totalWorkingDays > 0) {
                                const weekData = salesByWeekAndDay[currentWeekIndex + 1];
                                const salesSoFar = weekData.reduce((a, b) => a + b, 0);
                                const projectedWeekTotal = (salesSoFar / workingDaysPassed) * totalWorkingDays;
                                const remainder = projectedWeekTotal - salesSoFar;

                                if (remainder > 0 && remainingDaysIndices.length > 0) {
                                    const weightsForRemaining = remainingDaysIndices.map(d => m.dayWeights[d] || 0);
                                    const totalWeightRemaining = weightsForRemaining.reduce((a, b) => a + b, 0);

                                    remainingDaysIndices.forEach(dayIndex => {
                                        const weight = m.dayWeights[dayIndex] || 0;
                                        // If weights are available, use them. Otherwise distribute evenly.
                                        const share = totalWeightRemaining > 0 ? (weight / totalWeightRemaining) : (1 / remainingDaysIndices.length);
                                        weekData[dayIndex] = remainder * share;
                                    });
                                }
                            }
                        }

                        // 2. Fill Future Weeks with Historical Average (Distributed by Day Weights)
                        const weightsMonFri = [1, 2, 3, 4, 5].map(d => m.dayWeights[d] || 0);
                        const totalWeightMonFri = weightsMonFri.reduce((a, b) => a + b, 0);

                        for (let i = currentWeekIndex + 1; i < currentMonthWeeks.length; i++) {
                            const historicalTotal = m.charts.weeklyHistory[i] || 0;
                            if (historicalTotal > 0) {
                                const weekData = salesByWeekAndDay[i + 1];
                                // Fill Mon(1) to Fri(5)
                                for (let d = 1; d <= 5; d++) {
                                    const weight = m.dayWeights[d] || 0;
                                    const share = totalWeightMonFri > 0 ? (weight / totalWeightMonFri) : (1 / 5);
                                    weekData[d] = historicalTotal * share;
                                }
                            }
                        }
                    }
                    // --- FIM DA MODIFICAÇÃO ---

                    const dayNames = ['Domingo', 'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado'];
                    const professionalPalette = ['#a855f7', '#6366f1', '#ec4899', '#f97316', '#8b5cf6', '#06b6d4', '#f59e0b'];
                    const dailyBreakdownDatasets = dayNames.map((dayName, dayIndex) => ({ label: dayName, data: currentMonthWeeks.map((week, weekIndex) => salesByWeekAndDay[weekIndex + 1][dayIndex]), backgroundColor: professionalPalette[dayIndex % professionalPalette.length] }));
                    const weekLabelsForDailyChart = currentMonthWeeks.map((week, index) => { const startDateStr = week.start.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', timeZone: 'UTC' }); const endDateStr = week.end.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', timeZone: 'UTC' }); return `S${index + 1} (${startDateStr} à ${endDateStr})`; });

                    if (dailyBreakdownDatasets.some(ds => ds.data.some(d => d > 0))) {
                        createChart('dailyWeeklyComparisonChart', 'bar', weekLabelsForDailyChart, dailyBreakdownDatasets, {
                            plugins: {
                                legend: { display: true, position: 'top' },
                                tooltip: {
                                    mode: 'point',
                                    intersect: true,
                                    callbacks: {
                                        label: function(context) {
                                            let label = context.dataset.label || '';
                                            if (label) label += ': ';
                                            if (context.parsed.y !== null) {
                                                label += new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(context.parsed.y);
                                            }
                                            return label;
                                        },
                                        afterBody: function(context) {
                                            // Calculate Week Total
                                            const weekIndex = context[0].dataIndex; // All items in tooltip share same index (if grouped) or point
                                            // Ensure we are accessing the modified salesByWeekAndDay
                                            const weekData = salesByWeekAndDay[weekIndex + 1];
                                            const total = weekData.reduce((a, b) => a + b, 0);
                                            return '\nSemana: ' + new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(total);
                                        }
                                    }
                                },
                                datalabels: { display: false }
                            },
                            scales: { x: { stacked: false }, y: { stacked: false, ticks: { callback: (v) => (v / 1000).toFixed(0) + 'k' } } }
                        });
                    } else {
                        showNoDataMessage('dailyWeeklyComparisonChart', 'Sem dados para exibir.');
                    }

                    // Supervisor Table
                    const supervisorTableBody = document.getElementById('supervisorComparisonTableBody');
                    const supRows = Object.entries(m.charts.supervisorData).map(([sup, data]) => { const variation = data.history > 0 ? ((data.current - data.history) / data.history) * 100 : (data.current > 0 ? 100 : 0); const colorClass = variation > 0 ? 'text-green-400' : variation < 0 ? 'text-red-400' : 'text-slate-400'; return `<tr class="hover:bg-slate-700"><td class="px-4 py-2">${sup}</td><td class="px-4 py-2 text-right">${data.history.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</td><td class="px-4 py-2 text-right">${data.current.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</td><td class="px-4 py-2 text-right ${colorClass}">${variation.toFixed(2)}%</td></tr>`; }).join('');
                    supervisorTableBody.innerHTML = supRows;
                }, () => currentRenderId !== comparisonRenderId); // Cancel check
            }, () => currentRenderId !== comparisonRenderId); // Cancel check
        }

        function updateStockView() {
            stockRenderId++;
            const currentRenderId = stockRenderId;

            const { sales: filteredSales, history: filteredHistory } = getStockFilteredData();

            const filial = stockFilialFilter.value;
            const activeStockMap = getActiveStockMap(filial);

            // Sets for fast lookup
            const selectedSuppliersSet = new Set(selectedStockSuppliers);
            const selectedProductsSet = new Set(selectedStockProducts);
            const currentPasta = currentStockFornecedor;

            const productAnalysis = new Map();
            const productsWithFilteredActivity = new Set(); // Changed from Set to array logic below for chunking, but Set used for uniqueness

            // --- OPTIMIZATION: Pre-aggregate Sales Data by Product ---
            // Map<ProductCode, Array<Sale>>
            const salesByProduct = new Map();
            const historyByProduct = new Map();
            const historySalesListByProduct = new Map();
            const totalQtyByProduct = new Map();
            const currentMonthQtyByProduct = new Map();
            const uniqueMonthsByProduct = new Map();

            // Sync Pre-aggregation (O(N) is fast)
            const processSaleForAggregation = (s, isHistory) => {
                const p = s.PRODUTO;
                // Don't add to productsWithFilteredActivity yet, do it in the filtering loop below to respect stock filters

                if (!salesByProduct.has(p)) salesByProduct.set(p, []);
                salesByProduct.get(p).push(s);

                if (!uniqueMonthsByProduct.has(p)) uniqueMonthsByProduct.set(p, new Set());
                const d = parseDate(s.DTPED);
                if (d) uniqueMonthsByProduct.get(p).add(`${d.getUTCFullYear()}-${d.getUTCMonth()}`);

                const qty = s.QTVENDA_EMBALAGEM_MASTER;
                totalQtyByProduct.set(p, (totalQtyByProduct.get(p) || 0) + qty);

                if (isHistory) {
                    if (!historySalesListByProduct.has(p)) historySalesListByProduct.set(p, []);
                    historySalesListByProduct.get(p).push(s);

                    if (!historyByProduct.has(p)) historyByProduct.set(p, 0);
                    historyByProduct.set(p, historyByProduct.get(p) + 1);
                } else {
                    currentMonthQtyByProduct.set(p, (currentMonthQtyByProduct.get(p) || 0) + qty);
                }
            };

            filteredSales.forEach(s => processSaleForAggregation(s, false));
            filteredHistory.forEach(s => processSaleForAggregation(s, true));

            // Build the list of products to analyze based on STOCK or SALES activity + Filters
            activeStockMap.forEach((qty, productCode) => {
                if (qty > 0) {
                    const details = productDetailsMap.get(productCode);
                    if (!details) return;

                    if (selectedSuppliersSet.size > 0 && !selectedSuppliersSet.has(String(details.codfor))) return;
                    if (selectedProductsSet.size > 0 && !selectedProductsSet.has(String(productCode))) return;

                    if (currentPasta) {
                        const pastaDoProduto = optimizedData.productPastaMap.get(productCode) || '';
                        if (pastaDoProduto !== currentPasta) return;
                    }

                    productsWithFilteredActivity.add(productCode);
                }
            });

            // Also include products with sales even if 0 stock (logic from original)
            // Iterate salesByProduct keys (which implies sales existed)
            for (const productCode of totalQtyByProduct.keys()) {
                 if (productsWithFilteredActivity.has(productCode)) continue; // Already added

                 const details = productDetailsMap.get(productCode);
                 if (!details) continue; // Should be in details map if processed correctly

                 if (selectedSuppliersSet.size > 0 && !selectedSuppliersSet.has(String(details.codfor))) continue;
                 if (selectedProductsSet.size > 0 && !selectedProductsSet.has(String(productCode))) continue;
                 if (currentPasta) {
                    const pastaDoProduto = optimizedData.productPastaMap.get(productCode) || '';
                    if (pastaDoProduto !== currentPasta) continue;
                 }
                 productsWithFilteredActivity.add(productCode);
            }

            // Show Loading
            stockAnalysisTableBody.innerHTML = '<tr><td colspan="6" class="text-center p-8"><div class="flex justify-center items-center"><svg class="animate-spin h-8 w-8 text-teal-500 mr-3" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg><span class="text-slate-400">Calculando estoque e tendências...</span></div></td></tr>';

            // Convert to array for chunking
            const productsArray = Array.from(productsWithFilteredActivity);

            // Pre-calculate global dates for trend
            const endDate = parseDate(sortedWorkingDays[sortedWorkingDays.length - 1]);

            // ASYNC PROCESS
            runAsyncChunked(productsArray, (productCode) => {
                if (!activeProductCodesFromCadastro.has(productCode)) return;

                const details = productDetailsMap.get(productCode) || {
                    descricao: `Produto ${productCode}`,
                    fornecedor: 'N/A',
                    codfor: 'N/A'
                };

                const stock = activeStockMap.get(productCode) || 0;
                const totalQtySold = totalQtyByProduct.get(productCode) || 0;

                if (stock <= 0 && totalQtySold <= 0) return;

                const hasHistory = historyByProduct.has(productCode);
                const currentMonthSalesQty = currentMonthQtyByProduct.get(productCode) || 0;
                const soldThisMonth = currentMonthSalesQty > 0;

                const productAllSales = salesByProduct.get(productCode) || [];

                const productCadastroDate = parseDate(details.dtCadastro);
                let productFirstWorkingDayIndex = 0;

                if (productCadastroDate) {
                    const cadastroDateString = productCadastroDate.toISOString().split('T')[0];
                    productFirstWorkingDayIndex = sortedWorkingDays.findIndex(d => d >= cadastroDateString);
                    if (productFirstWorkingDayIndex === -1) {
                        productFirstWorkingDayIndex = sortedWorkingDays.length;
                    }
                }

                const productMaxLifeInWorkingDays = sortedWorkingDays.length - productFirstWorkingDayIndex;
                const daysFromBox = customWorkingDaysStock;
                let effectiveDaysToCalculate;

                const isFactuallyNewOrReactivated = (!hasHistory && soldThisMonth);

                if (isFactuallyNewOrReactivated) {
                    const daysToConsider = (daysFromBox > 0) ? daysFromBox : passedWorkingDaysCurrentMonth;
                    effectiveDaysToCalculate = Math.min(passedWorkingDaysCurrentMonth, daysToConsider);
                } else {
                    if (daysFromBox > 0) {
                        effectiveDaysToCalculate = Math.min(daysFromBox, productMaxLifeInWorkingDays);
                    } else {
                        effectiveDaysToCalculate = productMaxLifeInWorkingDays;
                    }
                }

                const daysDivisor = effectiveDaysToCalculate > 0 ? effectiveDaysToCalculate : 1;
                const targetIndex = Math.max(0, sortedWorkingDays.length - daysDivisor);
                const startDate = parseDate(sortedWorkingDays[targetIndex]);

                let totalQtySoldInRange = 0;
                // Optimized loop
                productAllSales.forEach(sale => {
                    const saleDate = parseDate(sale.DTPED);
                    if (saleDate && saleDate >= startDate && saleDate <= endDate) {
                        totalQtySoldInRange += sale.QTVENDA_EMBALAGEM_MASTER;
                    }
                });

                let dailyAvgSale = totalQtySoldInRange / daysDivisor;
                const isNew = productMaxLifeInWorkingDays <= passedWorkingDaysCurrentMonth;
                const trendDays = dailyAvgSale > 0 ? (stock / dailyAvgSale) : (stock > 0 ? Infinity : 0);

                const productHistorySales = historySalesListByProduct.get(productCode) || [];
                const monthlyAvgSale = calculateStockMonthlyAverage(productHistorySales);

                productAnalysis.set(productCode, {
                    code: productCode,
                    ...details,
                    stock,
                    monthlyAvgSale,
                    dailyAvgSale,
                    trendDays,
                    currentMonthSalesQty,
                    isNew,
                    hasHistory,
                    soldThisMonth
                });
            }, () => {
                // --- ON COMPLETE (Render) ---
                if (currentRenderId !== stockRenderId) return;

                let sortedAnalysis = [...productAnalysis.values()].sort((a, b) => {
                     const trendA = isFinite(a.trendDays) ? a.trendDays : -1;
                     const trendB = isFinite(b.trendDays) ? b.trendDays : -1;
                    return trendB - trendA;
                });

                if (stockTrendFilter !== 'all') {
                    sortedAnalysis = sortedAnalysis.filter(item => {
                        const trend = item.trendDays;
                        if (stockTrendFilter === 'low') return isFinite(trend) && trend < 15;
                        if (stockTrendFilter === 'medium') return isFinite(trend) && trend >= 15 && trend < 30;
                        if (stockTrendFilter === 'good') return isFinite(trend) && trend >= 30;
                        return false;
                    });
                }

                stockAnalysisTableBody.innerHTML = sortedAnalysis.slice(0, 500).map(item => {
                    let trendText;
                    let newTag = item.isNew ? ` <span class="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-blue-500/30 text-blue-300 ml-1">NOVO</span>` : '';

                    if (!isFinite(item.trendDays) && item.stock > 0) {
                        trendText = `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-500/30 text-gray-300">S/ VENDA</span>`;
                    } else if (isFinite(item.trendDays)) {
                        const trendDays = Math.floor(item.trendDays);
                        let trendColor = 'text-slate-400';
                        if (trendDays < 15) trendColor = 'text-red-400';
                        else if (trendDays < 30) trendColor = 'text-yellow-400';
                        else trendColor = 'text-green-400';
                        trendText = `<span class="font-bold ${trendColor}">${trendDays} dias</span>`;
                    } else {
                        trendText = '-';
                    }

                    return `
                        <tr class="hover:bg-slate-700/50">
                            <td class="px-4 py-2 text-xs">(${item.code}) ${item.descricao}</td>
                            <td class="px-4 py-2 text-xs">(${item.codfor}) ${item.fornecedor.split(' ').slice(0, 4).join(' ')}</td>
                            <td class="px-4 py-2 text-right text-xs">${item.stock.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                            <td class="px-4 py-2 text-right text-xs">${item.monthlyAvgSale.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                            <td class="px-4 py-2 text-right text-xs">${item.dailyAvgSale.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                            <td class="px-4 py-2 text-right text-xs flex items-center justify-end">${trendText}${newTag}</td>
                        </tr>
                    `;
                }).join('');

                const growth = [];
                const decline = [];
                const newProducts = [];
                const lostProducts = [];

                productAnalysis.forEach(p => {
                     if (p.soldThisMonth && p.hasHistory) {
                            const variation = p.monthlyAvgSale > 0 ? ((p.currentMonthSalesQty - p.monthlyAvgSale) / p.monthlyAvgSale) * 100 : (p.currentMonthSalesQty > 0 ? Infinity : 0);
                            const productWithVariation = { ...p, variation };

                        if (p.currentMonthSalesQty >= p.monthlyAvgSale) {
                            growth.push(productWithVariation);
                        } else if (p.currentMonthSalesQty < p.monthlyAvgSale && p.monthlyAvgSale > 0) {
                            decline.push(productWithVariation);
                        }
                     } else if (p.soldThisMonth && !p.hasHistory) {
                        newProducts.push(p);
                     } else if (!p.soldThisMonth && p.stock > 0) {
                        lostProducts.push(p);
                     }
                });

                const renderProductTable = (bodyElement, data, showVariation = true) => {
                    bodyElement.innerHTML = data.map(p => {
                        const variation = p.monthlyAvgSale > 0 ? ((p.currentMonthSalesQty - p.monthlyAvgSale) / p.monthlyAvgSale) * 100 : (p.currentMonthSalesQty > 0 ? Infinity : 0);
                        const colorClass = variation > 0 ? 'text-green-400' : (variation < 0 ? 'text-red-400' : 'text-slate-400');
                        let variationText = '0%';
                        if (isFinite(variation)) {
                            variationText = `${variation.toFixed(0)}%`;
                        } else if (variation === Infinity) {
                             variationText = 'Novo';
                        }

                        return `
                            <tr class="hover:bg-slate-700/50">
                                <td class="px-2 py-1.5 text-xs">(${p.code}) ${p.descricao}</td>
                                <td class="px-2 py-1.5 text-xs text-right">${p.currentMonthSalesQty.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                                <td class="px-2 py-1.5 text-xs text-right">${p.monthlyAvgSale.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                                ${showVariation ? `<td class="px-2 py-1.5 text-xs text-right font-bold ${colorClass}">${variationText}</td>` : ''}
                                <td class="px-2 py-1.5 text-xs text-right">${p.stock.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                            </tr>
                        `;
                    }).join('');
                };

                growth.sort((a, b) => b.variation - a.variation);
                decline.sort((a, b) => a.variation - b.variation);
                newProducts.sort((a,b) => b.currentMonthSalesQty - a.currentMonthSalesQty);
                lostProducts.sort((a,b) => b.monthlyAvgSale - a.monthlyAvgSale);

                renderProductTable(growthTableBody, growth);
                renderProductTable(declineTableBody, decline);
                renderProductTable(newProductsTableBody, newProducts, false);
                renderProductTable(lostProductsTableBody, lostProducts, false);
            }, () => currentRenderId !== stockRenderId);
        }


        function getInnovationsMonthFilteredData(options = {}) {
            const { excludeFilter = null } = options;

            const sellers = selectedInnovationsMonthSellers;
            const city = innovationsMonthCityFilter.value.trim().toLowerCase();
            const filial = innovationsMonthFilialFilter.value;

            let clients = allClientsData;

            if (filial !== 'ambas') {
                clients = clients.filter(c => clientLastBranch.get(c['Código']) === filial);
            }

            if (excludeFilter !== 'supervisor' && selectedInnovationsSupervisors.length > 0) {
                const rcasSet = new Set();
                selectedInnovationsSupervisors.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => rcasSet.add(rca));
                });
                clients = clients.filter(c => {
                    const clientRcas = (c.rcas && Array.isArray(c.rcas)) ? c.rcas : [];
                    return clientRcas.some(rca => rcasSet.has(rca));
                });
            }
            if (excludeFilter !== 'seller' && sellers.length > 0) {
                const rcasOfSellers = new Set();
                 sellers.forEach(sellerName => {
                    const rcaCode = optimizedData.rcaCodeByName.get(sellerName);
                    if (rcaCode) rcasOfSellers.add(rcaCode);
                });
                clients = clients.filter(c => {
                    const clientRcas = (c.rcas && Array.isArray(c.rcas)) ? c.rcas : [];
                    return clientRcas.some(rca => rcasOfSellers.has(rca));
                });
            }
            if (excludeFilter !== 'city' && city) {
                clients = clients.filter(c => c.cidade && c.cidade.toLowerCase() === city);
            }

            return { clients };
        }

        function resetInnovationsMonthFilters() {
            selectedInnovationsSupervisors = [];
            innovationsMonthCityFilter.value = '';
            innovationsMonthFilialFilter.value = 'ambas';
            innovationsMonthCategoryFilter.value = '';
            selectedInnovationsMonthSellers = [];
            selectedInnovationsMonthTiposVenda = [];

            selectedInnovationsSupervisors = updateSupervisorFilter(document.getElementById('innovations-month-supervisor-filter-dropdown'), document.getElementById('innovations-month-supervisor-filter-text'), selectedInnovationsSupervisors, allSalesData);
            updateSellerFilter(selectedInnovationsSupervisors, innovationsMonthVendedorFilterDropdown, innovationsMonthVendedorFilterText, [], allSalesData);
            selectedInnovationsMonthTiposVenda = updateTipoVendaFilter(innovationsMonthTipoVendaFilterDropdown, innovationsMonthTipoVendaFilterText, selectedInnovationsMonthTiposVenda, [...allSalesData, ...allHistoryData]);
            updateInnovationsMonthView();
        }

        function updateInnovationsMonthView() {
            const selectedCategory = innovationsMonthCategoryFilter.value;

            // Initialize Global Categories if not already done (Optimization)
            if (!globalInnovationCategories && innovationsMonthData && innovationsMonthData.length > 0) {
                globalInnovationCategories = {};
                globalProductToCategoryMap = new Map();
                innovationsMonthData.forEach(item => {
                    const categoryName = item.Inovacoes || item.inovacoes || item.INOVACOES;
                    if (!categoryName) return;
                    if (!globalInnovationCategories[categoryName]) {
                        globalInnovationCategories[categoryName] = { productCodes: new Set(), products: [] };
                    }
                    const productCode = String(item.Codigo || item.codigo || item.CODIGO).trim();
                    globalInnovationCategories[categoryName].productCodes.add(productCode);
                    globalInnovationCategories[categoryName].products.push({ ...item, Codigo: productCode, Inovacoes: categoryName });
                    globalProductToCategoryMap.set(productCode, categoryName);
                });
            }

            const categories = globalInnovationCategories || {};
            const currentFilterValue = innovationsMonthCategoryFilter.value;
            const allCategories = Object.keys(categories).sort();

            // Only update dropdown if empty or number of items changed significantly (simplistic check)
            if (innovationsMonthCategoryFilter.options.length <= 1 && allCategories.length > 0) {
                innovationsMonthCategoryFilter.innerHTML = '<option value="">Todas as Categorias</option>';
                allCategories.forEach(cat => {
                    innovationsMonthCategoryFilter.innerHTML += `<option value="${cat}">${cat}</option>`;
                });
                if (allCategories.includes(currentFilterValue)) {
                    innovationsMonthCategoryFilter.value = currentFilterValue;
                }
            }

            const { clients: filteredClients } = getInnovationsMonthFilteredData();

            const sellers = selectedInnovationsMonthSellers;
            const sellerRcaCodes = new Set();
            if (sellers.length > 0) {
                sellers.forEach(sellerName => {
                    const rcaCode = optimizedData.rcaCodeByName.get(sellerName);
                    if (rcaCode) sellerRcaCodes.add(rcaCode);
                });
            } else if (selectedInnovationsSupervisors.length > 0) {
                selectedInnovationsSupervisors.forEach(sup => {
                    (optimizedData.rcasBySupervisor.get(sup) || []).forEach(rca => sellerRcaCodes.add(rca));
                });
            }

            const activeClients = filteredClients.filter(c => {
                const codcli = c['Código'];
                const rca1 = String(c.rca1 || '').trim();
                if (rca1 === '306' || rca1 === '300') return false;
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(codcli));
            });
            const activeClientsCount = activeClients.length;
            const activeClientCodes = new Set(activeClients.map(c => c['Código']));

            // --- OPTIMIZED AGGREGATION LOGIC ---

            // Determine types to use
            const availableTypes = new Set([...allSalesData.map(s => s.TIPOVENDA), ...allHistoryData.map(s => s.TIPOVENDA)]);
            let currentSelection = selectedInnovationsMonthTiposVenda.length > 0 ? selectedInnovationsMonthTiposVenda : Array.from(availableTypes);
            const currentSelectionKey = currentSelection.slice().sort().join(',');

            // Caching Strategy: Reuse maps if Tipo Venda selection hasn't changed
            let mapsCurrent, mapsPrevious;
            if (viewState.inovacoes.lastTypesKey === currentSelectionKey && viewState.inovacoes.cache) {
                mapsCurrent = viewState.inovacoes.cache.mapsCurrent;
                mapsPrevious = viewState.inovacoes.cache.mapsPrevious;
            } else {
                const mainTypes = currentSelection.filter(t => t !== '5' && t !== '11');
                const bonusTypes = currentSelection.filter(t => t === '5' || t === '11');

                // Optimized Map Building (2 passes instead of 4)
                mapsCurrent = buildInnovationSalesMaps(allSalesData, mainTypes, bonusTypes);
                mapsPrevious = buildInnovationSalesMaps(allHistoryData, mainTypes, bonusTypes);

                viewState.inovacoes.lastTypesKey = currentSelectionKey;
                viewState.inovacoes.cache = { mapsCurrent, mapsPrevious };
            }

            // Structures to hold results
            // categoryResults[catName] = { current: Set<CodCli>, previous: Set<CodCli>, bonusCurrent: Set<CodCli>, bonusPrevious: Set<CodCli> }
            // productResults[prodCode] = { current: Set<CodCli> } -> For Top Item Logic
            const categoryResults = {};
            const productResults = {};

            for (const cat in categories) {
                categoryResults[cat] = {
                    current: new Set(),
                    previous: new Set(),
                    bonusCurrent: new Set(),
                    bonusPrevious: new Set()
                };
                categories[cat].productCodes.forEach(p => {
                    productResults[p] = { current: new Set(), previous: new Set() };
                });
            }

            // Helper to process maps and populate sets
            const processMap = (salesMap, isCurrent, isBonus) => {
                salesMap.forEach((productsMap, codCli) => {
                    // Only count if client is in the filtered active list
                    if (!activeClientCodes.has(codCli)) return;

                    productsMap.forEach((rcas, prodCode) => {
                        const category = globalProductToCategoryMap ? globalProductToCategoryMap.get(prodCode) : null;
                        if (!category) return; // Should not happen if innovation data is consistent

                        // Check RCA Filter
                        let soldBySelected = false;
                        if (sellerRcaCodes.size === 0) {
                            soldBySelected = true;
                        } else {
                            // Check intersection of rcas and sellerRcaCodes
                            for (const rca of rcas) {
                                if (sellerRcaCodes.has(rca)) {
                                    soldBySelected = true;
                                    break;
                                }
                            }
                        }

                        if (!soldBySelected) return;

                        const targetSetField = isCurrent ? 'current' : 'previous';

                        // Add to Category Set (Normal or Bonus)
                        if (categoryResults[category]) {
                            if (isBonus) {
                                categoryResults[category][isCurrent ? 'bonusCurrent' : 'bonusPrevious'].add(codCli);
                            } else {
                                categoryResults[category][targetSetField].add(codCli);
                            }
                        }

                        // Add to Product Set (For Top Item Logic)
                        if (productResults[prodCode]) {
                            productResults[prodCode][targetSetField].add(codCli);
                        }
                    });
                });
            };

            // Process all 4 maps efficiently (Looping over Sales, not all Clients)
            processMap(mapsCurrent.mainMap, true, false);
            processMap(mapsCurrent.bonusMap, true, true);
            processMap(mapsPrevious.mainMap, false, false);
            processMap(mapsPrevious.bonusMap, false, true);

            // Consolidate Results
            const categoryAnalysis = {};
            let topCoverageItem = { name: '-', coverage: 0, clients: 0 };

            // Consolidate Product Results for Top Item
            // We need to merge Main and Bonus for product coverage if needed,
            // but `processMap` populated `productResults` separately for main/bonus?
            // Wait, `processMap` handles Main and Bonus calls sequentially.
            // `productResults[prodCode].current` is a Set.
            // If we call processMap for Main, it adds clients.
            // If we call processMap for Bonus, it adds clients to SAME Set.
            // So `productResults` automatically handles the UNION of Main and Bonus coverage per product.

            if (selectedCategory && categories[selectedCategory]) {
                categories[selectedCategory].products.forEach(product => {
                    const pCode = String(product.Codigo).trim();
                    if (productResults[pCode]) {
                        const count = productResults[pCode].current.size;
                        const coverage = activeClientsCount > 0 ? (count / activeClientsCount) * 100 : 0;
                        if (coverage > topCoverageItem.coverage) {
                            topCoverageItem = { name: `(${pCode}) ${product.produto || product.Produto}`, coverage, clients: count };
                        }
                    }
                });
            } else {
                // Top Category Logic
                for (const cat in categoryResults) {
                    const set = categoryResults[cat].current; // Main coverage
                    // Wait, original logic: "gotItCurrent" if sales OR bonus.
                    // My categoryResults structure separates them. I need to union them for the metric?
                    // Original logic:
                    // clientsWhoGotAnyVisibleProductCurrent.add(codcli) if (Sales OR Bonus) of ANY product in category.

                    // Let's create a Union Set for the category coverage metric
                    const unionSet = new Set([...categoryResults[cat].current, ...categoryResults[cat].bonusCurrent]);
                    const count = unionSet.size;
                    const coverage = activeClientsCount > 0 ? (count / activeClientsCount) * 100 : 0;

                    if (coverage > topCoverageItem.coverage) {
                        topCoverageItem = { name: cat, coverage, clients: count };
                    }
                }
            }

            // Calculate Global KPIs (Union of all categories selected)
            const clientsWhoGotAnyVisibleProductCurrent = new Set();
            const clientsWhoGotAnyVisibleProductPrevious = new Set();
            const clientsWhoGotBonusAnyVisibleProductCurrent = new Set();
            const clientsWhoGotBonusAnyVisibleProductPrevious = new Set();

            for (const cat in categoryResults) {
                if (selectedCategory && cat !== selectedCategory) continue;

                // Merge sets into global KPI sets
                categoryResults[cat].current.forEach(c => clientsWhoGotAnyVisibleProductCurrent.add(c));
                categoryResults[cat].previous.forEach(c => clientsWhoGotAnyVisibleProductPrevious.add(c));
                categoryResults[cat].bonusCurrent.forEach(c => clientsWhoGotBonusAnyVisibleProductCurrent.add(c));
                categoryResults[cat].bonusPrevious.forEach(c => clientsWhoGotBonusAnyVisibleProductPrevious.add(c));

                // Prepare Analysis Object for Chart/Table
                const currentUnion = new Set([...categoryResults[cat].current, ...categoryResults[cat].bonusCurrent]);
                const previousUnion = new Set([...categoryResults[cat].previous, ...categoryResults[cat].bonusPrevious]);

                // NOTE: The chart/table in original code used "coverageCurrent" derived from
                // (clientsCurrentCount / activeClientsCount).
                // clientsCurrentCount was incremented if (Sales OR Bonus).

                const countCurr = currentUnion.size;
                const countPrev = previousUnion.size;

                const covCurr = activeClientsCount > 0 ? (countCurr / activeClientsCount) * 100 : 0;
                const covPrev = activeClientsCount > 0 ? (countPrev / activeClientsCount) * 100 : 0;
                const varPct = covPrev > 0 ? ((covCurr - covPrev) / covPrev) * 100 : (covCurr > 0 ? Infinity : 0);

                categoryAnalysis[cat] = {
                    coverageCurrent: covCurr,
                    coveragePrevious: covPrev,
                    variation: varPct,
                    clientsCount: countCurr,
                    clientsPreviousCount: countPrev
                };
            }

            // Total KPI calculations (Union of Sets)
            // Note: Original code did:
            // clientsWhoGotAnyVisibleProductCurrent -> Sales
            // clientsWhoGotBonusAnyVisibleProductCurrent -> Bonus
            // It kept them separate for the KPI cards at the top.
            // "Innovations Month Selection Coverage" -> Sales
            // "Innovations Month Bonus Coverage" -> Bonus

            const selectionCoveredCountCurrent = clientsWhoGotAnyVisibleProductCurrent.size;
            const selectionCoveragePercentCurrent = activeClientsCount > 0 ? (selectionCoveredCountCurrent / activeClientsCount) * 100 : 0;
            const selectionCoveredCountPrevious = clientsWhoGotAnyVisibleProductPrevious.size;
            const selectionCoveragePercentPrevious = activeClientsCount > 0 ? (selectionCoveredCountPrevious / activeClientsCount) * 100 : 0;

            const bonusCoveredCountCurrent = clientsWhoGotBonusAnyVisibleProductCurrent.size;
            const bonusCoveragePercentCurrent = activeClientsCount > 0 ? (bonusCoveredCountCurrent / activeClientsCount) * 100 : 0;
            const bonusCoveredCountPrevious = clientsWhoGotBonusAnyVisibleProductPrevious.size;
            const bonusCoveragePercentPrevious = activeClientsCount > 0 ? (bonusCoveredCountPrevious / activeClientsCount) * 100 : 0;

            // Update DOM
            innovationsMonthActiveClientsKpi.textContent = activeClientsCount.toLocaleString('pt-BR');
            innovationsMonthTopCoverageValueKpi.textContent = `${topCoverageItem.coverage.toFixed(2)}%`;
            innovationsMonthTopCoverageKpi.textContent = topCoverageItem.name;
            innovationsMonthTopCoverageKpi.title = topCoverageItem.name;
            innovationsMonthTopCoverageCountKpi.textContent = `${topCoverageItem.clients.toLocaleString('pt-BR')} PDVs`;
            document.getElementById('innovations-month-top-coverage-title').textContent = selectedCategory ? 'Produto com Maior Cobertura' : 'Categoria com Maior Cobertura';

            innovationsMonthSelectionCoverageValueKpi.textContent = `${selectionCoveragePercentCurrent.toFixed(2)}%`;
            innovationsMonthSelectionCoverageCountKpi.textContent = `${selectionCoveredCountCurrent.toLocaleString('pt-BR')} de ${activeClientsCount.toLocaleString('pt-BR')} clientes`;
            innovationsMonthSelectionCoverageValueKpiPrevious.textContent = `${selectionCoveragePercentPrevious.toFixed(2)}%`;
            innovationsMonthSelectionCoverageCountKpiPrevious.textContent = `${selectionCoveredCountPrevious.toLocaleString('pt-BR')} de ${activeClientsCount.toLocaleString('pt-BR')} clientes`;

            innovationsMonthBonusCoverageValueKpi.textContent = `${bonusCoveragePercentCurrent.toFixed(2)}%`;
            innovationsMonthBonusCoverageCountKpi.textContent = `${bonusCoveredCountCurrent.toLocaleString('pt-BR')} de ${activeClientsCount.toLocaleString('pt-BR')} clientes`;
            innovationsMonthBonusCoverageValueKpiPrevious.textContent = `${bonusCoveragePercentPrevious.toFixed(2)}%`;
            innovationsMonthBonusCoverageCountKpiPrevious.textContent = `${bonusCoveredCountPrevious.toLocaleString('pt-BR')} de ${activeClientsCount.toLocaleString('pt-BR')} clientes`;

            // Chart Update
            chartLabels = Object.keys(categoryAnalysis).sort((a,b) => categoryAnalysis[b].coverageCurrent - categoryAnalysis[a].coverageCurrent);
            const chartDataCurrent = chartLabels.map(cat => categoryAnalysis[cat].coverageCurrent);
            const chartDataPrevious = chartLabels.map(cat => categoryAnalysis[cat].coveragePrevious);

            if (chartLabels.length > 0) {
                createChart('innovations-month-chart', 'bar', chartLabels, [
                    { label: 'Mês Anterior', data: chartDataPrevious, backgroundColor: '#f97316' },
                    { label: 'Mês Atual', data: chartDataCurrent, backgroundColor: '#06b6d4' }
                ], {
                    plugins: {
                        legend: { display: true, position: 'top' },
                        datalabels: {
                            anchor: 'end',
                            align: 'top',
                            offset: 8,
                            formatter: (value) => value > 0 ? value.toFixed(1) + '%' : '',
                            color: '#cbd5e1',
                            font: { size: 10 }
                        },
                         tooltip: {
                            callbacks: {
                                label: function(context) {
                                    let label = context.dataset.label || '';
                                    if (label) label += ': ';
                                    if (context.parsed.y !== null) label += context.parsed.y.toFixed(2) + '%';
                                    return label;
                                }
                            }
                        }
                    },
                    scales: { y: { ticks: { callback: (v) => `${v}%` } } },
                    layout: { padding: { top: 20 } }
                });
            } else {
                showNoDataMessage('innovations-month-chart', 'Sem dados de inovações para exibir com os filtros atuais.');
            }

            // Table Update
            const tableData = [];
            const activeStockMap = getActiveStockMap(innovationsMonthFilialFilter.value);

            chartLabels.forEach(categoryName => {
                const categoryData = categories[categoryName];

                categoryData.products.forEach((product) => {
                    const productCode = product.Codigo;
                    const productName = product.produto || product.Produto;
                    const stock = activeStockMap.get(productCode) || 0;

                    // Re-calculate per product using the Product Results Sets
                    // Optimization: productResults[productCode] already contains sets of clients who bought
                    const pRes = productResults[productCode];

                    // IMPORTANT: productResults contains ALL clients who bought.
                    // We must filter by 'activeClientCodes' if not already done?
                    // 'processMap' already checked: `if (!activeClientCodes.has(codCli)) return;`
                    // So the sets in productResults are already filtered by active clients.

                    const clientsCurrentCount = pRes ? pRes.current.size : 0;
                    const clientsPreviousCount = pRes ? pRes.previous.size : 0;

                    const coverageCurrent = activeClientsCount > 0 ? (clientsCurrentCount / activeClientsCount) * 100 : 0;
                    const coveragePrevious = activeClientsCount > 0 ? (clientsPreviousCount / activeClientsCount) * 100 : 0;
                    const variation = coveragePrevious > 0 ? ((coverageCurrent - coveragePrevious) / coveragePrevious) * 100 : (coverageCurrent > 0 ? Infinity : 0);

                    tableData.push({
                        categoryName,
                        productCode,
                        productName,
                        stock,
                        coveragePrevious,
                        clientsPreviousCount,
                        coverageCurrent,
                        clientsCurrentCount,
                        variation
                    });
                });
            });

            tableData.sort((a,b) => b.coverageCurrent - a.coverageCurrent);
            innovationsMonthTableDataForExport = tableData;

            innovationsMonthTableBody.innerHTML = tableData.map(item => {
                let variationContent;
                if (isFinite(item.variation)) {
                    const colorClass = item.variation >= 0 ? 'text-green-400' : 'text-red-400';
                    variationContent = `<span class="${colorClass}">${item.variation.toFixed(1)}%</span>`;
                } else if (item.coverageCurrent > 0) {
                    variationContent = `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-purple-500/30 text-purple-300">Novo</span>`;
                } else {
                    variationContent = `<span>-</span>`;
                }

                return `
                    <tr class="hover:bg-slate-700/50">
                        <td class="px-2 py-1.5 text-xs">${item.categoryName}</td>
                        <td class="px-2 py-1.5 text-xs">${item.productCode} - ${item.productName}</td>
                        <td class="px-2 py-1.5 text-xs text-right">${item.stock.toLocaleString('pt-BR')}</td>
                        <td class="px-2 py-1.5 text-xs text-right">
                            <div class="tooltip">${item.coveragePrevious.toFixed(2)}%<span class="tooltip-text">${item.clientsPreviousCount} PDVs</span></div>
                        </td>
                        <td class="px-2 py-1.5 text-xs text-right">
                            <div class="tooltip">${item.coverageCurrent.toFixed(2)}%<span class="tooltip-text">${item.clientsCurrentCount} PDVs</span></div>
                        </td>
                        <td class="px-2 py-1.5 text-xs text-right">${variationContent}</td>
                    </tr>
                `;
            }).join('');

            // Innovations by Client Table
            const innovationsByClientTableHead = document.getElementById('innovations-by-client-table-head');
            const innovationsByClientTableBody = document.getElementById('innovations-by-client-table-body');
            const innovationsByClientLegend = document.getElementById('innovations-by-client-legend');

            categoryLegendForExport = chartLabels.map((name, index) => `${index + 1} - ${name}`);
            innovationsByClientLegend.innerHTML = `<strong>Legenda:</strong> ${categoryLegendForExport.join('; ')}`;

            let tableHeadHTML = `
                <tr>
                    <th class="px-2 py-2 text-left">Código</th>
                    <th class="px-2 py-2 text-left">Cliente</th>
                    <th class="px-2 py-2 text-left">Cidade</th>
                    <th class="px-2 py-2 text-left">Bairro</th>
                    <th class="px-2 py-2 text-center">Últ. Compra</th>
            `;
            chartLabels.forEach((name, index) => {
                tableHeadHTML += `<th class="px-2 py-2 text-center">${index + 1}</th>`;
            });
            tableHeadHTML += `</tr>`;
            innovationsByClientTableHead.innerHTML = tableHeadHTML;

            // Build Client Status List
            // Optimized: Iterate Active Clients and check sets in categoryResults
            const clientInnovationStatus = activeClients.map(client => {
                const codcli = client['Código'];
                const status = {};

                chartLabels.forEach(catName => {
                    // Check if client exists in Main OR Bonus sets for this category
                    const inMain = categoryResults[catName].current.has(codcli);
                    const inBonus = categoryResults[catName].bonusCurrent.has(codcli);
                    status[catName] = inMain || inBonus;
                });

                // Explicit copy for robustness against Proxies
                return {
                    'Código': client['Código'],
                    fantasia: client.fantasia,
                    razaoSocial: client.razaoSocial,
                    cidade: client.cidade,
                    bairro: client.bairro,
                    ultimaCompra: client.ultimaCompra,
                    innovationStatus: status
                };
            });

            clientInnovationStatus.sort((a, b) => {
                const cidadeA = a.cidade || '';
                const cidadeB = b.cidade || '';
                const bairroA = a.bairro || '';
                const bairroB = b.bairro || '';
                if (cidadeA.localeCompare(cidadeB) !== 0) return cidadeA.localeCompare(cidadeB);
                return bairroA.localeCompare(bairroB);
            });

            innovationsByClientForExport = clientInnovationStatus;

            // Pagination for Innovations Client Table
            const itemsPerPage = 100;
            const totalPages = Math.ceil(clientInnovationStatus.length / itemsPerPage);
            let currentPage = 1;

            const renderInnovationsPage = (page) => {
                const startIndex = (page - 1) * itemsPerPage;
                const endIndex = startIndex + itemsPerPage;
                const pageData = clientInnovationStatus.slice(startIndex, endIndex);

                const checkIcon = `<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-green-400 mx-auto" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd" /></svg>`;
                const xIcon = `<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-red-400 mx-auto" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clip-rule="evenodd" /></svg>`;

                let tableBodyHTML = '';
                pageData.forEach(client => {
                    tableBodyHTML += `
                        <tr class="hover:bg-slate-700/50">
                            <td class="px-2 py-1.5 text-xs">${client['Código']}</td>
                            <td class="px-2 py-1.5 text-xs">${client.fantasia || client.razaoSocial}</td>
                            <td class="px-2 py-1.5 text-xs">${client.cidade}</td>
                            <td class="px-2 py-1.5 text-xs">${client.bairro}</td>
                            <td class="px-2 py-1.5 text-xs text-center">${formatDate(client.ultimaCompra)}</td>
                    `;
                    chartLabels.forEach(catName => {
                        tableBodyHTML += `<td class="px-2 py-1.5 text-center">${client.innovationStatus[catName] ? checkIcon : xIcon}</td>`;
                    });
                    tableBodyHTML += `</tr>`;
                });
                innovationsByClientTableBody.innerHTML = tableBodyHTML;

                // Update Pagination Controls
                const prevBtn = document.getElementById('innovations-prev-page-btn');
                const nextBtn = document.getElementById('innovations-next-page-btn');
                const infoText = document.getElementById('innovations-page-info-text');
                const controls = document.getElementById('innovations-pagination-controls');

                if (controls) {
                    if (totalPages > 1) {
                        controls.classList.remove('hidden');
                        infoText.textContent = `Página ${page} de ${totalPages} (${clientInnovationStatus.length} clientes)`;
                        prevBtn.disabled = page === 1;
                        nextBtn.disabled = page === totalPages;

                        // Clone and replace buttons to remove old event listeners
                        const newPrevBtn = prevBtn.cloneNode(true);
                        const newNextBtn = nextBtn.cloneNode(true);
                        prevBtn.parentNode.replaceChild(newPrevBtn, prevBtn);
                        nextBtn.parentNode.replaceChild(newNextBtn, nextBtn);

                        newPrevBtn.addEventListener('click', () => {
                            if (currentPage > 1) {
                                currentPage--;
                                renderInnovationsPage(currentPage);
                            }
                        });
                        newNextBtn.addEventListener('click', () => {
                            if (currentPage < totalPages) {
                                currentPage++;
                                renderInnovationsPage(currentPage);
                            }
                        });

                    } else {
                        controls.classList.add('hidden');
                    }
                }
            };

            renderInnovationsPage(1);
        }

        async function exportInnovationsMonthPDF() {
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF('landscape');

            const supervisor = document.getElementById('innovations-month-supervisor-filter-text').textContent;
            const vendedor = document.getElementById('innovations-month-vendedor-filter-text').textContent;
            const filial = innovationsMonthFilialFilter.options[innovationsMonthFilialFilter.selectedIndex].text;
            const cidade = innovationsMonthCityFilter.value.trim();
            const categoria = innovationsMonthCategoryFilter.value || 'Todas';
            const generationDate = new Date().toLocaleString('pt-BR');

            doc.setFontSize(18);
            doc.text('Relatório de Inovações do Mês', 14, 22);
            doc.setFontSize(10);
            doc.setTextColor(10);
            doc.text(`Data de Emissão: ${generationDate}`, 14, 30);

            let filterText = `Filtros Aplicados: Supervisor: ${supervisor} | Vendedor: ${vendedor} | Filial: ${filial} | Cidade: ${cidade || 'Todas'} | Categoria: ${categoria}`;
            const splitFilters = doc.splitTextToSize(filterText, 270);
            doc.text(splitFilters, 14, 36);

            const chartCanvas = document.getElementById('innovations-month-chart');
            if (chartCanvas && charts['innovations-month-chart'] && chartLabels.length > 0) {
                try {
                    const chartInstance = charts['innovations-month-chart'];
                    const originalDatalabelsColor = chartInstance.options.plugins.datalabels.color;
                    const originalXColor = chartInstance.options.scales.x.ticks.color;
                    const originalYColor = chartInstance.options.scales.y.ticks.color;
                    const originalLegendColor = chartInstance.options.plugins.legend?.labels?.color;

                    chartInstance.options.plugins.datalabels.color = '#000000';
                    chartInstance.options.scales.x.ticks.color = '#000000';
                    chartInstance.options.scales.y.ticks.color = '#000000';
                    if (chartInstance.options.plugins.legend && chartInstance.options.plugins.legend.labels) {
                        chartInstance.options.plugins.legend.labels.color = '#000000';
                    }

                    chartInstance.update('none');

                    const chartImage = chartCanvas.toDataURL('image/png', 1.0);
                    doc.addImage(chartImage, 'PNG', 14, 50, 270, 100);

                    chartInstance.options.plugins.datalabels.color = originalDatalabelsColor;
                    chartInstance.options.scales.x.ticks.color = originalXColor;
                    chartInstance.options.scales.y.ticks.color = originalYColor;
                    if (chartInstance.options.plugins.legend && chartInstance.options.plugins.legend.labels) {
                        chartInstance.options.plugins.legend.labels.color = originalLegendColor;
                    }
                    chartInstance.update('none');
                } catch (e) {
                    console.error("Erro ao converter o gráfico para imagem:", e);
                    doc.text("Erro ao gerar a imagem do gráfico.", 14, 50);
                }
            } else {
                 doc.text("Gráfico não disponível para os filtros selecionados.", 14, 50);
            }

            const head = [['Categoria', 'Produto', 'Estoque', 'Cob. Mês Ant.', 'Cob. Mês Atual', 'Variação']];
            const body = [];

            innovationsMonthTableDataForExport.forEach(item => {
                let variationContent;
                if (isFinite(item.variation)) {
                    variationContent = `${item.variation.toFixed(1)}%`;
                } else if (item.coverageCurrent > 0) {
                    variationContent = 'Novo';
                } else {
                    variationContent = '-';
                }

                const row = [
                    item.categoryName,
                    `${item.productCode} - ${item.productName}`,
                    item.stock.toLocaleString('pt-BR'),
                    `${item.coveragePrevious.toFixed(2)}% (${item.clientsPreviousCount} PDVs)`,
                    `${item.coverageCurrent.toFixed(2)}% (${item.clientsCurrentCount} PDVs)`,
                    variationContent
                ];
                body.push(row);
            });

            doc.autoTable({
                head: head,
                body: body,
                startY: 155,
                theme: 'grid',
                styles: { fontSize: 7, cellPadding: 1.5, textColor: [0, 0, 0] },
                headStyles: { fillColor: [41, 128, 185], textColor: 255, fontSize: 7, fontStyle: 'bold' },
                alternateRowStyles: { fillColor: [240, 248, 255] },
                didDrawPage: function (data) {
                    doc.setFontSize(12);
                    doc.setTextColor(10);
                    doc.text("Dados do Gráfico", data.settings.margin.left, 152);
                }
            });

            if (innovationsByClientForExport.length > 0) {
                doc.addPage();
                doc.setFontSize(18);
                doc.text('Relatório de Inovações por Cliente', 14, 22);
                doc.setFontSize(10);
                doc.setTextColor(10);
                doc.text(`Data de Emissão: ${generationDate}`, 14, 30);
                doc.text(splitFilters, 14, 36);

                const legendText = `Legenda: ${categoryLegendForExport.join('; ')}`;
                const splitLegend = doc.splitTextToSize(legendText, 270);
                doc.text(splitLegend, 14, 42);

                const clientInnovationsHead = [['Código', 'Cliente', 'Cidade', 'Bairro', 'Últ. Compra', ...categoryLegendForExport.map((_, i) => `${i + 1}`)]];
                const clientInnovationsBody = innovationsByClientForExport.map(client => {
                    const row = [
                        client['Código'],
                        client.fantasia || client.razaoSocial,
                        client.cidade,
                        client.bairro,
                        formatDate(client.ultimaCompra)
                    ];
                    categoryLegendForExport.forEach((cat, index) => {
                        const catName = cat.split(' - ')[1];
                        const status = client.innovationStatus[catName];

                        const cell = {
                            content: status ? 'S' : 'N',
                            styles: {
                                textColor: status ? [34, 139, 34] : [220, 20, 60],
                                fontStyle: 'bold'
                            }
                        };
                        row.push(cell);
                    });
                    return row;
                });

                doc.autoTable({
                    head: clientInnovationsHead,
                    body: clientInnovationsBody,
                    startY: 55,
                    theme: 'grid',
                    styles: { fontSize: 7, cellPadding: 1.5, halign: 'center', textColor: [0, 0, 0] },
                    headStyles: { fillColor: [41, 128, 185], textColor: 255, fontSize: 7, fontStyle: 'bold' },
                    columnStyles: {
                        0: { halign: 'left' },
                        1: { halign: 'left' },
                        2: { halign: 'left' },
                        3: { halign: 'left' },
                        4: { halign: 'center' }
                    },
                    alternateRowStyles: { fillColor: [240, 248, 255] },
                });
            }

            const pageCount = doc.internal.getNumberOfPages();
            for(let i = 1; i <= pageCount; i++) {
                doc.setPage(i);
                doc.setFontSize(9);
                doc.setTextColor(10);
                doc.text(`Página ${i} de ${pageCount}`, doc.internal.pageSize.width / 2, doc.internal.pageSize.height - 10, { align: 'center' });
            }

            let fileNameParam = 'geral';
            if (selectedInnovationsMonthSellers.length === 1) {
                fileNameParam = getFirstName(selectedInnovationsMonthSellers[0]);
            } else if (cidade) {
                fileNameParam = cidade;
            }
            const safeFileNameParam = fileNameParam.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            doc.save(`relatorio_inovacoes_mes_${safeFileNameParam}_${new Date().toISOString().slice(0,10)}.pdf`);
        }


        async function exportCoveragePDF() {
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF('landscape');

            const supervisor = document.getElementById('coverage-supervisor-filter-text').textContent;
            const vendedor = document.getElementById('coverage-vendedor-filter-text').textContent;
            const filial = coverageFilialFilter.options[coverageFilialFilter.selectedIndex].text;
            const cidade = coverageCityFilter.value.trim();
            const supplierText = document.getElementById('coverage-supplier-filter-text').textContent;
            const generationDate = new Date().toLocaleString('pt-BR');

            doc.setFontSize(18);
            doc.text('Relatório de Cobertura (Estoque x PDVs)', 14, 22);
            doc.setFontSize(10);
            doc.setTextColor(10);
            doc.text(`Data de Emissão: ${generationDate}`, 14, 30);

            let filterText = `Filtros Aplicados: Supervisor: ${supervisor} | Vendedor: ${vendedor} | Filial: ${filial} | Cidade: ${cidade || 'Todas'} | Fornecedor: ${supplierText}`;
            const splitFilters = doc.splitTextToSize(filterText, 270);
            doc.text(splitFilters, 14, 36);

            // Add Chart if available
            const chartId = currentCoverageChartMode === 'city' ? 'coverageCityChart' : 'coverageSellerChart';
            const chartCanvas = document.getElementById(chartId);
            if (chartCanvas && charts[chartId]) {
                try {
                    const chartInstance = charts[chartId];
                    const originalDatalabelsColor = chartInstance.options.plugins.datalabels.color;
                    const originalXColor = chartInstance.options.scales.x.ticks.color;
                    const originalYColor = chartInstance.options.scales.y.ticks.color;
                    const originalLegendColor = chartInstance.options.plugins.legend?.labels?.color;

                    chartInstance.options.plugins.datalabels.color = '#000000';
                    chartInstance.options.scales.x.ticks.color = '#000000';
                    chartInstance.options.scales.y.ticks.color = '#000000';
                    if (chartInstance.options.plugins.legend && chartInstance.options.plugins.legend.labels) {
                        chartInstance.options.plugins.legend.labels.color = '#000000';
                    }

                    chartInstance.update('none');

                    const chartImage = chartCanvas.toDataURL('image/png', 1.0);
                    doc.addImage(chartImage, 'PNG', 14, 50, 270, 80);

                    chartInstance.options.plugins.datalabels.color = originalDatalabelsColor;
                    chartInstance.options.scales.x.ticks.color = originalXColor;
                    chartInstance.options.scales.y.ticks.color = originalYColor;
                    if (chartInstance.options.plugins.legend && chartInstance.options.plugins.legend.labels) {
                        chartInstance.options.plugins.legend.labels.color = originalLegendColor;
                    }
                    chartInstance.update('none');
                } catch (e) {
                    console.error("Erro ao converter o gráfico para imagem:", e);
                }
            }

            const head = [['Produto', 'Estoque (Cx)', 'Cx. Mês Ant.', 'Cx. Mês Atual', 'Caixas (%)', 'PDVs Ant.', 'PDVs Atual', 'Cobertura (%)']];
            const body = [];

            coverageTableDataForExport.forEach(item => {
                let boxesVariationContent;
                if (isFinite(item.boxesVariation)) {
                    boxesVariationContent = `${item.boxesVariation.toFixed(1)}%`;
                } else if (item.boxesVariation === Infinity) {
                    boxesVariationContent = 'Novo';
                } else {
                    boxesVariationContent = '-';
                }

                let pdvVariationContent;
                if (isFinite(item.pdvVariation)) {
                    pdvVariationContent = `${item.pdvVariation.toFixed(1)}%`;
                } else if (item.pdvVariation === Infinity) {
                    pdvVariationContent = 'Novo';
                } else {
                    pdvVariationContent = '-';
                }

                const row = [
                    item.descricao,
                    item.stockQty.toLocaleString('pt-BR'),
                    item.boxesSoldPreviousMonth.toLocaleString('pt-BR', {maximumFractionDigits: 2}),
                    item.boxesSoldCurrentMonth.toLocaleString('pt-BR', {maximumFractionDigits: 2}),
                    boxesVariationContent,
                    item.clientsPreviousCount.toLocaleString('pt-BR'),
                    item.clientsCurrentCount.toLocaleString('pt-BR'),
                    pdvVariationContent
                ];
                body.push(row);
            });

            doc.autoTable({
                head: head,
                body: body,
                startY: 140,
                theme: 'grid',
                styles: { fontSize: 8, cellPadding: 1.5, textColor: [0, 0, 0] },
                headStyles: { fillColor: [41, 128, 185], textColor: 255, fontSize: 8, fontStyle: 'bold' },
                alternateRowStyles: { fillColor: [240, 248, 255] },
                 didDrawPage: function (data) {
                    // Footer or Header on new pages
                }
            });

            const pageCount = doc.internal.getNumberOfPages();
            for(let i = 1; i <= pageCount; i++) {
                doc.setPage(i);
                doc.setFontSize(9);
                doc.setTextColor(10);
                doc.text(`Página ${i} de ${pageCount}`, doc.internal.pageSize.width / 2, doc.internal.pageSize.height - 10, { align: 'center' });
            }

            let fileNameParam = 'geral';
            if (selectedCoverageSellers.length === 1) {
                fileNameParam = getFirstName(selectedCoverageSellers[0]);
            } else if (cidade) {
                fileNameParam = cidade;
            }
            const safeFileNameParam = fileNameParam.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            doc.save(`relatorio_cobertura_${safeFileNameParam}_${new Date().toISOString().slice(0,10)}.pdf`);
        }


        function openModal(pedidoId) {
            const orderInfo = aggregatedOrders.find(order => order.PEDIDO == pedidoId);
            const itemsDoPedido = allSalesData.filter(item => item.PEDIDO == pedidoId);
            if (!orderInfo) return;
            modalPedidoId.textContent = pedidoId;
            modalHeaderInfo.innerHTML = `<div><p class="font-bold">Cód. Cliente:</p><p>${orderInfo.CODCLI || 'N/A'}</p></div><div><p class="font-bold">Cliente:</p><p>${orderInfo.CLIENTE_NOME || 'N/A'}</p></div><div><p class="font-bold">Vendedor:</p><p>${orderInfo.NOME || 'N/A'}</p></div><div><p class="font-bold">Data Pedido:</p><p>${formatDate(orderInfo.DTPED)}</p></div><div><p class="font-bold">Data Faturamento:</p><p>${formatDate(orderInfo.DTSAIDA)}</p></div><div><p class="font-bold">Cidade:</p><p>${orderInfo.CIDADE || 'N/A'}</p></div>`;
            modalTableBody.innerHTML = itemsDoPedido.map(item => { const unitPrice = (item.QTVENDA > 0) ? (item.VLVENDA / item.QTVENDA) : 0; return `<tr class="hover:bg-slate-700"><td class="px-4 py-2">(${item.PRODUTO}) ${item.DESCRICAO}</td><td class="px-4 py-2 text-right">${item.QTVENDA}</td><td class="px-4 py-2 text-right">${item.TOTPESOLIQ.toLocaleString('pt-BR', { minimumFractionDigits: 2 })} Kg</td><td class="px-4 py-2 text-right"><div class="tooltip">${unitPrice.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}<span class="tooltip-text" style="width: max-content; left: auto; right: 0; transform: none; margin-left: 0;">Subtotal: ${item.VLVENDA.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</span></div></td></tr>`; }).join('');
            modalFooterTotal.innerHTML = `<p class="text-lg font-bold text-teal-400">Mix de Produtos: ${itemsDoPedido.length}</p><p class="text-lg font-bold text-emerald-400">Total do Pedido: ${orderInfo.VLVENDA.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</p>`;
            modal.classList.remove('hidden');
        }

        function openClientModal(codcli) {
            const clientData = allClientsData.find(c => String(c['Código']) === String(codcli));
            if (!clientData) return;

            const getVal = (obj, keys) => {
                for (const k of keys) {
                    if (obj[k] !== undefined && obj[k] !== null && obj[k] !== 'N/A' && obj[k] !== '') return obj[k];
                }
                return undefined;
            };

            const endereco = getVal(clientData, ['Endereço Comercial', 'endereco', 'ENDERECO']) || 'N/A';
            const numero = getVal(clientData, ['NUMERO', 'numero', 'Número']) || 'SN';
            let finalAddress = endereco;
            if (numero !== 'SN' && finalAddress !== 'N/A' && !finalAddress.includes(numero)) finalAddress += `, ${numero}`;

            const cnpj = getVal(clientData, ['CNPJ/CPF', 'cnpj_cpf']) || 'N/A';
            const insc = getVal(clientData, ['Insc. Est. / Produtor', 'inscricaoEstadual', 'INSCRICAOESTADUAL']) || 'N/A';
            const razao = getVal(clientData, ['Cliente', 'razaoSocial', 'nomeCliente', 'RAZAOSOCIAL', 'NOMECLIENTE']) || 'N/A';
            const fantasia = getVal(clientData, ['FANTASIA', 'Fantasia', 'fantasia']) || 'N/A';
            const bairro = getVal(clientData, ['BAIRRO', 'Bairro', 'bairro']) || 'N/A';
            const cidade = getVal(clientData, ['CIDADE', 'Cidade', 'cidade', 'Nome da Cidade']) || 'N/A';
            const cep = getVal(clientData, ['CEP', 'cep']) || 'N/A';
            const telefone = getVal(clientData, ['Telefone Comercial', 'telefone', 'TELEFONE']) || 'N/A';
            const email = getVal(clientData, ['EMAIL', 'email', 'E-mail']) || 'N/A';
            const ramo = getVal(clientData, ['Descricao', 'ramo', 'DESCRICAO', 'Descricao']) || 'N/A';
            const ultimaCompra = getVal(clientData, ['Data da Última Compra', 'ultimaCompra', 'ULTIMACOMPRA']);

            clientModalContent.innerHTML = `<div class="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-4 text-sm"><div><p class="font-bold text-slate-400">Código:</p><p>${clientData['Código'] || 'N/A'}</p></div><div><p class="font-bold text-slate-400">CNPJ/CPF:</p><p>${cnpj}</p></div><div class="md:col-span-2"><p class="font-bold text-slate-400">Insc. Est. / Produtor:</p><p>${insc}</p></div><div class="md:col-span-2"><p class="font-bold text-slate-400">Razão Social:</p><p>${razao}</p></div><div class="md:col-span-2"><p class="font-bold text-slate-400">Nome Fantasia:</p><p>${fantasia}</p></div><div class="md:col-span-2"><p class="font-bold text-slate-400">Endereço:</p><p>${finalAddress}</p></div><div><p class="font-bold text-slate-400">Bairro:</p><p>${bairro}</p></div><div><p class="font-bold text-slate-400">Cidade:</p><p>${cidade}</p></div><div><p class="font-bold text-slate-400">CEP:</p><p>${cep}</p></div><div><p class="font-bold text-slate-400">Telefone:</p><p>${telefone}</p></div><div class="md:col-span-2"><p class="font-bold text-slate-400">E-mail:</p><p>${email}</p></div><div><p class="font-bold text-slate-400">Ramo de Atividade:</p><p>${ramo}</p></div><div><p class="font-bold text-slate-400">Última Compra:</p><p>${formatDate(ultimaCompra)}</p></div></div>`;
            clientModal.classList.remove('hidden');
        }

        function exportClientsPDF(clientList, title, filename, includeFaturamento) {
             if (clientList.length === 0) return;
            const { jsPDF } = window.jspdf; const doc = new jsPDF();
            const supervisor = document.getElementById('city-supervisor-filter-text').textContent;
            const vendedor = document.getElementById('city-vendedor-filter-text').textContent;
            const city = cityNameFilter.value.trim();
            const generationDate = new Date().toLocaleString('pt-BR'); const today = new Date();
            const firstDay = new Date(today.getFullYear(), today.getMonth(), 1).toLocaleDateString('pt-BR');
            const lastDay = new Date(today.getFullYear(), today.getMonth() + 1, 0).toLocaleDateString('pt-BR');
            doc.setFontSize(18); doc.text(title, 14, 22); doc.setFontSize(11); doc.setTextColor(10);
            doc.text(`Período de Análise: ${firstDay} a ${lastDay}`, 14, 32);
            doc.text(`Supervisor: ${supervisor}`, 14, 38); doc.text(`Vendedor: ${vendedor}`, 14, 44);
            doc.text(`Cidade: ${city || 'Todas'}`, 14, 50);
            doc.text(`Data de Emissão: ${generationDate}`, 14, 56);
            const tableColumn = ["Código", "Cliente", "Bairro", "Cidade", "Últ. Compra"];
            if (includeFaturamento) tableColumn.splice(2, 0, "Faturamento");

            clientList.sort((a, b) => {
                if (includeFaturamento) {
                    const valA = a.total || 0;
                    const valB = b.total || 0;
                    if (valB !== valA) return valB - valA;
                }

                const cidadeA = a.cidade || a.CIDADE || a['Nome da Cidade'] || '';
                const cidadeB = b.cidade || b.CIDADE || b['Nome da Cidade'] || '';
                const bairroA = a.bairro || a.BAIRRO || '';
                const bairroB = b.bairro || b.BAIRRO || '';
                if (cidadeA < cidadeB) return -1;
                if (cidadeA > cidadeB) return 1;
                if (bairroA < bairroB) return -1;
                if (bairroA > bairroB) return 1;
                return 0;
            });

            const tableRows = [];
            let totalFaturamento = 0;

            clientList.forEach(client => {
                const fantasia = client.fantasia || client.FANTASIA || client.Fantasia || '';
                const razao = client.razaoSocial || client.Cliente || client.RAZAOSOCIAL || '';
                const nome = fantasia || razao;
                const bairro = client.bairro || client.BAIRRO || client.Bairro || '';
                const cidade = client.cidade || client.CIDADE || client.Cidade || client['Nome da Cidade'] || '';
                const ultCompra = client.ultimaCompra || client['Data da Última Compra'] || client.ULTIMACOMPRA;

                const clientData = [ client['Código'] || '', nome, bairro, cidade, formatDate(ultCompra) || 'N/A' ];
                if (includeFaturamento) {
                    const val = client.total || 0;
                    totalFaturamento += val;
                    clientData.splice(2, 0, val.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }));
                }
                tableRows.push(clientData);
            });

            if (includeFaturamento) {
                const footerRow = [
                    { content: 'TOTAL:', colSpan: 2, styles: { halign: 'right', fontStyle: 'bold', fontSize: 10, fillColor: [50, 50, 50], textColor: [255, 255, 255] } },
                    { content: totalFaturamento.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }), styles: { fontStyle: 'bold', fontSize: 10, fillColor: [50, 50, 50], textColor: [50, 255, 100] } },
                    { content: '', colSpan: 3, styles: { fillColor: [50, 50, 50] } }
                ];
                tableRows.push(footerRow);
            } else {
                const footerRow = [
                    { content: 'TOTAL CLIENTES:', colSpan: 2, styles: { halign: 'right', fontStyle: 'bold', fontSize: 10, fillColor: [50, 50, 50], textColor: [255, 255, 255] } },
                    { content: String(clientList.length), colSpan: 3, styles: { fontStyle: 'bold', fontSize: 10, fillColor: [50, 50, 50], textColor: [255, 255, 255] } }
                ];
                tableRows.push(footerRow);
            }

            doc.autoTable({ head: [tableColumn], body: tableRows, startY: 60, theme: 'grid', styles: { fontSize: 8, cellPadding: 1.5, textColor: [0, 0, 0] }, headStyles: { fillColor: [41, 128, 185], textColor: 255, fontSize: 8, fontStyle: 'bold' }, alternateRowStyles: { fillColor: [240, 248, 255] }, margin: { top: 10 } });
            const pageCount = doc.internal.getNumberOfPages();
            for(let i = 1; i <= pageCount; i++) { doc.setPage(i); doc.setFontSize(9); doc.setTextColor(10); doc.text(`Página ${i} de ${pageCount}`, doc.internal.pageSize.width / 2, doc.internal.pageSize.height - 10, { align: 'center' }); }

            let fileNameParam = 'geral';
            if (selectedCitySellers.length === 1) {
                fileNameParam = getFirstName(selectedCitySellers[0]);
            } else if (city) {
                fileNameParam = city;
            }
            const safeFileNameParam = fileNameParam.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            doc.save(`${filename}_${safeFileNameParam}_${new Date().toISOString().slice(0,10)}.pdf`);
        }

        function renderCalendar(year, month) {
            const monthNames = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"];

            let calendarHTML = `
                <div class="flex justify-between items-center mb-4">
                    <button id="prev-month-btn" class="p-2 rounded-full hover:bg-slate-600">&lt;</button>
                    <h3 class="font-bold text-lg">${monthNames[month]} ${year}</h3>
                    <button id="next-month-btn" class="p-2 rounded-full hover:bg-slate-600">&gt;</button>
                </div>
                <div class="grid grid-cols-7 gap-1 text-center text-xs text-slate-400 mb-2">
                    <div>Dom</div><div>Seg</div><div>Ter</div><div>Qua</div><div>Qui</div><div>Sex</div><div>Sáb</div>
                </div>
                <div id="calendar-grid" class="grid grid-cols-7 gap-1">
            `;
            const firstDay = new Date(Date.UTC(year, month, 1)).getUTCDay();
            const daysInMonth = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();

            for (let i = 0; i < firstDay; i++) {
                calendarHTML += `<div></div>`;
            }

            for (let day = 1; day <= daysInMonth; day++) {
                const date = new Date(Date.UTC(year, month, day));
                const dateString = date.toISOString().split('T')[0];
                const isSelected = selectedHolidays.includes(dateString);
                const isToday = date.getTime() === lastSaleDate.getTime();
                let dayClasses = 'p-2 rounded-full cursor-pointer hover:bg-slate-600 flex items-center justify-center';
                if (isSelected) dayClasses += ' bg-red-500 text-white font-bold';
                if (isToday) dayClasses += ' border-2 border-teal-400';

                calendarHTML += `<div class="${dayClasses}" data-date="${dateString}">${day}</div>`;
            }

            calendarHTML += `</div>`;
            calendarContainer.innerHTML = calendarHTML;
        }

        function initializeRedeFilters() {
            const hasRedeData = allClientsData.some(client => client.ramo && client.ramo !== 'N/A');

            const mainRedeFilterWrapper = document.getElementById('main-rede-filter-wrapper');
            const cityRedeFilterWrapper = document.getElementById('city-rede-filter-wrapper');
            const comparisonRedeFilterWrapper = document.getElementById('comparison-rede-filter-wrapper');
            const stockRedeFilterWrapper = document.getElementById('stock-rede-filter-wrapper');

            if (mainRedeFilterWrapper) mainRedeFilterWrapper.style.display = hasRedeData ? '' : 'none';
            if (cityRedeFilterWrapper) cityRedeFilterWrapper.style.display = hasRedeData ? '' : 'none';
            if (comparisonRedeFilterWrapper) comparisonRedeFilterWrapper.style.display = hasRedeData ? '' : 'none';
            if (stockRedeFilterWrapper) stockRedeFilterWrapper.style.display = hasRedeData ? '' : 'none';
        }

        function debounce(func, delay = 300) {
            let timeout;
            return function(...args) {
                clearTimeout(timeout);
                timeout = setTimeout(() => {
                    func.apply(this, args);
                }, delay);
            };
        }

        function showLoader(text = 'Carregando...') {
            return new Promise(resolve => {
                const loader = document.getElementById('page-transition-loader');
                const loaderText = document.getElementById('loader-text');
                if (loader && loaderText) {
                    document.body.setAttribute('data-loading', 'true');
                    loaderText.textContent = text;
                    loader.classList.remove('opacity-0', 'pointer-events-none');
                    setTimeout(resolve, 50);
                } else {
                    resolve();
                }
            });
        }

        function hideLoader() {
             return new Promise(resolve => {
                const loader = document.getElementById('page-transition-loader');
                if (loader) {
                    document.body.removeAttribute('data-loading');
                    loader.classList.add('opacity-0', 'pointer-events-none');
                    setTimeout(resolve, 300);
                } else {
                    resolve();
                }
            });
        }

        function toggleSidebar() {
            const sideMenu = document.getElementById('side-menu');
            const sidebarOverlay = document.getElementById('sidebar-overlay');

            if (sideMenu) {
                sideMenu.classList.toggle('-translate-x-full');
                if (sidebarOverlay) {
                    if (sideMenu.classList.contains('-translate-x-full')) {
                        sidebarOverlay.classList.add('hidden');
                    } else {
                        sidebarOverlay.classList.remove('hidden');
                    }
                }
            }
        }

        async function navigateTo(view) {
            const sideMenu = document.getElementById('side-menu');
            const sidebarOverlay = document.getElementById('sidebar-overlay');

            if (sideMenu) sideMenu.classList.add('-translate-x-full');
            if (sidebarOverlay) sidebarOverlay.classList.add('hidden');

            const viewNameMap = {
                dashboard: 'Dashboard',
                pedidos: 'Pedidos',
                comparativo: 'Comparativo',
                estoque: 'Estoque',
                cobertura: 'Cobertura',
                cidades: 'Cidades',
                semanal: 'Semanal',
                'inovacoes-mes': 'Inovações',
                mix: 'Mix'
            };
            const friendlyName = viewNameMap[view] || 'a página';

            await showLoader(`Carregando ${friendlyName}...`);

            // This function now runs after the loader is visible
            const updateContent = () => {
                [mainDashboard, cityView, weeklyView, comparisonView, stockView, innovationsMonthView, coverageView, document.getElementById('mix-view'), goalsView].forEach(el => {
                    if(el) el.classList.add('hidden');
                });

                document.querySelectorAll('.nav-link').forEach(link => {
                    link.classList.remove('bg-slate-700', 'text-white');
                    const icon = link.querySelector('svg');
                    if(icon) icon.classList.remove('text-white');
                });

                const activeLink = document.querySelector(`.nav-link[data-target="${view}"]`);
                if (activeLink) {
                    activeLink.classList.add('bg-slate-700', 'text-white');
                    const icon = activeLink.querySelector('svg');
                    if(icon) icon.classList.add('text-white');
                }

                switch(view) {
                    case 'dashboard':
                        mainDashboard.classList.remove('hidden');
                        chartView.classList.remove('hidden');
                        tableView.classList.add('hidden');
                        tablePaginationControls.classList.add('hidden');
                        if (viewState.dashboard.dirty) {
                            updateAllVisuals();
                            viewState.dashboard.dirty = false;
                        }
                        break;
                    case 'pedidos':
                        mainDashboard.classList.remove('hidden');
                        chartView.classList.add('hidden');
                        tableView.classList.remove('hidden');
                        tablePaginationControls.classList.remove('hidden');
                        if (viewState.pedidos.dirty) {
                            updateAllVisuals();
                            viewState.pedidos.dirty = false;
                        }
                        break;
                    case 'comparativo':
                        comparisonView.classList.remove('hidden');
                        if (viewState.comparativo.dirty) {
                            updateAllComparisonFilters();
                            updateComparisonView();
                            viewState.comparativo.dirty = false;
                        }
                        break;
                    case 'estoque':
                        stockView.classList.remove('hidden');
                        if (viewState.estoque.dirty) {
                            handleStockFilterChange();
                            viewState.estoque.dirty = false;
                        }
                        break;
                    case 'cobertura':
                        coverageView.classList.remove('hidden');
                        if (viewState.cobertura.dirty) {
                            updateAllCoverageFilters();
                            updateCoverageView();
                            viewState.cobertura.dirty = false;
                        }
                        break;
                    case 'cidades':
                        cityView.classList.remove('hidden');
                        // Always trigger background sync if admin
                        syncGlobalCoordinates();
                        if (viewState.cidades.dirty) {
                            updateAllCityFilters();
                            updateCityView();
                            viewState.cidades.dirty = false;
                        }
                        break;
                    case 'semanal':
                        weeklyView.classList.remove('hidden');
                        if (viewState.semanal.dirty) {
                            populateWeeklyFilters();
                            updateWeeklyView();
                            viewState.semanal.dirty = false;
                        }
                        break;
                    case 'inovacoes-mes':
                        innovationsMonthView.classList.remove('hidden');
                        if (viewState.inovacoes.dirty) {
                            selectedInnovationsSupervisors = updateSupervisorFilter(document.getElementById('innovations-month-supervisor-filter-dropdown'), document.getElementById('innovations-month-supervisor-filter-text'), selectedInnovationsSupervisors, allSalesData);
                            updateSellerFilter(selectedInnovationsSupervisors, innovationsMonthVendedorFilterDropdown, innovationsMonthVendedorFilterText, [], allSalesData);
                            selectedInnovationsMonthTiposVenda = updateTipoVendaFilter(innovationsMonthTipoVendaFilterDropdown, innovationsMonthTipoVendaFilterText, selectedInnovationsMonthTiposVenda, [...allSalesData, ...allHistoryData]);
                            updateInnovationsMonthView();
                            viewState.inovacoes.dirty = false;
                        }
                        break;
                    case 'mix':
                        document.getElementById('mix-view').classList.remove('hidden');
                        if (viewState.mix.dirty) {
                            updateAllMixFilters();
                            updateMixView();
                            viewState.mix.dirty = false;
                        }
                        break;
                    case 'goals':
                        goalsView.classList.remove('hidden');
                        if (viewState.goals.dirty) {
                            updateGoalsView();
                            viewState.goals.dirty = false;
                        }
                        break;
                }
            };

            updateContent();

            await hideLoader();
        }

        async function enviarDadosParaSupabase(data) {
            const supabaseUrl = document.getElementById('supabase-url').value;

            // Tentamos obter a sessão atual do usuário
            const { data: { session } } = await supabaseClient.auth.getSession();

            // Definição da chave de autenticação (Token)
            // Agora usa estritamente o token de sessão do usuário logado
            const authToken = session?.access_token;

            // Definição da API Key (Header 'apikey')
            const apiKeyHeader = SUPABASE_ANON_KEY;

            if (!supabaseUrl || !authToken) {
                alert("Você precisa estar logado como Administrador para enviar dados.");
                return;
            }

            // Removido: const sbClient = supabase.createClient(supabaseUrl, supabaseKey);
            // Motivo: "Forbidden use of secret API key in browser"

            const statusText = document.getElementById('status-text');
            const progressBar = document.getElementById('progress-bar');
            const statusContainer = document.getElementById('status-container');

            statusContainer.classList.remove('hidden');
            const updateStatus = (msg, percent) => {
                statusText.textContent = msg;
                progressBar.style.width = `${percent}%`;
            };

            const BATCH_SIZE = 1000;

            const performUpsert = async (table, batch) => {
                const response = await fetch(`${supabaseUrl}/rest/v1/${table}`, {
                    method: 'POST',
                    headers: {
                        'apikey': apiKeyHeader,
                        'Authorization': `Bearer ${authToken}`,
                        'Content-Type': 'application/json',
                        'Prefer': 'resolution=merge-duplicates'
                    },
                    body: JSON.stringify(batch)
                });

                if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`Erro Supabase (${response.status}): ${errorText}`);
                }
            };

            const clearTable = async (table, pkColumn = 'id') => {
                // Tenta limpar usando a função RPC 'truncate_table' (muito mais rápido e sem timeout)
                // Isso resolve o erro "canceling statement due to statement timeout" em tabelas grandes
                try {
                    const rpcResponse = await fetch(`${supabaseUrl}/rest/v1/rpc/truncate_table`, {
                        method: 'POST',
                        headers: {
                            'apikey': apiKeyHeader,
                            'Authorization': `Bearer ${authToken}`,
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ table_name: table })
                    });

                    if (rpcResponse.ok) {
                        return; // Sucesso com TRUNCATE
                    } else {
                        // Se falhar (ex: função não existe), faz fallback para o método antigo
                        const errorText = await rpcResponse.text();
                        console.warn(`RPC truncate_table falhou para ${table} (Status: ${rpcResponse.status}). Msg: ${errorText}. Tentando DELETE convencional...`);
                    }
                } catch (e) {
                    console.warn(`Erro ao chamar RPC truncate_table para ${table}, tentando DELETE convencional...`, e);
                }

                // Fallback: Deleta todas as linhas da tabela (onde pkColumn não é nulo)
                const response = await fetch(`${supabaseUrl}/rest/v1/${table}?${pkColumn}=not.is.null`, {
                    method: 'DELETE',
                    headers: {
                        'apikey': apiKeyHeader,
                        'Authorization': `Bearer ${authToken}`,
                        'Content-Type': 'application/json'
                    }
                });

                if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`Erro ao limpar tabela ${table}: ${errorText}`);
                }
            };

            // List of columns that are dates and need conversion from timestamp (ms) to ISO String
            const dateColumns = new Set(['dtped', 'dtsaida', 'ultimacompra', 'datacadastro', 'dtcadastro', 'updated_at']);

            const formatValue = (key, value) => {
                // If it's a date column and value is a number, convert to ISO string
                if (dateColumns.has(key) && typeof value === 'number') {
                    // Check for valid timestamp (not 0 or crazy value) if needed,
                    // but usually new Date(val).toISOString() handles valid ms numbers.
                    // Postgres timestamp range: 4713 BC to 294276 AD.
                    // 1764720000000 is year 2025, which is fine.
                    try {
                        return new Date(value).toISOString();
                    } catch (e) {
                        return null; // Fallback for invalid dates
                    }
                }
                return value;
            };

            const uploadColumnarBatch = async (table, columnarData) => {
                const totalRows = columnarData.length;
                const columns = columnarData.columns;

                for (let i = 0; i < totalRows; i += BATCH_SIZE) {
                    const batch = [];
                    const end = Math.min(i + BATCH_SIZE, totalRows);

                    for (let j = i; j < end; j++) {
                        const row = {};
                        for (const col of columns) {
                            // OPTION B: Convert keys to lowercase to match Supabase DB schema
                            const lowerKey = col.toLowerCase();
                            const val = columnarData.values[col][j];
                            row[lowerKey] = formatValue(lowerKey, val);
                        }
                        batch.push(row);
                    }

                    await performUpsert(table, batch);

                    const progress = Math.round((i / totalRows) * 100);
                    updateStatus(`Enviando ${table}: ${progress}%`, progress);
                }
            };

            const uploadArrayBatch = async (table, arrayData) => {
                for (let i = 0; i < arrayData.length; i += BATCH_SIZE) {
                    const rawBatch = arrayData.slice(i, i + BATCH_SIZE);

                    // OPTION B: Convert keys to lowercase for Supabase
                    const batch = rawBatch.map(item => {
                        const newItem = {};
                        for (const key in item) {
                            const lowerKey = key.toLowerCase();
                            newItem[lowerKey] = formatValue(lowerKey, item[key]);
                        }
                        return newItem;
                    });

                    await performUpsert(table, batch);
                     const progress = Math.round((i / arrayData.length) * 100);
                    updateStatus(`Enviando ${table}: ${progress}%`, progress);
                }
            }

            try {
                if (data.detailed && data.detailed.length > 0) {
                    await clearTable('data_detailed');
                    await uploadColumnarBatch('data_detailed', data.detailed);
                }
                if (data.history && data.history.length > 0) {
                    await clearTable('data_history');
                    await uploadColumnarBatch('data_history', data.history);
                }
                if (data.byOrder && data.byOrder.length > 0) {
                    await clearTable('data_orders');
                    await uploadArrayBatch('data_orders', data.byOrder);
                }
                if (data.clients && data.clients.length > 0) {
                    await clearTable('data_clients');
                    await uploadColumnarBatch('data_clients', data.clients);
                }
                if (data.stock && data.stock.length > 0) {
                    await clearTable('data_stock');
                    await uploadArrayBatch('data_stock', data.stock);
                }
                if (data.innovations && data.innovations.length > 0) {
                    await clearTable('data_innovations');
                    await uploadArrayBatch('data_innovations', data.innovations);
                }
                if (data.product_details && data.product_details.length > 0) {
                    await clearTable('data_product_details', 'code');
                    await uploadArrayBatch('data_product_details', data.product_details);
                }
                if (data.active_products && data.active_products.length > 0) {
                    await clearTable('data_active_products', 'code');
                    await uploadArrayBatch('data_active_products', data.active_products);
                }
                if (data.metadata && data.metadata.length > 0) {
                    // Update last_update timestamp to current time (successful upload time)
                    const now = new Date();
                    const lastUpdateIdx = data.metadata.findIndex(m => m.key === 'last_update');
                    if (lastUpdateIdx !== -1) {
                        data.metadata[lastUpdateIdx].value = now.toISOString();
                    } else {
                        data.metadata.push({ key: 'last_update', value: now.toISOString() });
                    }

                    await clearTable('data_metadata', 'key');
                    await uploadArrayBatch('data_metadata', data.metadata);

                    // Update UI immediately
                    const lastUpdateText = document.getElementById('last-update-text');
                    if (lastUpdateText) {
                        lastUpdateText.textContent = `Dados atualizados em: ${now.toLocaleString('pt-BR')}`;
                    }
                }

                updateStatus('Upload Concluído com Sucesso!', 100);
                alert('Dados enviados com sucesso!');
                setTimeout(() => statusContainer.classList.add('hidden'), 3000);

            } catch (error) {
                console.error(error);
                let msg = error.message;
                // Detecta erros de permissão comuns
                if (msg.includes('403') || msg.includes('row-level security') || msg.includes('violates row-level security policy') || msg.includes('Access denied')) {
                     msg = "Permissão negada. Verifique se seu usuário tem permissão de 'adm' no Supabase. " + msg;
                }
                updateStatus('Erro: ' + msg, 0);
                alert('Erro durante o upload: ' + msg);
            }
        }

        function setupEventListeners() {
            // Uploader Logic
            const openAdminBtn = document.getElementById('open-admin-btn');
            const adminModal = document.getElementById('admin-uploader-modal');
            const adminCloseBtn = document.getElementById('admin-modal-close-btn');

            if (openAdminBtn) {
                openAdminBtn.addEventListener('click', () => {
                    if (window.userRole !== 'adm') {
                        alert('Apenas usuários com permissão "adm" podem acessar o Uploader.');
                        return;
                    }
                    adminModal.classList.remove('hidden');
                    // Close sidebar on mobile if open
                    const sideMenu = document.getElementById('side-menu');
                    const sidebarOverlay = document.getElementById('sidebar-overlay');
                    if (sideMenu) {
                        sideMenu.classList.remove('translate-x-0'); 
                        sideMenu.classList.add('-translate-x-full');
                    }
                    if (sidebarOverlay) sidebarOverlay.classList.add('hidden');
                });
            }
            if (adminCloseBtn) {
                adminCloseBtn.addEventListener('click', () => {
                    adminModal.classList.add('hidden');
                });
            }

            const generateBtn = document.getElementById('generate-btn');
            if (generateBtn) {
                generateBtn.addEventListener('click', () => {
                    const salesFile = document.getElementById('sales-file-input').files[0];
                    const clientsFile = document.getElementById('clients-file-input').files[0];
                    const productsFile = document.getElementById('products-file-input').files[0];
                    const historyFile = document.getElementById('history-file-input').files[0];
                    const innovationsFile = document.getElementById('innovations-file-input').files[0];

                    if (!salesFile && !historyFile) {
                        alert("Pelo menos o arquivo de Vendas ou Histórico é necessário.");
                        return;
                    }

                    // Initialize Worker
                    const worker = new Worker('worker.js');

                    document.getElementById('status-container').classList.remove('hidden');
                    document.getElementById('status-text').textContent = "Processando arquivos...";

                    worker.postMessage({ salesFile, clientsFile, productsFile, historyFile, innovationsFile });

                    worker.onmessage = (e) => {
                        const { type, data, status, percentage, message } = e.data;
                        if (type === 'progress') {
                            document.getElementById('status-text').textContent = status;
                            document.getElementById('progress-bar').style.width = percentage + '%';
                        } else if (type === 'result') {
                            enviarDadosParaSupabase(data);
                            worker.terminate();
                        } else if (type === 'error') {
                            alert('Erro no processamento: ' + message);
                            worker.terminate();
                        }
                    };
                });
            }

            // Helper to mark dirty states
            const markDirty = (view) => {
                if (viewState[view]) viewState[view].dirty = true;
            };

            document.querySelectorAll('.sidebar-toggle').forEach(btn => btn.addEventListener('click', toggleSidebar));
            const closeSidebarBtn = document.getElementById('close-sidebar-btn');
            if(closeSidebarBtn) closeSidebarBtn.addEventListener('click', toggleSidebar);
            const sidebarOverlay = document.getElementById('sidebar-overlay');
            if(sidebarOverlay) sidebarOverlay.addEventListener('click', toggleSidebar);

            document.querySelectorAll('.nav-link').forEach(link => {
                link.addEventListener('click', (e) => {
                    const target = e.currentTarget.dataset.target;

                    if (e.ctrlKey || e.metaKey) {
                        e.preventDefault();
                        const url = new URL(window.location.href);
                        url.searchParams.set('ir_para', target);
                        window.open(url.toString(), '_blank');
                    } else {
                        navigateTo(target);
                    }
                });
            });

            // --- Dashboard/Pedidos Filters ---
            const updateDashboard = () => {
                markDirty('dashboard'); markDirty('pedidos');
                updateAllVisuals();
            };

            const supervisorFilterBtn = document.getElementById('supervisor-filter-btn');
            const supervisorFilterDropdown = document.getElementById('supervisor-filter-dropdown');
            supervisorFilterBtn.addEventListener('click', () => supervisorFilterDropdown.classList.toggle('hidden'));
            supervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedMainSupervisors.push(value);
                    else selectedMainSupervisors = selectedMainSupervisors.filter(s => s !== value);

                    selectedSellers = []; // Reset sellers when supervisor changes to avoid inconsistent state? Or keep valid intersection?
                    // Better: Re-run updateSellerFilter which filters 'selectedSellers' to only keep valid ones.
                    selectedMainSupervisors = updateSupervisorFilter(supervisorFilterDropdown, document.getElementById('supervisor-filter-text'), selectedMainSupervisors, allSalesData);
                    selectedSellers = updateSellerFilter(selectedMainSupervisors, vendedorFilterDropdown, vendedorFilterText, selectedSellers, allSalesData);
                    mainTableState.currentPage = 1;
                    updateDashboard();
                }
            });

            const fornecedorFilterBtn = document.getElementById('fornecedor-filter-btn');
            const fornecedorFilterDropdown = document.getElementById('fornecedor-filter-dropdown');
            fornecedorFilterBtn.addEventListener('click', () => fornecedorFilterDropdown.classList.toggle('hidden'));
            fornecedorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedMainSuppliers.push(value);
                    else selectedMainSuppliers = selectedMainSuppliers.filter(s => s !== value);

                    let supplierDataSource = [...allSalesData, ...allHistoryData];
                    if (currentFornecedor) {
                        supplierDataSource = supplierDataSource.filter(s => s.OBSERVACAOFOR === currentFornecedor);
                    }
                    selectedMainSuppliers = updateSupplierFilter(fornecedorFilterDropdown, document.getElementById('fornecedor-filter-text'), selectedMainSuppliers, supplierDataSource, 'main');
                    mainTableState.currentPage = 1;
                    updateDashboard();
                }
            });

            vendedorFilterBtn.addEventListener('click', () => vendedorFilterDropdown.classList.toggle('hidden'));
            vendedorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedSellers.push(value); else selectedSellers = selectedSellers.filter(s => s !== value);
                    selectedSellers = updateSellerFilter(selectedMainSupervisors, vendedorFilterDropdown, vendedorFilterText, selectedSellers, allSalesData);
                    mainTableState.currentPage = 1;
                    updateDashboard();
                }
            });

            tipoVendaFilterBtn.addEventListener('click', () => tipoVendaFilterDropdown.classList.toggle('hidden'));
            tipoVendaFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedTiposVenda.push(value);
                    else selectedTiposVenda = selectedTiposVenda.filter(s => s !== value);
                    selectedTiposVenda = updateTipoVendaFilter(tipoVendaFilterDropdown, tipoVendaFilterText, selectedTiposVenda, allSalesData);
                    mainTableState.currentPage = 1;
                    updateDashboard();
                }
            });

            posicaoFilter.addEventListener('change', () => { mainTableState.currentPage = 1; updateDashboard(); });
            const debouncedUpdateDashboard = debounce(updateDashboard, 400);
            codcliFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[^0-9]/g, '');
                mainTableState.currentPage = 1;
                debouncedUpdateDashboard();
            });
            clearFiltersBtn.addEventListener('click', () => { resetMainFilters(); markDirty('dashboard'); markDirty('pedidos'); });

            prevPageBtn.addEventListener('click', () => {
                if (mainTableState.currentPage > 1) {
                    mainTableState.currentPage--;
                    renderTable(mainTableState.filteredData);
                }
            });
            nextPageBtn.addEventListener('click', () => {
                if (mainTableState.currentPage < mainTableState.totalPages) {
                    mainTableState.currentPage++;
                    renderTable(mainTableState.filteredData);
                }
            });

            mainComRedeBtn.addEventListener('click', () => mainRedeFilterDropdown.classList.toggle('hidden'));
            mainRedeGroupContainer.addEventListener('click', (e) => {
                if(e.target.closest('button')) {
                    const button = e.target.closest('button');
                    mainRedeGroupFilter = button.dataset.group;
                    mainRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    button.classList.add('active');
                    if (mainRedeGroupFilter !== 'com_rede') {
                        mainRedeFilterDropdown.classList.add('hidden');
                        selectedMainRedes = [];
                    }
                    updateRedeFilter(mainRedeFilterDropdown, mainComRedeBtnText, selectedMainRedes, allClientsData);
                    mainTableState.currentPage = 1;
                    updateDashboard();
                }
            });
            mainRedeFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedMainRedes.push(value);
                    else selectedMainRedes = selectedMainRedes.filter(r => r !== value);
                    selectedMainRedes = updateRedeFilter(mainRedeFilterDropdown, mainComRedeBtnText, selectedMainRedes, allClientsData);
                    mainTableState.currentPage = 1;
                    updateDashboard();
                }
            });

            // --- City View Filters ---
            const updateCity = () => {
                markDirty('cidades');
                handleCityFilterChange();
            };

            const citySupervisorFilterBtn = document.getElementById('city-supervisor-filter-btn');
            const citySupervisorFilterDropdown = document.getElementById('city-supervisor-filter-dropdown');
            citySupervisorFilterBtn.addEventListener('click', () => citySupervisorFilterDropdown.classList.toggle('hidden'));
            citySupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedCitySupervisors.push(value);
                    else selectedCitySupervisors = selectedCitySupervisors.filter(s => s !== value);

                    selectedCitySellers = [];
                    handleCityFilterChange();
                }
            });

            cityVendedorFilterBtn.addEventListener('click', () => cityVendedorFilterDropdown.classList.toggle('hidden'));
            cityVendedorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedCitySellers.includes(value)) selectedCitySellers.push(value);
                    } else {
                        selectedCitySellers = selectedCitySellers.filter(s => s !== value);
                    }
                    handleCityFilterChange({ skipFilter: 'seller' });
                }
            });

            citySupplierFilterBtn.addEventListener('click', () => citySupplierFilterDropdown.classList.toggle('hidden'));
            citySupplierFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox' && e.target.dataset.filterType === 'city') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedCitySuppliers.includes(value)) selectedCitySuppliers.push(value);
                    } else {
                        selectedCitySuppliers = selectedCitySuppliers.filter(s => s !== value);
                    }
                    handleCityFilterChange({ skipFilter: 'supplier' });
                }
            });

            cityTipoVendaFilterBtn.addEventListener('click', () => cityTipoVendaFilterDropdown.classList.toggle('hidden'));
            cityTipoVendaFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedCityTiposVenda.includes(value)) selectedCityTiposVenda.push(value);
                    } else {
                        selectedCityTiposVenda = selectedCityTiposVenda.filter(s => s !== value);
                    }
                    handleCityFilterChange({ skipFilter: 'tipoVenda' });
                }
            });

            cityComRedeBtn.addEventListener('click', () => cityRedeFilterDropdown.classList.toggle('hidden'));
            cityRedeGroupContainer.addEventListener('click', (e) => {
                if(e.target.closest('button')) {
                    const button = e.target.closest('button');
                    cityRedeGroupFilter = button.dataset.group;
                    cityRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    button.classList.add('active');

                    if (cityRedeGroupFilter !== 'com_rede') {
                        cityRedeFilterDropdown.classList.add('hidden');
                        selectedCityRedes = [];
                    }
                    handleCityFilterChange();
                }
            });
            cityRedeFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedCityRedes.push(value);
                    else selectedCityRedes = selectedCityRedes.filter(r => r !== value);

                    cityRedeGroupFilter = 'com_rede';
                    cityRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    cityComRedeBtn.classList.add('active');

                    handleCityFilterChange({ skipFilter: 'rede' });
                }
            });

            const toggleCityMapBtn = document.getElementById('toggle-city-map-btn');
            if (toggleCityMapBtn) {
                toggleCityMapBtn.addEventListener('click', () => {
                    const cityMapContainer = document.getElementById('city-map-container');
                    if (!cityMapContainer) return;

                    const isHidden = cityMapContainer.classList.contains('hidden');

                    if (isHidden) {
                        // Show Map
                        cityMapContainer.classList.remove('hidden');
                        toggleCityMapBtn.innerHTML = `
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.88 9.88l-3.29-3.29m7.532 7.532l3.29 3.29M3 3l3.59 3.59m0 0A9.953 9.953 0 0112 5c4.478 0 8.268 2.943 9.543 7a10.025 10.025 0 01-4.132 5.411m0 0L21 21"></path></svg>
                            Ocultar Mapa
                        `;

                        // Initialize or Refresh Leaflet
                        if (!leafletMap) {
                            initLeafletMap();
                        }

                        // Important: Invalidate size after removing 'hidden' so Leaflet calculates dimensions correctly
                        setTimeout(() => {
                            if (leafletMap) leafletMap.invalidateSize();
                            updateCityMap();
                        }, 100);

                    } else {
                        // Hide Map
                        cityMapContainer.classList.add('hidden');
                        toggleCityMapBtn.innerHTML = `
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7"></path></svg>
                            Ver Mapa
                        `;
                    }
                });
            }

            clearCityFiltersBtn.addEventListener('click', () => { resetCityFilters(); markDirty('cidades'); });
            const debouncedUpdateCity = debounce(updateCity, 400);
            cityCodCliFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[^0-9]/g, '');
                debouncedUpdateCity();
            });

            const debouncedCitySearch = debounce(() => {
                const { clients } = getCityFilteredData({ excludeFilter: 'city' });
                updateCitySuggestions(cityNameFilter, citySuggestions, clients);
            }, 300);

            cityNameFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                debouncedCitySearch();
            });
            cityNameFilter.addEventListener('focus', () => {
                const { clients } = getCityFilteredData({ excludeFilter: 'city' });
                citySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(cityNameFilter, citySuggestions, clients);
            });
            cityNameFilter.addEventListener('blur', () => setTimeout(() => citySuggestions.classList.add('hidden'), 150));
            cityNameFilter.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    citySuggestions.classList.add('hidden', 'manual-hide');
                    handleCityFilterChange();
                    e.target.blur();
                }
            });

            document.addEventListener('click', (e) => {
                if (!supervisorFilterBtn.contains(e.target) && !supervisorFilterDropdown.contains(e.target)) supervisorFilterDropdown.classList.add('hidden');
                if (!fornecedorFilterBtn.contains(e.target) && !fornecedorFilterDropdown.contains(e.target)) fornecedorFilterDropdown.classList.add('hidden');
                if (!vendedorFilterBtn.contains(e.target) && !vendedorFilterDropdown.contains(e.target)) vendedorFilterDropdown.classList.add('hidden');
                if (!tipoVendaFilterBtn.contains(e.target) && !tipoVendaFilterDropdown.contains(e.target)) tipoVendaFilterDropdown.classList.add('hidden');

                if (!citySupervisorFilterBtn.contains(e.target) && !citySupervisorFilterDropdown.contains(e.target)) citySupervisorFilterDropdown.classList.add('hidden');
                if (!cityVendedorFilterBtn.contains(e.target) && !cityVendedorFilterDropdown.contains(e.target)) cityVendedorFilterDropdown.classList.add('hidden');
                if (!citySupplierFilterBtn.contains(e.target) && !citySupplierFilterDropdown.contains(e.target)) citySupplierFilterDropdown.classList.add('hidden');
                if (!cityTipoVendaFilterBtn.contains(e.target) && !cityTipoVendaFilterDropdown.contains(e.target)) cityTipoVendaFilterDropdown.classList.add('hidden');
                if (!cityComRedeBtn.contains(e.target) && !cityRedeFilterDropdown.contains(e.target)) cityRedeFilterDropdown.classList.add('hidden');
                if (!mainComRedeBtn.contains(e.target) && !mainRedeFilterDropdown.contains(e.target)) mainRedeFilterDropdown.classList.add('hidden');

                if (!comparisonSupervisorFilterBtn.contains(e.target) && !comparisonSupervisorFilterDropdown.contains(e.target)) comparisonSupervisorFilterDropdown.classList.add('hidden');
                if (!comparisonComRedeBtn.contains(e.target) && !comparisonRedeFilterDropdown.contains(e.target)) comparisonRedeFilterDropdown.classList.add('hidden');
                if (!comparisonVendedorFilterBtn.contains(e.target) && !comparisonVendedorFilterDropdown.contains(e.target)) comparisonVendedorFilterDropdown.classList.add('hidden');
                if (!comparisonTipoVendaFilterBtn.contains(e.target) && !comparisonTipoVendaFilterDropdown.contains(e.target)) comparisonTipoVendaFilterDropdown.classList.add('hidden');
                if (!comparisonSupplierFilterBtn.contains(e.target) && !comparisonSupplierFilterDropdown.contains(e.target)) comparisonSupplierFilterDropdown.classList.add('hidden');
                if (!comparisonProductFilterBtn.contains(e.target) && !comparisonProductFilterDropdown.contains(e.target)) comparisonProductFilterDropdown.classList.add('hidden');

                if (!stockSupervisorFilterBtn.contains(e.target) && !stockSupervisorFilterDropdown.contains(e.target)) stockSupervisorFilterDropdown.classList.add('hidden');
                if (!stockComRedeBtn.contains(e.target) && !stockRedeFilterDropdown.contains(e.target)) stockRedeFilterDropdown.classList.add('hidden');
                if (!stockVendedorFilterBtn.contains(e.target) && !stockVendedorFilterDropdown.contains(e.target)) stockVendedorFilterDropdown.classList.add('hidden');
                if (!stockSupplierFilterBtn.contains(e.target) && !stockSupplierFilterDropdown.contains(e.target)) stockSupplierFilterDropdown.classList.add('hidden');
                if (!stockProductFilterBtn.contains(e.target) && !stockProductFilterDropdown.contains(e.target)) stockProductFilterDropdown.classList.add('hidden');
                if (!stockTipoVendaFilterBtn.contains(e.target) && !stockTipoVendaFilterDropdown.contains(e.target)) stockTipoVendaFilterDropdown.classList.add('hidden');

                if (!innovationsMonthSupervisorFilterBtn.contains(e.target) && !innovationsMonthSupervisorFilterDropdown.contains(e.target)) innovationsMonthSupervisorFilterDropdown.classList.add('hidden');
                if (!coverageSupervisorFilterBtn.contains(e.target) && !coverageSupervisorFilterDropdown.contains(e.target)) coverageSupervisorFilterDropdown.classList.add('hidden');

                if (e.target.closest('[data-pedido-id]')) { e.preventDefault(); openModal(e.target.closest('[data-pedido-id]').dataset.pedidoId); }
                if (e.target.closest('[data-codcli]')) { e.preventDefault(); openClientModal(e.target.closest('[data-codcli]').dataset.codcli); }
                if (e.target.closest('#city-suggestions > div')) { cityNameFilter.value = e.target.textContent; citySuggestions.classList.add('hidden'); updateCityView(); }
                if (e.target.closest('#comparison-city-suggestions > div')) { comparisonCityFilter.value = e.target.textContent; comparisonCitySuggestions.classList.add('hidden'); updateAllComparisonFilters(); updateComparisonView(); }
                else if (!comparisonCityFilter.contains(e.target)) comparisonCitySuggestions.classList.add('hidden');
                if (e.target.closest('#stock-city-suggestions > div')) { stockCityFilter.value = e.target.textContent; stockCitySuggestions.classList.add('hidden'); handleStockFilterChange(); }
                else if (!stockCityFilter.contains(e.target)) stockCitySuggestions.classList.add('hidden');
            });

            fornecedorToggleContainerEl.querySelectorAll('.fornecedor-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fornecedor = btn.dataset.fornecedor;
                    if (currentFornecedor === fornecedor) { currentFornecedor = ''; btn.classList.remove('active'); } else { currentFornecedor = fornecedor; fornecedorToggleContainerEl.querySelectorAll('.fornecedor-btn').forEach(b => b.classList.remove('active')); btn.classList.add('active'); }

                    let supplierDataSource = [...allSalesData, ...allHistoryData];
                    if (currentFornecedor) {
                        supplierDataSource = supplierDataSource.filter(s => s.OBSERVACAOFOR === currentFornecedor);
                    }
                    selectedMainSuppliers = updateSupplierFilter(document.getElementById('fornecedor-filter-dropdown'), document.getElementById('fornecedor-filter-text'), selectedMainSuppliers, supplierDataSource, 'main');

                    selectedMainSupervisors = updateSupervisorFilter(document.getElementById('supervisor-filter-dropdown'), document.getElementById('supervisor-filter-text'), selectedMainSupervisors, allSalesData);
                    selectedSellers = [];
                    updateSellerFilter(selectedMainSupervisors, vendedorFilterDropdown, vendedorFilterText, selectedSellers, allSalesData);
                    mainTableState.currentPage = 1;
                    updateDashboard();
                });
            });

            const updateWeekly = () => {
                markDirty('semanal');
                updateWeeklyView();
            };

            weeklyFornecedorToggleContainer.querySelectorAll('.fornecedor-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fornecedor = btn.dataset.fornecedor;
                    if (currentWeeklyFornecedor === fornecedor) { currentWeeklyFornecedor = ''; btn.classList.remove('active'); } else { currentWeeklyFornecedor = fornecedor; weeklyFornecedorToggleContainer.querySelectorAll('.fornecedor-btn').forEach(b => b.classList.remove('active')); btn.classList.add('active'); }
                    updateWeekly();
                });
            });

            weeklySupervisorFilterBtn.addEventListener('click', () => weeklySupervisorFilterDropdown.classList.toggle('hidden'));
            weeklySupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedWeeklySupervisors.push(value);
                    else selectedWeeklySupervisors = selectedWeeklySupervisors.filter(s => s !== value);

                    selectedWeeklySupervisors = updateSupervisorFilter(weeklySupervisorFilterDropdown, weeklySupervisorFilterText, selectedWeeklySupervisors, allSalesData);
                    selectedWeeklySellers = updateSellerFilter(selectedWeeklySupervisors, weeklyVendedorFilterDropdown, weeklyVendedorFilterText, selectedWeeklySellers, allSalesData);
                    updateWeekly();
                }
            });

            weeklyVendedorFilterBtn.addEventListener('click', () => weeklyVendedorFilterDropdown.classList.toggle('hidden'));
            weeklyVendedorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedWeeklySellers.includes(value)) selectedWeeklySellers.push(value);
                    } else {
                        selectedWeeklySellers = selectedWeeklySellers.filter(s => s !== value);
                    }
                    updateSellerFilter(selectedWeeklySupervisors, weeklyVendedorFilterDropdown, weeklyVendedorFilterText, selectedWeeklySellers, allSalesData);
                    updateWeekly();
                }
            });

            clearWeeklyFiltersBtn.addEventListener('click', () => { resetWeeklyFilters(); markDirty('semanal'); });

            // --- Comparison View Filters ---
            const updateComparison = () => {
                markDirty('comparativo');
                updateAllComparisonFilters();
                updateComparisonView();
            };

            const handleComparisonFilterChange = updateComparison;

            const comparisonSupervisorFilterBtn = document.getElementById('comparison-supervisor-filter-btn');
            const comparisonSupervisorFilterDropdown = document.getElementById('comparison-supervisor-filter-dropdown');
            comparisonSupervisorFilterBtn.addEventListener('click', () => comparisonSupervisorFilterDropdown.classList.toggle('hidden'));
            comparisonSupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedComparisonSupervisors.push(value);
                    else selectedComparisonSupervisors = selectedComparisonSupervisors.filter(s => s !== value);
                    selectedComparisonSellers = [];
                    handleComparisonFilterChange();
                }
            });

            comparisonFilialFilter.addEventListener('change', handleComparisonFilterChange);
            comparisonVendedorFilterBtn.addEventListener('click', () => comparisonVendedorFilterDropdown.classList.toggle('hidden'));
            comparisonVendedorFilterDropdown.addEventListener('change', (e) => { if (e.target.type === 'checkbox') { const { value, checked } = e.target; if (checked) selectedComparisonSellers.push(value); else selectedComparisonSellers = selectedComparisonSellers.filter(s => s !== value); handleComparisonFilterChange(); } });
            comparisonTipoVendaFilterBtn.addEventListener('click', () => comparisonTipoVendaFilterDropdown.classList.toggle('hidden'));
            comparisonTipoVendaFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedComparisonTiposVenda.includes(value)) selectedComparisonTiposVenda.push(value);
                    } else {
                        selectedComparisonTiposVenda = selectedComparisonTiposVenda.filter(s => s !== value);
                    }
                    selectedComparisonTiposVenda = updateTipoVendaFilter(comparisonTipoVendaFilterDropdown, comparisonTipoVendaFilterText, selectedComparisonTiposVenda, [...allSalesData, ...allHistoryData]);
                    handleComparisonFilterChange();
                }
            });
            comparisonFornecedorToggleContainer.addEventListener('click', (e) => { if (e.target.tagName === 'BUTTON') { const fornecedor = e.target.dataset.fornecedor; if (currentComparisonFornecedor === fornecedor) { currentComparisonFornecedor = ''; e.target.classList.remove('active'); } else { currentComparisonFornecedor = fornecedor; comparisonFornecedorToggleContainer.querySelectorAll('.fornecedor-btn').forEach(b => b.classList.remove('active')); e.target.classList.add('active'); } handleComparisonFilterChange(); } });
            comparisonSupplierFilterBtn.addEventListener('click', () => comparisonSupplierFilterDropdown.classList.toggle('hidden'));
            comparisonSupplierFilterDropdown.addEventListener('change', (e) => { if (e.target.type === 'checkbox' && e.target.dataset.filterType === 'comparison') { const { value, checked } = e.target; if (checked) selectedComparisonSuppliers.push(value); else selectedComparisonSuppliers = selectedComparisonSuppliers.filter(s => s !== value); handleComparisonFilterChange(); } });

            comparisonComRedeBtn.addEventListener('click', () => comparisonRedeFilterDropdown.classList.toggle('hidden'));
            comparisonRedeGroupContainer.addEventListener('click', (e) => {
                if(e.target.closest('button')) {
                    const button = e.target.closest('button');
                    comparisonRedeGroupFilter = button.dataset.group;
                    comparisonRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    button.classList.add('active');
                    if (comparisonRedeGroupFilter !== 'com_rede') {
                        comparisonRedeFilterDropdown.classList.add('hidden');
                        selectedComparisonRedes = [];
                    }
                    updateRedeFilter(comparisonRedeFilterDropdown, comparisonComRedeBtnText, selectedComparisonRedes, allClientsData);
                    handleComparisonFilterChange();
                }
            });
            comparisonRedeFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedComparisonRedes.push(value);
                    else selectedComparisonRedes = selectedComparisonRedes.filter(r => r !== value);

                    comparisonRedeGroupFilter = 'com_rede';
                    comparisonRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    comparisonComRedeBtn.classList.add('active');

                    selectedComparisonRedes = updateRedeFilter(comparisonRedeFilterDropdown, comparisonComRedeBtnText, selectedComparisonRedes, allClientsData);
                    handleComparisonFilterChange();
                }
            });

            comparisonCityFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                const { currentSales, historySales } = getComparisonFilteredData({ excludeFilter: 'city' });
                comparisonCitySuggestions.classList.remove('manual-hide');
                updateComparisonCitySuggestions([...currentSales, ...historySales]);
            });
            comparisonCityFilter.addEventListener('focus', () => {
                const { currentSales, historySales } = getComparisonFilteredData({ excludeFilter: 'city' });
                comparisonCitySuggestions.classList.remove('manual-hide');
                updateComparisonCitySuggestions([...currentSales, ...historySales]);
            });
            comparisonCityFilter.addEventListener('blur', () => setTimeout(() => comparisonCitySuggestions.classList.add('hidden'), 150));
            comparisonCityFilter.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    comparisonCitySuggestions.classList.add('hidden', 'manual-hide');
                    handleComparisonFilterChange();
                    e.target.blur();
                }
            });
            comparisonCitySuggestions.addEventListener('click', (e) => {
                if (e.target.tagName === 'DIV') {
                    comparisonCityFilter.value = e.target.textContent;
                    comparisonCitySuggestions.classList.add('hidden');
                    handleComparisonFilterChange();
                }
            });

            clearComparisonFiltersBtn.addEventListener('click', resetComparisonFilters);

            const handleProductFilterChange = (e, selectedArray) => {
                 if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedArray.includes(value)) selectedArray.push(value);
                    } else {
                        const index = selectedArray.indexOf(value);
                        if (index > -1) selectedArray.splice(index, 1);
                    }
                    return true;
                }
                return false;
            }

            comparisonProductFilterBtn.addEventListener('click', () => {
                updateComparisonProductFilter();
                comparisonProductFilterDropdown.classList.toggle('hidden');
            });

            const debouncedComparisonProductSearch = debounce(updateComparisonProductFilter, 250);
            comparisonProductFilterDropdown.addEventListener('input', (e) => {
                if (e.target.id === 'comparison-product-search-input') {
                    debouncedComparisonProductSearch();
                }
            });
            comparisonProductFilterDropdown.addEventListener('change', (e) => {
                if(e.target.dataset.filterType === 'comparison' && handleProductFilterChange(e, selectedComparisonProducts)) {
                    handleComparisonFilterChange();
                    updateComparisonProductFilter();
                }
            });
            stockProductFilterBtn.addEventListener('click', () => {
                updateStockProductFilter();
                stockProductFilterDropdown.classList.toggle('hidden');
            });

            const debouncedStockProductSearch = debounce(updateStockProductFilter, 250);
            stockProductFilterDropdown.addEventListener('input', (e) => {
                if (e.target.id === 'stock-product-search-input') {
                    debouncedStockProductSearch();
                }
            });

            stockProductFilterDropdown.addEventListener('change', (e) => {
                if(e.target.dataset.filterType === 'stock' && handleProductFilterChange(e, selectedStockProducts)) {
                    handleStockFilterChange();
                    updateStockProductFilter();
                }
            });

            stockFilialFilter.addEventListener('change', handleStockFilterChange);
            clearStockFiltersBtn.addEventListener('click', resetStockFilters);
            stockFornecedorToggleContainer.addEventListener('click', (e) => { if (e.target.tagName === 'BUTTON') { const fornecedor = e.target.dataset.fornecedor; if (currentStockFornecedor === fornecedor) { currentStockFornecedor = ''; e.target.classList.remove('active'); } else { currentStockFornecedor = fornecedor; stockFornecedorToggleContainer.querySelectorAll('.fornecedor-btn').forEach(b => b.classList.remove('active')); e.target.classList.add('active'); } handleStockFilterChange(); } });

            const stockSupervisorFilterBtn = document.getElementById('stock-supervisor-filter-btn');
            const stockSupervisorFilterDropdown = document.getElementById('stock-supervisor-filter-dropdown');
            stockSupervisorFilterBtn.addEventListener('click', () => stockSupervisorFilterDropdown.classList.toggle('hidden'));
            stockSupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedStockSupervisors.push(value);
                    else selectedStockSupervisors = selectedStockSupervisors.filter(s => s !== value);

                    selectedStockSellers = [];
                    handleStockFilterChange();
                }
            });

            stockVendedorFilterBtn.addEventListener('click', () => stockVendedorFilterDropdown.classList.toggle('hidden'));
            stockVendedorFilterDropdown.addEventListener('change', (e) => { if (e.target.type === 'checkbox') { const { value, checked } = e.target; if (checked) selectedStockSellers.push(value); else selectedStockSellers = selectedStockSellers.filter(s => s !== value); handleStockFilterChange({ skipFilter: 'seller' }); } });
            stockSupplierFilterBtn.addEventListener('click', () => stockSupplierFilterDropdown.classList.toggle('hidden'));
            stockSupplierFilterDropdown.addEventListener('change', (e) => { if (e.target.dataset.filterType === 'stock' && e.target.type === 'checkbox') { const { value, checked } = e.target; if (checked) { if(!selectedStockSuppliers.includes(value)) selectedStockSuppliers.push(value); } else { selectedStockSuppliers = selectedStockSuppliers.filter(s => s !== value); } handleStockFilterChange({ skipFilter: 'supplier' }); } });

            stockTipoVendaFilterBtn.addEventListener('click', () => stockTipoVendaFilterDropdown.classList.toggle('hidden'));
            stockTipoVendaFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedStockTiposVenda.includes(value)) selectedStockTiposVenda.push(value);
                    } else {
                        selectedStockTiposVenda = selectedStockTiposVenda.filter(s => s !== value);
                    }
                    selectedStockTiposVenda = updateTipoVendaFilter(stockTipoVendaFilterDropdown, stockTipoVendaFilterText, selectedStockTiposVenda, [...allSalesData, ...allHistoryData]);
                    handleStockFilterChange({ skipFilter: 'tipoVenda' });
                }
            });

            stockCityFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                const cityData = getStockFilteredData({ excludeFilter: 'city' });
                stockCitySuggestions.classList.remove('manual-hide');
                updateStockCitySuggestions([...cityData.sales, ...cityData.history]);
            });
            stockCityFilter.addEventListener('focus', () => {
                const cityData = getStockFilteredData({ excludeFilter: 'city' });
                stockCitySuggestions.classList.remove('manual-hide');
                updateStockCitySuggestions([...cityData.sales, ...cityData.history]);
            });
            stockCityFilter.addEventListener('blur', () => setTimeout(() => stockCitySuggestions.classList.add('hidden'), 150));
            stockCityFilter.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    stockCitySuggestions.classList.add('hidden', 'manual-hide');
                    handleStockFilterChange();
                    e.target.blur();
                }
            });
            stockCitySuggestions.addEventListener('click', (e) => {
                if (e.target.tagName === 'DIV') {
                    stockCityFilter.value = e.target.textContent;
                    stockCitySuggestions.classList.add('hidden');
                    handleStockFilterChange();
                }
            });

            stockComRedeBtn.addEventListener('click', () => stockRedeFilterDropdown.classList.toggle('hidden'));
            stockRedeGroupContainer.addEventListener('click', (e) => {
                if(e.target.closest('button')) {
                    const button = e.target.closest('button');
                    stockRedeGroupFilter = button.dataset.group;
                    stockRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    button.classList.add('active');
                    if (stockRedeGroupFilter !== 'com_rede') {
                        stockRedeFilterDropdown.classList.add('hidden');
                        selectedStockRedes = [];
                    }
                    updateRedeFilter(stockRedeFilterDropdown, stockComRedeBtnText, selectedStockRedes, allClientsData, 'Com Rede');
                    handleStockFilterChange();
                }
            });
            stockRedeFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedStockRedes.push(value);
                    else selectedStockRedes = selectedStockRedes.filter(r => r !== value);
                    selectedStockRedes = updateRedeFilter(stockRedeFilterDropdown, stockComRedeBtnText, selectedStockRedes, allClientsData, 'Com Rede');
                    handleStockFilterChange();
                }
            });


            comparisonTendencyToggle.addEventListener('click', () => {
                useTendencyComparison = !useTendencyComparison;
                comparisonTendencyToggle.textContent = useTendencyComparison ? 'Ver Dados Reais' : 'Calcular Tendência';
                comparisonTendencyToggle.classList.toggle('bg-orange-600');
                comparisonTendencyToggle.classList.toggle('hover:bg-orange-500');
                comparisonTendencyToggle.classList.toggle('bg-purple-600');
                comparisonTendencyToggle.classList.toggle('hover:bg-purple-500');
                updateComparison();
            });

            toggleWeeklyBtn.addEventListener('click', () => {
                comparisonChartType = 'weekly';
                toggleWeeklyBtn.classList.add('active');
                toggleMonthlyBtn.classList.remove('active');
                document.getElementById('comparison-monthly-metric-container').classList.add('hidden');
                updateComparison();
            });

            toggleMonthlyBtn.addEventListener('click', () => {
                comparisonChartType = 'monthly';
                toggleMonthlyBtn.classList.add('active');
                toggleWeeklyBtn.classList.remove('active');
                // The toggle visibility is handled inside updateComparisonView based on mode
                updateComparison();
            });

            // New Metric Toggle Listeners
            const toggleMonthlyFatBtn = document.getElementById('toggle-monthly-fat-btn');
            const toggleMonthlyClientsBtn = document.getElementById('toggle-monthly-clients-btn');

            if (toggleMonthlyFatBtn && toggleMonthlyClientsBtn) {
                toggleMonthlyFatBtn.addEventListener('click', () => {
                    comparisonMonthlyMetric = 'faturamento';
                    toggleMonthlyFatBtn.classList.add('active');
                    toggleMonthlyClientsBtn.classList.remove('active');
                    updateComparison();
                });

                toggleMonthlyClientsBtn.addEventListener('click', () => {
                    comparisonMonthlyMetric = 'clientes';
                    toggleMonthlyClientsBtn.classList.add('active');
                    toggleMonthlyFatBtn.classList.remove('active');
                    updateComparison();
                });
            }

            mainHolidayPickerBtn.addEventListener('click', () => {
                renderCalendar(calendarState.year, calendarState.month);
                holidayModal.classList.remove('hidden');
            });
            comparisonHolidayPickerBtn.addEventListener('click', () => {
                renderCalendar(calendarState.year, calendarState.month);
                holidayModal.classList.remove('hidden');
            });
            holidayModalCloseBtn.addEventListener('click', () => holidayModal.classList.add('hidden'));
            holidayModalDoneBtn.addEventListener('click', () => {
                holidayModal.classList.add('hidden');
                const holidayBtnText = selectedHolidays.length > 0 ? `${selectedHolidays.length} feriado(s)` : 'Selecionar Feriados';
                comparisonHolidayPickerBtn.textContent = holidayBtnText;
                mainHolidayPickerBtn.textContent = holidayBtnText;
                updateComparison();
                updateDashboard();
            });
            calendarContainer.addEventListener('click', (e) => {
                if (e.target.id === 'prev-month-btn') {
                    calendarState.month--;
                    if (calendarState.month < 0) {
                        calendarState.month = 11;
                        calendarState.year--;
                    }
                    renderCalendar(calendarState.year, calendarState.month);
                } else if (e.target.id === 'next-month-btn') {
                    calendarState.month++;
                    if (calendarState.month > 11) {
                        calendarState.month = 0;
                        calendarState.year++;
                    }
                    renderCalendar(calendarState.year, calendarState.month);
                } else if (e.target.dataset.date) {
                    const dateString = e.target.dataset.date;
                    const index = selectedHolidays.indexOf(dateString);
                    if (index > -1) {
                        selectedHolidays.splice(index, 1);
                    } else {
                        selectedHolidays.push(dateString);
                    }
                    renderCalendar(calendarState.year, calendarState.month);
                }
            });


            document.getElementById('export-active-pdf-btn').addEventListener('click', () => exportClientsPDF(activeClientsForExport, 'Relatório de Clientes Ativos no Mês', 'clientes_ativos', true));
            document.getElementById('export-inactive-pdf-btn').addEventListener('click', () => exportClientsPDF(inactiveClientsForExport, 'Relatório de Clientes Sem Vendas no Mês', 'clientes_sem_vendas', false));
            modalCloseBtn.addEventListener('click', () => modal.classList.add('hidden'));
            clientModalCloseBtn.addEventListener('click', () => clientModal.classList.add('hidden'));
            faturamentoBtn.addEventListener('click', () => { currentProductMetric = 'faturamento'; faturamentoBtn.classList.add('active'); pesoBtn.classList.remove('active'); updateDashboard(); });
            pesoBtn.addEventListener('click', () => { currentProductMetric = 'peso'; pesoBtn.classList.add('active'); faturamentoBtn.classList.remove('active'); updateDashboard(); });

            // --- Innovations View Filters ---
            const updateInnovations = () => {
                markDirty('inovacoes');
                updateInnovationsMonthView();
            };

            innovationsMonthCategoryFilter.addEventListener('change', updateInnovations);

            const debouncedUpdateInnovationsMonth = debounce(updateInnovations, 400);

            const innovationsMonthSupervisorFilterBtn = document.getElementById('innovations-month-supervisor-filter-btn');
            const innovationsMonthSupervisorFilterDropdown = document.getElementById('innovations-month-supervisor-filter-dropdown');
            innovationsMonthSupervisorFilterBtn.addEventListener('click', () => innovationsMonthSupervisorFilterDropdown.classList.toggle('hidden'));
            innovationsMonthSupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedInnovationsSupervisors.push(value);
                    else selectedInnovationsSupervisors = selectedInnovationsSupervisors.filter(s => s !== value);

                    selectedInnovationsSupervisors = updateSupervisorFilter(innovationsMonthSupervisorFilterDropdown, document.getElementById('innovations-month-supervisor-filter-text'), selectedInnovationsSupervisors, allSalesData, true);

                    selectedInnovationsMonthSellers = [];
                    updateSellerFilter(selectedInnovationsSupervisors, innovationsMonthVendedorFilterDropdown, innovationsMonthVendedorFilterText, selectedInnovationsMonthSellers, allSalesData);
                    debouncedUpdateInnovationsMonth();
                }
            });

            innovationsMonthVendedorFilterBtn.addEventListener('click', () => innovationsMonthVendedorFilterDropdown.classList.toggle('hidden'));
            innovationsMonthVendedorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedInnovationsMonthSellers.includes(value)) selectedInnovationsMonthSellers.push(value);
                    } else {
                        selectedInnovationsMonthSellers = selectedInnovationsMonthSellers.filter(s => s !== value);
                    }
                    updateSellerFilter(selectedInnovationsSupervisors, innovationsMonthVendedorFilterDropdown, innovationsMonthVendedorFilterText, selectedInnovationsMonthSellers, allSalesData);
                    debouncedUpdateInnovationsMonth();
                }
            });

            innovationsMonthCityFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                const cityDataSource = getInnovationsMonthFilteredData({ excludeFilter: 'city' }).clients;
                innovationsMonthCitySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(innovationsMonthCityFilter, innovationsMonthCitySuggestions, cityDataSource);
            });
            innovationsMonthCityFilter.addEventListener('focus', () => {
                const cityDataSource = getInnovationsMonthFilteredData({ excludeFilter: 'city' }).clients;
                innovationsMonthCitySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(innovationsMonthCityFilter, innovationsMonthCitySuggestions, cityDataSource);
            });
            innovationsMonthCityFilter.addEventListener('blur', () => setTimeout(() => innovationsMonthCitySuggestions.classList.add('hidden'), 150));
            innovationsMonthCityFilter.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    innovationsMonthCitySuggestions.classList.add('hidden', 'manual-hide');
                    debouncedUpdateInnovationsMonth();
                    e.target.blur();
                }
            });
            innovationsMonthCitySuggestions.addEventListener('click', (e) => {
                if (e.target.tagName === 'DIV') {
                    innovationsMonthCityFilter.value = e.target.textContent;
                    innovationsMonthCitySuggestions.classList.add('hidden');
                    debouncedUpdateInnovationsMonth();
                }
            });

            innovationsMonthFilialFilter.addEventListener('change', debouncedUpdateInnovationsMonth);
            clearInnovationsMonthFiltersBtn.addEventListener('click', () => { resetInnovationsMonthFilters(); markDirty('inovacoes'); });
            exportInnovationsMonthPdfBtn.addEventListener('click', exportInnovationsMonthPDF);

            innovationsMonthTipoVendaFilterBtn.addEventListener('click', () => innovationsMonthTipoVendaFilterDropdown.classList.toggle('hidden'));
            innovationsMonthTipoVendaFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedInnovationsMonthTiposVenda.includes(value)) selectedInnovationsMonthTiposVenda.push(value);
                    } else {
                        selectedInnovationsMonthTiposVenda = selectedInnovationsMonthTiposVenda.filter(s => s !== value);
                    }
                    selectedInnovationsMonthTiposVenda = updateTipoVendaFilter(innovationsMonthTipoVendaFilterDropdown, innovationsMonthTipoVendaFilterText, selectedInnovationsMonthTiposVenda, [...allSalesData, ...allHistoryData]);
                    debouncedUpdateInnovationsMonth();
                }
            });


            document.getElementById('export-coverage-pdf-btn').addEventListener('click', exportCoveragePDF);

            const coverageChartToggleBtn = document.getElementById('coverage-chart-toggle-btn');
            if (coverageChartToggleBtn) {
                coverageChartToggleBtn.addEventListener('click', () => {
                    currentCoverageChartMode = currentCoverageChartMode === 'city' ? 'seller' : 'city';
                    updateCoverageView();
                });
            }

            // --- Mix View Event Listeners ---
            // --- Goals View Event Listeners ---
            const updateGoals = () => {
                markDirty('goals');
                handleGoalsFilterChange();
            };

            document.addEventListener('goalsCleared', () => {
                updateGoals();
            });

            const debouncedUpdateGoals = debounce(updateGoals, 400);

            async function loadGoalsFromSupabase() {
                try {
                    const monthKey = new Date().toISOString().slice(0, 7);
                    const { data, error } = await window.supabaseClient
                        .from('goals_distribution')
                        .select('goals_data')
                        .eq('month_key', monthKey)
                        .eq('supplier', 'ALL')
                        .eq('brand', 'GENERAL')
                        .maybeSingle();

                    if (error) {
                        console.error('Erro ao carregar metas:', error);
                        return;
                    }

                    if (data && data.goals_data) {
                        const gd = data.goals_data;
                        let clientsData = {};
                        let targetsData = {};

                        if (gd.clients || gd.targets) {
                            clientsData = gd.clients || {};
                            targetsData = gd.targets || {};
                        } else {
                            clientsData = gd;
                        }

                        globalClientGoals = new Map();
                        for (const [key, val] of Object.entries(clientsData)) {
                            const clientMap = new Map();
                            for (const [k, v] of Object.entries(val)) {
                                clientMap.set(k, v);
                            }
                            globalClientGoals.set(key, clientMap);
                        }

                        if (targetsData && Object.keys(targetsData).length > 0) {
                            for (const key in targetsData) {
                                goalsTargets[key] = targetsData[key];
                            }
                        }

                        console.log('Metas carregadas do Supabase.');
                        updateGoals();
                    }
                } catch (err) {
                    console.error('Exceção ao carregar metas:', err);
                }
            }

            // Trigger Load
            loadGoalsFromSupabase();

            // Sub-tabs Switching
            const goalsSubTabsContainer = document.getElementById('goals-sub-tabs-container');
            if (goalsSubTabsContainer) {
                goalsSubTabsContainer.addEventListener('click', (e) => {
                    const btn = e.target.closest('.goals-sub-tab');
                    if (!btn) return;

                    // Ensure metrics are ready
                    if (Object.keys(globalGoalsMetrics).length === 0) {
                        calculateGoalsMetrics();
                    }

                    // Remove active styles from ALL sub-tabs across both groups
                    document.querySelectorAll('.goals-sub-tab').forEach(b => {
                        b.classList.remove('active', 'text-teal-400', 'font-bold', 'border-b-2', 'border-teal-400');
                        b.classList.add('text-slate-400', 'font-medium');
                        const indicator = b.querySelector('.indicator');
                        if (indicator) indicator.remove();
                    });

                    btn.classList.remove('text-slate-400', 'font-medium');
                    btn.classList.add('active', 'text-teal-400', 'font-bold', 'border-b-2', 'border-teal-400');

                    const indicator = document.createElement('span');
                    indicator.className = 'w-2 h-2 rounded-full bg-teal-400 inline-block indicator mr-2';
                    btn.prepend(indicator);

                    currentGoalsSupplier = btn.dataset.supplier;
                    currentGoalsBrand = btn.dataset.brand || null;

                    const cacheKey = currentGoalsSupplier + (currentGoalsBrand ? `_${currentGoalsBrand}` : '');

                    // --- Update Metrics Display and Pre-fill Inputs ---
                    const metrics = globalGoalsMetrics[cacheKey];

                    if (!goalsTargets[cacheKey]) {
                        goalsTargets[cacheKey] = { fat: 0, vol: 0 };
                    }
                    const target = goalsTargets[cacheKey];

                    if (metrics) {
                        // PRE-FILL logic: If target is 0 (uninitialized), use Previous Month values as default suggestion
                        if (target.fat === 0) target.fat = metrics.prevFat;
                        if (target.vol === 0) target.vol = metrics.prevVol;
                    }

                    // Update Input Fields
                    const fatInput = document.getElementById('goal-global-fat');
                    const volInput = document.getElementById('goal-global-vol');

                    if (fatInput) fatInput.value = target.fat.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    if (volInput) volInput.value = target.vol.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 });
                    // ----------------------------------------------------------

                    goalsTableState.currentPage = 1;
                    updateGoals();
                });
            }

            // Category Toggle Logic
            const btnPepsico = document.getElementById('goals-category-pepsico-btn');
            const btnElmaChips = document.getElementById('goals-category-elmachips-btn');
            const btnFoods = document.getElementById('goals-category-foods-btn');
            const btnSummary = document.getElementById('goals-category-summary-btn');
            const subTabsPepsico = document.getElementById('goals-sub-tabs-pepsico');
            const subTabsElmaChips = document.getElementById('goals-sub-tabs-elmachips');
            const subTabsFoods = document.getElementById('goals-sub-tabs-foods');

            // Containers
            const goalsMainContainer = document.getElementById('goals-input-cards');
            const goalsTableContainer = document.getElementById('goals-table-container'); // Main table wrapper
            const goalsFiltersContainer = document.querySelector('#goals-gv-content > div.mb-4'); // Filters wrapper
            const goalsSummaryContainer = document.getElementById('goals-summary-content');

            // Filter Wrappers
            const wrapperSupervisor = document.getElementById('goals-gv-supervisor-filter-wrapper');
            const wrapperSeller = document.getElementById('goals-gv-seller-filter-wrapper');
            const wrapperCodCli = document.getElementById('goals-gv-codcli-filter-wrapper');

            const toggleGoalsView = (view) => {
                // Reset Buttons
                [btnPepsico, btnElmaChips, btnFoods, btnSummary].forEach(btn => {
                    if (btn) {
                        btn.classList.remove('bg-[#0d9488]', 'text-white', 'shadow-lg', 'border-teal-500/50');
                        btn.classList.add('bg-[#334155]', 'text-slate-400', 'border-slate-700');
                    }
                });

                // Hide All Sub-tabs
                if(subTabsPepsico) subTabsPepsico.classList.add('hidden');
                if(subTabsElmaChips) subTabsElmaChips.classList.add('hidden');
                if(subTabsFoods) subTabsFoods.classList.add('hidden');

                // Toggle Content
                if (view === 'summary') {
                    if(btnSummary) {
                        btnSummary.classList.remove('bg-[#334155]', 'text-slate-400', 'border-slate-700');
                        btnSummary.classList.add('bg-[#0d9488]', 'text-white', 'shadow-lg', 'border-teal-500/50');
                    }
                    if(goalsSummaryContainer) goalsSummaryContainer.classList.remove('hidden');
                    if(goalsMainContainer) goalsMainContainer.classList.add('hidden');
                    if(goalsTableContainer) goalsTableContainer.classList.add('hidden');

                    // HIDE Main Filters Container completely
                    if(goalsFiltersContainer) goalsFiltersContainer.classList.add('hidden');

                    // Ensure Summary Filters are initialized/visible (they are inside summary container)
                    updateGoalsSummaryView();
                } else {
                    // Show Main Content
                    if(goalsSummaryContainer) goalsSummaryContainer.classList.add('hidden');
                    if(goalsMainContainer) goalsMainContainer.classList.remove('hidden');
                    if(goalsTableContainer) goalsTableContainer.classList.remove('hidden');

                    // SHOW Main Filters Container and all wrappers
                    if(goalsFiltersContainer) goalsFiltersContainer.classList.remove('hidden');
                    if(wrapperSupervisor) wrapperSupervisor.classList.remove('hidden');
                    if(wrapperSeller) wrapperSeller.classList.remove('hidden');
                    if(wrapperCodCli) wrapperCodCli.classList.remove('hidden');

                    if (view === 'pepsico') {
                        if(btnPepsico) {
                            btnPepsico.classList.remove('bg-[#334155]', 'text-slate-400', 'border-slate-700');
                            btnPepsico.classList.add('bg-[#0d9488]', 'text-white', 'shadow-lg', 'border-teal-500/50');
                        }
                        if(subTabsPepsico) subTabsPepsico.classList.remove('hidden');
                        const firstTab = subTabsPepsico.querySelector('.goals-sub-tab');
                        if (firstTab) firstTab.click();
                    } else if (view === 'elmachips') {
                        if(btnElmaChips) {
                            btnElmaChips.classList.remove('bg-[#334155]', 'text-slate-400', 'border-slate-700');
                            btnElmaChips.classList.add('bg-[#0d9488]', 'text-white', 'shadow-lg', 'border-teal-500/50');
                        }
                        if(subTabsElmaChips) subTabsElmaChips.classList.remove('hidden');

                        // Select First Tab of Elma Chips
                        const firstTab = subTabsElmaChips.querySelector('.goals-sub-tab');
                        if (firstTab) firstTab.click();
                    } else if (view === 'foods') {
                        if(btnFoods) {
                            btnFoods.classList.remove('bg-[#334155]', 'text-slate-400', 'border-slate-700');
                            btnFoods.classList.add('bg-[#0d9488]', 'text-white', 'shadow-lg', 'border-teal-500/50');
                        }
                        if(subTabsFoods) subTabsFoods.classList.remove('hidden');

                        // Select First Tab of Foods
                        const firstTab = subTabsFoods.querySelector('.goals-sub-tab');
                        if (firstTab) firstTab.click();
                    }
                }
            };

            if (btnPepsico && btnElmaChips && btnFoods && btnSummary) {
                btnPepsico.addEventListener('click', () => toggleGoalsView('pepsico'));
                btnElmaChips.addEventListener('click', () => toggleGoalsView('elmachips'));
                btnFoods.addEventListener('click', () => toggleGoalsView('foods'));
                btnSummary.addEventListener('click', () => toggleGoalsView('summary'));
            }

            // Tab Switching
            document.getElementById('goals-tabs').addEventListener('click', (e) => {
                if (e.target.tagName === 'BUTTON') {
                    const tab = e.target.dataset.tab;
                    document.querySelectorAll('#goals-tabs button').forEach(btn => {
                        btn.classList.remove('border-teal-500', 'text-teal-500', 'active');
                        btn.classList.add('border-transparent', 'hover:text-slate-300', 'hover:border-slate-300', 'text-slate-400');
                    });
                    e.target.classList.remove('border-transparent', 'hover:text-slate-300', 'hover:border-slate-300', 'text-slate-400');
                    e.target.classList.add('border-teal-500', 'text-teal-500', 'active');

                    if (tab === 'gv') {
                        goalsGvContent.classList.remove('hidden');
                        goalsSvContent.classList.add('hidden');
                        updateGoals(); // Refresh GV view
                    } else if (tab === 'sv') {
                        goalsGvContent.classList.add('hidden');
                        goalsSvContent.classList.remove('hidden');
                        // Initialize SV filters if needed - Wait, updateSupervisorFilter should be called first or on load
                        // But we want to refresh data
                        updateGoalsSvView();
                    }
                }
            });

            // SV Sub-tabs Logic and Toggle Logic REMOVED (Replaced by Single Table View)

            // GV Filters
            goalsGvSupervisorFilterBtn.addEventListener('click', () => goalsGvSupervisorFilterDropdown.classList.toggle('hidden'));
            goalsGvSupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedGoalsGvSupervisors.push(value);
                    else selectedGoalsGvSupervisors = selectedGoalsGvSupervisors.filter(s => s !== value);

                    selectedGoalsGvSupervisors = updateSupervisorFilter(goalsGvSupervisorFilterDropdown, goalsGvSupervisorFilterText, selectedGoalsGvSupervisors, allSalesData, true);

                    // Reset sellers or keep intersection?
                    // Standard: Update Seller Options based on Supervisor
                    selectedGoalsGvSellers = [];
                    selectedGoalsGvSellers = updateSellerFilter(selectedGoalsGvSupervisors, goalsGvSellerFilterDropdown, goalsGvSellerFilterText, selectedGoalsGvSellers, allSalesData);

                    updateGoals();
                }
            });

            goalsGvSellerFilterBtn.addEventListener('click', () => goalsGvSellerFilterDropdown.classList.toggle('hidden'));
            goalsGvSellerFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedGoalsGvSellers.push(value);
                    else selectedGoalsGvSellers = selectedGoalsGvSellers.filter(s => s !== value);

                    selectedGoalsGvSellers = updateSellerFilter(selectedGoalsGvSupervisors, goalsGvSellerFilterDropdown, goalsGvSellerFilterText, selectedGoalsGvSellers, allSalesData, true);

                    updateGoals();
                }
            });

            goalsGvCodcliFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[^0-9]/g, '');
                debouncedUpdateGoals();
            });

            // --- Summary View Independent Filters ---
            const goalsSummarySupervisorFilterBtn = document.getElementById('goals-summary-supervisor-filter-btn');
            const goalsSummarySupervisorFilterDropdown = document.getElementById('goals-summary-supervisor-filter-dropdown');
            const clearGoalsSummaryFiltersBtn = document.getElementById('clear-goals-summary-filters-btn');

            if (goalsSummarySupervisorFilterBtn && goalsSummarySupervisorFilterDropdown) {
                goalsSummarySupervisorFilterBtn.addEventListener('click', () => goalsSummarySupervisorFilterDropdown.classList.toggle('hidden'));

                goalsSummarySupervisorFilterDropdown.addEventListener('change', (e) => {
                    if (e.target.type === 'checkbox') {
                        const { value, checked } = e.target;
                        if (checked) selectedGoalsSummarySupervisors.push(value);
                        else selectedGoalsSummarySupervisors = selectedGoalsSummarySupervisors.filter(s => s !== value);

                        updateSupervisorFilter(goalsSummarySupervisorFilterDropdown, document.getElementById('goals-summary-supervisor-filter-text'), selectedGoalsSummarySupervisors, allSalesData, true);
                        updateGoalsSummaryView();
                    }
                });
            }

            if (clearGoalsSummaryFiltersBtn) {
                clearGoalsSummaryFiltersBtn.addEventListener('click', () => {
                    selectedGoalsSummarySupervisors = [];
                    updateSupervisorFilter(goalsSummarySupervisorFilterDropdown, document.getElementById('goals-summary-supervisor-filter-text'), selectedGoalsSummarySupervisors, allSalesData, true);
                    updateGoalsSummaryView();
                });
            }

            // Close dropdowns on outside click
            document.addEventListener('click', (e) => {
                if (goalsSummarySupervisorFilterBtn && goalsSummarySupervisorFilterDropdown) {
                    if (!goalsSummarySupervisorFilterBtn.contains(e.target) && !goalsSummarySupervisorFilterDropdown.contains(e.target)) {
                        goalsSummarySupervisorFilterDropdown.classList.add('hidden');
                    }
                }
            });

            const btnDistributeFat = document.getElementById('btn-distribute-fat');
            if (btnDistributeFat) {
                btnDistributeFat.addEventListener('click', () => {
                    const filterDesc = getFilterDescription();
                    const val = document.getElementById('goal-global-fat').value;
                    showConfirmationModal(`Você deseja inserir esta meta de Faturamento (${val}) para: ${filterDesc}?`, () => {
                        distributeGoals('fat');
                    });
                });
            }

            const btnDistributeVol = document.getElementById('btn-distribute-vol');
            if (btnDistributeVol) {
                btnDistributeVol.addEventListener('click', () => {
                    const filterDesc = getFilterDescription();
                    const val = document.getElementById('goal-global-vol').value;
                    showConfirmationModal(`Você deseja inserir esta meta de Volume (${val}) para: ${filterDesc}?`, () => {
                        distributeGoals('vol');
                    });
                });
            }

            const btnDistributeMixSalty = document.getElementById('btn-distribute-mix-salty');
            if (btnDistributeMixSalty) {
                btnDistributeMixSalty.addEventListener('click', () => {
                    const sellerName = selectedGoalsGvSellers[0];
                    if (!sellerName) return;
                    const valStr = document.getElementById('goal-global-mix-salty').value;
                    showConfirmationModal(`Confirmar ajuste de Meta Mix Salty para ${valStr} (Vendedor: ${getFirstName(sellerName)})?`, () => {
                        const val = parseFloat(valStr.replace(/\./g, '').replace(',', '.')) || 0;

                        // Calculate Natural Base again to store Delta
                        // We need the natural base for THIS seller to calculate delta (Input - Natural)
                        // It's cleaner if updateGoalsView handles this logic directly in the listener,
                        // OR we store the natural base somewhere.
                        // For simplicity, let's just trigger a custom event or call a handler that has access to context.
                        // Actually, since we need "Natural" value which varies by filter context, it's safer to handle this
                        // inside updateGoalsView where metrics are available, OR make this listener smart enough.

                        // Let's use the adjustment map directly.
                        // But we don't know the natural value here easily without recalculating.
                        // Solution: The input value IS the target. We want to store the adjustment.
                        // Adjustment = Target - Natural.

                        // Let's implement a specific helper function "saveMixAdjustment" that recalculates natural base for single seller.
                        saveMixAdjustment('salty', val, sellerName);
                    });
                });
            }

            const btnDistributeMixFoods = document.getElementById('btn-distribute-mix-foods');
            if (btnDistributeMixFoods) {
                btnDistributeMixFoods.addEventListener('click', () => {
                    const sellerName = selectedGoalsGvSellers[0];
                    if (!sellerName) return;
                    const valStr = document.getElementById('goal-global-mix-foods').value;
                    showConfirmationModal(`Confirmar ajuste de Meta Mix Foods para ${valStr} (Vendedor: ${getFirstName(sellerName)})?`, () => {
                        const val = parseFloat(valStr.replace(/\./g, '').replace(',', '.')) || 0;
                        saveMixAdjustment('foods', val, sellerName);
                    });
                });
            }

            // Add Input Listeners for Real-time State Updates
            const fatInput = document.getElementById('goal-global-fat');
            const volInput = document.getElementById('goal-global-vol');

            // REMOVED: Automatic update on change/input to prevent overwriting user input before distribution.
            // Values are now read directly from the input when the "Distribute" button is clicked.

            clearGoalsGvFiltersBtn.addEventListener('click', () => { resetGoalsGvFilters(); markDirty('goals'); });

            // SV Filters
            goalsSvSupervisorFilterBtn.addEventListener('click', () => goalsSvSupervisorFilterDropdown.classList.toggle('hidden'));
            goalsSvSupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedGoalsSvSupervisors.push(value);
                    else selectedGoalsSvSupervisors = selectedGoalsSvSupervisors.filter(s => s !== value);

                    selectedGoalsSvSupervisors = updateSupervisorFilter(goalsSvSupervisorFilterDropdown, goalsSvSupervisorFilterText, selectedGoalsSvSupervisors, allSalesData, true);
                    updateGoalsSvView();
                }
            });

            document.getElementById('goals-sv-export-xlsx-btn').addEventListener('click', exportGoalsSvXLSX);

            document.addEventListener('click', (e) => {
                if (!goalsGvSupervisorFilterBtn.contains(e.target) && !goalsGvSupervisorFilterDropdown.contains(e.target)) goalsGvSupervisorFilterDropdown.classList.add('hidden');
                if (!goalsGvSellerFilterBtn.contains(e.target) && !goalsGvSellerFilterDropdown.contains(e.target)) goalsGvSellerFilterDropdown.classList.add('hidden');
                if (!goalsSvSupervisorFilterBtn.contains(e.target) && !goalsSvSupervisorFilterDropdown.contains(e.target)) goalsSvSupervisorFilterDropdown.classList.add('hidden');
            });

            document.getElementById('goals-prev-page-btn').addEventListener('click', () => {
                if (goalsTableState.currentPage > 1) {
                    goalsTableState.currentPage--;
                    updateGoalsView();
                }
            });
            document.getElementById('goals-next-page-btn').addEventListener('click', () => {
                if (goalsTableState.currentPage < goalsTableState.totalPages) {
                    goalsTableState.currentPage++;
                    updateGoalsView();
                }
            });


            const updateMix = () => {
                markDirty('mix');
                handleMixFilterChange();
            };

            document.getElementById('mix-supervisor-filter-btn').addEventListener('click', (e) => {
                document.getElementById('mix-supervisor-filter-dropdown').classList.toggle('hidden');
            });
            document.getElementById('mix-supervisor-filter-dropdown').addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedMixSupervisors.push(value);
                    else selectedMixSupervisors = selectedMixSupervisors.filter(s => s !== value);
                    selectedMixSellers = [];
                    updateMix();
                }
            });

            document.getElementById('mix-vendedor-filter-btn').addEventListener('click', (e) => {
                document.getElementById('mix-vendedor-filter-dropdown').classList.toggle('hidden');
            });
            document.getElementById('mix-vendedor-filter-dropdown').addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedMixSellers.push(value);
                    else selectedMixSellers = selectedMixSellers.filter(s => s !== value);
                    handleMixFilterChange({ skipFilter: 'seller' });
                    markDirty('mix');
                }
            });

            document.getElementById('mix-tipo-venda-filter-btn').addEventListener('click', (e) => {
                document.getElementById('mix-tipo-venda-filter-dropdown').classList.toggle('hidden');
            });
            document.getElementById('mix-tipo-venda-filter-dropdown').addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedMixTiposVenda.push(value);
                    else selectedMixTiposVenda = selectedMixTiposVenda.filter(s => s !== value);
                    handleMixFilterChange({ skipFilter: 'tipoVenda' });
                    markDirty('mix');
                }
            });

            document.getElementById('mix-filial-filter').addEventListener('change', updateMix);

            const mixCityFilter = document.getElementById('mix-city-filter');
            const mixCitySuggestions = document.getElementById('mix-city-suggestions');

            mixCityFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                const { clients } = getMixFilteredData({ excludeFilter: 'city' });
                mixCitySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(mixCityFilter, mixCitySuggestions, clients);
            });
            mixCityFilter.addEventListener('focus', () => {
                const { clients } = getMixFilteredData({ excludeFilter: 'city' });
                mixCitySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(mixCityFilter, mixCitySuggestions, clients);
            });
            mixCityFilter.addEventListener('blur', () => setTimeout(() => mixCitySuggestions.classList.add('hidden'), 150));
            mixCityFilter.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    mixCitySuggestions.classList.add('hidden', 'manual-hide');
                    updateMix();
                    e.target.blur();
                }
            });
            mixCitySuggestions.addEventListener('click', (e) => {
                if (e.target.tagName === 'DIV') {
                    mixCityFilter.value = e.target.textContent;
                    mixCitySuggestions.classList.add('hidden');
                    updateMix();
                }
            });

            document.getElementById('mix-com-rede-btn').addEventListener('click', () => {
                document.getElementById('mix-rede-filter-dropdown').classList.toggle('hidden');
            });
            document.getElementById('mix-rede-group-container').addEventListener('click', (e) => {
                if(e.target.closest('button')) {
                    const button = e.target.closest('button');
                    mixRedeGroupFilter = button.dataset.group;
                    document.getElementById('mix-rede-group-container').querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    button.classList.add('active');
                    if (mixRedeGroupFilter !== 'com_rede') {
                        document.getElementById('mix-rede-filter-dropdown').classList.add('hidden');
                        selectedMixRedes = [];
                    }
                    handleMixFilterChange();
                }
            });
            document.getElementById('mix-rede-filter-dropdown').addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedMixRedes.push(value);
                    else selectedMixRedes = selectedMixRedes.filter(r => r !== value);

                    mixRedeGroupFilter = 'com_rede';
                    document.getElementById('mix-rede-group-container').querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    document.getElementById('mix-com-rede-btn').classList.add('active');

                    handleMixFilterChange({ skipFilter: 'rede' });
                }
            });

            document.getElementById('clear-mix-filters-btn').addEventListener('click', () => { resetMixFilters(); markDirty('mix'); });
            document.getElementById('export-mix-pdf-btn').addEventListener('click', exportMixPDF);

            document.getElementById('mix-kpi-toggle').addEventListener('change', (e) => {
                mixKpiMode = e.target.checked ? 'atendidos' : 'total';
                markDirty('mix');
                updateMixView();
            });

            document.getElementById('mix-prev-page-btn').addEventListener('click', () => {
                if (mixTableState.currentPage > 1) {
                    mixTableState.currentPage--;
                    updateMixView();
                }
            });
            document.getElementById('mix-next-page-btn').addEventListener('click', () => {
                if (mixTableState.currentPage < mixTableState.totalPages) {
                    mixTableState.currentPage++;
                    updateMixView();
                }
            });

            document.addEventListener('click', (e) => {
                // Close Mix Dropdowns
                if (!document.getElementById('mix-supervisor-filter-btn').contains(e.target) && !document.getElementById('mix-supervisor-filter-dropdown').contains(e.target)) document.getElementById('mix-supervisor-filter-dropdown').classList.add('hidden');
                if (!document.getElementById('mix-vendedor-filter-btn').contains(e.target) && !document.getElementById('mix-vendedor-filter-dropdown').contains(e.target)) document.getElementById('mix-vendedor-filter-dropdown').classList.add('hidden');
                if (!document.getElementById('mix-tipo-venda-filter-btn').contains(e.target) && !document.getElementById('mix-tipo-venda-filter-dropdown').contains(e.target)) document.getElementById('mix-tipo-venda-filter-dropdown').classList.add('hidden');
                if (!document.getElementById('mix-com-rede-btn').contains(e.target) && !document.getElementById('mix-rede-filter-dropdown').contains(e.target)) document.getElementById('mix-rede-filter-dropdown').classList.add('hidden');

                if (!document.getElementById('weekly-supervisor-filter-btn').contains(e.target) && !document.getElementById('weekly-supervisor-filter-dropdown').contains(e.target)) document.getElementById('weekly-supervisor-filter-dropdown').classList.add('hidden');
                if (!document.getElementById('weekly-vendedor-filter-btn').contains(e.target) && !document.getElementById('weekly-vendedor-filter-dropdown').contains(e.target)) document.getElementById('weekly-vendedor-filter-dropdown').classList.add('hidden');
            });

            // --- Coverage View Filters ---
            const updateCoverage = () => {
                markDirty('cobertura');
                handleCoverageFilterChange();
            };

            const debouncedHandleCoverageChange = debounce(updateCoverage, 400);

            coverageFilialFilter.addEventListener('change', updateCoverage);

            const coverageSupervisorFilterBtn = document.getElementById('coverage-supervisor-filter-btn');
            const coverageSupervisorFilterDropdown = document.getElementById('coverage-supervisor-filter-dropdown');
            coverageSupervisorFilterBtn.addEventListener('click', () => coverageSupervisorFilterDropdown.classList.toggle('hidden'));
            coverageSupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedCoverageSupervisors.push(value);
                    else selectedCoverageSupervisors = selectedCoverageSupervisors.filter(s => s !== value);

                    selectedCoverageSellers = [];
                    updateCoverage();
                }
            });

            coverageCityFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                const { clients } = getCoverageFilteredData({ excludeFilter: 'city' });
                coverageCitySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(coverageCityFilter, coverageCitySuggestions, clients);
            });
            coverageCityFilter.addEventListener('focus', () => {
                const { clients } = getCoverageFilteredData({ excludeFilter: 'city' });
                coverageCitySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(coverageCityFilter, coverageCitySuggestions, clients);
            });
            coverageCityFilter.addEventListener('blur', () => setTimeout(() => coverageCitySuggestions.classList.add('hidden'), 150));
            coverageCityFilter.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    coverageCitySuggestions.classList.add('hidden', 'manual-hide');
                    updateCoverage();
                    e.target.blur();
                }
            });
            coverageCitySuggestions.addEventListener('click', (e) => {
                if (e.target.tagName === 'DIV') {
                    coverageCityFilter.value = e.target.textContent;
                    coverageCitySuggestions.classList.add('hidden');
                    updateCoverage();
                }
            });

            coverageTipoVendaFilterBtn.addEventListener('click', () => coverageTipoVendaFilterDropdown.classList.toggle('hidden'));
            coverageTipoVendaFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedCoverageTiposVenda.includes(value)) selectedCoverageTiposVenda.push(value);
                    } else {
                        selectedCoverageTiposVenda = selectedCoverageTiposVenda.filter(s => s !== value);
                    }
                    updateCoverage();
                }
            });

            clearCoverageFiltersBtn.addEventListener('click', () => { resetCoverageFilters(); markDirty('cobertura'); });

            coverageVendedorFilterBtn.addEventListener('click', () => coverageVendedorFilterDropdown.classList.toggle('hidden'));
            coverageVendedorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedCoverageSellers.includes(value)) selectedCoverageSellers.push(value);
                    } else {
                        selectedCoverageSellers = selectedCoverageSellers.filter(s => s !== value);
                    }
                    markDirty('cobertura');
                    handleCoverageFilterChange({ skipFilter: 'seller' });
                }
            });

            coverageSupplierFilterBtn.addEventListener('click', () => coverageSupplierFilterDropdown.classList.toggle('hidden'));
            coverageSupplierFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox' && e.target.dataset.filterType === 'coverage') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedCoverageSuppliers.includes(value)) selectedCoverageSuppliers.push(value);
                    } else {
                        selectedCoverageSuppliers = selectedCoverageSuppliers.filter(s => s !== value);
                    }

                    markDirty('cobertura');
                    handleCoverageFilterChange({ skipFilter: 'supplier' });
                }
            });

            coverageProductFilterBtn.addEventListener('click', () => {
                const { sales, history } = getCoverageFilteredData({ excludeFilter: 'product' });
                selectedCoverageProducts = updateProductFilter(coverageProductFilterDropdown, coverageProductFilterText, selectedCoverageProducts, [...sales, ...history], 'coverage');
                coverageProductFilterDropdown.classList.toggle('hidden');
            });

            const debouncedCoverageProductUpdate = debounce(() => {
                 const { sales, history } = getCoverageFilteredData({ excludeFilter: 'product' });
                 selectedCoverageProducts = updateProductFilter(coverageProductFilterDropdown, coverageProductFilterText, selectedCoverageProducts, [...sales, ...history], 'coverage');
            }, 250);

            coverageProductFilterDropdown.addEventListener('input', (e) => {
                if (e.target.id === 'coverage-product-search-input') {
                    debouncedCoverageProductUpdate();
                }
            });

            coverageProductFilterDropdown.addEventListener('change', (e) => {
                if (e.target.dataset.filterType === 'coverage' && handleProductFilterChange(e, selectedCoverageProducts)) {
                    markDirty('cobertura');
                    handleCoverageFilterChange({ skipFilter: 'product' });
                }
            });

            const coverageUnitPriceInput = document.getElementById('coverage-unit-price-filter');
            if (coverageUnitPriceInput) {
                coverageUnitPriceInput.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter') {
                        updateCoverage();
                        e.target.blur();
                    }
                });
                coverageUnitPriceInput.addEventListener('blur', updateCoverage);
            }


            document.addEventListener('click', (e) => {
                if (!innovationsMonthVendedorFilterBtn.contains(e.target) && !innovationsMonthVendedorFilterDropdown.contains(e.target)) innovationsMonthVendedorFilterDropdown.classList.add('hidden');
                if (!innovationsMonthTipoVendaFilterBtn.contains(e.target) && !innovationsMonthTipoVendaFilterDropdown.contains(e.target)) innovationsMonthTipoVendaFilterDropdown.classList.add('hidden');
                if (!coverageVendedorFilterBtn.contains(e.target) && !coverageVendedorFilterDropdown.contains(e.target)) coverageVendedorFilterDropdown.classList.add('hidden');
                if (!coverageSupplierFilterBtn.contains(e.target) && !coverageSupplierFilterDropdown.contains(e.target)) coverageSupplierFilterDropdown.classList.add('hidden');
                if (!coverageProductFilterBtn.contains(e.target) && !coverageProductFilterDropdown.contains(e.target)) coverageProductFilterDropdown.classList.add('hidden');
                if (!coverageTipoVendaFilterBtn.contains(e.target) && !coverageTipoVendaFilterDropdown.contains(e.target)) coverageTipoVendaFilterDropdown.classList.add('hidden');
            });

            const stockWorkingDaysInput = document.getElementById('stock-working-days-input');
            if (stockWorkingDaysInput) {
                stockWorkingDaysInput.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        const value = parseInt(e.target.value);
                        if (!isNaN(value) && value > 0 && value <= maxWorkingDaysStock) {
                            customWorkingDaysStock = value;
                        } else {
                             e.target.value = customWorkingDaysStock;
                        }
                        handleStockFilterChange();
                        e.target.blur(); // <-- ADICIONADO: Remove o foco do input após pressionar Enter
                    }
                });
                stockWorkingDaysInput.addEventListener('blur', (e) => {
                    const value = parseInt(e.target.value);
                     if (isNaN(value) || value <= 0 || value > maxWorkingDaysStock) {
                         e.target.value = customWorkingDaysStock;
                    } else if (value !== customWorkingDaysStock) {

                        customWorkingDaysStock = value;
                        handleStockFilterChange();
                    }
                });
            }

            document.querySelectorAll('.stock-trend-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    stockTrendFilter = btn.dataset.trend;
                    document.querySelectorAll('.stock-trend-btn').forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    handleStockFilterChange();
                });
            });
        }

        initializeOptimizedDataStructures();
        calculateHistoricalBests(); // <-- MOVIDA PARA CIMA
        selectedMainSupervisors = updateSupervisorFilter(document.getElementById('supervisor-filter-dropdown'), document.getElementById('supervisor-filter-text'), selectedMainSupervisors, allSalesData);
        selectedMainSuppliers = updateSupplierFilter(document.getElementById('fornecedor-filter-dropdown'), document.getElementById('fornecedor-filter-text'), selectedMainSuppliers, [...allSalesData, ...allHistoryData], 'main');
        selectedCitySupervisors = updateSupervisorFilter(document.getElementById('city-supervisor-filter-dropdown'), document.getElementById('city-supervisor-filter-text'), selectedCitySupervisors, allSalesData);
        selectedStockSupervisors = updateSupervisorFilter(document.getElementById('stock-supervisor-filter-dropdown'), document.getElementById('stock-supervisor-filter-text'), selectedStockSupervisors, [...allSalesData, ...allHistoryData]);

        updateSellerFilter(selectedMainSupervisors, vendedorFilterDropdown, vendedorFilterText, selectedSellers, allSalesData);
        updateTipoVendaFilter(tipoVendaFilterDropdown, tipoVendaFilterText, selectedTiposVenda, allSalesData);
        updateSellerFilter(selectedCitySupervisors, cityVendedorFilterDropdown, cityVendedorFilterText, selectedCitySellers, allSalesData);

        // --- FIXED: Initialize Filters for Other Views ---
        // Coverage
        selectedCoverageSupervisors = updateSupervisorFilter(document.getElementById('coverage-supervisor-filter-dropdown'), document.getElementById('coverage-supervisor-filter-text'), selectedCoverageSupervisors, allSalesData);
        updateSellerFilter(selectedCoverageSupervisors, document.getElementById('coverage-vendedor-filter-dropdown'), document.getElementById('coverage-vendedor-filter-text'), selectedCoverageSellers, allSalesData);

        // Innovations
        selectedInnovationsSupervisors = updateSupervisorFilter(document.getElementById('innovations-month-supervisor-filter-dropdown'), document.getElementById('innovations-month-supervisor-filter-text'), selectedInnovationsSupervisors, allSalesData, true);
        updateSellerFilter(selectedInnovationsSupervisors, document.getElementById('innovations-month-vendedor-filter-dropdown'), document.getElementById('innovations-month-vendedor-filter-text'), selectedInnovationsMonthSellers, allSalesData);

        // Mix
        selectedMixSupervisors = updateSupervisorFilter(document.getElementById('mix-supervisor-filter-dropdown'), document.getElementById('mix-supervisor-filter-text'), selectedMixSupervisors, allSalesData);
        updateSellerFilter(selectedMixSupervisors, document.getElementById('mix-vendedor-filter-dropdown'), document.getElementById('mix-vendedor-filter-text'), selectedMixSellers, allSalesData);
        // -------------------------------------------------

        updateRedeFilter(mainRedeFilterDropdown, mainComRedeBtnText, selectedMainRedes, allClientsData);
        updateRedeFilter(cityRedeFilterDropdown, cityComRedeBtnText, selectedCityRedes, allClientsData);
        updateRedeFilter(comparisonRedeFilterDropdown, comparisonComRedeBtnText, selectedComparisonRedes, allClientsData);
        updateRedeFilter(stockRedeFilterDropdown, stockComRedeBtnText, selectedStockRedes, allClientsData, 'Com Rede');

        // Initial Population for Goals Filters
        selectedGoalsGvSupervisors = updateSupervisorFilter(goalsGvSupervisorFilterDropdown, goalsGvSupervisorFilterText, selectedGoalsGvSupervisors, allSalesData);
        selectedGoalsGvSellers = updateSellerFilter(selectedGoalsGvSupervisors, goalsGvSellerFilterDropdown, goalsGvSellerFilterText, selectedGoalsGvSellers, allSalesData);
        // Initialize SV Supervisor filter just in case
        selectedGoalsSvSupervisors = updateSupervisorFilter(goalsSvSupervisorFilterDropdown, goalsSvSupervisorFilterText, selectedGoalsSvSupervisors, allSalesData);
        // Initialize Summary Supervisor Filter
        if(document.getElementById('goals-summary-supervisor-filter-dropdown')) {
            selectedGoalsSummarySupervisors = updateSupervisorFilter(document.getElementById('goals-summary-supervisor-filter-dropdown'), document.getElementById('goals-summary-supervisor-filter-text'), selectedGoalsSummarySupervisors, allSalesData);
        }


        updateAllComparisonFilters();

        updateStockSellerFilter();
        updateStockSupplierFilter();
        updateStockProductFilter();
        updateStockCitySuggestions([...allSalesData, ...allHistoryData]);

        initializeRedeFilters();
        setupEventListeners();

        // Assegura que os dados históricos estão prontos antes da primeira renderização
        calculateHistoricalBests();

        // --- Initialization for Goals Metrics ---
        calculateGoalsMetrics();
        // Initialize Targets with Defaults (Pre-fill)
        for (const key in globalGoalsMetrics) {
            if (!goalsTargets[key]) {
                goalsTargets[key] = { fat: 0, vol: 0 };
            }
            if (globalGoalsMetrics[key]) {
                goalsTargets[key].fat = globalGoalsMetrics[key].prevFat;
                goalsTargets[key].vol = globalGoalsMetrics[key].prevVol;
            }
        }
        // Initialize Inputs and Refs for Default Tab (ELMA_ALL)
        const defGoalsMetric = globalGoalsMetrics['ELMA_ALL'];
        if (defGoalsMetric) {
            const fi = document.getElementById('goal-global-fat');
            const vi = document.getElementById('goal-global-vol');
            if (fi) fi.value = defGoalsMetric.prevFat.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            if (vi) vi.value = defGoalsMetric.prevVol.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 });

            const refAvgFat = document.getElementById('ref-avg-fat');
            const refPrevFat = document.getElementById('ref-prev-fat');
            const refAvgVol = document.getElementById('ref-avg-vol');
            const refPrevVol = document.getElementById('ref-prev-vol');
            const refAvgClients = document.getElementById('ref-avg-clients');
            const refPrevClients = document.getElementById('ref-prev-clients');
        }
        // ----------------------------------------

        const urlParams = new URLSearchParams(window.location.search);
        const targetPage = urlParams.get('ir_para');

        if (targetPage) {
            navigateTo(targetPage);
        } else {
            navigateTo('dashboard');
        }
        renderTable(aggregatedOrders);
