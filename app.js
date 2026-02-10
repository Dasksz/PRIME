(function() {
        const embeddedData = window.embeddedData;

        // --- CONFIGURATION ---
        const SUPPLIER_CONFIG = {
            inference: {
                triggerKeywords: ['PEPSICO'],
                matchValue: 'PEPSICO',
                defaultValue: 'MULTIMARCAS'
            },
            metaRealizado: {
                requiredPasta: 'PEPSICO'
            }
        };

        function resolveSupplierPasta(rowPasta, fornecedorName) {
            if (!rowPasta || rowPasta === '0' || rowPasta === '00' || rowPasta === 'N/A') {
                const rawFornecedor = String(fornecedorName || '').toUpperCase();
                const match = SUPPLIER_CONFIG.inference.triggerKeywords.some(k => rawFornecedor.includes(k));
                return match ? SUPPLIER_CONFIG.inference.matchValue : SUPPLIER_CONFIG.inference.defaultValue;
            }
            return rowPasta;
        }

        const GARBAGE_SELLER_KEYWORDS = ['TOTAL', 'GERAL', 'SUPERVISOR', 'BALCAO'];
        const GARBAGE_SELLER_EXACT = ['INATIVOS', 'N/A'];

        function isGarbageSeller(name) {
            if (!name) return true;
            // Normalize: Remove accents (NFD + Replace), Uppercase, Trim
            const upper = String(name).normalize('NFD').replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
            if (GARBAGE_SELLER_EXACT.includes(upper)) return true;
            return GARBAGE_SELLER_KEYWORDS.some(k => upper.includes(k));
        }

        let metaRealizadoDataForExport = { sellers: [], clients: [], weeks: [] };
        const dateCache = new Map();

        // Helper to normalize keys (remove leading zeros) to ensure consistent joins
        const normalizeKey = (key) => {
            if (!key) return '';
            const s = String(key).trim();
            if (/^\d+$/.test(s)) {
                return String(parseInt(s, 10));
            }
            return s;
        };

        // --- HELPER: Alternative Sales Type Logic ---
        function isAlternativeMode(selectedTypes) {
            if (!selectedTypes || selectedTypes.length === 0) return false;
            // "Alternative Mode" is active ONLY if we have selected types AND none of them are 1 or 9.
            return !selectedTypes.includes('1') && !selectedTypes.includes('9');
        }

        function getValueForSale(sale, selectedTypes) {
            if (isAlternativeMode(selectedTypes)) {
                return (Number(sale.VLBONIFIC) || 0) + (Number(sale.VLVENDA) || 0);
            }
            return Number(sale.VLVENDA) || 0;
        }
        // ---------------------------------------------

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

            // Se for uma string, verifica o cache
            if (typeof dateString === 'string') {
                const cached = dateCache.get(dateString);
                if (cached !== undefined) {
                    return cached !== null ? new Date(cached) : null;
                }
            } else if (typeof dateString === 'number') {
                // Se for um número (formato Excel ou Timestamp)
                // Excel Serial Date (approx < 50000 for current dates, Timestamp is > 1000000000000)
                if (dateString < 100000) return new Date(Math.round((dateString - 25569) * 86400 * 1000));
                // Timestamp
                return new Date(dateString);
            } else {
                return null;
            }

            let result = null;

            // Tentativa de parse para 'YYYY-MM-DDTHH:mm:ss.sssZ' ou 'YYYY-MM-DD'
            // O construtor do Date já lida bem com isso, mas vamos garantir o UTC.
            if (dateString.includes('T') || dateString.includes('-')) {
                 // Adiciona 'Z' se não tiver informação de fuso horário para forçar UTC
                const isoString = dateString.endsWith('Z') ? dateString : dateString + 'Z';
                const isoDate = new Date(isoString);
                if (!isNaN(isoDate.getTime())) {
                    result = isoDate;
                }
            }

            // Tentativa de parse para 'DD/MM/YYYY'
            if (!result && dateString.length === 10 && dateString.charAt(2) === '/' && dateString.charAt(5) === '/') {
                const [day, month, year] = dateString.split('/');
                if (year && month && day && year.length === 4) {
                    // Cria a data em UTC para evitar problemas de fuso horário
                    const utcDate = new Date(Date.UTC(parseInt(year), parseInt(month) - 1, parseInt(day)));
                    if (!isNaN(utcDate.getTime())) {
                        result = utcDate;
                    }
                }
            }

            // Fallback para outros formatos que o `new Date()` consegue interpretar
            if (!result) {
                const genericDate = new Date(dateString);
                if (!isNaN(genericDate.getTime())) {
                    result = genericDate;
                }
            }

            // Armazena no cache (apenas strings)
            dateCache.set(dateString, result !== null ? result.getTime() : null);

            return result;
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
                // Process in batches (50 items) to reduce overhead of time checks and loop condition
                const BATCH_SIZE = 50;

                while (index < total) {
                    const limit = Math.min(index + BATCH_SIZE, total);

                    if (isColumnar) {
                        for (; index < limit; index++) {
                            processItemFn(items.get(index), index);
                        }
                    } else {
                        for (; index < limit; index++) {
                            processItemFn(items[index], index);
                        }
                    }

                    if (performance.now() - start >= 12) { // Check budget (12ms)
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

        function normalizePastaInData(dataset) {
            // Cache for supplier -> pasta mapping to avoid repeated config lookups
            const supplierCache = new Map();

            const getResolvedPasta = (currentPasta, fornecedor) => {
                // If we already have a valid pasta, return it
                if (currentPasta && currentPasta !== '0' && currentPasta !== '00' && currentPasta !== 'N/A') {
                    return currentPasta;
                }

                // Check cache
                const key = String(fornecedor || '').toUpperCase();
                if (supplierCache.has(key)) {
                    return supplierCache.get(key);
                }

                // Calculate and cache
                const resolved = resolveSupplierPasta(currentPasta, fornecedor);
                supplierCache.set(key, resolved);
                return resolved;
            };

            if (dataset instanceof ColumnarDataset) {
                const data = dataset._data;
                const len = dataset.length;

                // Ensure columns exist or gracefully handle
                const pastaCol = data['OBSERVACAOFOR'] || new Array(len).fill(null);
                const supplierCol = data['FORNECEDOR'] || [];

                // If we created a new array for pasta, we must attach it to _data
                if (!data['OBSERVACAOFOR']) {
                    data['OBSERVACAOFOR'] = pastaCol;
                    if (dataset.columns && !dataset.columns.includes('OBSERVACAOFOR')) {
                        dataset.columns.push('OBSERVACAOFOR');
                    }
                }

                for (let i = 0; i < len; i++) {
                    const originalPasta = pastaCol[i];
                    const fornecedor = supplierCol[i];
                    const newPasta = getResolvedPasta(originalPasta, fornecedor);

                    // Only update if changed (optimization)
                    if (newPasta !== originalPasta) {
                        pastaCol[i] = newPasta;
                    }
                }
            } else if (Array.isArray(dataset)) {
                 for (let i = 0; i < dataset.length; i++) {
                    const item = dataset[i];
                    const originalPasta = item['OBSERVACAOFOR'];
                    const fornecedor = item['FORNECEDOR'];
                    const newPasta = getResolvedPasta(originalPasta, fornecedor);

                    if (newPasta !== originalPasta) {
                        item['OBSERVACAOFOR'] = newPasta;
                    }
                 }
            }
        }

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

        // --- PRE-PROCESSING: Normalize PASTA once to avoid repeated logic in loops ---
        normalizePastaInData(allSalesData);
        normalizePastaInData(allHistoryData);
        // -----------------------------------------------------------------------------

        let aggregatedOrders = embeddedData.byOrder;
        const stockData05 = new Map(Object.entries(embeddedData.stockMap05 || {}));
        const stockData08 = new Map(Object.entries(embeddedData.stockMap08 || {}));
        const innovationsMonthData = embeddedData.innovationsMonth;
        let clientMapForKPIs;
        if (allClientsData instanceof ColumnarDataset) {
            clientMapForKPIs = new IndexMap(allClientsData);
            // Optimization: Try to access raw column to avoid Proxy creation loop.
            // Accessing private _data is necessary for performance here to bypass the get() Proxy overhead.
            const rawData = allClientsData._data;
            let idCol = null;
            if (rawData) {
                if (rawData['Código']) idCol = rawData['Código'];
                else if (rawData['codigo_cliente']) idCol = rawData['codigo_cliente'];
            }

            // Check if idCol is an Array or TypedArray (has length and integer indexing)
            if (idCol && typeof idCol.length === 'number') {
                for (let i = 0; i < allClientsData.length; i++) {
                    clientMapForKPIs.set(normalizeKey(idCol[i]), i);
                }
            } else {
                for (let i = 0; i < allClientsData.length; i++) {
                    const c = allClientsData.get(i);
                    clientMapForKPIs.set(normalizeKey(c['Código'] || c['codigo_cliente']), i);
                }
            }
        } else {
            clientMapForKPIs = new Map();
            for (let i = 0; i < allClientsData.length; i++) {
                const c = allClientsData[i];
                clientMapForKPIs.set(normalizeKey(c['Código'] || c['codigo_cliente']), c);
            }
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
        let currentClientMixStatus = new Map(); // Map<ClientCode, {elma: bool, foods: bool}>
        let areMarkersGenerated = false;
        let cityMapJobId = 0;
        let isCityMapCalculating = false;

        // Load cached coordinates from embeddedData
        if (embeddedData.clientCoordinates) {
            // Robust check: Handle both Array and Object (if keys are used)
            const coords = Array.isArray(embeddedData.clientCoordinates) ? embeddedData.clientCoordinates : Object.values(embeddedData.clientCoordinates);
            coords.forEach(c => {
                let code = String(c.client_code).trim();
                // Normalize keys (remove leading zeros)
                if (/^\d+$/.test(code)) {
                    code = String(parseInt(code, 10));
                }

                clientCoordinatesMap.set(code, {
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
                radius: 15,
                blur: 15,
                maxZoom: 10,
                minOpacity: 0.05, // More transparent
                max: 20.0, // Increased max to prevent saturation and allow transparency
                gradient: {
                    0.2: 'rgba(0, 0, 255, 0.35)',
                    0.5: 'rgba(0, 255, 0, 0.35)',
                    1.0: 'rgba(255, 0, 0, 0.35)'
                }
            }).addTo(leafletMap);

            // Initialize Markers Layer (Hidden by default, shown on zoom)
            clientMarkersLayer = L.layerGroup();

            leafletMap.on('zoomend', () => {
                const zoom = leafletMap.getZoom();

                // Dynamic Heatmap Settings based on Zoom
                if (heatLayer) {
                    if (zoom >= 14) {
                        // High Zoom: Hide Heatmap completely, show Markers
                        if (leafletMap.hasLayer(heatLayer)) {
                            leafletMap.removeLayer(heatLayer);
                        }
                    } else {
                        // Low/Mid Zoom: Show Heatmap, Hide Markers (handled by updateMarkersVisibility)
                        if (!leafletMap.hasLayer(heatLayer)) {
                            leafletMap.addLayer(heatLayer);
                        }

                        let newOptions = {};
                        if (zoom >= 12) {
                            // Transition Zoom
                            newOptions = {
                                radius: 12,
                                blur: 12,
                                max: 5.0,
                                minOpacity: 0.2
                            };
                        } else {
                            // Low Zoom: Heatmap clouds (Current settings for density)
                            newOptions = {
                                radius: 15,
                                blur: 15,
                                max: 20.0, // High max to prevent saturation in clusters
                                minOpacity: 0.05
                            };
                        }
                        heatLayer.setOptions(newOptions);
                    }
                }

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

        function buildAddress(client, level) {
            // Priority: Key 'Endereço Comercial' (from init.js map) > lowercase > UPPERCASE
            const endereco = client['Endereço Comercial'] || client.endereco || client.ENDERECO || '';
            const numero = client.numero || client.NUMERO || '';
            const bairro = client.bairro || client.BAIRRO || '';
            const cidade = client.cidade || client.CIDADE || '';
            const nome = client.nomeCliente || client.nome || '';

            const parts = [];
            const isValid = (s) => s && s !== 'N/A' && s !== '0' && String(s).toUpperCase() !== 'S/N' && String(s).trim() !== '';

            // Level 0 (POI - Business Name): Name + Bairro + City
            if (level === 0) {
                if(isValid(nome)) parts.push(nome);
                if(isValid(bairro)) parts.push(bairro);
                if(isValid(cidade)) parts.push(cidade);
            }
            // Level 1 (Address Full - Street + Number): Street + Number + Bairro + City
            else if (level === 1) {
                if(isValid(endereco)) parts.push(endereco);
                if(isValid(numero)) parts.push(numero);
                if(isValid(bairro)) parts.push(bairro);
                if(isValid(cidade)) parts.push(cidade);
            }
            // Level 2 (Street): Street + Bairro + City
            else if (level === 2) {
                if(isValid(endereco)) parts.push(endereco);
                if(isValid(bairro)) parts.push(bairro);
                if(isValid(cidade)) parts.push(cidade);
            }
            // Level 3 (Neighborhood): Bairro + City
            else if (level === 3) {
                if(isValid(bairro)) parts.push(bairro);
                if(isValid(cidade)) parts.push(cidade);
            }
            // Level 4 (City): City only
            else if (level === 4) {
                if(isValid(cidade)) parts.push(cidade);
            }

            if (parts.length === 0) return null;

            // Append Context if not CEP only - Enforce Bahia
            parts.push("Bahia");
            parts.push("Brasil");
            return parts.join(', ');
        }

        // Rate-limited Queue Processor for Nominatim (1 req/1.2s)
        async function processNominatimQueue() {
            if (isProcessingQueue || nominatimQueue.length === 0) return;
            isProcessingQueue = true;

            const processNext = async () => {
                if (nominatimQueue.length === 0) {
                    isProcessingQueue = false;
                    console.log("[GeoSync] Fila de download finalizada.");
                    return;
                }

                const item = nominatimQueue.shift();
                const client = item.client;
                // Determine level (default 0)
                let level = item.level !== undefined ? item.level : 0;

                // Construct address or use legacy
                let address = item.address;
                if (!address) {
                    address = buildAddress(client, level);

                    // If address is null (e.g. invalid level data), auto-advance
                    if (!address && level < 4) {
                        nominatimQueue.unshift({ client, level: level + 1 });
                        setTimeout(processNext, 0);
                        return;
                    }
                }

                if (!address) {
                     console.log(`[GeoSync] Endereço inválido para ${client.nomeCliente} (L${level}), falha definitiva.`);
                     setTimeout(processNext, 100);
                     return;
                }

                console.log(`[GeoSync] Baixando (L${level}): ${client.nomeCliente} [${address}] (${nominatimQueue.length} restantes)...`);

                try {
                    const result = await geocodeAddressNominatim(address);
                    if (result) {
                        console.log(`[GeoSync] Sucesso: ${client.nomeCliente} -> Salvo.`);
                        const codCli = String(client['Código'] || client['codigo_cliente']);
                        await saveCoordinateToSupabase(codCli, result.lat, result.lng, result.formatted_address);

                        const cityMapContainer = document.getElementById('city-map-container');
                        if (heatLayer && cityMapContainer && !cityMapContainer.classList.contains('hidden')) {
                            // Fix: Check if layer is active on map to avoid "Cannot read properties of null (reading '_animating')"
                            if (heatLayer._map) {
                                heatLayer.addLatLng([result.lat, result.lng, 1]);
                            } else {
                                // If layer is hidden (e.g. high zoom), just update data source without redraw
                                heatLayer._latlngs.push([result.lat, result.lng, 1]);
                            }
                        }
                    } else {
                        // Retry Logic: If failed, try next level of fallback
                        if (level < 4) {
                             console.log(`[GeoSync] Falha L${level} para ${client.nomeCliente}. Tentando nível ${level+1}...`);
                             // Push back to front with incremented level
                             nominatimQueue.unshift({ client, level: level + 1 });
                        } else {
                             console.log(`[GeoSync] Falha Definitiva (Não encontrado): ${client.nomeCliente}`);
                        }
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
            const activeClientCodes = new Set();
            for (const c of activeClientsList) {
                activeClientCodes.add(String(c['Código'] || c['codigo_cliente']));
            }

            // 1. Cleanup Orphans
            const orphanedCodes = [];
            for (const [code, coord] of clientCoordinatesMap) {
                if (!activeClientCodes.has(code)) {
                    orphanedCodes.push(code);
                }
            }

            if (orphanedCodes.length > 0) {
                console.log(`Cleaning up ${orphanedCodes.length} orphaned coordinates...`);
                const { error } = await window.supabaseClient
                    .from('data_client_coordinates')
                    .delete()
                    .in('client_code', orphanedCodes);

                if (!error) {
                    orphanedCodes.forEach(c => clientCoordinatesMap.delete(c));
                }
            }

            // 2. Queue All Missing
            let queuedCount = 0;

            // Optimization: Use Set for O(1) lookup
            const queuedClientCodes = new Set();
            nominatimQueue.forEach(item => {
                queuedClientCodes.add(String(item.client['Código'] || item.client['codigo_cliente']));
            });

            activeClientsList.forEach(client => {
                const code = String(client['Código'] || client['codigo_cliente']);
                if (clientCoordinatesMap.has(code)) return;

                // Validate minimal info (City)
                const cidade = client.cidade || client.CIDADE || '';
                const cep = client.cep || client.CEP || '';

                // CEP Validation: Must be Bahia (40xxx to 48xxx)
                const cleanCep = cep.replace(/\D/g, '');
                const cepVal = parseInt(cleanCep);
                const isBahia = !isNaN(cepVal) && cepVal >= 40000000 && cepVal <= 48999999;

                if (!isBahia) {
                    // console.log(`[GeoSync] Ignorado: CEP fora da Bahia (${cep}) - ${client.nomeCliente}`);
                    return;
                }

                if (cidade && cidade !== 'N/A') {
                    // Check for duplicates
                    if (!queuedClientCodes.has(code)) {
                        nominatimQueue.push({ client, level: 0 });
                        queuedClientCodes.add(code);
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

        function renderMetaRealizadoPosChart(data) {
            const container = document.getElementById('metaRealizadoPosChartContainer');
            if (!container) return;

            let canvas = container.querySelector('canvas');
            if (!canvas) {
                canvas = document.createElement('canvas');
                container.appendChild(canvas);
            }

            const chartId = 'metaRealizadoPosChartInstance';

            // Aggregate Totals for Positivação
            const totalGoal = data.reduce((sum, d) => sum + (d.posGoal || 0), 0);
            const totalReal = data.reduce((sum, d) => sum + (d.posRealized || 0), 0);

            if (charts[chartId]) {
                charts[chartId].data.datasets[0].data = [totalGoal];
                charts[chartId].data.datasets[1].data = [totalReal];
                charts[chartId].update('none');
            } else {
                charts[chartId] = new Chart(canvas, {
                    type: 'bar',
                    data: {
                        labels: ['Positivação'],
                        datasets: [
                            {
                                label: 'Meta',
                                data: [totalGoal],
                                backgroundColor: '#a855f7', // Purple
                                barPercentage: 0.6,
                                categoryPercentage: 0.8
                            },
                            {
                                label: 'Realizado',
                                data: [totalReal],
                                backgroundColor: '#22c55e', // Green
                                barPercentage: 0.6,
                                categoryPercentage: 0.8
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        layout: {
                            padding: {
                                top: 50
                            }
                        },
                        plugins: {
                            legend: { position: 'top', labels: { color: '#cbd5e1' } },
                            tooltip: {
                                callbacks: {
                                    label: function(context) {
                                        return `${context.dataset.label}: ${context.parsed.y} Clientes`;
                                    }
                                }
                            },
                            datalabels: {
                                color: '#fff',
                                anchor: 'end',
                                align: 'top',
                                formatter: (value) => value,
                                font: { weight: 'bold' }
                            }
                        },
                        scales: {
                            y: {
                                beginAtZero: true,
                                grace: '10%',
                                grid: { color: '#334155' },
                                ticks: { color: '#94a3b8' }
                            },
                            x: {
                                grid: { display: false },
                                ticks: { color: '#94a3b8' }
                            }
                        }
                    }
                });
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

            const jobId = ++cityMapJobId;
            isCityMapCalculating = true;

            // Cache for Async Marker Generation
            currentFilteredClients = clients;
            areMarkersGenerated = false;
            if (clientMarkersLayer) clientMarkersLayer.clearLayers();

            const heatData = [];
            const missingCoordsClients = [];
            const validBounds = [];

            // Heatmap Loop (Sync - Fast) - Update UI immediately
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

            // Sales Aggregation (Async Chunked)
            const tempSalesMap = new Map();
            const tempMixStatus = new Map();

            if (sales) {
                runAsyncChunked(sales, (s) => {
                    const cod = s.CODCLI;
                    const val = Number(s.VLVENDA) || 0;
                    tempSalesMap.set(cod, (tempSalesMap.get(cod) || 0) + val);

                    // Mix Logic
                    let mix = tempMixStatus.get(cod);
                    if (!mix) {
                        mix = { elma: false, foods: false };
                        tempMixStatus.set(cod, mix);
                    }

                    const codFor = String(s.CODFOR);
                    // Elma: 707, 708, 752
                    if (codFor === '707' || codFor === '708' || codFor === '752') {
                        mix.elma = true;
                    }
                    // Foods: 1119
                    else if (codFor === '1119') {
                        mix.foods = true;
                    }
                }, () => {
                    // On Complete
                    if (jobId !== cityMapJobId) return; // Cancelled by newer request

                    currentFilteredSalesMap = tempSalesMap;
                    currentClientMixStatus = tempMixStatus;
                    areMarkersGenerated = false;
                    isCityMapCalculating = false;

                    // Trigger Marker Logic (Now that data is ready)
                    updateMarkersVisibility();

                }, () => jobId !== cityMapJobId); // isCancelled check
            } else {
                // No sales, clear maps
                if (jobId === cityMapJobId) {
                    currentFilteredSalesMap = new Map();
                    currentClientMixStatus = new Map();
                    areMarkersGenerated = false;
                    isCityMapCalculating = false;
                    updateMarkersVisibility();
                }
            }
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
            if (areMarkersGenerated || isCityMapCalculating) return;

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

                    // Color Logic
                    // Default: Red (No purchase or <= 0)
                    let markerColor = '#ef4444'; // red-500
                    let statusText = 'Não comprou';

                    if (val > 0) {
                        const mix = currentClientMixStatus.get(codCli) || { elma: false, foods: false };
                        if (mix.elma && mix.foods) {
                            markerColor = '#3b82f6'; // blue-500 (Elma & Foods)
                            statusText = 'Comprou Elma e Foods';
                        } else if (mix.elma) {
                            markerColor = '#22c55e'; // green-500 (Only Elma)
                            statusText = 'Apenas Elma';
                        } else if (mix.foods) {
                            markerColor = '#eab308'; // yellow-500 (Only Foods)
                            statusText = 'Apenas Foods';
                        } else {
                            markerColor = '#9ca3af'; // gray-400 (Other/Unknown)
                            statusText = 'Outros';
                        }
                    }

                    const tooltipContent = `
                        <div class="text-xs">
                            <b>${codCli} - ${client.nomeCliente || 'Cliente'}</b><br>
                            <span class="text-blue-500 font-semibold">RCA: ${rcaName}</span><br>
                            <span class="text-green-600 font-bold">Venda: ${formattedVal}</span><br>
                            <span style="color: ${markerColor}; font-weight: bold;">Status: ${statusText}</span><br>
                            ${client.bairro || ''}, ${client.cidade || ''}
                        </div>
                    `;

                    // SVG Pin Icon
                    const svgIcon = L.divIcon({
                        className: 'bg-transparent border-0', // Remove default styles
                        html: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="30" height="40" fill="${markerColor}" stroke="white" stroke-width="1.5" style="filter: drop-shadow(0px 2px 2px rgba(0,0,0,0.3));">
                                <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z"/>
                                <circle cx="12" cy="9" r="2.5" fill="white"/>
                               </svg>`,
                        iconSize: [30, 40],
                        iconAnchor: [15, 40],
                        tooltipAnchor: [0, -35]
                    });

                    const marker = L.marker([coords.lat, coords.lng], {
                        icon: svgIcon,
                        opacity: 1
                    });

                    marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, 0] });
                    clientMarkersLayer.addLayer(marker);
                }
            }, () => {
                areMarkersGenerated = true;
                updateMarkersVisibility();
            });
        }

        let sellerDetailsMap = new Map();

        // --- HIERARCHY FILTER SYSTEM ---
        const hierarchyState = {}; // Map<viewPrefix, { coords: Set, cocoords: Set, promotors: Set }>

        function getHierarchyFilteredClients(viewPrefix, sourceClients = allClientsData) {
            const state = hierarchyState[viewPrefix];
            if (!state) return sourceClients;

            const { coords, cocoords, promotors } = state;

            let effectiveCoords = new Set(coords);
            let effectiveCoCoords = new Set(cocoords);
            let effectivePromotors = new Set(promotors);

            // Apply User Context Constraints implicitly?
            // FIX: Only add if values exist to avoid filtering by 'undefined' (which causes 0 results)
            if (userHierarchyContext.role === 'coord' && userHierarchyContext.coord) effectiveCoords.add(userHierarchyContext.coord);
            if (userHierarchyContext.role === 'cocoord') {
                if (userHierarchyContext.coord) effectiveCoords.add(userHierarchyContext.coord);
                if (userHierarchyContext.cocoord) effectiveCoCoords.add(userHierarchyContext.cocoord);
            }
            if (userHierarchyContext.role === 'promotor') {
                if (userHierarchyContext.coord) effectiveCoords.add(userHierarchyContext.coord);
                if (userHierarchyContext.cocoord) effectiveCoCoords.add(userHierarchyContext.cocoord);
                if (userHierarchyContext.promotor) effectivePromotors.add(userHierarchyContext.promotor);
            }

            const isColumnar = sourceClients instanceof ColumnarDataset;
            const result = [];
            const len = sourceClients.length;

            if (viewPrefix === 'main') {
                 console.log(`[DEBUG] Filtering Clients for view 'main'. Total Clients: ${len}`);
                 console.log(`[DEBUG] Effective Coords: ${Array.from(effectiveCoords).join(', ')}`);
                 console.log(`[DEBUG] User Context Role: ${userHierarchyContext.role}`);
            }

            let missingNodeCount = 0;

            for(let i=0; i<len; i++) {
                const client = isColumnar ? sourceClients.get(i) : sourceClients[i];
                const codCli = normalizeKey(client['Código'] || client['codigo_cliente']);
                const node = optimizedData.clientHierarchyMap.get(codCli);

                if (!node) {
                    missingNodeCount++;
                    // FIX: Allow Orphans for Admins if no filters are active
                    if (userHierarchyContext.role === 'adm') {
                        const hasFilters = effectiveCoords.size > 0 || effectiveCoCoords.size > 0 || effectivePromotors.size > 0;
                        if (!hasFilters) {
                            result.push(client);
                        }
                    }
                    continue; 
                }

                // Check Coord
                if (effectiveCoords.size > 0 && !effectiveCoords.has(node.coord.code)) continue;
                // Check CoCoord
                if (effectiveCoCoords.size > 0 && !effectiveCoCoords.has(node.cocoord.code)) continue;
                // Check Promotor
                if (effectivePromotors.size > 0 && !effectivePromotors.has(node.promotor.code)) continue;

                result.push(client);
            }

            if (viewPrefix === 'main') {
                 console.log(`[DEBUG] Filter Result: ${result.length} clients kept. (Missing Node: ${missingNodeCount})`);
            }
            return result;
        }

        function updateFilterButtonText(element, selectedSet, defaultLabel) {
            if (!element) return;
            if (selectedSet.size === 0) {
                element.textContent = defaultLabel;
            } else if (selectedSet.size === 1) {
                // Find the label for the single selected value?
                // We don't have the label map easily accessible here without passing it.
                // For simplicity, showing count or generic text.
                // Or if we want the label, we'd need to lookup in optimizedData maps.
                // Let's iterate the set to get the value.
                const val = selectedSet.values().next().value;
                // Try to resolve name
                let name = val;
                if (optimizedData.coordMap.has(val)) name = optimizedData.coordMap.get(val);
                else if (optimizedData.cocoordMap.has(val)) name = optimizedData.cocoordMap.get(val);
                else if (optimizedData.promotorMap.has(val)) name = optimizedData.promotorMap.get(val);
                
                element.textContent = name;
            } else {
                element.textContent = `${selectedSet.size} selecionados`;
            }
        }

        function updateHierarchyDropdown(viewPrefix, level) {
            const state = hierarchyState[viewPrefix];
            const els = {
                coord: { dd: document.getElementById(`${viewPrefix}-coord-filter-dropdown`), text: document.getElementById(`${viewPrefix}-coord-filter-text`) },
                cocoord: { dd: document.getElementById(`${viewPrefix}-cocoord-filter-dropdown`), text: document.getElementById(`${viewPrefix}-cocoord-filter-text`) },
                promotor: { dd: document.getElementById(`${viewPrefix}-promotor-filter-dropdown`), text: document.getElementById(`${viewPrefix}-promotor-filter-text`) }
            };

            const target = els[level];
            if (!target.dd) return;

            let options = [];
            // Determine available options based on parent selection
            if (level === 'coord') {
                // Show all Coords (or restricted)
                if (userHierarchyContext.role === 'adm') {
                    options = Array.from(optimizedData.coordMap.entries()).map(([k, v]) => ({ value: k, label: v }));
                } else {
                    // Restricted: Only show own
                    if (userHierarchyContext.coord) {
                        options = [{ value: userHierarchyContext.coord, label: optimizedData.coordMap.get(userHierarchyContext.coord) || userHierarchyContext.coord }];
                    }
                }
            } else if (level === 'cocoord') {
                // Show CoCoords belonging to selected Coords
                let parentCoords = state.coords;
                // If no parent selected, and ADM, show ALL. If restricted, show allowed.
                // If restricted, state.coords might be empty initially, but user context implies restriction.
                
                let allowedCoords = parentCoords;
                if (allowedCoords.size === 0) {
                    if (userHierarchyContext.role === 'adm') {
                        // All coords
                        allowedCoords = new Set(optimizedData.coordMap.keys());
                    } else if (userHierarchyContext.coord) {
                        allowedCoords = new Set([userHierarchyContext.coord]);
                    }
                }

                const validCodes = new Set();
                allowedCoords.forEach(c => {
                    const children = optimizedData.cocoordsByCoord.get(c);
                    if(children) children.forEach(child => validCodes.add(child));
                });

                // Apply User Context Restriction for CoCoord level
                if (userHierarchyContext.role === 'cocoord' || userHierarchyContext.role === 'promotor') {
                    // Restrict to own cocoord
                    if (userHierarchyContext.cocoord && validCodes.has(userHierarchyContext.cocoord)) {
                        validCodes.clear();
                        validCodes.add(userHierarchyContext.cocoord);
                    } else {
                        validCodes.clear(); // Should not happen if data consistent
                    }
                }

                options = Array.from(validCodes).map(c => ({ value: c, label: optimizedData.cocoordMap.get(c) || c }));
            } else if (level === 'promotor') {
                // Show Promotors belonging to selected CoCoords
                let parentCoCoords = state.cocoords;
                
                let allowedCoCoords = parentCoCoords;
                if (allowedCoCoords.size === 0) {
                    // Need to resolve relevant CoCoords from relevant Coords
                    let relevantCoords = state.coords;
                    if (relevantCoords.size === 0) {
                         if (userHierarchyContext.role === 'adm') relevantCoords = new Set(optimizedData.coordMap.keys());
                         else if (userHierarchyContext.coord) relevantCoords = new Set([userHierarchyContext.coord]);
                    }

                    const validCoCoords = new Set();
                    relevantCoords.forEach(c => {
                        const children = optimizedData.cocoordsByCoord.get(c);
                        if(children) children.forEach(child => validCoCoords.add(child));
                    });
                    
                    // Filter by User Context
                    if (userHierarchyContext.role === 'cocoord' || userHierarchyContext.role === 'promotor') {
                         if (userHierarchyContext.cocoord) {
                             // Only keep own
                             if (validCoCoords.has(userHierarchyContext.cocoord)) {
                                 validCoCoords.clear();
                                 validCoCoords.add(userHierarchyContext.cocoord);
                             }
                         }
                    }
                    allowedCoCoords = validCoCoords;
                }

                const validCodes = new Set();
                allowedCoCoords.forEach(c => {
                    const children = optimizedData.promotorsByCocoord.get(c);
                    if(children) children.forEach(child => validCodes.add(child));
                });

                // Apply User Context Restriction for Promotor level
                if (userHierarchyContext.role === 'promotor') {
                    if (userHierarchyContext.promotor && validCodes.has(userHierarchyContext.promotor)) {
                        validCodes.clear();
                        validCodes.add(userHierarchyContext.promotor);
                    }
                }

                options = Array.from(validCodes).map(c => ({ value: c, label: optimizedData.promotorMap.get(c) || c }));
            }

            // Sort
            options.sort((a, b) => a.label.localeCompare(b.label));

            // Render
            let html = '';
            const selectedSet = state[level + 's']; // coords, cocoords, promotors
            options.forEach(opt => {
                const checked = selectedSet.has(opt.value) ? 'checked' : '';
                html += `
                    <label class="flex items-center justify-between p-2 hover:bg-slate-700 rounded cursor-pointer">
                        <span class="text-xs text-slate-300 truncate mr-2">${opt.label}</span>
                        <input type="checkbox" value="${opt.value}" ${checked} class="form-checkbox h-4 w-4 text-blue-600 bg-slate-700 border-slate-600 rounded focus:ring-blue-500 focus:ring-offset-slate-800">
                    </label>
                `;
            });
            target.dd.innerHTML = html;
            
            // Update Text Label
            let label = 'Todos';
            if (level === 'coord') label = 'Coordenador';
            if (level === 'cocoord') label = 'Co-Coord';
            if (level === 'promotor') label = 'Promotor';

            updateFilterButtonText(target.text, selectedSet, label);
        }

        function setupHierarchyFilters(viewPrefix, onUpdate) {
            // Init State
            if (!hierarchyState[viewPrefix]) {
                hierarchyState[viewPrefix] = { coords: new Set(), cocoords: new Set(), promotors: new Set() };
            }
            const state = hierarchyState[viewPrefix];

            const els = {
                coord: { btn: document.getElementById(`${viewPrefix}-coord-filter-btn`), dd: document.getElementById(`${viewPrefix}-coord-filter-dropdown`) },
                cocoord: { btn: document.getElementById(`${viewPrefix}-cocoord-filter-btn`), dd: document.getElementById(`${viewPrefix}-cocoord-filter-dropdown`) },
                promotor: { btn: document.getElementById(`${viewPrefix}-promotor-filter-btn`), dd: document.getElementById(`${viewPrefix}-promotor-filter-dropdown`) }
            };

            const bindToggle = (el) => {
                if (el.btn && el.dd) {
                    el.btn.addEventListener('click', (e) => {
                        e.stopPropagation();
                        // Close others
                        Object.values(els).forEach(x => { if(x.dd && x !== el) x.dd.classList.add('hidden'); });
                        el.dd.classList.toggle('hidden');
                    });
                }
            };
            bindToggle(els.coord);
            bindToggle(els.cocoord);
            bindToggle(els.promotor);

            // Close dropdowns when clicking outside
            document.addEventListener('click', (e) => {
                Object.values(els).forEach(x => {
                    if (x.dd && !x.dd.classList.contains('hidden')) {
                        if (x.btn && !x.btn.contains(e.target) && !x.dd.contains(e.target)) {
                            x.dd.classList.add('hidden');
                        }
                    }
                });
            });

            const bindChange = (level, nextLevel, nextNextLevel) => {
                const el = els[level];
                if (el && el.dd) {
                    el.dd.addEventListener('change', (e) => {
                        if (e.target.type === 'checkbox') {
                            const val = e.target.value;
                            const set = state[level + 's'];
                            if (e.target.checked) set.add(val); else set.delete(val);
                            
                            // Update Button Text
                            updateHierarchyDropdown(viewPrefix, level); // Re-render self? No, just text. But re-rendering handles text.
                            
                            // Cascade Clear
                            if (nextLevel) {
                                state[nextLevel + 's'].clear();
                                updateHierarchyDropdown(viewPrefix, nextLevel);
                            }
                            if (nextNextLevel) {
                                state[nextNextLevel + 's'].clear();
                                updateHierarchyDropdown(viewPrefix, nextNextLevel);
                            }

                            if (onUpdate) onUpdate();
                        }
                    });
                }
            };

            bindChange('coord', 'cocoord', 'promotor');
            bindChange('cocoord', 'promotor', null);
            bindChange('promotor', null, null);

            // Initial Population
            updateHierarchyDropdown(viewPrefix, 'coord');
            updateHierarchyDropdown(viewPrefix, 'cocoord');
            updateHierarchyDropdown(viewPrefix, 'promotor');
            
            // Auto-select for restricted users?
            // If I am Coord, my Coord ID is userHierarchyContext.coord.
            // Should I pre-select it?
            // If I pre-select it, `getHierarchyFilteredClients` uses it.
            // If I DON'T pre-select it (empty set), `getHierarchyFilteredClients` applies it anyway via context.
            // Visually, it's better if it shows "My Name" instead of "Coordenador" (which implies All/None).
            // So yes, let's pre-select.
            
            if (userHierarchyContext.role !== 'adm') {
                if (userHierarchyContext.coord) state.coords.add(userHierarchyContext.coord);
                if (userHierarchyContext.cocoord) state.cocoords.add(userHierarchyContext.cocoord);
                if (userHierarchyContext.promotor) state.promotors.add(userHierarchyContext.promotor);
                
                // Refresh UI to show checkmarks and text
                updateHierarchyDropdown(viewPrefix, 'coord');
                updateHierarchyDropdown(viewPrefix, 'cocoord');
                updateHierarchyDropdown(viewPrefix, 'promotor');
            }
        }

        function initializeOptimizedDataStructures() {
            console.log("[DEBUG] Starting initializeOptimizedDataStructures");
            sellerDetailsMap = new Map();
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
            optimizedData.rcasBySupervisor = new Map();
            optimizedData.productsBySupplier = new Map();
            optimizedData.salesByProduct = { current: new Map(), history: new Map() };
            optimizedData.rcaCodeByName = new Map();
            optimizedData.rcaNameByCode = new Map();
            optimizedData.supervisorCodeByName = new Map();
            optimizedData.productPastaMap = new Map();

            // --- HIERARCHY LOGIC START ---
            optimizedData.hierarchyMap = new Map(); // Promotor Code -> Hierarchy Node
            optimizedData.clientHierarchyMap = new Map(); // Client Code -> Hierarchy Node
            optimizedData.coordMap = new Map(); // Coord Code -> Name
            optimizedData.cocoordMap = new Map(); // CoCoord Code -> Name
            optimizedData.promotorMap = new Map(); // Promotor Code -> Name
            optimizedData.coordsByCocoord = new Map(); // CoCoord Code -> Coord Code
            optimizedData.cocoordsByCoord = new Map(); // Coord Code -> Set<CoCoord Code>
            optimizedData.promotorsByCocoord = new Map(); // CoCoord Code -> Set<Promotor Code>

            if (embeddedData.hierarchy) {
                console.log(`[DEBUG] Processing Hierarchy. Rows: ${embeddedData.hierarchy.length}`);
                embeddedData.hierarchy.forEach(h => {
                    // Robust key access (Handle lowercase/uppercase/mapped variations)
                    const getVal = (keys) => {
                        for (const k of keys) {
                            if (h[k] !== undefined && h[k] !== null) return String(h[k]);
                        }
                        return '';
                    };

                    const coordCode = getVal(['cod_coord', 'COD_COORD', 'COD COORD.']).trim().toUpperCase();
                    const coordName = (getVal(['nome_coord', 'NOME_COORD', 'COORDENADOR']) || coordCode).toUpperCase();
                    
                    const cocoordCode = getVal(['cod_cocoord', 'COD_COCOORD', 'COD CO-COORD.']).trim().toUpperCase();
                    const cocoordName = (getVal(['nome_cocoord', 'NOME_COCOORD', 'CO-COORDENADOR']) || cocoordCode).toUpperCase();
                    
                    const promotorCode = getVal(['cod_promotor', 'COD_PROMOTOR', 'COD PROMOTOR']).trim().toUpperCase();
                    const promotorName = (getVal(['nome_promotor', 'NOME_PROMOTOR', 'PROMOTOR']) || promotorCode).toUpperCase();

                    if (coordCode) {
                        optimizedData.coordMap.set(coordCode, coordName);
                        if (!optimizedData.cocoordsByCoord.has(coordCode)) optimizedData.cocoordsByCoord.set(coordCode, new Set());
                        if (cocoordCode) optimizedData.cocoordsByCoord.get(coordCode).add(cocoordCode);
                    }
                    if (cocoordCode) {
                        optimizedData.cocoordMap.set(cocoordCode, cocoordName);
                        if (coordCode) optimizedData.coordsByCocoord.set(cocoordCode, coordCode);
                        if (!optimizedData.promotorsByCocoord.has(cocoordCode)) optimizedData.promotorsByCocoord.set(cocoordCode, new Set());
                        if (promotorCode) optimizedData.promotorsByCocoord.get(cocoordCode).add(promotorCode);
                    }
                    if (promotorCode) optimizedData.promotorMap.set(promotorCode, promotorName);

                    if (promotorCode) {
                        optimizedData.hierarchyMap.set(promotorCode, {
                            coord: { code: coordCode, name: coordName },
                            cocoord: { code: cocoordCode, name: cocoordName },
                            promotor: { code: promotorCode, name: promotorName }
                        });
                    }
                });
            }

            if (embeddedData.clientPromoters) {
                console.log(`[DEBUG] Processing Client Promoters. Rows: ${embeddedData.clientPromoters.length}`);
                let matchCount = 0;
                let sampleLogged = false;
                embeddedData.clientPromoters.forEach(cp => {
                    let clientCode = String(cp.client_code).trim();
                    // Normalize client code to match dataset (remove leading zeros)
                    if (/^\d+$/.test(clientCode)) {
                        clientCode = String(parseInt(clientCode, 10));
                    }

                    const promotorCode = String(cp.promoter_code).trim().toUpperCase();
                    const hierarchyNode = optimizedData.hierarchyMap.get(promotorCode);
                    if (hierarchyNode) {
                        optimizedData.clientHierarchyMap.set(clientCode, hierarchyNode);
                        matchCount++;
                    } else if (!sampleLogged) {
                        console.warn(`[DEBUG] Hierarchy Node Not Found for Promotor: ${promotorCode} (Client: ${clientCode})`);
                        sampleLogged = true;
                    }
                });
                console.log(`[DEBUG] Client Promoters Merged: ${matchCount}/${embeddedData.clientPromoters.length}`);
            } else {
                console.warn("[DEBUG] embeddedData.clientPromoters is missing or empty.");
            }
            console.log(`[DEBUG] Final Hierarchy Map Size: ${optimizedData.hierarchyMap.size}`);
            console.log(`[DEBUG] Final Client Hierarchy Map Size: ${optimizedData.clientHierarchyMap.size}`);
            // --- HIERARCHY LOGIC END ---

            // Access via accessor method for potential ColumnarDataset
            const getClient = (i) => allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i];

            for (let i = 0; i < allClientsData.length; i++) {
                const client = getClient(i); // Hydrate object for processing
                const codCli = normalizeKey(client['Código'] || client['codigo_cliente']);

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
                    // Ensure global mapping for Import/Analysis lookup
                    optimizedData.rcaCodeByName.set('AMERICANAS', '1001');
                    sellerDetailsMap.set('1001', { name: 'AMERICANAS', supervisor: 'BALCAO' });
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

                const rawCnpj = client['CNPJ/CPF'] || client.cnpj_cpf || client.CNPJ || '';
                const cleanCnpj = String(rawCnpj).replace(/[^0-9]/g, '');
                optimizedData.searchIndices.clients[i] = {
                    code: codCli,
                    nameLower: (client.nomeCliente || '').toLowerCase(),
                    cityLower: (client.cidade || '').toLowerCase(),
                    cnpj: cleanCnpj
                };
            }

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

                // Cache for parsed dates to avoid repeated parsing of same timestamp/string
                const dateCache = new Map();

                for (let i = 0; i < data.length; i++) {
                    // Optimized: Use Integer Index as ID
                    const id = i;

                    // Note: dataMap is now the dataset itself, we don't need to set anything into it.
                    // We just index the position 'i'.

                    const supervisor = getVal(i, 'SUPERV') || 'N/A';
                    const rca = getVal(i, 'NOME') || 'N/A';

                    // Use pre-normalized PASTA
                    let pasta = getVal(i, 'OBSERVACAOFOR');

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
                        // Check cache
                        if (dateCache.has(dtPed)) {
                            const cached = dateCache.get(dtPed);
                            if (cached) workingDaysSet.add(cached);
                        } else {
                            // dtPed is likely a number (timestamp).
                            // If it's a number, new Date(dtPed) works.
                            // If it's a string, parseDate(dtPed) (from local function or global?)
                            // Global parseDate handles numbers too.
                            const dateObj = (typeof dtPed === 'number') ? new Date(dtPed) : parseDate(dtPed);
                            let result = null;

                            if(dateObj && !isNaN(dateObj.getTime())) {
                                const dayOfWeek = dateObj.getUTCDay();
                                if (dayOfWeek >= 1 && dayOfWeek <= 5) {
                                    result = dateObj.toISOString().split('T')[0];
                                    workingDaysSet.add(result);
                                }
                            }
                            dateCache.set(dtPed, result);
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
                // Convert to Date using robust parser
                sale.DTPED = parseDate(sale.DTPED);
                sale.DTSAIDA = parseDate(sale.DTSAIDA);

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
            const dateA = a.DTPED;
            const dateB = b.DTPED;
            if (!dateA) return 1;
            if (!dateB) return -1;
            return dateB - dateA;
        });

        Chart.register(ChartDataLabels);

        const mainDashboard = document.getElementById('main-dashboard');
        const cityView = document.getElementById('city-view');
        const comparisonView = document.getElementById('comparison-view');
        const stockView = document.getElementById('stock-view');

        const showCityBtn = document.getElementById('show-city-btn');
        const backToMainFromCityBtn = document.getElementById('back-to-main-from-city-btn');
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
        const cityVendedorFilterText = document.getElementById('city-vendedor-filter-text');
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

        const comparisonSupervisorFilter = document.getElementById('comparison-supervisor-filter');
        const comparisonVendedorFilterText = document.getElementById('comparison-vendedor-filter-text');
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

        const newProductsTableBody = document.getElementById('new-products-table-body');
        const lostProductsTableBody = document.getElementById('lost-products-table-body');


        const innovationsMonthView = document.getElementById('innovations-month-view');
        const innovationsMonthChartContainer = document.getElementById('innovations-month-chartContainer');
        const innovationsMonthTableBody = document.getElementById('innovations-month-table-body');
        const innovationsMonthCategoryFilter = document.getElementById('innovations-month-category-filter');
        const innovationsMonthSupervisorFilter = document.getElementById('innovations-month-supervisor-filter');
        const innovationsMonthVendedorFilterText = document.getElementById('innovations-month-vendedor-filter-text');
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
        const coverageVendedorFilterText = document.getElementById('coverage-vendedor-filter-text');
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

        const goalsGvSupervisorFilterText = document.getElementById('goals-gv-supervisor-filter-text');

        const goalsGvSellerFilterText = document.getElementById('goals-gv-seller-filter-text');

        const goalsGvCodcliFilter = document.getElementById('goals-gv-codcli-filter');
        const clearGoalsGvFiltersBtn = document.getElementById('clear-goals-gv-filters-btn');

        const goalsSvSupervisorFilterText = document.getElementById('goals-sv-supervisor-filter-text');


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
            cobertura: { dirty: true },
            cidades: { dirty: true },
            inovacoes: { dirty: true, cache: null, lastTypesKey: '' },
            mix: { dirty: true },
            goals: { dirty: true },
            metaRealizado: { dirty: true }
        };

        // Render IDs for Race Condition Guard
        let mixRenderId = 0;
        let coverageRenderId = 0;
        let cityRenderId = 0;
        let comparisonRenderId = 0;
        let goalsRenderId = 0;
        let goalsSvRenderId = 0;

        let charts = {};
        let currentProductMetric = 'faturamento';
        let currentFornecedor = '';
        let currentComparisonFornecedor = 'PEPSICO';
        let useTendencyComparison = false;
        let comparisonChartType = 'weekly';
        let comparisonMonthlyMetric = 'faturamento';
        let activeClientsForExport = [];
        let selectedMainCoords = [];
        let selectedMainCoCoords = [];
        let selectedMainPromotors = [];
        let selectedCityCoords = [];
        let selectedCityCoCoords = [];
        let selectedCityPromotors = [];
        let selectedComparisonCoords = [];
        let selectedComparisonCoCoords = [];
        let selectedComparisonPromotors = [];
        let selectedInnovationsCoords = [];
        let selectedInnovationsCoCoords = [];
        let selectedInnovationsPromotors = [];
        let selectedMixCoords = [];
        let selectedMixCoCoords = [];
        let selectedMixPromotors = [];
        let selectedCoverageCoords = [];
        let selectedCoverageCoCoords = [];
        let selectedCoveragePromotors = [];
        let selectedGoalsGvCoords = [];
        let selectedGoalsGvCoCoords = [];
        let selectedGoalsGvPromotors = [];
        let selectedGoalsSvCoords = [];
        let selectedGoalsSvCoCoords = [];
        let selectedGoalsSvPromotors = [];
        let selectedGoalsSummaryCoords = [];
        let selectedGoalsSummaryCoCoords = [];
        let selectedGoalsSummaryPromotors = [];
        let selectedMetaRealizadoCoords = [];
        let selectedMetaRealizadoCoCoords = [];
        let selectedMetaRealizadoPromotors = [];
        let inactiveClientsForExport = [];
        let selectedMainSuppliers = [];
        let selectedTiposVenda = [];
        var selectedCitySuppliers = [];
        let selectedComparisonSuppliers = [];
        let selectedComparisonProducts = [];
        let selectedCoverageTiposVenda = [];
        let selectedComparisonTiposVenda = [];
        let selectedCityTiposVenda = [];
        let historicalBests = {};
        let selectedHolidays = [];

        let selectedMainRedes = [];
        let selectedCityRedes = [];
        let selectedComparisonRedes = [];

        let mainRedeGroupFilter = '';
        let cityRedeGroupFilter = '';
        let comparisonRedeGroupFilter = '';

        let selectedInnovationsMonthTiposVenda = [];

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
        window.goalsTargets = goalsTargets;

        let globalGoalsMetrics = {};
        let globalGoalsTotalsCache = {};
        let globalClientGoals = new Map();
        window.globalClientGoals = globalClientGoals;
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

        let selectedMetaRealizadoSuppliers = [];
        let currentMetaRealizadoPasta = 'PEPSICO'; // Default
        let currentMetaRealizadoMetric = 'valor'; // 'valor' or 'peso'

        // let innovationsIncludeBonus = true; // REMOVED
        // let innovationsMonthIncludeBonus = true; // REMOVED

        let innovationsMonthTableDataForExport = [];
        let innovationsByClientForExport = [];
        let categoryLegendForExport = [];
        let chartLabels = [];
        let globalInnovationCategories = null;
        let globalProductToCategoryMap = null;

        let calendarState = { year: lastSaleDate.getUTCFullYear(), month: lastSaleDate.getUTCMonth() };

        let selectedCoverageSuppliers = [];
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

            const tiposVendaSet = new Set(selectedMixTiposVenda);
            const city = document.getElementById('mix-city-filter').value.trim().toLowerCase();
            const filial = document.getElementById('mix-filial-filter').value;

            // New Hierarchy Logic
            let clients = getHierarchyFilteredClients('mix', allClientsData);

            // OPTIMIZATION: Combine filters into a single pass
            const checkRede = excludeFilter !== 'rede';
            const isComRede = mixRedeGroupFilter === 'com_rede';
            const isSemRede = mixRedeGroupFilter === 'sem_rede';
            const redeSet = (isComRede && selectedMixRedes.length > 0) ? new Set(selectedMixRedes) : null;

            const checkFilial = filial !== 'ambas';
            const checkCity = excludeFilter !== 'city' && !!city;

            // Removed Supervisor/Seller checks
            // if (excludeFilter !== 'supplier' && selectedCitySuppliers.length > 0) { ... }

            clients = clients.filter(c => {
                // 1. Rede Logic
                if (checkRede) {
                    if (isComRede) {
                        if (!c.ramo || c.ramo === 'N/A') return false;
                        if (redeSet && !redeSet.has(c.ramo)) return false;
                    } else if (isSemRede) {
                        if (c.ramo && c.ramo !== 'N/A') return false;
                    }
                }

                // 2. Filial Logic
                if (checkFilial) {
                    if (clientLastBranch.get(c['Código']) !== filial) return false;
                }

                // 3. City Logic
                if (checkCity) {
                    if (!c.cidade || c.cidade.toLowerCase() !== city) return false;
                }

                // 4. Active Logic
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                // Keep if Americanas OR Not 53 OR Has Sales
                if (!isAmericanas && rca1 === '53' && !clientsWithSalesThisMonth.has(c['Código'])) return false;

                return true;
            });

            const clientCodes = new Set();
            for (const c of clients) {
                clientCodes.add(c['Código']);
            }

            const filters = {
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

            // Supervisor/Seller filters managed by setupHierarchyFilters

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

        function getSkeletonRows(cols, rows = 5) {
            let html = '';
            for (let i = 0; i < rows; i++) {
                html += `<tr class="border-b border-slate-800/50">`;
                for (let j = 0; j < cols; j++) {
                    // Empty data-label for skeleton to prevent "null" text on mobile, just shows bar
                    html += `<td class="p-4" data-label=""><div class="skeleton h-4 w-full"></div></td>`;
                }
                html += `</tr>`;
            }
            return html;
        }

        function updateMixView() {
            mixRenderId++;
            const currentRenderId = mixRenderId;

            const { clients, sales } = getMixFilteredData();
            // const activeClientCodes = new Set(clients.map(c => c['Código'])); // Not used if iterating clients array

            // Show Loading
            document.getElementById('mix-table-body').innerHTML = getSkeletonRows(13, 10);

            // 1. Agregar Valor Líquido por Produto por Cliente (Sync - O(Sales))
            const clientProductNetValues = new Map(); // Map<CODCLI, Map<PRODUTO, NetValue>>
            const clientProductDesc = new Map(); // Map<PRODUTO, Descricao> (Cache)

            sales.forEach(s => {
                if (!s.CODCLI || !s.PRODUTO) return;
                if (!isAlternativeMode(selectedMixTiposVenda) && s.TIPOVENDA !== '1' && s.TIPOVENDA !== '9') return;

                if (!clientProductNetValues.has(s.CODCLI)) {
                    clientProductNetValues.set(s.CODCLI, new Map());
                }
                const clientMap = clientProductNetValues.get(s.CODCLI);
                const currentVal = clientMap.get(s.PRODUTO) || 0;
                const val = getValueForSale(s, selectedMixTiposVenda);
                clientMap.set(s.PRODUTO, currentVal + val);

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
                    let saltyCols = MIX_SALTY_CATEGORIES.map(b => `<td data-label="${b}" class="px-1 py-2 text-center border-l border-slate-500">${row.brands.has(b) ? checkIcon : xIcon}</td>`).join('');
                    let foodsCols = MIX_FOODS_CATEGORIES.map(b => `<td data-label="${b}" class="px-1 py-2 text-center border-l border-slate-500">${row.brands.has(b) ? checkIcon : xIcon}</td>`).join('');

                    return `
                    <tr class="hover:bg-slate-700/50 border-b border-slate-500 last:border-0">
                        <td data-label="Cód" class="px-2 py-2 font-medium text-slate-300 text-xs">${escapeHtml(row.codcli)}</td>
                        <td data-label="Cliente" class="px-2 py-2 text-xs truncate max-w-[150px]" title="${escapeHtml(row.name)}">${escapeHtml(row.name)}</td>
                        <td data-label="Cidade" class="px-2 py-2 text-xs text-slate-300 truncate max-w-[100px]">${escapeHtml(row.city)}</td>
                        <td data-label="Vendedor" class="px-2 py-2 text-xs text-slate-400 truncate max-w-[100px]">${escapeHtml(getFirstName(row.vendedor))}</td>
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
            if (hierarchyState['mix'] && hierarchyState['mix'].promotors.size === 1) {
            } else if (city) {
                fileNameParam = city;
            }
            const safeFileNameParam = fileNameParam.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            doc.save(`relatorio_mix_detalhado_${safeFileNameParam}_${new Date().toISOString().slice(0,10)}.pdf`);
        }

        // --- GOALS VIEW LOGIC ---

        // --- GOALS REDISTRIBUTION LOGIC ---
        let goalsSellerTargets = new Map(); // Stores Seller-Level Targets (Positivation, etc.)
        window.goalsSellerTargets = goalsSellerTargets; // Export for init.js

        async function saveGoalsToSupabase() {
            try {
                const monthKey = new Date().toISOString().slice(0, 7);

                // Serialize globalClientGoals (Map<CodCli, Map<Key, {fat: 0, vol: 0}>>)
                const clientsObj = {};
                for (const [clientId, clientMap] of globalClientGoals) {
                    clientsObj[clientId] = Object.fromEntries(clientMap);
                }

                // Serialize goalsSellerTargets (Map<Seller, Targets>)
                const sellerTargetsObj = {};
                for (const [seller, targets] of goalsSellerTargets) {
                    sellerTargetsObj[seller] = targets;
                }

                const payload = {
                    clients: clientsObj,
                    targets: goalsTargets,
                    seller_targets: sellerTargetsObj
                };

                const { error } = await window.supabaseClient
                    .from('goals_distribution')
                    .upsert({
                        month_key: monthKey,
                        supplier: 'ALL',
                        brand: 'GENERAL',
                        goals_data: payload
                    });

                if (error) {
                    console.error('Erro ao salvar metas:', error);
                    alert('Erro ao salvar metas no banco de dados. Verifique o console.');
                    return false;
                }
                console.log('Metas salvas com sucesso.');
                return true;
            } catch (err) {
                console.error('Exceção ao salvar metas:', err);
                alert('Erro inesperado ao salvar metas.');
                return false;
            }
        }

        function distributeSellerGoal(sellerName, categoryId, newTotalValue, metric = 'fat') {
            // metric: 'fat' or 'vol'
            // categoryId: '707', '1119_TODDY', 'tonelada_elma', etc.

            const sellerCode = optimizedData.rcaCodeByName.get(sellerName);
            if (!sellerCode) { console.warn(`[Goals] Seller not found: ${sellerName}`); return; }

            const clients = optimizedData.clientsByRca.get(sellerCode) || [];
            const activeClients = clients.filter(c => {
                const cod = String(c['Código'] || c['codigo_cliente']);
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
            });

            if (activeClients.length === 0) return;

            // Define Sub-Categories for Cascade Logic
            let targetCategories = [categoryId];
            if (categoryId === 'tonelada_elma') targetCategories = ['707', '708', '752'];
            else if (categoryId === 'tonelada_foods') targetCategories = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];

            // 1. Calculate Total History for the Seller (All sub-cats combined)
            // AND Calculate individual client-subcat history to determine specific shares.
            const clientSubCatHistory = new Map(); // Map<Client, Map<SubCat, Value>>
            let totalSellerHistory = 0;

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const historyIds = optimizedData.indices.history.byClient.get(codCli);

                if (!clientSubCatHistory.has(codCli)) clientSubCatHistory.set(codCli, new Map());
                const subCatMap = clientSubCatHistory.get(codCli);

                if (historyIds) {
                    historyIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        const codFor = String(sale.CODFOR);
                        const desc = normalize(sale.DESCRICAO || '');

                        // Check against all target categories
                        targetCategories.forEach(subCat => {
                            let isMatch = false;
                            if (subCat === '707' && codFor === '707') isMatch = true;
                            else if (subCat === '708' && codFor === '708') isMatch = true;
                            else if (subCat === '752' && codFor === '752') isMatch = true;
                            else if (codFor === '1119') {
                                if (subCat === '1119_TODDYNHO' && desc.includes('TODDYNHO')) isMatch = true;
                                else if (subCat === '1119_TODDY' && desc.includes('TODDY')) isMatch = true;
                                else if (subCat === '1119_QUAKER_KEROCOCO' && (desc.includes('QUAKER') || desc.includes('KEROCOCO'))) isMatch = true;
                            }

                            if (isMatch) {
                                const val = metric === 'fat' ? (Number(sale.VLVENDA) || 0) : (Number(sale.TOTPESOLIQ) || 0);
                                if (val > 0) {
                                    subCatMap.set(subCat, (subCatMap.get(subCat) || 0) + val);
                                    totalSellerHistory += val;
                                }
                            }
                        });
                    });
                }
            });

            // 2. Distribute
            // NewGoal(Client, SubCat) = NewTotalValue * (ClientSubCatHistory / TotalSellerHistory)

            const clientCount = activeClients.length;
            const subCatCount = targetCategories.length;

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const subCatMap = clientSubCatHistory.get(codCli);

                targetCategories.forEach(subCat => {
                    let share = 0;
                    if (totalSellerHistory > 0) {
                        share = (subCatMap.get(subCat) || 0) / totalSellerHistory;
                    } else {
                        // Fallback: Even split across all clients and subcats?
                        share = 1 / (clientCount * subCatCount);
                    }

                    const goalVal = newTotalValue * share;

                    // Update Global
                    if (!globalClientGoals.has(codCli)) globalClientGoals.set(codCli, new Map());
                    const cGoals = globalClientGoals.get(codCli);

                    if (!cGoals.has(subCat)) cGoals.set(subCat, { fat: 0, vol: 0 });
                    const target = cGoals.get(subCat);

                    if (metric === 'fat') target.fat = goalVal;
                    else if (metric === 'vol') target.vol = goalVal;
                });
            });
            console.log(`[Goals] Distributed ${newTotalValue} (${metric}) for ${sellerName} / ${categoryId} (Cascade: ${targetCategories.join(',')})`);
        }

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

                                // Positivation (Aggregate): Use Stored Target
                                let posVal = d.metaPos;
                                if (goalsSellerTargets.has(seller.name)) {
                                    const t = goalsSellerTargets.get(seller.name);
                                    if (t && t[col.id] !== undefined) posVal = t[col.id];
                                }
                                // Make Editable (cellStyle instead of aggCellStyle)
                                rowData.push(createCell(posVal, editableStyle, fmtInt));
                            } else {
                                // Editable Cells
                                rowData.push(createCell(d.metaFat, cellStyle, fmtMoney));
                                rowData.push(createCell(d.metaPos, readOnlyStyle, fmtInt));

                                // Positivation (Standard): Use Stored Target
                                let posVal = d.metaPos;
                                if (goalsSellerTargets.has(seller.name)) {
                                    const t = goalsSellerTargets.get(seller.name);
                                    if (t && t[col.id] !== undefined) posVal = t[col.id];
                                }
                                rowData.push(createCell(posVal, cellStyle, fmtInt));
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
                            let posVal = d.metaPos;
                            if (goalsSellerTargets.has(seller.name)) {
                                const t = goalsSellerTargets.get(seller.name);
                                if (t && t['GERAL'] !== undefined) posVal = t['GERAL'];
                            }
                            rowData.push(createCell(posVal, editableStyle, fmtInt));

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
            const codCli = goalsGvCodcliFilter.value.trim();

            // Apply Hierarchy Filter + "Active" Filter logic
            let clients = getHierarchyFilteredClients('goals-gv', allClientsData).filter(c => isActiveClient(c));

            // Filter by Client Code
            if (codCli) {
                clients = clients.filter(c => String(c['Código']) === codCli);
            }

            return clients;
        }

        function getHistoricalMix(sellerName, type) {
            const sellerCode = optimizedData.rcaCodeByName.get(sellerName);
            if (!sellerCode) return 0;

            const clients = optimizedData.clientsByRca.get(sellerCode) || [];
            // Filter active clients same as main view
            const activeClients = clients.filter(c => {
                const cod = String(c['Código'] || c['codigo_cliente']);
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
            });

            // Iterate Active Clients
            let totalMixMonths = 0;
            const targetCategories = type === 'salty' ? MIX_SALTY_CATEGORIES : MIX_FOODS_CATEGORIES;

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const historyIds = optimizedData.indices.history.byClient.get(codCli);

                if (historyIds) {
                    // Bucket by Month
                    const monthlySales = new Map(); // Map<MonthKey, Set<Brand>>

                    historyIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        // Using same date parsing as elsewhere
                        let dateObj = null;
                        if (typeof sale.DTPED === 'number') dateObj = new Date(sale.DTPED);
                        else dateObj = parseDate(sale.DTPED);

                        if (dateObj) {
                            const monthKey = `${dateObj.getUTCFullYear()}-${dateObj.getUTCMonth()}`;
                            if (!monthlySales.has(monthKey)) monthlySales.set(monthKey, new Set());

                            // Check brand/category match
                            const desc = normalize(sale.DESCRICAO || '');
                            targetCategories.forEach(cat => {
                                if (desc.includes(cat)) {
                                    monthlySales.get(monthKey).add(cat);
                                }
                            });
                        }
                    });

                    // Count Successful Months
                    monthlySales.forEach(brandsSet => {
                        const achieved = targetCategories.every(cat => brandsSet.has(cat));
                        if (achieved) totalMixMonths++;
                    });
                }
            });

            // Return Average (Total Mix Months / 3)
            // Assuming Quarter History is 3 months.
            return totalMixMonths / 3;
        }

        function parseInputMoney(id) {
            const el = document.getElementById(id);
            if (!el) return 0;
            let val = el.value.replace(/\./g, '').replace(',', '.');
            return parseFloat(val) || 0;
        }

        function getMonthWeeksDistribution(date) {
            const year = date.getUTCFullYear();
            const month = date.getUTCMonth();

            // Start of Month
            const startDate = new Date(Date.UTC(year, month, 1));
            // End of Month
            const endDate = new Date(Date.UTC(year, month + 1, 0));
            const totalDays = endDate.getUTCDate();

            let currentWeekStart = new Date(startDate);
            const weeks = [];
            let totalWorkingDays = 0;

            // Loop through weeks
            while (currentWeekStart <= endDate) {
                // Get end of this week (Sunday or End of Month)
                // getUTCDay: 0 (Sun) to 6 (Sat).
                // We want weeks to be Calendar Weeks (Mon-Sun or Sun-Sat).
                // Standard: ISO weeks start on Monday. But JS getDay 0 is Sunday.
                // Let's assume standard calendar week view where Sunday breaks the week.
                // However, user said "reconhecer as semanas pelo calendário... De segunda a sexta".
                // Let's define week chunks.
                // Logic: A week ends on Saturday (or Sunday).

                // Find next Sunday (or End of Month)
                let dayOfWeek = currentWeekStart.getUTCDay(); // 0=Sun, 1=Mon...
                let daysToSunday = dayOfWeek === 0 ? 0 : 7 - dayOfWeek;

                let currentWeekEnd = new Date(currentWeekStart);
                currentWeekEnd.setUTCDate(currentWeekStart.getUTCDate() + daysToSunday);

                if (currentWeekEnd > endDate) currentWeekEnd = new Date(endDate);

                // Count Working Days in this chunk
                let workingDaysInWeek = 0;
                let tempDate = new Date(currentWeekStart);
                while (tempDate <= currentWeekEnd) {
                    const dow = tempDate.getUTCDay();
                    if (dow >= 1 && dow <= 5) {
                        workingDaysInWeek++;
                        totalWorkingDays++;
                    }
                    tempDate.setUTCDate(tempDate.getUTCDate() + 1);
                }

                if (workingDaysInWeek > 0 || weeks.length === 0 || currentWeekStart <= endDate) {
                     // Only push if valid week or just start
                     weeks.push({
                         start: new Date(currentWeekStart),
                         end: new Date(currentWeekEnd),
                         workingDays: workingDaysInWeek
                     });
                }

                // Next week starts day after currentWeekEnd
                currentWeekStart = new Date(currentWeekEnd);
                currentWeekStart.setUTCDate(currentWeekStart.getUTCDate() + 1);
            }

            return { weeks, totalWorkingDays };
        }

        function getMetaRealizadoFilteredData() {
            // New Hierarchy:
            const suppliersSet = new Set(selectedMetaRealizadoSuppliers);
            const pasta = currentMetaRealizadoPasta;

            // --- Fix: Define supervisorsSet and sellersSet ---
            const supervisorsSet = new Set();
            const sellersSet = new Set();

            const hState = hierarchyState['meta-realizado'];
            if (hState) {
                const validCodes = new Set();

                // 1. Promotors (Leaf)
                if (hState.promotors.size > 0) {
                    hState.promotors.forEach(p => validCodes.add(p));
                }
                // 2. CoCoords
                else if (hState.cocoords.size > 0) {
                     hState.cocoords.forEach(cc => {
                         const children = optimizedData.promotorsByCocoord.get(cc);
                         if (children) children.forEach(p => validCodes.add(p));
                     });
                }
                // 3. Coords
                else if (hState.coords.size > 0) {
                    hState.coords.forEach(c => {
                         const cocoords = optimizedData.cocoordsByCoord.get(c);
                         if (cocoords) {
                             cocoords.forEach(cc => {
                                 const children = optimizedData.promotorsByCocoord.get(cc);
                                 if (children) children.forEach(p => validCodes.add(p));
                             });
                         }
                    });
                }

                // Map Codes to Names
                if (validCodes.size > 0) {
                    validCodes.forEach(code => {
                        const name = optimizedData.promotorMap.get(code);
                        if (name) sellersSet.add(name);
                         // Also try mapping via rcaNameByCode if the code is RCA code (fallback)
                        const rcaName = optimizedData.rcaNameByCode.get(code);
                        if (rcaName) sellersSet.add(rcaName);
                    });
                }
            }

            // Determine Goal Keys based on Pasta (Moved to top level scope)
            let goalKeys = [];

            // If Supplier Filter is Active, restricting goals to selected supplier ONLY
            if (suppliersSet.size > 0) {
                // Map selections to goal keys
                suppliersSet.forEach(sup => {
                    // Filter validation: Ensure they belong to current Pasta
                    let valid = false;
                    if (pasta === 'PEPSICO') valid = true;
                    else if (pasta === 'ELMA') valid = ['707', '708', '752'].includes(sup);
                    else if (pasta === 'FOODS') valid = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'].includes(sup) || sup === '1119';

                    if (valid) {
                        if (sup === '1119') {
                            goalKeys.push('1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO');
                        } else {
                            goalKeys.push(sup);
                        }
                    }
                });
            } else {
                // Default Pasta Groups
                if (pasta === 'PEPSICO') {
                    goalKeys = ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                } else if (pasta === 'ELMA') {
                    goalKeys = ['707', '708', '752'];
                } else if (pasta === 'FOODS') {
                    goalKeys = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                }
            }

            // 1. Clients Filter
            // Apply Hierarchy Logic + "Active" Filter logic
            let clients = getHierarchyFilteredClients('meta-realizado', allClientsData).filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                // Same active logic as Goals
                if (isAmericanas) return true;
                if (rca1 === '53') return false;
                if (rca1 === '') return false;
                return true;
            });

            // Implement Supplier Filter Logic (Virtual IDs for Foods) - Step 7
            // Goals are derived from `globalClientGoals` and manual overrides.
            // To ensure consistency, both the base client list and the goal calculation must respect all active filters.

            const filteredClientCodes = new Set(clients.map(c => String(c['Código'] || c['codigo_cliente'])));

            // 2. Goals Aggregation (By Seller)
            // Structure: Map<SellerName, TotalGoal>
            // 2. Goals Aggregation (By Seller)
            // Structure: Map<SellerName, { totalFat: 0, totalVol: 0, totalPos: 0 }>
            const goalsBySeller = new Map();

            clients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const rcaCode = String(client.rca1 || '');
                const rcaName = optimizedData.rcaNameByCode.get(rcaCode) || rcaCode; // Map code to name for grouping

                // Filtering "Garbage" Sellers to fix Total Positivação (1965 vs 1977)
                if (isGarbageSeller(rcaName)) return;

                // Goal Keys are now determined at function scope (hoisted)

                if (globalClientGoals.has(codCli)) {
                    const clientGoals = globalClientGoals.get(codCli);
                    let clientTotalFatGoal = 0;
                    let clientTotalVolGoal = 0;
                    let hasGoal = false;

                    goalKeys.forEach(k => {
                        if (clientGoals.has(k)) {
                            const g = clientGoals.get(k);
                            clientTotalFatGoal += (g.fat || 0);
                            clientTotalVolGoal += (g.vol || 0);
                            if ((g.fat || 0) > 0) hasGoal = true;
                        }
                    });

                    if (!goalsBySeller.has(rcaName)) {
                        goalsBySeller.set(rcaName, { totalFat: 0, totalVol: 0, totalPos: 0 });
                    }
                    const sellerGoals = goalsBySeller.get(rcaName);

                    if (clientTotalFatGoal > 0) sellerGoals.totalFat += clientTotalFatGoal;
                    if (clientTotalVolGoal > 0) sellerGoals.totalVol += clientTotalVolGoal;
                    if (hasGoal) sellerGoals.totalPos += 1; // Count client as 1 target
                }
            });

            // Apply Positivation Overrides from goalsSellerTargets (Imported Absolute Values)
            // Apply Overrides from goalsSellerTargets (Imported Absolute Values for Pos, Fat, Vol)

            // --- FIX: Ensure all sellers with Manual Targets are present in goalsBySeller ---
            goalsSellerTargets.forEach((targets, sellerName) => {
                // Check if seller matches current filters
                if (supervisorsSet.size > 0) {
                    const supervisorName = (sellerDetailsMap.get(optimizedData.rcaCodeByName.get(sellerName) || '') || {}).supervisor;
                    if (!supervisorName || !supervisorsSet.has(supervisorName)) return;
                }
                if (sellersSet.size > 0 && !sellersSet.has(sellerName)) return;

                // Add to map if missing
                if (!goalsBySeller.has(sellerName)) {
                    goalsBySeller.set(sellerName, { totalFat: 0, totalVol: 0, totalPos: 0 });
                }
            });
            // ---------------------------------------------------------------------------------

            goalsBySeller.forEach((goals, sellerName) => {
                const targets = goalsSellerTargets.get(sellerName);
                if (targets) {
                    // 1. Positivação Overrides
                    let overrideKey = null;

                    // Improved Override Logic: Only apply aggregate pasta targets if NO specific supplier filter is active,
                    // or if all suppliers of that pasta are selected.
                    const elmaKeys = ['707', '708', '752'];
                    const foodsKeys = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                    const allElmaSelected = elmaKeys.every(k => goalKeys.includes(k));
                    const allFoodsSelected = foodsKeys.every(k => goalKeys.includes(k));

                    if (suppliersSet.size === 0) {
                        if (pasta === 'PEPSICO') {
                            overrideKey = targets['pepsico_all'] !== undefined ? 'pepsico_all' : 'GERAL';
                        } else if (pasta === 'ELMA') {
                            overrideKey = 'total_elma';
                        } else if (pasta === 'FOODS') {
                            overrideKey = 'total_foods';
                        }
                    } else if (suppliersSet.size === 1) {
                        const sup = [...suppliersSet][0];
                        if (sup === '1119_TODDYNHO') overrideKey = '1119_TODDYNHO';
                        else if (sup === '1119_TODDY') overrideKey = '1119_TODDY';
                        else if (sup === '1119_QUAKER' || sup === '1119_QUAKER_KEROCOCO') overrideKey = '1119_QUAKER_KEROCOCO';
                        else if (sup === '1119') {
                             if (allFoodsSelected) overrideKey = 'total_foods';
                        }
                        else overrideKey = sup;
                    } else {
                        // Multiple suppliers selected - Only use aggregate if it matches the selection
                        if (pasta === 'ELMA' && allElmaSelected) {
                            overrideKey = 'total_elma';
                        } else if (pasta === 'FOODS' && allFoodsSelected) {
                            overrideKey = 'total_foods';
                        } else if (pasta === 'PEPSICO' && allElmaSelected && allFoodsSelected) {
                            overrideKey = targets['pepsico_all'] !== undefined ? 'pepsico_all' : 'GERAL';
                        }
                    }

                    if (overrideKey && targets[overrideKey] !== undefined) {
                        goals.totalPos = targets[overrideKey];
                    }

                    // 2. Revenue (FAT) and Volume (VOL) Overrides
                    let overrideFat = 0;
                    let overrideVol = 0;
                    let hasOverrideFat = false;
                    let hasOverrideVol = false;

                    // Strategy:
                    // 1. Sum individual targets for all selected goalKeys
                    goalKeys.forEach(k => {
                        if (targets[`${k}_FAT`] !== undefined) {
                            overrideFat += targets[`${k}_FAT`];
                            hasOverrideFat = true;
                        }
                        if (targets[`${k}_VOL`] !== undefined) {
                            overrideVol += targets[`${k}_VOL`];
                            hasOverrideVol = true;
                        }
                    });

                    // 2. Aggregate fallbacks: If individual targets are missing, check for aggregate keys
                    // but ONLY if the current selection matches the aggregate (full pasta or no filter)
                    const noSupplierFilter = suppliersSet.size === 0;

                    // Pepsico / Elma Fallbacks
                    if (noSupplierFilter || allElmaSelected) {
                        const hasIndividualElmaFat = elmaKeys.some(k => targets[`${k}_FAT`] !== undefined);
                        if (!hasIndividualElmaFat && targets['total_elma_FAT'] !== undefined) {
                            overrideFat += targets['total_elma_FAT'];
                            hasOverrideFat = true;
                        }
                        const hasIndividualElmaVol = elmaKeys.some(k => targets[`${k}_VOL`] !== undefined);
                        if (!hasIndividualElmaVol && targets['tonelada_elma_VOL'] !== undefined) {
                            overrideVol += targets['tonelada_elma_VOL'];
                            hasOverrideVol = true;
                        }
                    }

                    // Pepsico / Foods Fallbacks
                    if (noSupplierFilter || allFoodsSelected) {
                        const hasIndividualFoodsFat = foodsKeys.some(k => targets[`${k}_FAT`] !== undefined);
                        if (!hasIndividualFoodsFat && targets['total_foods_FAT'] !== undefined) {
                            overrideFat += targets['total_foods_FAT'];
                            hasOverrideFat = true;
                        }
                        const hasIndividualFoodsVol = foodsKeys.some(k => targets[`${k}_VOL`] !== undefined);
                        if (!hasIndividualFoodsVol && targets['tonelada_foods_VOL'] !== undefined) {
                            overrideVol += targets['tonelada_foods_VOL'];
                            hasOverrideVol = true;
                        }
                    }

                    if (hasOverrideFat) goals.totalFat = overrideFat;
                    if (hasOverrideVol) goals.totalVol = overrideVol;
                }
            });

            // 3. Sales Aggregation (By Seller & Week)
            // Structure: Map<SellerName, { totalFat: 0, totalVol: 0, weeksFat: [], weeksVol: [] }>
            const salesBySeller = new Map();
            const { weeks } = getMonthWeeksDistribution(lastSaleDate); // Use current global date context

            // Helper to find week index
            const getWeekIndex = (date) => {
                const d = typeof date === 'number' ? new Date(date) : parseDate(date);
                if (!d) return -1;
                // Check against ranges
                for(let i=0; i<weeks.length; i++) {
                    // Week range is inclusive start, inclusive end
                    if (d >= weeks[i].start && d <= weeks[i].end) return i;
                }
                return -1;
            };

            // Iterate Sales
            // Optimized: Use indices if needed, or simple iteration.
            // Filter: Month, Types != 5,11, Pasta, Supervisor/Seller/Supplier
            const currentMonthIndex = lastSaleDate.getUTCMonth();
            const currentYear = lastSaleDate.getUTCFullYear();

            // Cache for Positivação Logic (Unique Clients per Seller)
            const sellerClients = new Map(); // Map<SellerName, Set<CodCli>>

            for(let i=0; i<allSalesData.length; i++) {
                const s = allSalesData instanceof ColumnarDataset ? allSalesData.get(i) : allSalesData[i];

                // Date Filter
                const d = typeof s.DTPED === 'number' ? new Date(s.DTPED) : parseDate(s.DTPED);
                if (!d || d.getUTCMonth() !== currentMonthIndex || d.getUTCFullYear() !== currentYear) continue;

                // Type Filter (Types 5 and 11 excluded)
                const tipo = String(s.TIPOVENDA);
                if (tipo === '5' || tipo === '11') continue;

                // Pasta Filter (OBSERVACAOFOR) logic for Pepsico/Elma/Foods
                // 1. Determine if row is PEPSICO/MULTIMARCAS
                let rowPasta = s.OBSERVACAOFOR;
                if (!rowPasta || rowPasta === '0' || rowPasta === '00' || rowPasta === 'N/A') {
                     const rawFornecedor = String(s.FORNECEDOR || '').toUpperCase();
                     rowPasta = rawFornecedor.includes('PEPSICO') ? 'PEPSICO' : 'MULTIMARCAS';
                }

                // "Meta Vs. Realizado" only cares about PEPSICO data
                if (rowPasta !== 'PEPSICO') continue;

                // 2. Check Sub-pasta logic (ELMA vs FOODS) based on CODFOR
                // If filter is PEPSICO, we include everything (since we already filtered for PEPSICO above)
                // If filter is ELMA, we check CODFOR 707, 708, 752
                // If filter is FOODS, we check CODFOR 1119 (or specific sub-brands if needed, but usually 1119 is Foods)

                const codFor = String(s.CODFOR);
                if (pasta === 'ELMA') {
                    if (!['707', '708', '752'].includes(codFor)) continue;
                } else if (pasta === 'FOODS') {
                    if (codFor !== '1119') continue;
                }
                // If pasta === 'PEPSICO', we include all (already filtered for PEPSICO rowPasta)

                // Client Filter (Must be in the filtered list of clients? Or just match filters?)
                // If we filtered clients by Supervisor/Seller, we should only count sales for those clients?
                // Or sales where the sale's Supervisor/Seller matches?
                // Usually Sales view filters by Sale attributes. Goals view filters by Client attributes.
                // "Meta Vs Realizado" implies comparing the same entity.
                // If I filter Supervisor "X", I show Seller Goals for X and Seller Sales for X.
                // Let's use the standard filter logic:

                if (supervisorsSet.size > 0 && !supervisorsSet.has(s.SUPERV)) continue;
                if (sellersSet.size > 0 && !sellersSet.has(s.NOME)) continue;

                // Client Filter: Ensure sale belongs to the same set of clients used for goals
                if (!filteredClientCodes.has(String(s.CODCLI))) continue;

                // Enhanced Supplier Logic to handle Virtual Foods Categories
                if (suppliersSet.size > 0) {
                    let supplierMatch = false;
                    const codFor = String(s.CODFOR);

                    // 1. Direct Match (Regular Suppliers)
                    if (suppliersSet.has(codFor)) {
                        supplierMatch = true;
                    }
                    // 2. Virtual Category Logic for 1119 (Foods)
                    else if (codFor === '1119') {
                        const desc = normalize(s.DESCRICAO || '');
                        if (suppliersSet.has('1119_TODDYNHO') && desc.includes('TODDYNHO')) supplierMatch = true;
                        else if (suppliersSet.has('1119_TODDY') && desc.includes('TODDY') && !desc.includes('TODDYNHO')) supplierMatch = true;
                        else if (suppliersSet.has('1119_QUAKER_KEROCOCO') && (desc.includes('QUAKER') || desc.includes('KEROCOCO'))) supplierMatch = true;
                    }

                    if (!supplierMatch) continue;
                }

                const sellerName = s.NOME;
                const valFat = Number(s.VLVENDA) || 0;
                const valVol = Number(s.TOTPESOLIQ) || 0;
                const weekIdx = getWeekIndex(d);

                if (!salesBySeller.has(sellerName)) {
                    salesBySeller.set(sellerName, { totalFat: 0, totalVol: 0, weeksFat: [0, 0, 0, 0, 0], weeksVol: [0, 0, 0, 0, 0], totalPos: 0 });
                }
                const entry = salesBySeller.get(sellerName);

                entry.totalFat += valFat;
                entry.totalVol += valVol;

                if (weekIdx !== -1 && weekIdx < 5) {
                    entry.weeksFat[weekIdx] += valFat;
                    entry.weeksVol[weekIdx] += valVol;
                }

                // Positivação Logic (Accumulate Clients)
                if (!sellerClients.has(sellerName)) sellerClients.set(sellerName, new Set());
                sellerClients.get(sellerName).add(String(s.CODCLI));
            }

            // Finalize Positivação Counts
            sellerClients.forEach((clientSet, sel) => {
                if (salesBySeller.has(sel)) {
                    salesBySeller.get(sel).totalPos = clientSet.size;
                }
            });

            return { goalsBySeller, salesBySeller, weeks };
        }

        function renderMetaRealizadoTable(data, weeks, totalWorkingDays) {
            const tableHead = document.getElementById('meta-realizado-table-head');
            const tableBody = document.getElementById('meta-realizado-table-body');

            // Build Headers
            let headerHTML = `
                <tr>
                    <th rowspan="2" class="px-3 py-2 text-left bg-[#161e3d] z-50 sticky left-0 border-r border-b border-slate-700 w-32 shadow-lg">VENDEDOR</th>
                    <th colspan="2" class="px-2 py-1 text-center bg-blue-900/30 text-blue-400 border-r border-slate-700 border-b-0">GERAL</th>
            `;

            // Week Headers (Top Row)
            weeks.forEach((week, i) => {
                headerHTML += `<th colspan="2" class="px-2 py-1 text-center border-r border-slate-700 border-b-0 text-slate-300">SEMANA ${i + 1} (${week.workingDays}d)</th>`;
            });
            headerHTML += `</tr><tr>`;

            // Sub-headers Row
            // Geral Sub-headers
            headerHTML += `
                <th class="px-2 py-2 text-right bg-blue-900/20 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">META</th>
                <th class="px-2 py-2 text-right bg-blue-900/20 text-blue-100 font-bold border-r border-b border-slate-700 text-[10px]">REALIZADO</th>
            `;

            // Week Sub-headers
            weeks.forEach(() => {
                headerHTML += `
                    <th class="px-2 py-2 text-right border-r border-b border-slate-700/50 text-slate-400 text-[10px]">META</th>
                    <th class="px-2 py-2 text-right border-r border-b border-slate-700 text-white font-bold text-[10px]">REAL.</th>
                `;
            });
            headerHTML += `</tr>`;

            tableHead.innerHTML = headerHTML;

            // Build Body
            // data is Array of { name, metaTotal, realTotal, weeks: [{meta, real}] }
            const rowsHTML = data.map(row => {
                const metaTotalStr = row.metaTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                const realTotalStr = row.realTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

                let cells = `
                    <td class="px-3 py-2 font-medium text-slate-200 border-r border-b border-slate-700 sticky left-0 bg-[#1d2347] z-30 truncate" title="${row.name}">${getFirstName(row.name)}</td>
                    <td class="px-2 py-2 text-right bg-blue-900/10 text-teal-400 border-r border-b border-slate-700/50 text-xs" title="Meta Contratual Mensal">${metaTotalStr}</td>
                    <td class="px-2 py-2 text-right bg-blue-900/10 text-yellow-400 font-bold border-r border-b border-slate-700 text-xs">${realTotalStr}</td>
                `;

                row.weekData.forEach(w => {
                    const wMetaStr = w.meta.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                    const wRealStr = w.real.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

                    // Simple logic: Green if Real >= Meta, Red if Real < Meta (only if week has passed? Or always?)
                    // Let's keep it neutral for now or simple colors.
                    const realClass = w.real >= w.meta ? 'text-green-400' : 'text-slate-300';
                    const metaClass = w.isPast ? 'text-red-500' : 'text-slate-400';

                    cells += `
                        <td class="px-2 py-3 text-right ${metaClass} text-xs border-r border-b border-slate-700">${wMetaStr}</td>
                        <td class="px-2 py-3 text-right ${realClass} text-xs font-medium border-r border-b border-slate-700">${wRealStr}</td>
                    `;
                });

                return `<tr class="hover:bg-slate-700/30 transition-colors">${cells}</tr>`;
            }).join('');

            tableBody.innerHTML = rowsHTML;
        }

        function renderMetaRealizadoChart(data) {
            const ctx = document.getElementById('metaRealizadoChartContainer');
            if (!ctx) return;

            // Destroy previous chart if exists (assume we store it in charts object)
            // Wait, createChart helper handles destruction if we pass ID. But here we have container ID.
            // Let's use a canvas inside the container.

            let canvas = ctx.querySelector('canvas');
            if (!canvas) {
                canvas = document.createElement('canvas');
                ctx.appendChild(canvas);
            }

            const chartId = 'metaRealizadoChartInstance';

            // Aggregate totals for the chart (Total Meta vs Total Realizado)
            const totalMeta = data.reduce((sum, d) => sum + d.metaTotal, 0);
            const totalReal = data.reduce((sum, d) => sum + d.realTotal, 0);

            // Adjust formatting based on metric
            const isVolume = currentMetaRealizadoMetric === 'peso';

            // If Volume, display in Tons (input was Kg)
            const displayTotalMeta = isVolume ? totalMeta / 1000 : totalMeta;
            const displayTotalReal = isVolume ? totalReal / 1000 : totalReal;

            const formatValue = (val) => {
                if (isVolume) {
                    return val.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 }) + ' Ton';
                }
                return val.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
            };

            if (charts[chartId]) {
                charts[chartId].data.datasets[0].data = [displayTotalMeta];
                charts[chartId].data.datasets[1].data = [displayTotalReal];
                // Update formatters closure
                charts[chartId].options.plugins.tooltip.callbacks.label = function(context) {
                    let label = context.dataset.label || '';
                    if (label) label += ': ';
                    if (context.parsed.y !== null) label += formatValue(context.parsed.y);
                    return label;
                };
                charts[chartId].options.plugins.datalabels.formatter = formatValue;
                charts[chartId].update('none');
            } else {
                charts[chartId] = new Chart(canvas, {
                    type: 'bar',
                    data: {
                        labels: ['Total'],
                        datasets: [
                            {
                                label: 'Meta',
                                data: [displayTotalMeta],
                                backgroundColor: '#14b8a6', // Teal
                                barPercentage: 0.6,
                                categoryPercentage: 0.8
                            },
                            {
                                label: 'Realizado',
                                data: [displayTotalReal],
                                backgroundColor: '#f59e0b', // Amber/Yellow
                                barPercentage: 0.6,
                                categoryPercentage: 0.8
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        layout: {
                            padding: {
                                top: 50
                            }
                        },
                        plugins: {
                            legend: { position: 'top', labels: { color: '#cbd5e1' } },
                            tooltip: {
                                callbacks: {
                                    label: function(context) {
                                        let label = context.dataset.label || '';
                                        if (label) {
                                            label += ': ';
                                        }
                                        if (context.parsed.y !== null) {
                                            label += formatValue(context.parsed.y);
                                        }
                                        return label;
                                    }
                                }
                            },
                            datalabels: {
                                display: true,
                                color: '#fff',
                                anchor: 'end',
                                align: 'top',
                                formatter: formatValue,
                                font: { weight: 'bold' }
                            }
                        },
                        scales: {
                            y: {
                                beginAtZero: true,
                                grace: '10%',
                                grid: { color: '#334155' },
                                ticks: { color: '#94a3b8' }
                            },
                            x: {
                                grid: { display: false },
                                ticks: { color: '#94a3b8' }
                            }
                        }
                    }
                });
            }
        }

        let metaRealizadoClientsTableState = {
            currentPage: 1,
            itemsPerPage: 50,
            filteredData: [],
            totalPages: 1
        };

        function updateMetaRealizadoView() {
            // 1. Get Data
            const { goalsBySeller, salesBySeller, weeks } = getMetaRealizadoFilteredData();

            // Re-calculate Total Working Days
            let totalWorkingDays = weeks.reduce((sum, w) => sum + w.workingDays, 0);
            if (totalWorkingDays === 0) totalWorkingDays = 1;

            // 2. Combine Data for Rendering (Sellers)
            const allSellers = new Set([...goalsBySeller.keys(), ...salesBySeller.keys()]);
            const rowData = [];

            allSellers.forEach(sellerName => {
                const goals = goalsBySeller.get(sellerName) || { totalFat: 0, totalVol: 0, totalPos: 0 };
                const sales = salesBySeller.get(sellerName) || { totalFat: 0, totalVol: 0, weeksFat: [], weeksVol: [], totalPos: 0 };

                // Determine which metric to use for the main chart/table
                // Note: The Table logic (renderMetaRealizadoTable) seems built for ONE metric (previously just Revenue).
                // If we want the Table to also toggle or show both, we need to adjust it.
                // Given the requirement "toggle button for R$/Ton", let's make the Table adhere to that too.

                let targetTotalGoal = 0;
                let targetRealizedTotal = 0;
                let targetRealizedWeeks = [];

                if (currentMetaRealizadoMetric === 'valor') {
                    targetTotalGoal = goals.totalFat;
                    targetRealizedTotal = sales.totalFat;
                    targetRealizedWeeks = sales.weeksFat || [];
                } else {
                    targetTotalGoal = goals.totalVol; // Kg
                    targetRealizedTotal = sales.totalVol; // Kg
                    targetRealizedWeeks = sales.weeksVol || [];
                }

                // Positivação Data
                const posGoal = goals.totalPos;
                const posRealized = sales.totalPos;

                // Calculate Dynamic Weekly Goals (For the selected metric)
                const adjustedGoals = calculateAdjustedWeeklyGoals(targetTotalGoal, targetRealizedWeeks, weeks);

                const weekData = weeks.map((w, i) => {
                    const wMeta = adjustedGoals[i];
                    const wReal = targetRealizedWeeks[i] || 0;
                    const isPast = w.end < lastSaleDate;
                    return { meta: wMeta, real: wReal, isPast: isPast };
                });

                rowData.push({
                    name: sellerName,
                    metaTotal: targetTotalGoal,
                    realTotal: targetRealizedTotal,
                    weekData: weekData,
                    // Additional Data for Positivação Chart
                    posGoal: posGoal,
                    posRealized: posRealized
                });
            });

            // Sort by Meta Total Descending
            rowData.sort((a, b) => b.metaTotal - a.metaTotal);

            // 3. Render Sellers Table & Chart
            renderMetaRealizadoTable(rowData, weeks, totalWorkingDays);
            renderMetaRealizadoChart(rowData); // Chart 1 (Selected Metric)
            renderMetaRealizadoPosChart(rowData); // Chart 2 (Positivação)

            // 4. Clients Table Processing
            const clientsData = getMetaRealizadoClientsData(weeks);

            // 5. Save Data for Export
            metaRealizadoDataForExport = {
                sellers: rowData,
                clients: clientsData,
                weeks: weeks
            };

            metaRealizadoClientsTableState.filteredData = clientsData;
            metaRealizadoClientsTableState.totalPages = Math.ceil(clientsData.length / metaRealizadoClientsTableState.itemsPerPage);

            // Validate Current Page
            if (metaRealizadoClientsTableState.currentPage > metaRealizadoClientsTableState.totalPages) {
                metaRealizadoClientsTableState.currentPage = metaRealizadoClientsTableState.totalPages > 0 ? metaRealizadoClientsTableState.totalPages : 1;
            }
            if (metaRealizadoClientsTableState.totalPages === 0) metaRealizadoClientsTableState.currentPage = 1;

            renderMetaRealizadoClientsTable(clientsData, weeks);
        }

        function getMetaRealizadoClientsData(weeks) {
            // New Hierarchy Logic
            const currentMonthIndex = lastSaleDate.getUTCMonth();
            const currentYear = lastSaleDate.getUTCFullYear();
            const suppliersSet = new Set(selectedMetaRealizadoSuppliers);
            const pasta = currentMetaRealizadoPasta;

            // --- Fix: Define Filter Sets from Hierarchy State ---
            const supervisorsSet = new Set();
            const sellersSet = new Set();
            
            const hState = hierarchyState['meta-realizado'];
            if (hState) {
                // If Coords selected, filter by them
                if (hState.coords.size > 0) {
                    hState.coords.forEach(c => {
                        const name = optimizedData.coordMap.get(c);
                        if(name) supervisorsSet.add(name);
                    });
                }
                // If Promotors selected (Sellers), filter by them
                if (hState.promotors.size > 0) {
                    hState.promotors.forEach(p => {
                        const name = optimizedData.promotorMap.get(p);
                        if(name) sellersSet.add(name);
                    });
                }
            }
            // ----------------------------------------------------

            // 1. Identify Target Clients (Active/Americanas/etc + Filtered)
            // Apply Hierarchy Logic + "Active" Filter logic
            let clients = getHierarchyFilteredClients('meta-realizado', allClientsData).filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                if (isAmericanas) return true;
                if (rca1 === '53') return false;
                if (rca1 === '') return false;
                return true;
            });

            // Optimization: Create Set of Client Codes
            const allowedClientCodes = new Set(clients.map(c => String(c['Código'] || c['codigo_cliente'])));

            // 2. Aggregate Data per Client
            const clientMap = new Map(); // Map<CodCli, { clientObj, goal: 0, salesTotal: 0, salesWeeks: [] }>

            // Determine Goal Keys based on Pasta (Copy logic)
            let goalKeys = [];
            if (pasta === 'PEPSICO') goalKeys = ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
            else if (pasta === 'ELMA') goalKeys = ['707', '708', '752'];
            else if (pasta === 'FOODS') goalKeys = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];

            // A. Populate Goals
            clients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                if (!clientMap.has(codCli)) {
                    clientMap.set(codCli, { clientObj: client, goal: 0, salesTotal: 0, salesWeeks: new Array(weeks.length).fill(0) });
                }
                const entry = clientMap.get(codCli);

                if (globalClientGoals.has(codCli)) {
                    const cGoals = globalClientGoals.get(codCli);
                    goalKeys.forEach(k => {
                        if (cGoals.has(k)) entry.goal += (cGoals.get(k).fat || 0);
                    });
                }
            });

            // B. Populate Sales (Iterate ALL Sales to catch those without Meta)
            // Filter Logic matches 'getMetaRealizadoFilteredData'

            // Helper for week index (Copied from getMetaRealizadoFilteredData scope, need to redefine or reuse)
            const getWeekIndex = (date) => {
                const d = typeof date === 'number' ? new Date(date) : parseDate(date);
                if (!d) return -1;
                for(let i=0; i<weeks.length; i++) {
                    if (d >= weeks[i].start && d <= weeks[i].end) return i;
                }
                return -1;
            };

            for(let i=0; i<allSalesData.length; i++) {
                const s = allSalesData instanceof ColumnarDataset ? allSalesData.get(i) : allSalesData[i];
                const d = typeof s.DTPED === 'number' ? new Date(s.DTPED) : parseDate(s.DTPED);

                // Basic Filters
                if (!d || d.getUTCMonth() !== currentMonthIndex || d.getUTCFullYear() !== currentYear) continue;
                const tipo = String(s.TIPOVENDA);
                if (tipo === '5' || tipo === '11') continue;

                // Pasta Filter
                let rowPasta = s.OBSERVACAOFOR;
                if (!rowPasta || rowPasta === '0' || rowPasta === '00' || rowPasta === 'N/A') {
                     const rawFornecedor = String(s.FORNECEDOR || '').toUpperCase();
                     rowPasta = rawFornecedor.includes('PEPSICO') ? 'PEPSICO' : 'MULTIMARCAS';
                }
                if (rowPasta !== 'PEPSICO') continue;

                const codFor = String(s.CODFOR);
                if (pasta === 'ELMA' && !['707', '708', '752'].includes(codFor)) continue;
                if (pasta === 'FOODS' && codFor !== '1119') continue;

                // Supervisor/Seller/Supplier Filter on SALE row
                if (supervisorsSet.size > 0 && !supervisorsSet.has(s.SUPERV)) continue;
                if (sellersSet.size > 0 && !sellersSet.has(s.NOME)) continue;
                if (suppliersSet.size > 0 && !suppliersSet.has(s.CODFOR)) continue;

                const codCli = String(s.CODCLI);
                // Check if client is in allowed list (Active/Filtered)
                // Note: User said "todos os clientes que possuírem metas OU vendas".
                // If a client has sales but was filtered out by "Active" check (e.g. Inactive RCA), should they appear?
                // Usually yes, sales override status.
                // However, we are filtering by Supervisor/Seller above.

                // Logic: If I filtered by Supervisor X, and Sale is by Supervisor X, I include it.
                // But do I include the Client Object?
                // If the client wasn't in 'clients' array (e.g. RCA 53?), we might miss metadata.
                // We should fetch client metadata from allClientsData map if missing.

                if (!clientMap.has(codCli)) {
                    // Try to find client object
                    const clientObj = clientMapForKPIs.get(codCli) || { 'Código': codCli, nomeCliente: 'DESCONHECIDO', cidade: 'N/A', rca1: 'N/A' };
                    // If we apply STRICT Supervisor/Seller filter, we should check if this sale matches.
                    // We already checked sale attributes above. So this sale is valid for the view.
                    clientMap.set(codCli, { clientObj: clientObj, goal: 0, salesTotal: 0, salesWeeks: new Array(weeks.length).fill(0) });
                }

                const entry = clientMap.get(codCli);
                const val = Number(s.VLVENDA) || 0;
                const weekIdx = getWeekIndex(d);

                entry.salesTotal += val;
                if (weekIdx !== -1) entry.salesWeeks[weekIdx] += val;
            }

            // 3. Transform to Array and Calculate Dynamic Goals
            const results = [];
            clientMap.forEach((data, codCli) => {
                // Filter out if No Goal AND No Sales (Clean up empty active clients)
                if (data.goal === 0 && data.salesTotal === 0) return;

                const adjustedGoals = calculateAdjustedWeeklyGoals(data.goal, data.salesWeeks, weeks);

                const weekData = weeks.map((w, i) => {
                    const isPast = w.end < lastSaleDate;
                    return { meta: adjustedGoals[i], real: data.salesWeeks[i], isPast: isPast };
                });

                // Resolve Vendor Name
                let vendorName = 'N/A';
                const rcaCode = (data.clientObj.rcas && data.clientObj.rcas.length > 0) ? data.clientObj.rcas[0] : (data.clientObj.rca1 || 'N/A');
                if (rcaCode !== 'N/A') {
                    vendorName = optimizedData.rcaNameByCode.get(String(rcaCode)) || rcaCode;
                }

                let nomeExibicao = data.clientObj.nomeCliente || data.clientObj.razaoSocial || 'N/A';
                if (nomeExibicao.toUpperCase().includes('AMERICANAS')) {
                    const fantasia = data.clientObj.fantasia || data.clientObj.FANTASIA || data.clientObj['Nome Fantasia'];
                    if (fantasia) {
                        nomeExibicao = fantasia;
                    }
                }

                results.push({
                    codcli: codCli,
                    razaoSocial: nomeExibicao,
                    cidade: data.clientObj.cidade || 'N/A',
                    vendedor: vendorName,
                    metaTotal: data.goal,
                    realTotal: data.salesTotal,
                    weekData: weekData
                });
            });

            // Sort: High Potential? High Sales?
            // Default: Meta Descending, then Sales Descending
            results.sort((a, b) => b.metaTotal - a.metaTotal || b.realTotal - a.realTotal);

            return results;
        }

        function renderMetaRealizadoClientsTable(data, weeks) {
            const tableHead = document.getElementById('meta-realizado-clients-table-head');
            const tableBody = document.getElementById('meta-realizado-clients-table-body');
            const controls = document.getElementById('meta-realizado-clients-pagination-controls');
            const infoText = document.getElementById('meta-realizado-clients-page-info-text');
            const prevBtn = document.getElementById('meta-realizado-clients-prev-page-btn');
            const nextBtn = document.getElementById('meta-realizado-clients-next-page-btn');

            // 1. Build Headers (Same logic as Seller Table but with Client Info)
            let headerHTML = `
                <tr>
                    <th rowspan="2" class="px-2 py-2 text-center bg-[#161e3d] border-r border-b border-slate-700 w-16">CÓD</th>
                    <th rowspan="2" class="px-3 py-2 text-left bg-[#161e3d] border-r border-b border-slate-700 min-w-[200px]">CLIENTE</th>
                    <th rowspan="2" class="px-3 py-2 text-left bg-[#161e3d] border-r border-b border-slate-700 w-32">VENDEDOR</th>
                    <th rowspan="2" class="px-3 py-2 text-left bg-[#161e3d] border-r border-b border-slate-700 w-32">CIDADE</th>
                    <th colspan="2" class="px-2 py-1 text-center bg-blue-900/30 text-blue-400 border-r border-slate-700 border-b-0">GERAL</th>
            `;

            weeks.forEach((week, i) => {
                headerHTML += `<th colspan="2" class="px-2 py-1 text-center border-r border-slate-700 border-b-0 text-slate-300">SEMANA ${i + 1}</th>`;
            });
            headerHTML += `</tr><tr>`;

            headerHTML += `
                <th class="px-2 py-2 text-right bg-blue-900/20 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">META</th>
                <th class="px-2 py-2 text-right bg-blue-900/20 text-blue-100 font-bold border-r border-b border-slate-700 text-[10px]">REALIZADO</th>
            `;

            weeks.forEach(() => {
                headerHTML += `
                    <th class="px-2 py-2 text-right border-r border-b border-slate-700/50 text-slate-400 text-[10px]">META</th>
                    <th class="px-2 py-2 text-right border-r border-b border-slate-700 text-white font-bold text-[10px]">REAL.</th>
                `;
            });
            headerHTML += `</tr>`;

            tableHead.innerHTML = headerHTML;

            // 2. Pagination Logic
            const startIndex = (metaRealizadoClientsTableState.currentPage - 1) * metaRealizadoClientsTableState.itemsPerPage;
            const endIndex = startIndex + metaRealizadoClientsTableState.itemsPerPage;
            const pageData = metaRealizadoClientsTableState.filteredData.slice(startIndex, endIndex);

            // 3. Build Body
            if (pageData.length === 0) {
                tableBody.innerHTML = `<tr><td colspan="${6 + (weeks.length * 2)}" class="px-4 py-8 text-center text-slate-500">Nenhum cliente encontrado com os filtros atuais.</td></tr>`;
            } else {
                const rowsHTML = pageData.map(row => {
                    const metaTotalStr = row.metaTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                    const realTotalStr = row.realTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

                    let cells = `
                        <td class="px-2 py-2 text-center text-slate-400 text-xs border-r border-b border-slate-700">${row.codcli}</td>
                        <td class="px-3 py-2 text-xs font-medium text-slate-200 border-r border-b border-slate-700 truncate" title="${escapeHtml(row.razaoSocial)}">${escapeHtml(row.razaoSocial)}</td>
                        <td class="px-3 py-2 text-xs text-slate-400 border-r border-b border-slate-700 truncate">${escapeHtml(getFirstName(row.vendedor))}</td>
                        <td class="px-3 py-2 text-xs text-slate-400 border-r border-b border-slate-700 truncate">${escapeHtml(row.cidade)}</td>
                        <td class="px-2 py-2 text-right bg-blue-900/10 text-teal-400 border-r border-b border-slate-700/50 text-xs" title="Meta Contratual Mensal">${metaTotalStr}</td>
                        <td class="px-2 py-2 text-right bg-blue-900/10 text-yellow-400 font-bold border-r border-b border-slate-700 text-xs">${realTotalStr}</td>
                    `;

                    row.weekData.forEach(w => {
                        const wMetaStr = w.meta.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                        const wRealStr = w.real.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                        const realClass = w.real >= w.meta && w.meta > 0 ? 'text-green-400' : 'text-slate-300';
                        const metaClass = w.isPast ? 'text-red-500' : 'text-slate-400';

                        cells += `
                            <td class="px-2 py-3 text-right ${metaClass} text-xs border-r border-b border-slate-700">${wMetaStr}</td>
                            <td class="px-2 py-3 text-right ${realClass} text-xs font-medium border-r border-b border-slate-700">${wRealStr}</td>
                        `;
                    });

                    return `<tr class="hover:bg-slate-700/30 transition-colors">${cells}</tr>`;
                }).join('');
                tableBody.innerHTML = rowsHTML;
            }

            // 4. Update Pagination Controls
            if (metaRealizadoClientsTableState.filteredData.length > 0) {
                infoText.textContent = `Página ${metaRealizadoClientsTableState.currentPage} de ${metaRealizadoClientsTableState.totalPages} (Total: ${metaRealizadoClientsTableState.filteredData.length} clientes)`;
                prevBtn.disabled = metaRealizadoClientsTableState.currentPage === 1;
                nextBtn.disabled = metaRealizadoClientsTableState.currentPage === metaRealizadoClientsTableState.totalPages;
                controls.classList.remove('hidden');
            } else {
                controls.classList.add('hidden');
            }
        }

        function calculateMetricsForClients(clientsList) {
            // Helper to init metrics structure
            const createMetric = () => ({
                fat: 0, vol: 0, prevFat: 0, prevVol: 0,
                prevClientsSet: new Set(),
                quarterlyPosClientsSet: new Set(),
                monthlyClientsSets: new Map()
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

            const clientCodes = new Set(clientsList.map(c => String(c['Código'] || c['codigo_cliente'])));

            clientCodes.forEach(codCli => {
                const historyIds = optimizedData.indices.history.byClient.get(codCli);
                const clientTotals = {};

                if (historyIds) {
                    historyIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
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

                        if (['707', '708', '752'].includes(codFor)) keysToProcess.push('ELMA_ALL');
                        if (codFor === '1119') keysToProcess.push('FOODS_ALL');
                        if (['707', '708', '752', '1119'].includes(codFor)) keysToProcess.push('PEPSICO_ALL');

                        keysToProcess.forEach(procKey => {
                            const d = parseDate(sale.DTPED);
                            const isPrevMonth = d && d.getUTCMonth() === prevMonthIndex && d.getUTCFullYear() === prevMonthYear;

                            if (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') {
                                metricsMap[procKey].fat += sale.VLVENDA;
                                metricsMap[procKey].vol += sale.TOTPESOLIQ;

                                if (isPrevMonth) {
                                    metricsMap[procKey].prevFat += sale.VLVENDA;
                                    metricsMap[procKey].prevVol += sale.TOTPESOLIQ;
                                }

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

                for (const key in clientTotals) {
                    const t = clientTotals[key];
                    if (t.globalFat >= 1) metricsMap[key].quarterlyPosClientsSet.add(codCli);
                    if (t.prevFat >= 1) metricsMap[key].prevClientsSet.add(codCli);
                    t.monthlyFat.forEach((val, mKey) => {
                        if (val >= 1) {
                            if (!metricsMap[key].monthlyClientsSets.has(mKey)) metricsMap[key].monthlyClientsSets.set(mKey, new Set());
                            metricsMap[key].monthlyClientsSets.get(mKey).add(codCli);
                        }
                    });
                }
            });

            const finalMetrics = {};
            for (const key in metricsMap) {
                const m = metricsMap[key];
                let sumClients = 0;
                m.monthlyClientsSets.forEach(set => sumClients += set.size);

                finalMetrics[key] = {
                    avgFat: m.fat / QUARTERLY_DIVISOR,
                    avgVol: m.vol / QUARTERLY_DIVISOR,
                    prevFat: m.prevFat,
                    prevVol: m.prevVol,
                    prevClients: m.prevClientsSet.size,
                    avgClients: sumClients / QUARTERLY_DIVISOR
                };
            }
            return finalMetrics;
        }

        // Wrapper for compatibility
        function getMetricsForSupervisors(supervisorsList) {
             let clients = allClientsData;
             if (supervisorsList && supervisorsList.length > 0) {
                 const rcas = new Set();
                 supervisorsList.forEach(sup => {
                     (optimizedData.rcasBySupervisor.get(sup) || []).forEach(r => rcas.add(r));
                 });
                 clients = clients.filter(c => c.rcas.some(r => rcas.has(r)));
             }
             clients = clients.filter(c => isActiveClient(c));
             return calculateMetricsForClients(clients);
        }

        function getSellerNaturalCount(sellerName, category) {
            const sellerCode = optimizedData.rcaCodeByName.get(sellerName);
            if (!sellerCode) return 0;

            const clients = optimizedData.clientsByRca.get(sellerCode) || [];
            const activeClients = clients.filter(c => {
                const cod = String(c['Código'] || c['codigo_cliente']);
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
            });

            let count = 0;

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);

                // For Mix Salty/Foods, we exclude Americanas from the base count (Seller 1001)
                const rca1 = String(client.rca1 || '').trim();
                if ((category === 'mix_salty' || category === 'mix_foods') && rca1 === '1001') return;

                const historyIds = optimizedData.indices.history.byClient.get(codCli);
                if (historyIds) {
                    let hasSale = false;

                    for (const id of historyIds) {
                        if (hasSale) break;
                        const sale = optimizedData.historyById.get(id);
                        // Exclude 9569 / 53 case
                        if (String(codCli).trim() === '9569' && (String(sale.CODUSUR).trim() === '53' || String(sale.CODUSUR).trim() === '053')) continue;

                        const isRev = (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9');
                        if (!isRev) continue;

                        const codFor = String(sale.CODFOR);
                        const desc = (sale.DESCRICAO || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase();

                        if (category === 'pepsico_all') {
                             if (['707', '708', '752'].includes(codFor) || (codFor === '1119' && (desc.includes('TODDYNHO') || desc.includes('TODDY') || desc.includes('QUAKER') || desc.includes('KEROCOCO')))) {
                                 hasSale = true;
                             }
                        } else if (category === 'total_elma') {
                             if (['707', '708', '752'].includes(codFor)) hasSale = true;
                        } else if (category === 'total_foods') {
                             if (codFor === '1119' && (desc.includes('TODDYNHO') || desc.includes('TODDY') || desc.includes('QUAKER') || desc.includes('KEROCOCO'))) hasSale = true;
                        } else if (category === '707' && codFor === '707') hasSale = true;
                        else if (category === '708' && codFor === '708') hasSale = true;
                        else if (category === '752' && codFor === '752') hasSale = true;
                        else if (category === '1119_TODDYNHO' && codFor === '1119' && desc.includes('TODDYNHO')) hasSale = true;
                        else if (category === '1119_TODDY' && codFor === '1119' && desc.includes('TODDY') && !desc.includes('TODDYNHO')) hasSale = true;
                        else if (category === '1119_QUAKER_KEROCOCO' && codFor === '1119' && (desc.includes('QUAKER') || desc.includes('KEROCOCO'))) hasSale = true;
                    }

                    if (hasSale) count++;
                }
            });
            return count;
        }

        function updateGoalsSummaryView() {
            const container = document.getElementById('goals-summary-grid');
            if (!container) return;

            // 1. Identify active sellers in the current summary filter
            let filteredSummaryClients = getHierarchyFilteredClients('goals-summary', allClientsData);

            // Apply "Active" Filter logic
            filteredSummaryClients = filteredSummaryClients.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                if (isAmericanas) return true;
                if (rca1 === '53') return false;
                if (rca1 === '') return false;
                return true;
            });

            // Calculate Metrics based on filtered clients
            // Since getMetricsForSupervisors only handles supervisor list, we need to rebuild metrics from scratch for arbitrary client list?
            // Or adapt getMetricsForSupervisors to accept client list?
            // getMetricsForSupervisors is a helper function I assume exists nearby? No, I don't see it in the grep.
            // Let's check if `getMetricsForSupervisors` exists. If not, the previous code block might have been hallucinated or I missed it.
            // Ah, line 4103: `const displayMetrics = getMetricsForSupervisors(selectedGoalsSummarySupervisors);`
            // Since I am modifying the filtering logic, I should likely implement a `getMetricsForClients(filteredSummaryClients)` or similar.
            // But let's look at `getMetricsForSupervisors` implementation first. It likely iterates `globalGoalsMetrics`?
            // No, `globalGoalsMetrics` is keyed by Product Category (707, etc.), NOT by Supervisor.
            // So `getMetricsForSupervisors` must be aggregating `globalClientGoals` for the filtered clients.

            // Let's assume we need to calculate display metrics from the filtered client list.
            const displayMetrics = calculateMetricsForClients(filteredSummaryClients);

            const activeSellersInSummary = new Set();
            filteredSummaryClients.forEach(c => {
                const rcaCode = String(c.rca1 || '').trim();
                if (rcaCode) {
                    const name = optimizedData.rcaNameByCode.get(rcaCode);
                    if (name) {
                        const upper = name.toUpperCase();
                        if (upper !== 'INATIVOS' && upper !== 'N/A' && !upper.includes('TOTAL') && !upper.includes('GERAL')) {
                            activeSellersInSummary.add(name);
                        }
                    }
                }
            });

            // 2. Sum up Revenue/Volume targets from `globalClientGoals` (Standard logic)
            const summaryGoalsSums = {
                '707': { fat: 0, vol: 0 },
                '708': { fat: 0, vol: 0 },
                '752': { fat: 0, vol: 0 },
                '1119_TODDYNHO': { fat: 0, vol: 0 },
                '1119_TODDY': { fat: 0, vol: 0 },
                '1119_QUAKER_KEROCOCO': { fat: 0, vol: 0 }
            };

            filteredSummaryClients.forEach(c => {
                const codCli = c['Código'];
                const rcaCode = String(c.rca1 || '').trim();
                let sellerName = null;
                if (rcaCode) sellerName = optimizedData.rcaNameByCode.get(rcaCode);

                if (sellerName && activeSellersInSummary.has(sellerName)) {
                    if (globalClientGoals.has(codCli)) {
                        const cGoals = globalClientGoals.get(codCli);
                        cGoals.forEach((val, key) => {
                            if (summaryGoalsSums[key]) {
                                summaryGoalsSums[key].fat += val.fat;
                                summaryGoalsSums[key].vol += val.vol;
                            }
                        });
                    }
                }
            });

            // 3. Helper to calculate Total Positivation Target for a Category
            // Checks if a manual target exists for the seller; otherwise, calculates default (Natural + Adjustment)
            const calcTotalPosTarget = (category) => {
                let total = 0;
                activeSellersInSummary.forEach(sellerName => {
                    // Check for explicit target in `goalsSellerTargets`
                    const targets = goalsSellerTargets.get(sellerName);

                    // If target exists (and is not null/undefined), use it.
                    // Note: Import logic sets targets.
                    if (targets && targets[category] !== undefined && targets[category] !== null) {
                        total += targets[category];
                    } else {
                        // Fallback: Default Calculation
                        // Logic mirrors calculateSellerDefaults but handles specific categories
                        // Special handling for Mix
                        if (category === 'mix_salty') {
                            const defaults = calculateSellerDefaults(sellerName);
                            // defaults.mixSalty already includes adjustments
                            total += defaults.mixSalty;
                        } else if (category === 'mix_foods') {
                            const defaults = calculateSellerDefaults(sellerName);
                            total += defaults.mixFoods;
                        } else {
                            // Standard Category
                            const natural = getSellerNaturalCount(sellerName, category);
                            const adjMap = goalsPosAdjustments[category];
                            const adj = adjMap ? (adjMap.get(sellerName) || 0) : 0;
                            total += Math.max(0, natural + adj);
                        }
                    }
                });
                return total;
            };

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

            const cardsHTML = summaryItems.map(item => {
                const key = item.supplier + (item.brand ? `_${item.brand}` : '');
                const target = summaryGoalsSums[key] || { fat: 0, vol: 0 };
                const metrics = displayMetrics[key] || { avgFat: 0, prevFat: 0 };

                let displayFat = target.fat;
                let displayVol = target.vol;

                if (displayFat < 0.01) displayFat = metrics.prevFat;
                if (displayVol < 0.001) displayVol = metrics.prevVol;

                totalFat += displayFat;
                totalVol += displayVol;

                // Calculate Pos Target using new Logic
                const posTarget = calcTotalPosTarget(key);

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
                                    ${posTarget.toLocaleString('pt-BR')}
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

            // Top Bar KPIs using same calculation logic
            const totalPosTarget = calcTotalPosTarget('pepsico_all'); // Use generic 'pepsico_all' key for Total Pos?
            // Note: Imported target for Total Pos usually comes as 'pepsico_all'.
            // If individual categories are set but not pepsico_all, what happens?
            // The user imports 'GERAL' which maps to 'pepsico_all'.
            if(totalPosEl) totalPosEl.textContent = totalPosTarget.toLocaleString('pt-BR');

            const mixSaltyTarget = calcTotalPosTarget('mix_salty');
            if(mixSaltyEl) mixSaltyEl.textContent = mixSaltyTarget.toLocaleString('pt-BR');

            const mixFoodsTarget = calcTotalPosTarget('mix_foods');
            if(mixFoodsEl) mixFoodsEl.textContent = mixFoodsTarget.toLocaleString('pt-BR');
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

                    // Strict Exclusion of INATIVOS from Base Calculation
                    if (sellerName === 'INATIVOS') continue;

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
            if (hierarchyState['goals-gv'] && (hierarchyState['goals-gv'].coords.size > 0 || hierarchyState['goals-gv'].promotors.size > 0)) {
                 // return 'filtro hierarquia';
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


        function exportGoalsGvPDF() {
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF('landscape');
            const data = goalsTableState.filteredData;

            if (!data || data.length === 0) {
                alert('Sem dados para exportar.');
                return;
            }

            const generationDate = new Date().toLocaleString('pt-BR');
            const supervisor = document.getElementById('goals-gv-supervisor-filter-text').textContent;
            const seller = document.getElementById('goals-gv-seller-filter-text').textContent;

            doc.setFontSize(18);
            doc.text('Relatório Rateio de Metas (GV)', 14, 22);
            doc.setFontSize(10);
            doc.setTextColor(100);
            doc.text(`Data de Emissão: ${generationDate}`, 14, 30);
            doc.text(`Filtros: Supervisor: ${supervisor} | Vendedor: ${seller}`, 14, 36);

            const monthLabels = quarterMonths.map(m => m.label);
            const head = [[
                'CÓD', 'CLIENTE', 'VEND',
                ...monthLabels,
                'MÉDIA R$', 'SHARE %', 'META R$',
                'META KG', 'MIX PDV'
            ]];

            const body = data.map(item => [
                item.cod,
                (item.name || '').substring(0, 25),
                getFirstName(item.seller),
                ...quarterMonths.map(m => (item.monthlyValues[m.key] || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})),
                item.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2}),
                (item.shareFat * 100).toFixed(2) + '%',
                item.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2}),
                item.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 3, maximumFractionDigits: 3}),
                (item.mixPdv || 0).toLocaleString('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})
            ]);

            doc.autoTable({
                head: head,
                body: body,
                startY: 45,
                theme: 'grid',
                styles: { fontSize: 7, cellPadding: 1, textColor: [0, 0, 0], halign: 'right' },
                headStyles: { fillColor: [20, 184, 166], textColor: 255, fontStyle: 'bold', fontSize: 8, halign: 'center' },
                columnStyles: {
                    0: { halign: 'center', cellWidth: 12 },
                    1: { halign: 'left', cellWidth: 40 },
                    2: { halign: 'left', cellWidth: 20 },
                    // Dynamic styling for months?
                },
                didParseCell: function(data) {
                    if (data.section === 'body' && data.column.index === head[0].length - 1) { // Mix PDV Column
                        data.cell.styles.fontStyle = 'bold';
                        data.cell.styles.textColor = [128, 0, 128]; // Purple
                    }
                }
            });

            let nameParam = '';
            // Simplified name param for now

            const safeFileNameParam = currentGoalsSupplier.replace(/[^a-z0-9]/gi, '_').toUpperCase();
            doc.save(`Metas_GV_${safeFileNameParam}${nameParam}.pdf`);
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
                'MÉDIA VOL (KG)', '% SHARE VOL', 'META VOL (KG)', 'MIX PDV'
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
                    item.mixPdv // Export calculated Mix PDV
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

            let nameParam = '';
            // Simplified name param for now

            const safeFileNameParam = currentGoalsSupplier.replace(/[^a-z0-9]/gi, '_').toUpperCase();
            XLSX.writeFile(wb, `Metas_GV_${safeFileNameParam}${nameParam}.xlsx`);
        }

        function getSellerTargetOverride(sellerName, metricType, context) {
            if (!goalsSellerTargets || !goalsSellerTargets.has(sellerName)) return null;
            const targets = goalsSellerTargets.get(sellerName);

            if (metricType === 'mix_salty') return targets['mix_salty'] !== undefined ? targets['mix_salty'] : null;
            if (metricType === 'mix_foods') return targets['mix_foods'] !== undefined ? targets['mix_foods'] : null;

            if (metricType === 'pos') {
                if (context === 'ELMA_ALL' || context === 'ELMA') return targets['total_elma'] !== undefined ? targets['total_elma'] : null;
                if (context === 'FOODS_ALL' || context === 'FOODS') return targets['total_foods'] !== undefined ? targets['total_foods'] : null;
                if (context === 'PEPSICO_ALL' || context === 'PEPSICO') {
                    // Check pepsico_all first (new standard) then GERAL (legacy)
                    if (targets['pepsico_all'] !== undefined) return targets['pepsico_all'];
                    if (targets['GERAL'] !== undefined) return targets['GERAL'];
                    return null;
                }
                // Check direct key match (e.g. 707, 708, 1119_TODDYNHO)
                if (targets[context] !== undefined) return targets[context];
            }
            return null;
        }

        // Helper to get historical positivation count for a seller/category
        function getHistoricalPositivation(sellerName, category) {
            const sellerCode = optimizedData.rcaCodeByName.get(sellerName);
            if (!sellerCode) return 0;

            const clients = optimizedData.clientsByRca.get(sellerCode) || [];
            // Filter active clients same as main view
            const activeClients = clients.filter(c => {
                const cod = String(c['Código'] || c['codigo_cliente']);
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
            });

            let count = 0;
            // Identify which products belong to this category
            // Reuse logic from 'shouldIncludeSale' or similar but specific to categories
            // Mapping Category -> Condition
            const checkSale = (codFor, desc) => {
                if (category === 'pepsico_all') return ['707', '708', '752', '1119'].includes(codFor);
                if (category === 'total_elma') return ['707', '708', '752'].includes(codFor);
                if (category === 'total_foods') return codFor === '1119';

                // Specifics
                if (category === '707') return codFor === '707';
                if (category === '708') return codFor === '708';
                if (category === '752') return codFor === '752';
                if (category === '1119_TODDYNHO') return codFor === '1119' && desc.includes('TODDYNHO');
                if (category === '1119_TODDY') return codFor === '1119' && desc.includes('TODDY') && !desc.includes('TODDYNHO');
                if (category === '1119_QUAKER_KEROCOCO') return codFor === '1119' && (desc.includes('QUAKER') || desc.includes('KEROCOCO'));

                return false;
            };

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const historyIds = optimizedData.indices.history.byClient.get(codCli);
                if (historyIds) {
                    // Check if client bought ANY product in category
                    for (let id of historyIds) {
                        const sale = optimizedData.historyById.get(id);
                        const codFor = String(sale.CODFOR);
                        const desc = normalize(sale.DESCRICAO || '');

                        // Check Rev Type only? Usually yes for Positivação.
                        if ((sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9') && checkSale(codFor, desc)) {
                            count++;
                            break; // Counted this client
                        }
                    }
                }
            });
            return count;
        }

        function distributeDown(sellerName, parentCategory, parentTargetValue) {
            // Recursive Cascade
            let children = [];
            if (parentCategory === 'pepsico_all') children = ['total_elma', 'total_foods'];
            else if (parentCategory === 'total_elma') children = ['707', '708', '752'];
            else if (parentCategory === 'total_foods') children = ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];

            if (children.length === 0) return;

            // 1. Get History for Children
            const childHistories = children.map(child => ({
                cat: child,
                hist: getHistoricalPositivation(sellerName, child)
            }));

            const parentHistory = getHistoricalPositivation(sellerName, parentCategory);

            childHistories.forEach(item => {
                let ratio = 0;
                if (parentHistory > 0) {
                    ratio = item.hist / parentHistory;
                }

                // New Target
                const childTarget = Math.round(parentTargetValue * ratio);

                // Update Seller Targets
                if (!goalsSellerTargets.has(sellerName)) goalsSellerTargets.set(sellerName, {});
                const t = goalsSellerTargets.get(sellerName);
                t[item.cat] = childTarget;

                // Recurse
                distributeDown(sellerName, item.cat, childTarget);
            });
        }

        function handleDistributePositivation(totalGoal, contextKey, filteredClientMetrics) {
            // filteredClientMetrics contains the list of sellers currently visible/active
            // We should distribute ONLY to them.

            // Map Context Key (Tab) to Target Key
            let targetKey = contextKey;
            if (contextKey === 'PEPSICO_ALL') targetKey = 'pepsico_all';
            if (contextKey === 'ELMA_ALL') targetKey = 'total_elma';
            if (contextKey === 'FOODS_ALL') targetKey = 'total_foods';

            // 1. Calculate Total History for THESE sellers in THIS context
            let totalHistoryPos = 0;
            const sellersHistory = [];

            // Group by Seller to avoid duplicates if clientMetrics has multiple rows per seller?
            // clientMetrics is PER CLIENT. So we need to aggregate unique sellers first.
            const uniqueSellers = new Set(filteredClientMetrics.map(c => c.seller));

            uniqueSellers.forEach(seller => {
                const hist = getHistoricalPositivation(seller, targetKey);
                sellersHistory.push({ seller, hist });
                totalHistoryPos += hist;
            });

            // 2. Distribute Total Goal
            // We use Largest Remainder Method or simple rounding? Simple rounding for now.

            sellersHistory.forEach(item => {
                let share = 0;
                if (totalHistoryPos > 0) {
                    share = item.hist / totalHistoryPos;
                }

                // If totalHistory is 0 but we have a Goal, distribute evenly?
                // Or leave 0? User said "proporcional". 0 history -> 0 share seems fair.

                const sellerTarget = Math.round(totalGoal * share);

                // Update Primary Target
                if (!goalsSellerTargets.has(item.seller)) goalsSellerTargets.set(item.seller, {});
                const t = goalsSellerTargets.get(item.seller);
                t[targetKey] = sellerTarget;

                // 3. Cascade Down
                distributeDown(item.seller, targetKey, sellerTarget);
            });

            // Auto-Redistribute Mix for PEPSICO_ALL context
            if (contextKey === 'PEPSICO_ALL') {
                const newSalty = Math.round(totalGoal * 0.50);
                const newFoods = Math.round(totalGoal * 0.30);

                // Update UI Inputs
                const inputSalty = document.getElementById('goal-global-mix-salty');
                const inputFoods = document.getElementById('goal-global-mix-foods');
                if(inputSalty) inputSalty.value = newSalty.toLocaleString('pt-BR');
                if(inputFoods) inputFoods.value = newFoods.toLocaleString('pt-BR');

                // Distribute Mix Targets
                // Note: handleDistributeMix calls updateGoalsView at the end.
                handleDistributeMix(newSalty, 'salty', contextKey, filteredClientMetrics);
                handleDistributeMix(newFoods, 'foods', contextKey, filteredClientMetrics);
            } else {
                // Trigger View Update normally
                updateGoalsView();
            }
        }

        function handleDistributeMix(totalGoal, type, contextKey, filteredClientMetrics) {
            let targetKey = type === 'salty' ? 'mix_salty' : 'mix_foods';

            // 1. Calculate Total History for THESE sellers in THIS context
            let totalHistoryMix = 0;
            const sellersHistory = [];

            const uniqueSellers = new Set(filteredClientMetrics.map(c => c.seller));

            uniqueSellers.forEach(seller => {
                const hist = getHistoricalMix(seller, type);
                sellersHistory.push({ seller, hist });
                totalHistoryMix += hist;
            });

            // 2. Distribute Total Goal
            sellersHistory.forEach(item => {
                let share = 0;
                if (totalHistoryMix > 0) {
                    share = item.hist / totalHistoryMix;
                }

                const sellerTarget = Math.round(totalGoal * share);

                // Update Primary Target
                if (!goalsSellerTargets.has(item.seller)) goalsSellerTargets.set(item.seller, {});
                const t = goalsSellerTargets.get(item.seller);
                t[targetKey] = sellerTarget;
            });

            // Trigger View Update
            updateGoalsView();
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
            // Use hierarchy state to filter clients for metrics calculation
            let filteredMetricsClients = getHierarchyFilteredClients('goals-gv', allClientsData);
            filteredMetricsClients = filteredMetricsClients.filter(c => isActiveClient(c));
            const displayMetrics = calculateMetricsForClients(filteredMetricsClients);

            // Update Header (Dynamic) - Same as before
            const thead = document.querySelector('#goals-table-container table thead');
            if (thead) {
                const monthHeaders = quarterMonths.map(m => `<th class="px-2 py-2 text-right w-20 bg-blue-900/10 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">${m.label}</th>`).join('');
                const monthsCount = quarterMonths.length;
                thead.innerHTML = `<tr><th rowspan="2" class="px-2 py-2 text-center w-16 border-r border-b border-slate-700">CÓD</th><th rowspan="2" class="px-3 py-2 text-left w-48 border-r border-b border-slate-700">CLIENTE</th><th rowspan="2" class="px-3 py-2 text-left w-24 border-r border-b border-slate-700">VENDEDOR</th><th colspan="${3 + monthsCount}" class="px-2 py-1 text-center bg-blue-900/30 text-blue-400 border-r border-slate-700 border-b-0">FATURAMENTO (R$)</th><th colspan="3" class="px-2 py-1 text-center bg-orange-900/30 text-orange-400 border-r border-slate-700 border-b-0">VOLUME (KG)</th><th rowspan="2" class="px-2 py-2 text-center w-16 bg-purple-900/20 text-purple-300 font-bold border-b border-slate-700">Mix PDV</th></tr><tr>${monthHeaders}<th class="px-2 py-2 text-right w-24 bg-blue-900/20 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">MÉDIA</th><th class="px-2 py-2 text-center w-16 bg-blue-900/20 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">% SHARE</th><th class="px-2 py-2 text-right w-24 bg-blue-900/20 text-blue-100 font-bold border-r border-b border-slate-700 text-[10px]">META AUTO</th><th class="px-2 py-2 text-right w-24 bg-orange-900/20 text-orange-300 border-r border-b border-slate-700/50 text-[10px]">MÉDIA KG</th><th class="px-2 py-2 text-center w-16 bg-orange-900/20 text-orange-300 border-r border-b border-slate-700/50 text-[10px]">% SHARE</th><th class="px-2 py-2 text-right w-24 bg-orange-900/20 text-orange-100 font-bold border-r border-b border-slate-700 text-[10px]">META KG</th></tr>`;
            }

            const filteredClients = getGoalsFilteredData();
            goalsGvTableBody.innerHTML = getSkeletonRows(15, 10);

            // Cache Key for Global Totals
            const cacheKey = currentGoalsSupplier + (currentGoalsBrand ? `_${currentGoalsBrand}` : '');
            const contextKey = cacheKey;

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
                const mixProducts = new Set(); // For Mix PDV Calc
                quarterMonths.forEach(m => { monthlyValues[m.key] = 0; });

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

                                    // Mix Logic: Add Product Code + Month Key
                                    // ELMA Constraint: If ELMA_ALL, only 707 and 708 are counted for Mix. 752 (Torcida) is excluded.
                                    let includeInMix = true;
                                    const codFor = String(sale.CODFOR);

                                    if (currentGoalsSupplier === 'ELMA_ALL') {
                                        if (codFor !== '707' && codFor !== '708') includeInMix = false;
                                    }

                                    if (includeInMix) {
                                        mixProducts.add(`${sale.PRODUTO}_${monthKey}`);
                                    }
                                }
                            }
                        }
                    });
                }

                // Mix Calculation: Average Unique Products per Month
                const monthKeys = Object.keys(monthlyValues); // Assumes last 3 months populated
                let sumUniqueProducts = 0;
                monthKeys.forEach(mKey => {
                    let uniqueCount = 0;
                    mixProducts.forEach(k => {
                        if (k.endsWith(mKey)) uniqueCount++;
                    });
                    sumUniqueProducts += uniqueCount;
                });
                const mixPdvAvg = monthKeys.length > 0 ? sumUniqueProducts / 3 : 0; // Using 3 as quarterly divisor

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
                // Add Mix PDV to Metric Object
                metric.mixPdv = mixPdvAvg;

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
                const isSingleSeller = hierarchyState['goals-gv'] && hierarchyState['goals-gv'].promotors.size === 1;

                if (goalMixInput) {
                    const newMixInput = goalMixInput.cloneNode(true);
                    goalMixInput.parentNode.replaceChild(newMixInput, goalMixInput);

                    // Calculate Total Adjustment for Current View Context
                    let contextAdjustment = 0;
                    const adjustmentMap = goalsPosAdjustments[contextKey];
                    let absoluteOverride = null;

                    if (isSingleSeller) {
                        // Check for Absolute Override from Import

                        if (absoluteOverride === null && adjustmentMap) {
                            // Specific Seller Context (Fallback)
                        }
                    } else {
                        // Aggregate Logic for Multiple Sellers (Supervisor/Global)
                        const visibleSellers = new Set(clientMetrics.map(c => c.seller));
                        const naturalPosBySeller = new Map();

                        // 1. Calculate Natural Pos per Seller
                        clientMetrics.forEach(c => {
                            if (c.metaPos > 0) {
                                naturalPosBySeller.set(c.seller, (naturalPosBySeller.get(c.seller) || 0) + c.metaPos);
                            }
                        });

                        // 2. Sum (Override OR (Natural + Adjustment))
                        let sumTotal = 0;
                        visibleSellers.forEach(seller => {
                            const override = getSellerTargetOverride(seller, 'pos', contextKey);
                            if (override !== null) {
                                sumTotal += override;
                            } else {
                                const nat = naturalPosBySeller.get(seller) || 0;
                                const adj = adjustmentMap ? (adjustmentMap.get(seller) || 0) : 0;
                                sumTotal += (nat + adj);
                            }
                        });

                        // Override the standard calculation
                        absoluteOverride = sumTotal;
                    }

                    const displayPos = absoluteOverride !== null ? absoluteOverride : (naturalTotalPos + contextAdjustment);
                    newMixInput.value = displayPos.toLocaleString('pt-BR');

                    if (isSingleSeller || isAggregatedTab) {
                        newMixInput.readOnly = false;
                        newMixInput.classList.remove('opacity-50', 'cursor-not-allowed');

                        if(btnDistributeMix) {
                            const newBtnDistributeMix = btnDistributeMix.cloneNode(true);
                            btnDistributeMix.parentNode.replaceChild(newBtnDistributeMix, btnDistributeMix);
                            newBtnDistributeMix.style.display = '';

                            newBtnDistributeMix.onclick = () => {
                                const valStr = newMixInput.value;
                                const val = parseFloat(valStr.replace(/\./g, '').replace(',', '.')) || 0;

                                if (isAggregatedTab) {
                                    const contextName = currentGoalsSupplier.replace('_ALL', '');
                                    showConfirmationModal(`Confirmar distribuição Top-Down de Positivação (${val}) para ${contextName}?`, () => {
                                        handleDistributePositivation(val, contextKey, clientMetrics);
                                    });
                                } else {
                                    const filterDesc = getFilterDescription();
                                    // Validation: Check against PEPSICO Limit
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
                                            updateGoalsView();
                                        }
                                    });
                                }
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
                        let absOverride = null;

                        if (isSingleSeller) {
                            if (absOverride === null) {
                            }
                        } else {
                            // Aggregate Logic for Multiple Sellers (Mix)
                            const visibleSellers = new Set(clientMetrics.map(c => c.seller));
                            const naturalBaseBySeller = new Map();

                            // 1. Calculate Natural Base per Seller
                            clientMetrics.forEach(c => {
                                const sellerCode = optimizedData.rcaCodeByName.get(c.seller) || '';
                                if (sellerCode !== '1001') {
                                    if (c.metaPos > 0) {
                                        naturalBaseBySeller.set(c.seller, (naturalBaseBySeller.get(c.seller) || 0) + 1);
                                    }
                                }
                            });

                            // 2. Sum
                            let sumTotal = 0;
                            visibleSellers.forEach(seller => {
                                const override = getSellerTargetOverride(seller, type === 'salty' ? 'mix_salty' : 'mix_foods', contextKey);
                                if (override !== null) {
                                    sumTotal += override;
                                } else {
                                    const base = naturalBaseBySeller.get(seller) || 0;
                                    const nat = Math.round(base * (type === 'salty' ? 0.5 : 0.3));
                                    const adj = adjustmentsMap ? (adjustmentsMap.get(seller) || 0) : 0;
                                    sumTotal += (nat + adj);
                                }
                            });
                            absOverride = sumTotal;
                        }

                        const displayVal = absOverride !== null ? absOverride : (naturalTarget + adj);
                        const input = document.getElementById(inputId);
                        const btn = document.getElementById(btnId);

                        if(input) {
                            input.value = displayVal.toLocaleString('pt-BR');

                            if (isSingleSeller || isAggregatedTab) {
                                input.readOnly = false;
                                input.classList.remove('opacity-50', 'cursor-not-allowed');
                                if(btn) {
                                    const newBtn = btn.cloneNode(true);
                                    btn.parentNode.replaceChild(newBtn, btn);
                                    newBtn.style.display = '';

                                    newBtn.onclick = () => {
                                        const valStr = input.value;
                                        const val = parseFloat(valStr.replace(/\./g, '').replace(',', '.')) || 0;

                                        if (isAggregatedTab) {
                                            const contextName = currentGoalsSupplier.replace('_ALL', '');
                                            showConfirmationModal(`Confirmar distribuição Proporcional de Mix ${type === 'salty' ? 'Salty' : 'Foods'} (${val}) para ${contextName}? (Base: Histórico)`, () => {
                                                handleDistributeMix(val, type, contextKey, clientMetrics);
                                            });
                                        } else {
                                            showConfirmationModal(`Confirmar ajuste de Meta Mix ${type === 'salty' ? 'Salty' : 'Foods'} para ${valStr} (Vendedor: ${getFirstName(sellerName)})?`, () => {
                                                saveMixAdjustment(type, val, sellerName);
                                            });
                                        }
                                    };
                                }
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
                    if (visibleSellersSet.size === 0 && hierarchyState['goals-gv'] && hierarchyState['goals-gv'].promotors.size > 0) {
                         visibleSellersSet = new Set(hierarchyState['goals-gv'].promotors);
                    }

                    const elmaTargetBase = getElmaTargetBase(displayMetrics, goalsPosAdjustments, visibleSellersSet);

                    // Card Natural Targets (Based on ELMA: 50% Salty / 30% Foods)
                    const globalNaturalSalty = Math.round(elmaTargetBase * 0.50);
                    const globalNaturalFoods = Math.round(elmaTargetBase * 0.30);

                    if (goalsMixSaltyAdjustments['PEPSICO_ALL']) {
                        handleMixCard('salty', globalNaturalSalty, goalsMixSaltyAdjustments['PEPSICO_ALL'], 'goal-global-mix-salty', 'btn-distribute-mix-salty');
                    }
                    if (goalsMixFoodsAdjustments['PEPSICO_ALL']) {
                        handleMixCard('foods', globalNaturalFoods, goalsMixFoodsAdjustments['PEPSICO_ALL'], 'goal-global-mix-foods', 'btn-distribute-mix-foods');
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
                        return `<tr class="hover:bg-slate-800 group transition-colors border-b border-slate-800"><td class="px-2 py-2 text-center border-r border-slate-800 bg-[#151c36] text-xs text-slate-300">${item.cod}</td><td class="px-2 py-2 text-left border-r border-slate-800 bg-[#151c36] text-xs font-bold text-white truncate max-w-[200px]" title="${item.name}">${(item.name || '').substring(0, 30)}</td><td class="px-2 py-2 text-left border-r border-slate-800 bg-[#151c36] text-[10px] text-slate-400 uppercase">${getFirstName(item.seller)}</td>${monthCells}<td class="px-2 py-2 text-right text-slate-300 font-medium bg-blue-900/10 border-r border-slate-800/50 text-xs">${item.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-2 py-2 text-center text-blue-400 text-xs bg-blue-900/10 border-r border-slate-800/50">${(item.shareFat * 100).toFixed(2)}%</td><td class="px-2 py-2 text-right font-bold text-blue-200 bg-blue-900/20 border-r border-slate-800 text-xs">${item.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-2 py-2 text-right text-slate-300 font-medium bg-orange-900/10 border-r border-slate-800/50 text-xs">${item.avgVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-2 py-2 text-center text-orange-400 text-xs bg-orange-900/10 border-r border-slate-800/50">${(item.shareVol * 100).toFixed(2)}%</td><td class="px-2 py-2 text-right font-bold text-orange-200 bg-orange-900/20 border-r border-slate-800 text-xs">${item.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-2 py-2 text-center font-bold text-purple-300 bg-purple-900/10 text-xs">${(item.mixPdv || 0).toLocaleString('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})}</td></tr>`;
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
            // Apply Hierarchy Logic
            let clients = getHierarchyFilteredClients('goals-sv', allClientsData);

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
            if (mainTable) mainTable.innerHTML = `<tbody>${getSkeletonRows(12, 10)}</tbody>`;

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

                // EXCLUSION: Skip INATIVOS and N/A from Goals View to prevent ghost totals
                if (sellerName === 'INATIVOS' || sellerName === 'N/A') return;

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
                        // Priority Check: Explicit Target from Import/Supabase (goalsSellerTargets)
                        let explicitTarget = undefined;
                        if (goalsSellerTargets && goalsSellerTargets.has(sellerName)) {
                            const targets = goalsSellerTargets.get(sellerName);
                            // Check exact key or upper case key
                            if (targets[colId] !== undefined) explicitTarget = targets[colId];
                            else if (targets[colId.toUpperCase()] !== undefined) explicitTarget = targets[colId.toUpperCase()];
                        }

                        if (explicitTarget !== undefined) {
                            // Apply Explicit Target
                            if (colId.startsWith('mix_')) {
                                data.metaMix = explicitTarget;
                            } else {
                                data.metaPos = explicitTarget;
                            }
                        } else {
                            // Fallback: Legacy Adjustment Logic (Session only)

                            // Apply Pos Adjustment
                            const adjKey = posKeys[colId] || colId; // Fallback to ID
                            if (goalsPosAdjustments[adjKey]) {
                                const adj = goalsPosAdjustments[adjKey].get(sellerName) || 0;
                                // Update Meta Pos: Natural (Summed from clients) + Adjustment
                                data.metaPos = data.metaPos + adj;
                            }

                            // Apply Mix Adjustment (Only for Mix Cols)
                            if (colId === 'mix_salty') {
                                const adj = goalsMixSaltyAdjustments['PEPSICO_ALL']?.get(sellerName) || 0;
                                data.metaMix = data.metaMix + adj;
                            }
                            if (colId === 'mix_foods') {
                                const adj = goalsMixFoodsAdjustments['PEPSICO_ALL']?.get(sellerName) || 0;
                                data.metaMix = data.metaMix + adj;
                            }
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
                    // DISABLED: We trust the sum of seller targets (which might be overridden)
                    // if (group.totals['mix_salty']) group.totals['mix_salty'].metaMix = groupMixSaltyMeta + groupMixSaltyAdj;
                    // if (group.totals['mix_foods']) group.totals['mix_foods'].metaMix = groupMixFoodsMeta + groupMixFoodsAdj;
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
                        // Sum metaMix
                        t.metaMix += s.metaMix || 0;
                    });
                });

                // Recalculate Grand Total Mix - DISABLED (Use Sum)
                // grandTotalRow.totals['mix_salty'].metaMix = Math.round(globalElmaBase * 0.50) + globalMixSaltyAdj;
                // grandTotalRow.totals['mix_foods'].metaMix = Math.round(globalElmaBase * 0.30) + globalMixFoodsAdj;

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
                // Let's use allSalesData for consistency with other views to populate seller lists.

                updateGoalsView();
            }, 50);
        }

        function resetGoalsGvFilters() {
            if (hierarchyState['goals-gv']) {
                hierarchyState['goals-gv'].coords.clear();
                hierarchyState['goals-gv'].cocoords.clear();
                hierarchyState['goals-gv'].promotors.clear();

                if (userHierarchyContext.role !== 'adm') {
                    if (userHierarchyContext.coord) hierarchyState['goals-gv'].coords.add(userHierarchyContext.coord);
                    if (userHierarchyContext.cocoord) hierarchyState['goals-gv'].cocoords.add(userHierarchyContext.cocoord);
                    if (userHierarchyContext.promotor) hierarchyState['goals-gv'].promotors.add(userHierarchyContext.promotor);
                }

                updateHierarchyDropdown('goals-gv', 'coord');
                updateHierarchyDropdown('goals-gv', 'cocoord');
                updateHierarchyDropdown('goals-gv', 'promotor');
            }

            const codcli = document.getElementById('goals-gv-codcli-filter');
            if(codcli) codcli.value = '';

            updateGoalsView();
        }

        // <!-- INÍCIO DO CÓDIGO RESTAURADO -->

        function getCoverageFilteredData(options = {}) {
            const { excludeFilter = null } = options;
            const isExcluded = (f) => excludeFilter === f || (Array.isArray(excludeFilter) && excludeFilter.includes(f));

            const city = coverageCityFilter.value.trim().toLowerCase();
            const filial = coverageFilialFilter.value;
            const suppliersSet = new Set(selectedCoverageSuppliers);
            const productsSet = new Set(selectedCoverageProducts);
            const tiposVendaSet = new Set(selectedCoverageTiposVenda);

            // New Hierarchy Logic applied to Active Clients
            let clients = getHierarchyFilteredClients('coverage', getActiveClientsData());

            if (filial !== 'ambas' || city) {
                clients = clients.filter(c => {
                    let pass = true;
                    if (filial !== 'ambas') {
                        if (clientLastBranch.get(c['Código']) !== filial) pass = false;
                    }
                    if (pass && !isExcluded('city') && city) {
                        if ((c.cidade || '').toLowerCase() !== city) pass = false;
                    }
                    return pass;
                });
            }

            const clientCodes = new Set(clients.map(c => c['Código']));

            const filters = {
                filial,
                city,
                tipoVenda: tiposVendaSet,
                supplier: suppliersSet,
                product: productsSet,
                clientCodes
            };

            let sales = getFilteredDataFromIndices(optimizedData.indices.current, optimizedData.salesById, filters, excludeFilter);
            let history = getFilteredDataFromIndices(optimizedData.indices.history, optimizedData.historyById, filters, excludeFilter);

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
            coverageCityFilter.value = '';
            coverageFilialFilter.value = 'ambas';

            const unitPriceInput = document.getElementById('coverage-unit-price-filter');
            if(unitPriceInput) unitPriceInput.value = '';

            const workingDaysInput = document.getElementById('coverage-working-days-input');
            if(workingDaysInput) workingDaysInput.value = customWorkingDaysCoverage;

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

            const activeClientsForCoverage = clients;
            const activeClientsCount = activeClientsForCoverage.length;
            const activeClientCodes = new Set(activeClientsForCoverage.map(c => c['Código']));

            coverageActiveClientsKpi.textContent = activeClientsCount.toLocaleString('pt-BR');

            // Show Loading State in Table
            coverageTableBody.innerHTML = getSkeletonRows(8, 10);

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
                if (!isAlternativeMode(selectedCoverageTiposVenda) && s.TIPOVENDA !== '1' && s.TIPOVENDA !== '9') return;
                const val = getValueForSale(s, selectedCoverageTiposVenda);

                // Coverage Map (Inverted for Performance)
                if (!productClientsCurrent.has(s.PRODUTO)) productClientsCurrent.set(s.PRODUTO, new Map());
                const clientMap = productClientsCurrent.get(s.PRODUTO);
                clientMap.set(s.CODCLI, (clientMap.get(s.CODCLI) || 0) + val);

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

                if (!isAlternativeMode(selectedCoverageTiposVenda) && s.TIPOVENDA !== '1' && s.TIPOVENDA !== '9') return;
                const val = getValueForSale(s, selectedCoverageTiposVenda);

                // Coverage Map (only if prev month)
                if (isPrevMonth) {
                    // Coverage Map (Inverted for Performance)
                    if (!productClientsPrevious.has(s.PRODUTO)) productClientsPrevious.set(s.PRODUTO, new Map());
                    const clientMap = productClientsPrevious.get(s.PRODUTO);
                    clientMap.set(s.CODCLI, (clientMap.get(s.CODCLI) || 0) + val);

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
                            <td data-label="Produto" class="px-2 py-1.5 text-xs">${item.descricao}</td>
                            <td data-label="Estoque" class="px-2 py-1.5 text-xs text-right">${item.stockQty.toLocaleString('pt-BR')}</td>
                            <td data-label="Vol Ant (Cx)" class="px-2 py-1.5 text-xs text-right">${item.boxesSoldPreviousMonth.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                            <td data-label="Vol Atual (Cx)" class="px-2 py-1.5 text-xs text-right">${item.boxesSoldCurrentMonth.toLocaleString('pt-BR', {maximumFractionDigits: 2})}</td>
                            <td data-label="Var Vol" class="px-2 py-1.5 text-xs text-right">${boxesVariationContent}</td>
                            <td data-label="PDV Ant" class="px-2 py-1.5 text-xs text-right">${item.clientsPreviousCount.toLocaleString('pt-BR')}</td>
                            <td data-label="PDV Atual" class="px-2 py-1.5 text-xs text-right">${item.clientsCurrentCount.toLocaleString('pt-BR')}</td>
                            <td data-label="Var PDV" class="px-2 py-1.5 text-xs text-right">${pdvVariationContent}</td>
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
                vendasPorCoord: {}, vendasPorCoCoord: {}, vendasPorPromotor: {}, // New Hierarchy Aggregation
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
                if (!isAlternativeMode(selectedTiposVenda) && sale.TIPOVENDA !== '1' && sale.TIPOVENDA !== '9') return;
                if (sale.CODCLI) {
                    const currentVal = clientTotalSales.get(sale.CODCLI) || 0;
                    // Considera apenas VLVENDA para consistência com o KPI "Clientes Atendidos" do Comparativo
                    // Se a regra de bonificação mudar lá, deve mudar aqui também.
                    // Atualmente Comparativo usa: (s.TIPOVENDA === '1' || s.TIPOVENDA === '9') -> VLVENDA
                    // Note que 'data' aqui já vem filtrado, mas precisamos checar se o valor agregado passa do threshold
                    const val = getValueForSale(sale, selectedTiposVenda);
                    clientTotalSales.set(sale.CODCLI, currentVal + val);

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
                if (!isAlternativeMode(selectedTiposVenda) && item.TIPOVENDA !== '1' && item.TIPOVENDA !== '9') return;
                const vlVenda = getValueForSale(item, selectedTiposVenda);
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

                // New Hierarchy Aggregation
                const hierarchy = optimizedData.clientHierarchyMap.get(item.CODCLI);
                if (hierarchy) {
                    const c = hierarchy.coord.name;
                    const cc = hierarchy.cocoord.name;
                    const p = hierarchy.promotor.name;
                    if (c) summary.vendasPorCoord[c] = (summary.vendasPorCoord[c] || 0) + vlVenda;
                    if (cc) summary.vendasPorCoCoord[cc] = (summary.vendasPorCoCoord[cc] || 0) + vlVenda;
                    if (p) summary.vendasPorPromotor[p] = (summary.vendasPorPromotor[p] || 0) + vlVenda;
                } else {
                    const unk = 'Sem Estrutura';
                    summary.vendasPorCoord[unk] = (summary.vendasPorCoord[unk] || 0) + vlVenda;
                    summary.vendasPorCoCoord[unk] = (summary.vendasPorCoCoord[unk] || 0) + vlVenda;
                    summary.vendasPorPromotor[unk] = (summary.vendasPorPromotor[unk] || 0) + vlVenda;
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

            // Semantic Palette: Green (Success), Blue (Good), Purple (Neutral/Meta), Amber (Warning), Red (Danger)
            const professionalPalette = ['#22c55e', '#3b82f6', '#a855f7', '#f59e0b', '#ef4444', '#64748b', '#06b6d4', '#ec4899'];

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
                charts[canvasId].update('none');
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
                tdPedido.dataset.label = 'Nº Pedido';
                tdPedido.appendChild(createLink(row.PEDIDO, 'pedidoId', row.PEDIDO));
                tr.appendChild(tdPedido);

                const tdCodCli = document.createElement('td');
                tdCodCli.className = "px-4 py-2";
                tdCodCli.dataset.label = 'Cliente';
                tdCodCli.appendChild(createLink(row.CODCLI, 'codcli', row.CODCLI));
                tr.appendChild(tdCodCli);

                const tdVendedor = document.createElement('td');
                tdVendedor.className = "px-4 py-2";
                tdVendedor.dataset.label = 'Vendedor';
                tdVendedor.textContent = getFirstName(row.NOME);
                tr.appendChild(tdVendedor);

                const tdForn = document.createElement('td');
                tdForn.className = "px-4 py-2";
                tdForn.dataset.label = 'Fornecedor';
                tdForn.textContent = row.FORNECEDORES_STR || '';
                tr.appendChild(tdForn);

                const tdDtPed = document.createElement('td');
                tdDtPed.className = "px-4 py-2";
                tdDtPed.dataset.label = 'Data Pedido';
                tdDtPed.textContent = formatDate(row.DTPED);
                tr.appendChild(tdDtPed);

                const tdDtSaida = document.createElement('td');
                tdDtSaida.className = "px-4 py-2";
                tdDtSaida.dataset.label = 'Data Saída';
                tdDtSaida.textContent = formatDate(row.DTSAIDA);
                tr.appendChild(tdDtSaida);

                const tdPeso = document.createElement('td');
                tdPeso.className = "px-4 py-2 text-right";
                tdPeso.dataset.label = 'Peso';
                tdPeso.textContent = (Number(row.TOTPESOLIQ) || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 }) + ' Kg';
                tr.appendChild(tdPeso);

                const tdValor = document.createElement('td');
                tdValor.className = "px-4 py-2 text-right";
                tdValor.dataset.label = 'Total';
                tdValor.textContent = (Number(row.VLVENDA) || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                tr.appendChild(tdValor);

                const tdPosicao = document.createElement('td');
                tdPosicao.className = "px-4 py-2 text-center";
                tdPosicao.dataset.label = 'Status';
                const badge = getPosicaoBadge(row.POSICAO);
                if (typeof badge === 'string') tdPosicao.textContent = badge;
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
            const pastQuarterRevenue = historicalSales.reduce((sum, sale) => {
                if (!isAlternativeMode(selectedTiposVenda) && sale.TIPOVENDA !== '1' && sale.TIPOVENDA !== '9') return sum;
                return sum + getValueForSale(sale, selectedTiposVenda);
            }, 0);
            let averageMonthlyRevenue = pastQuarterRevenue / QUARTERLY_DIVISOR;
            if (isNaN(averageMonthlyRevenue)) averageMonthlyRevenue = 0;

            const currentMonthRevenue = currentSales.reduce((sum, sale) => {
                if (!isAlternativeMode(selectedTiposVenda) && sale.TIPOVENDA !== '1' && sale.TIPOVENDA !== '9') return sum;
                return sum + getValueForSale(sale, selectedTiposVenda);
            }, 0);
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

                // Hierarchy Filter
                const hierarchyClients = getHierarchyFilteredClients('main');
                if (hierarchyClients.length < allClientsData.length) {
                    hasFilter = true;
                    const hierarchyIds = new Set();
                    hierarchyClients.forEach(c => {
                        const code = String(c['Código'] || c['codigo_cliente']);
                        const ids = indices.byClient.get(code);
                        if (ids) ids.forEach(id => hierarchyIds.add(id));
                    });
                    setsToIntersect.push(hierarchyIds);
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

            const hierarchyClientsForTable = getHierarchyFilteredClients('main');
            const hierarchyClientCodes = new Set(hierarchyClientsForTable.map(c => String(c['Código'] || c['codigo_cliente'])));
            const isHierarchyFiltered = hierarchyClientsForTable.length < allClientsData.length;

            const filteredTableData = aggregatedOrders.filter(order => {
                let matches = true;
                if (mainRedeGroupFilter) {
                    matches = matches && clientCodesInRede.has(order.CODCLI);
                }
                if (codcli) matches = matches && order.CODCLI === codcli;
                else {
                    if (isHierarchyFiltered) matches = matches && hierarchyClientCodes.has(order.CODCLI);
                }
                // Robust filtering with existence checks
                if (selectedTiposVenda.length > 0) matches = matches && order.TIPOVENDA && selectedTiposVenda.includes(order.TIPOVENDA);
                if (currentFornecedor) matches = matches && order.FORNECEDORES_LIST && order.FORNECEDORES_LIST.includes(currentFornecedor);
                if (selectedMainSuppliers.length > 0) matches = matches && order.CODFORS_LIST && order.CODFORS_LIST.some(c => selectedMainSuppliers.includes(c));
                if (posicao) matches = matches && order.POSICAO === posicao;
                return matches;
            });

            const isFiltered = isHierarchyFiltered || !!codcli || !!currentFornecedor || !!mainRedeGroupFilter || selectedMainSuppliers.length > 0 || !!posicao || selectedTiposVenda.length > 0;

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
                let chartData = summary.vendasPorCoord;
                let chartTitle = 'Performance por Coordenador';
                const mainState = hierarchyState['main'];
                
                if (mainState.cocoords.size > 0) {
                    chartData = summary.vendasPorPromotor;
                    chartTitle = 'Performance por Promotor';
                } else if (mainState.coords.size > 0) {
                    chartData = summary.vendasPorCoCoord;
                    chartTitle = 'Performance por Co-Coordenador';
                }

                const totalForPercentage = Object.values(chartData).reduce((a, b) => a + b, 0);
                const personChartTooltipOptions = { plugins: { tooltip: { callbacks: { label: function(context) { let label = context.dataset.label || ''; if (label) label += ': '; const value = context.parsed.y; if (value !== null) { label += new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value); if (totalForPercentage > 0) { const percentage = ((value / totalForPercentage) * 100).toFixed(2); label += ` (${percentage}%)`; } } return label; } } } } };
                
                salesByPersonTitle.textContent = chartTitle;
                createChart('salesByPersonChart', 'bar', Object.keys(chartData).map(getFirstName), Object.values(chartData), personChartTooltipOptions);

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


        function updateTipoVendaFilter(dropdown, filterText, selectedArray, dataSource, skipRender = false) {
            if (!dropdown || !filterText) return selectedArray;
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

        function updateRedeFilter(dropdown, buttonTextElement, selectedArray, dataSource, baseText = 'C/Rede') {
            if (!dropdown || !buttonTextElement) return selectedArray;
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
            selectedMainSuppliers = [];
            selectedTiposVenda = [];
            selectedMainRedes = [];
            mainRedeGroupFilter = '';

            const codcliFilter = document.getElementById('codcli-filter');
            if (codcliFilter) codcliFilter.value = '';

            selectedMainSuppliers = updateSupplierFilter(document.getElementById('fornecedor-filter-dropdown'), document.getElementById('fornecedor-filter-text'), selectedMainSuppliers, [...allSalesData, ...allHistoryData], 'main');
            updateTipoVendaFilter(tipoVendaFilterDropdown, tipoVendaFilterText, selectedTiposVenda, allSalesData);
            updateRedeFilter(mainRedeFilterDropdown, mainComRedeBtnText, selectedMainRedes, allClientsData);

            if (mainRedeGroupContainer) {
                mainRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                const defaultBtn = mainRedeGroupContainer.querySelector('button[data-group=""]');
                if (defaultBtn) defaultBtn.classList.add('active');
                if (mainRedeFilterDropdown) mainRedeFilterDropdown.classList.add('hidden');
            }

            const fornecedorToggleContainerEl = document.getElementById('fornecedor-toggle-container');
            if (fornecedorToggleContainerEl) {
                fornecedorToggleContainerEl.querySelectorAll('.fornecedor-btn').forEach(b => b.classList.remove('active'));
            }
            currentFornecedor = '';

            if (hierarchyState['main']) {
                hierarchyState['main'].coords.clear();
                hierarchyState['main'].cocoords.clear();
                hierarchyState['main'].promotors.clear();

                if (userHierarchyContext.role !== 'adm') {
                    if (userHierarchyContext.coord) hierarchyState['main'].coords.add(userHierarchyContext.coord);
                    if (userHierarchyContext.cocoord) hierarchyState['main'].cocoords.add(userHierarchyContext.cocoord);
                    if (userHierarchyContext.promotor) hierarchyState['main'].promotors.add(userHierarchyContext.promotor);
                }

                updateHierarchyDropdown('main', 'coord');
                updateHierarchyDropdown('main', 'cocoord');
                updateHierarchyDropdown('main', 'promotor');
            }

            updateDashboard();
        }


        function getCityFilteredData(options = {}) {
            const { excludeFilter = null } = options;

            const cityInput = cityNameFilter.value.trim().toLowerCase();
            const codCli = cityCodCliFilter.value.trim();
            const tiposVendaSet = new Set(selectedCityTiposVenda);

            // New Hierarchy Logic
            let clients = getHierarchyFilteredClients('city', allClientsData);

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

            const filters = {
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

            // Supervisor/Seller filters managed by setupHierarchyFilters

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
                if (!isAlternativeMode(selectedCityTiposVenda) && s.TIPOVENDA !== '1' && s.TIPOVENDA !== '9') continue;

                const d = parseDate(s.DTPED);
                if (d && d.getUTCFullYear() === currentYear && d.getUTCMonth() === currentMonth) {
                    const val = getValueForSale(s, selectedCityTiposVenda);
                    clientTotalsThisMonth.set(s.CODCLI, (clientTotalsThisMonth.get(s.CODCLI) || 0) + val);
                }
            }

            const detailedDataByClient = new Map(); // Map<CODCLI, { total, pepsico, multimarcas, maxDate }>

            // Pre-aggregate Sales Data for Analysis (Sync)
            salesForAnalysis.forEach(s => {
                const d = parseDate(s.DTPED);
                if (d) {
                    if (!isAlternativeMode(selectedCityTiposVenda) && s.TIPOVENDA !== '1' && s.TIPOVENDA !== '9') return;
                    if (!detailedDataByClient.has(s.CODCLI)) {
                        detailedDataByClient.set(s.CODCLI, { total: 0, pepsico: 0, multimarcas: 0, maxDate: 0 });
                    }
                    const entry = detailedDataByClient.get(s.CODCLI);
                    const ts = d.getTime();

                    if (ts > entry.maxDate) entry.maxDate = ts;

                    if (d.getUTCFullYear() === currentYear && d.getUTCMonth() === currentMonth) {
                        const val = getValueForSale(s, selectedCityTiposVenda);
                        entry.total += val;
                        if (s.OBSERVACAOFOR === 'PEPSICO') entry.pepsico += val;
                        else if (s.OBSERVACAOFOR === 'MULTIMARCAS') entry.multimarcas += val;
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
            cityActiveDetailTableBody.innerHTML = getSkeletonRows(6, 5);
            cityInactiveDetailTableBody.innerHTML = getSkeletonRows(6, 5);

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

        function getWorkingMonthWeeks(year, month) {
            const weeks = [];
            const lastDayOfMonth = new Date(Date.UTC(year, month + 1, 0));
            lastDayOfMonth.setUTCHours(23, 59, 59, 999);

            // Start looking from Day 1
            let currentDate = new Date(Date.UTC(year, month, 1));

            // Advance to first working day (Mon-Fri)
            // 0=Sun, 6=Sat
            while (currentDate.getUTCDay() === 0 || currentDate.getUTCDay() === 6) {
                currentDate.setUTCDate(currentDate.getUTCDate() + 1);
                // Safety: if month has no working days (impossible usually)
                if (currentDate > lastDayOfMonth) return [];
            }

            // Now currentDate is the start of Week 1
            let weekCount = 1;

            while (currentDate <= lastDayOfMonth) {
                // Determine End of this week bucket
                // Standard: Ends on Sunday.

                let weekEnd = new Date(currentDate);
                const day = weekEnd.getUTCDay(); // 1=Mon ... 5=Fri

                // Distance to next Sunday (0)
                // If Mon(1), need +6. If Sun(0), need +0.
                // (7 - 1) % 7 = 6. (7 - 0) % 7 = 0.
                const distToSunday = (7 - day) % 7;

                weekEnd.setUTCDate(weekEnd.getUTCDate() + distToSunday);
                weekEnd.setUTCHours(23, 59, 59, 999);

                // Cap at end of month
                if (weekEnd > lastDayOfMonth) weekEnd = new Date(lastDayOfMonth);

                weeks.push({
                    start: new Date(currentDate),
                    end: weekEnd,
                    id: weekCount++
                });

                // Next week starts day after weekEnd
                currentDate = new Date(weekEnd);
                currentDate.setUTCDate(currentDate.getUTCDate() + 1);
                currentDate.setUTCHours(0, 0, 0, 0);
            }

            return weeks;
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


        function updateSupplierFilter(dropdown, filterText, selectedArray, dataSource, filterType = 'comparison', skipRender = false) {
            if (!dropdown || !filterText) return selectedArray;
            const forbidden = ['CODFOR', 'FORNECEDOR', 'COD FOR', 'NOME DO FORNECEDOR', 'FORNECEDOR_NOME'];
            const suppliers = new Map();
            dataSource.forEach(s => {
                if(s.CODFOR && s.FORNECEDOR && !forbidden.includes(s.CODFOR.toUpperCase()) && !forbidden.includes(s.FORNECEDOR.toUpperCase())) {
                    suppliers.set(s.CODFOR, s.FORNECEDOR);
                }
            });

            // Special Handling for Meta Realizado: Inject Virtual Categories
            if (filterType === 'metaRealizado') {
                if (suppliers.has('707')) suppliers.set('707', 'EXTRUSADOS');
                if (suppliers.has('708')) suppliers.set('708', 'NÃO EXTRUSADOS');
                if (suppliers.has('752')) suppliers.set('752', 'TORCIDA');

                if (suppliers.has('1119')) {
                    suppliers.delete('1119');
                    suppliers.set('1119_TODDYNHO', 'TODDYNHO');
                    suppliers.set('1119_TODDY', 'TODDY');
                    suppliers.set('1119_QUAKER_KEROCOCO', 'QUAKER/KEROCOCO');
                }
            }

            const sortedSuppliers = [...suppliers.entries()].sort((a, b) => a[1].localeCompare(b[1]));

            selectedArray = selectedArray.filter(cod => suppliers.has(cod));

            if (!skipRender) {
                const htmlParts = [];
                for (let i = 0; i < sortedSuppliers.length; i++) {
                    let [cod, name] = sortedSuppliers[i];
                    const isChecked = selectedArray.includes(cod);

                    let displayName = name;
                    // For all pages except 'Meta Vs. Realizado', prefix Code to Name
                    if (filterType !== 'metaRealizado') {
                        // Ensure we don't double prefix if name already starts with code (rare but possible in data)
                        if (!name.startsWith(cod)) {
                            displayName = `${cod} ${name}`;
                        }
                    }

                    htmlParts.push(`<label class="flex items-center p-2 hover:bg-slate-600 cursor-pointer"><input type="checkbox" data-filter-type="${filterType}" class="form-checkbox h-4 w-4 bg-slate-800 border-slate-500 rounded text-teal-500 focus:ring-teal-500" value="${cod}" ${isChecked ? 'checked' : ''}><span class="ml-2 text-xs">${displayName}</span></label>`);
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
                 if (Array.isArray(dataset)) {
                     for(let i=0; i<dataset.length; i++) {
                         if(filters.clientCodes.has(normalizeKey(dataset[i].CODCLI))) allData.push(dataset[i]);
                     }
                 } else if (dataset.values && typeof dataset.values === 'function') {
                     const vals = dataset.values();
                     for(let i=0; i<vals.length; i++) if(filters.clientCodes.has(normalizeKey(vals[i].CODCLI))) allData.push(vals[i]);
                 } else {
                     for(let i=0; i<dataset.length; i++) {
                         const item = getItem(i);
                         if(filters.clientCodes.has(normalizeKey(item.CODCLI))) allData.push(item);
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
                if (!filters.clientCodes || filters.clientCodes.has(normalizeKey(item.CODCLI))) {
                    result.push(item);
                }
            }
            return result;
        };

        function getComparisonFilteredData(options = {}) {
            const { excludeFilter = null } = options;

            const suppliersSet = new Set(selectedComparisonSuppliers);
            const productsSet = new Set(selectedComparisonProducts);
            const tiposVendaSet = new Set(selectedComparisonTiposVenda);
            const redeSet = new Set(selectedComparisonRedes);

            const pasta = currentComparisonFornecedor;
            const city = comparisonCityFilter.value.trim().toLowerCase();
            const filial = comparisonFilialFilter.value;

            let clients = getHierarchyFilteredClients('comparison', allClientsData);

            if (comparisonRedeGroupFilter) {
                if (comparisonRedeGroupFilter === 'com_rede') {
                    clients = clients.filter(c => c.ramo && c.ramo !== 'N/A');
                     if (redeSet.size > 0) {
                        clients = clients.filter(c => redeSet.has(c.ramo));
                    }
                } else if (comparisonRedeGroupFilter === 'sem_rede') {
                    clients = clients.filter(c => !c.ramo || c.ramo === 'N/A');
                }
            }
            
            const clientCodes = new Set(clients.map(c => c['Código'] || c['codigo_cliente']));

            const filters = {
                filial,
                pasta,
                tipoVenda: tiposVendaSet,
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
            if (!dropdown) return selectedArray;
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

            if (!skipRender && listContainer) {
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

        function getActiveStockMap(filial) {
            const filterValue = filial || 'ambas';
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


        function updateComparisonView() {
            comparisonRenderId++;
            const currentRenderId = comparisonRenderId;
            const { currentSales, historySales } = getComparisonFilteredData();

            // Show Loading State on Charts (only if no chart exists)
            const chartContainers = ['weeklyComparisonChart', 'monthlyComparisonChart', 'dailyWeeklyComparisonChart'];
            chartContainers.forEach(id => {
                if (!charts[id]) {
                    const el = document.getElementById(id + 'Container');
                    if(el) el.innerHTML = '<div class="flex h-full items-center justify-center"><svg class="animate-spin h-8 w-8 text-teal-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg></div>';
                }
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
                if (!isAlternativeMode(selectedComparisonTiposVenda) && s.TIPOVENDA !== '1' && s.TIPOVENDA !== '9') return;
                const val = getValueForSale(s, selectedComparisonTiposVenda);

                metrics.current.fat += val;
                metrics.current.peso += s.TOTPESOLIQ;

                if (s.CODCLI) {
                    currentClientsSet.set(s.CODCLI, (currentClientsSet.get(s.CODCLI) || 0) + val);
                    if (!currentClientProductMap.has(s.CODCLI)) currentClientProductMap.set(s.CODCLI, new Map());
                    const cMap = currentClientProductMap.get(s.CODCLI);
                    if (!cMap.has(s.PRODUTO)) cMap.set(s.PRODUTO, { val: 0, desc: s.DESCRICAO, codfor: String(s.CODFOR) });
                    cMap.get(s.PRODUTO).val += val;
                }
                if (s.SUPERV) {
                    if (!metrics.charts.supervisorData[s.SUPERV]) metrics.charts.supervisorData[s.SUPERV] = { current: 0, history: 0 };
                    metrics.charts.supervisorData[s.SUPERV].current += val;
                }
                const d = parseDate(s.DTPED);
                if (d) {
                    const wIdx = currentMonthWeeks.findIndex(w => d >= w.start && d <= w.end);
                    if (wIdx !== -1) metrics.charts.weeklyCurrent[wIdx] += val;
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
                    if (!isAlternativeMode(selectedComparisonTiposVenda) && s.TIPOVENDA !== '1' && s.TIPOVENDA !== '9') return;
                    const val = getValueForSale(s, selectedComparisonTiposVenda);

                    metrics.history.fat += val;
                    metrics.history.peso += s.TOTPESOLIQ;

                    const d = parseDate(s.DTPED);
                    if (!d) return;

                    const monthKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
                    if (!historyMonths.has(monthKey)) historyMonths.set(monthKey, { fat: 0, clients: new Map(), productMap: new Map() });
                    const mData = historyMonths.get(monthKey);

                    mData.fat += val;

                    if (s.CODCLI) {
                        mData.clients.set(s.CODCLI, (mData.clients.get(s.CODCLI) || 0) + val);
                        if (!mData.productMap.has(s.CODCLI)) mData.productMap.set(s.CODCLI, new Map());
                        const cMap = mData.productMap.get(s.CODCLI);
                        if (!cMap.has(s.PRODUTO)) cMap.set(s.PRODUTO, { val: 0, desc: s.DESCRICAO, codfor: String(s.CODFOR) });
                        cMap.get(s.PRODUTO).val += val;
                    }

                    if (s.SUPERV) {
                        if (!metrics.charts.supervisorData[s.SUPERV]) metrics.charts.supervisorData[s.SUPERV] = { current: 0, history: 0 };
                        metrics.charts.supervisorData[s.SUPERV].history += val;
                    }

                    // Accumulate Day Totals for Day Weight Calculation
                    metrics.historicalDayTotals[d.getUTCDay()] += val;

                    if (!monthWeeksCache.has(monthKey)) monthWeeksCache.set(monthKey, getMonthWeeks(d.getUTCFullYear(), d.getUTCMonth()));
                    const weeks = monthWeeksCache.get(monthKey);
                    const wIdx = weeks.findIndex(w => d >= w.start && d <= w.end);
                    if (wIdx !== -1 && wIdx < metrics.charts.weeklyHistory.length) {
                        metrics.charts.weeklyHistory[wIdx] += val;
                    }
                    if (hasOverlap && d >= firstWeekStart && d < firstOfMonth) {
                        metrics.charts.weeklyCurrent[0] += val;
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

                    // Weekly Summary Table (Optimized)
                    const weeklySummaryTableBody = document.getElementById('weeklySummaryTableBody');
                    if (weeklySummaryTableBody) {
                         let grandTotal = 0;
                         const weekKeys = Object.keys(salesByWeekAndDay).sort((a,b) => parseInt(a) - parseInt(b));
                         const rowsHTML = weekKeys.map(weekNum => {
                             const weekTotal = Object.values(salesByWeekAndDay[weekNum]).reduce((a, b) => a + b, 0);
                             grandTotal += weekTotal;
                             return `<tr class="hover:bg-slate-700"><td class="px-4 py-2">Semana ${weekNum}</td><td class="px-4 py-2 text-right">${weekTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</td></tr>`;
                         }).join('');

                         weeklySummaryTableBody.innerHTML = rowsHTML + `<tr class="font-bold bg-slate-700/50"><td class="px-4 py-2">Total do Mês</td><td class="px-4 py-2 text-right">${grandTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</td></tr>`;
                    }

                    // Supervisor Table
                    const supervisorTableBody = document.getElementById('supervisorComparisonTableBody');
                    const supRows = Object.entries(m.charts.supervisorData).map(([sup, data]) => { const variation = data.history > 0 ? ((data.current - data.history) / data.history) * 100 : (data.current > 0 ? 100 : 0); const colorClass = variation > 0 ? 'text-green-400' : variation < 0 ? 'text-red-400' : 'text-slate-400'; return `<tr class="hover:bg-slate-700"><td class="px-4 py-2">${sup}</td><td class="px-4 py-2 text-right">${data.history.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</td><td class="px-4 py-2 text-right">${data.current.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })}</td><td class="px-4 py-2 text-right ${colorClass}">${variation.toFixed(2)}%</td></tr>`; }).join('');
                    supervisorTableBody.innerHTML = supRows;
                }, () => currentRenderId !== comparisonRenderId); // Cancel check
            }, () => currentRenderId !== comparisonRenderId); // Cancel check
        }



        function getInnovationsMonthFilteredData(options = {}) {
            const { excludeFilter = null } = options;

            const city = innovationsMonthCityFilter.value.trim().toLowerCase();
            const filial = innovationsMonthFilialFilter.value;

            let clients = getHierarchyFilteredClients('innovations-month', allClientsData);

            if (filial !== 'ambas') {
                clients = clients.filter(c => clientLastBranch.get(c['Código']) === filial);
            }

            if (excludeFilter !== 'city' && city) {
                clients = clients.filter(c => c.cidade && c.cidade.toLowerCase() === city);
            }

            return { clients };
        }

        function resetInnovationsMonthFilters() {
            innovationsMonthCityFilter.value = '';
            innovationsMonthFilialFilter.value = 'ambas';
            innovationsMonthCategoryFilter.value = '';
            selectedInnovationsMonthTiposVenda = [];

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
                let optionsHtml = '<option value="">Todas as Categorias</option>';
                allCategories.forEach(cat => {
                    optionsHtml += `<option value="${cat}">${cat}</option>`;
                });
                innovationsMonthCategoryFilter.innerHTML = optionsHtml;
                if (allCategories.includes(currentFilterValue)) {
                    innovationsMonthCategoryFilter.value = currentFilterValue;
                }
            }

            const { clients: filteredClients } = getInnovationsMonthFilteredData();


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

                // Filter History for Previous Month Only (Current Month - 1)
                const currentYear = lastSaleDate.getUTCFullYear();
                const currentMonth = lastSaleDate.getUTCMonth();
                // Start of Prev Month
                const prevMonthDate = new Date(Date.UTC(currentYear, currentMonth - 1, 1));
                const startTs = prevMonthDate.getTime();
                // End of Prev Month (Start of Current Month)
                const currentMonthStartDate = new Date(Date.UTC(currentYear, currentMonth, 1));
                const endTs = currentMonthStartDate.getTime();

                const previousMonthData = allHistoryData.filter(item => {
                    const val = item.DTPED;
                    let ts = 0;
                    if (typeof val === 'number') {
                        if (val < 100000) {
                             ts = Math.round((val - 25569) * 86400 * 1000);
                        } else {
                             ts = val;
                        }
                    } else {
                        const d = parseDate(val);
                        if(d) ts = d.getTime();
                    }
                    return ts >= startTs && ts < endTs;
                });

                mapsPrevious = buildInnovationSalesMaps(previousMonthData, mainTypes, bonusTypes);

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
            if (innovationsByClientLegend) innovationsByClientLegend.innerHTML = `<strong>Legenda:</strong> ${categoryLegendForExport.join('; ')}`;

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
            if (innovationsByClientTableHead) innovationsByClientTableHead.innerHTML = tableHeadHTML;

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
                if (innovationsByClientTableBody) innovationsByClientTableBody.innerHTML = tableBodyHTML;

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
            if (hierarchyState['innovations-month'] && hierarchyState['innovations-month'].promotors.size === 1) {
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
            if (hierarchyState['coverage'] && hierarchyState['coverage'].promotors.size === 1) {
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
            if (hierarchyState['city'] && hierarchyState['city'].promotors.size === 1) {
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

        function toggleMobileMenu() {
            const mobileMenu = document.getElementById('mobile-menu');
            if (mobileMenu) {
                if (mobileMenu.classList.contains('hidden')) {
                    mobileMenu.classList.remove('hidden');
                    setTimeout(() => mobileMenu.classList.add('open'), 10);
                } else {
                    mobileMenu.classList.remove('open');
                    setTimeout(() => mobileMenu.classList.add('hidden'), 300);
                }
            }
        }

        function navigateTo(view) {
            window.location.hash = view;
        }

        function showViewElement(el) {
            if (!el) return;
            el.classList.remove('hidden');
            el.classList.add('animate-fade-in-up');
            setTimeout(() => {
                el.classList.remove('animate-fade-in-up');
            }, 700);
        }

        async function renderView(view) {
            // Access Control for Comparison View
            // Restrict 'comparativo' to ADM and COORD only
            if (view === 'comparativo') {
                // Normalize role check (userHierarchyContext is more reliable than window.userRole here as it is resolved)
                const contextRole = userHierarchyContext.role;
                const isAuth = contextRole === 'adm' || contextRole === 'coord';

                if (!isAuth) {
                    console.warn(`[Access] Unauthorized access to 'comparativo'. Redirecting.`);
                    view = 'dashboard';
                    // alert("Acesso restrito a Coordenadores e Administradores.");
                }
            }

            const mobileMenu = document.getElementById('mobile-menu');
            if (mobileMenu && mobileMenu.classList.contains('open')) {
                toggleMobileMenu();
            }

            const viewNameMap = {
                dashboard: 'Visão Geral',
                pedidos: 'Pedidos',
                comparativo: 'Comparativo',
                estoque: 'Estoque',
                cobertura: 'Cobertura',
                cidades: 'Geolocalização',
                'inovacoes-mes': 'Inovações',
                mix: 'Mix',
                'meta-realizado': 'Meta Vs. Realizado',
                'goals': 'Metas',
                'consultas': 'Consultas',
                'clientes': 'Clientes',
                'produtos': 'Produtos'
            };
            const friendlyName = viewNameMap[view] || 'a página';

            await showLoader(`Carregando ${friendlyName}...`);

            // This function now runs after the loader is visible
            const updateContent = () => {
                [
                    mainDashboard,
                    cityView,
                    comparisonView,
                    stockView,
                    innovationsMonthView,
                    coverageView,
                    document.getElementById('mix-view'),
                    goalsView,
                    document.getElementById('meta-realizado-view'),
                    document.getElementById('ai-insights-full-page'),
                    document.getElementById('wallet-view'),
                    document.getElementById('consultas-view'),
                    document.getElementById('clientes-view'),
                    document.getElementById('produtos-view')
                ].forEach(el => {
                    if(el) el.classList.add('hidden');
                });

                document.querySelectorAll('.nav-link').forEach(link => {
                    link.classList.remove('active-nav');
                });

                const activeLink = document.querySelector(`.nav-link[data-target="${view}"]`);
                if (activeLink) {
                    activeLink.classList.add('active-nav');
                }
                // Also update mobile active state
                document.querySelectorAll('.mobile-nav-link').forEach(link => {
                    if (link.dataset.target === view) {
                        link.classList.add('bg-blue-600', 'text-white');
                        link.classList.remove('bg-slate-800', 'text-slate-300');
                    } else {
                        link.classList.remove('bg-blue-600', 'text-white');
                        link.classList.add('bg-slate-800', 'text-slate-300');
                    }
                });

                switch(view) {
                    case 'consultas':
                        showViewElement(document.getElementById('consultas-view'));
                        break;
                    case 'clientes':
                        showViewElement(document.getElementById('clientes-view'));
                        break;
                    case 'produtos':
                        showViewElement(document.getElementById('produtos-view'));
                        break;
                    case 'wallet':
                        showViewElement(document.getElementById('wallet-view'));
                        if (typeof renderWalletView === 'function') renderWalletView();
                        break;
                    case 'dashboard':
                        showViewElement(mainDashboard);
                        chartView.classList.remove('hidden');
                        tableView.classList.add('hidden');
                        tablePaginationControls.classList.add('hidden');
                        if (viewState.dashboard.dirty) {
                            updateAllVisuals();
                            viewState.dashboard.dirty = false;
                        }
                        break;
                    case 'pedidos':
                        showViewElement(mainDashboard);
                        chartView.classList.add('hidden');
                        tableView.classList.remove('hidden');
                        tablePaginationControls.classList.remove('hidden');
                        if (viewState.pedidos.dirty) {
                            updateAllVisuals();
                            viewState.pedidos.dirty = false;
                        }
                        break;
                    case 'comparativo':
                        showViewElement(comparisonView);
                        if (viewState.comparativo.dirty) {
                            updateAllComparisonFilters();
                            updateComparisonView();
                            viewState.comparativo.dirty = false;
                        }
                        break;
                    case 'estoque':
                        if (stockView) showViewElement(stockView);
                        if (viewState.estoque.dirty) {
                            handleStockFilterChange();
                            viewState.estoque.dirty = false;
                        }
                        break;
                    case 'cobertura':
                        showViewElement(coverageView);
                        if (viewState.cobertura.dirty) {
                            updateAllCoverageFilters();
                            updateCoverageView();
                            viewState.cobertura.dirty = false;
                        }
                        break;
                    case 'cidades':
                        showViewElement(cityView);
                        // Always trigger background sync if admin
                        syncGlobalCoordinates();
                        if (viewState.cidades.dirty) {
                            updateAllCityFilters();
                            updateCityView();
                            viewState.cidades.dirty = false;
                        }
                        break;
                    case 'inovacoes-mes':
                        showViewElement(innovationsMonthView);
                        if (viewState.inovacoes.dirty) {
                            selectedInnovationsMonthTiposVenda = updateTipoVendaFilter(innovationsMonthTipoVendaFilterDropdown, innovationsMonthTipoVendaFilterText, selectedInnovationsMonthTiposVenda, [...allSalesData, ...allHistoryData]);
                            updateInnovationsMonthView();
                            viewState.inovacoes.dirty = false;
                        }
                        break;
                    case 'mix':
                        showViewElement(document.getElementById('mix-view'));
                        if (viewState.mix.dirty) {
                            updateAllMixFilters();
                            updateMixView();
                            viewState.mix.dirty = false;
                        }
                        break;
                    case 'goals':
                        showViewElement(goalsView);
                        if (viewState.goals.dirty) {
                            updateGoalsView();
                            viewState.goals.dirty = false;
                        }
                        break;
                    case 'meta-realizado':
                        showViewElement(document.getElementById('meta-realizado-view'));
                        if (viewState.metaRealizado.dirty) {
                            // Initial filter logic if needed, similar to other views

                            updateMetaRealizadoView();
                            viewState.metaRealizado.dirty = false;
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

            const statusText = document.getElementById('status-text');
            const progressBar = document.getElementById('progress-bar');
            const statusContainer = document.getElementById('status-container');

            statusContainer.classList.remove('hidden');
            const updateStatus = (msg, percent) => {
                statusText.textContent = msg;
                progressBar.style.width = `${percent}%`;
            };

            // --- OPTIMIZATION: Increased Batch Size & Concurrency ---
            const BATCH_SIZE = 3000;
            const CONCURRENT_REQUESTS = 7;

            const retryOperation = async (operation, retries = 3, delay = 1000) => {
                for (let i = 0; i < retries; i++) {
                    try {
                        return await operation();
                    } catch (error) {
                        if (i === retries - 1) throw error;
                        console.warn(`Tentativa ${i + 1} falhou. Retentando em ${delay}ms...`, error);
                        await new Promise(resolve => setTimeout(resolve, delay));
                        delay *= 2; // Exponential backoff
                    }
                }
            };

            const performUpsert = async (table, batch) => {
                await retryOperation(async () => {
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
                });
            };

            const clearTable = async (table, pkColumn = 'id') => {
                await retryOperation(async () => {
                    // Tenta limpar usando a função RPC 'truncate_table' (muito mais rápido e sem timeout)
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
                            const errorText = await rpcResponse.text();
                            console.warn(`RPC truncate_table falhou para ${table} (Status: ${rpcResponse.status}). Msg: ${errorText}. Tentando DELETE convencional...`);
                        }
                    } catch (e) {
                        console.warn(`Erro ao chamar RPC truncate_table para ${table}, tentando DELETE convencional...`, e);
                    }

                    // Fallback: Deleta todas as linhas da tabela
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
                });
            };

            // List of columns that are dates and need conversion from timestamp (ms) to ISO String
            const dateColumns = new Set(['dtped', 'dtsaida', 'ultimacompra', 'datacadastro', 'dtcadastro', 'updated_at']);

            const formatValue = (key, value) => {
                if (dateColumns.has(key) && typeof value === 'number') {
                    try {
                        return new Date(value).toISOString();
                    } catch (e) {
                        return null;
                    }
                }
                return value;
            };

            // --- Unified Parallel Uploader ---
            const uploadBatchParallel = async (table, dataObj, isColumnar) => {
                const totalRows = isColumnar ? dataObj.length : dataObj.length;
                if (totalRows === 0) return;

                const totalBatches = Math.ceil(totalRows / BATCH_SIZE);
                let processedBatches = 0;

                const processChunk = async (chunkIndex) => {
                    const start = chunkIndex * BATCH_SIZE;
                    const end = Math.min(start + BATCH_SIZE, totalRows);
                    const batch = [];

                    if (isColumnar) {
                        const columns = dataObj.columns;
                        const values = dataObj.values;
                        // Pre-calculate lower keys to avoid repeated toLowerCase()
                        const colKeys = columns.map(c => c.toLowerCase());

                        for (let j = start; j < end; j++) {
                            const row = {};
                            for (let k = 0; k < columns.length; k++) {
                                const col = columns[k];
                                const lowerKey = colKeys[k];
                                row[lowerKey] = formatValue(lowerKey, values[col][j]);
                            }
                            batch.push(row);
                        }
                    } else {
                        // Array of Objects
                        for (let j = start; j < end; j++) {
                            const item = dataObj[j];
                            const newItem = {};
                            for (const key in item) {
                                const lowerKey = key.toLowerCase();
                                newItem[lowerKey] = formatValue(lowerKey, item[key]);
                            }
                            batch.push(newItem);
                        }
                    }

                    await performUpsert(table, batch);
                    processedBatches++;
                    const progress = Math.round((processedBatches / totalBatches) * 100);
                    updateStatus(`Enviando ${table}: ${progress}%`, progress);
                };

                // Queue worker pattern
                const queue = Array.from({ length: totalBatches }, (_, i) => i);
                const worker = async () => {
                    while (queue.length > 0) {
                        const index = queue.shift();
                        await processChunk(index);
                    }
                };

                const workers = Array.from({ length: Math.min(CONCURRENT_REQUESTS, totalBatches) }, worker);
                await Promise.all(workers);
            };

            try {
                if (data.detailed && data.detailed.length > 0) {
                    await clearTable('data_detailed');
                    await uploadBatchParallel('data_detailed', data.detailed, true);
                }
                if (data.history && data.history.length > 0) {
                    await clearTable('data_history');
                    await uploadBatchParallel('data_history', data.history, true);
                }
                if (data.byOrder && data.byOrder.length > 0) {
                    await clearTable('data_orders');
                    await uploadBatchParallel('data_orders', data.byOrder, false);
                }
                if (data.clients && data.clients.length > 0) {
                    await clearTable('data_clients');
                    await uploadBatchParallel('data_clients', data.clients, true);
                }
                if (data.stock && data.stock.length > 0) {
                    await clearTable('data_stock');
                    await uploadBatchParallel('data_stock', data.stock, false);
                }
                if (data.innovations && data.innovations.length > 0) {
                    await clearTable('data_innovations');
                    await uploadBatchParallel('data_innovations', data.innovations, false);
                }
                if (data.product_details && data.product_details.length > 0) {
                    await clearTable('data_product_details', 'code');
                    await uploadBatchParallel('data_product_details', data.product_details, false);
                }
                if (data.active_products && data.active_products.length > 0) {
                    await clearTable('data_active_products', 'code');
                    await uploadBatchParallel('data_active_products', data.active_products, false);
                }
                if (data.hierarchy && data.hierarchy.length > 0) {
                    await clearTable('data_hierarchy');
                    await uploadBatchParallel('data_hierarchy', data.hierarchy, false);
                }
                if (data.metadata && data.metadata.length > 0) {
                    // Update last_update timestamp
                    const now = new Date();
                    const lastUpdateIdx = data.metadata.findIndex(m => m.key === 'last_update');
                    if (lastUpdateIdx !== -1) {
                        data.metadata[lastUpdateIdx].value = now.toISOString();
                    } else {
                        data.metadata.push({ key: 'last_update', value: now.toISOString() });
                    }

                    // --- PRESERVE MANUAL KEYS ---
                    try {
                        const keysToPreserve = ['groq_api_key', 'senha_modal'];
                        const { data: currentMetadata } = await window.supabaseClient
                            .from('data_metadata')
                            .select('*')
                            .in('key', keysToPreserve);

                        if (currentMetadata && currentMetadata.length > 0) {
                            currentMetadata.forEach(item => {
                                const existsInNew = data.metadata.some(newM => newM.key === item.key);
                                if (!existsInNew) {
                                    data.metadata.push(item);
                                }
                            });
                        }
                    } catch (e) {
                        console.warn("[Upload] Failed to preserve manual keys:", e);
                    }

                    await clearTable('data_metadata', 'key');
                    await uploadBatchParallel('data_metadata', data.metadata, false);

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
                if (msg.includes('403') || msg.includes('row-level security') || msg.includes('violates row-level security policy') || msg.includes('Access denied')) {
                     msg = "Permissão negada. Verifique se seu usuário tem permissão de 'adm' no Supabase. " + msg;
                }
                updateStatus('Erro: ' + msg, 0);
                alert('Erro durante o upload: ' + msg);
            }
        }
        // Helper to mark dirty states
        const markDirty = (view) => {
            if (viewState[view]) viewState[view].dirty = true;
        };

        // --- Dashboard/Pedidos Filters ---
        const updateDashboard = () => {
            markDirty('dashboard'); markDirty('pedidos');
            updateAllVisuals();
        };

        function searchLocalClients(query) {
            if (!query || query.length < 3) return [];
            query = query.toLowerCase();
            const cleanQuery = query.replace(/[^a-z0-9]/g, '');

            const results = [];
            const indices = optimizedData.searchIndices.clients;
            const limit = 10;

            if (!indices || indices.length === 0) return [];

            for (let i = 0; i < indices.length; i++) {
                const idx = indices[i];
                if (!idx) continue;

                // Match Code (Exact or partial)
                if (idx.code.includes(cleanQuery)) {
                    results.push(allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i]);
                    if (results.length >= limit) break;
                    continue;
                }
                // Match Name
                if (idx.nameLower && idx.nameLower.includes(query)) {
                    results.push(allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i]);
                    if (results.length >= limit) break;
                    continue;
                }
                // Match CNPJ
                if (idx.cnpj && idx.cnpj.includes(cleanQuery)) {
                    results.push(allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i]);
                    if (results.length >= limit) break;
                    continue;
                }
            }
            return results;
        }

        function setupClientTypeahead(inputId, suggestionsId, onSelect) {
            const input = document.getElementById(inputId);
            const suggestions = document.getElementById(suggestionsId);
            if (!input || !suggestions) return;

            let debounce;

            input.addEventListener('input', (e) => {
                const val = e.target.value;
                if (!val || val.length < 3) {
                    suggestions.classList.add('hidden');
                    return;
                }

                clearTimeout(debounce);
                debounce = setTimeout(() => {
                    const results = searchLocalClients(val);
                    renderSuggestions(results);
                }, 300);
            });

            // Close on click outside
            document.addEventListener('click', (e) => {
                if (!input.contains(e.target) && !suggestions.contains(e.target)) {
                    suggestions.classList.add('hidden');
                }
            });

            function renderSuggestions(results) {
                suggestions.innerHTML = '';
                if (results.length === 0) {
                    suggestions.classList.add('hidden');
                    return;
                }

                results.forEach(c => {
                    const div = document.createElement('div');
                    div.className = 'px-4 py-3 border-b border-slate-700 hover:bg-slate-700 cursor-pointer flex justify-between items-center group';

                    const code = c['Código'] || c['codigo_cliente'];
                    const name = c.fantasia || c.nomeCliente || c.razaoSocial || 'Sem Nome';
                    const city = c.cidade || c.CIDADE || '';
                    const doc = c['CNPJ/CPF'] || c.cnpj_cpf || '';

                    div.innerHTML = `
                        <div>
                            <div class="text-sm font-bold text-white group-hover:text-blue-300 transition-colors">
                                <span class="font-mono text-slate-400 mr-2">${code}</span>
                                ${name}
                            </div>
                            <div class="text-xs text-slate-500">${city} • ${doc}</div>
                        </div>
                         <div class="p-2 bg-slate-800 rounded-full group-hover:bg-blue-600 transition-colors text-slate-400 group-hover:text-white">
                             <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
                        </div>
                    `;
                    div.onclick = () => {
                        input.value = code;
                        suggestions.classList.add('hidden');
                        if (onSelect) onSelect(code);
                    };
                    suggestions.appendChild(div);
                });
                suggestions.classList.remove('hidden');
            }
        }

        function handleClientFilterCascade(clientCode, viewPrefix) {
            if (!clientCode) return;

            // Apply only for Co-Coord and up
            const role = userHierarchyContext.role;
            if (role !== 'adm' && role !== 'coord' && role !== 'cocoord') return;

            const normalizedCode = normalizeKey(clientCode);

            // 1. Auto-Select Promoter
            if (optimizedData.clientHierarchyMap) {
                const node = optimizedData.clientHierarchyMap.get(normalizedCode);
                if (node && hierarchyState[viewPrefix]) {
                    const promotorCode = node.promotor.code;
                    if (promotorCode) {
                        hierarchyState[viewPrefix].promotors.clear();
                        hierarchyState[viewPrefix].promotors.add(promotorCode);
                        updateHierarchyDropdown(viewPrefix, 'promotor');
                    }
                }
            }

            // 2. Update Supplier Filter Options based on Client Data
            const salesIndices = optimizedData.indices.current.byClient.get(normalizedCode);
            const historyIndices = optimizedData.indices.history.byClient.get(normalizedCode);

            const filteredRows = [];
            if (salesIndices) salesIndices.forEach(i => filteredRows.push(allSalesData instanceof ColumnarDataset ? allSalesData.get(i) : allSalesData[i]));
            if (historyIndices) historyIndices.forEach(i => filteredRows.push(allHistoryData instanceof ColumnarDataset ? allHistoryData.get(i) : allHistoryData[i]));

            if (filteredRows.length > 0) {
                if (viewPrefix === 'main') {
                     // Note: We don't change 'selectedMainSuppliers' here, just the options available.
                     // The user said: "se eu filtrar um cliente... só deve aparecer para selecionar esse fornecedor"
                     updateSupplierFilter(document.getElementById('fornecedor-filter-dropdown'), document.getElementById('fornecedor-filter-text'), selectedMainSuppliers, filteredRows, 'main');
                }
            }
        }

        function setupEventListeners() {
            // Drag-to-Scroll for Desktop Nav
            const navContainer = document.getElementById('desktop-nav-container');
            if (navContainer) {
                let isDown = false;
                let startX;
                let scrollLeft;

                navContainer.addEventListener('mousedown', (e) => {
                    isDown = true;
                    navContainer.classList.add('cursor-grabbing');
                    startX = e.pageX - navContainer.offsetLeft;
                    scrollLeft = navContainer.scrollLeft;
                });

                navContainer.addEventListener('mouseleave', () => {
                    isDown = false;
                    navContainer.classList.remove('cursor-grabbing');
                });

                navContainer.addEventListener('mouseup', () => {
                    isDown = false;
                    navContainer.classList.remove('cursor-grabbing');
                });

                navContainer.addEventListener('mousemove', (e) => {
                    if (!isDown) return;
                    e.preventDefault();
                    const x = e.pageX - navContainer.offsetLeft;
                    const walk = (x - startX) * 2; // Scroll-fast
                    navContainer.scrollLeft = scrollLeft - walk;
                });
            }

            // Uploader Logic
            const openAdminBtn = document.getElementById('open-admin-btn');
            const adminModal = document.getElementById('admin-uploader-modal');
            const adminCloseBtn = document.getElementById('admin-modal-close-btn');

            // Password Modal Elements
            const pwdModal = document.getElementById('admin-password-modal');
            const pwdInput = document.getElementById('admin-password-input');
            const pwdConfirm = document.getElementById('admin-password-confirm-btn');
            const pwdCancel = document.getElementById('admin-password-cancel-btn');

            const openAdminModal = () => {
                if (pwdModal) pwdModal.classList.add('hidden');
                if (adminModal) adminModal.classList.remove('hidden');
                // Close mobile menu if open
                const mobileMenu = document.getElementById('mobile-menu');
                if (mobileMenu && mobileMenu.classList.contains('open')) {
                    toggleMobileMenu(); // Assuming this function exists in scope
                }
            };

            const checkPassword = () => {
                const input = pwdInput.value;
                // Find password in metadata
                let storedPwd = 'admin'; // Default fallback
                if (embeddedData.metadata && Array.isArray(embeddedData.metadata)) {
                    const entry = embeddedData.metadata.find(m => m.key === 'senha_modal');
                    if (entry && entry.value) storedPwd = entry.value;
                }
                
                if (input === storedPwd) {
                    openAdminModal();
                } else {
                    alert('Senha incorreta.');
                    pwdInput.value = '';
                    pwdInput.focus();
                }
            };

            if (openAdminBtn) {
                openAdminBtn.addEventListener('click', () => {
                    if (window.userRole === 'adm') {
                        openAdminModal();
                    } else {
                        // Show Password Prompt
                        if (pwdModal) {
                            pwdInput.value = '';
                            pwdModal.classList.remove('hidden');
                            pwdInput.focus();
                        }
                    }
                });
            }

            if (pwdConfirm) {
                pwdConfirm.addEventListener('click', checkPassword);
            }
            if (pwdCancel) {
                pwdCancel.addEventListener('click', () => {
                    if (pwdModal) pwdModal.classList.add('hidden');
                });
            }
            if (pwdInput) {
                pwdInput.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter') checkPassword();
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
                    const hierarchyFile = document.getElementById('hierarchy-file-input').files[0];

                    if (!salesFile && !historyFile && !hierarchyFile) {
                        alert("Pelo menos um arquivo (Vendas, Histórico ou Hierarquia) é necessário.");
                        return;
                    }

                    // Initialize Worker
                    const worker = new Worker('worker.js');

                    document.getElementById('status-container').classList.remove('hidden');
                    document.getElementById('status-text').textContent = "Processando arquivos...";

                    worker.postMessage({ salesFile, clientsFile, productsFile, historyFile, innovationsFile, hierarchyFile });

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

            const mobileMenuToggle = document.getElementById('mobile-menu-toggle');
            if (mobileMenuToggle) mobileMenuToggle.addEventListener('click', toggleMobileMenu);

            const handleNavClick = (e) => {
                const target = e.currentTarget.dataset.target;
                if (e.ctrlKey || e.metaKey) {
                    e.preventDefault();
                    const url = new URL(window.location.href);
                    url.searchParams.set('ir_para', target);
                    window.open(url.toString(), '_blank');
                } else {
                    navigateTo(target);
                }
            };

            document.querySelectorAll('.nav-link').forEach(link => link.addEventListener('click', handleNavClick));
            
            document.querySelectorAll('.mobile-nav-link').forEach(link => {
                link.addEventListener('click', (e) => {
                    handleNavClick(e);
                    toggleMobileMenu();
                });
            });

            const supervisorFilterBtn = document.getElementById('supervisor-filter-btn');
            const supervisorFilterDropdown = document.getElementById('supervisor-filter-dropdown');
            if (supervisorFilterBtn && supervisorFilterDropdown) {
                supervisorFilterBtn.addEventListener('click', () => supervisorFilterDropdown.classList.toggle('hidden'));
                supervisorFilterDropdown.addEventListener('change', (e) => {
                    if (e.target.type === 'checkbox') {
                        const { value, checked } = e.target;
                        mainTableState.currentPage = 1;
                        updateDashboard();
                    }
                });
            }

            const fornecedorFilterBtn = document.getElementById('fornecedor-filter-btn');
            const fornecedorFilterDropdown = document.getElementById('fornecedor-filter-dropdown');
            if (fornecedorFilterBtn && fornecedorFilterDropdown) {
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
            }

            if (vendedorFilterBtn && vendedorFilterDropdown) {
                vendedorFilterBtn.addEventListener('click', () => vendedorFilterDropdown.classList.toggle('hidden'));
                vendedorFilterDropdown.addEventListener('change', (e) => {
                    if (e.target.type === 'checkbox') {
                        const { value, checked } = e.target;
                        mainTableState.currentPage = 1;
                        updateDashboard();
                    }
                });
            }

            if (tipoVendaFilterBtn && tipoVendaFilterDropdown) {
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
            }

            if (posicaoFilter) posicaoFilter.addEventListener('change', () => { mainTableState.currentPage = 1; updateDashboard(); });
            const debouncedUpdateDashboard = debounce(updateDashboard, 400);
            if (codcliFilter) {
                setupClientTypeahead('codcli-filter', 'codcli-filter-suggestions', (code) => {
                    handleClientFilterCascade(code, 'main');
                    mainTableState.currentPage = 1;
                    debouncedUpdateDashboard();
                });
                codcliFilter.addEventListener('input', (e) => {
                    const val = e.target.value.trim();
                    if (val && clientMapForKPIs.has(normalizeKey(val))) {
                         handleClientFilterCascade(val, 'main');
                    }
                    mainTableState.currentPage = 1;
                    debouncedUpdateDashboard();
                });
                // Make Lupa Icon Interactive
                const codcliSearchIcon = document.getElementById('codcli-search-icon');
                if (codcliSearchIcon) {
                    codcliSearchIcon.addEventListener('click', () => {
                        codcliFilter.focus();
                        mainTableState.currentPage = 1;
                        updateDashboard(); // Immediate update
                    });
                }
            }

            const goalsGvCodcliFilter = document.getElementById('goals-gv-codcli-filter');
            if (goalsGvCodcliFilter) {
                setupClientTypeahead('goals-gv-codcli-filter', 'goals-gv-codcli-filter-suggestions', (code) => {
                    handleClientFilterCascade(code, 'goals-gv');
                    if (typeof updateGoalsView === 'function') {
                        goalsTableState.currentPage = 1;
                        updateGoalsView();
                    } else {
                        goalsGvCodcliFilter.dispatchEvent(new Event('input'));
                    }
                });
                goalsGvCodcliFilter.addEventListener('input', (e) => {
                    const val = e.target.value.trim();
                    if (val && clientMapForKPIs.has(normalizeKey(val))) {
                         handleClientFilterCascade(val, 'goals-gv');
                    }
                    if (typeof updateGoalsView === 'function') {
                        goalsTableState.currentPage = 1;
                        updateGoalsView();
                    }
                });
                // Make Goals Lupa Icon Interactive
                const goalsGvSearchIcon = document.getElementById('goals-gv-search-icon');
                if (goalsGvSearchIcon) {
                    goalsGvSearchIcon.addEventListener('click', () => {
                        goalsGvCodcliFilter.focus();
                        if (typeof updateGoalsView === 'function') {
                            goalsTableState.currentPage = 1;
                            updateGoalsView();
                        } else {
                            goalsGvCodcliFilter.dispatchEvent(new Event('input'));
                        }
                    });
                }
            }
            if (clearFiltersBtn) clearFiltersBtn.addEventListener('click', () => { resetMainFilters(); markDirty('dashboard'); markDirty('pedidos'); });

            if (prevPageBtn) {
                prevPageBtn.addEventListener('click', () => {
                    if (mainTableState.currentPage > 1) {
                        mainTableState.currentPage--;
                        renderTable(mainTableState.filteredData);
                    }
                });
            }
            if (nextPageBtn) {
                nextPageBtn.addEventListener('click', () => {
                    if (mainTableState.currentPage < mainTableState.totalPages) {
                        mainTableState.currentPage++;
                        renderTable(mainTableState.filteredData);
                    }
                });
            }

            if (mainComRedeBtn) mainComRedeBtn.addEventListener('click', () => mainRedeFilterDropdown.classList.toggle('hidden'));
            if (mainRedeGroupContainer) {
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
            }
            if (mainRedeFilterDropdown) {
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
            }

            // --- City View Filters ---
            const updateCity = () => {
                markDirty('cidades');
                handleCityFilterChange();
            };
            if (citySupplierFilterDropdown) {
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
            }

            if (cityTipoVendaFilterBtn && cityTipoVendaFilterDropdown) {
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
            }

            if (cityComRedeBtn) cityComRedeBtn.addEventListener('click', () => cityRedeFilterDropdown.classList.toggle('hidden'));
            if (cityRedeGroupContainer) {
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
            }
            if (cityRedeFilterDropdown) {
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
            }

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

            if (clearCityFiltersBtn) clearCityFiltersBtn.addEventListener('click', () => { resetCityFilters(); markDirty('cidades'); });
            const debouncedUpdateCity = debounce(updateCity, 400);
            if (cityCodCliFilter) {
                cityCodCliFilter.addEventListener('input', (e) => {
                    e.target.value = e.target.value.replace(/[^0-9]/g, '');
                    debouncedUpdateCity();
                });
            }

            const debouncedCitySearch = debounce(() => {
                const { clients } = getCityFilteredData({ excludeFilter: 'city' });
                updateCitySuggestions(cityNameFilter, citySuggestions, clients);
            }, 300);

            if (cityNameFilter) {
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
            }

            document.addEventListener('click', (e) => {
                if (supervisorFilterBtn && supervisorFilterDropdown && !supervisorFilterBtn.contains(e.target) && !supervisorFilterDropdown.contains(e.target)) supervisorFilterDropdown.classList.add('hidden');
                if (fornecedorFilterBtn && fornecedorFilterDropdown && !fornecedorFilterBtn.contains(e.target) && !fornecedorFilterDropdown.contains(e.target)) fornecedorFilterDropdown.classList.add('hidden');
                if (vendedorFilterBtn && vendedorFilterDropdown && !vendedorFilterBtn.contains(e.target) && !vendedorFilterDropdown.contains(e.target)) vendedorFilterDropdown.classList.add('hidden');
                if (tipoVendaFilterBtn && tipoVendaFilterDropdown && !tipoVendaFilterBtn.contains(e.target) && !tipoVendaFilterDropdown.contains(e.target)) tipoVendaFilterDropdown.classList.add('hidden');

                if (citySupplierFilterBtn && citySupplierFilterDropdown && !citySupplierFilterBtn.contains(e.target) && !citySupplierFilterDropdown.contains(e.target)) citySupplierFilterDropdown.classList.add('hidden');
                if (cityTipoVendaFilterBtn && cityTipoVendaFilterDropdown && !cityTipoVendaFilterBtn.contains(e.target) && !cityTipoVendaFilterDropdown.contains(e.target)) cityTipoVendaFilterDropdown.classList.add('hidden');
                if (cityComRedeBtn && cityRedeFilterDropdown && !cityComRedeBtn.contains(e.target) && !cityRedeFilterDropdown.contains(e.target)) cityRedeFilterDropdown.classList.add('hidden');
                if (mainComRedeBtn && mainRedeFilterDropdown && !mainComRedeBtn.contains(e.target) && !mainRedeFilterDropdown.contains(e.target)) mainRedeFilterDropdown.classList.add('hidden');

                if (comparisonComRedeBtn && comparisonRedeFilterDropdown && !comparisonComRedeBtn.contains(e.target) && !comparisonRedeFilterDropdown.contains(e.target)) comparisonRedeFilterDropdown.classList.add('hidden');
                if (comparisonTipoVendaFilterBtn && comparisonTipoVendaFilterDropdown && !comparisonTipoVendaFilterBtn.contains(e.target) && !comparisonTipoVendaFilterDropdown.contains(e.target)) comparisonTipoVendaFilterDropdown.classList.add('hidden');
                if (comparisonSupplierFilterBtn && comparisonSupplierFilterDropdown && !comparisonSupplierFilterBtn.contains(e.target) && !comparisonSupplierFilterDropdown.contains(e.target)) comparisonSupplierFilterDropdown.classList.add('hidden');
                if (comparisonProductFilterBtn && comparisonProductFilterDropdown && !comparisonProductFilterBtn.contains(e.target) && !comparisonProductFilterDropdown.contains(e.target)) comparisonProductFilterDropdown.classList.add('hidden');


                if (e.target.closest('[data-pedido-id]')) { e.preventDefault(); openModal(e.target.closest('[data-pedido-id]').dataset.pedidoId); }
                if (e.target.closest('[data-codcli]')) { e.preventDefault(); openClientModal(e.target.closest('[data-codcli]').dataset.codcli); }
                if (e.target.closest('#city-suggestions > div')) { if(cityNameFilter) cityNameFilter.value = e.target.textContent; citySuggestions.classList.add('hidden'); updateCityView(); }
                if (e.target.closest('#comparison-city-suggestions > div')) { if(comparisonCityFilter) comparisonCityFilter.value = e.target.textContent; comparisonCitySuggestions.classList.add('hidden'); updateAllComparisonFilters(); updateComparisonView(); }
                else if (comparisonCityFilter && !comparisonCityFilter.contains(e.target)) comparisonCitySuggestions.classList.add('hidden');
            });

            fornecedorToggleContainerEl.querySelectorAll('.fornecedor-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fornecedor = btn.dataset.fornecedor;
                    if (currentFornecedor === fornecedor) { currentFornecedor = ''; btn.classList.remove('active'); } else { currentFornecedor = fornecedor; fornecedorToggleContainerEl.querySelectorAll('.fornecedor-btn').forEach(b => b.classList.remove('active')); btn.classList.add('active'); }

                    // Update Supplier Filter Options
                    let supplierDataSource = [...allSalesData, ...allHistoryData];
                    if (currentFornecedor) {
                        supplierDataSource = supplierDataSource.filter(s => s.OBSERVACAOFOR === currentFornecedor);
                    }
                    selectedMainSuppliers = updateSupplierFilter(fornecedorFilterDropdown, document.getElementById('fornecedor-filter-text'), selectedMainSuppliers, supplierDataSource, 'main');
                    mainTableState.currentPage = 1;

                    updateDashboard();
                });
            });

            const updateComparison = () => {
                markDirty('comparativo');
                updateAllComparisonFilters();
                updateComparisonView();
            };

            const handleComparisonFilterChange = updateComparison;

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

            const debouncedComparisonCityUpdate = debounce(() => {
                const { currentSales, historySales } = getComparisonFilteredData({ excludeFilter: 'city' });
                comparisonCitySuggestions.classList.remove('manual-hide');
                updateComparisonCitySuggestions([...currentSales, ...historySales]);
            }, 300);

            comparisonCityFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                debouncedComparisonCityUpdate();
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

            const resetComparisonFilters = () => {
                selectedComparisonTiposVenda = [];
                currentComparisonFornecedor = 'PEPSICO';
                selectedComparisonSuppliers = [];
                comparisonRedeGroupFilter = '';
                selectedComparisonRedes = [];

                if (comparisonCityFilter) comparisonCityFilter.value = '';

                if (comparisonTipoVendaFilterDropdown) {
                    comparisonTipoVendaFilterDropdown.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
                    updateTipoVendaFilter(comparisonTipoVendaFilterDropdown, comparisonTipoVendaFilterText, selectedComparisonTiposVenda, [...allSalesData, ...allHistoryData]);
                }

                if (comparisonSupplierFilterDropdown) {
                    comparisonSupplierFilterDropdown.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
                    updateSupplierFilter(comparisonSupplierFilterDropdown, comparisonSupplierFilterText, selectedComparisonSuppliers, supplierOptionsData, 'comparison');
                }

                if (comparisonRedeFilterDropdown) {
                    comparisonRedeFilterDropdown.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
                }

                if (comparisonRedeGroupContainer) {
                    comparisonRedeGroupContainer.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                    const defaultBtn = comparisonRedeGroupContainer.querySelector('button[data-group=""]');
                    if (defaultBtn) defaultBtn.classList.add('active');
                    updateRedeFilter(comparisonRedeFilterDropdown, comparisonComRedeBtnText, selectedComparisonRedes, allClientsData);
                }

                if (comparisonFornecedorToggleContainer) {
                    comparisonFornecedorToggleContainer.querySelectorAll('.fornecedor-btn').forEach(b => b.classList.remove('active'));
                    const pepsicoBtn = comparisonFornecedorToggleContainer.querySelector('button[data-fornecedor="PEPSICO"]');
                    if (pepsicoBtn) pepsicoBtn.classList.add('active');
                }

                handleComparisonFilterChange();
            };

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

            const debouncedInnovationsCityUpdate = debounce(() => {
                const cityDataSource = getInnovationsMonthFilteredData({ excludeFilter: 'city' }).clients;
                innovationsMonthCitySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(innovationsMonthCityFilter, innovationsMonthCitySuggestions, cityDataSource);
            }, 300);

            innovationsMonthCityFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                debouncedInnovationsCityUpdate();
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
                        window.globalClientGoals = globalClientGoals;

                        if (targetsData && Object.keys(targetsData).length > 0) {
                            for (const key in targetsData) {
                                goalsTargets[key] = targetsData[key];
                            }
                        }

                        if (gd.seller_targets) {
                            goalsSellerTargets.clear();
                            for (const [seller, targets] of Object.entries(gd.seller_targets)) {
                                goalsSellerTargets.set(seller, targets);
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
                        // But we want to refresh data
                        updateGoalsSvView();
                    }
                }
            });

            // SV Sub-tabs Logic and Toggle Logic REMOVED (Replaced by Single Table View)

            // GV Filters
            const clearGoalsSummaryFiltersBtn = document.getElementById('clear-goals-summary-filters-btn');

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

            document.getElementById('goals-prev-page-btn').addEventListener('click', () => {
                if (goalsTableState.currentPage > 1) {
                    goalsTableState.currentPage--;
                    updateGoalsView();
                }
            });

            const goalsGvExportPdfBtn = document.getElementById('goals-gv-export-pdf-btn');
            if(goalsGvExportPdfBtn) {
                goalsGvExportPdfBtn.addEventListener('click', exportGoalsGvPDF);
            }

            const goalsGvExportXlsxBtn = document.getElementById('goals-gv-export-xlsx-btn');
            if(goalsGvExportXlsxBtn) {
                goalsGvExportXlsxBtn.addEventListener('click', exportGoalsCurrentTabXLSX);
            }
            document.getElementById('goals-next-page-btn').addEventListener('click', () => {
                if (goalsTableState.currentPage < goalsTableState.totalPages) {
                    goalsTableState.currentPage++;
                    updateGoalsView();
                }
            });

            // --- Meta Vs Realizado Listeners ---
            const updateMetaRealizado = () => {
                markDirty('metaRealizado');
                updateMetaRealizadoView();
            };

            const debouncedUpdateMetaRealizado = debounce(updateMetaRealizado, 400);

            // Supervisor Filter

            // Supplier Filter
            const metaRealizadoSupplierFilterBtn = document.getElementById('meta-realizado-supplier-filter-btn');
            const metaRealizadoSupplierFilterDropdown = document.getElementById('meta-realizado-supplier-filter-dropdown');
            metaRealizadoSupplierFilterBtn.addEventListener('click', () => metaRealizadoSupplierFilterDropdown.classList.toggle('hidden'));
            metaRealizadoSupplierFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedMetaRealizadoSuppliers.includes(value)) selectedMetaRealizadoSuppliers.push(value);
                    } else {
                        selectedMetaRealizadoSuppliers = selectedMetaRealizadoSuppliers.filter(s => s !== value);
                    }
                    selectedMetaRealizadoSuppliers = updateSupplierFilter(metaRealizadoSupplierFilterDropdown, document.getElementById('meta-realizado-supplier-filter-text'), selectedMetaRealizadoSuppliers, metaRealizadoSuppliersSource, 'metaRealizado', true);
                    debouncedUpdateMetaRealizado();
                }
            });

            // Pasta Filter
            const metaRealizadoPastaContainer = document.getElementById('meta-realizado-pasta-toggle-container');
            if (metaRealizadoPastaContainer) {
                metaRealizadoPastaContainer.addEventListener('click', (e) => {
                    if (e.target.tagName === 'BUTTON') {
                        const pasta = e.target.dataset.pasta;

                        // Toggle logic: If clicking active button, deselect (revert to PEPSICO/All).
                        // If clicking inactive, select it.
                        if (currentMetaRealizadoPasta === pasta) {
                            currentMetaRealizadoPasta = 'PEPSICO'; // Revert to default
                        } else {
                            currentMetaRealizadoPasta = pasta;
                        }

                        // Update UI
                        metaRealizadoPastaContainer.querySelectorAll('.pasta-btn').forEach(b => {
                            if (b.dataset.pasta === currentMetaRealizadoPasta) {
                                b.classList.remove('bg-slate-700');
                                b.classList.add('bg-teal-600', 'hover:bg-teal-500'); // Active State
                            } else {
                                b.classList.add('bg-slate-700');
                                b.classList.remove('bg-teal-600', 'hover:bg-teal-500');
                            }
                        });

                        debouncedUpdateMetaRealizado();
                    }
                });

                // Initialize default active button style
                metaRealizadoPastaContainer.querySelectorAll('.pasta-btn').forEach(b => {
                    if (b.dataset.pasta === currentMetaRealizadoPasta) {
                        b.classList.remove('bg-slate-700');
                        b.classList.add('bg-teal-600', 'hover:bg-teal-500');
                    }
                });
            }

            // Toggle Metric Logic
            const metaRealizadoMetricToggleBtn = document.getElementById('metaRealizadoMetricToggleBtn');
            if (metaRealizadoMetricToggleBtn) {
                metaRealizadoMetricToggleBtn.addEventListener('click', () => {
                    if (currentMetaRealizadoMetric === 'valor') {
                        currentMetaRealizadoMetric = 'peso';
                        metaRealizadoMetricToggleBtn.textContent = 'Toneladas';
                        metaRealizadoMetricToggleBtn.classList.remove('active', 'text-white');
                        metaRealizadoMetricToggleBtn.classList.add('text-slate-300'); // Inactive style? No, it's a toggle button.
                        // Better style: keep active but change text? Or standard toggle behavior?
                        // User image shows "R$ / Ton". It implies a switch.
                        // Let's toggle between states.
                    } else {
                        currentMetaRealizadoMetric = 'valor';
                        metaRealizadoMetricToggleBtn.textContent = 'R$ / Ton'; // Or 'Faturamento'? Image says "R$ / Ton" likely meaning the button label is static or toggles?
                        // "gostaria de ter um botão desse da imagem, deve ter a função de alternar entre R$ e Tonelada"
                        // The image shows "R$ / Ton". It might be a label for the button that cycles?
                        // Let's update text to indicate CURRENT state or NEXT state?
                        // Usually toggle buttons indicate current state.
                        // Let's use: "Faturamento (R$)" and "Volume (Ton)" as labels for clarity, or stick to user image.
                        // User image: "R$ / Ton".
                        // Let's assume the button text is static "R$ / Ton" and we just toggle state?
                        // No, usually buttons show what is selected.
                        // Let's change text to "Volume (Ton)" when Ton is selected, and "Faturamento (R$)" when R$ is selected?
                        // Or just keep "R$ / Ton" and toggle a visual indicator?
                        // Let's just update the chart and maybe change button style/text slightly.
                    }

                    // Simple Toggle Text Update
                    metaRealizadoMetricToggleBtn.textContent = currentMetaRealizadoMetric === 'valor' ? 'R$ / Ton' : 'Toneladas';

                    const metaRealizadoChartTitle = document.getElementById('metaRealizadoChartTitle');
                    if (metaRealizadoChartTitle) {
                        metaRealizadoChartTitle.textContent = currentMetaRealizadoMetric === 'valor' ? 'Meta Vs Realizado - Faturamento' : 'Meta Vs Realizado - Tonelada';
                    }

                    updateMetaRealizado();
                });
            }

            // Clear Filters
            document.getElementById('clear-meta-realizado-filters-btn').addEventListener('click', () => {
                selectedMetaRealizadoSuppliers = [];
                currentMetaRealizadoPasta = 'PEPSICO'; // Reset to default

                // Reset UI

                // Reset Supplier UI
                document.getElementById('meta-realizado-supplier-filter-text').textContent = 'Todos';
                metaRealizadoSupplierFilterDropdown.querySelectorAll('input').forEach(cb => cb.checked = false);

                // Reset Pasta UI (Deactivate all, since PEPSICO button is gone)
                metaRealizadoPastaContainer.querySelectorAll('.pasta-btn').forEach(b => {
                    b.classList.add('bg-slate-700');
                    b.classList.remove('bg-teal-600', 'hover:bg-teal-500');
                });

                debouncedUpdateMetaRealizado();
            });

            // Close Dropdowns on Click Outside
            document.addEventListener('click', (e) => {
                if (!metaRealizadoSupplierFilterBtn.contains(e.target) && !metaRealizadoSupplierFilterDropdown.contains(e.target)) metaRealizadoSupplierFilterDropdown.classList.add('hidden');
            });

            // Pagination Listeners for Meta Realizado Clients Table
            document.getElementById('meta-realizado-clients-prev-page-btn').addEventListener('click', () => {
                if (metaRealizadoClientsTableState.currentPage > 1) {
                    metaRealizadoClientsTableState.currentPage--;
                    updateMetaRealizadoView();
                }
            });

            document.getElementById('export-meta-realizado-pdf-btn').addEventListener('click', exportMetaRealizadoPDF);
            document.getElementById('export-meta-realizado-pdf-btn-bottom').addEventListener('click', exportMetaRealizadoPDF);
            document.getElementById('meta-realizado-clients-next-page-btn').addEventListener('click', () => {
                if (metaRealizadoClientsTableState.currentPage < metaRealizadoClientsTableState.totalPages) {
                    metaRealizadoClientsTableState.currentPage++;
                    updateMetaRealizadoView();
                }
            });


            const updateMix = () => {
                markDirty('mix');
                handleMixFilterChange();
            };

            const mixSupervisorBtn = document.getElementById('mix-supervisor-filter-btn');
            if (mixSupervisorBtn) {
                mixSupervisorBtn.addEventListener('click', () => {
                    const dropdown = document.getElementById('mix-supervisor-filter-dropdown');
                    if(dropdown) dropdown.classList.toggle('hidden');
                });
            }

            const mixTipoVendaBtn = document.getElementById('mix-tipo-venda-filter-btn');
            if (mixTipoVendaBtn) {
                mixTipoVendaBtn.addEventListener('click', (e) => {
                    const dd = document.getElementById('mix-tipo-venda-filter-dropdown');
                    if (dd) dd.classList.toggle('hidden');
                });
            }

            const mixTipoVendaDropdown = document.getElementById('mix-tipo-venda-filter-dropdown');
            if (mixTipoVendaDropdown) {
                mixTipoVendaDropdown.addEventListener('change', (e) => {
                    if (e.target.type === 'checkbox') {
                        const { value, checked } = e.target;
                        if (checked) selectedMixTiposVenda.push(value);
                        else selectedMixTiposVenda = selectedMixTiposVenda.filter(s => s !== value);
                        handleMixFilterChange({ skipFilter: 'tipoVenda' });
                        markDirty('mix');
                    }
                });
            }

            const mixFilialFilter = document.getElementById('mix-filial-filter');
            if (mixFilialFilter) mixFilialFilter.addEventListener('change', updateMix);

            const mixCityFilter = document.getElementById('mix-city-filter');
            const mixCitySuggestions = document.getElementById('mix-city-suggestions');

            if (mixCityFilter && mixCitySuggestions) {
                const debouncedMixCityUpdate = debounce(() => {
                    const { clients } = getMixFilteredData({ excludeFilter: 'city' });
                    mixCitySuggestions.classList.remove('manual-hide');
                    updateCitySuggestions(mixCityFilter, mixCitySuggestions, clients);
                }, 300);

                mixCityFilter.addEventListener('input', (e) => {
                    e.target.value = e.target.value.replace(/[0-9]/g, '');
                    debouncedMixCityUpdate();
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
            }

            const mixComRedeBtn = document.getElementById('mix-com-rede-btn');
            if (mixComRedeBtn) {
                mixComRedeBtn.addEventListener('click', () => {
                    const dd = document.getElementById('mix-rede-filter-dropdown');
                    if (dd) dd.classList.toggle('hidden');
                });
            }

            const mixRedeGroupContainer = document.getElementById('mix-rede-group-container');
            if (mixRedeGroupContainer) {
                mixRedeGroupContainer.addEventListener('click', (e) => {
                    if(e.target.closest('button')) {
                        const button = e.target.closest('button');
                        mixRedeGroupFilter = button.dataset.group;
                        document.getElementById('mix-rede-group-container').querySelectorAll('button').forEach(b => b.classList.remove('active'));
                        button.classList.add('active');
                        if (mixRedeGroupFilter !== 'com_rede') {
                            const dd = document.getElementById('mix-rede-filter-dropdown');
                            if (dd) dd.classList.add('hidden');
                            selectedMixRedes = [];
                        }
                        handleMixFilterChange();
                    }
                });
            }

            const mixRedeFilterDropdown = document.getElementById('mix-rede-filter-dropdown');
            if (mixRedeFilterDropdown) {
                mixRedeFilterDropdown.addEventListener('change', (e) => {
                    if (e.target.type === 'checkbox') {
                        const { value, checked } = e.target;
                        if (checked) selectedMixRedes.push(value);
                        else selectedMixRedes = selectedMixRedes.filter(r => r !== value);

                        mixRedeGroupFilter = 'com_rede';
                        const container = document.getElementById('mix-rede-group-container');
                        if (container) container.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                        const btn = document.getElementById('mix-com-rede-btn');
                        if (btn) btn.classList.add('active');

                        handleMixFilterChange({ skipFilter: 'rede' });
                    }
                });
            }

            const clearMixFiltersBtn = document.getElementById('clear-mix-filters-btn');
            if (clearMixFiltersBtn) clearMixFiltersBtn.addEventListener('click', () => { resetMixFilters(); markDirty('mix'); });

            const exportMixPdfBtn = document.getElementById('export-mix-pdf-btn');
            if (exportMixPdfBtn) exportMixPdfBtn.addEventListener('click', exportMixPDF);

            const mixKpiToggle = document.getElementById('mix-kpi-toggle');
            if (mixKpiToggle) {
                mixKpiToggle.addEventListener('change', (e) => {
                    mixKpiMode = e.target.checked ? 'atendidos' : 'total';
                    markDirty('mix');
                    updateMixView();
                });
            }

            const mixPrevPageBtn = document.getElementById('mix-prev-page-btn');
            if (mixPrevPageBtn) {
                mixPrevPageBtn.addEventListener('click', () => {
                    if (mixTableState.currentPage > 1) {
                        mixTableState.currentPage--;
                        updateMixView();
                    }
                });
            }

            const mixNextPageBtn = document.getElementById('mix-next-page-btn');
            if (mixNextPageBtn) {
                mixNextPageBtn.addEventListener('click', () => {
                    if (mixTableState.currentPage < mixTableState.totalPages) {
                        mixTableState.currentPage++;
                        updateMixView();
                    }
                });
            }

            document.addEventListener('click', (e) => {
                // Close Mix Dropdowns
                const safeClose = (btnId, ddId) => {
                    const btn = document.getElementById(btnId);
                    const dd = document.getElementById(ddId);
                    if (btn && dd && !btn.contains(e.target) && !dd.contains(e.target)) {
                        dd.classList.add('hidden');
                    }
                };

                safeClose('mix-supervisor-filter-btn', 'mix-supervisor-filter-dropdown');
                safeClose('mix-vendedor-filter-btn', 'mix-vendedor-filter-dropdown');
                safeClose('mix-tipo-venda-filter-btn', 'mix-tipo-venda-filter-dropdown');
                safeClose('mix-com-rede-btn', 'mix-rede-filter-dropdown');
            });

            // --- Coverage View Filters ---
            const updateCoverage = () => {
                markDirty('cobertura');
                handleCoverageFilterChange();
            };

            const debouncedHandleCoverageChange = debounce(updateCoverage, 400);

            coverageFilialFilter.addEventListener('change', updateCoverage);

            const debouncedCoverageCityUpdate = debounce(() => {
                const { clients } = getCoverageFilteredData({ excludeFilter: 'city' });
                coverageCitySuggestions.classList.remove('manual-hide');
                updateCitySuggestions(coverageCityFilter, coverageCitySuggestions, clients);
            }, 300);

            if (coverageCityFilter) coverageCityFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[0-9]/g, '');
                debouncedCoverageCityUpdate();
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
                if (!innovationsMonthTipoVendaFilterBtn.contains(e.target) && !innovationsMonthTipoVendaFilterDropdown.contains(e.target)) innovationsMonthTipoVendaFilterDropdown.classList.add('hidden');
                if (!coverageSupplierFilterBtn.contains(e.target) && !coverageSupplierFilterDropdown.contains(e.target)) coverageSupplierFilterDropdown.classList.add('hidden');
                if (!coverageProductFilterBtn.contains(e.target) && !coverageProductFilterDropdown.contains(e.target)) coverageProductFilterDropdown.classList.add('hidden');
                if (!coverageTipoVendaFilterBtn.contains(e.target) && !coverageTipoVendaFilterDropdown.contains(e.target)) coverageTipoVendaFilterDropdown.classList.add('hidden');
            });

        }

        initializeOptimizedDataStructures();

        // --- USER CONTEXT RESOLUTION ---
        let userHierarchyContext = { role: 'adm', coord: null, cocoord: null, promotor: null };

        function applyHierarchyVisibilityRules() {
            const role = (userHierarchyContext.role || '').toLowerCase();
            // Views to apply logic to (excluding 'goals' and 'wallet' as requested)
            const views = ['main', 'city', 'comparison', 'innovations-month', 'mix', 'coverage'];

            views.forEach(prefix => {
                const coordWrapper = document.getElementById(`${prefix}-coord-filter-wrapper`);
                const cocoordWrapper = document.getElementById(`${prefix}-cocoord-filter-wrapper`);
                const promotorWrapper = document.getElementById(`${prefix}-promotor-filter-wrapper`);

                // Reset visibility first (Show All)
                if (coordWrapper) coordWrapper.classList.remove('hidden');
                if (cocoordWrapper) cocoordWrapper.classList.remove('hidden');
                if (promotorWrapper) promotorWrapper.classList.remove('hidden');

                if (role === 'adm') {
                    // Show all
                } else if (role === 'coord') {
                    // Hide Coord filter
                    if (coordWrapper) coordWrapper.classList.add('hidden');
                } else if (role === 'cocoord') {
                    // Hide Coord and CoCoord
                    if (coordWrapper) coordWrapper.classList.add('hidden');
                    if (cocoordWrapper) cocoordWrapper.classList.add('hidden');
                } else if (role === 'promotor') {
                    // Hide All
                    if (coordWrapper) coordWrapper.classList.add('hidden');
                    if (cocoordWrapper) cocoordWrapper.classList.add('hidden');
                    if (promotorWrapper) promotorWrapper.classList.add('hidden');
                }
            });
        }

        function updateNavigationVisibility() {
            const role = (userHierarchyContext.role || '').toLowerCase();
            const isAuth = role === 'adm' || role === 'coord';
            
            // Toggle Comparison Buttons (Desktop and Mobile)
            const comparisonBtns = document.querySelectorAll('button[data-target="comparativo"]');
            comparisonBtns.forEach(btn => {
                if (isAuth) {
                    btn.classList.remove('hidden');
                    // If desktop nav, ensure we don't break layout (flex)
                    if (btn.classList.contains('nav-link')) btn.style.display = ''; 
                } else {
                    btn.classList.add('hidden');
                }
            });
        }

        function resolveUserContext() {
            const role = (window.userRole || '').trim().toUpperCase();
            console.log(`[DEBUG] Resolving User Context for Role: '${role}'`);

            if (role === 'ADM' || role === 'ADMIN') {
                userHierarchyContext.role = 'adm';
                console.log(`[DEBUG] Role identified as ADM`);
                return;
            }

            // Check if Role is a Coordinator
            if (optimizedData.coordMap.has(role)) {
                userHierarchyContext.role = 'coord';
                userHierarchyContext.coord = role;
                console.log(`[DEBUG] Role identified as COORD: ${role}`);
                return;
            }

            // Check if Role is a Co-Coordinator
            if (optimizedData.cocoordMap.has(role)) {
                userHierarchyContext.role = 'cocoord';
                userHierarchyContext.cocoord = role;
                userHierarchyContext.coord = optimizedData.coordsByCocoord.get(role);
                console.log(`[DEBUG] Role identified as COCOORD: ${role}`);
                return;
            }

            // Check if Role is a Promotor
            if (optimizedData.promotorMap.has(role)) {
                userHierarchyContext.role = 'promotor';
                userHierarchyContext.promotor = role;
                const node = optimizedData.hierarchyMap.get(role);
                if (node) {
                    userHierarchyContext.cocoord = node.cocoord.code;
                    userHierarchyContext.coord = node.coord.code;
                }
                console.log(`[DEBUG] Role identified as PROMOTOR: ${role}`);
                return;
            }
            
            // Fallback: Default to ADM (UI allows all, but Data is filtered by init.js)
            userHierarchyContext.role = 'adm';
            console.warn(`[DEBUG] Role '${role}' not found in Hierarchy Maps. Defaulting to ADM context (Data filtered by Init).`);
            console.log("Available Coords:", Array.from(optimizedData.coordMap.keys()));
        }
        resolveUserContext();
        updateNavigationVisibility(); // Update menu visibility based on resolved context
        applyHierarchyVisibilityRules();

        calculateHistoricalBests(); // <-- MOVIDA PARA CIMA
        // Initialize Hierarchy Filters
        setupHierarchyFilters('main', updateDashboard);
        setupHierarchyFilters('city', updateCityView);
        setupHierarchyFilters('comparison', updateComparisonView);
        setupHierarchyFilters('innovations-month', updateInnovationsMonthView);
        setupHierarchyFilters('mix', updateMixView);
        setupHierarchyFilters('meta-realizado', updateMetaRealizadoView);
        setupHierarchyFilters('coverage', updateCoverageView);
        setupHierarchyFilters('goals-gv', updateGoalsView);
        setupHierarchyFilters('goals-summary', updateGoalsSummaryView);
        setupHierarchyFilters('goals-sv', updateGoalsSvView);

        // Initialize Other Filters
        selectedMainSuppliers = updateSupplierFilter(document.getElementById('fornecedor-filter-dropdown'), document.getElementById('fornecedor-filter-text'), selectedMainSuppliers, [...allSalesData, ...allHistoryData], 'main');
        updateTipoVendaFilter(tipoVendaFilterDropdown, tipoVendaFilterText, selectedTiposVenda, allSalesData);

        updateRedeFilter(mainRedeFilterDropdown, mainComRedeBtnText, selectedMainRedes, allClientsData);
        updateRedeFilter(cityRedeFilterDropdown, cityComRedeBtnText, selectedCityRedes, allClientsData);
        updateRedeFilter(comparisonRedeFilterDropdown, comparisonComRedeBtnText, selectedComparisonRedes, allClientsData);

        // Fix: Pre-filter Suppliers for Meta Realizado (Only PEPSICO)
        const metaRealizadoSuppliersSource = [...allSalesData, ...allHistoryData].filter(s => {
            const rowPasta = resolveSupplierPasta(s.OBSERVACAOFOR, s.FORNECEDOR);
            return rowPasta === SUPPLIER_CONFIG.metaRealizado.requiredPasta;
        });
        selectedMetaRealizadoSuppliers = updateSupplierFilter(document.getElementById('meta-realizado-supplier-filter-dropdown'), document.getElementById('meta-realizado-supplier-filter-text'), selectedMetaRealizadoSuppliers, metaRealizadoSuppliersSource, 'metaRealizado');

        updateAllComparisonFilters();


        initializeRedeFilters();
        setupEventListeners();
        initFloatingFilters();

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

        window.addEventListener('hashchange', () => {
            const view = window.location.hash.substring(1) || 'dashboard';
            renderView(view);
        });

        const urlParams = new URLSearchParams(window.location.search);
        const targetParam = urlParams.get('ir_para');
        const hash = window.location.hash.substring(1);
        const targetPage = hash || targetParam || 'dashboard';

        if (window.location.hash.substring(1) === targetPage) {
            renderView(targetPage);
        } else {
            window.location.hash = targetPage;
        }
        renderTable(aggregatedOrders);

        // Helper to redistribute weekly goals
        function calculateAdjustedWeeklyGoals(totalGoal, realizedByWeek, weeks) {
            let adjustedGoals = new Array(weeks.length).fill(0);
            let remainingWorkingDays = 0;
            let pastDifference = 0;
            let totalWorkingDays = weeks.reduce((sum, w) => sum + w.workingDays, 0);
            if (totalWorkingDays === 0) totalWorkingDays = 1;

            const currentDate = lastSaleDate; // Global context

            // 1. First Pass: Identify Past Weeks and Calculate Initial Diff
            weeks.forEach((week, i) => {
                // Determine if week is fully past
                // A week is "past" if its END date is strictly BEFORE the currentDate (ignoring time)
                // Logic: "Check if first week passed... then redistribute difference".
                // If we are IN week 2, week 1 is past.
                // Assuming lastSaleDate represents "today".

                const isPast = week.end < currentDate;
                const dailyGoal = totalGoal / totalWorkingDays;
                let originalWeekGoal = dailyGoal * week.workingDays;

                if (isPast) {
                    // Week is closed.
                    // User Requirement: "case in the first week the goal that was 40k wasn't hit (realized 30k), the 10k missing must be reassigned"
                    //
                    // Implementation:
                    // 1. Past Weeks: Display Original Goal (to show variance/failure).
                    // 2. Future Weeks: Display Adjusted Goal (Original + Share of Deficit).
                    //
                    // Mathematical Note:
                    // Because we display Original Goal for past weeks (instead of Realized), the sum of displayed goals
                    // will NOT equal the Total Monthly Goal if there is any deficit/surplus.
                    // Sum(Displayed) = Total Goal + (Original Past - Realized Past).
                    //
                    // However, the Dynamic Planning Invariant holds:
                    // Realized Past + Future Adjusted Goals = Total Monthly Goal.
                    // This ensures the seller knows exactly what is needed in future weeks to hit the contract target.

                    adjustedGoals[i] = originalWeekGoal;
                    const realized = realizedByWeek[i] || 0;
                    pastDifference += (originalWeekGoal - realized); // Positive if deficit, Negative if surplus
                } else {
                    remainingWorkingDays += week.workingDays;
                }
            });

            // 2. Second Pass: Distribute Difference to Future Weeks
            if (remainingWorkingDays > 0) {
                weeks.forEach((week, i) => {
                    const isPast = week.end < currentDate;
                    if (!isPast) {
                        const dailyGoal = totalGoal / totalWorkingDays;
                        const originalWeekGoal = dailyGoal * week.workingDays;

                        // Distribute pastDifference proportionally to this week's weight in remaining time
                        const share = pastDifference * (week.workingDays / remainingWorkingDays);

                        // New Goal = Original + Share
                        // If deficit (pos), goal increases. If surplus (neg), goal decreases.
                        let newGoal = originalWeekGoal + share;

                        // Prevent negative goals? (Extreme surplus)
                        if (newGoal < 0) newGoal = 0;

                        adjustedGoals[i] = newGoal;
                    }
                });
            } else {
                // If no remaining days (month over), the deficit just sits there (or we add to last week?)
                // Usually just leave as is.
                weeks.forEach((week, i) => {
                    const isPast = week.end < currentDate;
                    if (!isPast) {
                         // Should not happen if logic is correct, unless current date is before start of month?
                         // If we are strictly before month starts, remaining = total. Loop above handles it (pastDifference=0).
                         const dailyGoal = totalGoal / totalWorkingDays;
                         adjustedGoals[i] = dailyGoal * week.workingDays;
                    }
                });
            }

            return adjustedGoals;
        }

        // --- IMPORT PARSER AND LOGIC ---

        function calculateSellerDefaults(sellerName) {
            const defaults = {
                elmaPos: 0,
                foodsPos: 0,
                mixSalty: 0,
                mixFoods: 0
            };

            const sellerCode = optimizedData.rcaCodeByName.get(sellerName);
            if (!sellerCode) return defaults;

            const clients = optimizedData.clientsByRca.get(sellerCode) || [];
            const activeClients = clients.filter(c => {
                const cod = String(c["Código"] || c["codigo_cliente"]);
                const rca1 = String(c.rca1 || "").trim();
                const isAmericanas = (c.razaoSocial || "").toUpperCase().includes("AMERICANAS");
                return (isAmericanas || rca1 !== "53" || clientsWithSalesThisMonth.has(cod));
            });

            activeClients.forEach(client => {
                const codCli = String(client["Código"] || client["codigo_cliente"]);
                const historyIds = optimizedData.indices.history.byClient.get(codCli);

                if (historyIds) {
                    let clientElmaFat = 0;
                    let clientFoodsFat = 0;

                    historyIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        if (String(codCli).trim() === "9569" && (String(sale.CODUSUR).trim() === "53" || String(sale.CODUSUR).trim() === "053")) return;

                        const isRev = (sale.TIPOVENDA === "1" || sale.TIPOVENDA === "9");
                        if (!isRev) return;

                        const codFor = String(sale.CODFOR);
                        const desc = (sale.DESCRICAO || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase();

                        if (codFor === "707" || codFor === "708" || codFor === "752") {
                            clientElmaFat += sale.VLVENDA;
                        } else if (codFor === "1119") {
                            if (desc.includes("TODDYNHO") || desc.includes("TODDY") || desc.includes("QUAKER") || desc.includes("KEROCOCO")) {
                                clientFoodsFat += sale.VLVENDA;
                            }
                        }
                    });

                    if (clientElmaFat >= 1) defaults.elmaPos++;
                    if (clientFoodsFat >= 1) defaults.foodsPos++;
                }
            });

            const elmaAdj = goalsPosAdjustments["ELMA_ALL"] ? (goalsPosAdjustments["ELMA_ALL"].get(sellerName) || 0) : 0;
            const elmaBase = defaults.elmaPos + elmaAdj;

            defaults.mixSalty = Math.round(elmaBase * 0.50);
            defaults.mixFoods = Math.round(elmaBase * 0.30);

            if (sellerCode === "1001") {
                defaults.mixSalty = 0;
                defaults.mixFoods = 0;
            }

            return defaults;
        }

        function parseGoalsSvStructure(text) {
            console.log("[Parser] Iniciando parse...");
            const lines = text.replace(/[\r\n]+$/, '').split(/\r?\n/);
            if (lines.length === 0) return null;

            // 1. Detect Delimiter (Heuristic)
            const firstLine = lines[0];
            let delimiter = '\t';
            if (firstLine.includes('\t')) delimiter = '\t';
            else if (firstLine.includes(';')) delimiter = ';';
            else if (firstLine.includes(',') && lines.length > 1) delimiter = ',';
            // Fallback for space separated copy-paste if single line has spaces
            else if (firstLine.trim().split(/\s{2,}/).length > 1) delimiter = /\s{2,}/; // At least 2 spaces

            console.log("[Parser] Delimitador detectado:", delimiter);

            const rows = lines.map(line => {
                // If delimiter is regex, use split directly
                if (delimiter instanceof RegExp) return line.trim().split(delimiter);
                return line.split(delimiter);
            });

            console.log(`[Parser] Linhas encontradas: ${rows.length}`);

            // Helper: Parse Value (Moved up for availability)
            const parseImportValue = (rawStr) => {
                if (!rawStr) return NaN;
                let clean = String(rawStr).trim().toUpperCase().replace(/[^0-9,.-]/g, '');
                if (!clean) return NaN;

                const dotIdx = clean.lastIndexOf('.');
                const commaIdx = clean.lastIndexOf(',');

                if (dotIdx > -1 && commaIdx > -1) {
                    if (dotIdx > commaIdx) clean = clean.replace(/,/g, '');
                    else clean = clean.replace(/\./g, '').replace(',', '.');
                } else if (commaIdx > -1) {
                    if (/,\d{3}$/.test(clean)) clean = clean.replace(/,/g, '');
                    else clean = clean.replace(',', '.');
                } else if (dotIdx > -1) {
                    if (/\.\d{3}$/.test(clean)) clean = clean.replace(/\./g, '');
                }
                return parseFloat(clean);
            };

            // Helper: Normalize Category
            const normalizeGoalCategory = (catKey) => {
                if (!catKey) return null;
                catKey = catKey.toUpperCase();
                if (catKey.includes('NÃO EXTRUSADOS') || catKey.includes('NAO EXTRUSADOS')) return '708';
                if (catKey.includes('EXTRUSADOS')) return '707';
                if (catKey.includes('TORCIDA')) return '752';
                if (catKey.includes('TODDYNHO')) return '1119_TODDYNHO';
                if (catKey.includes('TODDY')) return '1119_TODDY';
                if (catKey.includes('QUAKER') || catKey.includes('KEROCOCO')) return '1119_QUAKER_KEROCOCO';
                if (catKey === 'KG ELMA' || catKey === 'KG_ELMA') return 'tonelada_elma';
                if (catKey === 'KG FOODS' || catKey === 'KG_FOODS') return 'tonelada_foods';
                if (catKey === 'TOTAL ELMA' || catKey === 'TOTAL_ELMA') return 'total_elma';
                if (catKey === 'TOTAL FOODS' || catKey === 'TOTAL_FOODS') return 'total_foods';
                if (catKey === 'MIX SALTY' || catKey === 'MIX_SALTY') return 'mix_salty';
                if (catKey === 'MIX FOODS' || catKey === 'MIX_FOODS') return 'mix_foods';
                if (catKey === 'PEPSICO_ALL_POS' || catKey === 'PEPSICO_ALL' || catKey === 'GERAL') return 'pepsico_all';

                const validIds = ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO', 'tonelada_elma', 'tonelada_foods', 'total_elma', 'total_foods', 'mix_salty', 'mix_foods', 'pepsico_all'];
                if (validIds.includes(catKey.toLowerCase())) return catKey.toLowerCase();
                return null;
            };

            // Helper: Normalize Metric
            const normalizeGoalMetric = (metricKey) => {
                if (!metricKey) return null;
                metricKey = metricKey.toUpperCase();
                if (metricKey === 'FATURAMENTO' || metricKey === 'MÉDIA TRIM.' || metricKey === 'FAT' || metricKey === 'R$' || metricKey === 'VALOR') return 'FAT';
                if (metricKey === 'POSITIVAÇÃO' || metricKey === 'POSITIVACAO' || metricKey.includes('POSITIVA') || metricKey === 'POS') return 'POS';
                if (metricKey === 'TONELADA' || metricKey === 'META KG' || metricKey === 'VOL' || metricKey === 'KG' || metricKey === 'VOLUME') return 'VOL';
                if (metricKey === 'META MIX' || metricKey === 'MIX' || metricKey === 'QTD') return 'MIX';
                return null;
            };

            // Helper: Resolve Seller
            const resolveSeller = (rawName) => {
                if (!rawName) return null;
                if (isGarbageSeller(rawName)) return null;
                const upperName = rawName.normalize('NFD').replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
                if (optimizedData.rcasBySupervisor.has(upperName) || optimizedData.rcasBySupervisor.has(rawName)) return null;

                if (!isNaN(parseImportValue(rawName))) {
                     const codeStr = String(parseImportValue(rawName));
                     if (optimizedData.rcaNameByCode.has(codeStr)) return optimizedData.rcaNameByCode.get(codeStr);
                }

                for (const [sysName, sysCode] of optimizedData.rcaCodeByName) {
                     const sysUpper = sysName.normalize('NFD').replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
                     if (sysUpper === upperName) return sysName;
                }
                return rawName;
            };

            // 2. Identify Header Rows
            // We look for 3 consecutive rows that might be the header structure
            let startRow = 0;
            if (rows.length >= 3) {
                // Standard logic: Rows 0, 1, 2
                startRow = 0;
            } else {
                console.warn("[Parser] Menos de 3 linhas. Tentando modo simplificado...");
                const simplifiedUpdates = [];

                rows.forEach((row, rowIndex) => {
                    const cols = row.map(c => c ? c.trim() : '').filter(c => c !== '');
                    if (cols.length < 3) {
                         console.warn(`[Parser-Simples] Linha ${rowIndex+1} ignorada: Menos de 3 colunas válidas.`);
                         return;
                    }

                    const sellerName = resolveSeller(cols[0]);
                    if (!sellerName) return;

                    let catId = null;
                    let metricId = null;
                    let value = NaN;

                    // Try 4 Columns: Seller | Category | Metric | Value
                    if (cols.length >= 4) {
                        catId = normalizeGoalCategory(cols[1]);
                        metricId = normalizeGoalMetric(cols[2]);
                        value = parseImportValue(cols[3]);
                    }
                    // Try 3 Columns: Seller | Category | Value (Infer Metric)
                    else if (cols.length === 3) {
                        catId = normalizeGoalCategory(cols[1]);
                        value = parseImportValue(cols[2]);

                        if (catId) {
                            if (catId.startsWith('mix_')) metricId = 'MIX';
                            else if (catId.startsWith('tonelada_')) metricId = 'VOL';
                            else if (catId.startsWith('total_') || catId === 'pepsico_all') metricId = 'POS';
                            // Ambiguous: 707, 708... could be FAT or POS.
                            // If Value is small (< 200), maybe POS? If large, FAT? Dangerous.
                            // Default to FAT for 707/etc?
                            else if (['707','708','752','1119_TODDYNHO','1119_TODDY','1119_QUAKER_KEROCOCO'].includes(catId)) {
                                metricId = 'FAT'; // Default assumption for simplified input
                            }
                        }
                    }

                    if (sellerName && catId && metricId && !isNaN(value)) {
                        let type = 'rev';
                        if (metricId === 'VOL') type = 'vol';
                        if (metricId === 'POS') type = 'pos';
                        if (metricId === 'MIX') type = 'mix';

                        simplifiedUpdates.push({ type, seller: sellerName, category: catId, val: value });
                    }
                });

                return simplifiedUpdates.length > 0 ? simplifiedUpdates : null;
            }

            const header0 = rows[startRow].map(h => h ? h.trim().toUpperCase() : '');
            const header1 = rows[startRow + 1].map(h => h ? h.trim().toUpperCase() : '');
            const header2 = rows[startRow + 2].map(h => h ? h.trim().toUpperCase() : '');

            console.log("[Parser] Header 0:", header0.join('|'));
            console.log("[Parser] Header 1:", header1.join('|'));
            console.log("[Parser] Header 2:", header2.join('|'));

            const colMap = {};
            let currentCategory = null;
            let currentMetric = null;

            // Map Headers
            for (let i = 0; i < header0.length; i++) {
                if (header0[i]) currentCategory = header0[i];
                if (header1[i]) currentMetric = header1[i];
                let subMetric = header2[i]; // Meta, Ajuste, etc.

                if (currentCategory && subMetric) {
                    if (subMetric === 'AJ.' || subMetric === 'AJ') subMetric = 'AJUSTE';

                    let catKey = currentCategory;
                    // Normalize Category Names to IDs (Reuse helper if possible or keep logic)
                    const normalizedCat = normalizeGoalCategory(catKey);
                    if (normalizedCat) catKey = normalizedCat;

                    let metricKey = 'OTHER';
                    const normalizedMetric = normalizeGoalMetric(currentMetric);
                    if (normalizedMetric) metricKey = normalizedMetric;

                    const key = `${catKey}_${metricKey}_${subMetric}`;
                    colMap[key] = i;
                }
            }

            const updates = [];
            const processedSellers = new Set();

            const dataStartRow = startRow + 3;
            // Identify Vendor Column Index (Name)
            // Usually Index 1 (Code, Name, ...)
            // We scan first few rows to find valid seller names
            let nameColIndex = 1;
            // Basic Heuristic: If col 0 looks like a name and col 1 is number, maybe it's col 0.
            // But standard template is [Code, Name, ...]. We stick to 1 for now or 0 if 1 is empty.

            for (let i = dataStartRow; i < rows.length; i++) {
                const row = rows[i];
                if (!row || row.length < 2) continue;

                // Try col 1 for name, fallback to col 0 if col 1 is empty/numeric
                let sellerName = row[1];
                let sellerCodeCandidate = row[0]; // Candidate for Code

                if (!sellerName || !isNaN(parseImportValue(sellerName))) {
                     // If col 1 is number, maybe col 0 is name? Or col 2?
                     // Standard: Col 0 = Code, Col 1 = Name.
                     if (row[0] && isNaN(parseImportValue(row[0]))) {
                         sellerName = row[0];
                         sellerCodeCandidate = null; // Name is in Col 0
                     }
                }

                if (!sellerName) continue;

                // --- ENHANCED FILTER: Ignore Supervisors, Aggregates, and BALCAO ---
                if (isGarbageSeller(sellerName)) continue;
                const upperName = sellerName.normalize('NFD').replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();

                // --- RESOLUTION LOGIC: Normalize Seller Name to System Canonical Name ---
                let canonicalName = null;

                // 1. Try by Code (Col 0)
                if (sellerCodeCandidate) {
                    const parsedCode = parseImportValue(sellerCodeCandidate);
                    if (!isNaN(parsedCode)) {
                        const codeStr = String(parsedCode);
                        if (optimizedData.rcaNameByCode.has(codeStr)) {
                            canonicalName = optimizedData.rcaNameByCode.get(codeStr);
                        }
                    }
                }

                // 2. Try by Name (Fuzzy/Case-Insensitive)
                if (!canonicalName) {
                    // Iterate existing system names to find case-insensitive match
                    for (const [sysName, sysCode] of optimizedData.rcaCodeByName) {
                         const sysUpper = sysName.normalize('NFD').replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
                         if (sysUpper === upperName) {
                             canonicalName = sysName;
                             break;
                         }
                    }
                }

                const finalSellerName = canonicalName || sellerName;

                // 2. Dynamic Supervisor Check
                // If the name is a known Supervisor (key in rcasBySupervisor), ignore it.
                // Assuming supervisors are not also sellers in this context (or we only want leaf sellers).
                if (optimizedData.rcasBySupervisor.has(finalSellerName) || optimizedData.rcasBySupervisor.has(finalSellerName.toUpperCase())) {
                    continue;
                }
                // ------------------------------------------------

                if (processedSellers.has(finalSellerName)) continue;
                processedSellers.add(finalSellerName);

                // Helper to get value with priority: Adjust > Meta
                const getPriorityValue = (cat, metric) => {
                    // 1. Try AJUSTE
                    let idx = colMap[`${cat}_${metric}_AJUSTE`];
                    if (idx !== undefined && row[idx]) {
                        const val = parseImportValue(row[idx]);
                        if (!isNaN(val)) return val;
                    }
                    // 2. Try META
                    idx = colMap[`${cat}_${metric}_META`];
                    if (idx !== undefined && row[idx]) {
                        const val = parseImportValue(row[idx]);
                        if (!isNaN(val)) return val;
                    }
                    return NaN;
                };

                // 1. Revenue
                const revCats = ['707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                revCats.forEach(cat => {
                    const val = getPriorityValue(cat, 'FAT');
                    if (!isNaN(val)) updates.push({ type: 'rev', seller: sellerName, category: cat, val: val });
                });

                // 2. Volume
                // Metas de Volume são importadas pelos Totais (KG ELMA / KG FOODS) e distribuídas automaticamente
                const volCats = ['tonelada_elma', 'tonelada_foods'];
                volCats.forEach(cat => {
                    const val = getPriorityValue(cat, 'VOL');
                    if (!isNaN(val)) updates.push({ type: 'vol', seller: sellerName, category: cat, val: val });
                });

                // 3. Positivation
                const posCats = ['pepsico_all', 'total_elma', 'total_foods', '707', '708', '752', '1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                posCats.forEach(cat => {
                    const val = getPriorityValue(cat, 'POS');
                    if (!isNaN(val)) updates.push({ type: 'pos', seller: sellerName, category: cat, val: Math.round(val) });
                });

                // 4. Mix
                const mixCats = ['mix_salty', 'mix_foods'];
                mixCats.forEach(cat => {
                    const val = getPriorityValue(cat, 'MIX');
                    if (!isNaN(val)) updates.push({ type: 'mix', seller: sellerName, category: cat, val: Math.round(val) });
                });
            }
            return updates;
        }

        // --- Event Listeners for Import ---
        const importBtn = document.getElementById('goals-sv-import-btn');
        const importModal = document.getElementById('import-goals-modal');
        const importCloseBtn = document.getElementById('import-goals-close-btn');
        const importCancelBtn = document.getElementById('import-goals-cancel-btn');
        const importAnalyzeBtn = document.getElementById('import-goals-analyze-btn');
        const importConfirmBtn = document.getElementById('import-goals-confirm-btn');
        const importTextarea = document.getElementById('import-goals-textarea');
        const analysisContainer = document.getElementById('import-analysis-container');
        const analysisBody = document.getElementById('import-analysis-table-body');
        const analysisBadges = document.getElementById('import-summary-badges');
            const importPaginationControls = document.createElement('div');
            importPaginationControls.id = 'import-pagination-controls';
            importPaginationControls.className = 'flex justify-between items-center mt-4 hidden';
            importPaginationControls.innerHTML = `
                <button id="import-prev-page-btn" class="bg-slate-700 border border-slate-600 hover:bg-slate-600 text-slate-300 font-bold py-2 px-4 rounded-lg disabled:opacity-50 text-xs" disabled>Anterior</button>
                <span id="import-page-info-text" class="text-slate-400 text-xs">Página 1 de 1</span>
                <button id="import-next-page-btn" class="bg-slate-700 border border-slate-600 hover:bg-slate-600 text-slate-300 font-bold py-2 px-4 rounded-lg disabled:opacity-50 text-xs" disabled>Próxima</button>
            `;
            // Insert after table container (which is inside analysisContainer -> div.bg-slate-900)
            // analysisContainer contains a header div, result div, and then the table container div.
            // We need to find the table container.

        let pendingImportUpdates = [];
            let importTablePage = 1;
            const importTablePageSize = 19;

            function renderImportTable() {
                if (!analysisBody) return;
                analysisBody.innerHTML = '';

                const totalPages = Math.ceil(pendingImportUpdates.length / importTablePageSize);
                if (importTablePage > totalPages && totalPages > 0) importTablePage = totalPages;
                if (totalPages === 0) importTablePage = 1;

                const start = (importTablePage - 1) * importTablePageSize;
                const end = start + importTablePageSize;
                const pageItems = pendingImportUpdates.slice(start, end);

                const formatGoalValue = (val, type) => {
                    if (type === 'rev') return val.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
                    if (type === 'vol') return val.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 }) + ' Kg';
                    return Math.round(val).toString();
                };

                pageItems.forEach(u => {
                    const row = document.createElement('tr');

                    const currentVal = getSellerCurrentGoal(u.seller, u.category, u.type);
                    const newVal = u.val;
                    const diff = newVal - currentVal;

                    const currentValStr = formatGoalValue(currentVal, u.type);
                    const newValStr = formatGoalValue(newVal, u.type);
                    const diffStr = formatGoalValue(diff, u.type);

                    let diffClass = "text-slate-500";
                    if (diff > 0.001) diffClass = "text-green-400 font-bold";
                    else if (diff < -0.001) diffClass = "text-red-400 font-bold";

                    const sellerCode = optimizedData.rcaCodeByName.get(u.seller) || '-';

                    let displayCategory = u.category;
                    if (u.type === 'pos') displayCategory += '_POS';

                    row.innerHTML = `
                        <td class="px-4 py-2 text-xs text-slate-300">${sellerCode}</td>
                        <td class="px-4 py-2 text-xs text-slate-400">${u.seller}</td>
                        <td class="px-4 py-2 text-xs text-blue-300">${displayCategory}</td>
                        <td class="px-4 py-2 text-xs text-slate-400 font-mono text-right">${currentValStr}</td>
                        <td class="px-4 py-2 text-xs text-white font-bold font-mono text-right">${newValStr}</td>
                        <td class="px-4 py-2 text-xs ${diffClass} font-mono text-right">${diff > 0 ? '+' : ''}${diffStr}</td>
                        <td class="px-4 py-2 text-center text-xs"><span class="px-2 py-1 rounded-full bg-blue-900/50 text-blue-200 text-[10px]">Importar</span></td>
                    `;
                    analysisBody.appendChild(row);
                });

                // Update Pagination Controls
                const prevBtn = document.getElementById('import-prev-page-btn');
                const nextBtn = document.getElementById('import-next-page-btn');
                const infoText = document.getElementById('import-page-info-text');
                const paginationContainer = document.getElementById('import-pagination-controls');

                if (paginationContainer) {
                    if (pendingImportUpdates.length > importTablePageSize) {
                        paginationContainer.classList.remove('hidden');
                        if(infoText) infoText.textContent = `Página ${importTablePage} de ${totalPages}`;
                        if(prevBtn) prevBtn.disabled = importTablePage === 1;
                        if(nextBtn) nextBtn.disabled = importTablePage === totalPages;
                    } else {
                        paginationContainer.classList.add('hidden');
                    }
                }
            }

        if (importBtn && importModal) {
            const dropZone = document.getElementById('import-drop-zone');
            const fileInput = document.getElementById('import-goals-file');

            // Inject Pagination Controls into Analysis Container if not present
            if (!document.getElementById('import-pagination-controls')) {
                const tableContainer = analysisContainer.querySelector('.bg-slate-900.rounded-lg.border.border-slate-700');
                if (tableContainer) {
                    tableContainer.parentNode.insertBefore(importPaginationControls, tableContainer.nextSibling);
                }
            }

            // Bind Pagination Listeners
            const prevBtn = document.getElementById('import-prev-page-btn');
            const nextBtn = document.getElementById('import-next-page-btn');

            if (prevBtn) {
                prevBtn.addEventListener('click', () => {
                    if (importTablePage > 1) {
                        importTablePage--;
                        renderImportTable();
                    }
                });
            }
            if (nextBtn) {
                nextBtn.addEventListener('click', () => {
                    const totalPages = Math.ceil(pendingImportUpdates.length / importTablePageSize);
                    if (importTablePage < totalPages) {
                        importTablePage++;
                        renderImportTable();
                    }
                });
            }

            importBtn.addEventListener('click', () => {
                importModal.classList.remove('hidden');
                importTextarea.value = '';
                analysisContainer.classList.add('hidden');
                importConfirmBtn.disabled = true;
                importConfirmBtn.classList.add('opacity-50', 'cursor-not-allowed');

                // Reset File Input
                if (fileInput) fileInput.value = '';
                if (dropZone) {
                    dropZone.classList.remove('bg-slate-700/50', 'border-teal-500');
                    dropZone.innerHTML = `
                        <svg class="w-12 h-12 text-slate-400 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"></path>
                        </svg>
                        <p class="text-slate-300 font-medium mb-2">Arraste e solte o arquivo Excel aqui</p>
                        <p class="text-slate-500 text-sm mb-4">ou</p>
                        <label for="import-goals-file" class="bg-blue-600 hover:bg-blue-700 text-white font-bold py-2 px-6 rounded-lg cursor-pointer transition-colors shadow-lg">
                            Selecionar Arquivo
                        </label>
                        <p class="text-xs text-slate-500 mt-4">Formatos suportados: .xlsx, .xls, .csv</p>
                    `;
                }
            });

            const closeModal = () => {
                importModal.classList.add('hidden');
            };

            importCloseBtn.addEventListener('click', closeModal);
            importCancelBtn.addEventListener('click', closeModal);

            // Drag & Drop Logic
            if (dropZone) {
                ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
                    dropZone.addEventListener(eventName, preventDefaults, false);
                });

                function preventDefaults(e) {
                    e.preventDefault();
                    e.stopPropagation();
                }

                ['dragenter', 'dragover'].forEach(eventName => {
                    dropZone.addEventListener(eventName, () => {
                        dropZone.classList.add('bg-slate-700/50', 'border-teal-500');
                    });
                });

                ['dragleave', 'drop'].forEach(eventName => {
                    dropZone.addEventListener(eventName, () => {
                        dropZone.classList.remove('bg-slate-700/50', 'border-teal-500');
                    });
                });

                dropZone.addEventListener('drop', (e) => {
                    const dt = e.dataTransfer;
                    const files = dt.files;
                    handleFiles(files);
                });
            }

            if (fileInput) {
                fileInput.addEventListener('change', (e) => {
                    handleFiles(e.target.files);
                });
            }

            function handleFiles(files) {
                if (files.length === 0) return;
                const file = files[0];

                // Visual Feedback: Loading
                if (dropZone) {
                    dropZone.innerHTML = `
                        <div class="flex flex-col items-center justify-center">
                            <svg class="animate-spin h-10 w-10 text-teal-500 mb-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                            </svg>
                            <p class="text-slate-300 font-medium animate-pulse">Carregando ${file.name}...</p>
                        </div>
                    `;
                }

                const reader = new FileReader();
                reader.onload = (e) => {
                    try {
                        const data = new Uint8Array(e.target.result);
                        const workbook = XLSX.read(data, {type: 'array'});

                        const sheetName = workbook.SheetNames[0];
                        const sheet = workbook.Sheets[sheetName];

                        // Convert to TSV for the parser
                        const tsv = XLSX.utils.sheet_to_csv(sheet, {FS: "\t"});

                        // Update UI
                        importTextarea.value = tsv;
                        if (dropZone) {
                            dropZone.innerHTML = `
                                <svg class="w-12 h-12 text-green-500 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>
                                <p class="text-green-400 font-bold mb-2">Sucesso!</p>
                                <p class="text-slate-400 text-sm">${file.name} carregado.</p>
                            `;
                        }

                        // Auto-analyze
                        setTimeout(() => importAnalyzeBtn.click(), 500);

                    } catch (err) {
                        console.error(err);
                        if (dropZone) {
                            dropZone.innerHTML = `
                                <svg class="w-12 h-12 text-red-500 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
                                <p class="text-red-400 font-bold mb-2">Erro!</p>
                                <p class="text-slate-400 text-sm">Falha ao ler o arquivo.</p>
                            `;
                        }
                    }
                };
                reader.readAsArrayBuffer(file);
            }

            function resolveGoalCategory(category) {
                // Returns list of leaf categories and metric type hint if needed
                if (category === 'tonelada_elma') return ['707', '708', '752'];
                if (category === 'tonelada_foods') return ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                if (category === 'total_elma') return ['707', '708', '752'];
                if (category === 'total_foods') return ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                return [category];
            }

            function getSellerCurrentGoal(sellerName, category, type) {
                const sellerCode = optimizedData.rcaCodeByName.get(sellerName);
                if (!sellerCode) return 0;

                // Check for Overrides FIRST
                const targets = goalsSellerTargets.get(sellerName);
                if (type === 'rev' && targets && targets[`${category}_FAT`] !== undefined) {
                    return targets[`${category}_FAT`];
                }
                if (type === 'vol' && targets && targets[`${category}_VOL`] !== undefined) {
                    return targets[`${category}_VOL`];
                }

                if (type === 'pos' || type === 'mix') {
                    // FIX: Do not mask missing data. If manual targets exist for seller, do not fall back to defaults.
                    if (targets) {
                        // Manual Override Exists for this Seller
                        if (targets[category] !== undefined) {
                            return targets[category];
                        }
                        // Explicitly return 0 if category is missing (User intended 0 or skipped it)
                        return 0;
                    } else {
                        // Calculate Default (Auto-Pilot for unconfigured sellers)
                        const defaults = calculateSellerDefaults(sellerName);
                        if (category === 'total_elma') return defaults.elmaPos;
                        if (category === 'total_foods') return defaults.foodsPos;
                        if (category === 'mix_salty') return defaults.mixSalty;
                        if (category === 'mix_foods') return defaults.mixFoods;
                        return 0;
                    }
                }

                if (type === 'rev' || type === 'vol') {
                    // Aggregate from globalClientGoals
                    const clients = optimizedData.clientsByRca.get(sellerCode) || [];
                    const activeClients = clients.filter(c => {
                        const cod = String(c['Código'] || c['codigo_cliente']);
                        const rca1 = String(c.rca1 || '').trim();
                        const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                        return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
                    });

                    let total = 0;
                    const leafCategories = resolveGoalCategory(category);

                    activeClients.forEach(client => {
                        const codCli = String(client['Código'] || client['codigo_cliente']);
                        const clientGoals = globalClientGoals.get(codCli);
                        if (clientGoals) {
                            leafCategories.forEach(leaf => {
                                const goal = clientGoals.get(leaf);
                                if (goal) {
                                    if (type === 'rev') total += (goal.fat || 0);
                                    else if (type === 'vol') total += (goal.vol || 0);
                                }
                            });
                        }
                    });
                    return total;
                }
                return 0;
            }

            // --- AI Insights Logic ---
                        async function generateAiInsights() {
                const btn = document.getElementById('btn-generate-ai');

                if (!pendingImportUpdates || pendingImportUpdates.length === 0) return;

                // UI Loading State
                btn.disabled = true;
                btn.innerHTML = `<svg class="animate-spin h-5 w-5 mr-2" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg> Analisando...`;

                // Show loading overlay
                const pageLoader = document.getElementById('page-transition-loader');
                const loaderText = document.getElementById('loader-text');
                if (pageLoader && loaderText) {
                    loaderText.textContent = "A Inteligência Artificial está analisando os dados...";
                    pageLoader.classList.remove('hidden');
                }

                try {
                    // 1. Prepare Data Context (Grouped by Supervisor, Strict Types, Top 5)
                    const supervisorsMap = new Map(); // Map<SupervisorName, { total_fat_diff, sellers: [] }>

                    // Helper to get or create supervisor entry
                    const getSupervisorEntry = (supervisorName) => {
                        if (!supervisorsMap.has(supervisorName)) {
                            supervisorsMap.set(supervisorName, {
                                name: supervisorName,
                                total_fat_diff: 0,
                                sellers: []
                            });
                        }
                        return supervisorsMap.get(supervisorName);
                    };

                    // Helper to resolve human-readable category name
                    const resolveCategoryName = (catCode) => {
                        const map = {
                            '707': 'Extrusados',
                            '708': 'Não Extrusados',
                            '752': 'Torcida',
                            '1119_TODDYNHO': 'Toddynho',
                            '1119_TODDY': 'Toddy',
                            '1119_QUAKER_KEROCOCO': 'Quaker/Kero Coco',
                            'tonelada_elma': 'Elma Chips',
                            'tonelada_foods': 'Foods',
                            'total_elma': 'Elma Chips',
                            'total_foods': 'Foods',
                            'mix_salty': 'Mix Salty',
                            'mix_foods': 'Mix Foods'
                        };
                        return map[catCode] || catCode;
                    };

                    // Helper to resolve history (simplified for context)
                    const getSellerHistorySimple = (sellerName, type, category) => {
                       // Note: Full history calculation is expensive. We can use the 'current' logic as baseline if history isn't cached.
                       // For accurate comparison, we should use the same logic as renderImportTable if possible, or fetch from history data.
                       // Here we simply return 0 if strict calculation is too heavy, relying on the 'diff' already calculated in pendingImportUpdates?
                       // Actually pendingImportUpdates doesn't store history, it stores 'val' (new).
                       // We can compute history on the fly for the top items only? No, we need to sort first.
                       // Let's rely on 'getSellerCurrentGoal' as the "Old" value (which is Current Target).
                       // The Prompt asks for comparison with "History".
                       // getSellerCurrentGoal returns the *Current Goal* before update.
                       // The AI prompt usually compares New Goal vs History Avg.
                       // Let's provide [Current Goal] and [New Goal]. The AI can infer "Change".
                       return getSellerCurrentGoal(sellerName, category, type);
                    };

                    // Process Updates
                    for (const u of pendingImportUpdates) {
                        // Filter out supervisors/aggregates
                        if (isGarbageSeller(u.seller)) continue;

                        const upperUName = u.seller.normalize('NFD').replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
                        if (optimizedData.rcasBySupervisor.has(upperUName) || optimizedData.rcasBySupervisor.has(u.seller)) {
                            continue;
                        }

                        // Determine Supervisor
                        const sellerCode = optimizedData.rcaCodeByName.get(u.seller);
                        let supervisorName = 'Sem Supervisor';
                        if (u.seller === 'AMERICANAS') {
                            supervisorName = 'BALCAO';
                        } else if (sellerCode) {
                            const details = sellerDetailsMap.get(sellerCode);
                            if (details && details.supervisor) supervisorName = details.supervisor;
                        }

                        const oldVal = getSellerCurrentGoal(u.seller, u.category, u.type);
                        const diff = u.val - oldVal;
                        const impact = Math.abs(diff);

                        // FILTER: Ignore 0 variation globally (Irrelevant info)
                        if (Math.abs(diff) < 0.01) continue;

                        // Define Unit explicitly
                        let unit = '';
                        if (u.type === 'rev') unit = 'R$';
                        else if (u.type === 'vol') unit = 'Kg';
                        else unit = 'Clientes'; // Pos and Mix count as clients

                        // Add to Supervisor Group
                        const supervisorEntry = getSupervisorEntry(supervisorName);

                        // We aggregate items per seller? Or just list variations?
                        // Requirement: "list the main variations of 5 sellers".
                        // It's better to list *Variations* as items.
                        // One seller might have huge Rev change AND huge Vol change.

                        supervisorEntry.sellers.push({
                            seller: u.seller,
                            category: u.category,
                            metric_type: u.type,
                            unit: unit,
                            old_value: oldVal,
                            new_value: u.val,
                            diff: diff,
                            impact: impact
                        });
                    }

                    const optimizedContext = { supervisors: [] };

                    supervisorsMap.forEach(sup => {
                        // Sort by Impact (Magnitude of change)
                        sup.sellers.sort((a, b) => b.impact - a.impact);

                        // Deduplicate Variations (same seller + same metric)
                        const seen = new Set();
                        const uniqueVariations = [];

                        for (const v of sup.sellers) {
                            const catName = resolveCategoryName(v.category);
                            let metricName = '';
                            if (v.unit === 'R$') metricName = `Faturamento (${catName})`;
                            else if (v.unit === 'Kg') metricName = `Volume (${catName})`;
                            else metricName = `Positivação (${catName})`;

                            // Create unique signature
                            const sig = `${v.seller}|${metricName}`;

                            if (!seen.has(sig)) {
                                seen.add(sig);
                                // Attach resolved metric name for later use
                                v._resolvedMetricName = metricName;
                                uniqueVariations.push(v);
                            }
                        }

                        // Apply Limits: 5 for BALCAO, 10 for Others
                        const limit = sup.name === 'BALCAO' ? 5 : 10;
                        const topVariations = uniqueVariations.slice(0, limit).map(v => {
                            return {
                                seller: v.seller,
                                metric: v._resolvedMetricName,
                                details: `${v.unit} ${Math.round(v.old_value)} -> ${Math.round(v.new_value)} (Diff: ${v.diff > 0 ? '+' : ''}${Math.round(v.diff)})`
                            };
                        });

                        optimizedContext.supervisors.push({
                            name: sup.name,
                            top_variations: topVariations
                        });
                    });

                    const promptText = `
                        Atue como um Gerente Nacional de Vendas da Prime Distribuição.
                        Analise as alterações de metas propostas (Proposed Goals).

                        Dados: ${JSON.stringify(optimizedContext)}

                        Gere um relatório JSON estritamente com esta estrutura:
                        {
                            "global_summary": "Resumo executivo da estratégia geral percebida nas alterações. Use emojis.",
                            "supervisors": [
                                {
                                    "name": "Nome do Supervisor",
                                    "analysis": "Parágrafo de análise estratégica sobre este time. Identifique se o foco é agressividade em vendas, recuperação de volume ou cobertura.",
                                    "variations": [
                                        {
                                            "seller": "Nome Vendedor",
                                            "metric": "O nome completo da métrica (ex: Faturamento (Extrusados))",
                                            "change_display": "Texto ex: R$ 50k -> R$ 60k (+10k)",
                                            "insight": "Comentário curto sobre o impacto (ex: 'Aumento agressivo', 'Ajuste conservador')"
                                        }
                                    ]
                                }
                            ]
                        }

                        Regras:
                        1. "variations" deve conter EXATAMENTE os itens enviados no contexto.
                        2. Use o nome COMPLETO da métrica fornecido no input (ex: "Faturamento (Extrusados)"). NÃO simplifique para apenas "Faturamento".
                        3. Retorne APENAS o JSON.
                    `;

                    // 2. Call API
                    const metaEntry = embeddedData.metadata ? embeddedData.metadata.find(m => m.key === 'groq_api_key') : null;
                    const API_KEY = metaEntry ? metaEntry.value : null;

                    if (!API_KEY) throw new Error("Chave de API não configurada.");

                    const response = await fetch(`https://api.groq.com/openai/v1/chat/completions`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': `Bearer ${API_KEY}`
                        },
                        body: JSON.stringify({
                            model: "llama-3.3-70b-versatile",
                            messages: [{ role: "user", content: promptText }]
                        })
                    });

                    const data = await response.json();
                    if (data.error) throw new Error(data.error.message);

                    const aiText = data.choices[0].message.content;

                    // 3. Render Output
                    let result;
                    try {
                        const jsonStart = aiText.indexOf('{');
                        const jsonEnd = aiText.lastIndexOf('}');
                        result = JSON.parse(aiText.substring(jsonStart, jsonEnd + 1));

                        // Deduplicate Variations (Post-Processing)
                        if (result.supervisors) {
                            result.supervisors.forEach(sup => {
                                if (sup.variations) {
                                    const uniqueMap = new Map();
                                    const cleanVariations = [];

                                    sup.variations.forEach(v => {
                                        // Create signature: Seller + Metric Name
                                        // Normalize strings to avoid case/space issues
                                        const sig = `${v.seller}_${v.metric}`.trim().toLowerCase();

                                        if (!uniqueMap.has(sig)) {
                                            uniqueMap.set(sig, true);
                                            cleanVariations.push(v);
                                        }
                                    });

                                    // Enforce Limits (Safety Net)
                                    // If AI hallucinates or context structure varies, ensure BALCAO/Americanas is capped at 5
                                    // Other supervisors can show up to 10
                                    const isBalcao = sup.name && (sup.name.toUpperCase() === 'BALCAO' || sup.name.toUpperCase().includes('AMERICANAS'));
                                    const limit = isBalcao ? 5 : 10;

                                    sup.variations = cleanVariations.slice(0, limit);
                                }
                            });
                        }
                    } catch (e) {
                        console.error("AI JSON Parse Error", e);
                        // Fallback simple structure
                        result = { global_summary: aiText, supervisors: [] };
                    }

                    renderAiFullPage(result);

                } catch (err) {
                    console.error("AI Error:", err);
                    alert(`Erro na análise: ${err.message}`);
                } finally {
                    btn.disabled = false;
                    btn.innerHTML = `✨ Gerar Insights`;
                    if (pageLoader) pageLoader.classList.add('hidden');
                }
            }

            function renderAiSummaryChart(fatDiff) {
                const chartContainer = document.getElementById('ai-chart-container');
                if(!chartContainer) return;

                // Clear previous canvas
                chartContainer.innerHTML = '<canvas id="aiSummaryChart"></canvas>';
                const ctx = document.getElementById('aiSummaryChart').getContext('2d');

                // Calc Total Current vs Total Proposed based on diff (Approximation for visual)
                // We need the absolute totals to make a bar chart.
                // Let's iterate updates again to sum "Current" and "Proposed" totals for Revenue only.
                let totalCurrent = 0;
                let totalProposed = 0;

                pendingImportUpdates.forEach(u => {
                    if (u.type === 'rev') {
                        const cur = getSellerCurrentGoal(u.seller, u.category, u.type);
                        totalCurrent += cur;
                        totalProposed += u.val;
                    }
                });

                new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: ['Meta Atual', 'Nova Proposta'],
                        datasets: [{
                            label: 'Faturamento Total (R$)',
                            data: [totalCurrent, totalProposed],
                            backgroundColor: ['#64748b', fatDiff >= 0 ? '#22c55e' : '#ef4444'],
                            borderWidth: 0,
                            borderRadius: 4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false },
                            title: { display: true, text: 'Comparativo de Faturamento Total', color: '#fff' }
                        },
                        scales: {
                            y: {
                                beginAtZero: false,
                                grid: { color: '#334155' },
                                ticks: {
                                    color: '#94a3b8',
                                    callback: function(value) {
                                        return new Intl.NumberFormat('pt-BR', { notation: "compact", maximumFractionDigits: 1 }).format(value);
                                    }
                                }
                            },
                            x: { grid: { display: false }, ticks: { color: '#fff' } }
                        }
                    }
                });
            }

            const btnGenerateAi = document.getElementById('btn-generate-ai');
            if(btnGenerateAi) {
                btnGenerateAi.addEventListener('click', generateAiInsights);
            }

            importAnalyzeBtn.addEventListener('click', () => {
                console.log("Analisar Texto Colado clicado");
                try {
                    const text = importTextarea.value;
                    if (!text.trim()) {
                        alert("A área de texto está vazia. Cole os dados ou arraste um arquivo novamente.");
                        return;
                    }
                    console.log("Iniciando análise. Tamanho do texto:", text.length);

                    const updates = parseGoalsSvStructure(text);
                    console.log("Resultado da análise:", updates ? updates.length : "null");

                    if (!updates || updates.length === 0) {
                        alert("Nenhum dado válido encontrado para atualização. \n\nVerifique se:\n1. O arquivo possui os cabeçalhos corretos (3 linhas iniciais).\n2. As colunas de 'Ajuste' contêm valores numéricos.\n3. Os nomes dos vendedores correspondem ao cadastro.");
                        return;
                    }

                    pendingImportUpdates = updates;

                    // Reset to page 1 and render using the pagination function
                    importTablePage = 1;
                    renderImportTable();

                    analysisBadges.innerHTML = `<span class="bg-blue-600 text-white px-3 py-1 rounded-full text-xs font-bold">${updates.length} Registros Encontrados</span>`;
                    analysisContainer.classList.remove('hidden');

                    // Force Scroll
                    analysisContainer.scrollIntoView({ behavior: 'smooth', block: 'start' });

                    importConfirmBtn.disabled = false;
                    importConfirmBtn.classList.remove('opacity-50', 'cursor-not-allowed');
                } catch (e) {
                    console.error("Erro ao analisar dados importados:", e);
                    alert("Erro ao analisar dados: " + e.message);
                }
            });

            importConfirmBtn.addEventListener('click', async () => {
                const originalText = importConfirmBtn.textContent;
                importConfirmBtn.textContent = "Salvando...";
                importConfirmBtn.disabled = true;
                importConfirmBtn.classList.add('opacity-50', 'cursor-not-allowed');

                try {
                    let countRev = 0;
                    let countPos = 0;

                    // --- FULL RESET (Purge Everything) ---
                    // To guarantee no ghost data from Supervisors or previous states persists, we clear all targets.
                    // Only active sellers (backfilled) and imported sellers will remain.
                    goalsSellerTargets.clear();
                    globalClientGoals.clear();
                    // ------------------------

                    // 1. Process Manual Updates (Imported)
                    const importedSellers = new Set();
                    pendingImportUpdates.forEach(u => {
                        importedSellers.add(u.seller);
                        if (u.type === 'rev') {
                            distributeSellerGoal(u.seller, u.category, u.val, 'fat');
                            // Save Override
                            if (!goalsSellerTargets.has(u.seller)) goalsSellerTargets.set(u.seller, {});
                            const t = goalsSellerTargets.get(u.seller);
                            t[`${u.category}_FAT`] = u.val;
                            countRev++;
                        } else if (u.type === 'vol') {
                            distributeSellerGoal(u.seller, u.category, u.val, 'vol');
                            // Save Override
                            if (!goalsSellerTargets.has(u.seller)) goalsSellerTargets.set(u.seller, {});
                            const t = goalsSellerTargets.get(u.seller);
                            t[`${u.category}_VOL`] = u.val;
                            countRev++;
                        } else if (u.type === 'pos' || u.type === 'mix') {
                            // Update Seller Target Map
                            if (!goalsSellerTargets.has(u.seller)) goalsSellerTargets.set(u.seller, {});
                            const t = goalsSellerTargets.get(u.seller);
                            t[u.category] = u.val;
                            countPos++;
                        }
                    });

                    // 2. Backfill Defaults for ALL Active Sellers
                    // Iterate all active sellers to ensure their calculated "Suggestions" are saved if not manually set.
                    // We get active sellers from optimizedData.rcasBySupervisor
                    // 2. Backfill Defaults for ALL Active Sellers
                    // Iterate all active sellers to ensure their calculated "Suggestions" are saved if not manually set.
                    // 2. Backfill Defaults Removed
                    // We rely on getSellerCurrentGoal dynamic calculation for unconfigured sellers.
                    // This avoids materializing defaults into overrides, which would prevent strict mode behavior (returning 0 for missing manual targets).

                    // Save to Supabase (SKIPPED - Load to Memory Only)
                    // const success = await saveGoalsToSupabase();

                    alert(`Importação realizada! As metas foram carregadas para a aba "Rateio Metas". Verifique e salve manualmente.`);
                    closeModal();

                    // Switch to "Rateio Metas" tab to verify
                    const btnGv = document.querySelector('button[data-tab="gv"]');
                    if (btnGv) btnGv.click();
                } catch (e) {
                    console.error("Erro no processo de confirmação:", e);
                    alert("Erro ao processar/salvar: " + e.message);
                } finally {
                    importConfirmBtn.textContent = originalText;
                    importConfirmBtn.disabled = false;
                    importConfirmBtn.classList.remove('opacity-50', 'cursor-not-allowed');
                }
            });
        }
        async function exportMetaRealizadoPDF() {
            const { jsPDF } = window.jspdf;
            const doc = new jsPDF('landscape');

            const supervisor = document.getElementById('meta-realizado-supervisor-filter-text').textContent;
            const vendedor = document.getElementById('meta-realizado-vendedor-filter-text').textContent;
            const supplier = document.getElementById('meta-realizado-supplier-filter-text').textContent;
            const pasta = currentMetaRealizadoPasta;
            const generationDate = new Date().toLocaleString('pt-BR');

            // --- Header ---
            doc.setFontSize(18);
            doc.text('Painel Meta vs Realizado', 14, 22);
            doc.setFontSize(10);
            doc.setTextColor(100);
            doc.text(`Data de Emissão: ${generationDate}`, 14, 30);
            doc.text(`Filtros: Supervisor: ${supervisor} | Vendedor: ${vendedor} | Fornecedor: ${supplier} | Pasta: ${pasta}`, 14, 36);

            // --- Table 1: Sellers Summary ---
            // Build dynamic headers based on weeks
            const weeksHeaders = [];
            metaRealizadoDataForExport.weeks.forEach((w, i) => {
                weeksHeaders.push({ content: `Semana ${i + 1}`, colSpan: 2, styles: { halign: 'center' } });
            });

            const weeksSubHeaders = [];
            metaRealizadoDataForExport.weeks.forEach(() => {
                weeksSubHeaders.push('Meta');
                weeksSubHeaders.push('Real.');
            });

            const sellersHead = [
                [
                    { content: 'Vendedor', rowSpan: 2, styles: { valign: 'middle', halign: 'center' } },
                    { content: 'Geral', colSpan: 2, styles: { halign: 'center' } },
                    ...weeksHeaders,
                    { content: 'Positivação', colSpan: 2, styles: { halign: 'center' } }
                ],
                [
                    'Meta Total', 'Real. Total',
                    ...weeksSubHeaders,
                    'Meta', 'Real.'
                ]
            ];

            const sellersBody = metaRealizadoDataForExport.sellers.map(row => {
                const weekCells = [];
                row.weekData.forEach(w => {
                    weekCells.push(w.meta.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
                    weekCells.push(w.real.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
                });
                return [
                    getFirstName(row.name),
                    row.metaTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }),
                    row.realTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }),
                    ...weekCells,
                    row.posGoal,
                    row.posRealized
                ];
            });

            doc.autoTable({
                head: sellersHead,
                body: sellersBody,
                startY: 45,
                theme: 'grid',
                styles: { fontSize: 7, cellPadding: 1, textColor: [0, 0, 0], halign: 'center' },
                headStyles: { fillColor: [30, 41, 59], textColor: 255, fontStyle: 'bold', lineWidth: 0.1, lineColor: [200, 200, 200] },
                alternateRowStyles: { fillColor: [240, 240, 240] },
                columnStyles: {
                    0: { halign: 'left', fontStyle: 'bold' } // Vendedor Name
                },
                didParseCell: function(data) {
                    if (data.section === 'body' && data.column.index > 2) {
                        // Highlight Logic if needed (e.g. Red for Past weeks deficit)
                    }
                }
            });

            // --- Table 2: Clients Detail ---
            doc.addPage();
            doc.setFontSize(14);
            doc.text('Detalhamento por Cliente', 14, 20);

            const clientsHead = [
                [
                    { content: 'Cód', rowSpan: 2, styles: { valign: 'middle' } },
                    { content: 'Cliente', rowSpan: 2, styles: { valign: 'middle' } },
                    { content: 'Vendedor', rowSpan: 2, styles: { valign: 'middle' } },
                    { content: 'Cidade', rowSpan: 2, styles: { valign: 'middle' } },
                    { content: 'Geral', colSpan: 2, styles: { halign: 'center' } },
                    ...weeksHeaders
                ],
                [
                    'Meta', 'Real.',
                    ...weeksSubHeaders
                ]
            ];

            const clientsBody = metaRealizadoDataForExport.clients.map(row => {
                const weekCells = [];
                row.weekData.forEach(w => {
                    weekCells.push(w.meta.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
                    weekCells.push(w.real.toLocaleString('pt-BR', { minimumFractionDigits: 2 }));
                });
                return [
                    row.codcli,
                    row.razaoSocial,
                    getFirstName(row.vendedor),
                    row.cidade,
                    row.metaTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }),
                    row.realTotal.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }),
                    ...weekCells
                ];
            });

            doc.autoTable({
                head: clientsHead,
                body: clientsBody,
                startY: 25,
                theme: 'grid',
                styles: { fontSize: 6, cellPadding: 1, textColor: [0, 0, 0], halign: 'right' },
                headStyles: { fillColor: [22, 30, 61], textColor: 255, fontStyle: 'bold' },
                columnStyles: {
                    0: { halign: 'center', cellWidth: 15 },
                    1: { halign: 'left', cellWidth: 40 },
                    2: { halign: 'left', cellWidth: 20 },
                    3: { halign: 'left', cellWidth: 20 },
                }
            });

            // Add Page Numbers
            const pageCount = doc.internal.getNumberOfPages();
            for(let i = 1; i <= pageCount; i++) {
                doc.setPage(i);
                doc.setFontSize(8);
                doc.setTextColor(150);
                doc.text(`Página ${i} de ${pageCount}`, doc.internal.pageSize.width / 2, doc.internal.pageSize.height - 10, { align: 'center' });
            }

            let fileNameParam = 'geral';
            if (hierarchyState['meta-realizado'] && hierarchyState['meta-realizado'].promotors.size === 1) {
            }
            const safeFileNameParam = fileNameParam.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            doc.save(`meta_vs_realizado_${safeFileNameParam}_${new Date().toISOString().slice(0,10)}.pdf`);
        }

            function renderAiFullPage(data) {
                const fullPage = document.getElementById('ai-insights-full-page');
                const contentDiv = document.getElementById('ai-insights-full-content');
                const mainWrapper = document.getElementById('content-wrapper'); // This wraps dashboard
                const modal = document.getElementById('import-goals-modal'); // Close the modal

                if (!fullPage || !contentDiv) return;

                // Close Import Modal
                if (modal) modal.classList.add('hidden');

                // Hide Main Dashboard / Content Wrapper
                // Note: index.html structure shows content-wrapper wraps everything *except* the new page which I inserted *inside*?
                // Let's check where I inserted it.
                // "Insert the new full-screen AI insights container into index.html... inside content-wrapper"
                // If it is inside content-wrapper, hiding content-wrapper hides it too.
                // Wait, in my previous step I used  to insert it *before* Goals View.
                //  is inside .
                // So  IS inside .
                // Therefore, I should hide the *siblings* (dashboard, goals-view, etc) explicitly, NOT the wrapper.

                // Hide all main views
                ['main-dashboard', 'city-view', 'comparison-view', 'stock-view', 'coverage-view', 'goals-view', 'meta-realizado-view', 'mix-view', 'innovations-month-view'].forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.classList.add('hidden');
                });

                // Show AI Page
                fullPage.classList.remove('hidden');

                // Render Content
                let html = `
                    <div class="bg-gradient-to-r from-slate-800 to-slate-900 rounded-lg p-6 border border-slate-700 shadow-lg mb-8">
                        <h2 class="text-2xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-teal-400 to-blue-400 mb-4">
                            🌍 Resumo Estratégico Global
                        </h2>
                        <p class="text-lg text-slate-300 leading-relaxed">${data.global_summary || 'Análise indisponível.'}</p>
                    </div>

                    <div id="ai-full-page-chart-container" class="mt-8 mb-8 h-80 bg-slate-800 rounded-xl p-4 border border-slate-700 relative">
                        <canvas id="aiFullPageSummaryChart"></canvas>
                    </div>

                    <div class="grid grid-cols-1 xl:grid-cols-2 gap-8">
                `;

                if (data.supervisors) {
                    data.supervisors.forEach(sup => {
                        html += `
                            <div class="bg-slate-800 rounded-xl border border-slate-700 overflow-hidden shadow-md flex flex-col">
                                <div class="p-5 border-b border-slate-700 bg-slate-800/50">
                                    <h3 class="text-xl font-bold text-white flex items-center gap-2">
                                        <div class="w-2 h-8 bg-blue-500 rounded-full"></div>
                                        ${sup.name}
                                    </h3>
                                </div>
                                <div class="p-6 flex-1">
                                    <div class="mb-6">
                                        <h4 class="text-sm font-bold text-slate-400 uppercase tracking-wider mb-2">Análise do Time</h4>
                                        <p class="text-slate-300 text-sm leading-relaxed">${sup.analysis}</p>
                                    </div>

                                    <div>
                                        <div class="flex justify-between items-center mb-3">
                                            <h4 class="text-sm font-bold text-slate-400 uppercase tracking-wider">Detalhamento por Vendedor</h4>
                                            <span class="text-xs text-slate-500 bg-slate-800 border border-slate-700 px-2 py-1 rounded-full">Top ${sup.variations ? sup.variations.length : 0} Variações</span>
                                        </div>
                                        <div class="overflow-x-auto rounded-lg border border-slate-700/50">
                                            <table class="w-full text-sm text-left text-slate-300">
                                                <thead class="text-xs text-slate-400 uppercase bg-slate-900/80">
                                                    <tr>
                                                        <th class="px-4 py-3 font-semibold tracking-wide">Vendedor</th>
                                                        <th class="px-4 py-3 font-semibold tracking-wide">Métrica</th>
                                                        <th class="px-4 py-3 font-semibold tracking-wide text-right">Alteração</th>
                                                        <th class="px-4 py-3 font-semibold tracking-wide">Insight</th>
                                                    </tr>
                                                </thead>
                                                <tbody class="divide-y divide-slate-700/50 bg-slate-800/30">
                        `;

                        if (sup.variations) {
                            sup.variations.forEach(v => {
                                // Enhance change display with colors and icons
                                let coloredChange = v.change_display;

                                // Regex for (+...) -> Green with arrow up
                                if (coloredChange.includes('(+')) {
                                    coloredChange = coloredChange.replace(/(\(\+[^)]+\))/g, '<span class="text-emerald-400 font-bold bg-emerald-400/10 px-1.5 py-0.5 rounded ml-1 text-xs">$1</span>');
                                } else if (coloredChange.includes('(-')) {
                                    coloredChange = coloredChange.replace(/(\(-[^)]+\))/g, '<span class="text-rose-400 font-bold bg-rose-400/10 px-1.5 py-0.5 rounded ml-1 text-xs">$1</span>');
                                }

                                // Format metric with bold prefix
                                const metricParts = v.metric.split('(');
                                let formattedMetric = v.metric;
                                if (metricParts.length > 1) {
                                    formattedMetric = `<span class="text-slate-300 font-medium">${metricParts[0]}</span> <span class="text-slate-500 text-xs">(${metricParts[1]}</span>`;
                                }

                                html += `
                                    <tr class="hover:bg-slate-700/40 transition-colors group">
                                        <td class="px-4 py-3 font-medium text-white group-hover:text-blue-300 transition-colors">${v.seller}</td>
                                        <td class="px-4 py-3 text-slate-400">${formattedMetric}</td>
                                        <td class="px-4 py-3 font-mono text-xs text-right whitespace-nowrap">${coloredChange}</td>
                                        <td class="px-4 py-3 text-blue-300/90 text-xs italic border-l border-slate-700/50 pl-4">"${v.insight}"</td>
                                    </tr>
                                `;
                            });
                        }

                        html += `
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        `;
                    });
                }

                html += `</div>`;
                contentDiv.innerHTML = html;

                // Render Chart
                setTimeout(renderFullPageSummaryChart, 100);

                // Scroll to top
                window.scrollTo({ top: 0, behavior: 'smooth' });
            }

            function renderFullPageSummaryChart() {
                const ctx = document.getElementById('aiFullPageSummaryChart');
                if (!ctx) return;

                // Calculate Totals
                let totalCurrent = 0;
                let totalProposed = 0;

                // Ensure we have updates
                if (pendingImportUpdates) {
                    pendingImportUpdates.forEach(u => {
                        if (u.type === 'rev') {
                            const cur = getSellerCurrentGoal(u.seller, u.category, u.type);
                            totalCurrent += cur;
                            totalProposed += u.val;
                        }
                    });
                }

                const diff = totalProposed - totalCurrent;
                const diffColor = diff >= 0 ? '#22c55e' : '#ef4444';

                if (window.aiFullPageChartInstance) {
                    window.aiFullPageChartInstance.data.datasets[0].data = [totalCurrent, totalProposed];
                    window.aiFullPageChartInstance.data.datasets[0].backgroundColor = ['#64748b', diffColor];
                    window.aiFullPageChartInstance.options.plugins.title.text = `Comparativo Global de Faturamento (Diferença: ${diff > 0 ? '+' : ''}${diff.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'})})`;
                    window.aiFullPageChartInstance.update('none');
                } else {
                    window.aiFullPageChartInstance = new Chart(ctx, {
                        type: 'bar',
                        data: {
                            labels: ['Meta Atual', 'Nova Proposta'],
                            datasets: [{
                                label: 'Faturamento Total (R$)',
                                data: [totalCurrent, totalProposed],
                                backgroundColor: ['#64748b', diffColor],
                                borderRadius: 6,
                                barPercentage: 0.5
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {
                                legend: { display: false },
                                title: {
                                    display: true,
                                    text: `Comparativo Global de Faturamento (Diferença: ${diff > 0 ? '+' : ''}${diff.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'})})`,
                                    color: '#fff',
                                    font: { size: 16 }
                                },
                                datalabels: {
                                    color: '#fff',
                                    anchor: 'end',
                                    align: 'top',
                                    formatter: (val) => val.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }),
                                    font: { weight: 'bold' }
                                }
                            },
                            scales: {
                                y: {
                                    beginAtZero: false,
                                    grid: { color: '#334155' },
                                    ticks: { color: '#94a3b8' }
                                },
                                x: {
                                    grid: { display: false },
                                    ticks: { color: '#fff', font: { size: 14, weight: 'bold' } }
                                }
                            }
                        }
                    });
                }
            }

            // Export to HTML Function
            document.getElementById('ai-insights-export-btn')?.addEventListener('click', () => {
                const content = document.getElementById('ai-insights-full-content').innerHTML;
                const timestamp = new Date().toLocaleString();

                const fullHtml = `
                    <!DOCTYPE html>
                    <html lang="pt-br">
                    <head>
                        <meta charset="UTF-8">
                        <meta name="viewport" content="width=device-width, initial-scale=1.0">
                        <title>Relatório de Insights IA - ${timestamp}</title>
                        <script src="https://cdn.tailwindcss.com"></script>
                        <style>body { background-color: #0f172a; color: #cbd5e1; font-family: sans-serif; }</style>
                    </head>
                    <body class="p-8">
                        <div class="max-w-7xl mx-auto">
                            <h1 class="text-3xl font-bold text-white mb-2">Relatório de Insights IA</h1>
                            <p class="text-slate-400 mb-8">Gerado em: ${timestamp}</p>
                            ${content}
                        </div>
                    </body>
                    </html>
                `;

                const blob = new Blob([fullHtml], { type: 'text/html' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `insights_ia_${new Date().toISOString().slice(0,10)}.html`;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            });

            // Back Button Logic
            document.getElementById('ai-insights-back-btn')?.addEventListener('click', () => {
                document.getElementById('ai-insights-full-page').classList.add('hidden');
                // Restore Dashboard (or Goals View specifically since we came from there)
                // Default to Goals View since the Import Modal is there
                navigateTo('goals');

                // Re-open the Import Modal to preserve context/flow
                const modal = document.getElementById('import-goals-modal');
                if (modal) modal.classList.remove('hidden');
            });

        function initFloatingFilters() {
            const toggleBtn = document.getElementById('floating-filters-toggle');
            const sentinels = document.querySelectorAll('.filter-wrapper-sentinel');

            if (!toggleBtn || sentinels.length === 0) return;

            // Scroll Logic to Show/Hide Button and Auto-Dock
            window.addEventListener('scroll', () => {
                let visibleSentinel = null;
                // Find the sentinel in the currently visible view
                for (const s of sentinels) {
                    if (s.offsetParent !== null) {
                        visibleSentinel = s;
                        break;
                    }
                }

                if (visibleSentinel) {
                    const rect = visibleSentinel.getBoundingClientRect();
                    // Threshold: When the bottom of the sentinel passes the top navigation area (approx 80px)
                    const isPassed = rect.bottom < 80;

                    if (isPassed) {
                        toggleBtn.classList.remove('hidden');
                    } else {
                        toggleBtn.classList.add('hidden');

                        // Auto-dock if scrolled back up
                        const filters = visibleSentinel.querySelector('.sticky-filters');
                        if (filters && filters.classList.contains('filters-overlay-mode')) {
                            filters.classList.remove('filters-overlay-mode');
                            toggleBtn.innerHTML = '<span class="text-lg leading-none mb-0.5">+</span><span>Filtros</span>';
                        }
                    }
                } else {
                    toggleBtn.classList.add('hidden');
                }
            }, { passive: true });

            // Click Handler
            toggleBtn.addEventListener('click', () => {
                let visibleFilters = null;
                // Find visible filters inside visible sentinel
                for (const s of sentinels) {
                    if (s.offsetParent !== null) {
                        visibleFilters = s.querySelector('.sticky-filters');
                        break;
                    }
                }

                if (visibleFilters) {
                    visibleFilters.classList.toggle('filters-overlay-mode');
                    const isActive = visibleFilters.classList.contains('filters-overlay-mode');

                    if (isActive) {
                        toggleBtn.innerHTML = '<span class="text-lg leading-none mb-0.5">-</span><span>Filtros</span>';
                    } else {
                        toggleBtn.innerHTML = '<span class="text-lg leading-none mb-0.5">+</span><span>Filtros</span>';
                    }
                }
            });
        }

            // --- SYSTEM DIAGNOSIS TOOL ---
            const diagnosisBtn = document.getElementById('system-diagnosis-btn');
            const diagnosisModal = document.getElementById('diagnosis-modal');
            const diagnosisCloseBtn = document.getElementById('diagnosis-close-btn');
            const diagnosisCopyBtn = document.getElementById('diagnosis-copy-btn');
            const diagnosisContent = document.getElementById('diagnosis-content');

            if (diagnosisBtn && diagnosisModal) {
                diagnosisBtn.addEventListener('click', () => {
                    const report = generateSystemDiagnosis();
                    diagnosisContent.textContent = report;
                    diagnosisModal.classList.remove('hidden');
                });

                diagnosisCloseBtn.addEventListener('click', () => diagnosisModal.classList.add('hidden'));
                
                diagnosisCopyBtn.addEventListener('click', () => {
                    navigator.clipboard.writeText(diagnosisContent.textContent).then(() => {
                        const originalText = diagnosisCopyBtn.innerHTML;
                        diagnosisCopyBtn.innerHTML = `<span class="text-green-300 font-bold">Copiado!</span>`;
                        setTimeout(() => diagnosisCopyBtn.innerHTML = originalText, 2000);
                    });
                });
            }

            function generateSystemDiagnosis() {
                const now = new Date();
                let report = `=== RELATÓRIO DE DIAGNÓSTICO DO SISTEMA ===\n`;
                report += `Data: ${now.toLocaleString()}\n`;
                report += `User Agent: ${navigator.userAgent}\n\n`;

                report += `--- 1. CONTEXTO DO USUÁRIO ---\n`;
                report += `Role (Window): ${window.userRole}\n`;
                report += `Contexto Resolvido: ${JSON.stringify(userHierarchyContext, null, 2)}\n\n`;

                report += `--- 2. ESTRUTURA DE DADOS ---\n`;
                report += `Clientes Totais (Bruto): ${allClientsData ? allClientsData.length : 'N/A'}\n`;
                report += `Vendas Detalhadas (Bruto): ${allSalesData ? allSalesData.length : 'N/A'}\n`;
                report += `Histórico (Bruto): ${allHistoryData ? allHistoryData.length : 'N/A'}\n`;
                report += `Pedidos Agregados: ${aggregatedOrders ? aggregatedOrders.length : 'N/A'}\n`;
                
                report += `\n--- 3. HIERARQUIA ---\n`;
                const hierRaw = embeddedData.hierarchy;
                report += `Raw Data Length: ${hierRaw ? hierRaw.length : 'N/A (Null)'}\n`;
                if (hierRaw && hierRaw.length > 0) {
                    report += `Sample Keys (First Item): ${JSON.stringify(Object.keys(hierRaw[0]))}\n`;
                }
                report += `Nós na Árvore de Hierarquia: ${optimizedData.hierarchyMap ? optimizedData.hierarchyMap.size : 'N/A'}\n`;
                report += `Clientes Mapeados (Client->Promotor): ${optimizedData.clientHierarchyMap ? optimizedData.clientHierarchyMap.size : 'N/A'}\n`;
                report += `Coordenadores Únicos: ${optimizedData.coordMap ? optimizedData.coordMap.size : 'N/A'}\n`;
                
                report += `\n--- 4. FILTROS ATIVOS (MAIN) ---\n`;
                const mainState = hierarchyState['main'];
                report += `Coords Selecionados: ${mainState ? Array.from(mainState.coords).join(', ') : 'N/A'}\n`;
                report += `CoCoords Selecionados: ${mainState ? Array.from(mainState.cocoords).join(', ') : 'N/A'}\n`;
                report += `Promotores Selecionados: ${mainState ? Array.from(mainState.promotors).join(', ') : 'N/A'}\n`;
                
                report += `\n--- 5. TESTE DE FILTRAGEM (Simulação) ---\n`;
                try {
                    const filteredClients = getHierarchyFilteredClients('main', allClientsData);
                    report += `Clientes Após Filtro de Hierarquia: ${filteredClients.length}\n`;
                    
                    if (filteredClients.length === 0) {
                        report += `[ALERTA] Filtro retornou 0 clientes. Verifique se o usuário '${window.userRole}' está mapeado na hierarquia.\n`;
                    } else {
                        // Sample check
                        const sampleClient = filteredClients[0];
                        const cod = String(sampleClient['Código'] || sampleClient['codigo_cliente']);
                        const node = optimizedData.clientHierarchyMap.get(normalizeKey(cod));
                        report += `Exemplo Cliente Aprovado: ${cod} (${sampleClient.fantasia || sampleClient.razaoSocial})\n`;
                        report += ` -> Mapeado para: ${node ? JSON.stringify(node.promotor) : 'SEM NÓ (Erro?)'}\n`;
                    }
                } catch (e) {
                    report += `Erro ao simular filtro: ${e.message}\n`;
                }

                report += `\n--- 6. VALIDAÇÃO DE CHAVES ---\n`;
                if (allClientsData && allClientsData.length > 0) {
                    const c = allClientsData instanceof ColumnarDataset ? allClientsData.get(0) : allClientsData[0];
                    report += `Exemplo Chave Cliente (Raw): '${c['Código'] || c['codigo_cliente']}'\n`;
                    report += `Exemplo Chave Cliente (Normalized): '${normalizeKey(c['Código'] || c['codigo_cliente'])}'\n`;
                }
                
                return report;
            }

    // --- WALLET MANAGEMENT LOGIC ---
    let isWalletInitialized = false;
    let walletState = {
        selectedPromoter: null,
        promoters: []
    };

    function initWalletView() {
        if (isWalletInitialized) return;
        isWalletInitialized = true;
        
        const role = (window.userRole || '').trim().toUpperCase();
        
        // Setup User Menu
        const userMenuBtn = document.getElementById('user-menu-btn');
        const userMenuDropdown = document.getElementById('user-menu-dropdown');
        const userMenuWalletBtn = document.getElementById('user-menu-wallet-btn');
        const userMenuLogoutBtn = document.getElementById('user-menu-logout-btn');
        
        if (userMenuBtn) {
            // Update User Info in Menu
            const nameEl = document.getElementById('user-menu-name');
            const roleEl = document.getElementById('user-menu-role');
            if (nameEl && roleEl) {
                 roleEl.textContent = role;
                 // Try find name
                 const h = embeddedData.hierarchy || [];
                 const me = h.find(x => 
                    (x.cod_coord && x.cod_coord.trim().toUpperCase() === role) || 
                    (x.cod_cocoord && x.cod_cocoord.trim().toUpperCase() === role) ||
                    (x.cod_promotor && x.cod_promotor.trim().toUpperCase() === role)
                 );
                 if (me) {
                      if (me.cod_coord && me.cod_coord.trim().toUpperCase() === role) nameEl.textContent = me.nome_coord;
                      else if (me.cod_cocoord && me.cod_cocoord.trim().toUpperCase() === role) nameEl.textContent = me.nome_cocoord;
                      else nameEl.textContent = me.nome_promotor;
                 }
            }

            userMenuBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                userMenuDropdown.classList.toggle('hidden');
            });
            
            document.addEventListener('click', (e) => {
                if (!userMenuBtn.contains(e.target) && !userMenuDropdown.contains(e.target)) {
                    userMenuDropdown.classList.add('hidden');
                }
            });
            
            userMenuWalletBtn.addEventListener('click', () => {
                userMenuDropdown.classList.add('hidden');
                navigateTo('wallet');
            });
            
            userMenuLogoutBtn.addEventListener('click', async () => {
                 const { error } = await window.supabaseClient.auth.signOut();
                 if (!error) window.location.reload();
            });
        }
        
        // Setup Wallet Controls
        const selectBtn = document.getElementById('wallet-promoter-select-btn');
        const dropdown = document.getElementById('wallet-promoter-dropdown');
        
        if (selectBtn) {
            selectBtn.addEventListener('click', (e) => {
                if (walletState.promoters.length <= 1) return;
                e.stopPropagation();
                dropdown.classList.toggle('hidden');
            });
            
            document.addEventListener('click', (e) => {
                if (!selectBtn.contains(e.target) && !dropdown.contains(e.target)) {
                    dropdown.classList.add('hidden');
                }
            });
        }
        
        // Search
        const searchInput = document.getElementById('wallet-client-search');
        let debounce;
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                clearTimeout(debounce);
                debounce = setTimeout(() => handleWalletSearch(e.target.value), 400);
            });
            // Click outside suggestions
             document.addEventListener('click', (e) => {
                const sugg = document.getElementById('wallet-search-suggestions');
                if (sugg && !sugg.contains(e.target) && e.target !== searchInput) {
                    sugg.classList.add('hidden');
                }
            });
        }
        
        // Modal Actions
        const modalCancel = document.getElementById('wallet-modal-cancel-btn');
        const modalClose = document.getElementById('wallet-modal-close-btn');
        if(modalCancel) modalCancel.onclick = () => document.getElementById('wallet-client-modal').classList.add('hidden');
        if(modalClose) modalClose.onclick = () => document.getElementById('wallet-client-modal').classList.add('hidden');
    }

    window.renderWalletView = function() {
        initWalletView();
        
        // Populate Promoters if empty
        if (walletState.promoters.length === 0) {
             const role = (window.userRole || '').trim().toUpperCase();
             const hierarchy = embeddedData.hierarchy || [];
             const myPromoters = new Set();
             let isManager = (role === 'ADM');

             hierarchy.forEach(h => {
                 const c = (h.cod_coord||'').trim().toUpperCase();
                 const cc = (h.cod_cocoord||'').trim().toUpperCase();
                 const p = (h.cod_promotor||'').trim().toUpperCase();
                 const pName = h.nome_promotor || p;

                 if (role === 'ADM' || c === role || cc === role) {
                     if (role !== 'ADM') isManager = true;
                     if (p) myPromoters.add(JSON.stringify({ code: p, name: pName }));
                 } else if (p === role) {
                     myPromoters.add(JSON.stringify({ code: p, name: pName }));
                 }
            });
            
            walletState.promoters = Array.from(myPromoters).map(s => JSON.parse(s)).sort((a,b) => a.name.localeCompare(b.name));
            walletState.canEdit = isManager;
            
            // UI Toggle based on permission
            const searchContainer = document.getElementById('wallet-search-container');
            if (searchContainer) {
                if (walletState.canEdit) searchContainer.classList.remove('hidden');
                else searchContainer.classList.add('hidden');
            }
            
            // Build Dropdown
            const dropdown = document.getElementById('wallet-promoter-dropdown');
            if (dropdown) {
                dropdown.innerHTML = '';
                walletState.promoters.forEach(p => {
                     const div = document.createElement('div');
                     div.className = 'px-4 py-2 hover:bg-slate-700 cursor-pointer text-sm text-slate-300 hover:text-white border-b border-slate-700/50 last:border-0';
                     div.textContent = `${p.code} - ${p.name}`;
                     div.onclick = () => {
                         selectWalletPromoter(p.code, p.name);
                         dropdown.classList.add('hidden');
                     };
                     dropdown.appendChild(div);
                });
            }
            
            // Auto Select
            if (walletState.promoters.length === 1) {
                selectWalletPromoter(walletState.promoters[0].code, walletState.promoters[0].name);
                const btn = document.getElementById('wallet-promoter-select-btn');
                if(btn) {
                    btn.classList.add('opacity-75', 'cursor-default');
                    const svg = btn.querySelector('svg');
                    if(svg) svg.classList.add('hidden');
                }
            } else if (walletState.promoters.length > 0) {
                 if (!walletState.selectedPromoter) {
                     // Optionally select first
                 }
            }
        }
        
        renderWalletTable();
    }
    
    window.selectWalletPromoter = async function(code, name) {
        walletState.selectedPromoter = code;
        const txt = document.getElementById('wallet-promoter-select-text');
        const btn = document.getElementById('wallet-promoter-select-btn');
        
        if (code) {
             if(txt) txt.textContent = `${code} - ${name}`;
             
             // Inject Clear Icon if not exists
             let clearIcon = document.getElementById('wallet-promoter-clear-icon');
             if (!clearIcon && btn) {
                 clearIcon = document.createElement('div');
                 clearIcon.id = 'wallet-promoter-clear-icon';
                 clearIcon.className = 'p-1 hover:bg-slate-700 rounded-full cursor-pointer mr-2 transition-colors';
                 clearIcon.innerHTML = `<svg class="w-4 h-4 text-slate-400 hover:text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>`;
                 
                 clearIcon.onclick = (e) => {
                     e.stopPropagation();
                     selectWalletPromoter(null, null);
                 };
                 
                 // Insert before the arrow icon
                 const arrow = btn.querySelector('svg:not(#wallet-promoter-clear-icon svg)');
                 if (arrow) btn.insertBefore(clearIcon, arrow);
                 else btn.appendChild(clearIcon);
             }
        } else {
             if(txt) txt.textContent = 'Selecione...';
             const clearIcon = document.getElementById('wallet-promoter-clear-icon');
             if (clearIcon) clearIcon.remove();
        }
        
        // --- Fetch Missing Clients Logic ---
        if (code) {
            const targetPromoter = String(code).trim().toUpperCase();
            const clientCodes = [];
            
            // 1. Identify all clients linked to this promoter in the map
            if (embeddedData.clientPromoters) {
                embeddedData.clientPromoters.forEach(cp => {
                    if (cp.promoter_code && String(cp.promoter_code).trim().toUpperCase() === targetPromoter) {
                        clientCodes.push(normalizeKey(cp.client_code));
                    }
                });
            }
            
            if (clientCodes.length > 0) {
                // 2. Identify which are missing from local embeddedData.clients
                const dataset = embeddedData.clients;
                const existingCodes = new Set();
                const isColumnar = dataset && dataset.values && dataset.columns;
                
                if (isColumnar) {
                    const col = dataset.values['Código'] || dataset.values['CODIGO_CLIENTE'] || [];
                    const len = dataset.length || col.length || 0;
                    for(let i=0; i<len; i++) existingCodes.add(normalizeKey(col[i]));
                } else if (Array.isArray(dataset)) {
                    dataset.forEach(c => existingCodes.add(normalizeKey(c['Código'] || c['codigo_cliente'])));
                }
                
                const missing = clientCodes.filter(c => !existingCodes.has(c));
                
                if (missing.length > 0) {
                    const badge = document.getElementById('wallet-count-badge');
                    if(badge) badge.textContent = '...';
                    
                    try {
                        // 3. Fetch missing clients
                        const { data, error } = await window.supabaseClient
                            .from('data_clients')
                            .select('*')
                            .in('codigo_cliente', missing);
                            
                        if (!error && data && data.length > 0) {
                            // 4. Inject into embeddedData.clients
                            data.forEach(newClient => {
                                // Double check uniqueness before push (race condition)
                                if (existingCodes.has(normalizeKey(newClient.codigo_cliente))) return;
                                
                                const mapped = {
                                     'Código': newClient.codigo_cliente,
                                     'Fantasia': newClient.fantasia,
                                     'Razão Social': newClient.razaosocial,
                                     'CNPJ/CPF': newClient.cnpj_cpf,
                                     'Cidade': newClient.cidade,
                                     'PROMOTOR': code // Use the selected promoter code
                                 };
                                 
                                 if (isColumnar) {
                                     dataset.columns.forEach(colName => {
                                         let val = '';
                                         const c = colName.toUpperCase();
                                         if(c === 'CÓDIGO' || c === 'CODIGO_CLIENTE') val = newClient.codigo_cliente;
                                         else if(c === 'FANTASIA' || c === 'NOMECLIENTE') val = newClient.fantasia;
                                         else if(c === 'RAZÃO SOCIAL' || c === 'RAZAOSOCIAL' || c === 'RAZAO') val = newClient.razaosocial;
                                         else if(c === 'CNPJ/CPF' || c === 'CNPJ') val = newClient.cnpj_cpf;
                                         else if(c === 'CIDADE') val = newClient.cidade;
                                         else if(c === 'PROMOTOR') val = code;
                                         else if(c === 'RCA1' || c === 'RCA 1') val = newClient.rca1;
                                         
                                         if(dataset.values[colName]) dataset.values[colName].push(val);
                                     });
                                     dataset.length++;
                                 } else if (Array.isArray(dataset)) {
                                     dataset.push(mapped);
                                 }
                                 existingCodes.add(normalizeKey(newClient.codigo_cliente));
                            });
                        }
                    } catch (e) {
                        console.error("Erro ao buscar clientes faltantes:", e);
                    }
                }
            }
        }
        
        renderWalletTable();
    }
    
    function renderWalletTable() {
        const promoter = walletState.selectedPromoter;
        const tbody = document.getElementById('wallet-table-body');
        const empty = document.getElementById('wallet-empty-state');
        const badge = document.getElementById('wallet-count-badge');
        
        // Toggle Action Header
        const actionHeader = document.getElementById('wallet-table-action-header');
        if (actionHeader) {
            if (walletState.canEdit) actionHeader.classList.remove('hidden');
            else actionHeader.classList.add('hidden');
        }
        
        if (!tbody) return;
        tbody.innerHTML = '';
        
        const dataset = embeddedData.clients;
        const isColumnar = dataset && dataset.values && dataset.columns;
        const len = dataset.length || 0;
        
        const clientPromoterMap = new Map();
        
        // Normalize selected promoter for comparison
        const targetPromoter = String(promoter).trim().toUpperCase();

        if (embeddedData.clientPromoters) {
             embeddedData.clientPromoters.forEach(cp => {
                 // Store normalized promoter code in map
                 if (cp.promoter_code) {
                    clientPromoterMap.set(normalizeKey(cp.client_code), String(cp.promoter_code).trim().toUpperCase());
                 }
             });
        }
        
        let count = 0;
        const fragment = document.createDocumentFragment();
        
        for(let i=0; i<len; i++) {
             let rowCode, rowFantasia, rowRazao, rowCnpj;

             if (isColumnar) {
                 rowCode = dataset.values['Código']?.[i] || dataset.values['CODIGO_CLIENTE']?.[i] || dataset.values['codigo_cliente']?.[i];
                 rowFantasia = dataset.values['Fantasia']?.[i] || dataset.values['FANTASIA']?.[i] || dataset.values['fantasia']?.[i] || dataset.values['NOMECLIENTE']?.[i] || dataset.values['nomeCliente']?.[i];
                 rowRazao = dataset.values['Razão Social']?.[i] || dataset.values['RAZAOSOCIAL']?.[i] || dataset.values['razaoSocial']?.[i];
                 rowCnpj = dataset.values['CNPJ/CPF']?.[i] || dataset.values['CNPJ']?.[i] || dataset.values['cnpj_cpf']?.[i];
             } else if (Array.isArray(dataset)) {
                 const item = dataset[i];
                 if (!item) continue;
                 rowCode = item['Código'] || item['codigo_cliente'] || item['CODIGO_CLIENTE'];
                 rowFantasia = item['Fantasia'] || item['fantasia'] || item['FANTASIA'] || item['nomeCliente'] || item['NOMECLIENTE'];
                 rowRazao = item['Razão Social'] || item['razaosocial'] || item['razaoSocial'] || item['RAZAOSOCIAL'];
                 rowCnpj = item['CNPJ/CPF'] || item['cnpj_cpf'] || item['CNPJ'];
             } else {
                 continue; 
             }
             
             if (!rowCode) continue;
             
             const code = normalizeKey(rowCode);
             const linkedPromoter = clientPromoterMap.get(code);
             
             // Compare normalized values OR show all if no promoter selected
             if (!promoter || linkedPromoter === targetPromoter) {
                 count++;
                 const tr = document.createElement('tr');
                 tr.className = 'hover:bg-slate-800/50 transition-colors border-b border-slate-800/50';
                 tr.innerHTML = `
                    <td class="px-6 py-4 font-mono text-xs text-slate-400">${code}</td>
                    <td class="px-6 py-4">
                        <div class="text-sm font-bold text-white">${rowFantasia || 'N/A'}</div>
                        <div class="text-xs text-slate-500">${rowRazao || ''}</div>
                    </td>
                    <td class="px-6 py-4 text-xs text-slate-400">${rowCnpj || ''}</td>
                    <td class="px-6 py-4 text-center">
                         <button class="p-2 text-blue-400 hover:text-white hover:bg-blue-600 rounded-lg transition-all" onclick="openWalletClientModal('${code}')">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
                         </button>
                    </td>
                    ${walletState.canEdit ? `
                    <td class="px-6 py-4 text-center">
                        <button class="text-red-500 hover:text-red-400 hover:bg-red-500/10 p-2 rounded-lg transition-colors" onclick="handleWalletAction('${code}', 'remove')">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg>
                        </button>
                    </td>` : ''}
                 `;
                 fragment.appendChild(tr);
             }
        }
        
        tbody.appendChild(fragment);
        if (badge) badge.textContent = count;
        
        if (count === 0) empty.classList.remove('hidden');
        else empty.classList.add('hidden');
    }
    
    async function handleWalletSearch(query) {
        const sugg = document.getElementById('wallet-search-suggestions');
        if (!query || query.length < 3) {
            sugg.classList.add('hidden');
            return;
        }
        
        const cleanQ = query.replace(/[^a-zA-Z0-9]/g, '');
        let filter = `codigo_cliente.ilike.%${query}%,fantasia.ilike.%${query}%,razaosocial.ilike.%${query}%,cnpj_cpf.ilike.%${query}%`;
        
        if (cleanQ.length > 0 && cleanQ !== query) {
             filter += `,cnpj_cpf.ilike.%${cleanQ}%,codigo_cliente.ilike.%${cleanQ}%`;
        }

        const { data, error } = await window.supabaseClient
            .from('data_clients')
            .select('*')
            .or(filter)
            .limit(10);
            
        if (error || !data || data.length === 0) {
            sugg.classList.add('hidden');
            return;
        }
        
        sugg.innerHTML = '';
        data.forEach(c => {
            const div = document.createElement('div');
            div.className = 'px-4 py-3 border-b border-slate-700 hover:bg-slate-700 cursor-pointer flex justify-between items-center group';
            div.innerHTML = `
                <div>
                    <div class="text-sm font-bold text-white group-hover:text-blue-300 transition-colors">
                        <span class="font-mono text-slate-400 mr-2">${c.codigo_cliente}</span>
                        ${c.fantasia || c.razaosocial}
                    </div>
                    <div class="text-xs text-slate-500">${c.cidade || ''} • ${c.cnpj_cpf || ''}</div>
                </div>
                 <div class="p-2 bg-slate-800 rounded-full group-hover:bg-blue-600 transition-colors text-slate-400 group-hover:text-white">
                     <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
                </div>
            `;
            div.onclick = () => {
                sugg.classList.add('hidden');
                document.getElementById('wallet-client-search').value = '';
                openWalletClientModal(c.codigo_cliente, c);
            };
            sugg.appendChild(div);
        });
        sugg.classList.remove('hidden');
    }
    
    window.openWalletClientModal = async function(clientCode, clientData = null) {
        let client = clientData;
        if (!client) {
             const { data } = await window.supabaseClient.from('data_clients').select('*').eq('codigo_cliente', clientCode).single();
             client = data;
        }
        if (!client) return;
        
        const modal = document.getElementById('wallet-client-modal');
        document.getElementById('wallet-modal-code').textContent = client.codigo_cliente;
        document.getElementById('wallet-modal-cnpj').textContent = client.cnpj_cpf;
        document.getElementById('wallet-modal-razao').textContent = client.razaosocial;
        document.getElementById('wallet-modal-fantasia').textContent = client.fantasia;
        const bairro = client.bairro || client.BAIRRO || '';
        const cidade = client.cidade || client.CIDADE || '';
        document.getElementById('wallet-modal-city').textContent = (bairro && bairro !== 'N/A') ? `${bairro} - ${cidade}` : cidade;
        
        const statusArea = document.getElementById('wallet-modal-status-area');
        const statusTitle = document.getElementById('wallet-modal-status-title');
        const statusMsg = document.getElementById('wallet-modal-status-msg');
        const btn = document.getElementById('wallet-modal-action-btn');
        const btnText = document.getElementById('wallet-modal-action-text');
        
        let currentOwner = null;
        if (embeddedData.clientPromoters) {
             const match = embeddedData.clientPromoters.find(cp => normalizeKey(cp.client_code) === normalizeKey(client.codigo_cliente));
             if (match) currentOwner = match.promoter_code;
        }
        
        const myPromoter = walletState.selectedPromoter;
        const role = (window.userRole || '').trim().toUpperCase();
        
        // Normalize for comparison
        const normCurrent = currentOwner ? String(currentOwner).trim().toUpperCase() : null;
        const normMy = myPromoter ? String(myPromoter).trim().toUpperCase() : null;

        // Reset Status Area (Fix for Stale State)
        statusArea.classList.remove('hidden');
        statusTitle.textContent = '';
        statusMsg.textContent = '';
        statusArea.className = 'mt-4 p-4 rounded-lg hidden';
        btn.onclick = null;
        btn.disabled = false;
        
        let isPromoterOnly = true;
        const h = embeddedData.hierarchy || [];
        const me = h.find(x => 
            (x.cod_coord && x.cod_coord.trim().toUpperCase() === role) || 
            (x.cod_cocoord && x.cod_cocoord.trim().toUpperCase() === role)
        );
        if (me || role === 'ADM') isPromoterOnly = false;
        
        if (!myPromoter) {
            btn.classList.add('hidden');
            statusArea.classList.remove('hidden'); // Ensure visible
            if (currentOwner) {
                statusArea.className = 'mt-4 p-4 rounded-lg bg-orange-500/10 border border-orange-500/30';
                statusTitle.textContent = 'Cadastrado';
                statusTitle.className = 'text-sm font-bold text-orange-400 mb-1';
                statusMsg.textContent = `Pertence a: ${currentOwner}`;
            } else {
                statusArea.className = 'mt-4 p-4 rounded-lg bg-slate-700/50 border border-slate-600/50';
                statusTitle.textContent = 'Não Cadastrado';
                statusTitle.className = 'text-sm font-bold text-slate-400 mb-1';
                statusMsg.textContent = 'Este cliente não pertence a nenhuma carteira.';
            }
        } else {
             btn.classList.remove('hidden');
             statusArea.classList.remove('hidden');

             if (normCurrent && normMy && normCurrent === normMy) {
                 statusArea.className = 'mt-4 p-4 rounded-lg bg-green-500/10 border border-green-500/30';
                 statusTitle.textContent = 'Cliente na Carteira';
                 statusTitle.className = 'text-sm font-bold text-green-400 mb-1';
                 statusMsg.textContent = 'Este cliente já pertence à carteira selecionada.';
                 
                 btnText.textContent = 'Remover';
                 btn.className = 'px-4 py-2 bg-red-600 hover:bg-red-500 text-white rounded-lg font-bold shadow-lg transition-colors flex items-center gap-2';
                 btn.onclick = () => handleWalletAction(client.codigo_cliente, 'remove');
                 
             } else if (currentOwner) {
                 statusArea.className = 'mt-4 p-4 rounded-lg bg-orange-500/10 border border-orange-500/30';
                 statusTitle.textContent = 'Conflito';
                 statusTitle.className = 'text-sm font-bold text-orange-400 mb-1';
                 statusMsg.textContent = `Pertence a: ${currentOwner}. Transferir?`;
                 
                 btnText.textContent = 'Transferir';
                 btn.className = 'px-4 py-2 bg-orange-600 hover:bg-orange-500 text-white rounded-lg font-bold shadow-lg transition-colors flex items-center gap-2';
                 btn.onclick = () => handleWalletAction(client.codigo_cliente, 'upsert');

                 // Co-Coordinator Restriction Check: Prevent cross-base transfers
                 // Only allows transfer if the current owner belongs to the same Co-Coordinator
                 if (userHierarchyContext && userHierarchyContext.role === 'cocoord' && optimizedData) {
                     const ownerNode = optimizedData.hierarchyMap.get(String(currentOwner).trim().toUpperCase());
                     const myCocoord = userHierarchyContext.cocoord;

                     if (ownerNode && ownerNode.cocoord && ownerNode.cocoord.code) {
                         const ownerCocoord = ownerNode.cocoord.code;
                         if (String(ownerCocoord).trim() !== String(myCocoord).trim()) {
                             btn.disabled = true;
                             btnText.textContent = 'Não Permitido';
                             btn.className = 'px-4 py-2 bg-slate-700 text-slate-400 rounded-lg font-bold cursor-not-allowed flex items-center gap-2';
                             statusMsg.textContent += ' (Bloqueado: Cliente de outra base)';
                         }
                     }
                 }
                 
             } else {
                 statusArea.className = 'mt-4 p-4 rounded-lg bg-slate-700 border border-slate-600';
                 statusTitle.textContent = 'Disponível';
                 statusTitle.className = 'text-sm font-bold text-slate-300 mb-1';
                 statusMsg.textContent = 'Sem vínculo atual.';
                 
                 btnText.textContent = 'Adicionar';
                 btn.className = 'px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg font-bold shadow-lg transition-colors flex items-center gap-2';
                 btn.onclick = () => handleWalletAction(client.codigo_cliente, 'upsert');
             }
        }
        
        if (isPromoterOnly) {
             btn.classList.add('hidden');
             statusMsg.textContent += ' (Modo Leitura)';
        }
        
        modal.classList.remove('hidden');
    }
    
    window.handleWalletAction = async function(clientCode, action) {
         const promoter = walletState.selectedPromoter;
         if (!promoter) return;
         
         const btn = document.getElementById('wallet-modal-action-btn');
         const txt = document.getElementById('wallet-modal-action-text');
         const oldTxt = txt.textContent;
         btn.disabled = true;
         txt.textContent = '...';
         
         try {
             // Safety check for embeddedData
             if (!embeddedData.clientPromoters) embeddedData.clientPromoters = [];

             if (action === 'upsert') {
                 const { error } = await window.supabaseClient.from('data_client_promoters')
                    .upsert({ client_code: clientCode, promoter_code: promoter }, { onConflict: 'client_code' });
                 if(error) throw error;
                 
                 const idx = embeddedData.clientPromoters.findIndex(cp => normalizeKey(cp.client_code) === normalizeKey(clientCode));
                 if(idx >= 0) embeddedData.clientPromoters[idx].promoter_code = promoter;
                 else embeddedData.clientPromoters.push({ client_code: clientCode, promoter_code: promoter });
                 
                 // Ensure client exists in local dataset (for display)
                 const dataset = allClientsData;
                 let exists = false;
                 if (dataset instanceof ColumnarDataset) {
                     const col = dataset._data['Código'] || dataset._data['CODIGO_CLIENTE'];
                     if (col && col.includes(normalizeKey(clientCode))) exists = true;
                 } else {
                     if (dataset.find(c => normalizeKey(c['Código'] || c['codigo_cliente']) === normalizeKey(clientCode))) exists = true;
                 }
                 
                 if (!exists) {
                     // Fetch and inject
                     const { data: newClient } = await window.supabaseClient.from('data_clients').select('*').eq('codigo_cliente', clientCode).single();
                     if (newClient) {
                         const mapped = {
                             'Código': newClient.codigo_cliente,
                             'Fantasia': newClient.fantasia,
                             'Razão Social': newClient.razaosocial,
                             'CNPJ/CPF': newClient.cnpj_cpf,
                             'Cidade': newClient.cidade,
                             'PROMOTOR': promoter
                         };
                         
                         if (dataset instanceof ColumnarDataset) {
                             dataset.columns.forEach(col => {
                                 let val = '';
                                 const c = col.toUpperCase();
                                 if(c === 'CÓDIGO' || c === 'CODIGO_CLIENTE') val = newClient.codigo_cliente;
                                 else if(c === 'FANTASIA' || c === 'NOMECLIENTE') val = newClient.fantasia;
                                 else if(c === 'RAZÃO SOCIAL' || c === 'RAZAOSOCIAL' || c === 'RAZAO') val = newClient.razaosocial;
                                 else if(c === 'CNPJ/CPF' || c === 'CNPJ') val = newClient.cnpj_cpf;
                                 else if(c === 'CIDADE') val = newClient.cidade;
                                 
                                 if(dataset.values[col]) dataset.values[col].push(val);
                             });
                             dataset.length++;
                         } else {
                             dataset.push(mapped);
                         }
                     }
                 }
                 
             } else {
                 const { error } = await window.supabaseClient.from('data_client_promoters').delete().eq('client_code', clientCode);
                 if(error) throw error;
                 
                 const idx = embeddedData.clientPromoters.findIndex(cp => normalizeKey(cp.client_code) === normalizeKey(clientCode));
                 if(idx >= 0) embeddedData.clientPromoters.splice(idx, 1);
             }
             
             document.getElementById('wallet-client-modal').classList.add('hidden');
             renderWalletTable();
             
         } catch (e) {
             console.error(e);
             alert('Erro: ' + e.message);
         } finally {
             btn.disabled = false;
             txt.textContent = oldTxt;
         }
    }
    
    // Auto-init User Menu on load if ready (for Navbar)
    if (document.readyState === 'complete' || document.readyState === 'interactive') {
        initWalletView();
        updateNavigationVisibility();
    }

    // Expose renderView globally for HTML onclick handlers
    window.renderView = renderView;

})();
