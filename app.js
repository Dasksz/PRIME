(function() {
if (window.primeGoalsAppInitialized) return;
window.primeGoalsAppInitialized = true;
const embeddedData = window.embeddedData;

class ColumnarDataset {
            constructor(columnarData) {
                this.columns = columnarData.columns;
                this._data = columnarData.values; // Renamed to avoid shadowing values() method
                this.length = columnarData.length;
                this._overrides = new Map(); // Stores mutations: Map<index, Object>
                this._rowCache = new Array(this.length); // Caches row proxies to prevent reallocation

                // Shared Proxy Handler to avoid allocation per row
                this._proxyHandler = {
                    get: (target, prop) => {
                        if (prop === 'toJSON') return () => "ColumnarRowProxy"; // Debug help

                        const index = target.index;
                        const ds = target.dataset;

                        // 1. Check overrides first (mutations)
                        const ov = ds._overrides.get(index);
                        if (ov && prop in ov) {
                            return ov[prop];
                        }

                        // 2. Check columnar data (lazy read)
                        const colData = ds._data[prop];
                        if (colData) {
                            return colData[index];
                        }

                        return undefined;
                    },

                    set: (target, prop, value) => {
                        const index = target.index;
                        const ds = target.dataset;

                        let ov = ds._overrides.get(index);
                        if (!ov) {
                            ov = {};
                            ds._overrides.set(index, ov);
                        }
                        ov[prop] = value;
                        return true;
                    },

                    ownKeys: (target) => {
                        const index = target.index;
                        const ds = target.dataset;
                        const ov = ds._overrides.get(index);

                        // Return all original columns plus any new keys added via mutation
                        if (ov) {
                            const keys = new Set(ds.columns);
                            Object.keys(ov).forEach(k => keys.add(k));
                            return Array.from(keys);
                        }
                        return ds.columns;
                    },

                    getOwnPropertyDescriptor: (target, prop) => {
                        const index = target.index;
                        const ds = target.dataset;
                        const ov = ds._overrides.get(index);

                        // Check overrides or values to confirm existence
                        if ((ov && prop in ov) || (ds._data && ds._data[prop])) {
                            return { enumerable: true, configurable: true, writable: true };
                        }
                        return undefined;
                    },

                    has: (target, prop) => {
                        const index = target.index;
                        const ds = target.dataset;
                        const ov = ds._overrides.get(index);
                        return (ov && prop in ov) || (ds._data && prop in ds._data);
                    }
                };

                // Lightweight Target Class to avoid object literal allocation
                this.RowTarget = class RowTarget {
                    constructor(index, dataset) {
                        this.index = index;
                        this.dataset = dataset;
                    }
                };
            }

            get(index) {
                if (index < 0 || index >= this.length) return undefined;
                // Use cached row proxy if available
                if (this._rowCache[index] !== undefined) {
                    return this._rowCache[index];
                }
                // Instantiate new proxy and cache it
                const proxy = new Proxy(new this.RowTarget(index, this), this._proxyHandler);
                this._rowCache[index] = proxy;
                return proxy;
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

function parseDate(dateString) {
            if (!dateString) return null;

            if (typeof dateString === 'string') {
                const len = dateString.length;

                // Fast paths for fixed-length dates
                if (len === 10) {
                    // Check for YYYY-MM-DD
                    if (dateString.charCodeAt(4) === 45 && dateString.charCodeAt(7) === 45) { // '-' is 45
                        const c0 = dateString.charCodeAt(0), c1 = dateString.charCodeAt(1), c2 = dateString.charCodeAt(2), c3 = dateString.charCodeAt(3);
                        const c5 = dateString.charCodeAt(5), c6 = dateString.charCodeAt(6);
                        const c8 = dateString.charCodeAt(8), c9 = dateString.charCodeAt(9);
                        if (c0 >= 48 && c0 <= 57 && c1 >= 48 && c1 <= 57 && c2 >= 48 && c2 <= 57 && c3 >= 48 && c3 <= 57 &&
                            c5 >= 48 && c5 <= 57 && c6 >= 48 && c6 <= 57 && c8 >= 48 && c8 <= 57 && c9 >= 48 && c9 <= 57) {
                            const y = (c0 - 48) * 1000 + (c1 - 48) * 100 + (c2 - 48) * 10 + (c3 - 48);
                            const m = (c5 - 48) * 10 + (c6 - 48);
                            const d = (c8 - 48) * 10 + (c9 - 48);
                            if (m >= 1 && m <= 12 && d >= 1 && d <= 31) {
                                return new Date(Date.UTC(y, m - 1, d));
                            }
                        }
                    }
                    // Check for DD/MM/YYYY
                    else if (dateString.charCodeAt(2) === 47 && dateString.charCodeAt(5) === 47) { // '/' is 47
                        const c0 = dateString.charCodeAt(0), c1 = dateString.charCodeAt(1);
                        const c3 = dateString.charCodeAt(3), c4 = dateString.charCodeAt(4);
                        const c6 = dateString.charCodeAt(6), c7 = dateString.charCodeAt(7), c8 = dateString.charCodeAt(8), c9 = dateString.charCodeAt(9);
                        if (c0 >= 48 && c0 <= 57 && c1 >= 48 && c1 <= 57 && c3 >= 48 && c3 <= 57 && c4 >= 48 && c4 <= 57 &&
                            c6 >= 48 && c6 <= 57 && c7 >= 48 && c7 <= 57 && c8 >= 48 && c8 <= 57 && c9 >= 48 && c9 <= 57) {
                            const d = (c0 - 48) * 10 + (c1 - 48);
                            const m = (c3 - 48) * 10 + (c4 - 48);
                            const y = (c6 - 48) * 1000 + (c7 - 48) * 100 + (c8 - 48) * 10 + (c9 - 48);
                            if (m >= 1 && m <= 12 && d >= 1 && d <= 31) {
                                return new Date(Date.UTC(y, m - 1, d));
                            }
                        }
                    }
                }

                // Fast path for ISO 'YYYY-MM-DDTHH:mm:ss...' strings
                if (len >= 19 && dateString.charCodeAt(4) === 45 && dateString.charCodeAt(7) === 45 && dateString.charCodeAt(10) === 84) { // 'T' is 84
                     const isoString = dateString.charCodeAt(len - 1) === 90 ? dateString : dateString + 'Z'; // 'Z' is 90
                     const isoDate = new Date(isoString);
                     if (!isNaN(isoDate.getTime())) return isoDate;
                }

                if (dateString.includes('T') || dateString.includes('-')) {
                    const isoString = dateString.endsWith('Z') ? dateString : dateString + 'Z';
                    const isoDate = new Date(isoString);
                    if (!isNaN(isoDate.getTime())) {
                        return isoDate;
                    }
                }

                // Fallback para outros formatos
                const genericDate = new Date(dateString);
                if (!isNaN(genericDate.getTime())) {
                    return genericDate;
                }

                return null;
            }

            if (typeof dateString === 'number') {
                if (dateString < 100000) return new Date(Math.round((dateString - 25569) * 86400 * 1000));
                return new Date(dateString);
            }

            if (dateString instanceof Date) {
                return !isNaN(dateString.getTime()) ? dateString : null;
            }

            return null;
        }

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

                    if (index % 100 === 0 && performance.now() - start >= 40) { // Check budget to avoid long tasks (>50ms)
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

let allSalesData, allHistoryData, allClientsData;

const SANITIZE_FORBIDDEN_SET = new Set(['SUPERV', 'CODUSUR', 'CODSUPERVISOR', 'NOME', 'CODCLI', 'PRODUTO', 'DESCRICAO', 'FORNECEDOR', 'OBSERVACAOFOR', 'CODFOR', 'QTVENDA', 'VLVENDA', 'VLBONIFIC', 'TOTPESOLIQ', 'ESTOQUEUNIT', 'TIPOVENDA', 'FILIAL', 'ESTOQUECX', 'SUPERVISOR']);

function sanitizeData(data) {
            if (!data) return [];

            // Check if it's a ColumnarDataset proxy or regular array.
            // If it's a ColumnarDataset, we can't easily filter in-place without rebuilding.
            // However, ColumnarDataset usually proxies access.
            // Since we're trying to fix garbage, it's safer to check if we can filter.

            if (Array.isArray(data)) {
                const len = data.length;
                const result = [];

                for (let i = 0; i < len; i++) {
                    const item = data[i];
                    if (!item) continue;

                    // Optimization: Heuristic check for CODUSUR (Seller Code)
                    // If it starts with a digit, it's very likely a valid data row, not a header.
                    const codUsur = item.CODUSUR;
                    if (codUsur) {
                        if (typeof codUsur === 'number') {
                            result.push(item);
                            continue;
                        }
                        if (typeof codUsur === 'string') {
                            const firstChar = codUsur.charCodeAt(0);
                            // Check for digits 0-9 (ASCII 48-57)
                            if (firstChar >= 48 && firstChar <= 57) {
                                result.push(item);
                                continue;
                            }
                        }
                    }

                    // Slow Path: Check against forbidden headers
                    const superv = String(item.SUPERV || '').trim().toUpperCase();
                    if (SANITIZE_FORBIDDEN_SET.has(superv)) continue;

                    const nome = String(item.NOME || '').trim().toUpperCase();
                    if (SANITIZE_FORBIDDEN_SET.has(nome)) continue;

                    const codUsurStr = String(item.CODUSUR || '').trim().toUpperCase();
                    if (SANITIZE_FORBIDDEN_SET.has(codUsurStr)) continue;

                    result.push(item);
                }
                return result;
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

const pepsicoSuppliersMap = new Map();

const extractPepsico = (data) => {
            if (!data) return;
            const isCol = data instanceof ColumnarDataset;
            const len = data.length;
            const values = isCol ? data._data : null;
            for (let i = 0; i < len; i++) {
                let p, c, n;
                if (isCol && values) {
                    p = values['OBSERVACAOFOR'] ? values['OBSERVACAOFOR'][i] : null;
                    c = values['CODFOR'] ? values['CODFOR'][i] : null;
                    n = values['FORNECEDOR'] ? values['FORNECEDOR'][i] : null;
                } else {
                    const s = data[i];
                    p = s.OBSERVACAOFOR; c = s.CODFOR; n = s.FORNECEDOR;
                }
                if (!p || p === '0' || p === '00' || p === 'N/A') {
                    p = String(n || '').toUpperCase().includes('PEPSICO') ? 'PEPSICO' : 'MULTIMARCAS';
                }
                if (p === 'PEPSICO' && c) pepsicoSuppliersMap.set(String(c), n);
            }
        };

extractPepsico(allSalesData);

extractPepsico(allHistoryData);

pepsicoSuppliersSource = Array.from(pepsicoSuppliersMap).map(([CODFOR, FORNECEDOR]) => ({ CODFOR, FORNECEDOR }));

let aggregatedOrders = embeddedData.byOrder;

const clientMapForKPIs = new Map();

for(let i=0; i<allClientsData.length; i++) {
            const c = allClientsData instanceof ColumnarDataset ? allClientsData.get(i) : allClientsData[i];
            clientMapForKPIs.set(String(c['Código'] || c['codigo_cliente']), c);
        }

const productDetailsMap = new Map(Object.entries(embeddedData.productDetails || {}));

if (embeddedData.lastSaleDate) {
            const ts = parseInt(embeddedData.lastSaleDate);
            if (!isNaN(ts) && ts > 0) {
                lastSaleDate = new Date(ts);
            }
        }

const clientsWithSalesThisMonth = new Set();

if (allSalesData instanceof ColumnarDataset && allSalesData._data && allSalesData._data['CODCLI']) {
             const codCliCol = allSalesData._data['CODCLI'];
             const overrides = allSalesData._overrides;

             // Fast path: No overrides
             if (!overrides || overrides.size === 0) {
                 for(let i=0; i<allSalesData.length; i++) {
                     clientsWithSalesThisMonth.add(codCliCol[i]);
                 }
             } else {
                 // Slow path: Check overrides
                 for(let i=0; i<allSalesData.length; i++) {
                     const ov = overrides.get(i);
                     if (ov && 'CODCLI' in ov) {
                         clientsWithSalesThisMonth.add(ov.CODCLI);
                     } else {
                         clientsWithSalesThisMonth.add(codCliCol[i]);
                     }
                 }
             }
        } else {
            // Fallback
            for(let i=0; i<allSalesData.length; i++) {
                const s = allSalesData instanceof ColumnarDataset ? allSalesData.get(i) : allSalesData[i];
                clientsWithSalesThisMonth.add(s.CODCLI);
            }
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

let maxDateTs = 0;

if (allSalesData instanceof ColumnarDataset && allSalesData._data && allSalesData._data['DTPED']) {
            const dtpedCol = allSalesData._data['DTPED'];
            const overrides = allSalesData._overrides;

            // Fast Path: No Overrides (Typical Initial Load)
            if (!overrides || overrides.size === 0) {
                for (let i = 0; i < allSalesData.length; i++) {
                    const val = dtpedCol[i];
                    let ts = 0;
                    if (typeof val === 'number' && val > 1000000) {
                        ts = val;
                    } else {
                        const d = parseDate(val);
                        if (d && !isNaN(d)) ts = d.getTime();
                    }
                    if (ts > maxDateTs) maxDateTs = ts;
                }
            } else {
                // Slow Path: Check Overrides
                for (let i = 0; i < allSalesData.length; i++) {
                    const ov = overrides.get(i);
                    let val;
                    if (ov && 'DTPED' in ov) {
                        val = ov.DTPED;
                    } else {
                        val = dtpedCol[i];
                    }

                    let ts = 0;
                    if (typeof val === 'number' && val > 1000000) {
                        ts = val;
                    } else {
                        const d = parseDate(val);
                        if (d && !isNaN(d)) ts = d.getTime();
                    }
                    if (ts > maxDateTs) maxDateTs = ts;
                }
            }
        } else {
            // Fallback: Standard Access
            for (let i = 0; i < allSalesData.length; i++) {
                const s = allSalesData instanceof ColumnarDataset ? allSalesData.get(i) : allSalesData[i];
                let ts = 0;
                if (typeof s.DTPED === 'number' && s.DTPED > 1000000) {
                    ts = s.DTPED;
                } else {
                    const d = parseDate(s.DTPED);
                    if (d && !isNaN(d)) ts = d.getTime();
                }

                if (ts > maxDateTs) maxDateTs = ts;
            }
        }

const lastSaleDate = maxDateTs > 0 ? new Date(maxDateTs) : new Date();

lastSaleDate.setUTCHours(0,0,0,0);

let maxWorkingDaysStock = 0;

let customWorkingDaysStock = 0;

let clientCoordinatesMap = new Map();

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

let sellerDetailsMap = new Map();

function initializeOptimizedDataStructures() {
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
            optimizedData.rcaCodeByName = new Map();
            optimizedData.rcaNameByCode = new Map();
            optimizedData.supervisorCodeByName = new Map();
            optimizedData.productPastaMap = new Map();

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

                // --- OPTIMIZATION: Cache lowercase city for performance ---
                // Pre-calculate to avoid redundant .toLowerCase() in loops (e.g. processDatasetForIndices)
                client.cityLower = (client.cidade || client['Nome da Cidade'] || 'N/A').toLowerCase();

                if (rcas) {
                    for (let j = 0; j < rcas.length; j++) {
                        const rca = rcas[j];
                        if (rca) {
                            if (!optimizedData.clientsByRca.has(rca)) optimizedData.clientsByRca.set(rca, []);
                            optimizedData.clientsByRca.get(rca).push(client);
                        }
                    }
                }

                optimizedData.searchIndices.clients[i] = { code: codCli, nameLower: (client.nomeCliente || '').toLowerCase(), cityLower: client.cityLower };
            }

            const supervisorToRcaMap = new Map();
            const workingDaysSet = new Set();

            const processDatasetForIndices = (data, indexSet, dataMap, isHistory) => {
                const { bySupervisor, byRca, byPasta, bySupplier, byClient, byPosition, byRede, byTipoVenda, byProduct, byCity, byFilial } = indexSet;

                const isColumnar = data instanceof ColumnarDataset;
                // Use _data because .values is now a method
                const colValues = isColumnar ? data._data : null;
                // Pre-fetch columns for fast access
                const dtPedCol = isColumnar && colValues ? colValues['DTPED'] : null;

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
                    // --- OPTIMIZATION: Use cached lowercase city ---
                    const city = clientObj ? (clientObj.cityLower || (clientObj.cidade || clientObj['Nome da Cidade'] || 'N/A').toLowerCase()) : 'n/a';
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

                    const dtPed = dtPedCol ? dtPedCol[i] : getVal(i, 'DTPED');
                    if (dtPed) {
                        // dtPed is likely a number (timestamp).
                        // If it's a number, new Date(dtPed) works.
                        // If it's a string, parseDate(dtPed) (from local function or global?)
                        // Global parseDate handles numbers too.
                        const dateObj = (typeof dtPed === 'number') ? new Date(dtPed) : parseDate(dtPed);

                        if(dateObj && !isNaN(dateObj.getTime())) {
                            const dayOfWeek = dateObj.getUTCDay();
                            if (dayOfWeek >= 1 && dayOfWeek <= 5) {
                                // Optimized: Manual string construction avoids toISOString() overhead (~4x faster)
                                const y = dateObj.getUTCFullYear();
                                const m = dateObj.getUTCMonth() + 1;
                                const d = dateObj.getUTCDate();
                                const dateStr = `${y}-${m < 10 ? '0' + m : m}-${d < 10 ? '0' + d : d}`;
                                workingDaysSet.add(dateStr);
                            }
                        }
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

const goalsView = document.getElementById('goals-view');

const goalsGvContent = document.getElementById('goals-gv-content');

const goalsSvContent = document.getElementById('goals-sv-content');

const goalsGvTableBody = document.getElementById('goals-gv-table-body');

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

const viewState = { goals: { dirty: true } };

let goalsRenderId = 0;

let goalsSvRenderId = 0;

let currentGoalsSupplier = 'PEPSICO_ALL';

let currentGoalsBrand = null;

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

let goalsPosAdjustments = { 'ELMA_ALL': new Map(), 'FOODS_ALL': new Map(), 'PEPSICO_ALL': new Map(), '707': new Map(), '708': new Map(), '752': new Map(), '1119_TODDYNHO': new Map(), '1119_TODDY': new Map(), '1119_QUAKER_KEROCOCO': new Map() };

let goalsMixSaltyAdjustments = { 'PEPSICO_ALL': new Map(), 'ELMA_ALL': new Map(), 'FOODS_ALL': new Map() };

let goalsMixFoodsAdjustments = { 'PEPSICO_ALL': new Map(), 'ELMA_ALL': new Map(), 'FOODS_ALL': new Map() };

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
                // const codCliLocal = String(c['Código'] || c['codigo_cliente']);
                // if (rca1 === '53' && codCliLocal !== '12034') return false;
                // if (rca1 === '') return false; // Exclude INATIVOS
                return true;
            });

            // Optimization: Detect if history is columnar and IndexMap is available
            const isHistoryColumnar = optimizedData.historyById instanceof ColumnarDataset;
            const historyValues = isHistoryColumnar ? optimizedData.historyById._data : null;

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const clientHistoryIds = optimizedData.indices.history.byClient.get(codCli);

                // Temp accumulation for this client to ensure Positive Balance check
                const clientTotals = {}; // key -> { prevFat: 0, monthlyFat: Map<MonthKey, val> }

                if (clientHistoryIds) {
                    if (isHistoryColumnar) {
                        // Optimized Path: Use indices
                        clientHistoryIds.forEach(id => {
                            const idx = id;

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

let selectedGoalsSummarySellers = [];

const getFirstName = (fullName) => (fullName || '').split(' ')[0];

const MIX_SALTY_CATEGORIES = ['CHEETOS', 'DORITOS', 'FANDANGOS', 'RUFFLES', 'TORCIDA'];

const MIX_FOODS_CATEGORIES = ['TODDYNHO', 'TODDY ', 'QUAKER', 'KEROCOCO'];

let goalsSellerTargets = new Map();

window.goalsSellerTargets = goalsSellerTargets;

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
                return true; // return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
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
                        share = ((subCatMap ? subCatMap.get(subCat) : 0) || 0) / totalSellerHistory;
                    } else {
                        // Fallback: Even split across all clients and subcats
                        share = 1 / (clientCount * subCatCount);
                    }

                    if (isNaN(share) || !isFinite(share)) share = 0;
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
            const fmtMoney = "#,##0.00";
            const fmtVol = "#,##0.000";
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
            svColumns.forEach(c => colCellsForGrandTotal[c.id] = { fat: [], pos: [], vol: [], mix: [], avg: [], metaFat: [], metaPos: [], metaVol: [], metaMix: [] });

            currentGoalsSvData.forEach(sup => {
                const sellers = sup.sellers;
                const colCellsForSupTotal = {};
                svColumns.forEach(c => colCellsForSupTotal[c.id] = { fat: [], pos: [], vol: [], mix: [], avg: [], metaFat: [], metaPos: [], metaVol: [], metaMix: [] });

                sellers.forEach(seller => {
                    const rowData = [createCell(seller.code), createCell(seller.name)];

                    svColumns.forEach(col => {
                        const d = seller.data[col.id] || { metaFat: 0, metaVol: 0, metaPos: 0, avgVol: 0, avgMix: 0, metaMix: 0, avgFat: 0 };
                        const cIdx = colMap[col.id];
                        const excelRow = currentRow + 1;
                        const getColLet = (idx) => XLSX.utils.encode_col(idx);

                        // Highlight Logic
                        // Rule:
                        // Fat: Editable for Leafs (!isAgg), ReadOnly for Aggregates.
                        // Pos: Editable for Aggregates (TOTAL ELMA/FOODS), ReadOnly for Leafs.
                        // Vol: Editable for Tonnage Columns.
                        // Mix: Editable for Mix Columns.
                        // Geral: Pos Editable.

                        const isFatEditable = !col.isAgg;
                        const isPosEditable = (col.id === 'total_elma' || col.id === 'total_foods');

                        const fatStyle = isFatEditable ? editableStyle : readOnlyStyle;
                        const posStyle = isPosEditable ? editableStyle : readOnlyStyle;
                        const aggCellStyle = readOnlyStyle;

                        if (col.type === 'standard') {
                            rowData.push(createCell(d.metaFat, readOnlyStyle, fmtMoney));

                            // Formula for Aggregate Logic
                            if (col.id === 'total_elma' || col.id === 'total_foods') {
                                const ids = col.id === 'total_elma' ? ['707', '708', '752'] : ['1119_TODDYNHO', '1119_TODDY', '1119_QUAKER_KEROCOCO'];
                                const compCols = ids.map(id => colMap[id] + 1);
                                // const compColsPos = ids.map(id => colMap[id] + 3);
                                const formulaFat = compCols.map(c => `${getColLet(c)}${excelRow}`).join("+");

                                rowData.push({ t: 'n', v: d.metaFat, f: formulaFat, s: { ...aggCellStyle, numFmt: fmtMoney }, z: fmtMoney });
                                rowData.push(createCell(d.metaPos, readOnlyStyle, fmtInt));

                                // Positivation (Aggregate): Use Stored Target
                                let posVal = d.metaPos;
                                if (goalsSellerTargets.has(seller.name)) {
                                    const t = goalsSellerTargets.get(seller.name);
                                    if (t && t[col.id] !== undefined) posVal = t[col.id];
                                }
                                rowData.push(createCell(posVal, posStyle, fmtInt));
                            } else {
                                // Leaf Cells
                                rowData.push(createCell(d.metaFat, fatStyle, fmtMoney));
                                rowData.push(createCell(d.metaPos, readOnlyStyle, fmtInt));

                                // Positivation (Standard): Use Stored Target
                                let posVal = d.metaPos;
                                if (goalsSellerTargets.has(seller.name)) {
                                    const t = goalsSellerTargets.get(seller.name);
                                    if (t && t[col.id] !== undefined) posVal = t[col.id];
                                }
                                rowData.push(createCell(posVal, posStyle, fmtInt));
                            }

                            colCellsForSupTotal[col.id].metaFat.push(`${getColLet(cIdx)}${excelRow}`);
                            colCellsForSupTotal[col.id].fat.push(`${getColLet(cIdx + 1)}${excelRow}`);
                            colCellsForSupTotal[col.id].metaPos.push(`${getColLet(cIdx + 2)}${excelRow}`);
                            colCellsForSupTotal[col.id].pos.push(`${getColLet(cIdx + 3)}${excelRow}`);

                        } else if (col.type === 'tonnage') {
                            rowData.push(createCell(d.avgVol, readOnlyStyle, fmtVol));
                            rowData.push(createCell(d.metaVol, readOnlyStyle, fmtVol));
                            // Tonnage Adjustment: Always Yellow (Editable)
                            rowData.push(createCell(d.metaVol, editableStyle, fmtVol));
                            colCellsForSupTotal[col.id].avg.push(`${getColLet(cIdx)}${excelRow}`);
                            colCellsForSupTotal[col.id].metaVol.push(`${getColLet(cIdx + 1)}${excelRow}`);
                            colCellsForSupTotal[col.id].vol.push(`${getColLet(cIdx + 2)}${excelRow}`);

                        } else if (col.type === 'mix') {
                            rowData.push(createCell(d.avgMix, readOnlyStyle, fmtDec1));
                            rowData.push(createCell(d.metaMix, readOnlyStyle, fmtInt));
                            // Mix Adjustment: Always Yellow (Editable) - Assuming we want it editable
                            rowData.push(createCell(d.metaMix, editableStyle, fmtInt));
                            colCellsForSupTotal[col.id].avg.push(`${getColLet(cIdx)}${excelRow}`);
                            colCellsForSupTotal[col.id].metaMix.push(`${getColLet(cIdx + 1)}${excelRow}`);
                            colCellsForSupTotal[col.id].mix.push(`${getColLet(cIdx + 2)}${excelRow}`);

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

                            colCellsForSupTotal[col.id].avg.push(`${getColLet(cIdx)}${excelRow}`);
                            // Geral only has "Meta" columns basically, but treated as Adjustments in logic?
                            // Logic matches previous implementation:
                            colCellsForSupTotal[col.id].fat.push(`${getColLet(cIdx + 1)}${excelRow}`);
                            colCellsForSupTotal[col.id].vol.push(`${getColLet(cIdx + 2)}${excelRow}`);
                            colCellsForSupTotal[col.id].pos.push(`${getColLet(cIdx + 3)}${excelRow}`);

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
                        // Meta Fat Total
                        const rangeMetaFat = colCellsForSupTotal[col.id].metaFat;
                        const fMetaFatRange = rangeMetaFat.length > 0 ? `SUM(${rangeMetaFat[0]}:${rangeMetaFat[rangeMetaFat.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fMetaFatRange, s: { ...totalRowStyle, numFmt: fmtMoney }, z: fmtMoney });

                        // Ajuste Fat Total
                        const rangeFat = colCellsForSupTotal[col.id].fat;
                        const fFatRange = rangeFat.length > 0 ? `SUM(${rangeFat[0]}:${rangeFat[rangeFat.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fFatRange, s: { ...totalRowStyle, numFmt: fmtMoney }, z: fmtMoney });

                        // Meta Pos Total
                        const rangeMetaPos = colCellsForSupTotal[col.id].metaPos;
                        const fMetaPosRange = rangeMetaPos.length > 0 ? `SUM(${rangeMetaPos[0]}:${rangeMetaPos[rangeMetaPos.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fMetaPosRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });

                        // Ajuste Pos Total
                        const rangePos = colCellsForSupTotal[col.id].pos;
                        const fPosRange = rangePos.length > 0 ? `SUM(${rangePos[0]}:${rangePos[rangePos.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fPosRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });

                        // Capture Grand Totals
                        colCellsForGrandTotal[col.id].metaFat.push(`${getColLet(cIdx)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].fat.push(`${getColLet(cIdx+1)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].metaPos.push(`${getColLet(cIdx+2)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].pos.push(`${getColLet(cIdx+3)}${excelSupRow}`);

                    } else if (col.type === 'tonnage') {
                        // Avg Vol
                        const rangeAvg = colCellsForSupTotal[col.id].avg;
                        const fAvgRange = rangeAvg.length > 0 ? `SUM(${rangeAvg[0]}:${rangeAvg[rangeAvg.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fAvgRange, s: { ...totalRowStyle, numFmt: fmtVol }, z: fmtVol });

                        // Meta Vol
                        const rangeMetaVol = colCellsForSupTotal[col.id].metaVol;
                        const fMetaVolRange = rangeMetaVol.length > 0 ? `SUM(${rangeMetaVol[0]}:${rangeMetaVol[rangeMetaVol.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fMetaVolRange, s: { ...totalRowStyle, numFmt: fmtVol }, z: fmtVol });

                        // Ajuste Vol
                        const rangeVol = colCellsForSupTotal[col.id].vol;
                        const fVolRange = rangeVol.length > 0 ? `SUM(${rangeVol[0]}:${rangeVol[rangeVol.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fVolRange, s: { ...totalRowStyle, numFmt: fmtVol }, z: fmtVol });

                        colCellsForGrandTotal[col.id].avg.push(`${getColLet(cIdx)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].metaVol.push(`${getColLet(cIdx+1)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].vol.push(`${getColLet(cIdx+2)}${excelSupRow}`);

                    } else if (col.type === 'mix') {
                        // Avg Mix
                        const rangeAvg = colCellsForSupTotal[col.id].avg;
                        const fAvgRange = rangeAvg.length > 0 ? `SUM(${rangeAvg[0]}:${rangeAvg[rangeAvg.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fAvgRange, s: { ...totalRowStyle, numFmt: fmtDec1 }, z: fmtDec1 });

                        // Meta Mix
                        const rangeMetaMix = colCellsForSupTotal[col.id].metaMix;
                        const fMetaMixRange = rangeMetaMix.length > 0 ? `SUM(${rangeMetaMix[0]}:${rangeMetaMix[rangeMetaMix.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fMetaMixRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });

                        // Ajuste Mix
                        const rangeMix = colCellsForSupTotal[col.id].mix;
                        const fMixRange = rangeMix.length > 0 ? `SUM(${rangeMix[0]}:${rangeMix[rangeMix.length-1]})` : "0";
                        supRowData.push({ t: 'n', v: 0, f: fMixRange, s: { ...totalRowStyle, numFmt: fmtInt }, z: fmtInt });

                        colCellsForGrandTotal[col.id].avg.push(`${getColLet(cIdx)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].metaMix.push(`${getColLet(cIdx+1)}${excelSupRow}`);
                        colCellsForGrandTotal[col.id].mix.push(`${getColLet(cIdx+2)}${excelSupRow}`);

                    } else if (col.type === 'geral') {
                        // Geral columns are 1:1 mapped to 'adjustments' in logic but visualized as Meta.
                        // Assuming Supervisor totals just sum the columns directly.
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
                    // Meta Fat
                    const rangeMetaFat = colCellsForGrandTotal[col.id].metaFat;
                    const fMetaFat = rangeMetaFat.length > 0 ? rangeMetaFat.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fMetaFat, s: { ...grandTotalStyle, numFmt: fmtMoney }, z: fmtMoney });

                    // Ajuste Fat
                    const rangeFat = colCellsForGrandTotal[col.id].fat;
                    const fFat = rangeFat.length > 0 ? rangeFat.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fFat, s: { ...grandTotalStyle, numFmt: fmtMoney }, z: fmtMoney });

                    // Meta Pos
                    const rangeMetaPos = colCellsForGrandTotal[col.id].metaPos;
                    const fMetaPos = rangeMetaPos.length > 0 ? rangeMetaPos.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fMetaPos, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });

                    // Ajuste Pos
                    const rangePos = colCellsForGrandTotal[col.id].pos;
                    const fPos = rangePos.length > 0 ? rangePos.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fPos, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });

                } else if (col.type === 'tonnage') {
                    // Avg
                    const rangeAvg = colCellsForGrandTotal[col.id].avg;
                    const fAvg = rangeAvg.length > 0 ? rangeAvg.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fAvg, s: { ...grandTotalStyle, numFmt: fmtVol }, z: fmtVol });

                    // Meta Vol
                    const rangeMetaVol = colCellsForGrandTotal[col.id].metaVol;
                    const fMetaVol = rangeMetaVol.length > 0 ? rangeMetaVol.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fMetaVol, s: { ...grandTotalStyle, numFmt: fmtVol }, z: fmtVol });

                    // Ajuste Vol
                    const rangeVol = colCellsForGrandTotal[col.id].vol;
                    const fVol = rangeVol.length > 0 ? rangeVol.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fVol, s: { ...grandTotalStyle, numFmt: fmtVol }, z: fmtVol });

                } else if (col.type === 'mix') {
                    // Avg Mix
                    const rangeAvg = colCellsForGrandTotal[col.id].avg;
                    const fAvg = rangeAvg.length > 0 ? rangeAvg.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fAvg, s: { ...grandTotalStyle, numFmt: fmtDec1 }, z: fmtDec1 });

                    // Meta Mix
                    const rangeMetaMix = colCellsForGrandTotal[col.id].metaMix;
                    const fMetaMix = rangeMetaMix.length > 0 ? rangeMetaMix.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fMetaMix, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });

                    // Ajuste Mix
                    const rangeMix = colCellsForGrandTotal[col.id].mix;
                    const fMix = rangeMix.length > 0 ? rangeMix.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fMix, s: { ...grandTotalStyle, numFmt: fmtInt }, z: fmtInt });

                } else if (col.type === 'geral') {
                    const rangeAvg = colCellsForGrandTotal[col.id].avg;
                    const fAvg = rangeAvg.length > 0 ? rangeAvg.join("+") : "0";
                    grandRowData.push({ t: 'n', v: 0, f: fAvg, s: { ...grandTotalStyle, numFmt: fmtMoney }, z: fmtMoney });

                    // Geral columns only have 'fat', 'vol', 'pos' in colCellsForGrandTotal because we pushed cIdx+1/2/3 to them in supervisor loop.
                    // But in seller loop we pushed cIdx+1/2/3 to meta* arrays in SupTotal?
                    // Wait, let's check Geral logic in Supervisor Loop again.

                    // In Supervisor Loop for Geral:
                    // colCellsForGrandTotal[col.id].fat.push(cIdx+1) -> This is Meta Fat (Calculated)
                    // colCellsForGrandTotal[col.id].vol.push(cIdx+2) -> This is Meta Vol (Calculated)
                    // colCellsForGrandTotal[col.id].pos.push(cIdx+3) -> This is Meta Pos (Editable)

                    // So for Geral, the columns ARE the Metas. There are no separate "Adjustment" columns visualized.
                    // Row 3 headers: Média Trim., Meta, Meta, Meta.
                    // So existing logic for Geral is correct because there's no split between Meta vs Adjustment columns.
                    // It sums the Meta columns.

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
            const codCli = String(c['Código'] || c['codigo_cliente']);
            if (rca1 === '53' && codCli !== '12034') return false;
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

function getHistoricalMix(sellerName, type) {
            const sellerCode = optimizedData.rcaCodeByName.get(sellerName);
            if (!sellerCode) return 0;

            const clients = optimizedData.clientsByRca.get(sellerCode) || [];
            // Filter active clients same as main view
            const activeClients = clients.filter(c => {
                const cod = String(c['Código'] || c['codigo_cliente']);
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return true; // return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
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
                return true; // return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
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
            let filteredSummaryClients = allClientsData;

            // Apply "Active" Filter logic
            filteredSummaryClients = filteredSummaryClients.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                if (isAmericanas) return true;
                const codCliLocal = String(c['Código'] || c['codigo_cliente']);
                if (rca1 === '53' && codCliLocal !== '12034') return false;
                if (rca1 === '') return false;
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

            // Apply Seller Filter
            if (selectedGoalsSummarySellers.length > 0) {
                const sellersSet = new Set(selectedGoalsSummarySellers);
                // selectedGoalsSummarySellers contains Names.
                // Map names to codes to filter clients.rcas
                const sellerCodes = new Set();
                sellersSet.forEach(name => {
                    const code = optimizedData.rcaCodeByName.get(name);
                    if (code) sellerCodes.add(code);
                });
                filteredSummaryClients = filteredSummaryClients.filter(c => c.rcas.some(r => sellerCodes.has(r)));
            }

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
                if ((rca1 === '53' && codCli !== '12034') || rca1 === '') continue;

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
                    const codCliLocal = String(c['Código'] || c['codigo_cliente']);
                    if (rca1 === '53' && codCliLocal !== '12034') return false;
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
                const codCliLocal = String(c['Código'] || c['codigo_cliente']);
                if (isAmericanas || (((rca1 === '53' || rca1 === '053') && codCliLocal !== '12034') || rca1 === '' || rca1 === 'INATIVOS')) return false;

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

            // Calculate Totals
            const totals = {
                months: quarterMonths.map(() => 0),
                avgFat: 0,
                shareFat: 0,
                metaFat: 0,
                metaVol: 0,
                mixPdvSum: 0,
                mixPdvCount: 0
            };

            data.forEach(item => {
                quarterMonths.forEach((m, i) => {
                    totals.months[i] += (item.monthlyValues[m.key] || 0);
                });
                totals.avgFat += (item.avgFat || 0);
                totals.shareFat += (item.shareFat || 0);
                totals.metaFat += (item.metaFat || 0);
                totals.metaVol += (item.metaVol || 0);
                if ((item.mixPdv || 0) > 0) {
                    totals.mixPdvSum += (item.mixPdv || 0);
                    totals.mixPdvCount++;
                }
            });

            const mixPdvAvg = totals.mixPdvCount > 0 ? (totals.mixPdvSum / totals.mixPdvCount) : 0;

            const foot = [[
                'TOTAL', '', '',
                ...totals.months.map(v => v.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})),
                totals.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2}),
                (totals.shareFat * 100).toFixed(2) + '%',
                totals.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2}),
                totals.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 3, maximumFractionDigits: 3}),
                mixPdvAvg.toLocaleString('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})
            ]];

            doc.autoTable({
                head: head,
                body: body,
                foot: foot,
                showFoot: 'lastPage',
                startY: 45,
                theme: 'grid',
                styles: { fontSize: 7, cellPadding: 1, textColor: [0, 0, 0], halign: 'right' },
                headStyles: { fillColor: [20, 184, 166], textColor: 255, fontStyle: 'bold', fontSize: 8, halign: 'center' },
                footStyles: { fillColor: [50, 50, 50], textColor: 255, fontStyle: 'bold', halign: 'right', fontSize: 8 },
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
            if (selectedGoalsGvSellers && selectedGoalsGvSellers.length > 0) {
                const firstName = getFirstName(selectedGoalsGvSellers[0]);
                if (firstName) nameParam = '_' + firstName.toUpperCase();
            } else if (selectedGoalsGvSupervisors && selectedGoalsGvSupervisors.length > 0) {
                const firstName = getFirstName(selectedGoalsGvSupervisors[0]);
                if (firstName) nameParam = '_' + firstName.toUpperCase();
            }

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

            // Create Sheet directly from data
            const ws_flat = XLSX.utils.aoa_to_sheet(ws_data_flat);

            const range = XLSX.utils.decode_range(ws_flat['!ref']);
            const lastRowIndex = range.e.r + 1; // +1 because we will append a row
            const dataStartRow = 1; // 0-based, assuming header is row 0
            const dataEndRow = lastRowIndex - 1; // Last row of data (0-based)

            // Helper to get column letter
            const getColLetter = (colIndex) => {
                let letter = '';
                let temp = colIndex;
                while (temp >= 0) {
                    letter = String.fromCharCode((temp % 26) + 65) + letter;
                    temp = Math.floor(temp / 26) - 1;
                }
                return letter;
            };

            // Inject Formulas into Data Rows (Optional per request: "para o EXCEL ass colunas que devem ter formulas são ( MÉDIA FAT, % SHARE FAT, % SHARE VOL)")
            // To do this properly, we need to know the column indices.
            // Headers: CÓD, CLIENTE, VENDEDOR, [Months...], MÉDIA FAT, % SHARE FAT, META FAT, MÉDIA VOL, % SHARE VOL, META VOL, MIX PDV
            const colIndices = {
                monthsStart: 3,
                monthsEnd: 3 + quarterMonths.length - 1,
                avgFat: 3 + quarterMonths.length,
                shareFat: 3 + quarterMonths.length + 1,
                metaFat: 3 + quarterMonths.length + 2,
                avgVol: 3 + quarterMonths.length + 3,
                shareVol: 3 + quarterMonths.length + 4,
                metaVol: 3 + quarterMonths.length + 5,
                mixPdv: 3 + quarterMonths.length + 6
            };

            // Update Data Rows with Formulas
            for (let r = dataStartRow; r <= dataEndRow; r++) {
                const rowNum = r + 1; // 1-based row number for Excel formulas

                // Average Fat Formula: =AVERAGE(Start:End)
                const avgFatRef = `${getColLetter(colIndices.avgFat)}${rowNum}`;
                const monthsRange = `${getColLetter(colIndices.monthsStart)}${rowNum}:${getColLetter(colIndices.monthsEnd)}${rowNum}`;
                if (ws_flat[avgFatRef]) {
                    ws_flat[avgFatRef].f = `AVERAGE(${monthsRange})`;
                    ws_flat[avgFatRef].v = undefined; // Clear static value to force formula calc (if supported) or rely on formula
                }

                // Share Fat Formula: =AvgFat / TotalAvgFat
                // NOTE: This depends on the Total Row (which is at lastRowIndex + 1 in Excel, or rowNum = lastRowIndex + 1)
                // Let's defer Share Formulas until we define the Total Row position.
                // Assuming Total Row will be at `dataEndRow + 2` (1-based index: LastDataRow + 1)
                const totalRowExcelNum = dataEndRow + 2;

                const shareFatRef = `${getColLetter(colIndices.shareFat)}${rowNum}`;
                const totalAvgFatRef = `${getColLetter(colIndices.avgFat)}${totalRowExcelNum}`;
                // Only apply if totalAvgFat is not zero (avoid div/0) - hard to know in static generation, but formula handles it
                if (ws_flat[shareFatRef]) {
                    ws_flat[shareFatRef].f = `${avgFatRef}/$${totalAvgFatRef}`; // Absolute reference for Total
                    ws_flat[shareFatRef].t = 'n';
                    ws_flat[shareFatRef].z = '0.00%'; // Format as percentage
                    ws_flat[shareFatRef].v = undefined;
                }

                const shareVolRef = `${getColLetter(colIndices.shareVol)}${rowNum}`;
                const avgVolRef = `${getColLetter(colIndices.avgVol)}${rowNum}`; // Assuming Avg Vol is calculated or static? User asked for formula for Avg Fat, implies Avg Vol too?
                // "ass colunas que devem ter formulas são ( MÉDIA FAT, % SHARE FAT, % SHARE VOL)" - Doesn't explicitly say Avg Vol, but Share Vol depends on it.
                // If Avg Vol is static, we can still reference it.

                const totalAvgVolRef = `${getColLetter(colIndices.avgVol)}${totalRowExcelNum}`;
                if (ws_flat[shareVolRef]) {
                    ws_flat[shareVolRef].f = `${avgVolRef}/$${totalAvgVolRef}`;
                    ws_flat[shareVolRef].t = 'n';
                    ws_flat[shareVolRef].z = '0.00%';
                    ws_flat[shareVolRef].v = undefined;
                }
            }

            // Append Total Row with Formulas
            // We need to extend the sheet range
            range.e.r = lastRowIndex; // Extend range to cover new row
            ws_flat['!ref'] = XLSX.utils.encode_range(range);

            const totalRowIndex = lastRowIndex; // 0-based index for the new row
            const totalRowNum = totalRowIndex + 1; // 1-based for formulas

            // Initialize Total Cells
            const setTotalCell = (colIdx, formula, format) => {
                const addr = XLSX.utils.encode_cell({ r: totalRowIndex, c: colIdx });
                ws_flat[addr] = { t: 'n', f: formula };
                if (format) ws_flat[addr].z = format;
            };

            // 1. Label
            const labelAddr = XLSX.utils.encode_cell({ r: totalRowIndex, c: 0 });
            ws_flat[labelAddr] = { t: 's', v: 'TOTAL' };

            // 2. Month Totals
            for (let i = colIndices.monthsStart; i <= colIndices.monthsEnd; i++) {
                const colLetter = getColLetter(i);
                setTotalCell(i, `SUM(${colLetter}${dataStartRow + 1}:${colLetter}${dataEndRow + 1})`, '#,##0.00');
            }

            // 3. Avg Fat Total
            setTotalCell(colIndices.avgFat, `SUM(${getColLetter(colIndices.avgFat)}${dataStartRow + 1}:${getColLetter(colIndices.avgFat)}${dataEndRow + 1})`, '#,##0.00');

            // 4. Share Fat Total (Sum of percentages)
            setTotalCell(colIndices.shareFat, `SUM(${getColLetter(colIndices.shareFat)}${dataStartRow + 1}:${getColLetter(colIndices.shareFat)}${dataEndRow + 1})`, '0.00%');

            // 5. Meta Fat Total
            setTotalCell(colIndices.metaFat, `SUM(${getColLetter(colIndices.metaFat)}${dataStartRow + 1}:${getColLetter(colIndices.metaFat)}${dataEndRow + 1})`, '#,##0.00');

            // 6. Avg Vol Total
            setTotalCell(colIndices.avgVol, `SUM(${getColLetter(colIndices.avgVol)}${dataStartRow + 1}:${getColLetter(colIndices.avgVol)}${dataEndRow + 1})`, '#,##0.000');

            // 7. Share Vol Total
            setTotalCell(colIndices.shareVol, `SUM(${getColLetter(colIndices.shareVol)}${dataStartRow + 1}:${getColLetter(colIndices.shareVol)}${dataEndRow + 1})`, '0.00%');

            // 8. Meta Vol Total
            setTotalCell(colIndices.metaVol, `SUM(${getColLetter(colIndices.metaVol)}${dataStartRow + 1}:${getColLetter(colIndices.metaVol)}${dataEndRow + 1})`, '#,##0.000');

            // 9. Mix PDV Total (AVERAGEIF > 0)
            const mixCol = getColLetter(colIndices.mixPdv);
            const mixRange = `${mixCol}${dataStartRow + 1}:${mixCol}${dataEndRow + 1}`;
            setTotalCell(colIndices.mixPdv, `AVERAGEIF(${mixRange},">0")`, '0.0');


             // Style Header & Footer
            if (ws_flat['!ref']) {
                const range = XLSX.utils.decode_range(ws_flat['!ref']);

                // Header
                for (let C = range.s.c; C <= range.e.c; ++C) {
                    const addr = XLSX.utils.encode_cell({ r: 0, c: C });
                    if (!ws_flat[addr]) continue;
                    if (!ws_flat[addr].s) ws_flat[addr].s = {};
                    ws_flat[addr].s.fill = { fgColor: { rgb: "1E293B" } };
                    ws_flat[addr].s.font = { color: { rgb: "FFFFFF" }, bold: true };
                    ws_flat[addr].s.alignment = { horizontal: "center" };
                }

                // Footer (Total Row)
                const lastRowIdx = range.e.r;
                for (let C = range.s.c; C <= range.e.c; ++C) {
                    const addr = XLSX.utils.encode_cell({ r: lastRowIdx, c: C });
                    if (!ws_flat[addr]) continue;
                    if (!ws_flat[addr].s) ws_flat[addr].s = {};
                    ws_flat[addr].s.font = { bold: true };
                    ws_flat[addr].s.fill = { fgColor: { rgb: "E2E8F0" } }; // Light Gray
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
            if (selectedGoalsGvSellers && selectedGoalsGvSellers.length > 0) {
                const firstName = getFirstName(selectedGoalsGvSellers[0]);
                if (firstName) nameParam = '_' + firstName.toUpperCase();
            } else if (selectedGoalsGvSupervisors && selectedGoalsGvSupervisors.length > 0) {
                const firstName = getFirstName(selectedGoalsGvSupervisors[0]);
                if (firstName) nameParam = '_' + firstName.toUpperCase();
            }

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

function getHistoricalPositivation(sellerName, category) {
            const sellerCode = optimizedData.rcaCodeByName.get(sellerName);
            if (!sellerCode) return 0;

            const clients = optimizedData.clientsByRca.get(sellerCode) || [];
            // Filter active clients same as main view
            const activeClients = clients.filter(c => {
                const cod = String(c['Código'] || c['codigo_cliente']);
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return true; // return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
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
            const displayMetrics = getMetricsForSupervisors(selectedGoalsGvSupervisors);

            // Update Header (Dynamic) - Same as before
            const thead = document.querySelector('#goals-table-container table thead');
            if (thead) {
                const monthHeaders = quarterMonths.map(m => `<th class="px-2 py-2 text-right w-20 bg-blue-900/10 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">${m.label}</th>`).join('');
                const monthsCount = quarterMonths.length;
                thead.innerHTML = `<tr><th rowspan="2" class="px-2 py-2 text-center w-16 border-r border-b border-slate-700">CÓD</th><th rowspan="2" class="px-3 py-2 text-left w-48 border-r border-b border-slate-700">CLIENTE</th><th rowspan="2" class="px-3 py-2 text-left w-24 border-r border-b border-slate-700">VENDEDOR</th><th colspan="${3 + monthsCount}" class="px-2 py-1 text-center bg-blue-900/30 text-blue-400 border-r border-slate-700 border-b-0">FATURAMENTO (R$)</th><th colspan="3" class="px-2 py-1 text-center bg-orange-900/30 text-orange-400 border-r border-slate-700 border-b-0">VOLUME (KG)</th><th rowspan="2" class="px-2 py-2 text-center w-16 bg-purple-900/20 text-purple-300 font-bold border-b border-slate-700">Mix PDV</th></tr><tr>${monthHeaders}<th class="px-2 py-2 text-right w-24 bg-blue-900/20 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">MÉDIA</th><th class="px-2 py-2 text-center w-16 bg-blue-900/20 text-blue-300 border-r border-b border-slate-700/50 text-[10px]">% SHARE</th><th class="px-2 py-2 text-right w-24 bg-blue-900/20 text-blue-100 font-bold border-r border-b border-slate-700 text-[10px]">META AUTO</th><th class="px-2 py-2 text-right w-24 bg-orange-900/20 text-orange-300 border-r border-b border-slate-700/50 text-[10px]">MÉDIA KG</th><th class="px-2 py-2 text-center w-16 bg-orange-900/20 text-orange-300 border-r border-b border-slate-700/50 text-[10px]">% SHARE</th><th class="px-2 py-2 text-right w-24 bg-orange-900/20 text-orange-100 font-bold border-r border-b border-slate-700 text-[10px]">META KG</th></tr>`;
            }

            const filteredClients = getGoalsFilteredData();
            goalsGvTableBody.innerHTML = '<tr><td colspan="15" class="text-center p-8"><svg class="animate-spin h-8 w-8 text-teal-500 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg></td></tr>';

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
                const isSingleSeller = selectedGoalsGvSellers.length === 1;

                if (goalMixInput) {
                    const newMixInput = goalMixInput.cloneNode(true);
                    goalMixInput.parentNode.replaceChild(newMixInput, goalMixInput);

                    // Calculate Total Adjustment for Current View Context
                    let contextAdjustment = 0;
                    const adjustmentMap = goalsPosAdjustments[contextKey];
                    let absoluteOverride = null;

                    if (isSingleSeller) {
                        // Check for Absolute Override from Import
                        absoluteOverride = getSellerTargetOverride(selectedGoalsGvSellers[0], 'pos', contextKey);

                        if (absoluteOverride === null && adjustmentMap) {
                            // Specific Seller Context (Fallback)
                            contextAdjustment = adjustmentMap.get(selectedGoalsGvSellers[0]) || 0;
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
                            absOverride = getSellerTargetOverride(selectedGoalsGvSellers[0], type === 'salty' ? 'mix_salty' : 'mix_foods', contextKey);
                            if (absOverride === null) {
                                adj = adjustmentsMap.get(selectedGoalsGvSellers[0]) || 0;
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
                                            const sellerName = selectedGoalsGvSellers[0];
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
                    if (visibleSellersSet.size === 0 && selectedGoalsGvSellers.length > 0) {
                         visibleSellersSet = new Set(selectedGoalsGvSellers);
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
            const supervisorsSet = new Set(selectedGoalsSvSupervisors);

            let clients = allClientsData;

            clients = clients.filter(c => {
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                if (isAmericanas) return true;
                    // STRICT FILTER: Exclude RCA 53 (Balcão) and INATIVOS
                    const codCliLocal = String(c['Código'] || c['codigo_cliente']);
                    if (rca1 === '53' && codCliLocal !== '12034') return false;
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
                                const targets = goalsSellerTargets.get(sellerName) || {};
                                // IMPORTANT FIX: Only overwrite MetaPos if the target key exactly matches
                                // the column ID (e.g. 'total_elma' which is Positivação).
                                // If the column is 'tonelada_elma', and we only have 'tonelada_elma_VOL' in targets,
                                // the 'targets[colId]' (which is targets['tonelada_elma']) will be undefined.
                                // The legacy explicitTarget check above matched targets[colId.toUpperCase()] by mistake for volume
                                // but we shouldn't apply volume (tonelada_elma_VOL) as Positivation (metaPos).
                                if (targets[colId] !== undefined || targets[colId.toUpperCase()] !== undefined) {
                                    // Make sure it is actually the Positivation target (no suffix)
                                    // Volume and FAT have the _VOL and _FAT suffix.
                                    data.metaPos = explicitTarget;
                                }
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
                                // Rule: Fat Editable if !isAgg (Leafs). Pos Editable if Agg (Total Elma/Foods).
                                const isFatEditable = !col.isAgg;
                                const isPosEditable = (col.id === 'total_elma' || col.id === 'total_foods');

                                const fatInputClass = isFatEditable ? 'text-yellow-300' : 'text-slate-400 font-bold opacity-70';
                                const fatReadonlyAttr = isFatEditable ? '' : 'readonly disabled';
                                const fatCellBg = isFatEditable ? 'bg-[#1e293b]' : 'bg-[#151c36]';

                                const posInputClass = isPosEditable ? 'text-yellow-300' : 'text-slate-400 font-bold opacity-70';
                                const posReadonlyAttr = isPosEditable ? '' : 'readonly disabled';
                                const posCellBg = isPosEditable ? 'bg-[#1e293b]' : 'bg-[#151c36]';

                                quarterMonths.forEach(m => bodyHTML += `<td class="px-1 py-1 text-right text-slate-400 border-r border-slate-800/50 text-[10px] bg-blue-900/5">${(d.monthlyValues[m.key] || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>`);
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 bg-blue-900/10 font-medium">${d.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right ${col.colorClass} border-r border-slate-800/50 text-xs font-mono">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 ${fatCellBg} border-r border-slate-800/50"><input type="text" value="${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}" class="goals-sv-input bg-transparent text-right w-full outline-none ${fatInputClass} text-xs font-mono" ${fatReadonlyAttr}></td><td class="px-1 py-1 text-center text-slate-300 border-r border-slate-800/50">${d.metaPos}</td><td class="px-1 py-1 ${posCellBg} border-r border-slate-800/50"><input type="text" value="${d.metaPos}" class="goals-sv-input bg-transparent text-center w-full outline-none ${posInputClass} text-xs font-mono" ${posReadonlyAttr}></td>`;
                            } else if (col.type === 'tonnage') {
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-mono text-xs">${d.avgVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-bold font-mono text-xs">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 bg-[#1e293b] border-r border-slate-800/50"><input type="text" value="${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}" class="goals-sv-input bg-transparent text-right w-full outline-none text-yellow-300 text-xs font-mono"></td>`;
                            } else if (col.type === 'mix') {
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50">${d.avgMix.toLocaleString('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})}</td><td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-bold">${d.metaMix}</td><td class="px-1 py-1 bg-[#1e293b] border-r border-slate-800/50"><input type="text" value="${d.metaMix}" class="goals-sv-input bg-transparent text-right w-full outline-none text-yellow-300 text-xs font-mono"></td>`;
                            } else if (col.type === 'geral') {
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-400 border-r border-slate-800/50 font-mono text-xs">${d.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right text-white font-bold border-r border-slate-800/50 font-mono text-xs goals-sv-text" data-sup-id="${sup.id}" data-col-id="geral" data-field="fat" id="geral-${seller.id || seller.name.replace(/\s+/g,'_')}-fat">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right text-white font-bold border-r border-slate-800/50 font-mono text-xs goals-sv-text" data-sup-id="${sup.id}" data-col-id="geral" data-field="ton" id="geral-${seller.id || seller.name.replace(/\s+/g,'_')}-ton">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 text-center text-yellow-300 font-bold border-r border-slate-800/50 font-mono text-xs goals-sv-text" data-sup-id="${sup.id}" data-col-id="geral" data-field="pos" id="geral-${seller.id || seller.name.replace(/\s+/g,'_')}-pos">${d.metaPos}</td>`;
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
            const forbidden = ['NOME', 'VENDEDOR', 'SUPERV', 'CODUSUR', 'CODCLI', 'SUPERVISOR', 'INATIVOS'];
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

function normalize(str) {
            return str
                ? str.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/\s+/g, ' ').trim().toUpperCase()
                : '';
        }

const formatValue = (val, format) => {
            if (format === 'currency') return val.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
            if (format === 'decimal') return val.toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 });
            if (format === 'mix') return val.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            return val.toLocaleString('pt-BR', { maximumFractionDigits: 0 });
        };

function debounce(func, delay = 300) {
            let timeout;
            return function(...args) {
                clearTimeout(timeout);
                timeout = setTimeout(() => {
                    func.apply(this, args);
                }, delay);
            };
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

async function navigateTo() {
 document.body.removeAttribute('data-loading');
 document.getElementById('page-transition-loader')?.classList.add('opacity-0', 'pointer-events-none');
 document.getElementById('side-menu')?.classList.add('-translate-x-full');
 document.getElementById('sidebar-overlay')?.classList.add('hidden');
 goalsView.classList.remove('hidden');
 document.querySelector('.nav-link[data-target="goals"]')?.classList.add('bg-slate-700','text-white');
 if (viewState.goals.dirty) { updateGoalsView(); viewState.goals.dirty = false; }
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
            const BATCH_SIZE = 2000;
            const CONCURRENT_REQUESTS = 5;

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

                    // Fallback: Batched Delete para evitar Statement Timeout (57014)
                    console.info(`Iniciando exclusão em lote para a tabela ${table}...`);
                    let hasMore = true;
                    // Utiliza lote de 50 para garantir que a URL gerada com in.(id1,id2...)
                    // não exceda o limite de ~8KB de requisições HTTP do Supabase/NGINX
                    // (50 UUIDs * 37 chars = ~1850 bytes).
                    const batchSize = 50;

                    while (hasMore) {
                        // Busca um lote de IDs
                        const fetchResponse = await fetch(`${supabaseUrl}/rest/v1/${table}?select=${pkColumn}&limit=${batchSize}`, {
                            method: 'GET',
                            headers: {
                                'apikey': apiKeyHeader,
                                'Authorization': `Bearer ${authToken}`,
                                'Content-Type': 'application/json'
                            }
                        });

                        if (!fetchResponse.ok) {
                            const errorText = await fetchResponse.text();
                            throw new Error(`Erro ao buscar IDs para exclusão em lote na tabela ${table}: ${errorText}`);
                        }

                        const rows = await fetchResponse.json();

                        if (rows.length === 0) {
                            hasMore = false;
                            break;
                        }

                        const ids = rows.map(r => r[pkColumn]);

                        // Deleta o lote
                        const deleteUrl = new URL(`${supabaseUrl}/rest/v1/${table}`);

                        // Para evitar URL too long, não passamos todos os ids na URL se forem muitos (limitamos a 50 que é seguro)
                        deleteUrl.searchParams.append(pkColumn, `in.(${ids.join(',')})`);

                        const deleteResponse = await fetch(deleteUrl.toString(), {
                            method: 'DELETE',
                            headers: {
                                'apikey': apiKeyHeader,
                                'Authorization': `Bearer ${authToken}`,
                                'Content-Type': 'application/json',
                                'Prefer': 'return=representation'
                            }
                        });

                        if (!deleteResponse.ok) {
                            const errorText = await deleteResponse.text();
                            if (errorText.includes('57014') || errorText.includes('statement timeout')) {
                                throw new Error(`Erro de Timeout (57014) ao limpar a tabela ${table} em lotes. O banco de dados está extremamente lento.\n\nPara resolver isso permanentemente, você precisa criar a função 'truncate_table' no Supabase. Copie todo o conteúdo do arquivo 'SQL/SQL_GERAL.sql' e execute-o no SQL Editor do seu projeto Supabase.`);
                            }
                            throw new Error(`Erro ao deletar lote na tabela ${table}: ${errorText}`);
                        }

                        const deletedRows = await deleteResponse.json();

                        // Prevent infinite loop if RLS blocks deletion (i.e. we fetch rows but cannot delete them)
                        if (deletedRows.length === 0) {
                            throw new Error(`Permissão Negada: Nenhuma linha foi deletada na tabela ${table}. Suas políticas de segurança (RLS) permitem leitura mas bloqueiam exclusão.`);
                        }
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
                        const keysToPreserve = ['groq_api_key'];
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

function setupEventListeners() {
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
                generateBtn.addEventListener('click', async () => {
                    const salesFile = document.getElementById('sales-file-input').files[0];
                    const clientsFile = document.getElementById('clients-file-input').files[0];
                    const productsFile = document.getElementById('products-file-input').files[0];
                    const historyFile = document.getElementById('history-file-input').files[0];
                    const innovationsFile = document.getElementById('innovations-file-input').files[0];

                    if (!salesFile && !historyFile) {
                        alert("Pelo menos o arquivo de Vendas ou Histórico é necessário.");
                        return;
                    }

                    document.getElementById('status-container').classList.remove('hidden');
                    document.getElementById('status-text').textContent = "Buscando configurações de filiais...";
                    document.getElementById('progress-bar').style.width = '5%';

                    let configCityBranches = [];
                    try {
                        const { data, error } = await window.supabaseClient
                            .from('config_city_branches')
                            .select('cidade, filial');
                        if (error) throw error;
                        if (data) configCityBranches = data;
                    } catch (err) {
                        console.error("Erro ao buscar config_city_branches:", err);
                        alert("Não foi possível carregar a tabela de configuração de filiais. O processamento continuará sem essas regras.");
                    }

                    // Initialize Worker
                    const worker = new Worker('worker.js?v=20261013-client-headers');

                    document.getElementById('status-text').textContent = "Processando arquivos...";

                    worker.postMessage({ salesFile, clientsFile, productsFile, historyFile, innovationsFile, configCityBranches });

                    worker.onmessage = (e) => {
                        const { type, data, status, percentage, message, missingCities } = e.data;
                        if (type === 'progress') {
                            document.getElementById('status-text').textContent = status;
                            document.getElementById('progress-bar').style.width = percentage + '%';
                        } else if (type === 'result') {
                            if (missingCities && missingCities.length > 0) {
                                alert("Aviso: As seguintes cidades presentes no cadastro de clientes não foram encontradas na tabela 'config_city_branches':\n" + missingCities.join('\n'));
                            }
                            enviarDadosParaSupabase(data);
                            worker.terminate();
                        } else if (type === 'error') {
                            alert('Erro no processamento: ' + message);
                            worker.terminate();
                        }
                    };
                });
            }
const markDirty = (view) => {
                if (viewState[view]) viewState[view].dirty = true;
            };
document.addEventListener('click', (e) => {
                const target = e.target;
                if (target.closest('.sidebar-toggle') || target.closest('#close-sidebar-btn') || target.id === 'sidebar-overlay') {
                    toggleSidebar();
                }
            });
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
loadGoalsFromSupabase();
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
const btnPepsico = document.getElementById('goals-category-pepsico-btn');
const btnElmaChips = document.getElementById('goals-category-elmachips-btn');
const btnFoods = document.getElementById('goals-category-foods-btn');
const btnSummary = document.getElementById('goals-category-summary-btn');
const subTabsPepsico = document.getElementById('goals-sub-tabs-pepsico');
const subTabsElmaChips = document.getElementById('goals-sub-tabs-elmachips');
const subTabsFoods = document.getElementById('goals-sub-tabs-foods');
const goalsMainContainer = document.getElementById('goals-input-cards');
const goalsTableContainer = document.getElementById('goals-table-container');
const goalsFiltersContainer = document.querySelector('#goals-gv-content > div.mb-4');
const goalsSummaryContainer = document.getElementById('goals-summary-content');
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
goalsGvSupervisorFilterBtn.addEventListener('click', () => goalsGvSupervisorFilterDropdown.classList.toggle('hidden'));
goalsGvSupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedGoalsGvSupervisors.push(value);
                    else selectedGoalsGvSupervisors = selectedGoalsGvSupervisors.filter(s => s !== value);

                    selectedGoalsGvSupervisors = updateSupervisorFilter(goalsGvSupervisorFilterDropdown, goalsGvSupervisorFilterText, selectedGoalsGvSupervisors, [...allSalesData, ...allHistoryData], true);

                    // Reset sellers or keep intersection?
                    // Standard: Update Seller Options based on Supervisor
                    selectedGoalsGvSellers = [];
                    selectedGoalsGvSellers = updateSellerFilter(selectedGoalsGvSupervisors, goalsGvSellerFilterDropdown, goalsGvSellerFilterText, selectedGoalsGvSellers, [...allSalesData, ...allHistoryData]);

                    updateGoals();
                }
            });
goalsGvSellerFilterBtn.addEventListener('click', () => goalsGvSellerFilterDropdown.classList.toggle('hidden'));
goalsGvSellerFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedGoalsGvSellers.push(value);
                    else selectedGoalsGvSellers = selectedGoalsGvSellers.filter(s => s !== value);

                    selectedGoalsGvSellers = updateSellerFilter(selectedGoalsGvSupervisors, goalsGvSellerFilterDropdown, goalsGvSellerFilterText, selectedGoalsGvSellers, [...allSalesData, ...allHistoryData], true);

                    updateGoals();
                }
            });
goalsGvCodcliFilter.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/[^0-9]/g, '');
                debouncedUpdateGoals();
            });
const goalsSummarySupervisorFilterBtn = document.getElementById('goals-summary-supervisor-filter-btn');
const goalsSummarySupervisorFilterDropdown = document.getElementById('goals-summary-supervisor-filter-dropdown');
const goalsSummarySellerFilterBtn = document.getElementById('goals-summary-seller-filter-btn');
const goalsSummarySellerFilterDropdown = document.getElementById('goals-summary-seller-filter-dropdown');
const clearGoalsSummaryFiltersBtn = document.getElementById('clear-goals-summary-filters-btn');
if (goalsSummarySupervisorFilterBtn && goalsSummarySupervisorFilterDropdown) {
                goalsSummarySupervisorFilterBtn.addEventListener('click', () => goalsSummarySupervisorFilterDropdown.classList.toggle('hidden'));

                goalsSummarySupervisorFilterDropdown.addEventListener('change', (e) => {
                    if (e.target.type === 'checkbox') {
                        const { value, checked } = e.target;
                        if (checked) selectedGoalsSummarySupervisors.push(value);
                        else selectedGoalsSummarySupervisors = selectedGoalsSummarySupervisors.filter(s => s !== value);

                        selectedGoalsSummarySupervisors = updateSupervisorFilter(goalsSummarySupervisorFilterDropdown, document.getElementById('goals-summary-supervisor-filter-text'), selectedGoalsSummarySupervisors, allSalesData, true);

                        // Update Seller Filter Cascade
                        selectedGoalsSummarySellers = [];
                        selectedGoalsSummarySellers = updateSellerFilter(selectedGoalsSummarySupervisors, goalsSummarySellerFilterDropdown, document.getElementById('goals-summary-seller-filter-text'), selectedGoalsSummarySellers, allSalesData);

                        updateGoalsSummaryView();
                    }
                });
            }
if (goalsSummarySellerFilterBtn && goalsSummarySellerFilterDropdown) {
                goalsSummarySellerFilterBtn.addEventListener('click', () => goalsSummarySellerFilterDropdown.classList.toggle('hidden'));

                goalsSummarySellerFilterDropdown.addEventListener('change', (e) => {
                    if (e.target.type === 'checkbox') {
                        const { value, checked } = e.target;
                        if (checked) {
                            if (!selectedGoalsSummarySellers.includes(value)) selectedGoalsSummarySellers.push(value);
                        } else {
                            selectedGoalsSummarySellers = selectedGoalsSummarySellers.filter(s => s !== value);
                        }
                        selectedGoalsSummarySellers = updateSellerFilter(selectedGoalsSummarySupervisors, goalsSummarySellerFilterDropdown, document.getElementById('goals-summary-seller-filter-text'), selectedGoalsSummarySellers, allSalesData, true);
                        updateGoalsSummaryView();
                    }
                });
            }
if (clearGoalsSummaryFiltersBtn) {
                clearGoalsSummaryFiltersBtn.addEventListener('click', () => {
                    selectedGoalsSummarySupervisors = [];
                    selectedGoalsSummarySellers = [];
                    updateSupervisorFilter(goalsSummarySupervisorFilterDropdown, document.getElementById('goals-summary-supervisor-filter-text'), selectedGoalsSummarySupervisors, allSalesData, true);
                    updateSellerFilter(selectedGoalsSummarySupervisors, goalsSummarySellerFilterDropdown, document.getElementById('goals-summary-seller-filter-text'), selectedGoalsSummarySellers, allSalesData);
                    updateGoalsSummaryView();
                });
            }
document.addEventListener('click', (e) => {
                if (goalsSummarySupervisorFilterBtn && goalsSummarySupervisorFilterDropdown) {
                    if (!goalsSummarySupervisorFilterBtn.contains(e.target) && !goalsSummarySupervisorFilterDropdown.contains(e.target)) {
                        goalsSummarySupervisorFilterDropdown.classList.add('hidden');
                    }
                }
                if (goalsSummarySellerFilterBtn && goalsSummarySellerFilterDropdown) {
                    if (!goalsSummarySellerFilterBtn.contains(e.target) && !goalsSummarySellerFilterDropdown.contains(e.target)) {
                        goalsSummarySellerFilterDropdown.classList.add('hidden');
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
const fatInput = document.getElementById('goal-global-fat');
const volInput = document.getElementById('goal-global-vol');
clearGoalsGvFiltersBtn.addEventListener('click', () => { resetGoalsGvFilters(); markDirty('goals'); });
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
}

initializeOptimizedDataStructures();

selectedGoalsGvSupervisors = updateSupervisorFilter(goalsGvSupervisorFilterDropdown, goalsGvSupervisorFilterText, selectedGoalsGvSupervisors, allSalesData);

selectedGoalsGvSellers = updateSellerFilter(selectedGoalsGvSupervisors, goalsGvSellerFilterDropdown, goalsGvSellerFilterText, selectedGoalsGvSellers, [...allSalesData, ...allHistoryData]);

selectedGoalsSvSupervisors = updateSupervisorFilter(goalsSvSupervisorFilterDropdown, goalsSvSupervisorFilterText, selectedGoalsSvSupervisors, allSalesData);

if(document.getElementById('goals-summary-supervisor-filter-dropdown')) {
            selectedGoalsSummarySupervisors = updateSupervisorFilter(document.getElementById('goals-summary-supervisor-filter-dropdown'), document.getElementById('goals-summary-supervisor-filter-text'), selectedGoalsSummarySupervisors, allSalesData);
            selectedGoalsSummarySellers = updateSellerFilter(selectedGoalsSummarySupervisors, document.getElementById('goals-summary-seller-filter-dropdown'), document.getElementById('goals-summary-seller-filter-text'), selectedGoalsSummarySellers, allSalesData);
        }

calculateGoalsMetrics();

for (const key in globalGoalsMetrics) {
            if (!goalsTargets[key]) {
                goalsTargets[key] = { fat: 0, vol: 0 };
            }
            if (globalGoalsMetrics[key]) {
                goalsTargets[key].fat = globalGoalsMetrics[key].prevFat;
                goalsTargets[key].vol = globalGoalsMetrics[key].prevVol;
            }
        }

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

setupEventListeners();
navigateTo();

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
                return true; // return (isAmericanas || rca1 !== "53" || clientsWithSalesThisMonth.has(cod));
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

            // 2. Identify Header Rows and Construct ColMap
            const colMap = {};
            let dataStartRow = 0;

            if (rows.length >= 3) {
                // Standard logic: Rows 0, 1, 2
                const startRow = 0;

                const header0 = rows[startRow].map(h => h ? h.trim().toUpperCase() : '');
                const header1 = rows[startRow + 1].map(h => h ? h.trim().toUpperCase() : '');
                const header2 = rows[startRow + 2].map(h => h ? h.trim().toUpperCase() : '');

                console.log("[Parser] Header 0:", header0.join('|'));
                console.log("[Parser] Header 1:", header1.join('|'));
                console.log("[Parser] Header 2:", header2.join('|'));

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
                        // Normalize Category Names to IDs (Fuzzy Matching)
                        if (catKey.includes('NÃO EXTRUSADOS') || catKey.includes('NAO EXTRUSADOS')) catKey = '708';
                        else if (catKey.includes('EXTRUSADOS')) catKey = '707';
                        else if (catKey.includes('TORCIDA')) catKey = '752';
                        else if (catKey.includes('TODDYNHO')) catKey = '1119_TODDYNHO';
                        else if (catKey.includes('TODDY')) catKey = '1119_TODDY';
                        else if (catKey.includes('QUAKER') || catKey.includes('KEROCOCO')) catKey = '1119_QUAKER_KEROCOCO';
                        else if (catKey === 'KG ELMA') catKey = 'tonelada_elma';
                        else if (catKey === 'KG FOODS') catKey = 'tonelada_foods';
                        else if (catKey === 'TOTAL ELMA') catKey = 'total_elma';
                        else if (catKey === 'TOTAL FOODS') catKey = 'total_foods';
                        else if (catKey === 'MIX SALTY') catKey = 'mix_salty';
                        else if (catKey === 'MIX FOODS') catKey = 'mix_foods';
                        else if (catKey === 'PEPSICO_ALL_POS' || catKey === 'PEPSICO_ALL' || catKey === 'GERAL') catKey = 'pepsico_all';

                        let metricKey = 'OTHER';
                        if (currentMetric === 'FATURAMENTO' || currentMetric === 'MÉDIA TRIM.') metricKey = 'FAT';
                        else if (currentMetric === 'POSITIVAÇÃO' || currentMetric === 'POSITIVACAO' || currentMetric.includes('POSITIVA')) metricKey = 'POS';
                        else if (currentMetric === 'TONELADA' || currentMetric === 'META KG') metricKey = 'VOL';
                        else if (currentMetric === 'META MIX' || currentMetric === 'MIX' || currentMetric === 'QTD') metricKey = 'MIX';

                        const key = `${catKey}_${metricKey}_${subMetric}`;
                        colMap[key] = i;
                    }
                }

                dataStartRow = startRow + 3;
            } else {
                console.warn("[Parser] Menos de 3 linhas. Tentando modo simplificado...");
                // Simplified Mode: Hardcoded Column Map based on Standard Export
                // Columns structure matches exportGoalsSvXLSX

                let colIdx = 2; // Start after Vendedor (Index 2)
                const addKeys = (cat, keys) => {
                    keys.forEach((k, i) => {
                        if (k) colMap[`${cat}_${k}`] = colIdx + i;
                    });
                    colIdx += keys.length;
                };

                // 1. TOTAL ELMA (Standard Agg)
                addKeys('total_elma', ['FAT_META', 'FAT_AJUSTE', 'POS_META', 'POS_AJUSTE']);
                // 2. EXTRUSADOS (707)
                addKeys('707', ['FAT_META', 'FAT_AJUSTE', 'POS_META', 'POS_AJUSTE']);
                // 3. NÃO EXTRUSADOS (708)
                addKeys('708', ['FAT_META', 'FAT_AJUSTE', 'POS_META', 'POS_AJUSTE']);
                // 4. TORCIDA (752)
                addKeys('752', ['FAT_META', 'FAT_AJUSTE', 'POS_META', 'POS_AJUSTE']);
                // 5. KG ELMA (tonnage)
                addKeys('tonelada_elma', [null, 'VOL_VOLUME', 'VOL_AJUSTE']);
                // 6. MIX SALTY (mix)
                addKeys('mix_salty', [null, 'MIX_META', 'MIX_AJUSTE']);

                // 7. TOTAL FOODS (Standard Agg)
                addKeys('total_foods', ['FAT_META', 'FAT_AJUSTE', 'POS_META', 'POS_AJUSTE']);
                // 8. TODDYNHO (1119_TODDYNHO)
                addKeys('1119_TODDYNHO', ['FAT_META', 'FAT_AJUSTE', 'POS_META', 'POS_AJUSTE']);
                // 9. TODDY (1119_TODDY)
                addKeys('1119_TODDY', ['FAT_META', 'FAT_AJUSTE', 'POS_META', 'POS_AJUSTE']);
                // 10. QUAKER/KEROCOCO
                addKeys('1119_QUAKER_KEROCOCO', ['FAT_META', 'FAT_AJUSTE', 'POS_META', 'POS_AJUSTE']);
                // 11. KG FOODS (tonnage)
                addKeys('tonelada_foods', [null, 'VOL_VOLUME', 'VOL_AJUSTE']);
                // 12. MIX FOODS (mix)
                addKeys('mix_foods', [null, 'MIX_META', 'MIX_AJUSTE']);

                // 13. GERAL (pepsico_all)
                addKeys('pepsico_all', [null, 'FAT_META', 'VOL_META', 'POS_META']);

                // 14. PEDEV
                colIdx += 1;

                dataStartRow = 0; // Parse all rows
            }

            const updates = [];
            const processedSellers = new Set();

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
                    // Has comma, no dot. Assume comma is decimal in Brazil
                    // However, if the user exported raw CSV without decimals for thousands (e.g. 1,234 meaning 1234)
                    // it is highly ambiguous. In this system, we mostly use Brazilian format (1,234 is 1.234)
                    // We remove the old /,\d{3}$/ check because volume is often 3 decimals (e.g., 2,600 kg = 2.6).
                    clean = clean.replace(',', '.');
                } else if (dotIdx > -1) {
                    // Has dot, no comma.
                    // If it has multiple dots, they are definitely thousands separators.
                    if (clean.split('.').length > 2) {
                        clean = clean.replace(/\./g, '');
                    } else {
                        // Single dot. In raw Excel data or standard floats (e.g., 1359.041), this is a decimal point.
                        // We DO NOT remove it. Removing it would inflate the value 1000x for volume.
                    }
                }
                return parseFloat(clean);
            };
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
                const upperName = sellerName.normalize('NFD').replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();

                // 1. Explicit Blocklist
                if (upperName === 'BALCAO' || upperName === 'BALCÃO' ||
                    upperName.includes('TOTAL') || upperName.includes('SUPERVISOR') || upperName.includes('GERAL') ||
                    upperName === 'VENDEDOR' || upperName === 'NOME' || upperName === 'CODIGO' || upperName === 'CÓDIGO') {
                    continue;
                }

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
                    // FIX: Return Default Calculated Value if Manual Target is Missing
                    if (targets && targets[category] !== undefined) {
                        return targets[category];
                    } else {
                        // Calculate Default
                        const defaults = calculateSellerDefaults(sellerName);
                        if (category === 'total_elma') return defaults.elmaPos;
                        if (category === 'total_foods') return defaults.foodsPos;
                        if (category === 'mix_salty') return defaults.mixSalty;
                        if (category === 'mix_foods') return defaults.mixFoods;
                        // Fallback for leaf components? Currently Pos adjustments are manual.
                        // If category is a leaf (e.g. 707), default adjustment is 0 (Natural Base is not stored here).
                        // Note: parseGoalsSvStructure sends '707', '708' etc for Positivação.
                        // We assume 0 for leaf adjustments if not set.
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
                        return true; // return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
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
                        const upperUName = u.seller.normalize('NFD').replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
                        if (upperUName === 'BALCAO' || upperUName === 'BALCÃO' ||
                            upperUName.includes('TOTAL') || upperUName.includes('SUPERVISOR') || upperUName.includes('GERAL') ||
                            optimizedData.rcasBySupervisor.has(upperUName) || optimizedData.rcasBySupervisor.has(u.seller)) {
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
                    pendingImportUpdates.forEach(u => {
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
                    // We get active sellers from optimizedData.rcasBySupervisor
                    const activeSellerNames = new Set();
                    const forbiddenBackfill = ['INATIVOS', 'N/A', 'BALCAO', 'BALCÃO', 'TOTAL', 'GERAL'];
                    const supervisorNames = new Set(optimizedData.rcasBySupervisor.keys());

                    optimizedData.rcasBySupervisor.forEach(rcas => {
                        rcas.forEach(code => {
                            const name = optimizedData.rcaNameByCode.get(code);
                            if (name) {
                                const upper = name.toUpperCase();
                                // Ensure we exclude supervisors themselves from being treated as sellers
                                // and exclude forbidden keywords
                                if (!forbiddenBackfill.some(f => upper.includes(f)) && !supervisorNames.has(name)) {
                                    activeSellerNames.add(name);
                                }
                            }
                        });
                    });

                    activeSellerNames.forEach(sellerName => {
                        if (!goalsSellerTargets.has(sellerName)) goalsSellerTargets.set(sellerName, {});
                        const targets = goalsSellerTargets.get(sellerName);

                        // Calculate Defaults
                        const defaults = calculateSellerDefaults(sellerName);

                        // Backfill if missing
                        if (targets['total_elma'] === undefined) targets['total_elma'] = defaults.elmaPos;
                        if (targets['total_foods'] === undefined) targets['total_foods'] = defaults.foodsPos;
                        if (targets['mix_salty'] === undefined) targets['mix_salty'] = defaults.mixSalty;
                        if (targets['mix_foods'] === undefined) targets['mix_foods'] = defaults.mixFoods;
                    });

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
                ['goals-view'].forEach(id => {
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
                    window.aiFullPageChartInstance.destroy();
                }

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

document.getElementById('ai-insights-back-btn')?.addEventListener('click', () => {
                document.getElementById('ai-insights-full-page').classList.add('hidden');
                // Restore Dashboard (or Goals View specifically since we came from there)
                // Default to Goals View since the Import Modal is there
                navigateTo('goals');

                // Re-open the Import Modal to preserve context/flow
                const modal = document.getElementById('import-goals-modal');
                if (modal) modal.classList.remove('hidden');
            });
})();
