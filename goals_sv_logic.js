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
                    const metaPos = t.fat > 1 ? 1 : 0;

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
                if (clientElmaFat > 1) sellerObj.elmaPos++;

                let clientFoodsFat = (clientCatTotals['1119_TODDYNHO']?.fat || 0) + (clientCatTotals['1119_TODDY']?.fat || 0) + (clientCatTotals['1119_QUAKER_KEROCOCO']?.fat || 0);
                if (clientFoodsFat > 1) sellerObj.foodsPos++;

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
                        if (totalFat > 1) activeClientsCount++;
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
                        if (sale.VLVENDA > 1) {
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
                const gearIcon = `<svg class="w-3 h-3 mx-auto text-yellow-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>`;
                svColumns.forEach(col => {
                    if (col.type === 'standard') {
                        quarterMonths.forEach(m => headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal w-12">${m.label}</th>`);
                        headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Média</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">${gearIcon}</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">${gearIcon}</th>`;
                    } else if (col.type === 'tonnage') headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Volume</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Volume</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">${gearIcon}</th>`;
                    else if (col.type === 'mix') headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Qtd</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">${gearIcon}</th>`;
                    else if (col.type === 'geral') headerHTML += `<th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Média Trim.</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-right border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th><th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">Meta</th>`;
                    else if (col.type === 'pedev') headerHTML += `<th class="px-1 py-1 text-center border-r border-b border-slate-700 text-slate-500 font-normal">${gearIcon}</th>`;
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
                                const isReadOnly = col.isAgg; const inputClass = isReadOnly ? 'text-slate-400 font-bold opacity-70' : 'text-yellow-300'; const readonlyAttr = isReadOnly ? 'readonly' : ''; const cellBg = isReadOnly ? 'bg-[#151c36]' : 'bg-[#1e293b]';
                                quarterMonths.forEach(m => bodyHTML += `<td class="px-1 py-1 text-right text-slate-400 border-r border-slate-800/50 text-[10px] bg-blue-900/5">${(d.monthlyValues[m.key] || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>`);
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 bg-blue-900/10 font-medium">${d.avgFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 text-right ${col.colorClass} border-r border-slate-800/50 text-xs font-mono">${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td><td class="px-1 py-1 ${cellBg} border-r border-slate-800/50"><input type="text" value="${d.metaFat.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}" class="goals-sv-input bg-transparent text-right w-full outline-none ${inputClass} text-xs font-mono" data-sup-id="${sup.id}" data-col-id="${col.id}" data-seller-id="${seller.id || seller.name.replace(/\s+/g,'_')}" data-field="fat" oninput="recalculateGoalsSvTotals(this)" ${readonlyAttr}></td><td class="px-1 py-1 text-center text-slate-300 border-r border-slate-800/50">${d.metaPos}</td><td class="px-1 py-1 ${cellBg} border-r border-slate-800/50"><input type="text" value="${d.metaPos}" class="goals-sv-input bg-transparent text-center w-full outline-none ${inputClass} text-xs font-mono" data-sup-id="${sup.id}" data-col-id="${col.id}" data-seller-id="${seller.id || seller.name.replace(/\s+/g,'_')}" data-field="pos" oninput="recalculateGoalsSvTotals(this)" ${readonlyAttr}></td>`;
                            } else if (col.type === 'tonnage') {
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-mono text-xs">${d.avgVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-bold font-mono text-xs">${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} Kg</td><td class="px-1 py-1 bg-[#1e293b] border-r border-slate-800/50"><input type="text" value="${d.metaVol.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}" class="goals-sv-input bg-transparent text-right w-full outline-none text-yellow-300 text-xs font-mono" data-sup-id="${sup.id}" data-col-id="${col.id}" data-field="vol" oninput="recalculateGoalsSvTotals(this)"></td>`;
                            } else if (col.type === 'mix') {
                                bodyHTML += `<td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50">${d.avgMix.toLocaleString('pt-BR', {minimumFractionDigits: 1, maximumFractionDigits: 1})}</td><td class="px-1 py-1 text-right text-slate-300 border-r border-slate-800/50 font-bold">${d.metaMix}</td><td class="px-1 py-1 bg-[#1e293b] border-r border-slate-800/50"><input type="text" value="${d.metaMix}" class="goals-sv-input bg-transparent text-right w-full outline-none text-yellow-300 text-xs font-mono" data-sup-id="${sup.id}" data-col-id="${col.id}" data-field="mix" oninput="recalculateGoalsSvTotals(this)"></td>`;
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
