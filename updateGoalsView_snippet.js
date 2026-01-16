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
