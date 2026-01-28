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
