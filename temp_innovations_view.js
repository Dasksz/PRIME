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
