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

            // --- Meta Vs Realizado Listeners ---
            const updateMetaRealizado = () => {
                markDirty('metaRealizado');
                updateMetaRealizadoView();
            };

            const debouncedUpdateMetaRealizado = debounce(updateMetaRealizado, 400);

            // Supervisor Filter
            const metaRealizadoSupervisorFilterBtn = document.getElementById('meta-realizado-supervisor-filter-btn');
            const metaRealizadoSupervisorFilterDropdown = document.getElementById('meta-realizado-supervisor-filter-dropdown');
            metaRealizadoSupervisorFilterBtn.addEventListener('click', () => metaRealizadoSupervisorFilterDropdown.classList.toggle('hidden'));
            metaRealizadoSupervisorFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) selectedMetaRealizadoSupervisors.push(value);
                    else selectedMetaRealizadoSupervisors = selectedMetaRealizadoSupervisors.filter(s => s !== value);

                    selectedMetaRealizadoSupervisors = updateSupervisorFilter(metaRealizadoSupervisorFilterDropdown, document.getElementById('meta-realizado-supervisor-filter-text'), selectedMetaRealizadoSupervisors, allSalesData);

                    // Reset or Filter Sellers
                    selectedMetaRealizadoSellers = [];
                    selectedMetaRealizadoSellers = updateSellerFilter(selectedMetaRealizadoSupervisors, document.getElementById('meta-realizado-vendedor-filter-dropdown'), document.getElementById('meta-realizado-vendedor-filter-text'), selectedMetaRealizadoSellers, [...allSalesData, ...allHistoryData]);

                    debouncedUpdateMetaRealizado();
                }
            });

            // Seller Filter
            const metaRealizadoSellerFilterBtn = document.getElementById('meta-realizado-vendedor-filter-btn');
            const metaRealizadoSellerFilterDropdown = document.getElementById('meta-realizado-vendedor-filter-dropdown');
            metaRealizadoSellerFilterBtn.addEventListener('click', () => metaRealizadoSellerFilterDropdown.classList.toggle('hidden'));
            metaRealizadoSellerFilterDropdown.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    const { value, checked } = e.target;
                    if (checked) {
                        if (!selectedMetaRealizadoSellers.includes(value)) selectedMetaRealizadoSellers.push(value);
                    } else {
                        selectedMetaRealizadoSellers = selectedMetaRealizadoSellers.filter(s => s !== value);
                    }
                    debouncedUpdateMetaRealizado();
                }
            });

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
                selectedMetaRealizadoSupervisors = [];
                selectedMetaRealizadoSellers = [];
                selectedMetaRealizadoSuppliers = [];
                currentMetaRealizadoPasta = 'PEPSICO'; // Reset to default

                // Reset UI
                updateSupervisorFilter(metaRealizadoSupervisorFilterDropdown, document.getElementById('meta-realizado-supervisor-filter-text'), [], allSalesData);
                updateSellerFilter([], metaRealizadoSellerFilterDropdown, document.getElementById('meta-realizado-vendedor-filter-text'), [], allSalesData);

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
                if (!metaRealizadoSupervisorFilterBtn.contains(e.target) && !metaRealizadoSupervisorFilterDropdown.contains(e.target)) metaRealizadoSupervisorFilterDropdown.classList.add('hidden');
                if (!metaRealizadoSellerFilterBtn.contains(e.target) && !metaRealizadoSellerFilterDropdown.contains(e.target)) metaRealizadoSellerFilterDropdown.classList.add('hidden');
                if (!metaRealizadoSupplierFilterBtn.contains(e.target) && !metaRealizadoSupplierFilterDropdown.contains(e.target)) metaRealizadoSupplierFilterDropdown.classList.add('hidden');
            });

            // Pagination Listeners for Meta Realizado Clients Table
            document.getElementById('meta-realizado-clients-prev-page-btn').addEventListener('click', () => {
                if (metaRealizadoClientsTableState.currentPage > 1) {
                    metaRealizadoClientsTableState.currentPage--;
                    updateMetaRealizadoView();
                }
            });
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
        selectedGoalsGvSellers = updateSellerFilter(selectedGoalsGvSupervisors, goalsGvSellerFilterDropdown, goalsGvSellerFilterText, selectedGoalsGvSellers, [...allSalesData, ...allHistoryData]);
        // Initialize SV Supervisor filter just in case
        selectedGoalsSvSupervisors = updateSupervisorFilter(goalsSvSupervisorFilterDropdown, goalsSvSupervisorFilterText, selectedGoalsSvSupervisors, allSalesData);
        // Initialize Summary Supervisor Filter
        if(document.getElementById('goals-summary-supervisor-filter-dropdown')) {
            selectedGoalsSummarySupervisors = updateSupervisorFilter(document.getElementById('goals-summary-supervisor-filter-dropdown'), document.getElementById('goals-summary-supervisor-filter-text'), selectedGoalsSummarySupervisors, allSalesData);
        }

        // Initialize Meta Vs Realizado Filters
        selectedMetaRealizadoSupervisors = updateSupervisorFilter(document.getElementById('meta-realizado-supervisor-filter-dropdown'), document.getElementById('meta-realizado-supervisor-filter-text'), selectedMetaRealizadoSupervisors, allSalesData);
        updateSellerFilter(selectedMetaRealizadoSupervisors, document.getElementById('meta-realizado-vendedor-filter-dropdown'), document.getElementById('meta-realizado-vendedor-filter-text'), selectedMetaRealizadoSellers, [...allSalesData, ...allHistoryData]);

        // Fix: Pre-filter Suppliers for Meta Realizado (Only PEPSICO)
        const pepsicoSuppliersSource = [...allSalesData, ...allHistoryData].filter(s => {
            let rowPasta = s.OBSERVACAOFOR;
            if (!rowPasta || rowPasta === '0' || rowPasta === '00' || rowPasta === 'N/A') {
                 const rawFornecedor = String(s.FORNECEDOR || '').toUpperCase();
                 rowPasta = rawFornecedor.includes('PEPSICO') ? 'PEPSICO' : 'MULTIMARCAS';
            }
            return rowPasta === 'PEPSICO';
        });
        selectedMetaRealizadoSuppliers = updateSupplierFilter(document.getElementById('meta-realizado-supplier-filter-dropdown'), document.getElementById('meta-realizado-supplier-filter-text'), selectedMetaRealizadoSuppliers, pepsicoSuppliersSource, 'metaRealizado');

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
                // Note: user said "check if first week passed... then redistribute difference".
                // If we are IN week 2, week 1 is past.
                // Assuming lastSaleDate represents "today".

                const isPast = week.end < currentDate;
                const dailyGoal = totalGoal / totalWorkingDays;
                let originalWeekGoal = dailyGoal * week.workingDays;

                if (isPast) {
                    // Week is closed. The goal is fixed? No, the user says:
                    // "case in the first week the goal that was 40k wasn't hit (realized 30k), the 10k missing must be reassigned"
                    // This implies the "Goal" for the past week stays as original or realized?
                    // Usually in these reports, you show the original goal for past weeks, and the *future* weeks get adjusted.
                    // Or does the user want to see the "gap" moved?
                    // "the 10.000 missing must be reassigned to other weeks"
                    // This means the TARGET for future weeks increases. The displayed Goal for past week usually remains static (Original) so you can see the failure.
                    // HOWEVER, if I change the future goals, the sum of all displayed goals will be Total Goal.
                    // If I show Original Goal for W1 (40k) and Realized (30k), I have a deficit of 10k.
                    // If I add 10k to W2, W3, W4...
                    // The sum of displayed goals would be: 40k (W1) + (Original W2 + share of 10k) + ... = Total Goal + 10k.
                    // This is mathmatically wrong if "Meta Total" column shows the fixed monthly target.
                    // BUT, dynamic planning often works this way: "To hit the month, now I need to do X".
                    // The user said: "as colunas onde mostram as metas por semana fossem dinâmicas de acordo com o realizado"

                    // Let's assume:
                    // Displayed Goal for Past Week = Original Proportional Goal (so you can see the variance).
                    // Displayed Goal for Future Week = Original + Redistribution.

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
