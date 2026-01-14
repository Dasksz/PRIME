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
            // Use same active filter as updateGoalsSvView
            const activeClients = clients.filter(c => {
                const cod = String(c['Código'] || c['codigo_cliente']);
                const rca1 = String(c.rca1 || '').trim();
                const isAmericanas = (c.razaoSocial || '').toUpperCase().includes('AMERICANAS');
                return (isAmericanas || rca1 !== '53' || clientsWithSalesThisMonth.has(cod));
            });

            // Calculate Positivation Base (Unique Clients buying > 1 in History)
            // Using allHistoryData (optimizedData.historyById)
            // We need to iterate History for these active clients.

            // Reusing logic from updateGoalsSvView
            // Optimized: We can iterate clientHistoryIds directly

            activeClients.forEach(client => {
                const codCli = String(client['Código'] || client['codigo_cliente']);
                const historyIds = optimizedData.indices.history.byClient.get(codCli);

                if (historyIds) {
                    let clientElmaFat = 0;
                    let clientFoodsFat = 0;

                    historyIds.forEach(id => {
                        const sale = optimizedData.historyById.get(id);
                        // EXCEPTION: Exclude Balcão (53) sales for Client 9569
                        if (String(codCli).trim() === '9569' && (String(sale.CODUSUR).trim() === '53' || String(sale.CODUSUR).trim() === '053')) return;

                        const isRev = (sale.TIPOVENDA === '1' || sale.TIPOVENDA === '9');
                        if (!isRev) return;

                        const codFor = String(sale.CODFOR);
                        const desc = (sale.DESCRICAO || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();

                        if (codFor === '707' || codFor === '708' || codFor === '752') {
                            clientElmaFat += sale.VLVENDA;
                        } else if (codFor === '1119') {
                            if (desc.includes('TODDYNHO') || desc.includes('TODDY') || desc.includes('QUAKER') || desc.includes('KEROCOCO')) {
                                clientFoodsFat += sale.VLVENDA;
                            }
                        }
                    });

                    if (clientElmaFat >= 1) defaults.elmaPos++;
                    if (clientFoodsFat >= 1) defaults.foodsPos++;
                }
            });

            // Calculate Mix Targets
            // Base: Active Elma Clients + ELMA Adjustment (from global `goalsPosAdjustments`)
            const elmaAdj = goalsPosAdjustments['ELMA_ALL'] ? (goalsPosAdjustments['ELMA_ALL'].get(sellerName) || 0) : 0;
            const elmaBase = defaults.elmaPos + elmaAdj;

            defaults.mixSalty = Math.round(elmaBase * 0.50);
            defaults.mixFoods = Math.round(elmaBase * 0.30);

            if (sellerCode === '1001') {
                defaults.mixSalty = 0;
                defaults.mixFoods = 0;
            }

            return defaults;
        }
