        self.importScripts('https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js');
        self.importScripts('https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js');

        const FORBIDDEN_KEYS = ['SUPERV', 'CODUSUR', 'CODSUPERVISOR', 'NOME', 'CODCLI', 'PRODUTO', 'DESCRICAO', 'FORNECEDOR', 'OBSERVACAOFOR', 'CODFOR', 'QTVENDA', 'VLVENDA', 'VLBONIFIC', 'TOTPESOLIQ', 'ESTOQUEUNIT', 'TIPOVENDA', 'FILIAL', 'ESTOQUECX', 'SUPERVISOR', 'PASTA', 'RAMO', 'ATIVIDADE', 'CIDADE', 'MUNICIPIO', 'BAIRRO'];

        const mandatoryColumns = {
            sales: ['CODCLI', 'PEDIDO', 'CODUSUR', 'CODSUPERVISOR', 'DTPED', 'DTSAIDA', 'PRODUTO', 'DESCRICAO', 'FORNECEDOR', 'CODFOR', 'QTVENDA', 'VLVENDA', 'VLBONIFIC', 'TOTPESOLIQ', 'ESTOQUEUNIT', 'TIPOVENDA', 'FILIAL', 'ESTOQUECX'],
            clients: [], // Validation relaxed to support variable headers (aliases handled in processing)
            products: ['Código', 'Qtde embalagem master(Compra)', 'Descrição', 'Nome do fornecedor', 'Fornecedor', 'Dt.Cadastro'],
            history: ['CODCLI', 'NOME', 'SUPERV', 'PEDIDO', 'CODUSUR', 'CODSUPERVISOR', 'DTPED', 'DTSAIDA', 'PRODUTO', 'DESCRICAO', 'FORNECEDOR', 'OBSERVACAOFOR', 'CODFOR', 'QTVENDA', 'VLVENDA', 'VLBONIFIC', 'TOTPESOLIQ', 'POSICAO', 'ESTOQUEUNIT', 'TIPOVENDA', 'FILIAL', 'ESTOQUECX']
        };

        const columnFormats = {
            sales: {
                'CODCLI': 'number',
                'QTVENDA': 'number',
                'VLVENDA': 'number',
                'VLBONIFIC': 'number',
                'TOTPESOLIQ': 'number',
                'DTPED': 'date',
                'DTSAIDA': 'date',
                'PEDIDO': 'number',
                'TIPOVENDA': 'number',
                'FILIAL': 'number',
                'ESTOQUEUNIT': 'number',
                'ESTOQUECX': 'number'
            },
            clients: {
                'Código': 'number',
                'Data da Última Compra': 'date',
                'Data e Hora de Cadastro': 'date'
            },
            products: {
                'Código': 'number',
                'Qtde embalagem master(Compra)': 'number',
                'Dt.Cadastro': 'date'
            },
            history: {
                'CODCLI': 'number',
                'QTVENDA': 'number',
                'VLVENDA': 'number',
                'VLBONIFIC': 'number',
                'TOTPESOLIQ': 'number',
                'DTPED': 'date',
                'DTSAIDA': 'date',
                'PEDIDO': 'number',
                'TIPOVENDA': 'number',
                'FILIAL': 'number',
                'ESTOQUEUNIT': 'number',
                'ESTOQUECX': 'number'
            }
        };

        function parseDate(dateString) {
            if (!dateString) return null;
            if (dateString instanceof Date) {
                return !isNaN(dateString.getTime()) ? dateString : null;
            }
            if (typeof dateString === 'number') {
                // Fix: Check if number is a JS Timestamp (large number) or Excel Serial (small number)
                // Excel dates are around 45000 (for year 2023). Timestamps are > 1.6e12
                if (dateString > 1000000) {
                    return new Date(dateString);
                }
                return new Date(Math.round((dateString - 25569) * 86400 * 1000));
            }
            if (typeof dateString !== 'string') return null;

            // NEW: Handle datetime strings by taking only the date part first.
            const dateOnlyString = dateString.split(' ')[0];

            // Handle "DD/MM/YYYY" format specifically.
            if (dateOnlyString.length === 10 && dateOnlyString.charAt(2) === '/' && dateOnlyString.charAt(5) === '/') {
                const [day, month, year] = dateOnlyString.split('/');
                if (year && month && day && year.length === 4) {
                    // Use UTC to avoid timezone issues when only date is provided.
                    // Using Date.UTC ensures the date is created at 00:00:00 UTC directly.
                    const utcDate = new Date(Date.UTC(parseInt(year), parseInt(month) - 1, parseInt(day)));
                    if (!isNaN(utcDate.getTime())) {
                        return utcDate;
                    }
                }
            }

            // Fallback for ISO 8601 or other formats that new Date() can parse.
            // Try parsing the original string first, in case it's a valid ISO datetime.
            const isoDate = new Date(dateString);
            if (!isNaN(isoDate.getTime())) {
                return isoDate;
            }

            // If that fails, try parsing just the date part.
            const isoDateFromDateOnly = new Date(dateOnlyString);
            return !isNaN(isoDateFromDateOnly.getTime()) ? isoDateFromDateOnly : null;
        }

        function parseBrazilianNumber(value) {
            if (typeof value === 'number') return value;
            if (typeof value !== 'string' || !value) return 0;

            let numberString = value.replace(/R\$\s?/g, '').trim();

            const lastComma = numberString.lastIndexOf(',');
            const lastDot = numberString.lastIndexOf('.');

            if (lastComma > lastDot) {
                numberString = numberString.replace(/\./g, '').replace(',', '.');
            } else {
                numberString = numberString.replace(/,/g, '');
            }

            const number = parseFloat(numberString);
            return isNaN(number) ? 0 : number;
        }

        function isValidDate(dateString) {
            return parseDate(dateString) !== null;
        }

        function isValidNumber(value) {
            return !isNaN(parseBrazilianNumber(String(value)));
        }

        function normalizeKey(key) {
            if (!key) return '';
            const s = String(key).trim();
            // Remove leading zeros if it's a numeric string
            if (/^\d+$/.test(s)) {
                return String(parseInt(s, 10));
            }
            return s;
        }

        function validateData(data, fileType, fileName) {
            if (!data || data.length === 0) {
                throw new Error(`O arquivo '${fileName}' (${fileType}) está vazio ou não contém dados.`);
            }

            const requiredColumns = mandatoryColumns[fileType];
            const formats = columnFormats[fileType];
            if (!requiredColumns) return;

            // Scan first 50 rows to detect available columns (headers)
            // because the first row might be a partial "stock line" lacking some columns.
            const headersSet = new Set();
            const scanLimit = Math.min(data.length, 50);
            for (let i = 0; i < scanLimit; i++) {
                Object.keys(data[i]).forEach(k => headersSet.add(k));
            }
            const headers = Array.from(headersSet);

            const missingColumns = requiredColumns.filter(col => !headers.includes(col));

            if (missingColumns.length > 0) {
                throw new Error(`Erro no arquivo '${fileName}'. Colunas obrigatórias não encontradas: ${missingColumns.join(', ')}.`);
            }

            const emptyOrZeroColumns = requiredColumns.filter(col => {
                return data.every(row => {
                    const value = row[col];
                    return value === null || value === undefined || String(value).trim() === '' || String(value).trim() === '0';
                });
            });

            if (emptyOrZeroColumns.length > 0) {
                throw new Error(`Erro no arquivo '${fileName}'. As seguintes colunas estão vazias ou contêm apenas zeros: ${emptyOrZeroColumns.join(', ')}.`);
            }

            if (formats) {
                for (let i = 0; i < data.length; i++) {
                    const row = data[i];
                    for (const col in formats) {
                        if (row.hasOwnProperty(col)) {
                            const value = row[col];
                            const format = formats[col];
                            let isValid = true;

                            if (value !== null && value !== undefined && String(value).trim() !== '') {
                                if (format === 'number') {
                                    isValid = isValidNumber(value);
                                } else if (format === 'date') {
                                    isValid = isValidDate(value);
                                }
                            }

                            if (!isValid) {
                                // EXCEÇÃO: Linhas de Estoque (com Produto mas sem Vendedor) podem ter datas inválidas/zeros. Ignorar.
                                const codUsurCheck = String(row['CODUSUR'] || '').trim();
                                const prodCheck = String(row['PRODUTO'] || '').trim();
                                if ((codUsurCheck === '' || codUsurCheck === '0' || codUsurCheck === '00') && prodCheck !== '') {
                                    continue;
                                }
                                throw new Error(`Erro no arquivo '${fileName}', linha ${i + 2}. A coluna '${col}' contém um valor inválido: '${value}'. O formato esperado é '${format}'.`);
                            }
                        }
                    }
                }
            }
        }

        const readFile = (file, fileType) => {
            return new Promise((resolve, reject) => {
                if (!file) {
                    if (fileType !== 'innovations') {
                         reject(new Error(`Arquivo obrigatório para '${fileType}' não foi fornecido.`));
                         return;
                    }
                    resolve([]);
                    return;
                }

                if (file.name.endsWith('.csv')) {
                     // Read file content first to handle encoding robustly (reusing original logic)
                     const textReader = new FileReader();
                     textReader.onload = (e) => {
                         const buffer = e.target.result;
                         let decodedData;
                         try {
                             decodedData = new TextDecoder('utf-8', { fatal: true }).decode(new Uint8Array(buffer));
                         } catch (err) {
                             decodedData = new TextDecoder('iso-8859-1').decode(new Uint8Array(buffer));
                         }

                         // Use PapaParse on the decoded string
                         Papa.parse(decodedData, {
                             header: true,
                             skipEmptyLines: true,
                             complete: (results) => {
                                 try {
                                     let jsonData = results.data;
                                     if (jsonData.length === 0 && fileType !== 'innovations') {
                                          throw new Error(`O arquivo CSV '${file.name}' está vazio.`);
                                     }

                                     // Validate
                                     if (fileType !== 'innovations') {
                                        validateData(jsonData, fileType, file.name);
                                     }
                                     resolve(jsonData);
                                 } catch (err) {
                                     reject(err);
                                 }
                             },
                             error: (err) => {
                                 reject(new Error(`Erro no parsing CSV: ${err.message}`));
                             }
                         });
                     };
                     textReader.onerror = () => reject(new Error(`Erro ao ler o arquivo '${file.name}'.`));
                     textReader.readAsArrayBuffer(file);
                     return;
                }

                // XLSX Fallback
                const reader = new FileReader();
                reader.onload = (event) => {
                    try {
                        const data = event.target.result;
                        const workbook = XLSX.read(new Uint8Array(data), {type: 'array'});
                        const firstSheetName = workbook.SheetNames[0];
                        const worksheet = workbook.Sheets[firstSheetName];
                        const jsonData = XLSX.utils.sheet_to_json(worksheet, { raw: false, cellDates: true });

                        if (fileType !== 'innovations') {
                            validateData(jsonData, fileType, file.name);
                        }

                        resolve(jsonData);
                    } catch (error) {
                        reject(error);
                    }
                };
                reader.onerror = () => reject(new Error(`Erro ao ler o arquivo '${file.name}'.`));
                reader.readAsArrayBuffer(file);
            });
        };

        const processSalesData = (rawData, clientMap, productMasterMap, newRcaSupervisorMap, stockLinesCollector = null, fallbackDate = null) => {
            return rawData.map(rawRow => {
                // --- HEADER DETECTION: Ignore rows that look like headers ---
                // Enhanced robust detection: Check against forbidden keys list
                const checkHeader = (val) => val && FORBIDDEN_KEYS.includes(val.trim().toUpperCase());

                if (
                    checkHeader(String(rawRow['CODCLI'] || '')) ||
                    checkHeader(String(rawRow['PRODUTO'] || '')) ||
                    checkHeader(String(rawRow['SUPERV'] || '')) ||
                    checkHeader(String(rawRow['NOME'] || '')) ||
                    String(rawRow['CODCLI'] || '').trim().toUpperCase() === 'CODCLI' ||
                    String(rawRow['PRODUTO'] || '').trim().toUpperCase() === 'PRODUTO' ||
                    String(rawRow['PRODUTO'] || '').trim().toUpperCase() === 'CÓDIGO' ||
                    String(rawRow['SUPERV'] || '').trim().toUpperCase() === 'SUPERV' ||
                    String(rawRow['NOME'] || '').trim().toUpperCase() === 'NOME' ||
                    String(rawRow['TIPOVENDA'] || '').trim().toUpperCase() === 'TIPOVENDA'
                ) {
                    return null;
                }

                // --- NOVA LÓGICA: Derivação de OBSERVACAOFOR se vazio ---
                let observacaoFor = String(rawRow['OBSERVACAOFOR'] || '').trim();

                // Trata '0', '00' ou vazio como inválido
                if (!observacaoFor || observacaoFor === '0' || observacaoFor === '00') {
                    const fornecedorUpper = String(rawRow['FORNECEDOR'] || '').toUpperCase();
                    if (fornecedorUpper.includes('PEPSICO')) {
                        observacaoFor = 'PEPSICO';
                    } else {
                        observacaoFor = 'MULTIMARCAS';
                    }
                }
                // --------------------------------------------------------

                // --- INÍCIO DA MODIFICAÇÃO: FILTRO E CAPTURA DE LINHAS DE ESTOQUE (SEM VENDEDOR) ---
                const productCheck = String(rawRow['PRODUTO'] || '').trim();
                const codUsurCheck = String(rawRow['CODUSUR'] || '').trim();

                // Se existe produto mas não tem vendedor, é uma linha apenas de estoque (inserida artificialmente)
                if (productCheck !== '' && (codUsurCheck === '' || codUsurCheck === '0' || codUsurCheck === '00')) {
                    if (stockLinesCollector) {
                        // Captura as informações da linha para uso posterior (ex: enriquecer cadastro de produtos)
                        // Substitui a data (que pode estar bugada/zerada) pela data mais recente válida (fallbackDate)
                        stockLinesCollector.push({
                            PRODUTO: productCheck,
                            DESCRICAO: String(rawRow['DESCRICAO'] || ''),
                            FORNECEDOR: String(rawRow['FORNECEDOR'] || ''),
                            OBSERVACAOFOR: observacaoFor,
                            CODFOR: String(rawRow['CODFOR'] || ''),
                            ESTOQUECX: parseBrazilianNumber(rawRow['ESTOQUECX']),
                            DTPED: fallbackDate, // Data sanitizada
                            PASTA: observacaoFor // Adiciona PASTA explicitamente
                        });
                    }
                    return null; // Remove da listagem principal de vendas
                }
                // --- FIM DA MODIFICAÇÃO ---

                // --- INTEGRATED: VIRTUAL FORKING FOR CLIENT 9569 ---
                let codCliOriginal = normalizeKey(rawRow['CODCLI']);
                const rcaCheck = String(rawRow['CODUSUR'] || '').trim();
                if (codCliOriginal === '9569' && (rcaCheck === '53' || rcaCheck === '053')) {
                    codCliOriginal = '7706';
                }

                let codCliStr = codCliOriginal;
                const clientInfo = clientMap.get(codCliStr) || {};

                // --- INÍCIO DA MODIFICAÇÃO: LÓGICA DE ATRIBUIÇÃO DE VENDEDOR ---

                // 1. Pega os valores padrão da linha de venda
                let vendorName = String(rawRow['NOME'] || '').trim();
                let supervisorName = String(rawRow['SUPERV'] || '').trim();
                let codUsur = String(rawRow['CODUSUR'] || '').trim(); // Este é o 'codUsurVenda'
                let codSupervisor = String(rawRow['CODSUPERVISOR'] || '').trim();
                const pedido = String(rawRow['PEDIDO'] || '');
                let isAmericanas = false;

                // 2. Regra de Prioridade 1: AMERICANAS S.A (Como estava antes)
                const nomeClienteParaLogica = (clientInfo.razaoSocial || clientInfo.fantasia || clientInfo.nomeCliente || '').toUpperCase();
                if (nomeClienteParaLogica.includes('AMERICANAS S.A')) {
                    vendorName = 'AMERICANAS';
                    codUsur = '1001';
                    supervisorName = 'BALCAO';
                    codSupervisor = '';
                    isAmericanas = true;

                    if (clientInfo.rcas) {
                         clientInfo.rca1 = '1001';
                         if (!clientInfo.rcas.includes('1001')) {
                            clientInfo.rcas.unshift('1001');
                         }
                    }
                }

                // 3. Regra de Prioridade 2: Se NÃO for Americanas, usa a nova lógica
                // EXCEÇÃO: Vendas do Cliente 9569 para RCA 53 (Balcão) com Tipo 1 ou 9 devem permanecer no RCA 53
                const codUsurVendaCheck = String(rawRow['CODUSUR'] || '').trim();
                const tipoVendaCheck = String(rawRow['TIPOVENDA'] || 'N/A').trim();

                // Lógica para preservar RCA original em vendas do mês atual
                const isCurrentSales = stockLinesCollector !== null;
                const clientExists = clientMap.has(codCliStr);
                const rca1Cliente = (clientInfo.rca1 || '').trim();
                const isClientMissingOrRca53 = (!clientExists || rca1Cliente === '53' || rca1Cliente === '053');

                // Se for venda atual E (cliente não existe OU cliente é RCA 53), preserva o RCA original da venda
                const shouldPreserveOriginalRca = isCurrentSales && isClientMissingOrRca53;

                // Normalize CodCli for 9569 check as well
                if (!isAmericanas && !(codCliStr === '9569' && (codUsurVendaCheck === '53' || codUsurVendaCheck === '053') && (tipoVendaCheck === '1' || tipoVendaCheck === '9'))) {
                    const codUsurVenda = codUsur; // Guarda o CODUSUR original da linha de venda

                    // Prioriza o RCA 1 do cadastro de clientes. Se não tiver (ou se deve preservar original), usa o RCA da linha de venda.
                    let codUsurParaBusca;

                    if (shouldPreserveOriginalRca) {
                        codUsurParaBusca = codUsurVenda;
                    } else {
                        codUsurParaBusca = rca1Cliente || codUsurVenda;
                    }

                    const rcaInfo = newRcaSupervisorMap.get(codUsurParaBusca);

                    if (rcaInfo) {
                        // Encontrou informações no mapa mestre (usando RCA1 do cliente ou RCA da venda)
                        vendorName = rcaInfo.NOME;
                        supervisorName = rcaInfo.SUPERV;
                        codSupervisor = rcaInfo.CODSUPERVISOR;
                        codUsur = codUsurParaBusca; // Define o CODUSUR final
                    }
                    else if (rca1Cliente && rca1Cliente !== codUsurVenda && !shouldPreserveOriginalRca) {
                        // Usou o RCA1 do cliente, não achou. Tenta um fallback com o RCA da linha de venda.
                        // Mas apenas se não estamos forçando a preservação do original (embora se shouldPreserveOriginalRca fosse true, rca1Cliente seria ignorado acima)
                        const fallbackInfo = newRcaSupervisorMap.get(codUsurVenda);
                        if (fallbackInfo) {
                            vendorName = fallbackInfo.NOME;
                            supervisorName = fallbackInfo.SUPERV;
                            codSupervisor = fallbackInfo.CODSUPERVISOR;
                            codUsur = codUsurVenda; // Define o CODUSUR final como o da venda (fallback)
                        } else {
                            // Não achou em lugar nenhum, mantém os dados da linha (já definidos no passo 1)
                            codUsur = codUsurVenda;
                        }
                    }
                    else {
                        // Se rca1Cliente estava vazio, codUsurParaBusca == codUsurVenda.
                        // Se não achou rcaInfo, significa que o codUsurVenda não está no mapa.
                        // Mantém os dados da linha (já definidos no passo 1) e o codUsur da venda.
                        codUsur = codUsurVenda;
                    }
                }
                // --- FIM DA MODIFICAÇÃO ---


                // --- INICIO DA MODIFICAÇÃO: REGRA INATIVOS ---
                // Se o cliente não tiver RCA1 cadastrado na planilha de clientes, rotular como INATIVOS
                // (Isso substitui o vendedor original da venda, pois a carteira está 'sem dono')
                // EXCEÇÃO: Se deve preservar original (venda atual de cliente sem cadastro/RCA 53), não aplica INATIVOS.
                if (!shouldPreserveOriginalRca && !isAmericanas && (!clientInfo || !clientInfo.rca1 || clientInfo.rca1.trim() === '')) {
                    vendorName = 'INATIVOS';
                    supervisorName = 'INATIVOS'; // Alterado de BALCAO para INATIVOS
                    codSupervisor = '99'; // Alterado de 8 para 99 para separar do Balcão
                }
                // --- FIM DA MODIFICAÇÃO ---

                const supervisorUpper = (supervisorName || '').trim().toUpperCase();
                if (supervisorUpper === 'BALCAO' || supervisorUpper === 'BALCÃO') supervisorName = 'BALCAO';

                let dtPed = rawRow['DTPED'];
                const dtSaida = rawRow['DTSAIDA'];

                // --- OPTIMIZATION: Parse Dates to Timestamp here in Worker ---
                let parsedDtPed = parseDate(dtPed);
                const parsedDtSaida = parseDate(dtSaida);

                if (parsedDtPed && parsedDtSaida && (parsedDtPed.getUTCFullYear() < parsedDtSaida.getUTCFullYear() || (parsedDtPed.getUTCFullYear() === parsedDtSaida.getUTCFullYear() && parsedDtPed.getUTCMonth() < parsedDtSaida.getUTCMonth()))) {
                    parsedDtPed = parsedDtSaida;
                }

                // Convert to Timestamp (Number) for lighter JSON and faster parsing in main thread
                const tsDtPed = parsedDtPed ? parsedDtPed.getTime() : null;
                const tsDtSaida = parsedDtSaida ? parsedDtSaida.getTime() : null;

                const productCode = String(rawRow['PRODUTO'] || '').trim();
                let qtdeMaster = productMasterMap.get(productCode);
                if (!qtdeMaster || qtdeMaster <= 0) qtdeMaster = 1;
                const qtVenda = parseInt(String(rawRow['QTVENDA'] || '0').trim(), 10) || 0;

                // --- INÍCIO DA MODIFICAÇÃO: REGRA TIPOVENDA ---
                // Captura o tipo de venda primeiro
                const tipoVenda = String(rawRow['TIPOVENDA'] || 'N/A').trim();

                // Apenas TIPOVENDA '1' e '9' contam para faturamento e peso
                const isFaturamento = (tipoVenda === '1' || tipoVenda === '9');

                // Captura os valores originais
                const vlVendaOriginal = parseBrazilianNumber(rawRow['VLVENDA']);
                const vlBonificOriginal = parseBrazilianNumber(rawRow['VLBONIFIC']);
                const pesoOriginal = parseBrazilianNumber(rawRow['TOTPESOLIQ']);
                // --- FIM DA MODIFICAÇÃO ---

                let filialValue = String(rawRow['FILIAL'] || '').trim();
                if (filialValue === '5') filialValue = '05';
                if (filialValue === '8') filialValue = '08';

                return {
                    PEDIDO: pedido, NOME: vendorName, SUPERV: supervisorName, PRODUTO: productCode,
                    DESCRICAO: String(rawRow['DESCRICAO'] || ''), FORNECEDOR: String(rawRow['FORNECEDOR'] || ''),
                    OBSERVACAOFOR: observacaoFor, CODFOR: String(rawRow['CODFOR'] || '').trim(),
                    CODUSUR: codUsur, CODCLI: codCliStr,
                    // OPTIMIZATION: Removed CLIENTE_NOME, CIDADE, BAIRRO to save file size (Lookup in runtime)
                    QTVENDA: qtVenda,
                    CODSUPERVISOR: codSupervisor, // Added to ensure supervisor code is available for indexing

                    // --- INÍCIO DA MODIFICAÇÃO: APLICAÇÃO DA REGRA ---
                    // Se não for faturamento (1 ou 9), VLVENDA é 0
                    VLVENDA: isFaturamento ? vlVendaOriginal : 0,

                    // Se não for faturamento, soma o que veio em VLVENDA (indevidamente) ao VLBONIFIC
                    VLBONIFIC: isFaturamento ? vlBonificOriginal : (vlVendaOriginal + vlBonificOriginal),

                    // Peso (tonelada) - [Instrução do usuário: Deve somar sempre, mesmo não sendo faturamento]
                    TOTPESOLIQ: pesoOriginal,
                    DTPED: tsDtPed, DTSAIDA: tsDtSaida, // Optimized: Numbers
                    POSICAO: String(rawRow['POSICAO'] || ''),
                    ESTOQUEUNIT: parseBrazilianNumber(rawRow['ESTOQUEUNIT']),
                    QTVENDA_EMBALAGEM_MASTER: isNaN(qtdeMaster) || qtdeMaster === 0 ? 0 : qtVenda / qtdeMaster,
                    // Garante que o tipo de venda correto é passado
                    TIPOVENDA: tipoVenda,
                    // Adiciona a filial normalizada para permitir filtragem
                    FILIAL: filialValue
                    // --- FIM DA MODIFICAÇÃO ---
                };
            }).filter(item => item !== null);
        };

        function getMaxDate(data) {
            let maxTs = 0;
            for (let i = 0; i < data.length; i++) {
                const row = data[i];
                // Ignore rows without CODUSUR (Stock lines) to find the TRUE max sales date
                const codUsur = String(row['CODUSUR'] || '').trim();
                if (codUsur === '' || codUsur === '0' || codUsur === '00') continue;

                const d = parseDate(row['DTPED']);
                if (d) {
                    const ts = d.getTime();
                    if (ts > maxTs) maxTs = ts;
                }
            }
            return maxTs > 0 ? maxTs : Date.now();
        }

        function getPassedWorkingDaysInSpecificMonth(year, month, today) {
            let count = 0;
            const date = new Date(Date.UTC(year, month, 1));
            // Ensure comparison is done in UTC terms
            const todayUTC = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));

            while (date <= todayUTC && date.getUTCMonth() === month) {
                const dayOfWeek = date.getUTCDay();
                if (dayOfWeek >= 1 && dayOfWeek <= 5) {
                    count++;
                }
                date.setUTCDate(date.getUTCDate() + 1);
            }
            return count > 0 ? count : 1;
        }

        // --- OPTIMIZATION: Columnar Data Transformation ---
        function toColumnar(data) {
            if (!data || data.length === 0) return { columns: [], values: {}, length: 0 };

            // Scan first 50 rows to get all possible keys to avoid missing columns if first row is partial
            const keysSet = new Set();
            const limit = Math.min(data.length, 50);
            for(let i=0; i<limit; i++) {
                Object.keys(data[i]).forEach(k => keysSet.add(k));
            }
            const columns = Array.from(keysSet);

            const values = {};
            columns.forEach(col => values[col] = new Array(data.length));

            for (let i = 0; i < data.length; i++) {
                for (let j = 0; j < columns.length; j++) {
                    values[columns[j]][i] = data[i][columns[j]];
                }
            }
            return { columns, values, length: data.length };
        }

        self.onmessage = async (event) => {
            const { salesFile, clientsFile, productsFile, historyFile, innovationsFile } = event.data;

            try {
                self.postMessage({ type: 'progress', status: 'Lendo arquivos...', percentage: 10 });
                const [salesDataRaw, clientsDataRaw, productsDataRaw, historyDataRaw, innovationsDataRaw] = await Promise.all([
                    readFile(salesFile, 'sales'),
                    readFile(clientsFile, 'clients'),
                    readFile(productsFile, 'products'),
                    readFile(historyFile, 'history'),
                    readFile(innovationsFile, 'innovations')
                ]);



                self.postMessage({ type: 'progress', status: 'Criando mapa mestre de supervisores...', percentage: 20 });
                const newRcaSupervisorMap = new Map();
                const lastSaleDateMap = new Map();

                // --- INÍCIO DA MODIFICAÇÃO: Usar vendas atuais E históricas para o mapa mestre ---
                const allSalesForMap = [...salesDataRaw, ...historyDataRaw];

                allSalesForMap.forEach(row => {
                // --- FIM DA MODIFICAÇÃO ---
                    try {
                        const codUsur = String(row['CODUSUR'] || '').trim();
                        // Filter out headers that might have slipped through
                        const supervCheck = String(row['SUPERV'] || '').trim().toUpperCase();
                        if (codUsur.toUpperCase() === 'CODUSUR' || codUsur.toUpperCase() === 'COD USUR' || supervCheck === 'SUPERV' || supervCheck === 'SUPERVISOR' || isNaN(parseInt(codUsur))) return;

                        if (codUsur === '1001') return;

                        const dtPed = row['DTPED'];
                        if (!codUsur || !dtPed) return;

                        const saleDate = parseDate(dtPed);
                        if (!saleDate || isNaN(saleDate.getTime())) return;

                        const lastDate = lastSaleDateMap.get(codUsur);
                        if (!lastDate || saleDate >= lastDate) {
                            lastSaleDateMap.set(codUsur, saleDate);
                            newRcaSupervisorMap.set(codUsur, {
                                NOME: String(row['NOME'] || ''),
                                SUPERV: String(row['SUPERV'] || ''),
                                CODSUPERVISOR: String(row['CODSUPERVISOR'] || '').trim()
                            });
                        }
                    } catch (e) {
                        console.error('Erro ao processar linha para mapa mestre:', e, row);
                    }
                });

                self.postMessage({ type: 'progress', status: 'Extraindo estoque do arquivo de vendas...', percentage: 25 });
                const stockMap05 = new Map();
                const stockMap08 = new Map();

                salesDataRaw.forEach(item => {
                    const productCode = String(item['PRODUTO'] || '').trim();
                    let branch = String(item['FILIAL'] || '').trim();
                    const stockQtyCx = parseBrazilianNumber(item['ESTOQUECX']);

                    if (productCode && branch) {
                        if (branch === '5') branch = '05';
                        if (branch === '8') branch = '08';

                        if (branch === '05') {
                            stockMap05.set(productCode, stockQtyCx);
                        } else if (branch === '08') {
                            stockMap08.set(productCode, stockQtyCx);
                        }
                    }
                });

                self.postMessage({ type: 'progress', status: 'Mapeando produtos e criando lista de ativos...', percentage: 30 });
                const productMasterMap = new Map();
                const activeProductCodesFromCadastro = new Set();
                const productDetailsMap = new Map();

                productsDataRaw.forEach(prod => {
                    const productCode = String(prod['Código'] || '').trim();
                    if (!productCode) return;
                    activeProductCodesFromCadastro.add(productCode);
                    let qtdeMaster = parseInt(prod['Qtde embalagem master(Compra)'], 10);
                    if (isNaN(qtdeMaster) || qtdeMaster <= 0) qtdeMaster = 1;
                    productMasterMap.set(productCode, qtdeMaster);
                    if (!productDetailsMap.has(productCode)) {
                            const dtCad = parseDate(prod['Dt.Cadastro']);
                            productDetailsMap.set(productCode, {
                                code: productCode,
                                descricao: String(prod['Descrição'] || `Produto ${productCode}`),
                                fornecedor: String(prod['Nome do fornecedor'] || 'N/A'),
                                codfor: String(prod['Fornecedor'] || 'N/A'),
                                dtCadastro: dtCad ? dtCad.getTime() : null,
                                pasta: null
                            });
                        }
                });


                const clientRcaOverrides = new Map();
                salesDataRaw.forEach(rawRow => {
                    const pedido = String(rawRow['PEDIDO'] || '');
                    const codCli = String(rawRow['CODCLI'] || '').trim();
                    if(!codCli) return;
                });

                self.postMessage({ type: 'progress', status: 'Processando clientes...', percentage: 50 });
                const clientMap = new Map();

                // Helper to get value from multiple possible keys
                const getVal = (row, keys) => {
                    if (!row) return undefined;
                    // Direct match first
                    for (const k of keys) {
                        if (row[k] !== undefined && row[k] !== null && String(row[k]).trim() !== '') return row[k];
                    }
                    // Case-insensitive match
                    const rowKeys = Object.keys(row);
                    for (const k of keys) {
                        const match = rowKeys.find(rk => rk.trim().toUpperCase() === k.toUpperCase());
                        if (match && row[match] !== undefined && row[match] !== null && String(row[match]).trim() !== '') return row[match];
                    }
                    return undefined;
                };

                clientsDataRaw.forEach(client => {
                    const codCliRaw = getVal(client, ['Código', 'CODIGO', 'Codigo', 'Cod. Cliente', 'CODCLI', 'CodCliente']);
                    const codCli = normalizeKey(codCliRaw);
                    if (!codCli) return;

                    const rca1 = String(getVal(client, ['RCA 1', 'RCA', 'Rca 1', 'Vendedor 1', 'VENDEDOR']) || '').trim();
                    const rcas = new Set();
                    if (rca1) rcas.add(rca1);

                    const ucRaw = getVal(client, ['Data da Última Compra', 'Ultima Compra', 'DTULTCOMPRA', 'Data Ultima Compra']);
                    const dcRaw = getVal(client, ['Data e Hora de Cadastro', 'Data Cadastro', 'DTCADASTRO', 'Data de Cadastro']);

                    const uc = parseDate(ucRaw);
                    const dc = parseDate(dcRaw);

                    const cidade = String(getVal(client, ['Nome da Cidade', 'Cidade', 'CIDADE', 'MUNICIPIO', 'Município', 'City', 'CIDADE_CLIENTE']) || 'N/A');
                    const bairro = String(getVal(client, ['Bairro', 'BAIRRO', 'Bairro/Distrito', 'BAIRRO_CLIENTE']) || 'N/A');
                    const fantasia = String(getVal(client, ['Fantasia', 'Nome Fantasia', 'NOME FANTASIA', 'NOME_FANTASIA', 'Nome', 'NOME']) || 'N/A');
                    const razao = String(getVal(client, ['Cliente', 'Razão Social', 'RAZAO SOCIAL', 'RAZAOSOCIAL', 'Nome Cliente', 'Razão']) || 'N/A');

                    // Fallback logic for Name
                    const nomeCliente = (fantasia !== 'N/A') ? fantasia : razao;

                    const clientData = {
                        'codigo_cliente': codCli,
                        rcas: Array.from(rcas),
                        rca1: rca1,
                        cidade: cidade,
                        nomeCliente: nomeCliente,
                        bairro: bairro,
                        razaoSocial: razao,
                        fantasia: fantasia,
                        cnpj_cpf: String(getVal(client, ['CNPJ/CPF', 'CNPJ', 'CPF', 'Cgc/Cpf']) || 'N/A'),
                        endereco: String(getVal(client, ['Endereço Comercial', 'Endereço', 'ENDERECO', 'Logradouro', 'Rua']) || 'N/A'),
                        numero: String(getVal(client, ['Número', 'Numero', 'NUMERO', 'No']) || 'SN'),
                        cep: String(getVal(client, ['CEP', 'Cep']) || 'N/A'),
                        telefone: String(getVal(client, ['Telefone Comercial', 'Telefone', 'TELEFONE', 'Tel']) || 'N/A'),
                        email: String(getVal(client, ['E-mail', 'Email', 'EMAIL', 'Correo']) || 'N/A'),
                        ramo: String(getVal(client, ['Descricao', 'Ramo', 'Atividade', 'RAMO_ATIVIDADE']) || 'N/A'),
                        ultimaCompra: uc ? uc.getTime() : null,
                        dataCadastro: dc ? dc.getTime() : null,
                        bloqueio: String(getVal(client, ['Bloqueio', 'BLOQUEIO', 'Status']) || '').trim().toUpperCase(),
                        inscricaoEstadual: String(getVal(client, ['Insc. Est. / Produtor', 'Inscricao Estadual', 'IE', 'INSCRICAO']) || 'N/A')
                    };
                    if (clientRcaOverrides.has(codCli)) clientData.rcas.unshift(clientRcaOverrides.get(codCli));
                    clientMap.set(codCli, clientData);
                });

                self.postMessage({ type: 'progress', status: 'Cruzando dados de vendas...', percentage: 70 });

                // Calculate Max Date from Raw Sales Data (ignoring stock lines)
                const maxSalesDate = getMaxDate(salesDataRaw);
                const stockLinesCollector = [];

                // Pass collector and maxDate to processSalesData
                const processedSalesData = processSalesData(salesDataRaw, clientMap, productMasterMap, newRcaSupervisorMap, stockLinesCollector, maxSalesDate).filter(item => item !== null);
                // History data usually doesn't have stock lines, but passing null/null is safe or consistent
                const processedHistoryData = processSalesData(historyDataRaw, clientMap, productMasterMap, newRcaSupervisorMap, null, null).filter(item => item !== null);

                // Update productMasterMap / productDetailsMap with info from stockLines (if missing)
                // This ensures "Lost Products" table has Description/Supplier even if product is not in the Products File
                stockLinesCollector.forEach(item => {
                    const code = item.PRODUTO;

                    // CRITICAL: Ensure these products are considered "Active" so they appear in lists
                    activeProductCodesFromCadastro.add(code);

                    // Note: productMasterMap stores qtdeMaster. We probably don't have that in stock lines,
                    // but we have Description/Supplier which goes into productDetailsMap (used for display).

                    if (!productDetailsMap.has(code)) {
                        // Create a partial entry for display purposes
                        productDetailsMap.set(code, {
                            code: code,
                            descricao: item.DESCRICAO || `Produto ${code}`,
                            fornecedor: item.FORNECEDOR || 'N/A',
                            codfor: item.CODFOR || 'N/A',
                            dtCadastro: item.DTPED, // Use the fixed date (Max Date)
                            pasta: item.PASTA || item.OBSERVACAOFOR || null // Ensure Pasta is available
                        });
                    }
                });

                self.postMessage({ type: 'progress', status: 'Aplicando regra de filial...', percentage: 75 });

                const allProcessedSales = [...processedSalesData, ...processedHistoryData].sort((a, b) => {
                    const dateA = parseDate(a.DTPED) || new Date(0);
                    const dateB = parseDate(b.DTPED) || new Date(0);
                    return dateA - dateB;
                });

                const clientLastBranch = new Map();
                const clientsWith05Purchase = new Set();

                allProcessedSales.forEach(sale => {
                    const codCli = sale.CODCLI;
                    const filial = sale.FILIAL;
                    if (codCli && filial) {
                        clientLastBranch.set(codCli, filial);
                        if (filial === '05') {
                            clientsWith05Purchase.add(codCli);
                        }
                    }
                });

                const clientBranchOverride = new Map();
                clientsWith05Purchase.forEach(codCli => {
                    const lastBranch = clientLastBranch.get(codCli);
                    if (lastBranch && lastBranch === '08') {
                        clientBranchOverride.set(codCli, '08');
                    }
                });

                const applyBranchOverride = (salesArray, overrideMap) => {
                    // Use standard loop for better performance in worker
                    for(let i=0; i<salesArray.length; i++) {
                        const sale = salesArray[i];
                        const override = overrideMap.get(sale.CODCLI);
                        if (override && sale.FILIAL === '05') {
                            sale.FILIAL = override;
                        }
                    }
                    return salesArray;
                };

                let finalSalesData = applyBranchOverride(processedSalesData, clientBranchOverride);
                let finalHistoryData = applyBranchOverride(processedHistoryData, clientBranchOverride);

                self.postMessage({ type: 'progress', status: 'Aplicando regra específica para Supervisor Tiago...', percentage: 78 });
                const tiagoSellersToMoveTo08 = new Set(['291', '292', '293', '284', '289', '287', '286']);

                const applyTiagoRule = (salesArray) => {
                    for(let i=0; i<salesArray.length; i++) {
                        const sale = salesArray[i];
                        if (sale.CODSUPERVISOR === '12' && tiagoSellersToMoveTo08.has(sale.CODUSUR)) {
                            sale.FILIAL = '08';
                        }
                    }
                    return salesArray;
                };

                finalSalesData = applyTiagoRule(finalSalesData);
                finalHistoryData = applyTiagoRule(finalHistoryData);


                self.postMessage({ type: 'progress', status: 'Atualizando datas de compra...', percentage: 80 });
                const latestSaleDateByClient = new Map();
                // Performance: use for loop
                for(let i=0; i<finalSalesData.length; i++) {
                    const sale = finalSalesData[i];
                    const codcli = sale.CODCLI;
                    const saleDate = sale.DTPED; // Already timestamp (number)
                    if (codcli && saleDate) {
                        const existingDate = latestSaleDateByClient.get(codcli);
                        if (!existingDate || saleDate > existingDate) latestSaleDateByClient.set(codcli, saleDate);
                    }
                }

                clientMap.forEach((client, codcli) => {
                    const lastPurchaseDate = client.ultimaCompra; // Already timestamp
                    const latestSaleDate = latestSaleDateByClient.get(codcli);
                    // Compare numbers
                    if (latestSaleDate && (!lastPurchaseDate || latestSaleDate > lastPurchaseDate)) {
                        client.ultimaCompra = latestSaleDate;
                    }
                });

                self.postMessage({ type: 'progress', status: 'Agregando pedidos...', percentage: 90 });
                const aggregateOrders = (data) => {
                    const orders = new Map();
                    // Performance: for loop
                    for(let i=0; i<data.length; i++) {
                        const row = data[i];
                        if (!row.PEDIDO) continue;
                        if (!orders.has(row.PEDIDO)) {
                            // Restore Client Info for Order Header (needed for Modal)
                            const client = clientMap.get(row.CODCLI);
                            orders.set(row.PEDIDO, {
                                ...row,
                                TIPOVENDA: String(row.TIPOVENDA || 'N/A'),
                                CLIENTE_NOME: client ? (client.nomeCliente || client.fantasia) : 'N/A',
                                CIDADE: client ? client.cidade : 'N/A',
                                QTVENDA: 0,
                                VLVENDA: 0,
                                VLBONIFIC: 0,
                                TOTPESOLIQ: 0,
                                FORNECEDORES: new Set(),
                                CODFORS: new Set()
                            });
                        }
                        const order = orders.get(row.PEDIDO);
                        order.QTVENDA += (Number(row.QTVENDA) || 0);
                        order.VLVENDA += (Number(row.VLVENDA) || 0);
                        order.VLBONIFIC += (Number(row.VLBONIFIC) || 0);
                        order.TOTPESOLIQ += (Number(row.TOTPESOLIQ) || 0);
                        if (row.OBSERVACAOFOR) order.FORNECEDORES.add(row.OBSERVACAOFOR);
                        if (row.CODFOR) order.CODFORS.add(row.CODFOR);
                    }
                    return Array.from(orders.values()).map(order => {
                        // Construct a clean object matching data_orders schema to avoid "column not found" errors
                        return {
                            PEDIDO: order.PEDIDO,
                            CODCLI: order.CODCLI,
                            CLIENTE_NOME: order.CLIENTE_NOME,
                            CIDADE: order.CIDADE,
                            NOME: order.NOME,
                            SUPERV: order.SUPERV,
                            FORNECEDORES_STR: Array.from(order.FORNECEDORES).join(', '),
                            DTPED: order.DTPED,
                            DTSAIDA: order.DTSAIDA,
                            POSICAO: order.POSICAO,
                            VLVENDA: order.VLVENDA,
                            TOTPESOLIQ: order.TOTPESOLIQ,
                            FILIAL: order.FILIAL,
                            // Added for Filtering
                            TIPOVENDA: String(order.TIPOVENDA || 'N/A'),
                            FORNECEDORES_LIST: Array.from(order.FORNECEDORES),
                            CODFORS_LIST: Array.from(order.CODFORS)
                        };
                    });
                };
                const aggregatedByOrder = aggregateOrders(finalSalesData);

                // Calculate Max Date using Numbers (Timestamps)
                let maxTs = 0;
                for(let i=0; i<finalSalesData.length; i++) {
                    const ts = finalSalesData[i].DTPED;
                    if(ts && ts > maxTs) maxTs = ts;
                }
                const lastSaleDate = maxTs > 0 ? new Date(maxTs) : new Date();
                lastSaleDate.setUTCHours(0,0,0,0);
                const passedWorkingDaysCurrentMonth = getPassedWorkingDaysInSpecificMonth(
                    lastSaleDate.getUTCFullYear(),
                    lastSaleDate.getUTCMonth(),
                    lastSaleDate
                );


                self.postMessage({ type: 'progress', status: 'Otimizando e Finalizando...', percentage: 95 });

                // Convert large datasets to Columnar format
                const columnarDetailed = toColumnar(finalSalesData);
                const columnarHistory = toColumnar(finalHistoryData);
                const columnarClients = toColumnar(Array.from(clientMap.values()));

                // --- RESTORED LOGIC FOR UPLOAD ARRAYS ---
                const finalProductDetailsData = Array.from(productDetailsMap.values());
                const finalActiveProductsData = Array.from(activeProductCodesFromCadastro).map(code => ({ code: code }));

                const finalStockData = [];
                // Reconstruct stock array from Maps (05 and 08)
                stockMap05.forEach((qty, code) => {
                    finalStockData.push({ product_code: code, filial: '05', stock_qty: qty });
                });
                stockMap08.forEach((qty, code) => {
                    finalStockData.push({ product_code: code, filial: '08', stock_qty: qty });
                });

                const finalInnovationsData = innovationsDataRaw.map(item => ({
                    codigo: item['Codigo'] || item['codigo'] || null,
                    produto: item['Produto'] || item['produto'] || null,
                    inovacoes: item['Inovacoes'] || item['inovacoes'] || null
                }));

                const finalMetadata = [];
                finalMetadata.push({ key: 'passed_working_days', value: String(passedWorkingDaysCurrentMonth) });
                finalMetadata.push({ key: 'last_update', value: new Date().toISOString() });
                finalMetadata.push({ key: 'last_sale_date', value: String(maxTs) });

                self.postMessage({ type: 'progress', status: 'Pronto!', percentage: 100 });

                self.postMessage({
                    type: 'result',
                    data: {
                        detailed: columnarDetailed,
                        history: columnarHistory,
                        byOrder: aggregatedByOrder,
                        clients: columnarClients,
                        // Upload Support Arrays
                        stock: finalStockData,
                        innovations: finalInnovationsData,
                        product_details: finalProductDetailsData,
                        active_products: finalActiveProductsData,
                        metadata: finalMetadata,

                        // Legacy Maps for Frontend (if needed, or logic script handles it)
                        // The logic script (report-logic-script) seems to expect stockMap05/08 in 'embeddedData'.
                        // But wait, the uploader Worker sends data to 'enviarDadosParaSupabase', NOT to the dashboard logic directly.
                        // The dashboard logic reads from Supabase via 'carregarDadosDoSupabase'.
                        // So the Worker ONLY needs to satisfy the Uploader requirements.
                        // However, keeping the Maps might be useful if we ever wanted to hydrate locally.
                        stockMap05: Object.fromEntries(stockMap05),
                        stockMap08: Object.fromEntries(stockMap08),
                        activeProductCodes: Array.from(activeProductCodesFromCadastro),
                        productDetails: Object.fromEntries(productDetailsMap),
                        passedWorkingDaysCurrentMonth: passedWorkingDaysCurrentMonth,
                        lastSaleDate: String(maxTs),
                        isColumnar: true
                    }
                });

            } catch (error) {
                self.postMessage({ type: 'error', message: error.message + (error.stack ? `\nStack: ${error.stack}`: '') });
            }
        };
