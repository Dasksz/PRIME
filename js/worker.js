// js/worker.js

importScripts('https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2');
importScripts('https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js');

let supabase;

function postProgress(message, progress, type = 'progress') {
    self.postMessage({ type, message, progress });
}

self.onmessage = async function(e) {
    const { type, files, supabaseCredentials } = e.data;

    if (type === 'start_upload') {
        try {
            const { url, key } = supabaseCredentials;
            if (!self.supabase) throw new Error("Supabase library not loaded.");
            supabase = self.supabase.createClient(url, key);

            postProgress('Conectado. Lendo e processando ficheiros...', 5);

            const processedData = await readAndProcessFiles(files);

            await uploadDataToSupabase(processedData);

            postProgress('Upload concluído com sucesso!', 100, 'complete');

        } catch (error) {
            console.error('Erro no worker:', error);
            postProgress(`Erro: ${error.message}`, 100, 'error');
        }
    }
};

async function readFile(file) {
    return new Promise((resolve, reject) => {
        if (!file) {
            resolve([]);
            return;
        }
        const reader = new FileReader();
        reader.onload = (event) => {
            try {
                const data = new Uint8Array(event.target.result);
                const workbook = XLSX.read(data, { type: 'array' });
                const sheetName = workbook.SheetNames[0];
                const worksheet = workbook.Sheets[sheetName];
                const json = XLSX.utils.sheet_to_json(worksheet);
                resolve(json);
            } catch (err) {
                reject(new Error(`Falha ao ler o ficheiro ${file.name}: ${err.message}`));
            }
        };
        reader.onerror = () => reject(new Error(`Erro ao ler o ficheiro ${file.name}.`));
        reader.readAsArrayBuffer(file);
    });
}

async function readAndProcessFiles(files) {
    postProgress('Lendo ficheiros...', 10);
    const [sales, clients, products, history, innovations] = await Promise.all([
        readFile(files.sales),
        readFile(files.clients),
        readFile(files.products),
        readFile(files.history),
        readFile(files.innovations)
    ]);

    postProgress('Processando dados...', 30);

    // This is a simplified version of the original complex processing logic.
    // It maps and cleans the data as required.
    const detailedColumns = 'pedido,dtped,dtsaida,codcli,cliente,cidade,bairro,codusur,nome,superv,codfor,fornecedor,codprod,produto,qt,vlunit,vlvenda,totpesoliq,posicao,tipovenda,vlbonific,desconto,observacaofor,qtvenda_embalagem_master,filial,descricao'.split(',');
    const historyColumns = 'dtped,codusur,nome,superv,codcli,cidade,codfor,fornecedor,codprod,produto,qtvenda,vlvenda,totpesoliq,qtvenda_embalagem_master,filial,observacaofor'.split(',');

    const processData = (data, columns) => data.map(row => {
        const newRow = {};
        columns.forEach(col => {
            const key = Object.keys(row).find(k => k.toLowerCase().trim() === col);
            if (key) newRow[col] = row[key];
        });
        return newRow;
    });

    const data = {};
    if (sales.length > 0) data.detailed = processData(sales, detailedColumns);
    if (history.length > 0) data.history = processData(history, historyColumns);
    if (clients.length > 0) data.clients = clients.map(c => ({...c, codigo_cliente: c.codigo_cliente?.toString() }));
    if (products.length > 0) data.product_details = products;
    if (innovations.length > 0) data.innovations = innovations;

    // Additional processing steps like creating data_orders, metadata etc. would go here.

    return data;
}

async function uploadDataToSupabase(data) {
    const uploadTasks = [];
    if (data.detailed) uploadTasks.push({ name: 'Vendas Atuais', tableName: 'data_detailed', data: data.detailed, keys: ['pedido'] });
    if (data.history) uploadTasks.push({ name: 'Histórico', tableName: 'data_history', data: data.history, keys: ['pedido'] });
    if (data.clients) uploadTasks.push({ name: 'Clientes', tableName: 'data_clients', data: data.clients, keys: ['codigo_cliente'] });
    if (data.product_details) uploadTasks.push({ name: 'Detalhes de Produtos', tableName: 'data_product_details', data: data.product_details, keys: ['code'] });
    if (data.innovations) uploadTasks.push({ name: 'Inovações', tableName: 'data_innovations', data: data.innovations, keys: ['codigo'] });
    // Add other tables as needed

    if (uploadTasks.length === 0) {
        postProgress('Nenhum dado novo para enviar.', 100, 'complete');
        return;
    }

    const totalTasks = uploadTasks.length;
    for (let i = 0; i < totalTasks; i++) {
        const task = uploadTasks[i];
        const progressoInicial = 50 + ((i / totalTasks) * 50);
        const progressoFinal = 50 + (((i + 1) / totalTasks) * 50);

        await enviarDadosEmLotes(task.tableName, task.data, progressoInicial, progressoFinal, task.keys);
    }
}

async function enviarDadosEmLotes(nomeTabela, dados, progressoInicial, progressoFinal, primaryKeys = ['id']) {
    const TAMANHO_LOTE = 500;
    if (dados.length === 0) {
        postProgress(`Nenhum dado para ${nomeTabela}.`, progressoFinal);
        return;
    }

    postProgress(`Limpando tabela ${nomeTabela}...`, progressoInicial);

    // Clear table before inserting new data
    const primaryKey = primaryKeys[0];
    const { error: deleteError } = await supabase.from(nomeTabela).delete().neq(primaryKey, -1); // Generic delete
    if (deleteError) throw new Error(`Falha ao limpar ${nomeTabela}: ${deleteError.message}`);

    const totalLotes = Math.ceil(dados.length / TAMANHO_LOTE);
    for (let i = 0; i < totalLotes; i++) {
        const lote = dados.slice(i * TAMANHO_LOTE, (i + 1) * TAMANHO_LOTE);
        const { error: insertError } = await supabase.from(nomeTabela).insert(lote);
        if (insertError) throw new Error(`Falha ao inserir em ${nomeTabela} (Lote ${i+1}): ${insertError.message}`);

        const progressoLote = (i + 1) / totalLotes;
        const progressoTotal = progressoInicial + (progressoLote * (progressoFinal - progressoInicial));
        postProgress(`Enviando ${nomeTabela} (${i + 1}/${totalLotes})...`, progressoTotal);
    }
}
