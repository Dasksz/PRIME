const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { webcrypto } = require('node:crypto');
const workerPath = '../worker.js';
function reader(rows) {
  const sheet = { '!ref': 'A1:D' + rows.length, rows };
  const XLSX = { read: () => ({ SheetNames: ['Cadastro'], Sheets: { Cadastro: sheet } }), utils: {
    decode_range: () => ({ s: { r: 0, c: 0 }, e: { r: rows.length - 1, c: 3 } }),
    sheet_to_json: (sheet, options) => {
      assert.equal(options.raw, true, 'cadastro must use stored values, never currency display text');
      if (options.header === 1) return rows.slice(options.range.s.r, options.range.e.r + 1);
      return rows.slice(options.range + 1).map(row => Object.fromEntries(rows[options.range].map((h, c) => [h, row[c] ?? null])));
    },
  } };
  class FileReader { readAsArrayBuffer() { this.onload({ target: { result: new ArrayBuffer(0) } }); } }
  const context = vm.createContext({ XLSX, FileReader, TextEncoder, TextDecoder, console,
    self: { importScripts() {}, crypto: webcrypto, postMessage() {} } });
  vm.runInContext(fs.readFileSync(path.join(__dirname, workerPath), 'utf8'), context);
  return {
    read: () => vm.runInContext("readFile({name:'cadastro.xls'},'clients')", context),
    normalize: data => { context.rows = data; return vm.runInContext("normalizeClientRows(rows,'cadastro.csv')", context); },
  };
}
test('both cadastro layouts produce identical client identifiers and preserve contacts', async () => {
  const headers = ['Código', 'Cliente', 'RCA 1', 'CNPJ/CPF'];
  const clean = await reader([headers, [3, 'Cliente A', 53, '00123456000100']]).read();
  const formatted = await reader([['01 - Capa', null, null, null], headers, [3, 'Cliente A', '53', '00123456000100']]).read();
  assert.deepEqual(JSON.parse(JSON.stringify(clean)), JSON.parse(JSON.stringify(formatted)));
  assert.equal(clean[0]['Código'], '3');
  assert.equal(clean[0]['RCA 1'], '53');
  assert.equal(clean[0]['CNPJ/CPF'], '00123456000100');
});
test('header aliases and integer text are accepted', async () => {
  const rows = await reader([['Codigo', 'Nome Cliente', 'RCA', 'CNPJ'], ['00027', 'Cliente B', '282.0', '001']]).read();
  assert.equal(rows[0]['Código'], '27');
  assert.equal(rows[0]['RCA 1'], '282');
});
test('invalid or empty registration cannot reach the upload result', async () => {
  await assert.rejects(reader([['01 - Capa', null, null, null]]).read(), /cabeçalhos/);
  await assert.rejects(reader([['Código', 'Cliente', 'RCA 1', 'CNPJ']]).read(), /nenhum cliente/);
  await assert.rejects(reader([['Código', 'Cliente', 'RCA 1', 'CNPJ'], [3, 'A', 'erro', null]]).read(), /RCA 1 inválido/);
  await assert.rejects(reader([['Código', 'Cliente', 'RCA 1', 'CNPJ'], [null, 'A', 53, null]]).read(), /sem código/);
  await assert.rejects(reader([['Código', 'Cliente', 'RCA 1', 'CNPJ'], [3, 'A', 53, null], ['003', 'B', 53, null]]).read(), /duplicado/);
  await assert.rejects(reader([['Código', 'CODCLI', 'Cliente', 'RCA 1'], [3, 4, 'A', 53]]).read(), /cabeçalhos duplicados/);
});
test('CSV numeric identifiers are normalized without changing CNPJ or phone', () => {
  const rows = reader([]).normalize([{ Código: ' 3 ', Cliente: 'A', 'RCA 1': '53,0', 'CNPJ/CPF': '001', Telefone: '0730000000' }]);
  assert.equal(rows[0]['RCA 1'], '53');
  assert.equal(rows[0].Telefone, '0730000000');
  assert.equal(rows[0]['CNPJ/CPF'], '001');
});
