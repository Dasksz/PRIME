const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const {webcrypto}=require('node:crypto');
test('Metas accepts only sales, history and clients and does not write removed catalogs',async()=>{
 let source=fs.readFileSync('worker.js','utf8');
 const start=source.indexOf('        const readFile =');
 const end=source.indexOf('        const processSalesData =',start);
 assert.ok(start>=0&&end>start);
 // File decoding is covered by client-import.test; exercise the real processing pipeline here.
 source=source.slice(0,start)+'const readFile = async file => file.rows;\n'+source.slice(end);
 const messages=[];
 const context=vm.createContext({console,TextEncoder,TextDecoder,self:{importScripts(){},crypto:webcrypto,postMessage:x=>messages.push(x)}});
 vm.runInContext(source,context);
 const sale={CODCLI:'1',PEDIDO:'123',CODUSUR:'10',NOME:'VENDEDOR',SUPERV:'SUPERV A',CODSUPERVISOR:'1',DTPED:'15/10/2026',DTSAIDA:'15/10/2026',PRODUTO:'100',DESCRICAO:'PRODUTO A',FORNECEDOR:'ELMA',CODFOR:'707',QTVENDA:24,VLVENDA:240,VLBONIFIC:0,TOTPESOLIQ:12,ESTOQUEUNIT:0,ESTOQUECX:0,TIPOVENDA:'1',FILIAL:'05',POSICAO:'F'};
 await context.self.onmessage({data:{salesFile:{rows:[sale]},historyFile:{rows:[{...sale,DTPED:'15/09/2026',DTSAIDA:'15/09/2026'}]},clientsFile:{rows:[{'Código':'1',Cliente:'Cliente A','RCA 1':'10',Cidade:'ILHEUS'}]},configCityBranches:[{cidade:'ILHEUS',filial:'05'}]}});
 const error=messages.find(x=>x.type==='error');assert.equal(error,undefined,JSON.stringify(error));
 const result=messages.find(x=>x.type==='result');assert.ok(result);
 assert.equal(result.data.detailed.values.VLVENDA[0],240);
 assert.equal(result.data.detailed.values.TOTPESOLIQ[0],12);
 assert.equal(result.data.history.values.VLVENDA[0],240);
 assert.equal(result.data.clients.length,1);
 for(const key of ['innovations','product_details','active_products'])assert.equal(key in result.data,false,key);
 const app=fs.readFileSync('app.js','utf8'),init=fs.readFileSync('init.js','utf8'),html=fs.readFileSync('index.html','utf8');
 for(const table of ['data_innovations','data_product_details','data_active_products']){
  assert.equal(app.includes("clearTable('"+table+"'"),false);
  assert.equal(init.includes("fetchAll('"+table+"'"),false);
 }
 for(const id of ['products-file-input','innovations-file-input'])assert.equal(html.includes(id),false);
});
