const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('app.js','utf8');
const html = fs.readFileSync('index.html','utf8');
function boot() {
 const ids = new Set([...html.matchAll(/id="([^"]+)"/g)].map(x=>x[1]));
 const elements=new Map(), callbacks=[];
 ids.add('btn-export-goals-gv'); // Inserted dynamically by the existing pagination renderer.
 function element(id='generated') {
  if(elements.has(id)) return elements.get(id);
  const tag=html.match(new RegExp('<[^>]*id="'+id+'"[^>]*>'));
  const classes=new Set(tag?.[0].match(/class="([^"]*)"/)?.[1].split(/\s+/)||[]);
  const el={id,value:'',textContent:'',innerHTML:'',style:{},dataset:{},disabled:false,
   classList:{add(...xs){xs.forEach(x=>classes.add(x))},remove(...xs){xs.forEach(x=>classes.delete(x))},contains:x=>classes.has(x),toggle(x,on){if(on===undefined)on=!classes.has(x);on?classes.add(x):classes.delete(x)}},
   addEventListener(){},appendChild(){},insertBefore(){},replaceChild(){},cloneNode(){return element(id+'-clone')},remove(){},setAttribute(){},getAttribute(){return null},removeAttribute(){},contains(){return false},
   querySelector(){return element(id+'-child')},querySelectorAll(){return []},getContext(){return {}},getBoundingClientRect(){return {top:0,bottom:100}},
  };el.parentElement=el.parentNode=el;elements.set(id,el);return el;
 }
 const clients=[{'Código':'1',Cliente:'Cliente A',rca1:'10',rcas:['10'],ramo:'1',razaoSocial:'Cliente A'},{'Código':'2',Cliente:'Cliente B',rca1:'20',rcas:['20'],ramo:'1',razaoSocial:'Cliente B'}];
 const history=[];
 for(const month of [6,7,8])for(const [client,seller,value] of [['1','10',300],['2','20',600]])history.push({id:history.length+1,CODCLI:client,CODUSUR:seller,NOME:'Vendedor '+seller,SUPERV:'Supervisor A',CODPROD:'100',CODFOR:'707',FORNECEDOR:'ELMA',PASTA:'SALTY',TIPOVENDA:'1',VLVENDA:value,TOTPESOLIQ:value/10,QTVENDA:10,DTPED:new Date(2026,month,15).getTime(),FILIAL:'05'});
 const columnar=rows=>({columns:Object.keys(rows[0]||{}),values:Object.fromEntries(Object.keys(rows[0]||{}).map(k=>[k,rows.map(r=>r[k])])),length:rows.length});
 const window={embeddedData:{detailed:columnar([{...history[0],DTPED:new Date(2026,9,15).getTime(),VLVENDA:0,TOTPESOLIQ:0}]),history:columnar(history),clients:columnar(clients),byOrder:[],stockMap05:[],stockMap08:[],innovationsMonth:[],activeProductCodes:['100'],productDetails:new Map([['100',{codfor:'707',category:'SALTY'}]]),isColumnar:true},supabaseClient:{from:()=>({select:()=>({eq(){return this},maybeSingle:async()=>({data:null,error:null})})})},location:{href:'https://dasksz.github.io/PRIME/',search:''},userRole:'adm'};
 const doc={getElementById:id=>ids.has(id)?element(id):null,querySelector:s=>element(s),querySelectorAll:()=>[],createElement:()=>element('generated-'+elements.size),addEventListener(){},body:element('body')};
 const c=vm.createContext({window,document:doc,console:{log(){},warn(){},error(){}},Chart:{register(){}},ChartDataLabels:{},Map,Set,Date,Intl,Promise,URL,URLSearchParams,localStorage:{getItem(){return null},setItem(){}},alert(){},setTimeout:fn=>{callbacks.push(fn);return callbacks.length},clearTimeout(){},requestAnimationFrame:fn=>{callbacks.push(fn)},performance:{now:()=>0}});
 const exported=source.replace(/\}\)\(\);\s*$/, 'window.testGoals = { globalGoalsMetrics, goalsTargets, globalClientGoals, calculateDistributedGoals, getGoalsFilteredData, parseGoalsSvStructure, updateGoalsSvView, updateGoalsSummaryView, filterSeller(name) { selectedGoalsGvSellers = name ? [name] : []; }, filterSupervisor(name) { selectedGoalsGvSupervisors = name ? [name] : []; } };})();');
 vm.runInContext(exported,c,{timeout:5000});
 return {window,elements,c,callbacks};
}
test('only Metas is a routable page in markup and code',()=>{
 assert.deepEqual([...html.matchAll(/data-target="([^"]+)"/g)].map(x=>x[1]),['goals']);
 for(const id of ['main-dashboard','city-view','weekly-view','comparison-view','stock-view','innovations-month-view','mix-view','meta-realizado-view','coverage-view'])assert.equal(html.includes('id="'+id+'"'),false,id);
 for(const fn of ['updateCityView','updateWeeklyView','updateComparisonView','updateStockView','updateMixView','updateMetaRealizadoView','updateCoverageView'])assert.equal(source.includes('function '+fn+'('),false,fn);
});
test('Metas initializes its original calculations and import parser without removed page elements',()=>{
 const s=boot();
 assert.equal(s.elements.get('goals-view').classList.contains('hidden'),false);
 assert.equal(s.window.testGoals.globalGoalsMetrics['707'].fat,2700);
 assert.equal(s.window.testGoals.globalGoalsMetrics['707'].prevFat,900);
 const filtered=s.window.testGoals.getGoalsFilteredData();assert.equal(filtered.length,2);
 const allocation=s.window.testGoals.calculateDistributedGoals(filtered,'707',null,900,90);
 assert.equal(allocation.globalTotalAvgFat,900);
 assert.equal(allocation.globalTotalAvgVol,90);
 assert.equal(allocation.clientMetrics.find(c=>c.cod==='1').avgFat,300);
 assert.equal(allocation.clientMetrics.find(c=>c.cod==='2').avgFat,600);
 assert.equal(allocation.clientMetrics.reduce((total,c)=>total+c.shareFat,0),1);
 s.window.testGoals.filterSeller('Vendedor 10');
 assert.equal(s.window.testGoals.getGoalsFilteredData().length,1);
 assert.equal(s.window.testGoals.getGoalsFilteredData()[0]['Código'],'1');
 s.window.testGoals.filterSeller('');
 s.elements.get('goals-gv-codcli-filter').value='2';
 assert.equal(s.window.testGoals.getGoalsFilteredData()[0]['Código'],'2');
 assert.equal(typeof s.window.testGoals.parseGoalsSvStructure,'function');
});

test('rateio rows, summary and supervisor report render without removed panels',async()=>{
 const s=boot();
 for(let i=0;s.callbacks.length&&i<100;i++)await s.callbacks.shift()();
 await new Promise(resolve=>setImmediate(resolve));
 assert.match(s.elements.get('goals-gv-table-body').innerHTML,/Cliente A/);
 assert.match(s.elements.get('goals-gv-table-body').innerHTML,/Cliente B/);
 s.window.testGoals.updateGoalsSummaryView();
 s.elements.get('goals-sv-content').classList.remove('hidden');
 s.window.testGoals.updateGoalsSvView();
 for(let i=0;s.callbacks.length&&i<100;i++)await s.callbacks.shift()();
 await new Promise(resolve=>setImmediate(resolve));
 assert.ok(s.elements.get('goals-summary-grid').innerHTML.length>0);
 assert.ok([...s.elements].some(([id,el])=>id.includes('goals-sv')&&el.innerHTML.includes('Supervisor A')));
});

