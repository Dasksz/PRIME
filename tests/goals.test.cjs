const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const GoalsCore = require('../goals-core.js');
const app = fs.readFileSync(require.resolve('../app.js'), 'utf8');
function source(name) {
    const start = app.indexOf(`        function ${name}(`);
    assert.ok(start >= 0, name);
    const end = app.indexOf('\n        }', start) + '\n        }'.length;
    return app.slice(start, end);
}
function context(extra = {}, names = []) {
    const ctx = vm.createContext({GoalsCore, Map, Set, window: {}, console: {log(){}, warn(){}},
        normalize: s => s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase(),
        parseDate: s => new Date(s), globalClientGoals: new Map(), goalsSellerTargets: new Map(),
        goalsTargets: {'707': {fat: 5, vol: 10}},
        goalsPosAdjustments: {ELMA_ALL: new Map()},
        goalsMixSaltyAdjustments: {PEPSICO_ALL: new Map()},
        goalsMixFoodsAdjustments: {PEPSICO_ALL: new Map()},
        optimizedData: {rcaCodeByName: new Map([['Ana', '1'], ['Bia', '2'], ['AMERICANAS', '1001']]),
            clientsByRca: new Map(), indices: {history: {byClient: new Map()}}, historyById: new Map()},
        updateGoalsView(){}, document: {getElementById(){return null}}, alert(){}, ...extra});
    vm.runInContext(names.map(source).join('\n'), ctx);
    return ctx;
}
function fixture(ctx, seller, clients) {
    ctx.optimizedData.clientsByRca.set(ctx.optimizedData.rcaCodeByName.get(seller), clients.map(([id, sales]) => {
        const ids = sales.map((sale, i) => {
            const key = `${id}-${i}`;
            ctx.optimizedData.historyById.set(key, {CODFOR:'1119', TIPOVENDA:'1', CODUSUR:'1', VLVENDA:10, TOTPESOLIQ:1, DTPED:'2026-09-01', ...sale});
            return key;
        });
        ctx.optimizedData.indices.history.byClient.set(id, ids);
        return {'Código': id, rca1:'1', razaoSocial:'Cliente'};
    }));
}
test('Rateio inteiro fecha a meta, inclusive um cliente entre dois vendedores', () => {
    assert.deepEqual(GoalsCore.allocate(1, [1, 1]), [1, 0]);
    for(let n=1;n<40;n++) for(let total=0;total<50;total++) {
        const result = GoalsCore.allocate(total, Array.from({length:n},(_,i)=>(i*17)%9));
        assert.equal(result.reduce((a,b)=>a+b,0), total);
        assert.ok(result.every(Number.isInteger));
    }
});
test('Rateio de dinheiro e kg conserva centavos e gramas', () => {
    for (const [total, precision] of [[100.01,2],[0.001,3],[60203.350,3]]) {
        const values = GoalsCore.allocate(total,[3,8,17,0,2],precision);
        assert.equal(values.reduce((sum,value)=>sum+Math.round(value*10**precision),0),Math.round(total*10**precision));
    }
    assert.deepEqual(GoalsCore.allocate(5,[0,0,0]),[2,2,1]);
});
test('Entrada brasileira não converte lixo ou valor negativo silenciosamente em zero', () => {
    assert.equal(GoalsCore.parseInput('1.234,56'),1234.56);
    assert.equal(GoalsCore.parseInput('0'),0);
    for (const input of ['', '-1', 'NaN', '1.5', '1,2,3', 'abc']) assert.throws(()=>GoalsCore.parseInput(input));
});
test('Toddynho e Toddy têm categorias exclusivas', () => {
    assert.equal(GoalsCore.category({CODFOR:1119,DESCRICAO:'TODDYNHO'}),'1119_TODDYNHO');
    assert.equal(GoalsCore.category({CODFOR:1119,DESCRICAO:'TODDY'}),'1119_TODDY');
});
test('Rateio real de Foods não duplica Toddynho em Toddy', () => {
    const c=context({},['isActiveClient','distributeSellerGoal']);
    fixture(c,'Ana',[['1',[{DESCRICAO:'TODDYNHO'}]]]);
    c.distributeSellerGoal('Ana','tonelada_foods',100,'vol');
    assert.equal(c.globalClientGoals.get('1').get('1119_TODDYNHO').vol,100);
    assert.equal(c.globalClientGoals.get('1').get('1119_TODDY').vol,0);
});
test('Rateio real fecha sem histórico e preserva a outra métrica', () => {
    const c=context({},['isActiveClient','distributeSellerGoal']);
    fixture(c,'Ana',[['1',[]],['2',[]],['3',[]]]);
    c.distributeSellerGoal('Ana','707',100.01,'fat');
    c.distributeSellerGoal('Ana','707',0.001,'vol');
    assert.equal([...c.globalClientGoals.values()].reduce((s,g)=>s+Math.round(g.get('707').fat*100),0),10001);
    assert.equal([...c.globalClientGoals.values()].reduce((s,g)=>s+Math.round(g.get('707').vol*1000),0),1);
    c.distributeSellerGoal('Ana','707',0,'fat');
    assert.equal([...c.globalClientGoals.values()].reduce((s,g)=>s+g.get('707').fat,0),0);
});
test('Positivação usa saldo >= R$1 e tipos de venda válidos', () => {
    const c=context({},['isActiveClient','getSellerNaturalCount']);
    fixture(c,'Ana',[['1',[{CODFOR:'707',VLVENDA:10},{CODFOR:'707',VLVENDA:-10}]],['2',[{CODFOR:'707',VLVENDA:.5}]],['3',[{CODFOR:'707',TIPOVENDA:'5'}]],['4',[{CODFOR:'707',VLVENDA:1}]]]);
    assert.equal(c.getSellerNaturalCount('Ana','total_elma'),1);
});
test('Mix histórico ignora bonificação e valores abaixo de R$1 e mantém divisor 3', () => {
    const c=context({MIX_SALTY_CATEGORIES:['A','B'],MIX_FOODS_CATEGORIES:['C']},['isActiveClient','getHistoricalMix']);
    fixture(c,'Ana',[['1',[{DESCRICAO:'A'},{DESCRICAO:'B',TIPOVENDA:'5'}]],['2',[{DESCRICAO:'A'},{DESCRICAO:'B',VLVENDA:.5}]],['3',[{DESCRICAO:'A'},{DESCRICAO:'B'}]]]);
    assert.equal(c.getHistoricalMix('Ana','salty'),1/3);
});
test('Mix distribuído fecha exatamente e exclui Americanas', () => {
    const c=context({getHistoricalMix:()=>1},['handleDistributeMix']);
    c.handleDistributeMix(1,'salty','PEPSICO_ALL',[{seller:'Ana'},{seller:'Bia'},{seller:'AMERICANAS'}]);
    assert.equal([...c.goalsSellerTargets.values()].reduce((s,t)=>s+t.mix_salty,0),1);
    assert.equal(c.goalsSellerTargets.get('AMERICANAS').mix_salty,0);
});
test('Snapshot restaura e limpa todos os ajustes mantendo referências dos Maps', () => {
    const c=context();
    const start=app.indexOf('        function serializeGoalAdjustments(');
    const end=app.indexOf('        function updateGoalsDistributionStatus(',start);
    vm.runInContext(app.slice(start,end),c);
    const original=c.globalClientGoals;
    c.globalClientGoals.set('1',new Map([['707',{fat:1,vol:2}]]));
    c.goalsSellerTargets.set('Ana',{total_elma:20});
    c.goalsPosAdjustments.ELMA_ALL.set('Ana',2);
    c.goalsMixSaltyAdjustments.PEPSICO_ALL.set('Ana',3);
    c.goalsMixFoodsAdjustments.PEPSICO_ALL.set('Ana',4);
    const saved=JSON.parse(JSON.stringify(c.window.getGoalsSnapshot()));
    c.window.resetGoalsState();
    assert.equal(c.globalClientGoals.size,0);
    assert.equal(c.goalsSellerTargets.size,0);
    assert.equal(c.goalsPosAdjustments.ELMA_ALL.size,0);
    assert.equal(c.goalsMixSaltyAdjustments.PEPSICO_ALL.size,0);
    assert.equal(c.goalsMixFoodsAdjustments.PEPSICO_ALL.size,0);
    c.window.restoreGoalsSnapshot(saved);
    assert.equal(c.globalClientGoals,original);
    assert.equal(c.globalClientGoals.get('1').get('707').fat,1);
    assert.equal(c.goalsPosAdjustments.ELMA_ALL.get('Ana'),2);
    assert.equal(c.goalsMixSaltyAdjustments.PEPSICO_ALL.get('Ana'),3);
    assert.equal(c.goalsMixFoodsAdjustments.PEPSICO_ALL.get('Ana'),4);
    c.window.restoreGoalsSnapshot({clients:saved.clients});
    assert.equal(c.goalsSellerTargets.size,0); // legacy snapshots remain supported
});
test('Status distingue meta vazia, rateio pendente e distribuído', () => {
    const els={'goals-distribution-status':{},'goal-global-fat':{value:'0'},'goal-global-vol':{value:'0'}};
    const c=context({document:{getElementById:id=>els[id]}},['updateGoalsDistributionStatus']);
    c.updateGoalsDistributionStatus(0,0);assert.equal(els['goals-distribution-status'].textContent,'Sem meta distribuída');
    els['goal-global-fat'].value='10';c.updateGoalsDistributionStatus(0,0);assert.equal(els['goals-distribution-status'].textContent,'Rateio pendente');
    c.updateGoalsDistributionStatus(10,0);assert.equal(els['goals-distribution-status'].textContent,'100% Distribuído');
    els['goal-global-fat'].value='abc';c.updateGoalsDistributionStatus(10,0);assert.equal(els['goals-distribution-status'].textContent,'Meta inválida');
});
test('Edição no relatório alimenta o modelo e rejeita metas fracionárias de clientes', () => {
    let handler;let renders=0;let warnings=0;const distributed=[];
    const c=context({document:{getElementById:()=>({addEventListener:(_,h)=>handler=h})},distributeSellerGoal:(...args)=>distributed.push(args),recalculateTotalGoals(){},distributeDown(){},markDirty(){},updateGoalsSvView(){renders++},alert(){warnings++}});
    const start=app.indexOf("            document.getElementById('goals-sv-main-table').addEventListener('change'");
    const end=app.indexOf("            document.getElementById('goals-sv-export-xlsx-btn')",start);
    vm.runInContext(app.slice(start,end),c);
    const change=(category,metric,value)=>handler({target:{dataset:{goalSeller:'Ana',goalCategory:category,goalMetric:metric},value}});
    change('707','fat','1.234,56');assert.deepEqual(distributed[0],['Ana','707',1234.56,'fat']);
    change('total_elma','pos','20');assert.equal(c.goalsSellerTargets.get('Ana').total_elma,20);
    change('mix_salty','mix','3');assert.equal(c.goalsSellerTargets.get('Ana').mix_salty,3);
    change('total_elma','pos','1,5');assert.equal(c.goalsSellerTargets.get('Ana').total_elma,20);assert.equal(warnings,1);assert.equal(renders,4);
});
