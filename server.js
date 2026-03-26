require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const DIAS_ESTAGNADO = 14;
const DIAS_DATA_DECAY = 30;

// ═══════════════════════════════════════════════════════════════
// CONFIGURAÇÃO DINÂMICA DE CLIENTES
// Basta adicionar CLIENTEN_NOME, CLIENTEN_SUBDOMINIO, CLIENTEN_TOKEN
// e CLIENTEN_VENCIMENTO no .env — o servidor detecta automaticamente.
// ═══════════════════════════════════════════════════════════════

function carregarClientes() {
    const clientes = [];
    let id = 1;
    while (process.env[`CLIENTE${id}_NOME`]) {
        clientes.push({
            id,
            nome: process.env[`CLIENTE${id}_NOME`],
            subdominio: process.env[`CLIENTE${id}_SUBDOMINIO`],
            token: process.env[`CLIENTE${id}_TOKEN`],
            vencimento: process.env[`CLIENTE${id}_VENCIMENTO`] || null,
            recorrencia: (process.env[`CLIENTE${id}_RECORRENCIA`] || 'sim').toLowerCase() === 'sim'
        });
        id++;
    }
    console.log(`📋 ${clientes.length} cliente(s) carregado(s) do .env`);
    clientes.forEach(c => console.log(`   • ${c.nome} (${c.subdominio})`));
    return clientes;
}

const clientes = carregarClientes();

const MESES_PT = ['janeiro','fevereiro','março','abril','maio','junho','julho','agosto','setembro','outubro','novembro','dezembro'];

// ═══════════════════════════════════════════════════════════════
// RATE LIMITER — respeita o limite de 7 req/s por conta Kommo
// ═══════════════════════════════════════════════════════════════

class KommoRateLimiter {
    constructor() {
        this.queues = {};
        this.timestamps = {};
        this.MAX_PER_SECOND = 7;
        this.SAFETY_MARGIN = 1;
        this.EFFECTIVE_LIMIT = this.MAX_PER_SECOND - this.SAFETY_MARGIN;
        this.MAX_RETRIES = 4;
        this.BASE_DELAY_MS = 1000;
    }

    async waitForSlot(subdominio) {
        if (!this.timestamps[subdominio]) this.timestamps[subdominio] = [];
        const now = Date.now();
        this.timestamps[subdominio] = this.timestamps[subdominio].filter(t => now - t < 1000);
        if (this.timestamps[subdominio].length >= this.EFFECTIVE_LIMIT) {
            const oldestInWindow = this.timestamps[subdominio][0];
            const waitTime = 1000 - (now - oldestInWindow) + 50;
            console.log(`   ⏳ Rate limit: aguardando ${waitTime}ms para ${subdominio}`);
            await this.sleep(waitTime);
            return this.waitForSlot(subdominio);
        }
        this.timestamps[subdominio].push(Date.now());
    }

    async request(subdominio, config, retryCount = 0) {
        if (!this.queues[subdominio]) this.queues[subdominio] = Promise.resolve();
        const result = new Promise(async (resolve, reject) => {
            await this.queues[subdominio];
            try {
                await this.waitForSlot(subdominio);
                const response = await axios(config);
                resolve(response);
            } catch (error) {
                if (error.response && error.response.status === 429) {
                    if (retryCount >= this.MAX_RETRIES) { reject(error); return; }
                    const delay = this.BASE_DELAY_MS * Math.pow(2, retryCount);
                    console.warn(`   ⚠️  429 de ${subdominio}. Retry #${retryCount + 1} em ${delay}ms...`);
                    await this.sleep(delay);
                    this.timestamps[subdominio] = [];
                    try { resolve(await this.request(subdominio, config, retryCount + 1)); }
                    catch (e) { reject(e); }
                    return;
                }
                if (error.response && error.response.status === 403) {
                    console.error(`   🚫 403 — IP bloqueado para ${subdominio}. Pausando 60s...`);
                    if (retryCount < 2) {
                        await this.sleep(60000);
                        this.timestamps[subdominio] = [];
                        try { resolve(await this.request(subdominio, config, retryCount + 1)); }
                        catch (e) { reject(e); }
                        return;
                    }
                    reject(error); return;
                }
                reject(error);
            }
        });
        this.queues[subdominio] = result.catch(() => {});
        return result;
    }

    sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
}

const rateLimiter = new KommoRateLimiter();

async function kommoGet(subdominio, token, urlPath) {
    return rateLimiter.request(subdominio, {
        method: 'get',
        url: `https://${subdominio}.kommo.com${urlPath}`,
        headers: { 'Authorization': `Bearer ${token}` }
    });
}

// ═══════════════════════════════════════════════════════════════
// CACHE
// ═══════════════════════════════════════════════════════════════

const cache = {};
const CACHE_TTL = 5 * 60 * 1000;
function getCached(key) {
    const e = cache[key];
    if (e && Date.now() - e.timestamp < CACHE_TTL) return e.data;
    return null;
}
function setCache(key, data) { cache[key] = { data, timestamp: Date.now() }; }

// ═══════════════════════════════════════════════════════════════
// BUSCAR DADOS DO CLIENTE
// ═══════════════════════════════════════════════════════════════

async function buscarDadosCliente(cliente, dataInicio, dataFim) {
    const hoje = Math.floor(Date.now() / 1000);
    const sub = cliente.subdominio;
    const token = cliente.token;

    // 1. Pipelines (cache)
    const ckP = `pipelines_${sub}`;
    let pipelinesDaConta = getCached(ckP);
    if (!pipelinesDaConta) {
        const r = await kommoGet(sub, token, '/api/v4/leads/pipelines');
        pipelinesDaConta = r.data._embedded.pipelines;
        setCache(ckP, pipelinesDaConta);
    }

    // 2. Loss Reasons (cache)
    const ckLR = `loss_reasons_${sub}`;
    let lossReasonsMap = getCached(ckLR);
    if (!lossReasonsMap) {
        lossReasonsMap = {};
        try {
            const r = await kommoGet(sub, token, '/api/v4/leads/loss_reasons');
            if (r.data?._embedded) r.data._embedded.loss_reasons.forEach(lr => { lossReasonsMap[lr.id] = lr.name; });
        } catch (err) {
            if (!err.response || err.response.status !== 204) console.log(`⚠ Sem motivos de perda para ${cliente.nome}`);
        }
        setCache(ckLR, lossReasonsMap);
    }

    // 3. Leads (paginação)
    let ldf = '';
    if (dataInicio && dataFim) ldf = `&filter[created_at][from]=${dataInicio}&filter[created_at][to]=${dataFim}`;
    else if (dataInicio) ldf = `&filter[created_at][from]=${dataInicio}`;
    else if (dataFim) ldf = `&filter[created_at][to]=${dataFim}`;

    let allLeads = [], page = 1, hasMore = true;
    console.log(`\n⏳ Puxando leads de ${cliente.nome}...`);
    while (hasMore) {
        try {
            const r = await kommoGet(sub, token, `/api/v4/leads?limit=250&page=${page}&with=loss_reason${ldf}`);
            const leads = r.data?._embedded?.leads || [];
            allLeads = allLeads.concat(leads);
            if (leads.length < 250) hasMore = false; else page++;
        } catch (err) {
            if (err.response?.status === 204) hasMore = false; else throw err;
        }
    }
    console.log(`   ✅ ${allLeads.length} leads (${page} pág.) — ${cliente.nome}`);

    // 4. Tarefas atrasadas
    let tarefasAtrasadasCount = 0;
    try {
        const r = await kommoGet(sub, token, `/api/v4/tasks?filter[is_completed]=0&filter[complete_till][to]=${hoje}&limit=250`);
        tarefasAtrasadasCount = r.data?._embedded?.tasks?.length || 0;
    } catch (err) { }

    // 5. Usuários (cache)
    const ckU = `users_${sub}`;
    let usersData = getCached(ckU);
    if (!usersData) {
        let totalUsers = 0, adminUsers = 0;
        try {
            const r = await kommoGet(sub, token, '/api/v4/users');
            const users = r.data?._embedded?.users || [];
            const active = users.filter(u => u.rights?.is_active !== false);
            totalUsers = active.length;
            adminUsers = active.filter(u => u.rights?.is_admin === true).length;
        } catch (err) { }
        usersData = { totalUsers, adminUsers };
        setCache(ckU, usersData);
    }
    const { totalUsers, adminUsers } = usersData;

    // ── 6. Processar ──
    let pipelinesData = [];
    let totalStagnantLeadsConta = 0, totalDataDecayConta = 0;
    let totalWonRevenue = 0, totalSalesCycleDays = 0, wonLeadsForCycle = 0, pipelineRevenue = 0;
    let monthlyStats = {}, lossReasonCounts = {};

    let globalWonStatusIds = new Set([142]);
    let globalLostStatusIds = new Set([143]);

    pipelinesDaConta.forEach(p => {
        p._embedded.statuses.forEach(s => {
            const n = s.name.toLowerCase();
            if (s.id == 142 || n.includes('ganho') || n.includes('sucesso') || n.includes('success')) globalWonStatusIds.add(s.id);
            else if (s.id == 143 || n.includes('perdid') || n.includes('lost')) globalLostStatusIds.add(s.id);
        });
    });

    allLeads.forEach(lead => {
        const d = new Date(lead.created_at * 1000);
        const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
        if (!monthlyStats[mk]) monthlyStats[mk] = { leads: 0, won: 0, lost: 0 };
        monthlyStats[mk].leads++;
        if (globalWonStatusIds.has(lead.status_id)) monthlyStats[mk].won++;
        if (globalLostStatusIds.has(lead.status_id)) {
            monthlyStats[mk].lost++;
            const rid = lead.loss_reason_id;
            if (rid && lossReasonsMap[rid]) lossReasonCounts[lossReasonsMap[rid]] = (lossReasonCounts[lossReasonsMap[rid]] || 0) + 1;
            else if (rid) lossReasonCounts['Motivo não identificado'] = (lossReasonCounts['Motivo não identificado'] || 0) + 1;
            else lossReasonCounts['Sem motivo registrado'] = (lossReasonCounts['Sem motivo registrado'] || 0) + 1;
        }
    });

    pipelinesDaConta.forEach(pipeline => {
        let wonCount = 0, lostCount = 0, stages = [], totalLeadsNestePipeline = 0;
        const statusList = pipeline._embedded.statuses;
        let wonStatusIds = [142], lostStatusIds = [143], exemptStatusIds = [];

        statusList.forEach(s => {
            const n = s.name.toLowerCase();
            if (s.id == 142 || n.includes('ganho') || n.includes('sucesso') || n.includes('success')) { if (!wonStatusIds.includes(s.id)) wonStatusIds.push(s.id); }
            else if (s.id == 143 || n.includes('perdid') || n.includes('lost')) { if (!lostStatusIds.includes(s.id)) lostStatusIds.push(s.id); }
            else if (n.includes('não respondeu') || n.includes('nao respondeu') || n.includes('follow up') || n.includes('follow-up')) { if (!exemptStatusIds.includes(s.id)) exemptStatusIds.push(s.id); }
        });

        const closedStatuses = [...wonStatusIds, ...lostStatusIds];
        const statsPorStatus = {};
        statusList.forEach(s => { statsPorStatus[s.id] = { count: 0, totalDays: 0, name: s.name }; });

        let pipelineMonthly = {}, pipelineLossReasons = {};

        allLeads.forEach(lead => {
            if (lead.pipeline_id !== pipeline.id) return;
            totalLeadsNestePipeline++;
            const diasSemAtt = (hoje - lead.updated_at) / 86400;
            const price = Number(lead.price) || 0;

            // Monthly stats per pipeline
            const d = new Date(lead.created_at * 1000);
            const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
            if (!pipelineMonthly[mk]) pipelineMonthly[mk] = { leads: 0, won: 0, lost: 0 };
            pipelineMonthly[mk].leads++;

            if (!closedStatuses.includes(lead.status_id)) {
                pipelineRevenue += price;
                if (!exemptStatusIds.includes(lead.status_id)) {
                    if (diasSemAtt >= DIAS_ESTAGNADO) totalStagnantLeadsConta++;
                    const semTarefa = !lead.closest_task_at || lead.closest_task_at < hoje;
                    if (diasSemAtt >= DIAS_DATA_DECAY && semTarefa) totalDataDecayConta++;
                }
                if (statsPorStatus[lead.status_id]) statsPorStatus[lead.status_id].totalDays += diasSemAtt;
            } else {
                if (wonStatusIds.includes(lead.status_id)) {
                    totalWonRevenue += price;
                    wonLeadsForCycle++;
                    const closedAt = lead.closed_at || lead.updated_at;
                    const cycle = (closedAt - lead.created_at) / 86400;
                    if (cycle > 0) totalSalesCycleDays += cycle;
                    pipelineMonthly[mk].won++;
                } else if (lostStatusIds.includes(lead.status_id)) {
                    pipelineMonthly[mk].lost++;
                    const rid = lead.loss_reason_id;
                    if (rid && lossReasonsMap[rid]) pipelineLossReasons[lossReasonsMap[rid]] = (pipelineLossReasons[lossReasonsMap[rid]] || 0) + 1;
                    else if (rid) pipelineLossReasons['Motivo não identificado'] = (pipelineLossReasons['Motivo não identificado'] || 0) + 1;
                    else pipelineLossReasons['Sem motivo registrado'] = (pipelineLossReasons['Sem motivo registrado'] || 0) + 1;
                }
            }
            if (statsPorStatus[lead.status_id]) statsPorStatus[lead.status_id].count++;
        });

        statusList.forEach(status => {
            const stat = statsPorStatus[status.id];
            if (wonStatusIds.includes(status.id)) wonCount += stat.count;
            else if (lostStatusIds.includes(status.id)) lostCount += stat.count;
            else stages.push({ id: status.id, name: status.name, count: stat.count, avgDays: stat.count > 0 ? (stat.totalDays / stat.count).toFixed(1) : 0 });
        });

        // Monthly array per pipeline
        const pipelineMonthlyArray = Object.keys(pipelineMonthly).sort().map(key => {
            const [year, month] = key.split('-');
            return { key, label: `${MESES_PT[parseInt(month) - 1]}/${year.slice(2)}`, ...pipelineMonthly[key] };
        });
        // Loss reasons per pipeline
        const pipelineTotalLost = Object.values(pipelineLossReasons).reduce((a, b) => a + b, 0);
        const pipelineLossArray = Object.entries(pipelineLossReasons)
            .map(([name, count]) => ({ name, count, percent: pipelineTotalLost > 0 ? parseFloat(((count / pipelineTotalLost) * 100).toFixed(2)) : 0 }))
            .sort((a, b) => b.count - a.count);

        pipelinesData.push({ id: pipeline.id, name: pipeline.name, totalLeadsEntrants: totalLeadsNestePipeline, stages, results: { won: wonCount, lost: lostCount }, monthlyStats: pipelineMonthlyArray, lossReasons: pipelineLossArray });
    });

    const avgSalesCycle = wonLeadsForCycle > 0 ? (totalSalesCycleDays / wonLeadsForCycle) : 0;

    // Monthly array
    const monthlyArray = Object.keys(monthlyStats).sort().map(key => {
        const [year, month] = key.split('-');
        return { key, label: `${MESES_PT[parseInt(month) - 1]}/${year.slice(2)}`, labelFull: `${MESES_PT[parseInt(month) - 1]} ${year}`, ...monthlyStats[key] };
    });

    // Loss reasons array
    const totalLost = Object.values(lossReasonCounts).reduce((a, b) => a + b, 0);
    const lossReasonsArray = Object.entries(lossReasonCounts)
        .map(([name, count]) => ({ name, count, percent: totalLost > 0 ? parseFloat(((count / totalLost) * 100).toFixed(2)) : 0 }))
        .sort((a, b) => b.count - a.count);

    // Billing
    let diasRestantes = null, statusFatura = 'Sem Registro', dataFormatada = '--/--/----';
    if (cliente.vencimento) {
        const vd = new Date(`${cliente.vencimento}T23:59:59`);
        diasRestantes = Math.ceil((vd - new Date()) / 86400000);
        dataFormatada = vd.toLocaleDateString('pt-BR');
        if (diasRestantes < 0) statusFatura = `Vencido há ${Math.abs(diasRestantes)} dias`;
        else if (diasRestantes === 0) statusFatura = 'Vence hoje!';
        else statusFatura = `Em dia (vence em ${diasRestantes} dias)`;
    }

    return {
        id: cliente.id, name: cliente.nome, recorrencia: cliente.recorrencia,
        leadsLast30Days: allLeads.length, stagnantLeads: totalStagnantLeadsConta,
        dataDecayLeads: totalDataDecayConta, overdueTasks: tarefasAtrasadasCount,
        usersTotal: totalUsers, usersAdmins: adminUsers,
        wonRevenue: totalWonRevenue, pipelineRevenue,
        avgSalesCycle: parseFloat(avgSalesCycle.toFixed(1)),
        pipelines: pipelinesData, monthlyStats: monthlyArray, lossReasons: lossReasonsArray,
        billing: { date: dataFormatada, daysRemaining: diasRestantes, statusText: statusFatura }
    };
}

// ═══════════════════════════════════════════════════════════════
// ROTAS
// ═══════════════════════════════════════════════════════════════

app.get('/api/clientes', (req, res) => {
    res.json(clientes.map(c => ({ id: c.id, nome: c.nome })));
});

app.get('/api/dados-dashboard', async (req, res) => {
    const { inicio: dataInicio, fim: dataFim, clienteId: clienteIdFiltro } = req.query;
    let lista = clientes;
    if (clienteIdFiltro && clienteIdFiltro !== 'todos') {
        const found = clientes.find(c => c.id === parseInt(clienteIdFiltro));
        if (found) lista = [found];
    }
    let resultados = [];
    for (let cliente of lista) {
        try { resultados.push(await buscarDadosCliente(cliente, dataInicio, dataFim)); }
        catch (e) { console.error(`❌ ${cliente.nome}: ${e.response?.status || e.message}`); }
    }
    res.json(resultados);
});

app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString(), clients: clientes.length,
        rateLimiter: { activeQueues: Object.keys(rateLimiter.queues).length, effectiveLimit: `${rateLimiter.EFFECTIVE_LIMIT} req/s` }
    });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ Porta ${PORT} | ${clientes.length} clientes | Rate limiting ativo`));