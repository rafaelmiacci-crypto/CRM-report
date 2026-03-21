require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');

const path = require('path');

const app = express();
app.use(cors());

// Serve o frontend estático
app.use(express.static(path.join(__dirname, 'public')));

const DIAS_ESTAGNADO = 14;
const DIAS_DATA_DECAY = 30;

const clientes = [
    { id: 1, nome: process.env.CLIENTE1_NOME, subdominio: process.env.CLIENTE1_SUBDOMINIO, token: process.env.CLIENTE1_TOKEN, vencimento: process.env.CLIENTE1_VENCIMENTO },
    { id: 2, nome: process.env.CLIENTE2_NOME, subdominio: process.env.CLIENTE2_SUBDOMINIO, token: process.env.CLIENTE2_TOKEN, vencimento: process.env.CLIENTE2_VENCIMENTO },
    { id: 3, nome: process.env.CLIENTE3_NOME, subdominio: process.env.CLIENTE3_SUBDOMINIO, token: process.env.CLIENTE3_TOKEN, vencimento: process.env.CLIENTE3_VENCIMENTO }
];

const MESES_PT = ['janeiro','fevereiro','março','abril','maio','junho','julho','agosto','setembro','outubro','novembro','dezembro'];

// ═══════════════════════════════════════════════════════════════
// RATE LIMITER — respeita o limite de 7 req/s por conta Kommo
// ═══════════════════════════════════════════════════════════════

class KommoRateLimiter {
    constructor() {
        // Fila separada por subdomínio (cada conta Kommo tem seu próprio limite)
        this.queues = {};          // { subdominio: Promise }
        this.timestamps = {};      // { subdominio: [timestamps] }
        this.MAX_PER_SECOND = 7;
        this.SAFETY_MARGIN = 1;    // Usa no máximo 6 req/s (margem de segurança)
        this.EFFECTIVE_LIMIT = this.MAX_PER_SECOND - this.SAFETY_MARGIN;
        this.MAX_RETRIES = 4;
        this.BASE_DELAY_MS = 1000;
    }

    /**
     * Aguarda até que haja "slot" disponível para a conta (subdomínio).
     * Garante que não ultrapasse EFFECTIVE_LIMIT requisições por segundo.
     */
    async waitForSlot(subdominio) {
        if (!this.timestamps[subdominio]) this.timestamps[subdominio] = [];

        const now = Date.now();
        // Remove timestamps com mais de 1 segundo
        this.timestamps[subdominio] = this.timestamps[subdominio].filter(t => now - t < 1000);

        if (this.timestamps[subdominio].length >= this.EFFECTIVE_LIMIT) {
            // Calcula quanto tempo falta para o timestamp mais antigo "expirar"
            const oldestInWindow = this.timestamps[subdominio][0];
            const waitTime = 1000 - (now - oldestInWindow) + 50; // +50ms de margem
            console.log(`   ⏳ Rate limit: aguardando ${waitTime}ms para ${subdominio}`);
            await this.sleep(waitTime);
            return this.waitForSlot(subdominio); // Recursão para re-checar
        }

        this.timestamps[subdominio].push(Date.now());
    }

    /**
     * Executa uma requisição com rate limiting + retry com backoff exponencial.
     * Trata os códigos 429 (rate limit) e 403 (IP bloqueado).
     */
    async request(subdominio, config, retryCount = 0) {
        // Serializa requisições por subdomínio para evitar race conditions
        if (!this.queues[subdominio]) this.queues[subdominio] = Promise.resolve();

        const result = new Promise(async (resolve, reject) => {
            await this.queues[subdominio];

            try {
                await this.waitForSlot(subdominio);
                const response = await axios(config);
                resolve(response);
            } catch (error) {
                // ── 429: Rate limit excedido ──
                if (error.response && error.response.status === 429) {
                    if (retryCount >= this.MAX_RETRIES) {
                        console.error(`   ❌ 429 persistente após ${this.MAX_RETRIES} tentativas: ${config.url}`);
                        reject(error);
                        return;
                    }
                    const delay = this.BASE_DELAY_MS * Math.pow(2, retryCount);
                    console.warn(`   ⚠️  429 recebido de ${subdominio}. Retry #${retryCount + 1} em ${delay}ms...`);
                    await this.sleep(delay);
                    // Limpa timestamps para essa conta (servidor já rejeitou, resetar janela)
                    this.timestamps[subdominio] = [];
                    try {
                        const retryResult = await this.request(subdominio, config, retryCount + 1);
                        resolve(retryResult);
                    } catch (retryErr) {
                        reject(retryErr);
                    }
                    return;
                }

                // ── 403: IP bloqueado ──
                if (error.response && error.response.status === 403) {
                    console.error(`   🚫 403 — IP possivelmente bloqueado para ${subdominio}. Pausando 60s...`);
                    if (retryCount < 2) {
                        await this.sleep(60000); // Pausa longa de 60s
                        this.timestamps[subdominio] = [];
                        try {
                            const retryResult = await this.request(subdominio, config, retryCount + 1);
                            resolve(retryResult);
                        } catch (retryErr) {
                            reject(retryErr);
                        }
                        return;
                    }
                    reject(error);
                    return;
                }

                // ── 204: Sem conteúdo (normal para fim de paginação) ──
                if (error.response && error.response.status === 204) {
                    reject(error); // Deixa o caller tratar
                    return;
                }

                // ── Outros erros ──
                reject(error);
            }
        });

        // Atualiza a fila serializada para este subdomínio
        this.queues[subdominio] = result.catch(() => {}); // Evita unhandled rejection na chain

        return result;
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

const rateLimiter = new KommoRateLimiter();

// ═══════════════════════════════════════════════════════════════
// HELPER: Faz GET via rate limiter
// ═══════════════════════════════════════════════════════════════

async function kommoGet(subdominio, token, path) {
    const url = `https://${subdominio}.kommo.com${path}`;
    const config = {
        method: 'get',
        url,
        headers: { 'Authorization': `Bearer ${token}` }
    };
    return rateLimiter.request(subdominio, config);
}

// ═══════════════════════════════════════════════════════════════
// CACHE SIMPLES EM MEMÓRIA (evita requisições repetidas)
// ═══════════════════════════════════════════════════════════════

const cache = {};
const CACHE_TTL = 5 * 60 * 1000; // 5 minutos

function getCached(key) {
    const entry = cache[key];
    if (entry && Date.now() - entry.timestamp < CACHE_TTL) return entry.data;
    return null;
}

function setCache(key, data) {
    cache[key] = { data, timestamp: Date.now() };
}

// ═══════════════════════════════════════════════════════════════
// BUSCAR DADOS DO CLIENTE (com rate limiting integrado)
// ═══════════════════════════════════════════════════════════════

async function buscarDadosCliente(cliente, dataInicio, dataFim) {
    const hoje = Math.floor(Date.now() / 1000);
    const sub = cliente.subdominio;
    const token = cliente.token;

    // 1. Pipelines (usa cache — quase nunca muda)
    const cacheKeyPipelines = `pipelines_${sub}`;
    let pipelinesDaConta = getCached(cacheKeyPipelines);
    if (!pipelinesDaConta) {
        const responsePipelines = await kommoGet(sub, token, '/api/v4/leads/pipelines');
        pipelinesDaConta = responsePipelines.data._embedded.pipelines;
        setCache(cacheKeyPipelines, pipelinesDaConta);
    }

    // 2. Loss Reasons (usa cache)
    const cacheKeyLR = `loss_reasons_${sub}`;
    let lossReasonsMap = getCached(cacheKeyLR);
    if (!lossReasonsMap) {
        lossReasonsMap = {};
        try {
            const responseLR = await kommoGet(sub, token, '/api/v4/leads/loss_reasons');
            if (responseLR.data && responseLR.data._embedded) {
                responseLR.data._embedded.loss_reasons.forEach(lr => {
                    lossReasonsMap[lr.id] = lr.name;
                });
            }
        } catch (err) {
            if (!err.response || err.response.status !== 204) {
                console.log(`⚠ Sem motivos de perda para ${cliente.nome}`);
            }
        }
        setCache(cacheKeyLR, lossReasonsMap);
    }

    // 3. Leads (com paginação — rate-limited automaticamente)
    let leadDateFilter = '';
    if (dataInicio && dataFim) leadDateFilter = `&filter[created_at][from]=${dataInicio}&filter[created_at][to]=${dataFim}`;
    else if (dataInicio) leadDateFilter = `&filter[created_at][from]=${dataInicio}`;
    else if (dataFim) leadDateFilter = `&filter[created_at][to]=${dataFim}`;

    let allLeads = [];
    let page = 1;
    let hasMoreLeads = true;
    console.log(`\n⏳ Puxando leads do cliente ${cliente.nome}...`);

    while (hasMoreLeads) {
        try {
            const responseLeads = await kommoGet(
                sub, token,
                `/api/v4/leads?limit=250&page=${page}&with=loss_reason${leadDateFilter}`
            );
            const leadsDaPagina = responseLeads.data && responseLeads.data._embedded
                ? responseLeads.data._embedded.leads : [];
            allLeads = allLeads.concat(leadsDaPagina);
            if (leadsDaPagina.length < 250) hasMoreLeads = false;
            else page++;
        } catch (err) {
            if (err.response && err.response.status === 204) hasMoreLeads = false;
            else throw err;
        }
    }

    console.log(`   ✅ ${allLeads.length} leads encontrados para ${cliente.nome} (${page} páginas)`);

    // 4. Tarefas atrasadas
    let tarefasAtrasadasCount = 0;
    try {
        const responseTarefas = await kommoGet(
            sub, token,
            `/api/v4/tasks?filter[is_completed]=0&filter[complete_till][to]=${hoje}&limit=250`
        );
        tarefasAtrasadasCount = responseTarefas.data && responseTarefas.data._embedded
            ? responseTarefas.data._embedded.tasks.length : 0;
    } catch (err) { }

    // 5. Usuários (usa cache — raramente muda)
    const cacheKeyUsers = `users_${sub}`;
    let usersData = getCached(cacheKeyUsers);
    if (!usersData) {
        let totalUsers = 0;
        let adminUsers = 0;
        try {
            const responseUsers = await kommoGet(sub, token, '/api/v4/users');
            const users = responseUsers.data && responseUsers.data._embedded
                ? responseUsers.data._embedded.users : [];
            const activeUsersList = users.filter(u => u.rights && u.rights.is_active !== false);
            totalUsers = activeUsersList.length;
            adminUsers = activeUsersList.filter(u => u.rights && u.rights.is_admin === true).length;
        } catch (err) { }
        usersData = { totalUsers, adminUsers };
        setCache(cacheKeyUsers, usersData);
    }
    const { totalUsers, adminUsers } = usersData;

    // 6. Processar dados (lógica idêntica à original)
    let pipelinesData = [];
    let totalStagnantLeadsConta = 0;
    let totalDataDecayConta = 0;
    let totalWonRevenue = 0;
    let totalSalesCycleDays = 0;
    let wonLeadsForCycle = 0;
    let pipelineRevenue = 0;

    let monthlyStats = {};
    let lossReasonCounts = {};

    let globalWonStatusIds = new Set([142]);
    let globalLostStatusIds = new Set([143]);

    pipelinesDaConta.forEach(pipeline => {
        pipeline._embedded.statuses.forEach(s => {
            const nameLower = s.name.toLowerCase();
            if (s.id == 142 || nameLower.includes('ganho') || nameLower.includes('sucesso') || nameLower.includes('success')) globalWonStatusIds.add(s.id);
            else if (s.id == 143 || nameLower.includes('perdid') || nameLower.includes('lost')) globalLostStatusIds.add(s.id);
        });
    });

    allLeads.forEach(lead => {
        const createdDate = new Date(lead.created_at * 1000);
        const monthKey = `${createdDate.getFullYear()}-${String(createdDate.getMonth() + 1).padStart(2, '0')}`;

        if (!monthlyStats[monthKey]) monthlyStats[monthKey] = { leads: 0, won: 0, lost: 0 };
        monthlyStats[monthKey].leads++;

        if (globalWonStatusIds.has(lead.status_id)) monthlyStats[monthKey].won++;
        if (globalLostStatusIds.has(lead.status_id)) {
            monthlyStats[monthKey].lost++;
            const reasonId = lead.loss_reason_id;
            if (reasonId && lossReasonsMap[reasonId]) {
                const reasonName = lossReasonsMap[reasonId];
                lossReasonCounts[reasonName] = (lossReasonCounts[reasonName] || 0) + 1;
            } else if (reasonId) {
                lossReasonCounts['Motivo não identificado'] = (lossReasonCounts['Motivo não identificado'] || 0) + 1;
            } else {
                lossReasonCounts['Sem motivo registrado'] = (lossReasonCounts['Sem motivo registrado'] || 0) + 1;
            }
        }
    });

    pipelinesDaConta.forEach(pipeline => {
        let wonCount = 0; let lostCount = 0; let stages = []; let totalLeadsNestePipeline = 0;
        const statusList = pipeline._embedded.statuses;
        let wonStatusIds = [142]; let lostStatusIds = [143]; let exemptStatusIds = [];

        statusList.forEach(s => {
            const nameLower = s.name.toLowerCase();
            if (s.id == 142 || nameLower.includes('ganho') || nameLower.includes('sucesso') || nameLower.includes('success')) { if (!wonStatusIds.includes(s.id)) wonStatusIds.push(s.id); }
            else if (s.id == 143 || nameLower.includes('perdid') || nameLower.includes('lost')) { if (!lostStatusIds.includes(s.id)) lostStatusIds.push(s.id); }
            else if (nameLower.includes('não respondeu') || nameLower.includes('nao respondeu') || nameLower.includes('follow up') || nameLower.includes('follow-up')) { if (!exemptStatusIds.includes(s.id)) exemptStatusIds.push(s.id); }
        });

        const closedStatuses = [...wonStatusIds, ...lostStatusIds];
        const statsPorStatus = {};
        statusList.forEach(s => { statsPorStatus[s.id] = { count: 0, totalDays: 0, name: s.name }; });

        allLeads.forEach(lead => {
            if (lead.pipeline_id === pipeline.id) {
                totalLeadsNestePipeline++;
                const diasSemAtualizacao = (hoje - lead.updated_at) / (60 * 60 * 24);
                const price = Number(lead.price) || 0;
                if (!closedStatuses.includes(lead.status_id)) {
                    pipelineRevenue += price;
                    if (!exemptStatusIds.includes(lead.status_id)) {
                        if (diasSemAtualizacao >= DIAS_ESTAGNADO) totalStagnantLeadsConta++;
                        const semTarefaFutura = !lead.closest_task_at || lead.closest_task_at < hoje;
                        if (diasSemAtualizacao >= DIAS_DATA_DECAY && semTarefaFutura) totalDataDecayConta++;
                    }
                    if (statsPorStatus[lead.status_id]) statsPorStatus[lead.status_id].totalDays += diasSemAtualizacao;
                } else {
                    if (wonStatusIds.includes(lead.status_id)) {
                        totalWonRevenue += price;
                        wonLeadsForCycle++;
                        const closedAt = lead.closed_at || lead.updated_at;
                        const cycle = (closedAt - lead.created_at) / (60 * 60 * 24);
                        if (cycle > 0) totalSalesCycleDays += cycle;
                    }
                }
                if (statsPorStatus[lead.status_id]) statsPorStatus[lead.status_id].count++;
            }
        });

        statusList.forEach(status => {
            const stat = statsPorStatus[status.id];
            if (wonStatusIds.includes(status.id)) wonCount += stat.count;
            else if (lostStatusIds.includes(status.id)) lostCount += stat.count;
            else stages.push({ id: status.id, name: status.name, count: stat.count, avgDays: stat.count > 0 ? (stat.totalDays / stat.count).toFixed(1) : 0 });
        });

        pipelinesData.push({ id: pipeline.id, name: pipeline.name, totalLeadsEntrants: totalLeadsNestePipeline, stages, results: { won: wonCount, lost: lostCount } });
    });

    const avgSalesCycle = wonLeadsForCycle > 0 ? (totalSalesCycleDays / wonLeadsForCycle) : 0;

    // 7. Monthly stats array
    const monthlyArray = Object.keys(monthlyStats).sort().map(key => {
        const [year, month] = key.split('-');
        return { key, label: `${MESES_PT[parseInt(month) - 1]}/${year.slice(2)}`, labelFull: `${MESES_PT[parseInt(month) - 1]} ${year}`, ...monthlyStats[key] };
    });

    // 8. Loss reasons array
    const totalLost = Object.values(lossReasonCounts).reduce((a, b) => a + b, 0);
    const lossReasonsArray = Object.entries(lossReasonCounts)
        .map(([name, count]) => ({ name, count, percent: totalLost > 0 ? parseFloat(((count / totalLost) * 100).toFixed(2)) : 0 }))
        .sort((a, b) => b.count - a.count);

    // 9. Billing
    let diasRestantesPagamento = null;
    let statusFatura = 'Sem Registro';
    let dataFormatada = '--/--/----';
    if (cliente.vencimento) {
        const vencimentoDate = new Date(`${cliente.vencimento}T23:59:59`);
        const hojeDate = new Date();
        diasRestantesPagamento = Math.ceil((vencimentoDate - hojeDate) / (1000 * 60 * 60 * 24));
        dataFormatada = vencimentoDate.toLocaleDateString('pt-BR');
        if (diasRestantesPagamento < 0) statusFatura = `Vencido há ${Math.abs(diasRestantesPagamento)} dias`;
        else if (diasRestantesPagamento === 0) statusFatura = 'Vence hoje!';
        else statusFatura = `Em dia (vence em ${diasRestantesPagamento} dias)`;
    }

    return {
        id: cliente.id, name: cliente.nome,
        leadsLast30Days: allLeads.length, stagnantLeads: totalStagnantLeadsConta,
        dataDecayLeads: totalDataDecayConta, overdueTasks: tarefasAtrasadasCount,
        usersTotal: totalUsers, usersAdmins: adminUsers,
        wonRevenue: totalWonRevenue, pipelineRevenue,
        avgSalesCycle: parseFloat(avgSalesCycle.toFixed(1)),
        pipelines: pipelinesData,
        monthlyStats: monthlyArray,
        lossReasons: lossReasonsArray,
        billing: { date: dataFormatada, daysRemaining: diasRestantesPagamento, statusText: statusFatura }
    };
}

// ═══════════════════════════════════════════════════════════════
// ROTA PRINCIPAL
// ═══════════════════════════════════════════════════════════════

app.get('/api/dados-dashboard', async (req, res) => {
    const dataInicio = req.query.inicio;
    const dataFim = req.query.fim;
    const clienteIdFiltro = req.query.clienteId;

    let clientesParaBuscar = clientes;
    if (clienteIdFiltro && clienteIdFiltro !== 'todos') {
        const encontrado = clientes.find(c => c.id === parseInt(clienteIdFiltro));
        if (encontrado) clientesParaBuscar = [encontrado];
    }

    // Processa clientes SEQUENCIALMENTE (não em paralelo)
    // Isso evita rajadas de requisições quando temos múltiplos clientes
    let resultados = [];
    for (let cliente of clientesParaBuscar) {
        try {
            const dados = await buscarDadosCliente(cliente, dataInicio, dataFim);
            resultados.push(dados);
        } catch (erro) {
            console.error(`\n❌ ERRO NO CLIENTE: ${cliente.nome}`);
            console.error(erro.response ? `   Status: ${erro.response.status}` : erro.message);
        }
    }
    res.json(resultados);
});

// ═══════════════════════════════════════════════════════════════
// ROTA DE HEALTH CHECK
// ═══════════════════════════════════════════════════════════════

app.get('/api/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        rateLimiter: {
            activeQueues: Object.keys(rateLimiter.queues).length,
            effectiveLimit: `${rateLimiter.EFFECTIVE_LIMIT} req/s por conta`
        }
    });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ Servidor rodando na porta ${PORT} (com rate limiting Kommo)`));