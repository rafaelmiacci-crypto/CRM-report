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
const DIAS_VENDEDOR_INATIVO = 7;

// ═══════════════════════════════════════════════════════════════
// HEALTH SCORE — CONFIGURAÇÃO DOS CRITÉRIOS
// ═══════════════════════════════════════════════════════════════
// Pesos dos 3 pilares (ordem de prioridade do negócio):
const PESO_CONVERSAO = 40;        // 1º — Mais importante
const PESO_VENDEDORES_ATIVOS = 35; // 2º — Atividade da operação
const PESO_LEADS_PARADOS = 25;     // 3º — Higiene do funil

// Thresholds de conversão (% de vendas / decididos)
const CONV_SAUDAVEL = 10;   // >= 10% → 100% dos pontos do pilar
const CONV_ATENCAO = 5;     // 5-10% → 50% dos pontos
                            // < 5% → 0 pontos

// Thresholds de leads parados em etapas ATIVAS (não tolerantes)
const PARADOS_OK = 20;      // <= 20% → 100% dos pontos
const PARADOS_RUIM = 40;    // 20-40% → 50% pontos
                            // > 40% → 0 pontos

// Classificação final do score (0-100)
const SCORE_SAUDAVEL = 70;
const SCORE_ATENCAO = 40;
// < 40 → Em Risco

// Regex de etapas TOLERANTES (não penaliza leads parados nelas)
const REGEX_ETAPAS_TOLERANTES = /follow.?up|n[ãa]o respondeu|nutri[çc][ãa]o|aguardando|stand.?by|reengajamento|reativa[çc][ãa]o/i;

// Mínimo de leads decididos pra confiar no pilar de conversão
const MIN_LEADS_DECIDIDOS = 5;

// ═══════════════════════════════════════════════════════════════
// CONFIGURAÇÃO DINÂMICA DE CLIENTES
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
// RATE LIMITER
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
// CACHE INTERNO
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
// 🎯 NOVA FUNÇÃO: CÁLCULO DO HEALTH SCORE POR FUNIL
// ═══════════════════════════════════════════════════════════════
/**
 * Calcula a saúde de UM funil com base em 3 pilares ponderados:
 *  1. Conversão (40pts) - vendas / decididos
 *  2. Vendedores ativos (35pts) - % com atividade nos últimos N dias
 *  3. Leads parados (25pts) - % parados em etapas ATIVAS (não-tolerantes)
 *
 * Retorna: { score, level, label, action, breakdown }
 */
function calcularHealthScore({
    won,
    lost,
    leadsAtivosNaoTolerantes,
    leadsParadosNaoTolerantes,
    closers,
    usuariosTotaisConta
}) {
    const breakdown = {
        conversao: { pontos: 0, max: PESO_CONVERSAO, detalhe: '' },
        vendedoresAtivos: { pontos: 0, max: PESO_VENDEDORES_ATIVOS, detalhe: '' },
        leadsParados: { pontos: 0, max: PESO_LEADS_PARADOS, detalhe: '' }
    };

    // ─── PILAR 1: TAXA DE CONVERSÃO (40 pts) ───
    const decididos = won + lost;
    if (decididos < MIN_LEADS_DECIDIDOS) {
        // Sem dados suficientes — dá benefício da dúvida (50% do pilar)
        breakdown.conversao.pontos = PESO_CONVERSAO * 0.5;
        breakdown.conversao.detalhe = `Poucos decididos (${decididos}) — neutro`;
    } else {
        const taxaConv = (won / decididos) * 100;
        if (taxaConv >= CONV_SAUDAVEL) {
            breakdown.conversao.pontos = PESO_CONVERSAO;
            breakdown.conversao.detalhe = `${taxaConv.toFixed(1)}% — saudável`;
        } else if (taxaConv >= CONV_ATENCAO) {
            breakdown.conversao.pontos = PESO_CONVERSAO * 0.5;
            breakdown.conversao.detalhe = `${taxaConv.toFixed(1)}% — atenção`;
        } else {
            breakdown.conversao.pontos = 0;
            breakdown.conversao.detalhe = `${taxaConv.toFixed(1)}% — baixa`;
        }
    }

    // ─── PILAR 2: VENDEDORES ATIVOS (35 pts) ───
    // Considera "ativo" quem moveu pelo menos 1 lead nos últimos N dias
    // (ganhou, perdeu ou atualizou um lead do funil)
    const totalClosers = closers.length;
    const closersAtivos = closers.filter(c => c.ativo === true).length;
    if (totalClosers === 0) {
        // Funil sem vendedor atribuído — sinal ruim
        breakdown.vendedoresAtivos.pontos = 0;
        breakdown.vendedoresAtivos.detalhe = 'Sem vendedores atribuídos';
    } else {
        const pctAtivos = (closersAtivos / totalClosers) * 100;
        if (pctAtivos >= 80) {
            breakdown.vendedoresAtivos.pontos = PESO_VENDEDORES_ATIVOS;
            breakdown.vendedoresAtivos.detalhe = `${closersAtivos}/${totalClosers} ativos`;
        } else if (pctAtivos >= 50) {
            breakdown.vendedoresAtivos.pontos = PESO_VENDEDORES_ATIVOS * 0.5;
            breakdown.vendedoresAtivos.detalhe = `${closersAtivos}/${totalClosers} ativos`;
        } else {
            breakdown.vendedoresAtivos.pontos = 0;
            breakdown.vendedoresAtivos.detalhe = `Apenas ${closersAtivos}/${totalClosers} ativos`;
        }
    }

    // ─── PILAR 3: LEADS PARADOS EM ETAPAS ATIVAS (25 pts) ───
    if (leadsAtivosNaoTolerantes === 0) {
        // Sem leads ativos pra avaliar — neutro
        breakdown.leadsParados.pontos = PESO_LEADS_PARADOS * 0.5;
        breakdown.leadsParados.detalhe = 'Sem leads ativos';
    } else {
        const pctParados = (leadsParadosNaoTolerantes / leadsAtivosNaoTolerantes) * 100;
        if (pctParados <= PARADOS_OK) {
            breakdown.leadsParados.pontos = PESO_LEADS_PARADOS;
            breakdown.leadsParados.detalhe = `${pctParados.toFixed(0)}% parados — ok`;
        } else if (pctParados <= PARADOS_RUIM) {
            breakdown.leadsParados.pontos = PESO_LEADS_PARADOS * 0.5;
            breakdown.leadsParados.detalhe = `${pctParados.toFixed(0)}% parados — atenção`;
        } else {
            breakdown.leadsParados.pontos = 0;
            breakdown.leadsParados.detalhe = `${pctParados.toFixed(0)}% parados — crítico`;
        }
    }

    const score = Math.round(
        breakdown.conversao.pontos +
        breakdown.vendedoresAtivos.pontos +
        breakdown.leadsParados.pontos
    );

    // ─── OVERRIDES DE RISCO CRÍTICO ───
    let level, label, action;
    const overrideCritico = (
        usuariosTotaisConta === 0 ||
        (totalClosers > 0 && closersAtivos === 0 && leadsAtivosNaoTolerantes > 0) ||
        (decididos >= 20 && won / decididos < 0.03)
    );

    if (overrideCritico) {
        level = 'danger';
        label = 'Em Risco';
        action = 'Ação Crítica Urgente';
    } else if (score >= SCORE_SAUDAVEL) {
        level = 'good';
        label = 'Saudável';
        action = 'Tudo OK';
    } else if (score >= SCORE_ATENCAO) {
        level = 'warning';
        label = 'Atenção';
        action = 'Revisar Gargalos';
    } else {
        level = 'danger';
        label = 'Em Risco';
        action = 'Limpar Pipeline Urgente';
    }

    return {
        score,
        level,
        label,
        action,
        breakdown,
        classClass: `status-${level}`
    };
}

// ═══════════════════════════════════════════════════════════════
// LÓGICA PRINCIPAL — BUSCAR DADOS DO CLIENTE
// ═══════════════════════════════════════════════════════════════

async function buscarDadosCliente(cliente, dataInicio, dataFim) {
    const hoje = Math.floor(Date.now() / 1000);
    const limiteVendedorInativo = hoje - (DIAS_VENDEDOR_INATIVO * 86400);
    const sub = cliente.subdominio;
    const token = cliente.token;

    // 1. Pipelines
    const ckP = `pipelines_${sub}`;
    let pipelinesDaConta = getCached(ckP);
    if (!pipelinesDaConta) {
        const r = await kommoGet(sub, token, '/api/v4/leads/pipelines');
        pipelinesDaConta = r.data._embedded.pipelines;
        setCache(ckP, pipelinesDaConta);
    }

    // 2. Loss Reasons
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

    // 3. Leads
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

    // 5. Usuários
    const ckU = `users_${sub}`;
    let usersData = getCached(ckU);
    if (!usersData) {
        let totalUsers = 0, adminUsers = 0, usersMap = {};
        try {
            const r = await kommoGet(sub, token, '/api/v4/users');
            const users = r.data?._embedded?.users || [];
            users.forEach(u => { usersMap[u.id] = u.name; });
            const active = users.filter(u => u.rights?.is_active !== false);
            totalUsers = active.length;
            adminUsers = active.filter(u => u.rights?.is_admin === true).length;
        } catch (err) { }
        usersData = { totalUsers, adminUsers, usersMap };
        setCache(ckU, usersData);
    }
    const { totalUsers, adminUsers, usersMap } = usersData;

    // ── 6. Processar ──
    let pipelinesData = [];
    let totalStagnantLeadsConta = 0, totalDataDecayConta = 0;
    let totalWonRevenue = 0, totalSalesCycleDays = 0, wonLeadsForCycle = 0, pipelineRevenue = 0;
    let monthlyStats = {}, lossReasonCounts = {};

    // Detecção de status GLOBAL
    let globalWonStatusIds = new Set();
    let globalLostStatusIds = new Set();

    pipelinesDaConta.forEach(p => {
        p._embedded.statuses.forEach(s => {
            const isWon = s.type == 1 || s.id === 142 || /ganho|sucesso|won|success/i.test(s.name);
            const isLost = s.type == 2 || s.id === 143 || /perdid|loss/i.test(s.name);
            if (isWon) globalWonStatusIds.add(s.id);
            else if (isLost) globalLostStatusIds.add(s.id);
        });
    });

    allLeads.forEach(lead => {
        const dCreated = new Date(lead.created_at * 1000);
        const mkCreated = `${dCreated.getFullYear()}-${String(dCreated.getMonth() + 1).padStart(2, '0')}`;
        if (!monthlyStats[mkCreated]) monthlyStats[mkCreated] = { leads: 0, won: 0, lost: 0 };
        monthlyStats[mkCreated].leads++;

        if (globalWonStatusIds.has(lead.status_id)) monthlyStats[mkCreated].won++;
        if (globalLostStatusIds.has(lead.status_id)) {
            monthlyStats[mkCreated].lost++;
            const rid = lead.loss_reason_id;
            if (rid && lossReasonsMap[rid]) lossReasonCounts[lossReasonsMap[rid]] = (lossReasonCounts[lossReasonsMap[rid]] || 0) + 1;
            else if (rid) lossReasonCounts['Motivo não identificado'] = (lossReasonCounts['Motivo não identificado'] || 0) + 1;
            else lossReasonCounts['Sem motivo registrado'] = (lossReasonCounts['Sem motivo registrado'] || 0) + 1;
        }
    });

    pipelinesDaConta.forEach(pipeline => {
        let wonCount = 0, lostCount = 0, stages = [], totalLeadsNestePipeline = 0;
        const statusList = pipeline._embedded.statuses;

        // ─── Classificação de status (won, lost, tolerante, ativo-normal) ───
        let wonStatusIds = [], lostStatusIds = [], tolerantStatusIds = [];
        const tolerantStatusNames = []; // Pra debug/transparência

        statusList.forEach(s => {
            const isWon = s.type == 1 || s.id === 142 || /ganho|sucesso|won|success/i.test(s.name);
            const isLost = s.type == 2 || s.id === 143 || /perdid|loss/i.test(s.name);

            if (isWon) {
                wonStatusIds.push(s.id);
            } else if (isLost) {
                lostStatusIds.push(s.id);
            } else {
                // Etapa ativa — checa se é tolerante (não penaliza leads parados aqui)
                if (REGEX_ETAPAS_TOLERANTES.test(s.name)) {
                    tolerantStatusIds.push(s.id);
                    tolerantStatusNames.push(s.name);
                }
            }
        });

        const closedStatuses = [...wonStatusIds, ...lostStatusIds];
        const statsPorStatus = {};
        statusList.forEach(s => { statsPorStatus[s.id] = { count: 0, totalDays: 0, name: s.name }; });

        let pipelineMonthly = {}, pipelineLossReasons = {};
        let pipelineWonRevenue = 0;
        let pipelineLostRevenue = 0;
        let pipelineSalesCycleDays = 0;
        let pipelineWonForCycle = 0;
        let pipelineClosers = {};

        // ─── Contadores específicos pro Health Score deste funil ───
        let leadsAtivosNaoTolerantes = 0;
        let leadsParadosNaoTolerantes = 0;
        let stagnantLeadsNoFunil = 0;
        let dataDecayNoFunil = 0;

        allLeads.forEach(lead => {
            if (lead.pipeline_id !== pipeline.id) return;
            totalLeadsNestePipeline++;

            const diasSemAtt = (hoje - lead.updated_at) / 86400;
            const price = Number(lead.price) || 0;
            const respId = lead.responsible_user_id;
            const isTolerante = tolerantStatusIds.includes(lead.status_id);
            const isClosed = closedStatuses.includes(lead.status_id);

            // Inicia rastreamento do vendedor
            if (respId && !pipelineClosers[respId]) {
                pipelineClosers[respId] = {
                    id: respId,
                    name: usersMap[respId] || `Usuário ID: ${respId}`,
                    leads: 0,
                    won: 0,
                    lost: 0,
                    stagnant: 0,
                    ultimaAtividade: 0,
                    ativo: false
                };
            }
            if (respId) {
                pipelineClosers[respId].leads++;
                if (lead.updated_at > pipelineClosers[respId].ultimaAtividade) {
                    pipelineClosers[respId].ultimaAtividade = lead.updated_at;
                }
            }

            const dCreated = new Date(lead.created_at * 1000);
            const mkCreated = `${dCreated.getFullYear()}-${String(dCreated.getMonth() + 1).padStart(2, '0')}`;
            if (!pipelineMonthly[mkCreated]) pipelineMonthly[mkCreated] = { leads: 0, won: 0, lost: 0 };
            pipelineMonthly[mkCreated].leads++;

            const closedTs = lead.closed_at || lead.updated_at;

            if (!isClosed) {
                pipelineRevenue += price;

                // Conta leads ativos NÃO-tolerantes pra base do health score
                if (!isTolerante) {
                    leadsAtivosNaoTolerantes++;
                    if (diasSemAtt >= DIAS_ESTAGNADO) {
                        leadsParadosNaoTolerantes++;
                        stagnantLeadsNoFunil++;
                        totalStagnantLeadsConta++;
                        if (respId) pipelineClosers[respId].stagnant++;
                    }
                    const semTarefa = !lead.closest_task_at || lead.closest_task_at < hoje;
                    if (diasSemAtt >= DIAS_DATA_DECAY && semTarefa) {
                        dataDecayNoFunil++;
                        totalDataDecayConta++;
                    }
                }
                if (statsPorStatus[lead.status_id]) statsPorStatus[lead.status_id].totalDays += diasSemAtt;
            } else {
                if (wonStatusIds.includes(lead.status_id)) {
                    totalWonRevenue += price;
                    pipelineWonRevenue += price;
                    wonLeadsForCycle++;
                    if (respId) pipelineClosers[respId].won++;
                    const cycle = (closedTs - lead.created_at) / 86400;
                    if (cycle > 0) {
                        totalSalesCycleDays += cycle;
                        pipelineSalesCycleDays += cycle;
                        pipelineWonForCycle++;
                    }
                    pipelineMonthly[mkCreated].won++;
                } else if (lostStatusIds.includes(lead.status_id)) {
                    pipelineLostRevenue += price;
                    pipelineMonthly[mkCreated].lost++;
                    if (respId) pipelineClosers[respId].lost++;
                    const rid = lead.loss_reason_id;
                    if (rid && lossReasonsMap[rid]) pipelineLossReasons[lossReasonsMap[rid]] = (pipelineLossReasons[lossReasonsMap[rid]] || 0) + 1;
                    else if (rid) pipelineLossReasons['Motivo não identificado'] = (pipelineLossReasons['Motivo não identificado'] || 0) + 1;
                    else pipelineLossReasons['Sem motivo registrado'] = (pipelineLossReasons['Sem motivo registrado'] || 0) + 1;
                }
            }
            if (statsPorStatus[lead.status_id]) statsPorStatus[lead.status_id].count++;
        });

        // Marca quais vendedores estão ATIVOS (mexeram em lead nos últimos 7d)
        Object.values(pipelineClosers).forEach(c => {
            c.ativo = c.ultimaAtividade >= limiteVendedorInativo;
        });

        statusList.forEach(status => {
            const stat = statsPorStatus[status.id];
            if (wonStatusIds.includes(status.id)) wonCount += stat.count;
            else if (lostStatusIds.includes(status.id)) lostCount += stat.count;
            else stages.push({
                id: status.id,
                name: status.name,
                count: stat.count,
                avgDays: stat.count > 0 ? (stat.totalDays / stat.count).toFixed(1) : 0,
                tolerante: tolerantStatusIds.includes(status.id)
            });
        });

        const pipelineMonthlyArray = Object.keys(pipelineMonthly).sort().map(key => {
            const [year, month] = key.split('-');
            return { key, label: `${MESES_PT[parseInt(month) - 1]}/${year.slice(2)}`, ...pipelineMonthly[key] };
        });

        const pipelineTotalLost = Object.values(pipelineLossReasons).reduce((a, b) => a + b, 0);
        const pipelineLossArray = Object.entries(pipelineLossReasons)
            .map(([name, count]) => ({ name, count, percent: pipelineTotalLost > 0 ? parseFloat(((count / pipelineTotalLost) * 100).toFixed(2)) : 0 }))
            .sort((a, b) => b.count - a.count);

        const pipelineAvgCycle = pipelineWonForCycle > 0
            ? parseFloat((pipelineSalesCycleDays / pipelineWonForCycle).toFixed(1))
            : 0;

        const closersArray = Object.values(pipelineClosers).sort((a, b) => b.won - a.won);

        // 🎯 CALCULA O HEALTH SCORE DESTE FUNIL (com base no período filtrado)
        const healthScore = calcularHealthScore({
            won: wonCount,
            lost: lostCount,
            leadsAtivosNaoTolerantes,
            leadsParadosNaoTolerantes,
            closers: closersArray,
            usuariosTotaisConta: totalUsers
        });

        pipelinesData.push({
            id: pipeline.id,
            name: pipeline.name,
            totalLeadsEntrants: totalLeadsNestePipeline,
            stages,
            results: { won: wonCount, lost: lostCount },
            monthlyStats: pipelineMonthlyArray,
            lossReasons: pipelineLossArray,
            wonRevenue: pipelineWonRevenue,
            lostRevenue: pipelineLostRevenue,
            avgSalesCycle: pipelineAvgCycle,
            closers: closersArray,
            // ─── Métricas específicas do funil pra UI ───
            stagnantLeads: stagnantLeadsNoFunil,
            dataDecayLeads: dataDecayNoFunil,
            leadsAtivosNaoTolerantes,
            leadsParadosNaoTolerantes,
            tolerantStages: tolerantStatusNames,
            // 🎯 HEALTH SCORE
            healthScore
        });
    });

    const avgSalesCycle = wonLeadsForCycle > 0 ? (totalSalesCycleDays / wonLeadsForCycle) : 0;

    const monthlyArray = Object.keys(monthlyStats).sort().map(key => {
        const [year, month] = key.split('-');
        return { key, label: `${MESES_PT[parseInt(month) - 1]}/${year.slice(2)}`, labelFull: `${MESES_PT[parseInt(month) - 1]} ${year}`, ...monthlyStats[key] };
    });

    const totalLost = Object.values(lossReasonCounts).reduce((a, b) => a + b, 0);
    const lossReasonsArray = Object.entries(lossReasonCounts)
        .map(([name, count]) => ({ name, count, percent: totalLost > 0 ? parseFloat(((count / totalLost) * 100).toFixed(2)) : 0 }))
        .sort((a, b) => b.count - a.count);

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
// SWR CACHE
// ═══════════════════════════════════════════════════════════════

const dashboardCache = {};
const SWR_TTL = 5 * 60 * 1000;

async function getDashboardDataSWR(cliente, dataInicio, dataFim) {
    const cacheKey = `dash_${cliente.id}_${dataInicio || 'all'}_${dataFim || 'all'}`;
    const cached = dashboardCache[cacheKey];
    const now = Date.now();

    const fetchAndUpdate = async () => {
        if (dashboardCache[cacheKey] && dashboardCache[cacheKey].isFetching) return;
        if (!dashboardCache[cacheKey]) dashboardCache[cacheKey] = { isFetching: true };
        else dashboardCache[cacheKey].isFetching = true;
        try {
            const freshData = await buscarDadosCliente(cliente, dataInicio, dataFim);
            dashboardCache[cacheKey] = { data: freshData, timestamp: Date.now(), isFetching: false };
        } catch (error) {
            console.error(`Erro no background fetch para ${cliente.nome}:`, error.message);
            if (dashboardCache[cacheKey]) dashboardCache[cacheKey].isFetching = false;
        }
    };

    if (!cached || !cached.data) {
        console.log(`[SWR] ❌ Primeiro acesso de ${cliente.nome}. Aguardando API...`);
        await fetchAndUpdate();
        return dashboardCache[cacheKey].data;
    }

    if (now - cached.timestamp > SWR_TTL) {
        console.log(`[SWR] 🔄 Dados vencidos para ${cliente.nome}. Entregando cache e atualizando no fundo...`);
        fetchAndUpdate();
        return cached.data;
    }

    console.log(`[SWR] ⚡ Dados frescos entregues instantaneamente para ${cliente.nome}.`);
    return cached.data;
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
        try {
            const dados = await getDashboardDataSWR(cliente, dataInicio, dataFim);
            if (dados) resultados.push(dados);
        }
        catch (e) { console.error(`❌ ${cliente.nome}: ${e.response?.status || e.message}`); }
    }
    res.json(resultados);
});

app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString(), clients: clientes.length,
        rateLimiter: { activeQueues: Object.keys(rateLimiter.queues).length, effectiveLimit: `${rateLimiter.EFFECTIVE_LIMIT} req/s` }
    });
});

// ═══════════════════════════════════════════════════════════════
// PRÉ-AQUECIMENTO
// ═══════════════════════════════════════════════════════════════

async function preWarmCache() {
    console.log("\n🔥 Iniciando o Pré-Aquecimento SWR...");
    for (let cliente of clientes) {
        await getDashboardDataSWR(cliente, '', '');
    }
    console.log("🚀 Todos os clientes em cache!\n");
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`✅ Porta ${PORT} | ${clientes.length} clientes | Rate limiting ativo`);
    preWarmCache();
});
