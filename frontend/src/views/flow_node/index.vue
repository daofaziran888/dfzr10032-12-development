<template>
  <section class="page" data-module="flow_node">
    <header class="page-head">
      <div>
        <h2>数据流转执行节点</h2>
        <p class="page-desc">
          用采集调度批次清单和其中的依赖结果生成可执行节点；节点始终关联前置采集批次，
          依赖结果未确认的批次不会生成节点，同一批次的同一管道也只生成一次。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openGenerate">按批次生成节点</button>
        <button class="btn" type="button" @click="exportRows">导出执行节点</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>节点/批次/任务编号</span>
        <input v-model="keyword" placeholder="按节点编号、批次编号或任务编号检索" />
      </label>
      <label class="filter-item">
        <span>节点状态</span>
        <select v-model="statusFilter">
          <option value="">全部</option>
          <option v-for="status in statuses" :key="status" :value="status">{{ status }}</option>
        </select>
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th>节点编号</th>
          <th>管道编号</th>
          <th>批次编号</th>
          <th>任务编号</th>
          <th>调度时段</th>
          <th>前置批次</th>
          <th>依赖结果摘要</th>
          <th>节点状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td>{{ row['节点编号'] }}</td>
          <td>{{ row['管道编号'] }}</td>
          <td>{{ row['批次编号'] }}</td>
          <td>{{ row['任务编号'] }}</td>
          <td>{{ row['调度时段'] }}</td>
          <td>{{ row['前置批次'] }}</td>
          <td>{{ row['依赖结果摘要'] || '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in availableActions(row)"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <span v-if="!availableActions(row).length">—</span>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td colspan="9" class="empty-state">暂无执行节点，可在采集批次确认依赖结果后生成</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 个执行节点</span>
      <span v-if="message" class="ok-text">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <Teleport to="body">
      <div v-if="showGenerateModal" class="modal-overlay" @click.self="showGenerateModal = false">
        <div class="modal-card">
          <header class="modal-head">
            <h3>按批次生成执行节点</h3>
            <button class="btn ghost" type="button" @click="showGenerateModal = false">&times;</button>
          </header>
          <form class="modal-body" @submit.prevent="submitGenerate">
            <label class="modal-field">
              <span>批次编号</span>
              <input v-model="generateFields['批次编号']" placeholder="BATCH-0003" />
            </label>
            <label class="modal-field">
              <span>管道编号（留空使用已启用管道）</span>
              <input v-model="generateFields['管道编号']" placeholder="FLOW-0003" />
            </label>
            <p class="hint-text">
              批次编号留空时，也可以只填调度时段，一次为该时段内依赖已确认的批次生成节点。
            </p>
            <label class="modal-field">
              <span>调度时段（可选）</span>
              <input v-model="generateFields['调度时段']" placeholder="2026-11-20 08:00" />
            </label>
            <footer class="modal-foot">
              <button class="btn" type="button" @click="showGenerateModal = false">取消</button>
              <button class="btn primary" type="submit">生成节点</button>
            </footer>
          </form>
        </div>
      </div>
    </Teleport>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, unknown>
type ActionResult = { ok: boolean; message: string; entry: Row | null }

const ENDPOINT = '/api/flow_node'
const statuses = ['待编排', '可执行', '执行中', '已完成', '已阻断']

const rows = ref<Row[]>([])
const total = ref(0)
const keyword = ref('')
const statusFilter = ref('')
const message = ref('')
const errorMessage = ref('')
const showGenerateModal = ref(false)
const generateFields = ref<Record<string, string>>({})
const stats = ref([
  { label: '可执行节点', value: 0 },
  { label: '执行中节点', value: 0 },
  { label: '已完成节点', value: 0 },
])

function updateStats() {
  const data = rows.value
  stats.value = [
    { label: '可执行节点', value: data.filter((row) => row.status === '可执行').length },
    { label: '执行中节点', value: data.filter((row) => row.status === '执行中').length },
    { label: '已完成节点', value: data.filter((row) => row.status === '已完成').length },
  ]
}

function availableActions(row: Row): string[] {
  if (row.status === '可执行') return ['开始流转']
  if (row.status === '执行中') return ['完成流转']
  return []
}

function resetFilters() {
  keyword.value = ''
  statusFilter.value = ''
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openGenerate() {
  errorMessage.value = ''
  generateFields.value = {}
  showGenerateModal.value = true
}

async function submitGenerate() {
  errorMessage.value = ''
  message.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ values: generateFields.value }),
    })
    const data = (await response.json()) as ActionResult
    if (!data.ok) {
      throw new Error(data.message || '生成节点失败')
    }
    showGenerateModal.value = false
    message.value = data.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成节点失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  message.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const data = (await response.json()) as ActionResult
    if (!data.ok) {
      throw new Error(data.message || '执行节点动作未生效')
    }
    message.value = data.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '执行节点操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const params = new URLSearchParams()
  if (keyword.value.trim()) params.set('keyword', keyword.value.trim())
  if (statusFilter.value) params.set('status', statusFilter.value)
  try {
    const response = await request(`${ENDPOINT}?${params.toString()}`)
    if (!response.ok) {
      throw new Error('执行节点列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    updateStats()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '执行节点列表读取失败'
  }
}

onMounted(reload)
</script>
