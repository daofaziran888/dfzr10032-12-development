<template>
  <section class="page" data-module="schedule_batch">
    <header class="page-head">
      <div>
        <h2>采集调度批次</h2>
        <p class="page-desc">
          按调度时段生成批次清单，记录等待中的依赖与已确认的结果；同一任务在同一时段只派发一次，
          后续批次只消费前置批次已确认的数据。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openDispatch">按调度时段派发</button>
        <button class="btn" type="button" @click="exportRows">导出批次清单</button>
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
        <span>批次/任务/计划编号</span>
        <input v-model="keyword" placeholder="按批次编号、任务编号或计划编号检索" />
      </label>
      <label class="filter-item">
        <span>批次状态</span>
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
          <th v-for="column in columns" :key="column">{{ headerLabels[column] ?? column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column" :class="{ 'warn-cell': column === '依赖状态' && row['依赖状态'] !== '无依赖' && row['依赖状态'] !== '已确认' }">
            {{ row[column] ?? '—' }}
          </td>
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
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无采集批次，可按调度时段派发一批</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集批次</span>
      <span v-if="message" class="ok-text">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <Teleport to="body">
      <div v-if="showDispatchModal" class="modal-overlay" @click.self="showDispatchModal = false">
        <div class="modal-card">
          <header class="modal-head">
            <h3>按调度时段派发批次</h3>
            <button class="btn ghost" type="button" @click="showDispatchModal = false">&times;</button>
          </header>
          <form class="modal-body" @submit.prevent="submitDispatch">
            <label class="modal-field">
              <span>调度时段</span>
              <input v-model="dispatchFields['调度时段']" placeholder="2026-11-20 08:00" required />
            </label>
            <label class="modal-field">
              <span>计划编号（留空则按节拍派发全部到点计划）</span>
              <input v-model="dispatchFields['计划编号']" placeholder="SCHE-0002" />
            </label>
            <p class="hint-text">
              同一任务在同一调度时段已有批次时不会重复派发；前置任务未确认时，新批次接续该前置任务并给出预计可用时间。
            </p>
            <footer class="modal-foot">
              <button class="btn" type="button" @click="showDispatchModal = false">取消</button>
              <button class="btn primary" type="submit">提交派发</button>
            </footer>
          </form>
        </div>
      </div>
    </Teleport>

    <Teleport to="body">
      <div v-if="showConfirmModal" class="modal-overlay" @click.self="showConfirmModal = false">
        <div class="modal-card">
          <header class="modal-head">
            <h3>确认批次结果</h3>
            <button class="btn ghost" type="button" @click="showConfirmModal = false">&times;</button>
          </header>
          <form class="modal-body" @submit.prevent="submitConfirm">
            <p class="hint-text">批次 {{ confirmTarget?.批次编号 }}（{{ confirmTarget?.任务编号 }}）</p>
            <label class="modal-field">
              <span>确认数据</span>
              <input v-model="confirmFields['确认数据']" placeholder="库存快照 2026-11-20，12480 条" required />
            </label>
            <label class="modal-field">
              <span>结果标识（留空自动生成）</span>
              <input v-model="confirmFields['结果标识']" placeholder="RESULT-INV-2026-11-20" />
            </label>
            <footer class="modal-foot">
              <button class="btn" type="button" @click="showConfirmModal = false">取消</button>
              <button class="btn primary" type="submit">提交确认</button>
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

const ENDPOINT = '/api/schedule_batch'
const columns = ['批次编号', '计划编号', '任务编号', '调度周期', '调度时段', '依赖任务', '依赖状态', '预计可用时间', '确认数据', 'status']
const statuses = ['等待依赖', '待执行', '执行中', '已确认', '已挂起']
const headerLabels: Record<string, string> = { status: '批次状态' }

const rows = ref<Row[]>([])
const total = ref(0)
const keyword = ref('')
const statusFilter = ref('')
const message = ref('')
const errorMessage = ref('')
const showDispatchModal = ref(false)
const dispatchFields = ref<Record<string, string>>({})
const showConfirmModal = ref(false)
const confirmTarget = ref<Row | null>(null)
const confirmFields = ref<Record<string, string>>({})
const stats = ref([
  { label: '等待依赖', value: 0 },
  { label: '待执行/执行中', value: 0 },
  { label: '已确认批次', value: 0 },
])

function updateStats() {
  const data = rows.value
  stats.value = [
    { label: '等待依赖', value: data.filter((row) => row.status === '等待依赖').length },
    {
      label: '待执行/执行中',
      value: data.filter((row) => row.status === '待执行' || row.status === '执行中').length,
    },
    { label: '已确认批次', value: data.filter((row) => row.status === '已确认').length },
  ]
}

function availableActions(row: Row): string[] {
  if (row.status === '已确认') return []
  if (row.status === '等待依赖') {
    return row['依赖状态'] === '依赖缺失' ? ['重试依赖', '挂起批次'] : ['挂起批次']
  }
  if (row.status === '已挂起') return ['恢复批次']
  return ['开始执行', '确认结果', '挂起批次']
}

function resetFilters() {
  keyword.value = ''
  statusFilter.value = ''
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openDispatch() {
  errorMessage.value = ''
  dispatchFields.value = { 调度时段: '2026-11-20 08:00' }
  showDispatchModal.value = true
}

function openConfirm(row: Row) {
  confirmTarget.value = row
  confirmFields.value = { 确认数据: '', 结果标识: '' }
  showConfirmModal.value = true
}

async function submitDispatch() {
  errorMessage.value = ''
  message.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ values: dispatchFields.value }),
    })
    const data = (await response.json()) as ActionResult
    if (!data.ok) {
      throw new Error(data.message || '派发失败')
    }
    showDispatchModal.value = false
    message.value = data.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '派发失败'
  }
}

async function submitConfirm() {
  if (!confirmTarget.value) return
  await runAction('确认结果', confirmTarget.value, confirmFields.value)
  showConfirmModal.value = false
}

async function runAction(action: string, row: Row, extra: Record<string, string> = {}) {
  errorMessage.value = ''
  message.value = ''
  if (action === '确认结果' && !extra['确认数据']) {
    openConfirm(row)
    return
  }
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action, ...extra } }),
    })
    const data = (await response.json()) as ActionResult
    if (!data.ok) {
      throw new Error(data.message || '采集批次动作未生效')
    }
    message.value = data.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集批次操作失败'
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
      throw new Error('采集批次列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    updateStats()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集批次列表读取失败'
  }
}

onMounted(reload)
</script>
