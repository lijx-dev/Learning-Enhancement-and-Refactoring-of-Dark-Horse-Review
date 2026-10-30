import { money } from "./api.js";
import {
  cacheSummary,
  readSource,
  receiptsComplete,
  receiptCount,
  sameOrder,
  orderStatus,
} from "./experience-helpers.js";

Vue.component("experience-page", {
  props: { api: { type: Function, required: true }, user: Object },
  data() {
    return {
      capabilities: null,
      shops: [],
      shopId: "",
      samples: [],
      cacheBusy: false,
      kafkaBusy: false,
      eventIds: [],
      receipts: [],
      kafkaMessage: "",
      heatBefore: null,
      heatAfter: null,
      vouchers: [],
      voucherId: "",
      voucherLoading: true,
      voucherFailures: 0,
      orderBusy: false,
      confirmOrder: false,
      orderId: "",
      lastVoucher: null,
      requestState: "",
      order: null,
      progress: [],
      duplicate: null,
      orderMessage: "",
      evidence: null,
      error: "",
      loading: true,
    };
  },
  computed: {
    working() {
      return this.cacheBusy || this.kafkaBusy || this.orderBusy;
    },
    cacheResult() {
      return cacheSummary(this.samples);
    },
    received() {
      return receiptCount(this.eventIds, this.receipts, this.shopId);
    },
    selectedVoucher() {
      return this.vouchers.find(
        (voucher) => String(voucher.id) === String(this.voucherId),
      );
    },
    loginUrl() {
      return "/login.html?return=" + encodeURIComponent("/experience.html");
    },
  },
  created() {
    this.disposed = false;
    this.waitTimer = null;
    this.waitResolve = null;
    this.load();
  },
  beforeDestroy() {
    this.disposed = true;
    clearTimeout(this.waitTimer);
    if (this.waitResolve) this.waitResolve();
  },
  methods: {
    money,
    readSource,
    orderStatus,
    layerState(read, key) {
      const events = (read.events || []).filter((event) => event.layer === key);
      if (!events.length) return "未访问";
      if (events.some((event) => event.result === "load")) return "回源";
      return events.some((event) => event.result === "hit") ? "命中" : "未命中";
    },
    layerClass(read, key) {
      const state = this.layerState(read, key);
      return state === "命中" ? "hit" : state === "回源" ? "source" : "idle";
    },
    capabilityText(enabled, observed, activeText) {
      if (!enabled) return "未启用";
      if (observed === null || observed === undefined) return "未采集";
      return observed ? activeText : "尚未就绪";
    },
    async load() {
      this.loading = true;
      this.error = "";
      this.voucherFailures = 0;
      try {
        const [capabilities, catalog] = await Promise.all([
          this.api("/experience/capabilities"),
          this.api("/shop/catalog", { params: { size: 20 } }),
        ]);
        if (this.disposed) return;
        this.capabilities = capabilities.data;
        this.shops = catalog.data || [];
        this.shopId = this.shops.length ? String(this.shops[0].id) : "";
        const vouchers = [];
        for (let index = 0; index < this.shops.length; index += 4) {
          if (this.disposed) return;
          const rows = await Promise.allSettled(
            this.shops.slice(index, index + 4).map(async (shop) => {
              const result = await this.api("/voucher/list/" + shop.id);
              return (result.data || [])
                .filter((voucher) => voucher.orderable === true)
                .map((voucher) => ({ ...voucher, shopName: shop.name }));
            }),
          );
          rows.forEach((row) => {
            if (row.status === "fulfilled") vouchers.push(...row.value);
            else this.voucherFailures++;
          });
        }
        this.vouchers = vouchers.sort(
          (a, b) => Number(a.payValue) - Number(b.payValue),
        );
        this.voucherId = this.vouchers.length
          ? String(this.vouchers[0].id)
          : "";
      } catch (error) {
        if (!this.disposed) this.error = error.message;
      } finally {
        this.loading = false;
        this.voucherLoading = false;
      }
      try {
        const response = await fetch("/assets/experience-evidence.json");
        if (!response.ok) throw new Error("验证报告暂不可用");
        const evidence = await response.json();
        if (!this.disposed) this.evidence = evidence;
      } catch {
        /* Live observations remain usable without the historical report. */
      }
    },
    async runCache() {
      if (this.working || !this.shopId) return;
      this.cacheBusy = true;
      this.samples = [];
      this.error = "";
      const id = this.shopId;
      try {
        for (let index = 0; index < 3; index++) {
          const result = await this.api("/experience/cache", {
            params: { shopId: id },
          });
          if (this.disposed) return;
          this.samples.push(result.data);
        }
      } catch (error) {
        if (!this.disposed) this.error = error.message;
      } finally {
        this.cacheBusy = false;
      }
    },
    wait() {
      return new Promise((resolve) => {
        this.waitResolve = resolve;
        this.waitTimer = setTimeout(() => {
          this.waitResolve = null;
          resolve();
        }, 1500);
      });
    },
    async heat() {
      const result = await this.api("/shop/hot-ranking", {
        params: { windowMinutes: 5, limit: 100 },
      });
      return (
        (result.data || []).find(
          (row) => String(row.shop_id) === String(this.shopId),
        ) || { pv: 0, logged_uv: 0, heat: 0 }
      );
    },
    async runKafka() {
      if (this.working || !this.shopId) return;
      this.kafkaBusy = true;
      this.error = "";
      this.receipts = [];
      this.eventIds = [];
      this.heatAfter = null;
      this.kafkaMessage = "正在提交 3 次真实浏览…";
      try {
        this.heatBefore = await this.heat();
        const result = await this.api("/experience/visits", {
          method: "POST",
          params: { shopId: this.shopId },
        });
        this.eventIds = result.data.eventIds;
        for (let attempt = 0; attempt < 12 && !this.disposed; attempt++) {
          const response = await this.api("/experience/visits", {
            params: { eventIds: this.eventIds.join(",") },
          });
          this.receipts = response.data || [];
          if (receiptsComplete(this.eventIds, this.receipts, this.shopId)) {
            this.kafkaMessage = "本次 3 个事件已全部消费并持久化。";
            this.heatAfter = await this.heat();
            return;
          }
          this.kafkaMessage = "已提交，正在等待 Kafka 消费回执…";
          await this.wait();
        }
        if (!this.disposed)
          this.kafkaMessage =
            "仍有事件未确认。浏览采集允许丢失，请稍后查询回执；页面不会自动重复提交。";
      } catch (error) {
        if (!this.disposed) {
          this.error = error.message;
          this.kafkaMessage = "本次结果尚未确认。";
        }
      } finally {
        this.kafkaBusy = false;
      }
    },
    async refreshReceipts() {
      if (this.working || !this.eventIds.length) return;
      this.kafkaBusy = true;
      try {
        this.receipts =
          (
            await this.api("/experience/visits", {
              params: { eventIds: this.eventIds.join(",") },
            })
          ).data || [];
        this.heatAfter = await this.heat();
        this.kafkaMessage = receiptsComplete(
          this.eventIds,
          this.receipts,
          this.shopId,
        )
          ? "本次 3 个事件已全部消费并持久化。"
          : "仍有事件待确认，可稍后再查询。";
      } catch (error) {
        this.error = error.message;
      } finally {
        this.kafkaBusy = false;
      }
    },
    async submitOrder(repeat = false) {
      if (!this.user || this.working) return;
      const voucher = repeat ? this.lastVoucher : this.selectedVoucher;
      if (!voucher) return;
      this.confirmOrder = false;
      this.orderBusy = true;
      this.error = "";
      try {
        const result = await this.api(
          "/voucher-order/" +
            (voucher.type ? "seckill/" : "ordinary/") +
            voucher.id,
          { method: "POST" },
        );
        const id = String(result.data);
        if (repeat)
          this.duplicate = {
            previous: this.orderId,
            returned: id,
            same: sameOrder(this.orderId, id),
          };
        else {
          this.orderId = id;
          this.lastVoucher = { ...voucher };
          this.order = null;
          this.progress = [];
          this.duplicate = null;
        }
        await this.pollOrder();
      } catch (error) {
        this.error = error.message;
        this.orderMessage =
          !error.status || error.status >= 500
            ? "提交结果暂未确认，请先去我的订单查看。页面不会自动重复提交。"
            : "本次请求被拒绝，请查看上方提示。";
      } finally {
        this.orderBusy = false;
      }
    },
    async pollOrder() {
      for (let attempt = 0; attempt < 12 && !this.disposed; attempt++) {
        if (this.lastVoucher.type) {
          const status = (
            await this.api("/voucher-order/requests/" + this.orderId)
          ).data;
          this.requestState = status.state;
          if (["FAILED", "REVIEW"].includes(status.state)) {
            this.orderMessage =
              status.state === "REVIEW"
                ? "请求已转人工核对，资格仍被占用。"
                : "请求处理失败，请查看订单状态。";
            return;
          }
          if (status.state !== "SUCCESS") {
            this.orderMessage = "资格已入队，正在等待异步履约…";
            await this.wait();
            continue;
          }
        }
        this.order = (await this.api("/voucher-order/" + this.orderId)).data;
        this.progress =
          (await this.api("/experience/orders/" + this.orderId)).data || [];
        if (
          this.progress.some(
            (event) =>
              event.kind === "ORDER_CREATED" && Number(event.inbox_count) > 0,
          )
        ) {
          this.orderMessage =
            "订单已落库，创建事件已有 Inbox 消费回执。订单支付状态见下方。";
          return;
        }
        this.orderMessage = this.capabilities.mqEnabled
          ? "订单已落库，正在等待事件消费回执…"
          : "订单已落库；当前未启用 RocketMQ。";
        if (!this.capabilities.mqEnabled) return;
        await this.wait();
      }
      if (!this.disposed)
        this.orderMessage = "订单已提交，事件回执尚未齐全，可以稍后刷新。";
    },
    async refreshOrder() {
      if (this.working || !this.orderId || !this.user || !this.lastVoucher)
        return;
      this.orderBusy = true;
      try {
        await this.pollOrder();
      } catch (error) {
        this.error = error.message;
      } finally {
        this.orderBusy = false;
      }
    },
    async cancelOrder() {
      if (
        this.working ||
        !this.order ||
        !this.lastVoucher ||
        this.lastVoucher.type ||
        Number(this.order.status) !== 1
      )
        return;
      this.orderBusy = true;
      try {
        await this.api("/voucher-order/" + this.orderId + "/cancel", {
          method: "POST",
        });
        this.order = (await this.api("/voucher-order/" + this.orderId)).data;
        this.progress =
          (await this.api("/experience/orders/" + this.orderId)).data || [];
        this.orderMessage = "这笔普通待付款订单已取消。";
        this.lastVoucher = null;
      } catch (error) {
        this.error = error.message;
      } finally {
        this.orderBusy = false;
      }
    },
  },
  template: `
    <div class="experience-page">
      <section class="experience-hero">
        <div><span class="eyebrow">体验验证 · 从操作到结果</span><h1>亮点，不止写在简历上。</h1><p>选一家店，点一次按钮，看看真实请求背后发生了什么。</p><div class="experience-badges"><span>前两项无需登录</span><span>真实接口与回执</span><span>约 1 分钟开始体验</span></div></div>
        <div class="experience-hero-mark"><i class="el-icon-data-line" aria-hidden="true"></i><span>TRY · OBSERVE · UNDERSTAND</span></div>
      </section>
      <div v-if="error" class="experience-error" role="alert">{{ error }}<button class="text-button" @click="error = ''">关闭</button></div>
      <div v-if="loading" class="experience-loading" role="status">正在准备可体验的店铺与接口…</div>
      <div v-else-if="!capabilities || !capabilities.enabled" class="panel experience-disabled"><h2>体验观察尚未启用</h2><p>部署者启用 HMDP_EXPERIENCE_ENABLED 后可使用此页面。正常业务页面仍可访问。</p><a class="button outline" href="/">先逛逛首页</a></div>
      <template v-else>
        <section class="experience-status" aria-label="运行状态"><div><span>缓存查询</span><strong>{{ capabilities.cacheEnabled ? '已开启' : '已关闭' }}</strong></div><div><span>Kafka 消费线程</span><strong>{{ capabilityText(capabilities.analyticsEnabled, capabilities.kafkaConsumerRunning, '运行中') }}</strong></div><div><span>RocketMQ 客户端</span><strong>{{ capabilityText(capabilities.mqEnabled, capabilities.mqClientStarted, '已启动') }}</strong></div><div><span>Canal 连接</span><strong>{{ capabilityText(capabilities.cdcEnabled, capabilities.cdcConnected, '已连接') }}</strong></div></section>
        <div class="experience-picker"><label for="experience-shop">先选一家店</label><select id="experience-shop" v-model="shopId" :disabled="working" @change="samples = []; eventIds = []; receipts = []; heatBefore = null; heatAfter = null; kafkaMessage = ''"><option v-for="shop in shops" :key="shop.id" :value="String(shop.id)">{{ shop.name }}</option></select><span>缓存与浏览事件使用这家店铺</span></div>
        <section class="panel experience-card" aria-labelledby="cache-experience-title">
          <div class="experience-card-head"><span class="experience-step">01</span><div><span class="eyebrow">多级缓存</span><h2 id="cache-experience-title">同一份数据，少走几步路。</h2><p>连续发出 3 个请求，每个请求读取店铺两次，观察数据来自哪一层。</p></div><button class="button primary" :disabled="working || !shopId" @click="runCache">{{ cacheBusy ? '正在采样…' : samples.length ? '再采样一次' : '连续读取 3 次' }}<i class="el-icon-right" aria-hidden="true"></i></button></div>
          <div class="experience-layer-guide"><span>请求内缓存</span><i class="el-icon-arrow-right" aria-hidden="true"></i><span>Caffeine</span><i class="el-icon-arrow-right" aria-hidden="true"></i><span>Memcache</span><i class="el-icon-arrow-right" aria-hidden="true"></i><span>Redis</span><i class="el-icon-arrow-right" aria-hidden="true"></i><span>MySQL</span></div>
          <div v-if="!samples.length" class="experience-placeholder"><i class="el-icon-view" aria-hidden="true"></i><p>点击上方按钮，这里会显示本次请求的真实命中路径。</p></div>
          <template v-else><div class="experience-summary"><div><strong>{{ cacheResult.requests }}</strong><span>实际请求</span></div><div><strong>{{ cacheResult.reads }}</strong><span>详情读取</span></div><div><strong>{{ cacheResult.scopeHits }}</strong><span>请求内命中</span></div><div><strong>{{ cacheResult.mysqlLoads }}</strong><span>MySQL 回源</span></div></div><div class="experience-table-wrap"><table class="experience-table"><thead><tr><th>本次读取</th><th>数据来源</th><th>请求内缓存</th><th>Caffeine</th><th>Memcache</th><th>Redis</th><th>MySQL</th><th>后端耗时</th></tr></thead><tbody v-for="(sample, index) in samples" :key="sample.requestId"><tr v-for="read in sample.reads" :key="read.sequence"><td>请求 {{ index + 1 }} · 第 {{ read.sequence }} 次</td><td class="source-name">{{ readSource(read) }}</td><td v-for="key in ['scope','jvm','memcache','redis','mysql']" :key="key"><span class="experience-cell" :class="layerClass(read, key)">{{ layerState(read, key) }}</span></td><td>{{ (read.durationMicros / 1000).toFixed(3) }} ms</td></tr></tbody></table></div></template>
          <p class="experience-note">第二次读取若命中请求内缓存，就无需再次访问下层。已有热缓存可能让首次读取直接命中 Caffeine；未访问的层不会标成成功。这里只采样当前请求，不清空缓存，也不是容量压测。</p>
        </section>
        <section class="panel experience-card" aria-labelledby="kafka-experience-title">
          <div class="experience-card-head"><span class="experience-step">02</span><div><span class="eyebrow">Kafka · 异步统计</span><h2 id="kafka-experience-title">你的一次浏览，会被看见。</h2><p>发出 3 个真实浏览事件，跟踪它们从提交到消费落库的过程。</p></div><button class="button primary" :disabled="working || !shopId || !capabilities.analyticsEnabled" @click="runKafka">{{ kafkaBusy ? '等待消费回执…' : '产生 3 次浏览' }}<i class="el-icon-right" aria-hidden="true"></i></button></div>
          <div v-if="!eventIds.length" class="experience-placeholder"><i class="el-icon-connection" aria-hidden="true"></i><p>事件回执会出现在这里。无需登录也能体验。</p></div>
          <template v-else><div class="experience-progress"><span>已提交 {{ eventIds.length }} 个事件</span><div role="progressbar" aria-label="已持久化的浏览事件" :aria-valuenow="received" :aria-valuemax="eventIds.length" aria-valuemin="0"><span :style="{ width: (received / eventIds.length * 100) + '%' }"></span></div><strong>{{ received }} / {{ eventIds.length }} 已落库</strong></div><p class="experience-result" role="status">{{ kafkaMessage }}</p><ul class="experience-receipts"><li v-for="id in eventIds" :key="id"><code>{{ id }}</code><span :class="receipts.some(row => row.eventId === id && row.state === 'PERSISTED' && String(row.shopId) === String(shopId)) ? 'confirmed' : 'pending'">{{ receipts.some(row => row.eventId === id && row.state === 'PERSISTED' && String(row.shopId) === String(shopId)) ? '已消费并持久化' : '等待回执' }}</span></li></ul><div v-if="heatBefore" class="experience-heat"><span>5 分钟热榜快照</span><span>浏览 PV：{{ heatBefore.pv }} → {{ heatAfter ? heatAfter.pv : '等待刷新' }}</span><span>登录 UV：{{ heatAfter ? heatAfter.logged_uv : heatBefore.logged_uv }}</span><button class="text-button" :disabled="working" @click="refreshReceipts">查询回执与热榜</button></div></template>
          <p class="experience-note">提交成功不等于消费完成。只有本次事件号出现在数据库中才显示完成；热榜约每 5 秒刷新，其他访客也会影响统计。匿名浏览增加 PV，同一登录用户重复浏览不会重复增加窗口 UV。此操作会记录真实浏览，浏览采集允许丢失。</p>
        </section>
        <section class="panel experience-card" aria-labelledby="order-experience-title">
          <div class="experience-card-head"><span class="experience-step">03</span><div><span class="eyebrow">订单幂等 · RocketMQ · 秒杀履约</span><h2 id="order-experience-title">点两次，也只得到一笔订单。</h2><p>使用测试账号创建订单，查看事件回执，再主动重复提交一次。</p></div><a v-if="!user" class="button outline" :href="loginUrl">登录后体验<i class="el-icon-right" aria-hidden="true"></i></a><span v-else class="tag">{{ user.nickName }} · 已登录</span></div>
          <div v-if="voucherLoading" class="inline-empty">正在寻找可用优惠券…</div><div v-else-if="voucherFailures && !vouchers.length" class="inline-empty">优惠券接口未能全部返回，当前无法确认是否有可用券，请刷新页面后重试。</div><div v-else-if="!vouchers.length" class="inline-empty">当前没有可下单的优惠券。部署者准备普通券或有效期内的秒杀券后即可体验。</div>
          <template v-else><p v-if="voucherFailures" class="experience-note">部分店铺的优惠券未能加载，下方仅展示已查询成功的可用券。</p><div class="experience-order-picker"><label for="experience-voucher">选择优惠券</label><select id="experience-voucher" v-model="voucherId" :disabled="working || !!orderId" @change="confirmOrder = false"><option v-for="voucher in vouchers" :key="voucher.id" :value="String(voucher.id)">{{ voucher.shopName }} · {{ voucher.title }} · ¥{{ money(voucher.payValue) }} · {{ voucher.type ? '秒杀券' : '普通券' }}</option></select><button v-if="user && !orderId" class="button outline" :disabled="working" @click="confirmOrder = true">准备提交</button></div>
          <div v-if="confirmOrder && selectedVoucher" class="experience-confirm"><h3>确认创建这笔真实订单？</h3><p>{{ selectedVoucher.title }} · ¥{{ money(selectedVoucher.payValue) }}。{{ selectedVoucher.type ? '秒杀成功会占用库存与领取资格，当前不支持取消秒杀订单。' : Number(selectedVoucher.payValue) === 0 ? '免费券会直接成为已领取状态，请使用测试账号。' : '仅创建待付款订单，不会扣款；测试后可以取消。' }}</p><div><button class="button soft" @click="confirmOrder = false">先不提交</button><button class="button primary" :disabled="working" @click="submitOrder(false)">确认提交</button></div></div>
          <template v-if="orderId"><div class="experience-order-result"><span>本次订单号</span><code>{{ orderId }}</code><strong>{{ order ? orderStatus(order) : requestState || '等待确认' }}</strong></div><p class="experience-result" role="status">{{ orderMessage }}</p><div class="experience-actions"><button class="button outline small" :disabled="working || !lastVoucher" @click="submitOrder(true)">{{ orderBusy ? '正在处理…' : '重复提交一次，验证幂等' }}</button><button class="button soft small" :disabled="working || !lastVoucher" @click="refreshOrder">刷新订单与回执</button><button v-if="order && Number(order.status) === 1 && lastVoucher && !lastVoucher.type" class="text-button" :disabled="working" @click="cancelOrder">取消本次普通待付款订单</button><a class="text-button" href="/orders.html">我的订单<i class="el-icon-right" aria-hidden="true"></i></a></div><div v-if="duplicate" class="experience-duplicate" :class="{ confirmed: duplicate.same }"><strong>{{ duplicate.same ? '本次重复提交返回了同一个订单号' : '两次返回的订单号不同，请检查结果' }}</strong><span>首次 {{ duplicate.previous }} → 再次 {{ duplicate.returned }}</span></div><div v-if="progress.length" class="experience-table-wrap"><table class="experience-table"><thead><tr><th>事件号</th><th>事件类型</th><th>Outbox 状态</th><th>Inbox 回执数</th><th>消费时间</th></tr></thead><tbody><tr v-for="event in progress" :key="event.event_id"><td><code>{{ event.event_id }}</code></td><td>{{ event.kind }}</td><td>{{ event.state }}</td><td>{{ event.inbox_count }}</td><td>{{ event.consumed_at || '尚未确认' }}</td></tr></tbody></table></div></template></template>
          <p class="experience-note">普通券用于体验订单防重与业务事件消费。选择有效秒杀券时，页面还会查询 PROCESSING / SUCCESS / REVIEW / FAILED 履约状态。SUCCESS 代表订单落库，不代表已付款。订单和事件只允许本人查询。</p>
        </section>
      </template>
      <section v-if="evidence" class="experience-evidence"><div><span class="eyebrow">更多验证 · 历史证据</span><h2>页面之外，也有可复现的测试。</h2><p>故障接管、CDC 变更、补偿和并发竞争需要独立测试环境。这份报告记录的是最近完成的隔离测试，与你本次操作分开显示。</p><span>{{ new Date(evidence.generatedAt).toLocaleString('zh-CN') }} · {{ evidence.tests }} 项通过 · {{ evidence.failures }} 失败</span></div><details><summary>查看覆盖范围与边界<i class="el-icon-arrow-down" aria-hidden="true"></i></summary><ul><li v-for="feature in evidence.features" :key="feature.feature"><strong>{{ feature.feature }}</strong><span>{{ feature.boundary }}</span></li></ul><p><span v-if="evidence.checkstylePassed">该轮功能测试与 Checkstyle 检查均通过。</span><span v-else>该轮功能测试临时跳过了 Checkstyle。</span>历史报告不表示当前访客运行过全部测试。真实支付、退款及短信发送不在此页面验证范围。</p><a class="text-button" href="/assets/experience-evidence.json" target="_blank" rel="noopener">查看原始报告</a></details></section>
    </div>`,
});
