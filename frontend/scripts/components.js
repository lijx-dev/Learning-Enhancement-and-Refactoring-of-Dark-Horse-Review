import { imagePath, images, money } from "./api.js";

Vue.component("app-image", {
  props: ["src", "alt", "eager"],
  data() {
    return { failed: false };
  },
  watch: {
    src() {
      this.failed = false;
    },
  },
  computed: {
    source() {
      return this.failed
        ? "/assets/image-placeholder.svg"
        : imagePath(this.src);
    },
  },
  template:
    '<img :src="source" :alt="alt || \'\'" :loading="eager ? \'eager\' : \'lazy\'" decoding="async" referrerpolicy="no-referrer" @error="failed = true">',
});

Vue.component("blog-card", {
  props: ["blog"],
  methods: { images },
  template: `
    <article class="blog-card">
      <a :href="'/blog-detail.html?id=' + blog.id" class="blog-cover" :aria-label="blog.title">
        <app-image :src="images(blog.images)[0]" :alt="blog.title"></app-image>
      </a>
      <div class="blog-card-body">
        <a :href="'/blog-detail.html?id=' + blog.id" class="blog-card-title" :title="blog.title">{{ blog.title }}</a>
        <div class="blog-card-footer">
          <a :href="'/other-info.html?id=' + blog.userId" class="author">
            <app-image :src="blog.icon || '/imgs/icons/default-icon.png'" alt=""></app-image>
            <span>{{ blog.name || '探店记录员' }}</span>
          </a>
          <button class="like-button" :class="{ liked: blog.isLike }" :disabled="blog.busy" :aria-pressed="!!blog.isLike" :aria-label="(blog.isLike ? '取消点赞：' : '点赞：') + blog.title" @click="$emit('like', blog)">
            <i class="el-icon-star-off" aria-hidden="true"></i><span>{{ blog.liked || 0 }}</span>
          </button>
        </div>
      </div>
    </article>`,
});

Vue.component("rating-stars", {
  props: ["score"],
  computed: {
    fill() {
      return Math.max(0, Math.min(100, Number(this.score || 0) * 2)) + "%";
    },
  },
  template:
    '<span class="rating-stars" aria-hidden="true">★★★★★<span :style="{ width: fill }">★★★★★</span></span>',
});

Vue.component("shop-card", {
  props: ["shop", "typeName"],
  methods: { images },
  template: `
    <a class="shop-card" :href="'/shop-detail.html?id=' + shop.id">
      <div class="shop-cover"><app-image :src="images(shop.images)[0]" :alt="shop.name"></app-image><span v-if="typeName" class="image-tag">{{ typeName }}</span></div>
      <div class="shop-card-body"><h3>{{ shop.name }}</h3>
        <div class="shop-score"><rating-stars :score="shop.score"></rating-stars><strong>{{ shop.score == null ? '暂无评分' : (shop.score / 10).toFixed(1) }}</strong><span class="muted">{{ shop.comments || 0 }} 条评价</span></div>
        <p class="muted shop-address"><i class="el-icon-location-outline" aria-hidden="true"></i>{{ shop.area || shop.address }}</p>
        <div class="shop-card-bottom"><span><strong>¥{{ shop.avgPrice == null ? '—' : shop.avgPrice }}</strong> / 人</span><span class="muted">查看店铺 <i class="el-icon-right" aria-hidden="true"></i></span></div>
      </div>
    </a>`,
});

Vue.component("voucher-card", {
  props: ["voucher", "shopName", "busy"],
  methods: { money },
  computed: {
    available() {
      const v = this.voucher;
      if (v.orderable === false || (v.status != null && v.status !== 1)) {
        return false;
      }
      return (
        !v.type ||
        (Number(v.stock) > 0 &&
          Date.now() >= Date.parse(v.beginTime) &&
          Date.now() <= Date.parse(v.endTime))
      );
    },
    label() {
      if (this.busy) {
        return "正在提交…";
      }
      if (!this.available) {
        return "暂不可领取";
      }
      return this.voucher.type
        ? "限时抢购"
        : Number(this.voucher.payValue) === 0
          ? "立即领取"
          : "购买优惠券";
    },
  },
  template: `
    <article class="voucher-card">
      <div class="ticket-mark"><i class="el-icon-tickets" aria-hidden="true"></i></div>
      <div class="voucher-main"><a v-if="shopName" :href="'/shop-detail.html?id=' + voucher.shopId" class="eyebrow">{{ shopName }}</a><h3>{{ voucher.title }}</h3><p>{{ voucher.subTitle || '使用规则详见店铺说明' }}</p><div class="voucher-price">¥<strong>{{ money(voucher.payValue) }}</strong><span>价值 ¥{{ money(voucher.actualValue) }}</span><span v-if="voucher.type" class="tag">限时券 · 剩余 {{ voucher.stock || 0 }} 张</span></div></div>
      <button class="button primary" :disabled="!available || busy" @click="$emit('claim', voucher)">{{ label }}</button>
    </article>`,
});
