import { createApi, images, imagePath, money, safeReturn } from "./api.js";
import "./components.js";
import "./experience.js";

const query = new URLSearchParams(location.search);
const routes = {
  "/": "home",
  "/index.html": "home",
  "/shop-list.html": "shops",
  "/shop-detail.html": "shop",
  "/blog-detail.html": "blog",
  "/blog-edit.html": "publish",
  "/login.html": "login",
  "/login2.html": "login",
  "/info.html": "profile",
  "/other-info.html": "profile",
  "/orders.html": "orders",
  "/vouchers.html": "vouchers",
  "/experience.html": "experience",
};
const page = routes[location.pathname] || "not-found";
let app;
const api = createApi({
  tokenProvider: () => sessionStorage.getItem("token"),
  onUnauthorized: (quiet) => {
    sessionStorage.removeItem("token");
    if (app) {
      app.user = null;
    }
    if (!quiet) {
      location.assign(
        "/login.html?return=" +
          encodeURIComponent(location.pathname + location.search),
      );
    }
  },
});

app = new Vue({
  el: "#app",
  data: {
    page,
    user: null,
    userReady: false,
    types: [],
    loading: true,
    pageError: "",
    keyword: query.get("keyword") || "",
    toast: null,
    dialog: null,
    blogs: [],
    blogPage: 1,
    blogLoading: false,
    blogHasMore: true,
    feed: query.get("feed") || "hot",
    feedLastId: Date.now(),
    feedOffset: 0,
    homeShops: [],
    homeOffer: null,
    asideError: "",
    shops: [],
    catalogPage: 1,
    catalogTotal: 0,
    catalogLoading: false,
    catalogSequence: 0,
    catalogType: query.get("type") || "",
    catalogSort: query.get("sort") || "score",
    shop: null,
    vouchers: [],
    shopBlogs: [],
    blog: null,
    blogShop: null,
    following: false,
    followingBusy: false,
    galleryIndex: 0,
    claimBusy: "",
    orderError: "",
    offerShopsPage: 1,
    offerShopsTotal: 0,
    offers: [],
    offerLoading: false,
    offerHasMore: true,
    profileUser: null,
    signCount: 0,
    signBusy: false,
    signed: false,
    orders: [],
    orderPage: 1,
    orderTotal: 0,
    orderLoading: false,
    pendingRequest: null,
    requestMessage: "",
    phone: "",
    code: "",
    loginBusy: false,
    codeBusy: false,
    codeCountdown: 0,
    loginError: "",
    publishShops: [],
    shopQuery: "",
    publishShopLoading: false,
    uploadBusy: false,
    publishBusy: false,
    draft: {
      title: "",
      content: "",
      shopId: query.get("shop") || "",
      pictures: [],
    },
    draftSaved: false,
  },
  computed: {
    activeNav() {
      if (this.page === "experience") return "experience";
      if (this.page === "vouchers") {
        return "vouchers";
      }
      if (["shops", "shop"].includes(this.page)) {
        return "shops";
      }
      return "home";
    },
    currentType() {
      return this.types.find(
        (type) => String(type.id) === String(this.catalogType),
      );
    },
    isOwnProfile() {
      return (
        !query.get("id") ||
        (this.user && String(query.get("id")) === String(this.user.id))
      );
    },
    shopPictures() {
      return images(this.shop && this.shop.images);
    },
    blogPictures() {
      return images(this.blog && this.blog.images);
    },
    catalogHasMore() {
      return this.shops.length < this.catalogTotal;
    },
    profileName() {
      return this.profileUser ? this.profileUser.nickName : "探店记录员";
    },
  },
  created() {
    this.toastTimer = null;
    this.codeTimer = null;
    this.requestTimer = null;
    this.boot();
  },
  beforeDestroy() {
    clearTimeout(this.toastTimer);
    clearInterval(this.codeTimer);
    clearTimeout(this.requestTimer);
  },
  watch: {
    dialog(value) {
      this.$nextTick(() => {
        if (value) {
          this.dialogOrigin = document.activeElement;
          document.body.style.overflow = "hidden";
          const button =
            this.$refs.dialogPanel &&
            this.$refs.dialogPanel.querySelector("button");
          if (button) {
            button.focus();
          }
        } else {
          document.body.style.overflow = "";
          if (this.dialogOrigin) {
            this.dialogOrigin.focus();
          }
        }
      });
    },
  },
  methods: {
    experienceApi(path, options) {
      return api(path, options);
    },
    images,
    imagePath,
    money,
    typeName(id) {
      const type = this.types.find((item) => String(item.id) === String(id));
      return type ? type.name : "本地好店";
    },
    typeIcon(id) {
      return (
        {
          1: "knife-fork",
          2: "microphone",
          3: "female",
          10: "magic-stick",
          5: "coffee-cup",
          6: "cherry",
          7: "user",
          8: "goblet",
          9: "house",
          4: "basketball",
        }[id] || "discover"
      );
    },
    date(value) {
      if (!value) {
        return "—";
      }
      const time = new Date(value);
      return Number.isNaN(time.getTime())
        ? "—"
        : time.toLocaleDateString("zh-CN");
    },
    orderStatus(status) {
      return (
        {
          1: "待付款",
          2: "已领取",
          3: "已使用",
          4: "已取消",
          5: "退款中",
          6: "已退款",
        }[status] || "处理中"
      );
    },
    notify(message, kind = "success") {
      this.toast = { message, kind };
      clearTimeout(this.toastTimer);
      this.toastTimer = setTimeout(() => {
        this.toast = null;
      }, 4500);
    },
    showError(error) {
      if (error.status !== 401) {
        this.notify(error.message || "操作失败，请重试", "error");
      }
    },
    reload() {
      location.reload();
    },
    trapDialog(event) {
      const controls = this.$refs.dialogPanel.querySelectorAll(
        "button:not([disabled]), a[href], input, select, textarea",
      );
      const first = controls[0];
      const last = controls[controls.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      }
      if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    },
    requireUser() {
      if (this.user) {
        return true;
      }
      location.assign(
        "/login.html?return=" +
          encodeURIComponent(location.pathname + location.search),
      );
      return false;
    },
    async boot() {
      if (sessionStorage.getItem("token")) {
        try {
          this.user = (await api("/user/me", { quiet: true })).data;
        } catch (error) {
          if (error.status !== 401) {
            this.showError(error);
          }
        }
      }
      this.userReady = true;
      if (["orders", "publish", "blog", "profile"].includes(this.page)) {
        if (!this.requireUser()) {
          return;
        }
      }
      try {
        this.types = (await api("/shop-type/list")).data || [];
      } catch (error) {
        this.showError(error);
      }
      try {
        if (this.page === "home") {
          await this.loadHome();
        }
        if (this.page === "shops") {
          await this.loadCatalog(true);
        }
        if (this.page === "shop") {
          await this.loadShop();
        }
        if (this.page === "blog") {
          await this.loadBlog();
        }
        if (this.page === "vouchers") {
          await this.loadOffers(true);
        }
        if (this.page === "profile") {
          await this.loadProfile();
        }
        if (this.page === "orders") {
          await this.loadOrders();
          this.watchRequest();
        }
        if (this.page === "publish") {
          await this.loadPublishShops();
        }
      } catch (error) {
        if (error.status !== 401) {
          this.pageError = error.message;
        }
      } finally {
        this.loading = false;
      }
      document.title =
        (this.shop
          ? this.shop.name
          : this.blog
            ? this.blog.title
            : {
                shops: "发现好店",
                vouchers: "超值优惠",
                profile: this.profileName,
                orders: "我的订单",
                publish: "发布探店笔记",
                login: "欢迎回来",
                experience: "体验验证",
              }[this.page] || "发现身边的好去处") + " · 黑马点评";
    },
    search() {
      location.assign(
        "/shop-list.html?" +
          new URLSearchParams({ keyword: this.keyword.trim() }),
      );
    },
    async loadHome() {
      const results = await Promise.allSettled([
        this.loadBlogs(true),
        api("/shop/catalog", { params: { page: 1, size: 3, sort: "score" } }),
      ]);
      if (results[1].status === "fulfilled") {
        this.homeShops = results[1].value.data || [];
      } else {
        this.asideError = results[1].reason.message;
      }
      const ids = [
        ...new Set(
          this.blogs
            .map((blog) => blog.shopId)
            .concat(this.homeShops.map((shop) => shop.id)),
        ),
      ];
      if (!ids.length) {
        return;
      }
      const vouchers = await Promise.allSettled(
        ids.map((id) => api("/voucher/list/" + id)),
      );
      const offers = vouchers.flatMap((item) =>
        item.status === "fulfilled" ? item.value.data || [] : [],
      );
      this.homeOffer =
        offers.find((offer) => offer.orderable !== false && !offer.type) ||
        null;
    },
    async changeFeed(feed) {
      if (feed === this.feed) {
        return;
      }
      if (feed === "follow" && !this.requireUser()) {
        return;
      }
      this.feed = feed;
      this.pageError = "";
      await this.loadBlogs(true);
    },
    async loadBlogs(reset = false) {
      if (this.blogLoading) {
        return;
      }
      this.blogLoading = true;
      if (reset) {
        this.blogs = [];
        this.blogPage = 1;
        this.feedLastId = Date.now();
        this.feedOffset = 0;
      }
      try {
        let endpoint = "/blog/hot";
        let params = { current: this.blogPage };
        if (this.page === "profile") {
          endpoint = this.isOwnProfile ? "/blog/of/me" : "/blog/of/user";
          params.id = query.get("id");
        } else if (this.feed === "follow") {
          if (!this.requireUser()) {
            return;
          }
          endpoint = "/blog/of/follow";
          params = { lastId: this.feedLastId, offset: this.feedOffset };
        }
        const result = await api(endpoint, { params });
        const rows =
          this.feed === "follow" && this.page !== "profile"
            ? (result.data && result.data.list) || []
            : result.data || [];
        if (this.page === "profile" && this.profileUser) {
          rows.forEach((blog) => {
            blog.name = blog.name || this.profileUser.nickName;
            blog.icon = blog.icon || this.profileUser.icon;
          });
        }
        this.blogs = this.blogs.concat(rows);
        this.blogHasMore = rows.length > 0;
        this.blogPage += 1;
        if (result.data && result.data.minTime != null) {
          this.feedLastId = result.data.minTime;
          this.feedOffset = result.data.offset;
        }
      } catch (error) {
        this.pageError = error.message;
      } finally {
        this.blogLoading = false;
      }
    },
    async like(blog) {
      if (!this.requireUser() || blog.busy) {
        return;
      }
      this.$set(blog, "busy", true);
      try {
        await api("/blog/like/" + blog.id + "/state", {
          method: "PUT",
          params: { liked: !blog.isLike },
        });
        const fresh = (await api("/blog/" + blog.id)).data;
        this.$set(blog, "liked", fresh.liked);
        this.$set(blog, "isLike", fresh.isLike);
      } catch (error) {
        this.showError(error);
      } finally {
        this.$set(blog, "busy", false);
      }
    },
    async loadCatalog(reset = false) {
      if (this.catalogLoading && !reset) {
        return;
      }
      const sequence = ++this.catalogSequence;
      if (reset) {
        this.catalogPage = 1;
        this.shops = [];
      }
      this.catalogLoading = true;
      this.pageError = "";
      try {
        const result = await api("/shop/catalog", {
          params: {
            keyword: query.get("keyword") || "",
            typeId: this.catalogType,
            page: this.catalogPage,
            size: 12,
            sort: this.catalogSort,
          },
        });
        if (sequence !== this.catalogSequence) {
          return;
        }
        this.shops = this.shops.concat(result.data || []);
        this.catalogTotal = Number(result.total || 0);
        this.catalogPage += 1;
      } catch (error) {
        if (sequence === this.catalogSequence) {
          this.pageError = error.message;
        }
      } finally {
        if (sequence === this.catalogSequence) {
          this.catalogLoading = false;
        }
      }
    },
    async loadShop() {
      const id = query.get("id");
      if (!/^\d+$/.test(id || "")) {
        throw new Error("店铺地址无效，请从店铺列表重新进入");
      }
      this.shop = (await api("/shop/" + id)).data;
      if (!this.shop) {
        throw new Error("没有找到这家店铺");
      }
      const results = await Promise.allSettled([
        api("/voucher/list/" + id),
        api("/blog/of/shop", { params: { shopId: id, current: 1 } }),
      ]);
      if (results[0].status === "fulfilled") {
        this.vouchers = results[0].value.data || [];
      } else {
        this.notify("优惠券暂时加载失败，可刷新重试", "error");
      }
      if (results[1].status === "fulfilled") {
        this.shopBlogs = results[1].value.data || [];
      }
    },
    async loadBlog() {
      const id = query.get("id");
      if (!/^\d+$/.test(id || "")) {
        throw new Error("笔记地址无效，请从首页重新进入");
      }
      this.blog = (await api("/blog/" + id)).data;
      if (!this.blog) {
        throw new Error("没有找到这篇笔记");
      }
      try {
        this.blogShop = (await api("/shop/" + this.blog.shopId)).data;
      } catch {
        /* The note remains readable if its shop is unavailable. */
      }
      if (this.user && String(this.user.id) !== String(this.blog.userId)) {
        this.following = Boolean(
          (await api("/follow/or/not/" + this.blog.userId)).data,
        );
      }
    },
    async toggleFollow() {
      if (!this.requireUser() || this.followingBusy) {
        return;
      }
      this.followingBusy = true;
      try {
        await api("/follow/" + this.blog.userId + "/" + !this.following, {
          method: "PUT",
        });
        this.following = !this.following;
        this.notify(this.following ? "已关注这位作者" : "已取消关注");
      } catch (error) {
        this.showError(error);
      } finally {
        this.followingBusy = false;
      }
    },
    async loadOffers(initial = false) {
      if (this.offerLoading) {
        return;
      }
      this.offerLoading = true;
      try {
        const result = await api("/shop/catalog", {
          params: { page: this.offerShopsPage, size: 20, sort: "score" },
        });
        const shops = result.data || [];
        this.offerShopsTotal = Number(result.total || 0);
        let failures = 0;
        // Bound parallel requests when listing vouchers across many shops.
        for (let index = 0; index < shops.length; index += 4) {
          const rows = await Promise.allSettled(
            shops.slice(index, index + 4).map(async (shop) => {
              const vouchers =
                (await api("/voucher/list/" + shop.id)).data || [];
              return vouchers
                .filter((voucher) => voucher.orderable !== false)
                .map((voucher) => ({ ...voucher, shopName: shop.name }));
            }),
          );
          rows.forEach((row) => {
            if (row.status === "fulfilled") {
              this.offers.push(...row.value);
            } else {
              failures += 1;
            }
          });
        }
        if (failures) {
          this.notify("部分店铺的优惠加载失败，请稍后刷新重试", "error");
        }
        this.offerHasMore = this.offerShopsPage * 20 < this.offerShopsTotal;
        this.offerShopsPage += 1;
      } catch (error) {
        this.showError(error);
        if (initial === true) {
          throw error;
        }
      } finally {
        this.offerLoading = false;
      }
    },
    claim(voucher) {
      if (!this.requireUser() || this.claimBusy) {
        return;
      }
      this.dialog = { kind: "claim", voucher };
    },
    async confirmClaim() {
      const voucher = this.dialog.voucher;
      this.dialog = null;
      this.claimBusy = String(voucher.id);
      try {
        const result = await api(
          "/voucher-order/" +
            (voucher.type ? "seckill/" : "ordinary/") +
            voucher.id,
          { method: "POST" },
        );
        location.assign(
          "/orders.html" +
            (voucher.type
              ? "?request=" + encodeURIComponent(String(result.data))
              : ""),
        );
      } catch (error) {
        if (!error.status || error.status >= 500) {
          try {
            const existing = (
              await api("/voucher-order/of/voucher/" + voucher.id)
            ).data;
            if (existing) {
              location.assign("/orders.html");
              return;
            }
          } catch {
            /* Leave the uncertain result visible; do not automatically resubmit. */
          }
          this.notify("提交结果暂未确认，请到我的订单查看后再操作", "error");
        } else {
          this.showError(error);
        }
      } finally {
        this.claimBusy = "";
      }
    },
    async loadProfile() {
      this.profileUser = this.isOwnProfile
        ? this.user
        : (await api("/user/" + query.get("id"))).data;
      if (!this.profileUser) {
        throw new Error("这位用户暂时没有公开资料");
      }
      if (this.isOwnProfile) {
        this.signCount = Number((await api("/user/sign/count")).data || 0);
      }
      await this.loadBlogs(true);
    },
    async sign() {
      if (!this.requireUser() || this.signBusy) {
        return;
      }
      this.signBusy = true;
      try {
        await api("/user/sign", { method: "POST" });
        this.signCount = Number((await api("/user/sign/count")).data || 0);
        this.signed = true;
        this.notify("签到成功，今天也要好好生活");
      } catch (error) {
        this.showError(error);
      } finally {
        this.signBusy = false;
      }
    },
    async logout() {
      try {
        await api("/user/logout", { method: "POST", quiet: true });
      } catch (error) {
        if (error.status !== 401) {
          this.showError(error);
          return;
        }
      }
      sessionStorage.removeItem("token");
      location.assign("/");
    },
    async loadOrders() {
      if (this.orderLoading) {
        return;
      }
      this.orderLoading = true;
      this.orderError = "";
      try {
        const result = await api("/voucher-order/mine", {
          params: { page: this.orderPage },
        });
        this.orders = result.data || [];
        this.orderTotal = Number(result.total || 0);
      } catch (error) {
        this.orderError = error.message;
      } finally {
        this.orderLoading = false;
      }
    },
    async orderPageTo(pageNumber) {
      this.orderPage = pageNumber;
      await this.loadOrders();
    },
    async watchRequest(attempt = 0) {
      const id = query.get("request");
      if (!id || !/^\d+$/.test(id)) {
        return;
      }
      try {
        this.pendingRequest = (await api("/voucher-order/requests/" + id)).data;
        const state = this.pendingRequest && this.pendingRequest.state;
        if (state === "SUCCESS") {
          this.requestMessage = "领取已确认，请查看下面的订单";
          await this.loadOrders();
          return;
        }
        if (state === "FAILED") {
          this.requestMessage = "本次领取未成功，请返回店铺查看";
          return;
        }
        if (state === "REVIEW") {
          this.requestMessage = "领取结果仍在核对，请稍后重新查看";
          return;
        }
        this.requestMessage = "领取请求已受理，正在确认结果…";
        if (attempt < 10) {
          this.requestTimer = setTimeout(
            () => this.watchRequest(attempt + 1),
            2000,
          );
        } else {
          this.requestMessage = "订单仍在处理中，可稍后刷新查看，请勿重复提交";
        }
      } catch (error) {
        this.requestMessage = error.message;
      }
    },
    cancelOrder(order) {
      this.dialog = { kind: "cancel", order };
    },
    async confirmCancel() {
      const order = this.dialog.order;
      this.dialog = null;
      try {
        await api("/voucher-order/" + order.order_id + "/cancel", {
          method: "POST",
        });
        await this.loadOrders();
        this.notify("订单已取消");
      } catch (error) {
        this.showError(error);
      }
    },
    async sendCode() {
      this.loginError = "";
      if (!/^1[3-9]\d{9}$/.test(this.phone.trim())) {
        this.loginError = "请输入正确的 11 位手机号码";
        return;
      }
      if (this.codeBusy || this.codeCountdown) {
        return;
      }
      this.codeBusy = true;
      try {
        await api("/user/code", {
          method: "POST",
          params: { phone: this.phone.trim() },
        });
        this.codeCountdown = 60;
        this.codeTimer = setInterval(() => {
          this.codeCountdown -= 1;
          if (this.codeCountdown <= 0) {
            clearInterval(this.codeTimer);
          }
        }, 1000);
        this.notify("验证码已生成，请使用当前演示环境提供的验证码");
      } catch (error) {
        this.loginError = error.message;
      } finally {
        this.codeBusy = false;
      }
    },
    async login() {
      if (
        !/^1[3-9]\d{9}$/.test(this.phone.trim()) ||
        !/^\d{6}$/.test(this.code.trim())
      ) {
        this.loginError = "请填写正确的手机号和 6 位验证码";
        return;
      }
      this.loginBusy = true;
      this.loginError = "";
      try {
        const result = await api("/user/login", {
          method: "POST",
          body: { phone: this.phone.trim(), code: this.code.trim() },
        });
        sessionStorage.setItem("token", String(result.data));
        location.assign(safeReturn(query.get("return"), location.origin));
      } catch (error) {
        this.loginError = error.message;
      } finally {
        this.loginBusy = false;
      }
    },
    async loadPublishShops() {
      if (this.publishShopLoading) {
        return;
      }
      this.publishShopLoading = true;
      try {
        this.publishShops =
          (
            await api("/shop/catalog", {
              params: {
                keyword: this.shopQuery.trim(),
                size: 20,
                sort: "score",
              },
            })
          ).data || [];
      } catch (error) {
        this.showError(error);
      } finally {
        this.publishShopLoading = false;
      }
    },
    async upload(event) {
      const files = Array.from(event.target.files || []);
      if (files.length + this.draft.pictures.length > 9) {
        this.notify("最多上传 9 张图片", "error");
        event.target.value = "";
        return;
      }
      if (
        files.some(
          (file) =>
            file.size > 5000000 || !/\.(jpg|jpeg|png|webp)$/i.test(file.name),
        )
      ) {
        this.notify("请选择 5 MB 以内的 JPG、PNG 或 WebP 图片", "error");
        event.target.value = "";
        return;
      }
      this.uploadBusy = true;
      try {
        for (const file of files) {
          const body = new FormData();
          body.append("file", file);
          const result = await api("/upload/blog", { method: "POST", body });
          this.draft.pictures.push(String(result.data));
        }
      } catch (error) {
        this.showError(error);
      } finally {
        this.uploadBusy = false;
        event.target.value = "";
      }
    },
    async removePicture(path) {
      try {
        await api("/upload/blog", { method: "DELETE", params: { name: path } });
        this.draft.pictures = this.draft.pictures.filter(
          (item) => item !== path,
        );
      } catch (error) {
        this.showError(error);
      }
    },
    async saveBlog() {
      if (
        !this.draft.title.trim() ||
        !this.draft.content.trim() ||
        !this.draft.shopId
      ) {
        this.notify("请填写标题、探店内容并选择关联店铺", "error");
        return;
      }
      if (this.publishBusy || this.uploadBusy) {
        return;
      }
      this.publishBusy = true;
      try {
        const result = await api("/blog", {
          method: "POST",
          body: {
            title: this.draft.title.trim(),
            content: this.draft.content.trim(),
            shopId: this.draft.shopId,
            images: this.draft.pictures.join(","),
          },
        });
        this.draftSaved = true;
        location.assign("/blog-detail.html?id=" + result.data);
      } catch (error) {
        this.showError(error);
      } finally {
        this.publishBusy = false;
      }
    },
  },
});

window.addEventListener("beforeunload", (event) => {
  if (
    page === "publish" &&
    !app.draftSaved &&
    (app.draft.title || app.draft.content || app.draft.pictures.length)
  ) {
    event.preventDefault();
    event.returnValue = "";
  }
});
