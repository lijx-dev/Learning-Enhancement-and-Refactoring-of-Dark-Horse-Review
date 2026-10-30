import test from "node:test";
import assert from "node:assert/strict";
import {
  ApiError,
  createApi,
  parseResponseText,
  safeReturn,
  imagePath,
  money,
} from "../scripts/api.js";

test("Java Long IDs retain every digit while normal numbers and string contents stay unchanged", () => {
  const result = parseResponseText(
    '{"success":true,"data":{"order_id":339587467398192145,"stock":9,"text":"订单 339587467398192145，\\"价格\\"","price":0.12345678901234567,"scientific":1.2e3,"negative":-9223372036854775808}}',
  );
  assert.equal(result.data.order_id, "339587467398192145");
  assert.equal(result.data.negative, "-9223372036854775808");
  assert.equal(result.data.stock, 9);
  assert.equal(result.data.text, '订单 339587467398192145，"价格"');
  assert.equal(result.data.price, 0.12345678901234567);
  assert.equal(result.data.scientific, 1200);
});

test("login returns only to the current origin and never loops back into login", () => {
  const origin = "http://127.0.0.1:8080";
  assert.equal(
    safeReturn("/shop-detail.html?id=4", origin),
    "/shop-detail.html?id=4",
  );
  assert.equal(safeReturn("//example.com/path", origin), "/");
  assert.equal(safeReturn("https://example.com/path", origin), "/");
  assert.equal(safeReturn("javascript:alert(1)", origin), "/");
  assert.equal(safeReturn("/login.html?return=/login.html", origin), "/");
});

test("uploaded paths resolve to original image storage and unsafe schemes use the placeholder", () => {
  assert.equal(
    imagePath("/blogs/1/2/example.png"),
    "/imgs/blogs/1/2/example.png",
  );
  assert.equal(imagePath("/imgs/blogs/example.png"), "/imgs/blogs/example.png");
  assert.equal(
    imagePath("javascript:alert(1)"),
    "/assets/image-placeholder.svg",
  );
  assert.equal(
    imagePath("/imgs/../scripts/app.js"),
    "/assets/image-placeholder.svg",
  );
});

test("voucher cents render correctly including free vouchers and missing amounts", () => {
  assert.equal(money(4750), "47.50");
  assert.equal(money(5000), "50");
  assert.equal(money(0), "0");
  assert.equal(money(null), "—");
});

test("requests encode Chinese searches and retain zero/false values and fresh authorization", async () => {
  let token = "first-token";
  const requests = [];
  const request = createApi({
    tokenProvider: () => token,
    onUnauthorized() {},
    fetchImpl: async (url, options) => {
      requests.push({ url, options });
      return new Response('{"success":true,"data":[]}');
    },
  });
  await request("/shop/catalog", {
    params: { keyword: "茶餐厅 & 咖啡", page: 0, liked: false },
  });
  token = "second-token";
  await request("/user/me");
  const url = new URL(requests[0].url, "http://127.0.0.1");
  assert.equal(url.searchParams.get("keyword"), "茶餐厅 & 咖啡");
  assert.equal(url.searchParams.get("page"), "0");
  assert.equal(url.searchParams.get("liked"), "false");
  assert.equal(requests[0].options.headers.authorization, "first-token");
  assert.equal(requests[1].options.headers.authorization, "second-token");
});

test("business rejection retains its actual message and differs from an uncertain network failure", async () => {
  const request = createApi({
    tokenProvider: () => null,
    onUnauthorized() {},
    fetchImpl: async () =>
      new Response('{"success":false,"errorMsg":"库存不足"}'),
  });
  await assert.rejects(
    request("/voucher-order/ordinary/1", { method: "POST" }),
    (error) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.message, "库存不足");
      assert.equal(error.status, 200);
      return true;
    },
  );
});

test("401 clears login through the callback and retains quiet/redirect intent", async () => {
  let quietValue;
  const request = createApi({
    tokenProvider: () => "expired",
    onUnauthorized: (quiet) => {
      quietValue = quiet;
    },
    fetchImpl: async () => new Response("", { status: 401 }),
  });
  await assert.rejects(
    request("/user/me", { quiet: true }),
    (error) => error.status === 401,
  );
  assert.equal(quietValue, true);
});

test("timeout and throttling show actionable messages rather than reporting success", async () => {
  const timeoutRequest = createApi({
    tokenProvider: () => null,
    onUnauthorized() {},
    timeout: 5,
    fetchImpl: (_, options) =>
      new Promise((resolve, reject) => {
        options.signal.addEventListener("abort", () =>
          reject(new DOMException("Aborted", "AbortError")),
        );
      }),
  });
  await assert.rejects(timeoutRequest("/shop/catalog"), /请求超时/);
  const limitedRequest = createApi({
    tokenProvider: () => null,
    onUnauthorized() {},
    fetchImpl: async () => new Response("", { status: 429 }),
  });
  await assert.rejects(limitedRequest("/user/code"), /操作太频繁/);
});
