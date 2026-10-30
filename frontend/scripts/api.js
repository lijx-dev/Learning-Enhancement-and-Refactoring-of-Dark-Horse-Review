export class ApiError extends Error {
  constructor(message, status = 0) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

// Java Long order IDs must survive parsing without JavaScript precision loss.
export function parseResponseText(text) {
  const lossless = text.replace(
    /"(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
    (token) =>
      /^-?\d{16,}$/.test(token) && !Number.isSafeInteger(Number(token))
        ? JSON.stringify(token)
        : token,
  );
  return JSON.parse(lossless);
}

export function safeReturn(value, origin) {
  try {
    const target = new URL(value || "/", origin);
    return target.origin === origin && !target.pathname.startsWith("/login")
      ? target.pathname + target.search + target.hash
      : "/";
  } catch {
    return "/";
  }
}

export function imagePath(value) {
  const path = String(value || "").trim();
  if (/^https?:\/\//i.test(path)) {
    return path;
  }
  if (/^\/(imgs|assets)\//.test(path) && !path.includes("..")) {
    return path;
  }
  if (/^\/blogs\//.test(path) && !path.includes("..")) {
    return "/imgs" + path;
  }
  return "/assets/image-placeholder.svg";
}

export function images(value) {
  return String(value || "")
    .split(",")
    .filter((path) => path.trim())
    .map(imagePath);
}

export function money(cents) {
  if (cents == null || cents === "" || !Number.isFinite(Number(cents))) {
    return "—";
  }
  return (Number(cents) / 100).toFixed(2).replace(/\.00$/, "");
}

export function createApi({
  fetchImpl = fetch,
  tokenProvider,
  onUnauthorized,
  timeout = 10000,
}) {
  return async function request(
    path,
    { method = "GET", params, body, quiet = false } = {},
  ) {
    const query = new URLSearchParams();
    Object.entries(params || {}).forEach(([key, value]) => {
      if (value != null && value !== "") {
        query.set(key, String(value));
      }
    });
    const url = "/api" + path + (query.size ? "?" + query.toString() : "");
    const headers = { Accept: "application/json" };
    const token = tokenProvider();
    if (token) {
      headers.authorization = token;
    }
    const multipart =
      typeof FormData !== "undefined" && body instanceof FormData;
    if (body != null && !multipart) {
      headers["Content-Type"] = "application/json";
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    try {
      const response = await fetchImpl(url, {
        method,
        headers,
        credentials: "same-origin",
        signal: controller.signal,
        body:
          body == null ? undefined : multipart ? body : JSON.stringify(body),
      });
      if (response.status === 401) {
        onUnauthorized(quiet);
        throw new ApiError("登录已过期，请重新登录", 401);
      }
      if (!response.ok) {
        throw new ApiError(
          response.status === 429
            ? "操作太频繁，请稍后再试"
            : "服务暂时不可用，请稍后重试",
          response.status,
        );
      }
      const result = parseResponseText(await response.text());
      if (!result.success) {
        throw new ApiError(
          result.errorMsg || "操作未完成，请重试",
          response.status,
        );
      }
      return result;
    } catch (error) {
      if (error instanceof ApiError) {
        throw error;
      }
      throw new ApiError(
        error.name === "AbortError"
          ? "请求超时，请稍后重试"
          : "网络连接失败，请检查连接",
      );
    } finally {
      clearTimeout(timer);
    }
  };
}
