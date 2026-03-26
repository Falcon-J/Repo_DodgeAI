export const API_BASE_URL = resolveApiBaseUrl();

export async function fetchJson(url, options) {
  const response = await fetch(`${API_BASE_URL}${url}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options?.headers || {})
    },
    ...options
  });
  const contentType = response.headers.get("content-type") || "";

  if (!response.ok) {
    if (contentType.includes("application/json")) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || "Request failed");
    }

    const text = await response.text().catch(() => "");
    throw new Error(normalizeTextError(text) || "Request failed");
  }

  if (!contentType.includes("application/json")) {
    const text = await response.text().catch(() => "");
    throw new Error(normalizeTextError(text) || "The server returned a non-JSON response.");
  }

  return response.json();
}

export function describeApiTarget() {
  return API_BASE_URL || window.location.origin;
}

function resolveApiBaseUrl() {
  const configured = (import.meta.env.VITE_API_BASE_URL || "").trim().replace(/\/$/, "");
  if (configured) {
    return configured;
  }

  if (typeof window !== "undefined") {
    const { protocol, hostname, port } = window.location;
    const isLocal = hostname === "localhost" || hostname === "127.0.0.1";
    if (isLocal && port && port !== "8000") {
      return `${protocol}//${hostname}:8000`;
    }
  }

  return "";
}

function normalizeTextError(text) {
  const trimmed = text.trim();
  if (!trimmed) {
    return "";
  }

  if (trimmed.toLowerCase().startsWith("<!doctype") || trimmed.toLowerCase().startsWith("<html")) {
    return (
      `The frontend reached ${describeApiTarget()} but received HTML instead of JSON. ` +
      "If your backend is on a different host, set VITE_API_BASE_URL to that backend origin."
    );
  }

  return trimmed;
}
