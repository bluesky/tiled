// src/utils/apiClient.ts

const API_BASE = import.meta.env.VITE_API_URL || "/api/v1";

/**
 * Make an HTTP request with common options
 */
async function request<T>(
  endpoint: string,
  options: RequestInit = {},
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;

  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
    ...options,
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || `Request failed: ${response.status}`);
  }

  return response.json();
}

/**
 * Make GET request
 */
export async function get<T>(endpoint: string, token?: string): Promise<T> {
  return request<T>(endpoint, {
    method: "GET",
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
}

/**
 * Make POST request
 */
export async function post<T>(
  endpoint: string,
  body: unknown,
  token?: string,
): Promise<T> {
  return request<T>(endpoint, {
    method: "POST",
    body: JSON.stringify(body),
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
}

/**
 * Make POST request with form data
 */
export async function postForm<T>(
  endpoint: string,
  formData: Record<string, string>,
): Promise<T> {
  const body = new URLSearchParams(formData);

  const response = await fetch(`${API_BASE}${endpoint}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body.toString(),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || "Request failed");
  }

  return response.json();
}
