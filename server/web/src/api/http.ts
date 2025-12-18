export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const text = await response.text().catch(() => "Unknown error");
    throw new ApiError(response.status, text);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return response.json();
}

export async function get<T>(
  url: string,
  params?: Record<string, string>,
): Promise<T> {
  const urlWithParams = params ? `${url}?${new URLSearchParams(params)}` : url;

  const response = await fetch(urlWithParams, {
    method: "GET",
    headers: { "Content-Type": "application/json" },
  });

  return handleResponse<T>(response);
}

export async function post<T, B = unknown>(url: string, body: B): Promise<T> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  return handleResponse<T>(response);
}

export async function del(
  url: string,
  params?: Record<string, string>,
): Promise<void> {
  const urlWithParams = params ? `${url}?${new URLSearchParams(params)}` : url;

  const response = await fetch(urlWithParams, {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
  });

  return handleResponse<void>(response);
}
