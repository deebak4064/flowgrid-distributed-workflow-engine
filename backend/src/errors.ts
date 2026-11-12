// Domain error mapped to one RFC 7807 problem+json shape by the API's error handler.
export class ApiError extends Error {
  readonly status: number; readonly code: string; readonly errors: string[];
  constructor(status: number, code: string, message: string, errors: string[] = []) { super(message); this.status = status; this.code = code; this.errors = errors; }
  static notFound = (what: string) => new ApiError(404, "not_found", `${what} not found`);
  static conflict = (msg: string) => new ApiError(409, "conflict", msg);
  static badRequest = (msg: string, errors: string[] = []) => new ApiError(400, "validation_failed", msg, errors);
  static forbidden = () => new ApiError(403, "forbidden", "Your role does not permit this action");
}

/** Clamp user-supplied paging so a single request can never ask for an unbounded result set. */
export const pageOf = (page?: unknown, size?: unknown) => ({
  page: Math.max(0, Number.parseInt(String(page ?? 0), 10) || 0),
  size: Math.max(1, Math.min(Number.parseInt(String(size ?? 25), 10) || 25, 200)),
});
