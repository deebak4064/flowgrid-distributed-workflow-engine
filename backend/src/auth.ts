// Authentication primitives on the standard library only: HS256 JWTs and scrypt password hashes.
import crypto from "node:crypto";
import { config, log } from "./config.ts";

export type Role = "ADMIN" | "OPERATOR" | "VIEWER";
export interface Principal { userId: string; orgId: string; email: string; role: Role }
const RANK: Record<Role, number> = { VIEWER: 0, OPERATOR: 1, ADMIN: 2 };
/** ADMIN can do everything an OPERATOR can, who can do everything a VIEWER can. */
export const hasRole = (p: Principal, need: Role) => RANK[p.role] >= RANK[need];

const ISSUER = "flowgrid";
let key: Buffer;
function signingKey(): Buffer {
  if (key) return key;
  if (config.security.jwtSecret) {
    key = Buffer.from(config.security.jwtSecret, "utf8");
    if (key.length < 32) throw new Error("FLOWGRID_JWT_SECRET must be at least 32 bytes");
  } else {
    if (process.env.NODE_ENV === "production") throw new Error("FLOWGRID_JWT_SECRET is required in production");
    // Never ship a hard-coded signing key: without config, generate one per process (dev only).
    key = crypto.randomBytes(32);
    log("warn", "FLOWGRID_JWT_SECRET not set: using an ephemeral key; tokens die with this process");
  }
  return key;
}

const b64 = (b: Buffer | string) => Buffer.from(b).toString("base64url");

export function signToken(p: Principal, ttlMinutes = config.security.tokenTtlMinutes) {
  const now = Math.floor(Date.now() / 1000), exp = now + ttlMinutes * 60;
  const body = `${b64(JSON.stringify({ alg: "HS256", typ: "JWT" }))}.${b64(JSON.stringify({ iss: ISSUER, sub: p.userId, org: p.orgId, email: p.email, role: p.role, iat: now, exp }))}`;
  return { token: `${body}.${b64(crypto.createHmac("sha256", signingKey()).update(body).digest())}`, expiresAt: new Date(exp * 1000) };
}

export function verifyToken(token: string): Principal | null {
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const expected = crypto.createHmac("sha256", signingKey()).update(`${parts[0]}.${parts[1]}`).digest();
  const given = Buffer.from(parts[2], "base64url");
  if (given.length !== expected.length || !crypto.timingSafeEqual(given, expected)) return null;
  try {
    const h = JSON.parse(Buffer.from(parts[0], "base64url").toString());
    const c = JSON.parse(Buffer.from(parts[1], "base64url").toString());
    if (h.alg !== "HS256" || c.iss !== ISSUER || typeof c.exp !== "number" || c.exp < Date.now() / 1000) return null;
    if (!["ADMIN", "OPERATOR", "VIEWER"].includes(c.role)) return null;
    return { userId: c.sub, orgId: c.org, email: c.email, role: c.role };
  } catch {
    return null;
  }
}

/** scrypt (N=2^14, r=8, p=1), random 16-byte salt; format scrypt$salt$hash. */
export function hashPassword(pw: string): string {
  const salt = crypto.randomBytes(16);
  return `scrypt$${salt.toString("base64")}$${crypto.scryptSync(pw, salt, 32).toString("base64")}`;
}

export function verifyPassword(pw: string, stored: string): boolean {
  const [alg, salt, hash] = stored.split("$");
  if (alg !== "scrypt" || !salt || !hash) return false;
  const want = Buffer.from(hash, "base64");
  const got = crypto.scryptSync(pw, Buffer.from(salt, "base64"), want.length);
  return crypto.timingSafeEqual(got, want);
}
