import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["./test/global-setup.ts"],
    fileParallelism: false, // integration tests share one Postgres + Redis
    testTimeout: 60_000,
    hookTimeout: 120_000,
    reporters: ["verbose"],
  },
});
