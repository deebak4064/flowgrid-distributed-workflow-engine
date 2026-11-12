import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { lazy, StrictMode, Suspense, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import "./index.css";
import { Shell } from "@/components/Shell";
import { session } from "@/lib/api";
import Login from "@/pages/Login";

const Overview = lazy(() => import("@/pages/Overview"));
const Workflows = lazy(() => import("@/pages/Workflows"));
const Builder = lazy(() => import("@/pages/Builder"));
const LiveMonitor = lazy(() => import("@/pages/LiveMonitor"));
const Executions = lazy(() => import("@/pages/Executions"));
const ExecutionDetail = lazy(() => import("@/pages/ExecutionDetail"));
const Workers = lazy(() => import("@/pages/Workers"));
const Queues = lazy(() => import("@/pages/Queues"));
const DeadLetters = lazy(() => import("@/pages/DeadLetters"));
const Schedules = lazy(() => import("@/pages/Schedules"));
const Analytics = lazy(() => import("@/pages/Analytics"));
const Logs = lazy(() => import("@/pages/Logs"));
const SettingsPage = lazy(() => import("@/pages/Settings"));

const qc = new QueryClient({ defaultOptions: { queries: { staleTime: 2_000, retry: 1, refetchOnWindowFocus: false } } });

function RequireAuth({ children }: { children: ReactNode }) {
  return session.token() ? <>{children}</> : <Navigate to="/login" replace />;
}

const Loading = () => <div className="p-8 text-sm text-ink-3">Loading…</div>;

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={qc}>
      <BrowserRouter>
        <Suspense fallback={<Loading />}>
          <Routes>
            <Route path="/login" element={<Login />} />
            <Route element={<RequireAuth><Shell /></RequireAuth>}>
              <Route index element={<Overview />} />
              <Route path="workflows" element={<Workflows />} />
              <Route path="workflows/new" element={<Builder />} />
              <Route path="workflows/:id" element={<Builder />} />
              <Route path="workflows/:id/dependencies" element={<Builder tab="dependencies" />} />
              <Route path="live" element={<LiveMonitor />} />
              <Route path="executions" element={<Executions />} />
              <Route path="executions/:id" element={<ExecutionDetail />} />
              <Route path="workers" element={<Workers />} />
              <Route path="queues" element={<Queues />} />
              <Route path="dead-letters" element={<DeadLetters />} />
              <Route path="schedules" element={<Schedules />} />
              <Route path="analytics" element={<Analytics />} />
              <Route path="logs" element={<Logs />} />
              <Route path="settings" element={<SettingsPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Route>
          </Routes>
        </Suspense>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);
