import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./api";

function invalidateLineageData(qc: ReturnType<typeof useQueryClient>) {
  qc.invalidateQueries({ queryKey: ["sources"] });
  qc.invalidateQueries({ queryKey: ["source-files"] });
  qc.invalidateQueries({ queryKey: ["tables"] });
  qc.invalidateQueries({ queryKey: ["columns"] });
  qc.invalidateQueries({ queryKey: ["lineage"] });
  qc.invalidateQueries({ queryKey: ["paths"] });
  qc.invalidateQueries({ queryKey: ["impact"] });
  qc.invalidateQueries({ queryKey: ["search"] });
  qc.invalidateQueries({ queryKey: ["warnings"] });
  qc.invalidateQueries({ queryKey: ["lineage-trace"] });
}

export function useSources() {
  return useQuery({ queryKey: ["sources"], queryFn: api.sources.list });
}

export function useDeleteSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.delete,
    onSuccess: () => invalidateLineageData(qc),
  });
}

export function useRefreshSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.refresh,
    onSuccess: () => invalidateLineageData(qc),
  });
}

export function useRegisterSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.register,
    onSuccess: () => invalidateLineageData(qc),
  });
}

export function useSourceFiles(sourceId: string | null) {
  return useQuery({
    queryKey: ["source-files", sourceId],
    queryFn: () => api.sources.files(sourceId!),
    enabled: sourceId !== null,
  });
}

export function useTables() {
  return useQuery({ queryKey: ["tables"], queryFn: api.tables.list });
}

export function useColumns(table: string | null) {
  return useQuery({
    queryKey: ["columns", table],
    queryFn: () => api.tables.columns(table!),
    enabled: table !== null,
  });
}

export function useLineage(table: string | null, column: string | null) {
  return useQuery({
    queryKey: ["lineage", table, column],
    queryFn: () => api.lineage(table!, column!),
    enabled: table !== null && column !== null,
  });
}

export function usePaths(table: string | null, column: string | null, enabled = true) {
  return useQuery({
    queryKey: ["paths", table, column],
    queryFn: () => api.paths(table!, column!),
    enabled: enabled && table !== null && column !== null,
  });
}

export function useImpact(table: string | null, column: string | null) {
  return useQuery({
    queryKey: ["impact", table, column],
    queryFn: () => api.impact(table!, column!),
    enabled: table !== null && column !== null,
  });
}

export function useWarnings() {
  return useQuery({ queryKey: ["warnings"], queryFn: api.warnings });
}

export function useSearch(q: string) {
  return useQuery({
    queryKey: ["search", q],
    queryFn: () => api.search(q),
    enabled: q.length >= 2,
    staleTime: 10_000,
  });
}

export function useLineageTrace(table: string | null, column: string | null) {
  return useQuery({
    queryKey: ["lineage-trace", table, column],
    queryFn: () => api.lineageTrace(table!, column!),
    enabled: table !== null && column !== null,
  });
}
