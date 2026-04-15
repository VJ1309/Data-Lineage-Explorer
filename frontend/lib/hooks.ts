import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./api";

export function useSources() {
  return useQuery({ queryKey: ["sources"], queryFn: api.sources.list });
}

export function useDeleteSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.delete,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sources"] }),
  });
}

export function useRefreshSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.refresh,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sources"] });
      qc.invalidateQueries({ queryKey: ["tables"] });
      qc.invalidateQueries({ queryKey: ["warnings"] });
    },
  });
}

export function useRegisterSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.register,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sources"] }),
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
