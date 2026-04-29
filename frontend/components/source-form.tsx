"use client";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useRegisterSource, useRefreshSource } from "@/lib/hooks";

export function SourceForm() {
  const register = useRegisterSource();
  const refresh = useRefreshSource();
  const [file, setFile] = useState<File | null>(null);

  async function handleUpload() {
    if (!file) return;
    const fd = new FormData();
    fd.append("source_type", "upload");
    fd.append("file", file);
    const src = await register.mutateAsync(fd);
    await refresh.mutateAsync(src.id);
    setFile(null);
  }

  const busy = register.isPending || refresh.isPending;

  return (
    <div className="space-y-3">
      <div className="space-y-1">
        <Label>ZIP archive containing .ipynb / .py / .sql files</Label>
        <Input type="file" accept=".zip" onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
      </div>
      <Button onClick={handleUpload} disabled={busy || !file}>
        {busy ? "Uploading…" : "Upload & Parse"}
      </Button>
    </div>
  );
}
