"use client";
import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useRegisterSource, useRefreshSource } from "@/lib/hooks";

export function SourceForm() {
  const register = useRegisterSource();
  const refresh = useRefreshSource();
  const [gitUrl, setGitUrl] = useState("");
  const [gitToken, setGitToken] = useState("");
  const [dbHost, setDbHost] = useState("");
  const [dbToken, setDbToken] = useState("");
  const [file, setFile] = useState<File | null>(null);

  async function handleGit() {
    const fd = new FormData();
    fd.append("source_type", "git");
    fd.append("url", gitUrl);
    fd.append("token", gitToken);
    const src = await register.mutateAsync(fd);
    await refresh.mutateAsync(src.id);
  }

  async function handleDatabricks() {
    const fd = new FormData();
    fd.append("source_type", "databricks");
    fd.append("url", dbHost);
    fd.append("token", dbToken);
    const src = await register.mutateAsync(fd);
    await refresh.mutateAsync(src.id);
  }

  async function handleUpload() {
    if (!file) return;
    const fd = new FormData();
    fd.append("source_type", "upload");
    fd.append("file", file);
    const src = await register.mutateAsync(fd);
    await refresh.mutateAsync(src.id);
  }

  const busy = register.isPending || refresh.isPending;

  return (
    <Tabs defaultValue="git">
      <TabsList>
        <TabsTrigger value="git">Git Repo</TabsTrigger>
        <TabsTrigger value="databricks">Databricks API</TabsTrigger>
        <TabsTrigger value="upload">Upload ZIP</TabsTrigger>
      </TabsList>

      <TabsContent value="git" className="space-y-3 pt-3">
        <div className="space-y-1">
          <Label>Repository URL</Label>
          <Input placeholder="https://github.com/org/repo" value={gitUrl} onChange={(e) => setGitUrl(e.target.value)} />
        </div>
        <div className="space-y-1">
          <Label>Personal Access Token (optional)</Label>
          <Input type="password" placeholder="ghp_..." value={gitToken} onChange={(e) => setGitToken(e.target.value)} />
        </div>
        <Button onClick={handleGit} disabled={busy || !gitUrl}>
          {busy ? "Connecting…" : "Connect & Parse"}
        </Button>
      </TabsContent>

      <TabsContent value="databricks" className="space-y-3 pt-3">
        <div className="space-y-1">
          <Label>Workspace Host</Label>
          <Input placeholder="https://adb-xxx.azuredatabricks.net" value={dbHost} onChange={(e) => setDbHost(e.target.value)} />
        </div>
        <div className="space-y-1">
          <Label>Access Token</Label>
          <Input type="password" placeholder="dapi..." value={dbToken} onChange={(e) => setDbToken(e.target.value)} />
        </div>
        <Button onClick={handleDatabricks} disabled={busy || !dbHost || !dbToken}>
          {busy ? "Connecting…" : "Connect & Parse"}
        </Button>
      </TabsContent>

      <TabsContent value="upload" className="space-y-3 pt-3">
        <div className="space-y-1">
          <Label>ZIP archive containing .ipynb / .py / .sql files</Label>
          <Input type="file" accept=".zip" onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
        </div>
        <Button onClick={handleUpload} disabled={busy || !file}>
          {busy ? "Uploading…" : "Upload & Parse"}
        </Button>
      </TabsContent>
    </Tabs>
  );
}
