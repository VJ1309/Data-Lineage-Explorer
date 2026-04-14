import { NextRequest, NextResponse } from "next/server";

const API_URL = process.env.API_URL ?? "http://localhost:8000";

async function handler(
  req: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  const { path } = await params;
  const upstreamPath = "/" + path.join("/");
  const search = req.nextUrl.search;
  const url = `${API_URL}${upstreamPath}${search}`;

  const headers = new Headers();
  req.headers.forEach((value, key) => {
    if (!["host", "connection"].includes(key.toLowerCase())) {
      headers.set(key, value);
    }
  });

  const body =
    req.method === "GET" || req.method === "HEAD" ? undefined : req.body;

  const upstream = await fetch(url, {
    method: req.method,
    headers,
    body,
    // @ts-expect-error Node fetch supports duplex
    duplex: body ? "half" : undefined,
  });

  const resHeaders = new Headers();
  upstream.headers.forEach((value, key) => {
    resHeaders.set(key, value);
  });

  return new NextResponse(upstream.body, {
    status: upstream.status,
    headers: resHeaders,
  });
}

export const GET = handler;
export const POST = handler;
export const PUT = handler;
export const PATCH = handler;
export const DELETE = handler;
