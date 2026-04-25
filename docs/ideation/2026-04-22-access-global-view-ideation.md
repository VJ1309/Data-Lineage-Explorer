---
date: 2026-04-22
topic: access-management-global-view
focus: auth options, Supabase integration benefits, global lineage graph visualization
mode: conversational-ideation
---

# Ideation: Access Management & Global View — 2026-04-22

## Topics Discussed

### 1. Access Management Options

Current state: app is fully open, no auth layer.

Four options explored:

| Option | Approach | Cost | Code changes |
|--------|----------|------|-------------|
| Vercel Authentication | Deployment-level password/team protection, blocks before Next.js | Free (team access) / $20/mo Pro (password) | Zero |
| Next.js middleware auth | Clerk or NextAuth in `middleware.ts`, gates all pages | Free tier available | Moderate |
| API key on backend | Bearer token check in FastAPI `Depends()` | Free | Small |
| Full user auth (Supabase) | User accounts, roles, per-user isolation | Free up to 50k MAU | Significant |

**Key insight on Vercel Auth:** Only whitelisted Vercel team members can access — not all Vercel users. Gap: Railway backend URL remains exposed since it has no auth; fixable with a `BACKEND_SECRET` header checked in `routes.py`.

**Supabase Auth** is free up to 50,000 MAU, supports email/password, magic link, OAuth providers.

---

### 2. Supabase Integration Benefits

Beyond auth, Supabase would unlock:

- **Persistence** — biggest win. Currently all uploaded data is lost on every Railway redeploy. Storing edges in Postgres makes data survive restarts permanently.
- **Per-user data isolation** — each user sees only their own sources. Currently all uploads share global in-memory state and can collide.
- **File storage** — store original uploaded zip files in Supabase Storage for re-download/re-processing.
- **Saved views / bookmarks** — save frequently accessed lineage paths, column searches, impact analyses.
- **Upload history** — track when files were uploaded, by whom, what sources they contained.
- **Collaboration** — share a specific lineage view or source via a permalink.

**Core shift:** App currently works like a stateless CLI tool (upload → view → gone). With Supabase it becomes a proper multi-user SaaS with persistent workspaces per user.

---

### 3. Global Lineage View

**Concept:** Multiple uploads (use cases) can share source tables. A global graph merges all per-upload lineage into one network — shared tables become bridge nodes connecting different use case subgraphs.

**How it works technically:**
- Backend already uses NetworkX DiGraph — merging is natural
- Store edges in Supabase with a `source_id` tag per upload
- `engine.py` builds graphs per-source OR merged across all sources
- Frontend toggle: "Source view" vs "Global view"
- Existing `@xyflow/react` component handles more nodes with no changes

**Key capability unlocked:** Cross-use-case impact analysis — "if this raw table changes, which use cases are affected?"

---

### 4. Visualization Options for Global View

**codeflow repo** (`braedonsaunders/codeflow`) reviewed:
- Uses D3.js 7 force-directed graph
- Single HTML file, no build process, no extractable components
- Supports folder/layer/churn/blast radius modes

**Key insight:** Two different visualization paradigms suit different views:

| View | Library | Why |
|------|---------|-----|
| Per-source lineage | `@xyflow/react` (current) | Clean DAG, shows directional flow left-to-right |
| Global cross-source graph | D3 force-directed | Dense interconnections, clusters emerge naturally around use cases |

The global graph won't have a clean DAG structure once multiple use cases are merged — force-directed allows related tables to cluster visually, with shared bridge nodes sitting between clusters.

**Concepts to borrow from codeflow's approach:**
- Color-code nodes by source/use case
- Blast radius highlighting — click a shared table, see all affected use cases
- Drag/zoom/explore interaction

**Further visualization research:** Deferred — user requested broader research into global view visualization options. Visual companion offer made, pending response.

---

## Open Questions / Next Steps

- [ ] Confirm auth direction (Vercel team protection vs Supabase vs API key)
- [ ] Decide whether to pursue Supabase integration as a whole (persistence + auth + storage)
- [ ] Research additional global view visualization approaches (D3 force-directed, hierarchical, matrix, sankey, etc.)
- [ ] Design spec for global lineage view feature
