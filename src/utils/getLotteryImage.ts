type LogoManifest = {
  by_final_slug: Record<string, string>;
  by_canonical_slug: Record<string, string>;
  meta?: {
    generated_at?: string;
    total_files?: number;
  };
};

let manifestCache: LogoManifest | null = null;

async function loadManifest(): Promise<LogoManifest> {
  if (manifestCache) return manifestCache;

  const res = await fetch("/lottery-logos-manifest.json", { cache: "no-store" });
  if (!res.ok) {
    manifestCache = {
      by_final_slug: {},
      by_canonical_slug: {},
    };
    return manifestCache;
  }

  manifestCache = await res.json();
  return manifestCache!;
}

export async function getLotteryImage(
  finalSlug?: string | null,
  canonicalSlug?: string | null
): Promise<string> {
  const manifest = await loadManifest();

  if (finalSlug && manifest.by_final_slug[finalSlug]) {
    return manifest.by_final_slug[finalSlug];
  }

  if (canonicalSlug && manifest.by_canonical_slug[canonicalSlug]) {
    return manifest.by_canonical_slug[canonicalSlug];
  }

  return "/lottery-logos/default-game.png";
}