import type { Metadata } from "next";
import { Bricolage_Grotesque, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import { QueryProvider } from "@/components/query-provider";
import { Nav } from "@/components/nav";

const bricolage = Bricolage_Grotesque({
  subsets: ["latin"],
  variable: "--font-sans",
});

const jetbrainsMono = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-geist-mono",
});

export const metadata: Metadata = {
  title: "DataLineage Explorer",
  description: "Column-level data lineage for Databricks pipelines",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className={`dark ${bricolage.variable} ${jetbrainsMono.variable}`}>
      <body>
        <QueryProvider>
          <Nav />
          <main className="p-6">{children}</main>
        </QueryProvider>
      </body>
    </html>
  );
}
