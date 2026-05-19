import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "OrderFlow",
  description: "Distributed Order Processing System",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
