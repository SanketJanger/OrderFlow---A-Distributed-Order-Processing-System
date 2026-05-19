"use client";

import { useEffect, useState } from "react";
import axios from "axios";

const API = "http://localhost:8000";

interface Product { id: string; name: string; price: number; stock: number; }

const ORIGINAL: Record<string, number> = {
  "AirPods Pro": 50,
  "Apple Watch Series 9": 20,
  "iPad Air": 15,
  "iPhone 15 Pro": 25,
  'MacBook Pro 14"': 10,
};

export default function InventoryPanel() {
  const [products, setProducts] = useState<Product[]>([]);

  useEffect(() => {
    fetch();
    const t = setInterval(fetch, 3000);
    return () => clearInterval(t);
  }, []);

  const fetch = async () => {
    try { const r = await axios.get(`${API}/products`); setProducts(r.data); } catch {}
  };

  return (
    <div style={{ background: "#fff", border: "1px solid #e9ecef", borderRadius: 10, overflow: "hidden" }}>
      <div style={{
        padding: "16px 20px", borderBottom: "1px solid #e9ecef",
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 600, color: "#1a1a2e" }}>Inventory</div>
          <div style={{ fontSize: 11, color: "#868e96", marginTop: 2 }}>Live stock levels</div>
        </div>
        <span style={{
          fontSize: 10, color: "#12b886", background: "#e6fcf5",
          padding: "2px 7px", borderRadius: 4, fontWeight: 500,
        }}>
          Live · 3s
        </span>
      </div>

      <div style={{ padding: "12px 16px", display: "flex", flexDirection: "column", gap: 12 }}>
        {products.map(p => {
          const original = ORIGINAL[p.name] || 50;
          const pct = Math.round((p.stock / original) * 100);
          const isLow = pct <= 30;
          const isCritical = pct <= 10;
          const color = isCritical ? "#f03e3e" : isLow ? "#f59f00" : "#12b886";

          return (
            <div key={p.id}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 5 }}>
                <span style={{ fontSize: 12, color: "#495057" }}>{p.name}</span>
                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  {isLow && (
                    <span style={{
                      fontSize: 9, fontWeight: 600,
                      color: isCritical ? "#f03e3e" : "#f59f00",
                      background: isCritical ? "#fff5f5" : "#fff9db",
                      padding: "1px 5px", borderRadius: 3,
                      textTransform: "uppercase", letterSpacing: "0.05em",
                    }}>
                      {isCritical ? "Critical" : "Low"}
                    </span>
                  )}
                  <span style={{
                    fontSize: 11, color: "#868e96",
                    fontFamily: "JetBrains Mono, monospace",
                  }}>
                    {p.stock}/{original}
                  </span>
                </div>
              </div>
              <div style={{ height: 3, background: "#f1f3f5", borderRadius: 2, overflow: "hidden" }}>
                <div style={{
                  height: "100%", width: `${pct}%`,
                  background: color, borderRadius: 2,
                  transition: "width 0.5s ease, background 0.3s ease",
                }} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
