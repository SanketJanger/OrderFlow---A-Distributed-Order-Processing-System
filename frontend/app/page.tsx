"use client";

import { useState, useEffect, useRef } from "react";
import axios from "axios";
import InventoryPanel from "./components/InventoryPanel";

const API = "http://localhost:8000";

interface Product { id: string; name: string; price: number; stock: number; }
interface Order { id: string; user_id: string; status: string; total_amount: number; created_at: string; }
interface Health { status: string; total_orders: number; total_jobs: number; dlq_count: number; postgres: string; redis: string; rabbitmq: string; }

const STATUS: Record<string, { color: string; bg: string; label: string }> = {
  pending:             { color: "#868e96", bg: "#f1f3f5", label: "Pending" },
  queued:              { color: "#7950f2", bg: "#f3f0ff", label: "Queued" },
  validating:          { color: "#f59f00", bg: "#fff9db", label: "Validating" },
  inventory_reserving: { color: "#f59f00", bg: "#fff9db", label: "Reserving Stock" },
  payment_processing:  { color: "#fd7e14", bg: "#fff4e6", label: "Processing Payment" },
  confirmed:           { color: "#12b886", bg: "#e6fcf5", label: "Confirmed" },
  fulfilling:          { color: "#2f54eb", bg: "#ebf0ff", label: "Fulfilling" },
  shipped:             { color: "#2f54eb", bg: "#ebf0ff", label: "Shipped" },
  delivered:           { color: "#12b886", bg: "#e6fcf5", label: "Delivered" },
  failed:              { color: "#f03e3e", bg: "#fff5f5", label: "Failed" },
  payment_failed:      { color: "#f03e3e", bg: "#fff5f5", label: "Payment Failed" },
  validation_failed:   { color: "#f03e3e", bg: "#fff5f5", label: "Validation Failed" },
  inventory_failed:    { color: "#f03e3e", bg: "#fff5f5", label: "Out of Stock" },
  cancelled:           { color: "#868e96", bg: "#f1f3f5", label: "Cancelled" },
};

const isPulsing = (s: string) => ["validating","inventory_reserving","payment_processing","queued"].includes(s);

function Badge({ status }: { status: string }) {
  const cfg = STATUS[status] || { color: "#868e96", bg: "#f1f3f5", label: status };
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", gap: 5,
      padding: "3px 8px", borderRadius: 5, fontSize: 11,
      fontFamily: "JetBrains Mono, monospace", fontWeight: 500,
      color: cfg.color, background: cfg.bg, letterSpacing: "0.02em",
      whiteSpace: "nowrap",
    }}>
      <span style={{
        width: 5, height: 5, borderRadius: "50%",
        background: cfg.color, flexShrink: 0,
        animation: isPulsing(status) ? "blink 1.2s infinite" : "none",
      }} />
      {cfg.label}
      <style>{`@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}`}</style>
    </span>
  );
}

function ServiceDot({ ok }: { ok: boolean }) {
  return (
    <span style={{
      display: "inline-block", width: 6, height: 6,
      borderRadius: "50%", background: ok ? "#12b886" : "#f03e3e",
      animation: ok ? "blink 2.5s infinite" : "none",
    }} />
  );
}

export default function Home() {
  const [products, setProducts]           = useState<Product[]>([]);
  const [orders, setOrders]               = useState<Order[]>([]);
  const [health, setHealth]               = useState<Health | null>(null);
  const [selected, setSelected]           = useState<Record<string, number>>({});
  const [priority, setPriority]           = useState("medium");
  const [userId, setUserId]               = useState("user_loading");
  const [loading, setLoading]             = useState(false);
  const [liveStatuses, setLiveStatuses]   = useState<Record<string, string>>({});
  const wsRefs = useRef<Record<string, WebSocket>>({});

  useEffect(() => {
    setUserId(`user_${Math.random().toString(36).slice(2, 8)}`);
    fetchAll();
    const t = setInterval(fetchAll, 5000);
    return () => clearInterval(t);
  }, []);

  const fetchAll = () => { fetchProducts(); fetchOrders(); fetchHealth(); };
  const fetchProducts = async () => { try { const r = await axios.get(`${API}/products`); setProducts(r.data); } catch {} };
  const fetchOrders   = async () => { try { const r = await axios.get(`${API}/orders`);   setOrders(r.data);   } catch {} };
  const fetchHealth   = async () => { try { const r = await axios.get(`${API}/health`);   setHealth(r.data);   } catch {} };

  const connectWS = (id: string) => {
    if (wsRefs.current[id]) return;
    const ws = new WebSocket(`ws://localhost:8000/ws/${id}`);
    ws.onmessage = (e) => {
      const d = JSON.parse(e.data);
      setLiveStatuses(p => ({ ...p, [d.order_id]: d.status }));
      setOrders(p => p.map(o => o.id === d.order_id ? { ...o, status: d.status } : o));
    };
    wsRefs.current[id] = ws;
  };

  const placeOrder = async () => {
    const items = Object.entries(selected).filter(([,q]) => q > 0).map(([product_id, quantity]) => ({ product_id, quantity }));
    if (!items.length) return;
    setLoading(true);
    try {
      const r = await axios.post(`${API}/orders`, { user_id: userId, items, priority });
      connectWS(r.data.order_id);
      setSelected({});
      fetchOrders();
    } catch (e: any) { alert(e.response?.data?.detail || "Failed"); }
    finally { setLoading(false); }
  };

  const cancelOrder = async (id: string) => {
    try { await axios.post(`${API}/orders/${id}/cancel`); fetchOrders(); }
    catch (e: any) { alert(e.response?.data?.detail || "Cannot cancel"); }
  };

  const total = Object.entries(selected).reduce((s, [id, q]) => {
    const p = products.find(p => p.id === id);
    return s + (p ? p.price * q : 0);
  }, 0);

  const PRIORITY_CFG: Record<string, { color: string; bg: string; border: string }> = {
    low:    { color: "#868e96", bg: "#f1f3f5",   border: "#dee2e6" },
    medium: { color: "#f59f00", bg: "#fff9db",   border: "#f59f00" },
    high:   { color: "#f03e3e", bg: "#fff5f5",   border: "#f03e3e" },
  };

  return (
    <div style={{ minHeight: "100vh", background: "#f8f9fa" }}>

      {/* Top nav */}
      <div style={{
        background: "#fff", borderBottom: "1px solid #e9ecef",
        padding: "0 32px", height: 56,
        display: "flex", alignItems: "center", justifyContent: "space-between",
        position: "sticky", top: 0, zIndex: 100,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{
            width: 28, height: 28, borderRadius: 7,
            background: "#2f54eb", display: "flex",
            alignItems: "center", justifyContent: "center",
          }}>
            <span style={{ color: "#fff", fontSize: 13, fontWeight: 600 }}>O</span>
          </div>
          <span style={{ fontWeight: 600, fontSize: 15, color: "#1a1a2e", letterSpacing: "-0.01em" }}>
            OrderFlow - A Distributed Order Processing System
          </span>
          <span style={{
            marginLeft: 4, fontSize: 10, fontWeight: 500,
            color: "#868e96", background: "#f1f3f5",
            padding: "1px 6px", borderRadius: 4,
            fontFamily: "JetBrains Mono, monospace",
          }}>
          </span>
        </div>
      </div>

      <div style={{ padding: "24px 32px", maxWidth: 1300, margin: "0 auto" }}>

        {/* Stats */}
        {health && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 24 }}>
            {[
              { label: "Total orders",  value: health.total_orders, sub: "all time"          },
              { label: "Jobs processed",value: health.total_jobs,   sub: "sync + async"      },
              { label: "DLQ items",     value: health.dlq_count,    sub: "failed jobs",      },
              { label: "System status", value: health.status,       sub: "all services up"   },
            ].map((s, i) => (
              <div key={i} style={{
                background: "#fff", border: "1px solid #e9ecef",
                borderRadius: 10, padding: "16px 20px",
              }}>
                <div style={{ fontSize: 12, color: "#868e96", marginBottom: 4 }}>{s.label}</div>
                <div style={{
                  fontSize: 22, fontWeight: 600, color: i === 2 && s.value > 0 ? "#f03e3e" : "#1a1a2e",
                  letterSpacing: "-0.02em", fontFamily: "JetBrains Mono, monospace",
                }}>
                  {s.value}
                </div>
                <div style={{ fontSize: 11, color: "#adb5bd", marginTop: 2 }}>{s.sub}</div>
              </div>
            ))}
          </div>
        )}

        {/* Main layout */}
        <div style={{ display: "grid", gridTemplateColumns: "340px 1fr", gap: 16, alignItems: "start" }}>

          {/* Left column */}
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>

            {/* Order form */}
            <div style={{ background: "#fff", border: "1px solid #e9ecef", borderRadius: 10, overflow: "hidden" }}>
              <div style={{ padding: "16px 20px", borderBottom: "1px solid #e9ecef" }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: "#1a1a2e" }}>New order</div>
                <div style={{ fontSize: 11, color: "#868e96", marginTop: 2 }}>Select products and quantity</div>
              </div>

              <div style={{ padding: "12px 16px", display: "flex", flexDirection: "column", gap: 6 }}>
                {products.map(p => (
                  <div key={p.id} style={{
                    padding: "10px 12px", borderRadius: 7,
                    border: `1px solid ${selected[p.id] ? "#2f54eb" : "#e9ecef"}`,
                    background: selected[p.id] ? "#f8f9ff" : "#fff",
                    transition: "all 0.15s",
                  }}>
                    <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
                      <div>
                        <div style={{ fontSize: 12, fontWeight: 500, color: "#1a1a2e" }}>{p.name}</div>
                        <div style={{ fontSize: 11, color: "#868e96", marginTop: 1 }}>
                          ${p.price.toLocaleString()} · {p.stock} left
                        </div>
                      </div>
                    </div>
                    <div style={{ display: "flex", gap: 4 }}>
                      {[0,1,2,3].map(n => (
                        <button key={n} onClick={() => setSelected(prev => ({ ...prev, [p.id]: n }))}
                          style={{
                            width: 28, height: 28, borderRadius: 5, cursor: "pointer",
                            border: `1px solid ${selected[p.id] === n ? "#2f54eb" : "#dee2e6"}`,
                            background: selected[p.id] === n ? "#2f54eb" : "#fff",
                            color: selected[p.id] === n ? "#fff" : "#868e96",
                            fontSize: 11, fontWeight: 500, transition: "all 0.1s",
                          }}>
                          {n}
                        </button>
                      ))}
                      <span style={{ fontSize: 11, color: "#adb5bd", alignSelf: "center", marginLeft: 2 }}>qty</span>
                    </div>
                  </div>
                ))}
              </div>

              <div style={{ padding: "12px 16px", borderTop: "1px solid #e9ecef" }}>
                <div style={{ fontSize: 11, color: "#868e96", marginBottom: 8, fontWeight: 500 }}>Priority</div>
                <div style={{ display: "flex", gap: 6, marginBottom: 12 }}>
                  {["low","medium","high"].map(p => {
                    const cfg = PRIORITY_CFG[p];
                    const active = priority === p;
                    return (
                      <button key={p} onClick={() => setPriority(p)}
                        style={{
                          flex: 1, padding: "6px 0", borderRadius: 6, cursor: "pointer",
                          border: `1px solid ${active ? cfg.border : "#dee2e6"}`,
                          background: active ? cfg.bg : "#fff",
                          color: active ? cfg.color : "#868e96",
                          fontSize: 11, fontWeight: 500, transition: "all 0.1s",
                          textTransform: "capitalize",
                        }}>
                        {p}
                      </button>
                    );
                  })}
                </div>

                {total > 0 && (
                  <div style={{
                    display: "flex", justifyContent: "space-between",
                    marginBottom: 10, padding: "8px 10px",
                    background: "#f8f9fa", borderRadius: 6,
                  }}>
                    <span style={{ fontSize: 12, color: "#868e96" }}>Order total</span>
                    <span style={{ fontSize: 13, fontWeight: 600, color: "#1a1a2e", fontFamily: "JetBrains Mono, monospace" }}>
                      ${total.toFixed(2)}
                    </span>
                  </div>
                )}

                <button onClick={placeOrder} disabled={loading || total === 0}
                  style={{
                    width: "100%", padding: "9px", borderRadius: 7,
                    border: "none", cursor: loading || total === 0 ? "not-allowed" : "pointer",
                    background: loading || total === 0 ? "#f1f3f5" : "#2f54eb",
                    color: loading || total === 0 ? "#adb5bd" : "#fff",
                    fontSize: 12, fontWeight: 600, transition: "all 0.15s",
                  }}>
                  {loading ? "Placing order..." : "Place order"}
                </button>

                <div style={{ marginTop: 8, fontSize: 10, color: "#adb5bd", textAlign: "center", fontFamily: "JetBrains Mono, monospace" }}>
                  {userId}
                </div>
              </div>
            </div>

            {/* Inventory */}
            <InventoryPanel />
          </div>

          {/* Orders table */}
          <div style={{ background: "#fff", border: "1px solid #e9ecef", borderRadius: 10, overflow: "hidden" }}>
            <div style={{
              padding: "16px 20px", borderBottom: "1px solid #e9ecef",
              display: "flex", alignItems: "center", justifyContent: "space-between",
            }}>
              <div>
                <div style={{ fontSize: 13, fontWeight: 600, color: "#1a1a2e" }}>Live order tracker</div>
                <div style={{ fontSize: 11, color: "#868e96", marginTop: 2 }}>Updates in real time via WebSocket</div>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                <span style={{
                  width: 6, height: 6, borderRadius: "50%",
                  background: "#12b886", animation: "blink 2s infinite",
                  display: "inline-block",
                }} />
                <span style={{ fontSize: 11, color: "#12b886", fontWeight: 500 }}>Live</span>
              </div>
            </div>

            {orders.length === 0 ? (
              <div style={{ padding: "60px 20px", textAlign: "center", color: "#adb5bd" }}>
                <div style={{ fontSize: 32, marginBottom: 8 }}>—</div>
                <div style={{ fontSize: 13 }}>No orders yet</div>
              </div>
            ) : (
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "#f8f9fa" }}>
                    {["Order ID", "User", "Total", "Status", "Actions"].map(h => (
                      <th key={h} style={{
                        textAlign: "left", padding: "10px 16px",
                        fontSize: 11, fontWeight: 500, color: "#868e96",
                        borderBottom: "1px solid #e9ecef", letterSpacing: "0.01em",
                      }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {orders.map((order, i) => {
                    const status = liveStatuses[order.id] || order.status;
                    const canCancel = ["pending","queued"].includes(status);
                    return (
                      <tr key={order.id} style={{
                        borderBottom: i < orders.length - 1 ? "1px solid #f1f3f5" : "none",
                        transition: "background 0.1s",
                      }}
                        onMouseEnter={e => (e.currentTarget.style.background = "#fafafa")}
                        onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                      >
                        <td style={{ padding: "12px 16px", fontFamily: "JetBrains Mono, monospace", fontSize: 11, color: "#868e96" }}>
                          {order.id.slice(0, 8)}
                        </td>
                        <td style={{ padding: "12px 16px", fontSize: 12, color: "#495057" }}>
                          {order.user_id}
                        </td>
                        <td style={{ padding: "12px 16px", fontFamily: "JetBrains Mono, monospace", fontSize: 12, fontWeight: 600, color: "#1a1a2e" }}>
                          ${Number(order.total_amount).toFixed(2)}
                        </td>
                        <td style={{ padding: "12px 16px" }}>
                          <Badge status={status} />
                        </td>
                        <td style={{ padding: "12px 16px" }}>
                          {canCancel && (
                            <button onClick={() => cancelOrder(order.id)}
                              style={{
                                padding: "4px 10px", borderRadius: 5,
                                border: "1px solid #dee2e6", background: "#fff",
                                color: "#868e96", fontSize: 11, cursor: "pointer",
                                transition: "all 0.1s",
                              }}
                              onMouseEnter={e => {
                                (e.target as HTMLElement).style.borderColor = "#f03e3e";
                                (e.target as HTMLElement).style.color = "#f03e3e";
                              }}
                              onMouseLeave={e => {
                                (e.target as HTMLElement).style.borderColor = "#dee2e6";
                                (e.target as HTMLElement).style.color = "#868e96";
                              }}
                            >
                              Cancel
                            </button>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
