"use client";

import React from "react";

type AskResultRow = { [key: string]: any } & { label?: string; pct?: number; value?: number };
type AskResponse = {
  status?: string;
  question?: string;
  source?: string;
  sql?: string;
  pie_chart?: boolean;
  answer?: string;
  expected_columns?: string[];
  result?: { columns: string[]; rows: AskResultRow[]; rowcount: number };
  error?: string;
  message?: string;
};

function PieChart({ data, size = 320 }: { data: { label: string; pct: number; color?: string }[]; size?: number }) {
  const radius = size / 2;
  const center = { x: radius, y: radius };
  let cumulative = 0;
  const colors = [
    "#4f46e5",
    "#22c55e",
    "#f59e0b",
    "#ef4444",
    "#06b6d4",
    "#a855f7",
    "#84cc16",
    "#e11d48",
    "#0ea5e9",
    "#d946ef",
  ];

  const toRadians = (deg: number) => (deg - 90) * (Math.PI / 180);
  const polarToCartesian = (angle: number) => {
    const a = toRadians(angle);
    return { x: center.x + radius * Math.cos(a), y: center.y + radius * Math.sin(a) };
  };

  const slices = data.map((d, i) => {
    const startAngle = cumulative;
    const sliceAngle = Math.max(0, Math.min(360, (d.pct || 0)));
    cumulative += sliceAngle;
    const endAngle = cumulative;
    const largeArc = sliceAngle > 180 ? 1 : 0;
    const start = polarToCartesian(startAngle);
    const end = polarToCartesian(endAngle);
    const pathData = [
      `M ${center.x} ${center.y}`,
      `L ${start.x} ${start.y}`,
      `A ${radius} ${radius} 0 ${largeArc} 1 ${end.x} ${end.y}`,
      "Z",
    ].join(" ");
    const color = d.color || colors[i % colors.length];
    return <path key={i} d={pathData} fill={color} stroke="#fff" strokeWidth={1} />;
  });

  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} role="img" aria-label="Pie chart">
      <circle cx={center.x} cy={center.y} r={radius} fill="#f3f4f6" />
      {slices}
    </svg>
  );
}

function ScatterPlot({
  data,
  width = 520,
  height = 360,
  pointRadius = 3,
}: {
  data: { x: number; y: number; label?: string }[];
  width?: number;
  height?: number;
  pointRadius?: number;
}) {
  const margin = { top: 10, right: 10, bottom: 36, left: 56 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const xs = data.map((d) => d.x);
  const ys = data.map((d) => d.y);
  const xMin = Math.min(...xs);
  const xMax = Math.max(...xs);
  const yMin = Math.min(...ys);
  const yMax = Math.max(...ys);
  const xSpan = xMax - xMin || 1;
  const ySpan = yMax - yMin || 1;
  const xScale = (x: number) => margin.left + ((x - xMin) / xSpan) * innerW;
  const yScale = (y: number) => margin.top + innerH - ((y - yMin) / ySpan) * innerH;

  // Helpers for ticks/labels
  const niceStep = (span: number, target: number) => {
    const rough = span / Math.max(1, target);
    const pow10 = Math.pow(10, Math.floor(Math.log10(rough)));
    const steps = [1, 2, 2.5, 5, 10].map((m) => m * pow10);
    // pick step that makes <= target ticks
    const step = steps.find((s) => span / s <= target) || steps[steps.length - 1];
    return step;
  };
  const makeTicks = (min: number, max: number, count = 5) => {
    const step = niceStep(max - min, count);
    const start = Math.ceil(min / step) * step;
    const ticks: number[] = [];
    for (let v = start; v <= max + 1e-9; v += step) ticks.push(Number(v.toFixed(12)));
    if (ticks.length === 0) ticks.push(min, max);
    return ticks;
  };
  const fmtX = (n: number) => new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(n);
  const fmtY = (n: number) => {
    const abs = Math.abs(n);
    if (abs >= 1e12) return (n / 1e12).toFixed(1).replace(/\.0$/, "") + "T";
    if (abs >= 1e9) return (n / 1e9).toFixed(1).replace(/\.0$/, "") + "B";
    if (abs >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
    if (abs >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, "") + "K";
    return String(Math.round(n));
  };

  const xTicks = makeTicks(xMin, xMax, 6);
  const yTicks = makeTicks(yMin, yMax, 6);

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Scatter plot">
      <rect x={0} y={0} width={width} height={height} fill="#ffffff" stroke="#e5e7eb" />
      {/* Axes */}
      <line x1={margin.left} y1={margin.top} x2={margin.left} y2={margin.top + innerH} stroke="#9ca3af" />
      <line x1={margin.left} y1={margin.top + innerH} x2={margin.left + innerW} y2={margin.top + innerH} stroke="#9ca3af" />
      {/* Grid + tick labels */}
      {yTicks.map((t, i) => (
        <g key={`yt-${i}`}>
          <line x1={margin.left} y1={yScale(t)} x2={margin.left + innerW} y2={yScale(t)} stroke="#f3f4f6" />
          <text x={margin.left - 8} y={yScale(t)} textAnchor="end" dominantBaseline="middle" fontSize={11} fill="#6b7280">
            {fmtY(t)}
          </text>
        </g>
      ))}
      {xTicks.map((t, i) => (
        <g key={`xt-${i}`}>
          <line x1={xScale(t)} y1={margin.top + innerH} x2={xScale(t)} y2={margin.top} stroke="#f9fafb" />
          <text x={xScale(t)} y={margin.top + innerH + 16} textAnchor="middle" fontSize={11} fill="#6b7280">
            {fmtX(t)}
          </text>
        </g>
      ))}
      {/* Points */}
      {data.map((d, i) => (
        <circle key={i} cx={xScale(d.x)} cy={yScale(d.y)} r={pointRadius} fill="#2563eb" opacity={0.75} />
      ))}
    </svg>
  );
}

function Histogram({
  data,
  width = 560,
  height = 360,
  barGap = 2,
}: {
  data: { bin: number; n: number }[];
  width?: number;
  height?: number;
  barGap?: number;
}) {
  const margin = { top: 10, right: 10, bottom: 48, left: 56 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const counts = data.map((d) => d.n);
  const nMax = Math.max(1, ...counts);
  const barW = data.length > 0 ? Math.max(2, Math.floor(innerW / data.length)) : 10;
  const fmtN = (n: number) => new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(n);
  const niceStep = (span: number, target: number) => {
    const rough = span / Math.max(1, target);
    const pow10 = Math.pow(10, Math.floor(Math.log10(rough)));
    const steps = [1, 2, 2.5, 5, 10].map((m) => m * pow10);
    return steps.find((s) => span / s <= target) || steps[steps.length - 1];
  };
  const makeTicks = (min: number, max: number, count = 5) => {
    const step = niceStep(max - min, count);
    const start = Math.ceil(min / step) * step;
    const ticks: number[] = [];
    for (let v = start; v <= max + 1e-9; v += step) ticks.push(Number(v.toFixed(12)));
    if (ticks.length === 0) ticks.push(min, max);
    return ticks;
  };
  const yTicks = makeTicks(0, nMax, 5);

  const bars = data.map((d, i) => {
    const h = Math.round((d.n / nMax) * innerH);
    const x = margin.left + i * barW;
    const y = margin.top + (innerH - h);
    return <rect key={i} x={x} y={y} width={Math.max(1, barW - barGap)} height={h} fill="#2563eb" opacity={0.8} />;
  });

  // Choose up to ~10 x labels to avoid clutter
  const xStep = Math.max(1, Math.ceil(data.length / 10));

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Histogram">
      <rect x={0} y={0} width={width} height={height} fill="#ffffff" stroke="#e5e7eb" />
      {/* Axes */}
      <line x1={margin.left} y1={margin.top} x2={margin.left} y2={margin.top + innerH} stroke="#9ca3af" />
      <line x1={margin.left} y1={margin.top + innerH} x2={margin.left + innerW} y2={margin.top + innerH} stroke="#9ca3af" />
      {/* Grid + Y tick labels */}
      {yTicks.map((t, i) => (
        <g key={`yt-h-${i}`}>
          <line x1={margin.left} y1={margin.top + innerH - (t / nMax) * innerH} x2={margin.left + innerW} y2={margin.top + innerH - (t / nMax) * innerH} stroke="#f3f4f6" />
          <text x={margin.left - 8} y={margin.top + innerH - (t / nMax) * innerH} textAnchor="end" dominantBaseline="middle" fontSize={11} fill="#6b7280">
            {fmtN(t)}
          </text>
        </g>
      ))}
      {/* Bars */}
      {bars}
      {/* X labels */}
      {data.map((d, i) => (
        i % xStep === 0 ? (
          <text key={`xl-${i}`} x={margin.left + i * barW + barW / 2} y={margin.top + innerH + 16} textAnchor="middle" fontSize={11} fill="#6b7280">
            {fmtN(d.bin)}
          </text>
        ) : null
      ))}
    </svg>
  );
}

export default function Page() {
  const [question, setQuestion] = React.useState("");
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [resp, setResp] = React.useState<AskResponse | null>(null);

  const onAsk = async () => {
    setLoading(true);
    setError(null);
    setResp(null);
    try {
      const r = await fetch("http://localhost:5000/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question }),
      });
      const j: AskResponse = await r.json();
      if (!r.ok) {
        throw new Error(j?.message || j?.error || `HTTP ${r.status}`);
      }
      setResp(j);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  };

  const insufficient = (resp?.answer || "").toLowerCase() === "insufficient data";
  const isPie = !!resp?.pie_chart && !!resp?.result?.rows?.length && !insufficient;
  const isScatter = !!resp?.scatter_plot && !!resp?.result?.rows?.length && !insufficient;
  const isHistogram = !!resp?.histogram && !!resp?.result?.rows?.length && !insufficient;
  const pieData = isPie
    ? resp!.result!.rows
        .map((r) => ({ label: String(r.label ?? ""), pct: Number(r.pct ?? 0) }))
        .filter((d) => d.label && isFinite(d.pct) && d.pct > 0)
    : [];
  const scatterData = isScatter
    ? resp!.result!.rows
        .map((r) => ({ x: Number((r as any).x), y: Number((r as any).y), label: (r as any).label }))
        .filter((d) => isFinite(d.x) && isFinite(d.y))
    : [];
  const histData = isHistogram
    ? resp!.result!.rows
        .map((r) => ({ bin: Number((r as any).bin), n: Number((r as any).n) }))
        .filter((d) => isFinite(d.bin) && isFinite(d.n) && d.n > 0)
    : [];

  return (
    <div style={{ maxWidth: 900, margin: "24px auto", padding: 16 }}>
      <h1 style={{ margin: 0, marginBottom: 8 }}>Prompt Visualizer</h1>
      <p style={{ marginTop: 0, color: "#6b7280" }}>Ask questions about the SaaS dataset.</p>

      <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") onAsk();
          }}
          placeholder='e.g. "Create a pie chart representing industry breakdown"'
          style={{ flex: 1, padding: "10px 12px", fontSize: 16, border: "1px solid #d1d5db", borderRadius: 6 }}
        />
        <button
          onClick={onAsk}
          disabled={loading || !question.trim()}
          style={{
            padding: "10px 16px",
            fontSize: 16,
            borderRadius: 6,
            border: "1px solid #1f2937",
            background: loading ? "#9ca3af" : "#111827",
            color: "white",
            cursor: loading ? "default" : "pointer",
          }}
        >
          {loading ? "Asking…" : "Ask"}
        </button>
      </div>

      {error && (
        <div style={{ marginTop: 12, color: "#b91c1c" }}>Error: {error}</div>
      )}

      {resp && (
        <div style={{ display: "grid", gridTemplateColumns: isPie ? "360px 1fr" : (isScatter || isHistogram) ? "560px 1fr" : "1fr", gap: 16, marginTop: 20 }}>
          {isPie && (
            <div>
              <PieChart data={pieData} size={320} />
              <div style={{ marginTop: 8 }}>
                {pieData.map((d, i) => (
                  <div key={i} style={{ fontSize: 14, color: "#374151" }}>
                    {d.label}: {d.pct}%
                  </div>
                ))}
              </div>
            </div>
          )}
          {isScatter && (
            <div>
              <ScatterPlot data={scatterData} width={520} height={360} />
              <div style={{ marginTop: 8, fontSize: 12, color: "#6b7280" }}>
                Points: {scatterData.length} (x vs y)
              </div>
            </div>
          )}
          {isHistogram && (
            <div>
              <Histogram data={histData} width={560} height={360} />
              <div style={{ marginTop: 8, fontSize: 12, color: "#6b7280" }}>
                Bins: {histData.length}
              </div>
            </div>
          )}

          <div>
            <div style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace", fontSize: 13, whiteSpace: "pre-wrap", background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 6, padding: 12 }}>
              <strong>Question:</strong> {resp.question}
              {resp.answer && (
                <>
                  {"\n"}
                  <strong>Answer:</strong> {resp.answer}
                </>
              )}
              {resp.sql && (
                <>
                  {"\n"}
                  <strong>SQL:</strong> {resp.sql}
                </>
              )}
              {resp.source && (
                <>
                  {"\n"}
                  <strong>Source:</strong> {resp.source}
                </>
              )}
              {isPie && resp.expected_columns && (
                <>
                  {"\n"}
                  <strong>Expected columns:</strong> {resp.expected_columns.join(", ")}
                </>
              )}
            </div>

            {insufficient && (
              <div style={{ marginTop: 12, padding: 12, border: "1px solid #fde68a", background: "#fffbeb", color: "#92400e", borderRadius: 6 }}>
                insufficient data
              </div>
            )}

            {resp.result && !insufficient && (
              <div style={{ marginTop: 12 }}>
                <div style={{ fontWeight: 600, marginBottom: 6 }}>Rows ({resp.result.rowcount}):</div>
                <div style={{ overflowX: "auto" }}>
                  <table style={{ borderCollapse: "collapse", width: "100%" }}>
                    <thead>
                      <tr>
                        {resp.result.columns.map((c) => (
                          <th key={c} style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb", padding: "6px 8px", fontSize: 13 }}>
                            {c}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {resp.result.rows.map((r, i) => (
                        <tr key={i}>
                          {resp.result!.columns.map((c) => (
                            <td key={c} style={{ borderBottom: "1px solid #f3f4f6", padding: "6px 8px", fontSize: 13 }}>
                              {String((r as any)[c] ?? "")}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
