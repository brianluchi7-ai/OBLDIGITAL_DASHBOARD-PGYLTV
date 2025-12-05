import re
import pandas as pd
import dash
from dash import html, dcc, Input, Output, dash_table
import plotly.express as px
from conexion_mysql import crear_conexion

# ======================================================
# === OBL DIGITAL DASHBOARD — GENERAL LTV (Dark Gold, + Filtro SOURCE)
# ======================================================

def cargar_datos():
    """Carga datos desde MySQL o CSV local."""
    try:
        conexion = crear_conexion()
        if conexion:
            print("✅ Leyendo GENERAL_LTV_PGY_CLEAN desde Railway MySQL...")
            query = "SELECT * FROM GENERAL_LTV_PGY_CLEAN"
            df = pd.read_sql(query, conexion)
            conexion.close()
            return df
    except Exception as e:
        print(f"⚠️ Error conectando a SQL, leyendo CSV local: {e}")

    print("📁 Leyendo GENERAL_LTV_preview.csv (local)...")
    return pd.read_csv("GENERAL_LTV_preview.csv", dtype=str)


# === 1️⃣ Cargar datos ===
df = cargar_datos()
df.columns = [c.strip().lower() for c in df.columns]

# === 2️⃣ Normalizar columnas ===
if "usd_total" not in df.columns:
    for alt in ["usd", "total_amount"]:
        if alt in df.columns:
            df.rename(columns={alt: "usd_total"}, inplace=True)
            break

if "count_ftd" not in df.columns:
    for alt in ["ftd", "ftds", "count"]:
        if alt in df.columns:
            df.rename(columns={alt: "count_ftd"}, inplace=True)
            break

# === 3️⃣ Normalizar fechas ===
def convertir_fecha(valor):
    try:
        s = str(valor).strip()
        if "/" in s:
            return pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
        elif "-" in s:
            return pd.to_datetime(s.split(" ")[0], errors="coerce")
    except Exception:
        return pd.NaT
    return pd.NaT

df["date"] = df["date"].astype(str).str.strip().apply(convertir_fecha)
df = df[df["date"].notna()]
df["date"] = pd.to_datetime(df["date"], utc=False).dt.tz_localize(None)

# === 4️⃣ Limpieza de montos ===
def limpiar_usd(valor):
    if pd.isna(valor):
        return 0.0
    s = str(valor).strip()
    s = re.sub(r"[^\d,.\-]", "", s)
    if "." in s and "," in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        partes = s.split(",")
        s = s.replace(",", ".") if len(partes[-1]) == 2 else s.replace(",", "")
    elif s.count(".") > 1:
        s = s.replace(".", "")
    try:
        return float(s)
    except:
        return 0.0

df["usd_total"] = df["usd_total"].apply(limpiar_usd)
df["count_ftd"] = pd.to_numeric(df.get("count_ftd", 0), errors="coerce").fillna(0).astype(float)
df["general_ltv"] = pd.to_numeric(df.get("general_ltv", 0), errors="coerce").fillna(0.0)

# === 5️⃣ Limpieza de texto ===
for col in ["country", "affiliate", "source"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title()
        df[col].replace({"Nan": None, "None": None, "": None}, inplace=True)

# === 6️⃣ Eliminar duplicados exactos ===
df = df.drop_duplicates(subset=["date", "country", "affiliate", "source"], keep="last")

fecha_min, fecha_max = df["date"].min(), df["date"].max()

# === 7️⃣ Formato K/M ===
def formato_km(valor):
    if valor >= 1_000_000:
        return f"{valor/1_000_000:.2f}M"
    elif valor >= 1_000:
        return f"{valor/1_000:.1f}K"
    else:
        return f"{valor:.0f}"


# === 8️⃣ Inicializar app ===
app = dash.Dash(__name__)
server = app.server
app.title = "OBL Digital — GENERAL LTV Dashboard"

# === 9️⃣ Layout (agregamos filtro de Source) ===
app.layout = html.Div(
    style={
        "backgroundColor": "#0d0d0d",
        "color": "#000000",
        "fontFamily": "Arial",
        "padding": "20px",
    },
    children=[
        html.H1("📊 DASHBOARD GENERAL LTV", style={
            "textAlign": "center",
            "color": "#D4AF37",
            "marginBottom": "30px",
            "fontWeight": "bold"
        }),

        html.Div(
            style={"display": "flex", "justifyContent": "space-between"},
            children=[
                # --- Panel de Filtros ---
                html.Div(
                    style={
                        "width": "25%",
                        "backgroundColor": "#1a1a1a",
                        "padding": "20px",
                        "borderRadius": "12px",
                        "boxShadow": "0 0 15px rgba(212,175,55,0.3)",
                        "textAlign": "center",
                    },
                    children=[
                        html.H4("Date", style={"color": "#D4AF37"}),
                        dcc.DatePickerRange(
                            id="filtro-fecha",
                            start_date=fecha_min,
                            end_date=fecha_max,
                            display_format="YYYY-MM-DD",
                            style={"marginBottom": "25px"},
                        ),

                        html.H4("Affiliate", style={"color": "#D4AF37"}),
                        dcc.Dropdown(
                            sorted(df["affiliate"].dropna().unique()),
                            [],
                            multi=True,
                            id="filtro-affiliate",
                            style={"marginBottom": "20px"},
                        ),

                        html.H4("Source", style={"color": "#D4AF37"}),
                        dcc.Dropdown(
                            sorted(df["source"].dropna().unique()),
                            [],
                            multi=True,
                            id="filtro-source",
                            style={"marginBottom": "20px"},
                        ),

                        html.H4("Country", style={"color": "#D4AF37"}),
                        dcc.Dropdown(
                            sorted(df["country"].dropna().unique()),
                            [],
                            multi=True,
                            id="filtro-country",
                        ),
                    ],
                ),

                # --- Panel de contenido ---
                html.Div(
                    style={"width": "72%"},
                    children=[
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-around"},
                            children=[
                                html.Div(id="indicador-ftds", style={"width": "30%"}),
                                html.Div(id="indicador-amount", style={"width": "30%"}),
                                html.Div(id="indicador-ltv", style={"width": "30%"}),
                            ],
                        ),
                        html.Br(),
                        html.Div(
                            style={"display": "flex", "flexWrap": "wrap", "gap": "20px"},
                            children=[
                                dcc.Graph(id="grafico-ltv-affiliate", style={"width": "48%", "height": "340px"}),
                                dcc.Graph(id="grafico-ltv-country", style={"width": "48%", "height": "340px"}),
                                dcc.Graph(id="grafico-bar-country-aff", style={"width": "100%", "height": "360px"}),
                            ],
                        ),
                        html.Br(),
                        html.H4("📋 Detalle General LTV", style={"color": "#D4AF37"}),
                        dash_table.DataTable(
                            id="tabla-detalle",
                            columns=[
                                {"name": "DATE", "id": "date"},
                                {"name": "COUNTRY", "id": "country"},
                                {"name": "AFFILIATE", "id": "affiliate"},
                                {"name": "SOURCE", "id": "source"},
                                {"name": "TOTAL AMOUNT", "id": "usd_total"},
                                {"name": "FTD'S", "id": "count_ftd"},
                                {"name": "GENERAL LTV", "id": "general_ltv"},
                            ],
                            style_table={"overflowX": "auto", "backgroundColor": "#0d0d0d"},
                            page_size=15,
                            style_cell={"textAlign": "center", "color": "#f2f2f2", "backgroundColor": "#1a1a1a"},
                            style_header={"backgroundColor": "#D4AF37", "color": "#000", "fontWeight": "bold"},
                        ),
                    ],
                ),
            ],
        ),
    ],
)

# === 🔟 Callback (se agrega parámetro filtro-source) ===
@app.callback(
    [
        Output("indicador-ftds", "children"),
        Output("indicador-amount", "children"),
        Output("indicador-ltv", "children"),
        Output("grafico-ltv-affiliate", "figure"),
        Output("grafico-ltv-country", "figure"),
        Output("grafico-bar-country-aff", "figure"),
        Output("tabla-detalle", "data"),
    ],
    [
        Input("filtro-fecha", "start_date"),
        Input("filtro-fecha", "end_date"),
        Input("filtro-affiliate", "value"),
        Input("filtro-source", "value"),
        Input("filtro-country", "value"),
    ],
)
def actualizar_dashboard(start, end, affiliates, sources, countries):
    df_filtrado = df.copy()

    if start and end:
        start_dt, end_dt = pd.to_datetime(start), pd.to_datetime(end)
        df_filtrado = df_filtrado[(df_filtrado["date"] >= start_dt) & (df_filtrado["date"] <= end_dt)]
    if affiliates:
        df_filtrado = df_filtrado[df_filtrado["affiliate"].isin(affiliates)]
    if sources:
        df_filtrado = df_filtrado[df_filtrado["source"].isin(sources)]
    if countries:
        df_filtrado = df_filtrado[df_filtrado["country"].isin(countries)]

    # === Agrupar por fecha, país, afiliado y source ===
    df_agregado = (
        df_filtrado.groupby(["date", "country", "affiliate", "source"], as_index=False)
        .agg({"usd_total": "sum", "count_ftd": "sum"})
    )
    df_agregado["general_ltv"] = df_agregado.apply(
        lambda r: r["usd_total"] / r["count_ftd"] if r["count_ftd"] > 0 else 0, axis=1
    )

    # === Totales ===
    total_amount = df_agregado["usd_total"].sum()
    total_ftds = df_agregado["count_ftd"].sum()
    general_ltv_total = total_amount / total_ftds if total_ftds > 0 else 0

    card_style = {
        "backgroundColor": "#1a1a1a",
        "borderRadius": "10px",
        "padding": "20px",
        "width": "80%",
        "textAlign": "center",
        "boxShadow": "0 0 10px rgba(212,175,55,0.3)",
    }

    indicador_ftds = html.Div([
        html.H4("FTD'S", style={"color": "#D4AF37", "fontWeight": "bold"}),
        html.H2(f"{int(total_ftds):,}", style={"color": "#FFFFFF", "fontSize": "36px"})
    ], style=card_style)

    indicador_amount = html.Div([
        html.H4("TOTAL AMOUNT", style={"color": "#D4AF37", "fontWeight": "bold"}),
        html.H2(f"${formato_km(total_amount)}", style={"color": "#FFFFFF", "fontSize": "36px"})
    ], style=card_style)

    indicador_ltv = html.Div([
        html.H4("GENERAL LTV (AMOUNT / FTD'S)", style={"color": "#D4AF37", "fontWeight": "bold"}),
        html.H2(f"${general_ltv_total:,.2f}", style={"color": "#FFFFFF", "fontSize": "36px"})
    ], style=card_style)

    # === Gráficos ===
    df_aff = df_agregado.groupby("affiliate", as_index=False).agg({"usd_total": "sum", "count_ftd": "sum"})
    df_aff["general_ltv"] = df_aff.apply(lambda r: r["usd_total"] / r["count_ftd"] if r["count_ftd"] > 0 else 0, axis=1)
    fig_affiliate = px.pie(df_aff, names="affiliate", values="general_ltv",
                           title="GENERAL LTV by Affiliate", color_discrete_sequence=px.colors.sequential.YlOrBr)

    df_cty = df_agregado.groupby("country", as_index=False).agg({"usd_total": "sum", "count_ftd": "sum"})
    df_cty["general_ltv"] = df_cty.apply(lambda r: r["usd_total"] / r["count_ftd"] if r["count_ftd"] > 0 else 0, axis=1)
    fig_country = px.pie(df_cty, names="country", values="general_ltv",
                         title="GENERAL LTV by Country", color_discrete_sequence=px.colors.sequential.YlOrBr)

    df_bar = df_agregado.groupby(["country", "affiliate"], as_index=False).agg({"usd_total": "sum", "count_ftd": "sum"})
    df_bar["general_ltv"] = df_bar.apply(lambda r: r["usd_total"] / r["count_ftd"] if r["count_ftd"] > 0 else 0, axis=1)
    fig_bar = px.bar(df_bar, x="country", y="general_ltv", color="affiliate",
                     title="GENERAL LTV by Country and Affiliate", barmode="group",
                     color_discrete_sequence=px.colors.sequential.YlOrBr)

    for fig in [fig_affiliate, fig_country, fig_bar]:
        fig.update_layout(paper_bgcolor="#0d0d0d", plot_bgcolor="#0d0d0d",
                          font_color="#f2f2f2", title_font_color="#D4AF37")

    # === Tabla ===
    tabla = df_agregado.copy()
    tabla["date"] = tabla["date"].dt.strftime("%Y-%m-%d")
    tabla_data = tabla.round(2).to_dict("records")

    return indicador_ftds, indicador_amount, indicador_ltv, fig_affiliate, fig_country, fig_bar, tabla_data


# === 9️⃣ Captura PDF/PPT desde iframe ===
app.index_string = '''
<!DOCTYPE html>
<html>
<head>
  {%metas%}
  <title>OBL Digital — Dashboard FTD</title>
  {%favicon%}
  {%css%}
  <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
</head>
<body>
  {%app_entry%}
  <footer>
    {%config%}
    {%scripts%}
    {%renderer%}
  </footer>

  <script>
    window.addEventListener("message", async (event) => {
      if (!event.data || event.data.action !== "capture_dashboard") return;

      try {
        const canvas = await html2canvas(document.body, { useCORS: true, scale: 2, backgroundColor: "#0d0d0d" });
        const imgData = canvas.toDataURL("image/png");

        window.parent.postMessage({
          action: "capture_image",
          img: imgData,
          filetype: event.data.type
        }, "*");
      } catch (err) {
        console.error("Error al capturar dashboard:", err);
        window.parent.postMessage({ action: "capture_done" }, "*");
      }
    });
  </script>
</body>
</html>
'''


if __name__ == "__main__":
    app.run_server(debug=True, port=8053)



