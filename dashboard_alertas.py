from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import json
import os
from collections import defaultdict

ALERTAS_FILE = os.getenv("ALERTAS_FILE", "/shared/alertas.json")

app = Dash(__name__)

app.layout = html.Div([
    html.H2("⚠️ Alertas de Calidad del Aire (Kafka + Plotly)"),

    # Panel donde mostramos la última alerta
    html.Div(id="panel-alerta", style={
        "padding": "10px",
        "margin": "10px 0",
        "border": "2px solid #cc0000",
        "borderRadius": "6px",
        "backgroundColor": "#ffeeee",
        "fontSize": "18px",
        "fontWeight": "bold"
    }),

    dcc.Graph(id="grafico-alertas"),

    # 🔥 NUEVA SECCIÓN: Selector de zona
    html.H3("🔍 Filtrar alertas por zona"),
    dcc.Dropdown(
        id="selector-zona",
        options=[],
        placeholder="Selecciona una zona...",
        style={"width": "300px", "marginBottom": "20px"}
    ),

    html.Div(id="panel-alertas-zona", style={
        "padding": "10px",
        "border": "1px solid #888",
        "borderRadius": "6px",
        "backgroundColor": "#f7f7f7",
        "minHeight": "120px",
        "fontSize": "16px"
    }),

    # Actualizar cada 2 segundos
    dcc.Interval(id="interval", interval=2000, n_intervals=0)
])

@app.callback(
    [
        Output("grafico-alertas", "figure"),
        Output("panel-alerta", "children"),
        Output("selector-zona", "options"),
        Output("panel-alertas-zona", "children")
    ],
    [
        Input("interval", "n_intervals"),
        Input("selector-zona", "value")
    ]
)
def actualizar_ui(n, zona_seleccionada):
    # ---------------------------------------------------
    # LEER alertas.json
    # ---------------------------------------------------
    try:
        with open(ALERTAS_FILE, "r") as f:
            alertas_buffer = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        alertas_buffer = []

    if len(alertas_buffer) == 0:
        fig = go.Figure()
        return fig, "Sin alertas todavía", [], "Sin datos"

    # ---------------------------------------------------
    # AGRUPAR POR ZONA PARA EL GRÁFICO
    # ---------------------------------------------------
    zonas = defaultdict(lambda: {"x": [], "y": [], "raw": []})

    for a in alertas_buffer:
        zona = a.get("estacion") or a.get("nombre", "desconocido")
        zonas[zona]["x"].append(a["fecha_carg"])
        zonas[zona]["y"].append(a["nivel_no2"])
        zonas[zona]["raw"].append(a)

    # ---------------------------------------------------
    # FIGURA PRINCIPAL
    # ---------------------------------------------------
    fig = go.Figure()

    for zona, datos in zonas.items():
        fig.add_trace(go.Scatter(
            x=datos["x"],
            y=datos["y"],
            mode="lines+markers",
            name=zona,
            hovertemplate=(
                "Zona: " + zona +
                "<br>NO₂: %{y} µg/m³<br>Fecha: %{x}"
            )
        ))

    fig.update_layout(
        title="Niveles de NO₂ por zonas",
        xaxis_title="Tiempo",
        yaxis_title="µg/m³ (NO₂)",
        legend_title="Estaciones"
    )

    # ---------------------------------------------------
    # PANEL ÚLTIMA ALERTA
    # ---------------------------------------------------
    ultima = alertas_buffer[-1]  # la última
    texto_alerta = (
        f"🚨 {ultima.get('tipo_aviso', 'Alerta')} | "
        f"Zona: {ultima.get('estacion', ultima.get('nombre'))} | "
        f"NO₂: {ultima.get('nivel_no2')} µg/m³ | "
        f"{ultima.get('texto', '')}"
    )

    # ---------------------------------------------------
    # OPCIONES DEL DROPDOWN
    # ---------------------------------------------------
    opciones_dropdown = [{"label": z, "value": z} for z in zonas.keys()]

    # ---------------------------------------------------
    # PANEL DE ALERTAS POR ZONA
    # ---------------------------------------------------
    if zona_seleccionada and zona_seleccionada in zonas:
        alertas_zona = zonas[zona_seleccionada]["raw"]

        lista = [
            html.Div([
                html.B(f"{a.get('tipo_aviso', 'Alerta')} — {a.get('nivel_no2', '?')} µg/m³"),
                html.Div(f"Fecha: {a.get('fecha_carg', 'desconocida')}"),
                html.Em(a.get("texto", "")),
                html.Hr()
            ]) for a in reversed(alertas_zona)
        ]
        panel_zona = lista
    else:
        panel_zona = "Selecciona una zona para ver sus alertas."

    return fig, texto_alerta, opciones_dropdown, panel_zona


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8050)
