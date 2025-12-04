from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import json
import os
from confluent_kafka import Consumer
import threading

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
alertas_recibidas = []
datos_contaminantes = {}
lock = threading.Lock()

LIMITES = {
    "NO₂": 200,
    "O₃": 180,
    "PM10": 50,
    "PM2.5": 25
}

ZONAS_POR_CIUDAD = {
    "Madrid": [
        "Pza. de España", "Escuelas Aguirre", "Avda. Ramón y Cajal",
        "Arturo Soria", "Villaverde Alto", "Farolillo", "Casa de Campo",
        "Barajas Pueblo", "Pza. del Carmen", "Moratalaz", "Cuatro Caminos",
        "Barrio del Pilar", "Vallecas", "Méndez Álvaro", "Castellana",
        "Retiro", "Pza. Castilla", "Ensanche de Vallecas",
        "Urbanización Embajada", "Pza. Elíptica", "Sanchinarro",
        "El Pardo", "Parque Juan Carlos I", "Tres Olivos"
    ],
    "Valencia": [
        "Olivereta", "Dr. Lluch", "Centro", "Universidad Politécnica",
        "Molí del Sol", "Cabanyal", "Patraix", "Francia", "Viveros",
        "Pista de Silla", "Boulevar Sur"
    ]
}

# =============================================================================
# KAFKA CONSUMER
# =============================================================================
def kafka_consumer_thread():
    global alertas_recibidas, datos_contaminantes
    
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092'),
        'group.id': 'grupo_dashboard_v3',
        'auto.offset.reset': 'latest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['alertas_poblacion'])
    print(f"🔌 Consumer Kafka conectado")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            try:
                datos = json.loads(msg.value().decode('utf-8'))
                estacion = datos.get("estacion")
                estado = datos.get("alerta_activa")
                fecha_carg = datos.get("fecha_carg", "")
                
                with lock:
                    datos_contaminantes[estacion] = {
                        "no2": datos.get("nivel_no2", 0) or 0,
                        "o3": datos.get("nivel_o3", 0) or 0,
                        "pm10": datos.get("nivel_pm10", 0) or 0,
                        "pm25": datos.get("nivel_pm25", 0) or 0,
                        "fecha": fecha_carg
                    }
                
                if estado == True:
                    print(f"🚨 ALERTA: {estacion}")
                    with lock:
                        alertas_recibidas.append({
                            "estacion": estacion,
                            "nivel_no2": datos.get("nivel_no2"),
                            "fecha_carg": fecha_carg
                        })
            except:
                pass
    except:
        pass
    finally:
        consumer.close()

# =============================================================================
# FUNCIONES GRÁFICOS
# =============================================================================
def crear_grafico_radar(datos):
    if not datos:
        fig = go.Figure()
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=False), angularaxis=dict(visible=False)),
            showlegend=False, height=300, margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            annotations=[dict(text="Sin datos", x=0.5, y=0.5, showarrow=False, font=dict(size=16, color="#999"))]
        )
        return fig
    
    categorias = ["NO₂", "O₃", "PM10", "PM2.5"]
    valores_raw = [datos.get("no2", 0), datos.get("o3", 0), datos.get("pm10", 0), datos.get("pm25", 0)]
    valores_norm = [min((v / LIMITES[cat]) * 100, 150) if LIMITES[cat] > 0 else 0 for v, cat in zip(valores_raw, categorias)]
    
    categorias_closed = categorias + [categorias[0]]
    valores_closed = valores_norm + [valores_norm[0]]
    valores_raw_closed = valores_raw + [valores_raw[0]]
    
    max_pct = max(valores_norm) if valores_norm else 0
    if max_pct < 50:
        color_fill, color_line = "rgba(40, 167, 69, 0.3)", "rgb(40, 167, 69)"
    elif max_pct < 75:
        color_fill, color_line = "rgba(255, 193, 7, 0.3)", "rgb(255, 193, 7)"
    else:
        color_fill, color_line = "rgba(220, 53, 69, 0.3)", "rgb(220, 53, 69)"
    
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=[100] * 5, theta=categorias_closed, fill='toself',
        fillcolor='rgba(220, 53, 69, 0.1)', line=dict(color='rgba(220, 53, 69, 0.3)', dash='dash'),
        hoverinfo='skip'
    ))
    fig.add_trace(go.Scatterpolar(
        r=valores_closed, theta=categorias_closed, fill='toself',
        fillcolor=color_fill, line=dict(color=color_line, width=3),
        customdata=valores_raw_closed,
        hovertemplate='<b>%{theta}</b><br>%{customdata:.1f} µg/m³<extra></extra>'
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 150], tickvals=[0, 50, 100, 150],
                           ticktext=['0%', '50%', '100%', '150%'], tickfont=dict(size=9), gridcolor='rgba(0,0,0,0.1)'),
            angularaxis=dict(tickfont=dict(size=11), gridcolor='rgba(0,0,0,0.1)'),
            bgcolor='rgba(255,255,255,0.9)'
        ),
        showlegend=False, margin=dict(l=40, r=40, t=20, b=20), height=280, paper_bgcolor='rgba(0,0,0,0)'
    )
    return fig

def crear_grafico_barras(datos):
    if not datos:
        fig = go.Figure()
        fig.update_layout(
            showlegend=False, height=280, margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            annotations=[dict(text="Sin datos", x=0.5, y=0.5, showarrow=False, font=dict(size=16, color="#999"))]
        )
        return fig
    
    categorias = ["NO₂", "O₃", "PM10", "PM2.5"]
    valores_raw = [datos.get("no2", 0), datos.get("o3", 0), datos.get("pm10", 0), datos.get("pm25", 0)]
    limites = [LIMITES[cat] for cat in categorias]
    
    colores = []
    for v, lim in zip(valores_raw, limites):
        pct = (v / lim * 100) if lim > 0 else 0
        if pct < 50:
            colores.append("#28a745")
        elif pct < 100:
            colores.append("#ffc107")
        else:
            colores.append("#dc3545")
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=categorias, y=valores_raw, marker_color=colores, name='Actual',
        text=[f"{v:.1f}" for v in valores_raw], textposition='outside',
        hovertemplate='<b>%{x}</b><br>%{y:.1f} µg/m³<extra></extra>'
    ))
    fig.add_trace(go.Scatter(
        x=categorias, y=limites, mode='markers+lines', name='Límite OMS',
        line=dict(color='#dc3545', width=2, dash='dash'), marker=dict(size=6, symbol='diamond')
    ))
    fig.update_layout(
        yaxis=dict(title="µg/m³", gridcolor='rgba(0,0,0,0.1)'),
        showlegend=True, legend=dict(orientation="h", y=1.15, x=0.5, xanchor="center"),
        margin=dict(l=40, r=20, t=40, b=30), height=280,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(255,255,255,0.9)', bargap=0.3
    )
    return fig

# =============================================================================
# DASH APP - LAYOUT FIJO
# =============================================================================
app = Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    # Barra superior
    html.Div([
        html.Div([
            html.Label("🏙️ Ciudad:", style={"fontWeight": "bold", "marginRight": "10px", "color": "white"}),
            dcc.Dropdown(id="selector-ciudad", options=[{"label": c, "value": c} for c in ZONAS_POR_CIUDAD.keys()],
                        placeholder="Selecciona ciudad...", style={"width": "180px"})
        ], style={"display": "inline-block", "marginRight": "20px", "verticalAlign": "middle"}),
        html.Div([
            html.Label("📍 Zona:", style={"fontWeight": "bold", "marginRight": "10px", "color": "white"}),
            dcc.Dropdown(id="selector-barrio", options=[], placeholder="Selecciona zona...",
                        style={"width": "220px"}, disabled=True)
        ], style={"display": "inline-block", "verticalAlign": "middle"}),
    ], style={"padding": "15px 25px", "backgroundColor": "#2c3e50"}),

    # Contenido principal - ESTRUCTURA FIJA
    html.Div([
        # Mensaje inicial (se oculta cuando hay selección)
        html.Div(id="mensaje-inicial", children=[
            html.H1("🌍 Monitor de Calidad del Aire", style={"color": "#2c3e50", "marginTop": "100px"}),
            html.P("Selecciona tu ciudad y zona en el menú superior", style={"fontSize": "18px", "color": "#666"})
        ], style={"textAlign": "center"}),
        
        # Panel de datos (oculto inicialmente)
        html.Div(id="panel-datos", children=[
            # Semáforo
            html.Div(id="semaforo-container", style={"textAlign": "center"}),
            
            # Info zona
            html.Div(id="info-zona", style={"textAlign": "center", "marginBottom": "15px"}),
            
            # Contenedor de gráficos
            html.Div([
                html.H3("📊 Niveles de Contaminantes", style={"textAlign": "center", "color": "#2c3e50", "marginBottom": "15px"}),
                html.Div([
                    html.Div([
                        html.H4("Vista Radar", style={"textAlign": "center", "color": "#666", "marginBottom": "5px", "fontSize": "14px"}),
                        dcc.Graph(id="grafico-radar", config={'displayModeBar': False}, style={"height": "280px"})
                    ], style={"flex": "1", "minWidth": "320px"}),
                    html.Div([
                        html.H4("Vista Barras", style={"textAlign": "center", "color": "#666", "marginBottom": "5px", "fontSize": "14px"}),
                        dcc.Graph(id="grafico-barras", config={'displayModeBar': False}, style={"height": "280px"})
                    ], style={"flex": "1", "minWidth": "320px"}),
                ], style={"display": "flex", "flexWrap": "wrap", "justifyContent": "center", "gap": "10px"}),
                
                # Valores y timestamp
                html.Div(id="valores-texto", style={"textAlign": "center", "marginTop": "15px"}),
            ], style={
                "maxWidth": "850px", "margin": "15px auto", "padding": "20px",
                "backgroundColor": "white", "borderRadius": "10px", "boxShadow": "0 2px 8px rgba(0,0,0,0.1)"
            })
        ], style={"display": "none"})
    ], style={"padding": "15px"}),

    dcc.Interval(id="interval", interval=2000, n_intervals=0)
])

# =============================================================================
# CALLBACKS
# =============================================================================
@app.callback(
    [Output("selector-barrio", "options"), Output("selector-barrio", "disabled"), Output("selector-barrio", "value")],
    Input("selector-ciudad", "value")
)
def actualizar_barrios(ciudad):
    if not ciudad:
        return [], True, None
    return [{"label": b, "value": b} for b in sorted(ZONAS_POR_CIUDAD.get(ciudad, []))], False, None

@app.callback(
    [Output("mensaje-inicial", "style"), Output("panel-datos", "style")],
    [Input("selector-ciudad", "value"), Input("selector-barrio", "value")]
)
def toggle_paneles(ciudad, barrio):
    if ciudad and barrio:
        return {"display": "none"}, {"display": "block"}
    return {"textAlign": "center"}, {"display": "none"}

@app.callback(
    Output("semaforo-container", "children"),
    [Input("interval", "n_intervals"), Input("selector-barrio", "value")]
)
def actualizar_semaforo(n, barrio):
    if not barrio:
        return None
    
    with lock:
        alertas = [a for a in alertas_recibidas if a["estacion"] == barrio]
    
    if alertas:
        ultima = alertas[-1]
        return html.Div([
            html.Div("🚨", style={"fontSize": "60px"}),
            html.Div("¡ALERTA!", style={"fontSize": "20px", "fontWeight": "bold"}),
            html.Div(f"{ultima['nivel_no2']} µg/m³", style={"fontSize": "22px"})
        ], style={
            "width": "180px", "height": "180px", "borderRadius": "50%", "margin": "20px auto",
            "backgroundColor": "#dc3545", "display": "flex", "flexDirection": "column",
            "alignItems": "center", "justifyContent": "center", "color": "white",
            "boxShadow": "0 0 40px #dc3545"
        })
    else:
        return html.Div([
            html.Div("✅", style={"fontSize": "60px"}),
            html.Div("AIRE LIMPIO", style={"fontSize": "20px", "fontWeight": "bold"})
        ], style={
            "width": "180px", "height": "180px", "borderRadius": "50%", "margin": "20px auto",
            "backgroundColor": "#28a745", "display": "flex", "flexDirection": "column",
            "alignItems": "center", "justifyContent": "center", "color": "white",
            "boxShadow": "0 0 40px #28a745"
        })

@app.callback(
    Output("info-zona", "children"),
    [Input("selector-ciudad", "value"), Input("selector-barrio", "value")]
)
def actualizar_info(ciudad, barrio):
    if not ciudad or not barrio:
        return None
    return html.Div([
        html.Span(f"📍 {barrio}", style={"fontSize": "20px", "fontWeight": "bold"}),
        html.Span(f" — {ciudad}", style={"fontSize": "16px", "color": "#666"})
    ])

@app.callback(
    Output("grafico-radar", "figure"),
    [Input("interval", "n_intervals"), Input("selector-barrio", "value")]
)
def actualizar_radar(n, barrio):
    with lock:
        datos = datos_contaminantes.get(barrio, {}) if barrio else {}
    return crear_grafico_radar(datos)

@app.callback(
    Output("grafico-barras", "figure"),
    [Input("interval", "n_intervals"), Input("selector-barrio", "value")]
)
def actualizar_barras(n, barrio):
    with lock:
        datos = datos_contaminantes.get(barrio, {}) if barrio else {}
    return crear_grafico_barras(datos)

@app.callback(
    Output("valores-texto", "children"),
    [Input("interval", "n_intervals"), Input("selector-barrio", "value")]
)
def actualizar_valores(n, barrio):
    with lock:
        datos = datos_contaminantes.get(barrio, {}) if barrio else {}
    
    if not datos:
        return html.P("⏳ Esperando datos...", style={"color": "#999"})
    
    return html.Div([
        html.Div([
            html.Span(f"NO₂: {datos.get('no2', 0):.1f}", style={"margin": "3px 8px", "padding": "6px 10px", "backgroundColor": "#e3f2fd", "borderRadius": "4px", "fontSize": "13px"}),
            html.Span(f"O₃: {datos.get('o3', 0):.1f}", style={"margin": "3px 8px", "padding": "6px 10px", "backgroundColor": "#e8f5e9", "borderRadius": "4px", "fontSize": "13px"}),
            html.Span(f"PM10: {datos.get('pm10', 0):.1f}", style={"margin": "3px 8px", "padding": "6px 10px", "backgroundColor": "#fff3e0", "borderRadius": "4px", "fontSize": "13px"}),
            html.Span(f"PM2.5: {datos.get('pm25', 0):.1f}", style={"margin": "3px 8px", "padding": "6px 10px", "backgroundColor": "#fce4ec", "borderRadius": "4px", "fontSize": "13px"}),
        ], style={"display": "flex", "justifyContent": "center", "flexWrap": "wrap", "marginBottom": "8px"}),
        html.Div([
            html.Span("Actualizado: ", style={"color": "#999", "fontSize": "12px"}),
            html.Span(datos.get("fecha", ""), style={"fontSize": "12px"})
        ])
    ])


if __name__ == "__main__":
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    print("🚀 Dashboard en http://0.0.0.0:8050")
    app.run(debug=False, host='0.0.0.0', port=8050)