import streamlit as st
import pandas as pd
import pyodbc
import math
import io
import datetime

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    page_title="Cierres de Caja Diarios - Equipark",
    page_icon="💰",
    layout="wide"
)
# ---------------------------------------------------------
# 🔒
# ---------------------------------------------------------
def check_password():
    """Retorna `True` si el usuario tiene la contraseña correcta."""

    def password_entered():
        """Verifica si la contraseña ingresada coincide con la de secrets."""
        if st.session_state["password"] == st.secrets["general"]["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  
        else:
            st.session_state["password_correct"] = False

    # 1. Si ya está autenticado, retornar True
    if st.session_state.get("password_correct", False):
        return True

    # 2. Interfaz de Login
    # Usamos columnas para centrar el cuadro de login
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("<h2 style='text-align: center;'>🔐 Acceso Restringido</h2>", unsafe_allow_html=True)
        st.info("Por favor ingresa la contraseña administrativa para acceder al Dashboard de Recaudo.")
        
        st.text_input(
            "Contraseña", 
            type="password", 
            on_change=password_entered, 
            key="password"
        )
        
        if "password_correct" in st.session_state:
            st.error("😕 Contraseña incorrecta")

    return False

if not check_password():
    st.stop()  

# CSS para alineación visual de los controles
st.markdown("""
    <style>
    div[data-testid="column"] {
        display: flex;
        align-items: center;
    }
    div[data-testid="column"]:nth-child(5) > div {
         align-items: end;
    }
    </style>
""", unsafe_allow_html=True)

# --- CONEXIÓN SQL ---
@st.cache_resource
def init_connection():
    try:
        creds = st.secrets["sqlserver"]
        connection_string = f"""
            DRIVER={creds['DRIVER']};
            SERVER={creds['SERVER']};
            DATABASE={creds['DATABASE']};
            UID={creds['UID']};
            PWD={creds['PWD']};
            TrustServerCertificate=yes;
        """
        return pyodbc.connect(connection_string)
    except Exception as e:
        st.error(f"Error conectando a la base de datos: {e}")
        st.stop()

conn = init_connection()

# --- OBTENER OPCIONES DE FILTRO ---
@st.cache_data(ttl=5600)
def get_filter_options():
    query = "SELECT DISTINCT [REGIONAL], [ESTACIONAMIENTO] FROM recaudo.vw_consolidado_cierres_adjuntos ORDER BY [REGIONAL], [ESTACIONAMIENTO]"
    df = pd.read_sql(query, conn)
    return df

# --- CONSTRUCTOR DE WHERE ---
def build_where_clause(regional_filter, estacionamiento_filter, date_range):
    where_clauses = []
    params = []

    if regional_filter:
        placeholders = ','.join(['?'] * len(regional_filter))
        where_clauses.append(f"[REGIONAL] IN ({placeholders})")
        params.extend(regional_filter)

    if estacionamiento_filter:
        placeholders = ','.join(['?'] * len(estacionamiento_filter))
        where_clauses.append(f"[ESTACIONAMIENTO] IN ({placeholders})")
        params.extend(estacionamiento_filter)

    if date_range:
        if len(date_range) == 2:
            start_date, end_date = date_range
            where_clauses.append("[FECHA] BETWEEN ? AND ?")
            params.extend([start_date, end_date])
        elif len(date_range) == 1:
            single_date = date_range[0]
            where_clauses.append("[FECHA] = ?")
            params.append(single_date)

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return where_sql, params

# --- CONSULTAS SQL ---

def get_data_paginated(regional_filter, estacionamiento_filter, date_range, page_number, page_size):
    where_sql, params = build_where_clause(regional_filter, estacionamiento_filter, date_range)

    # 1. Contar total
    count_query = f"SELECT COUNT(*) FROM recaudo.vw_consolidado_cierres_adjuntos{where_sql}"
    try:
        cursor = conn.cursor()
        cursor.execute(count_query, params)
        total_rows = cursor.fetchval()
    except Exception as e:
        st.error(f"Error al contar registros: {e}")
        return pd.DataFrame(), 0

    # 2. Traer página
    offset = (page_number - 1) * page_size
    data_query = f"""
        SELECT *
        FROM recaudo.vw_consolidado_cierres_adjuntos
        {where_sql}
        ORDER BY [FECHA] DESC, [ID CIERRE] DESC
        OFFSET {offset} ROWS
        FETCH NEXT {page_size} ROWS ONLY;
    """

    try:
        df = pd.read_sql(data_query, conn, params=params)
        return df, total_rows
    except Exception as e:
        st.error(f"Error consulta datos: {e}")
        return pd.DataFrame(), 0

@st.cache_data(ttl=600)
def get_kpi_metrics(regional_filter, estacionamiento_filter, date_range):
    where_sql, params = build_where_clause(regional_filter, estacionamiento_filter, date_range)

    query = f"""
        SELECT 
            ISNULL(SUM([TOTAL RECAUDADO DIA]), 0) as TotalRecaudo,
            ISNULL(SUM([TOTAL ENTRADAS]), 0) as TotalVehiculos,
            ISNULL(SUM([VALOR CONSIGNADO]), 0) as TotalConsignado,
            ISNULL(SUM([DIFERENCIA VALOR RECAUDO VS CONSIGNACION]), 0) as Diferencia,
            ISNULL(SUM([TOTAL RECAUDO MANUAL]), 0) as RecaudoManual
        FROM recaudo.vw_consolidado_cierres_adjuntos
        {where_sql}
    """
    try:
        df = pd.read_sql(query, conn, params=params)
        return df.iloc[0]
    except Exception as e:
        st.error(f"Error calculando métricas: {e}")
        return None

@st.cache_data(ttl=600, show_spinner="Generando Excel...")
def convert_df_to_excel(regional_filter, estacionamiento_filter, date_range):
    where_sql, params = build_where_clause(regional_filter, estacionamiento_filter, date_range)
    
    full_query = f"""
        SELECT *
        FROM recaudo.vw_consolidado_cierres_adjuntos
        {where_sql}
        ORDER BY [FECHA] DESC, [ID CIERRE] DESC
    """
    try:
        df_all = pd.read_sql(full_query, conn, params=params)
    except Exception as e:
        st.error(f"Error al exportar: {e}")
        return None

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_all.to_excel(writer, index=False, sheet_name='Reporte Consolidado')
        worksheet = writer.sheets['Reporte Consolidado']
        for i, col in enumerate(df_all.columns):
            worksheet.set_column(i, i, 22) 
    return output.getvalue()

# --- INTERFAZ DE USUARIO ---

st.title("Reporte Diario de Operaciones - Equipark")

if 'page_number' not in st.session_state: st.session_state.page_number = 1
if 'page_size_val' not in st.session_state: st.session_state.page_size_val = 20

# 1. FILTROS
with st.container():
    st.subheader("Filtros de Búsqueda")
    df_options = get_filter_options()
    
    c1, c2, c3 = st.columns(3)

    with c1:
        all_regionals = sorted(df_options['REGIONAL'].unique().tolist())
        selected_regionals = st.multiselect("📍 Regional:", all_regionals)

    with c2:
        if selected_regionals:
            filtered_estacionamientos = df_options[df_options['REGIONAL'].isin(selected_regionals)]['ESTACIONAMIENTO'].unique().tolist()
        else:
            filtered_estacionamientos = sorted(df_options['ESTACIONAMIENTO'].unique().tolist()) 
        selected_estacionamientos = st.multiselect("🅿️ Estacionamiento:", filtered_estacionamientos)

    with c3:
        today = datetime.date.today()
        default_start = today - datetime.timedelta(days=30)
        selected_date_range = st.date_input("📅 Fecha de Cierre:", value=(default_start, today), max_value=today)
        if not isinstance(selected_date_range, tuple): selected_date_range = (selected_date_range,)

st.divider()

# 2. LÓGICA DE CONTROL
date_hash = str(selected_date_range)
filters_hash = hash(tuple(selected_regionals) + tuple(selected_estacionamientos) + tuple(date_hash))

if 'last_filters_hash' not in st.session_state or st.session_state.last_filters_hash != filters_hash:
    st.session_state.page_number = 1
    st.session_state.last_filters_hash = filters_hash

current_page_size = st.session_state.page_size_val

# 3. KPI (ARRIBA)
kpi_data = get_kpi_metrics(selected_regionals, selected_estacionamientos, selected_date_range)
if kpi_data is not None:
    with st.container():
        st.subheader("📈 Resumen Operativo")
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("💰 Total Recaudado", f"${kpi_data['TotalRecaudo']:,.0f}")
        k2.metric("🚗 Ingresos Vehiculares Totales", f"{int(kpi_data['TotalVehiculos']):,}")
        k3.metric("🏦 Consignado", f"${kpi_data['TotalConsignado']:,.0f}")
        k4.metric("⚠️ Diferencia", f"${kpi_data['Diferencia']:,.0f}", delta=float(kpi_data['Diferencia']), delta_color="normal")
        pct_manual = 0
        if kpi_data['TotalRecaudo'] > 0: pct_manual = (kpi_data['RecaudoManual'] / kpi_data['TotalRecaudo']) * 100
        k5.metric("🖐️ Recaudo Manual", f"${kpi_data['RecaudoManual']:,.0f}", delta=f"{pct_manual:.1f}%", delta_color="off")
    st.divider()

# 4. TABLA Y DATOS
with st.spinner('Cargando datos de la vista...'):
    df_results, total_rows = get_data_paginated(
        selected_regionals, 
        selected_estacionamientos,
        selected_date_range,
        st.session_state.page_number, 
        current_page_size
    )

total_pages = math.ceil(total_rows / current_page_size) if total_rows > 0 else 1

col_pg, col_prev, col_next, col_down, col_info = st.columns([1, 1, 1, 1.5, 2])
with col_pg:
    st.selectbox("Filas", options=[10, 20, 50, 100], key='page_size_val', label_visibility="collapsed")
with col_prev:
    if st.button("⬅️", disabled=(st.session_state.page_number <= 1), use_container_width=True):
        st.session_state.page_number -= 1
        st.rerun()
with col_next:
    if st.button("➡️", disabled=(st.session_state.page_number >= total_pages), use_container_width=True):
        st.session_state.page_number += 1
        st.rerun()
with col_down:
    if total_rows > 0:
        excel_data = convert_df_to_excel(selected_regionals, selected_estacionamientos, selected_date_range)
        if excel_data:
            st.download_button("📥 Excel Completo", excel_data, "reporte_consolidado.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
with col_info:
    st.markdown(f"<div style='text-align: right; font-size: 0.9em;'>Pág <b>{st.session_state.page_number}</b> de <b>{total_pages}</b> (Total: {total_rows:,})</div>", unsafe_allow_html=True)

st.write("") 

# --- SECCIÓN INFERIOR: TABLA DE RESULTADOS ---
if total_rows > 0:
    
    # 1. RENOMBRADO INTELIGENTE DE COLUMNAS
    # Esto soluciona el problema de que los nombres no coincidan (mayúsculas/minúsculas/typos)
    # Buscamos la columna que contenga ciertas palabras y la estandarizamos.
    rename_map = {}
    for col in df_results.columns:
        col_lower = col.lower()
        # Detectar Consignación (incluso con el error 'Consignaicon')
        if 'url' in col_lower and 'consigna' in col_lower:
            rename_map[col] = "URL_CONSIGNACION"
        # Detectar Cierre Sistema
        elif 'url' in col_lower and 'cierre' in col_lower:
            rename_map[col] = "URL_CIERRE"
        # Detectar Formulario
        elif 'url' in col_lower and 'formulario' in col_lower:
            rename_map[col] = "URL_FORMULARIO"
        # Detectar Otros
        elif 'url' in col_lower and 'otros' in col_lower:
            rename_map[col] = "URL_OTROS"
            
    if rename_map:
        df_results = df_results.rename(columns=rename_map)

    # 2. LIMPIEZA Y PREPARACIÓN
    # Reemplazamos vacíos por None para que no salgan enlaces rotos
    df_results = df_results.replace('', None)
    
    # Creamos la columna auxiliar con el texto del botón
    df_results['texto_boton'] = "📂 Abrir adjunto"

    # 3. CONFIGURACIÓN DEL ENLACE
    # Le decimos a Streamlit: "Usa la columna 'texto_boton' como etiqueta visible"
    link_config = st.column_config.LinkColumn(
        "Soporte", 
        display_text="Ver", 
        help="Clic para ver el documento",
        width="small"
    )
    

    
    st.dataframe(
        df_results,
        use_container_width=True,
        hide_index=True,
        height=600,
        column_config={
            # Columnas estándar
            "ID CIERRE": st.column_config.TextColumn("ID", width="small"),
            "FECHA": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
            
            # --- APLICAMOS LA CONFIGURACIÓN A LOS NOMBRES ESTANDARIZADOS ---
            "URL_CONSIGNACION": st.column_config.LinkColumn(
                    "Soporte Consignacion", 
                    display_text="Ver", 
                    help="Clic para ver el documento",
                    width="medium"
                ),
            "URL_CIERRE": st.column_config.LinkColumn(
        "Soporte Cierre", 
        display_text="Ver", 
        help="Clic para ver el documento",
        width="medium"
    ),
            "URL_FORMULARIO": st.column_config.LinkColumn(
        "Soporte Formulario", 
        display_text="Ver", 
        help="Clic para ver el documento",
        width="medium"
    ),
            "URL_OTROS": st.column_config.LinkColumn(
        "Soporte Otros Medios", 
        display_text="Ver", 
        help="Clic para ver el documento",
        width="medium"
    ),
            
            # --- OCULTAMOS LAS COLUMNAS AUXILIARES (Usando None) ---
            "texto_boton": None,
            
            # Si quieres ocultar las columnas originales de nombres de archivo si existen:
            "nombre_consignacion": None,
            "nombre_cierre_sistema": None, 
            # (Agrega aquí cualquier otra columna que quieras esconder poniendo: "Nombre": None)

            # Formatos de dinero (Asegúrate que coincidan con tus nombres de columna de dinero)
            "TOTAL RECAUDADO DIA": st.column_config.NumberColumn("Total Día", format="$%.2f"),
            "VALOR CONSIGNADO": st.column_config.NumberColumn("Consignado", format="$%.2f"),
        }
    )
else:
    st.warning("No se encontraron registros con los filtros aplicados.")