import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery

# ==========================================
# 1. Configurações da Página e Tema Netflix
# ==========================================
st.set_page_config(
    page_title="Netflix Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilização personalizada em Dark Theme
st.markdown("""
<style>
    .main {
        background-color: #141414;
    }
    .metric-card {
        background: #1f1f1f;
        padding: 18px;
        border-radius: 10px;
        border-left: 5px solid #3B82F6;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    .metric-title {
        color: #999999;
        font-size: 0.9rem;
        margin-bottom: 5px;
    }
    .metric-value {
        color: #FFFFFF;
        font-size: 1.8rem;
        font-weight: 700;
    }
    h1, h2, h3 {
        color: #FFFFFF !important;
    }
</style>
""", unsafe_allow_html=True)

# Carrega .env se existir (para execução local fora do Docker)
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("GCP_DATASET_GOLD", "netflix_analitical")

# ==========================================
# 2. Conexão com o Google BigQuery (com Cache)
# ==========================================
@st.cache_resource
def get_bigquery_client():
    # Localiza as credenciais automaticamente no ambiente local ou container
    cred_file = os.getenv("GCP_CREDENTIALS_FILE", "google_credentials.json")
    possible_paths = [
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        os.path.join(os.path.dirname(__file__), "config", cred_file),
        f"/app/config/{cred_file}",
        f"/opt/airflow/config/{cred_file}",
    ]
    for path in possible_paths:
        if path and os.path.exists(path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
            break

    return bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=300)
def load_data(query: str) -> pd.DataFrame:
    client = get_bigquery_client()
    return client.query(query).to_dataframe()

# ==========================================
# 3. Carregamento dos Dados Analíticos
# ==========================================
try:
    df_top_movies = load_data(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.vw_top_movies`")
    df_movies_kpis = load_data(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.vw_movies_kpis`")
    df_genres = load_data(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.vw_genre_performance` ORDER BY total_ratings DESC")
    df_heatmap = load_data(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.vw_ratings_heatmap` ORDER BY year, month_number")
    df_users = load_data(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.vw_user_summary`")
except Exception as e:
    st.error(f"Erro ao conectar com o BigQuery: {e}")
    st.info("Verifique se o arquivo de credenciais da GCP está na pasta config/ e se as views foram criadas no BigQuery.")
    st.stop()

# ==========================================
# 4. Barra Lateral - Filtros
# ==========================================
st.sidebar.title("Filtros")

# Lista de gêneros únicos
all_genres = sorted([g for g in df_genres["genre"].dropna().unique() if g])
selected_genres = st.sidebar.multiselect("Gênero", options=all_genres, default=[])

# Filtro de Ano
min_year = int(df_movies_kpis["release_year"].dropna().min()) if not df_movies_kpis["release_year"].dropna().empty else 1950
max_year = int(df_movies_kpis["release_year"].dropna().max()) if not df_movies_kpis["release_year"].dropna().empty else 2026
selected_years = st.sidebar.slider("Ano de Lançamento", min_value=min_year, max_value=max_year, value=(min_year, max_year))

# Filtro de Mínimo de Avaliações
min_ratings_filter = st.sidebar.slider("Mínimo de Avaliações por Filme", min_value=1, max_value=100, value=10)

# Aplicar filtros ao dataset principal de filmes
df_filtered_movies = df_movies_kpis.copy()
df_filtered_movies = df_filtered_movies[
    (df_filtered_movies["release_year"] >= selected_years[0]) &
    (df_filtered_movies["release_year"] <= selected_years[1]) &
    (df_filtered_movies["total_ratings"] >= min_ratings_filter)
]

if selected_genres:
    pattern = "|".join(selected_genres)
    df_filtered_movies = df_filtered_movies[df_filtered_movies["genres"].str.contains(pattern, na=False)]

# ==========================================
# 5. Cabeçalho e KPIs Principais
# ==========================================
st.title("Netflix Data Lakehouse & Analytics")
st.caption("Visão analítica gerada via Pipeline ELT (PySpark + Google Cloud Storage + BigQuery)")

col1, col2, col3, col4 = st.columns(4)

total_movies = len(df_movies_kpis)
total_ratings = int(df_users["total_ratings"].sum()) if not df_users.empty else 0
total_active_users = len(df_users)

# Calcula a média apenas de filmes que possuem avaliações significativas (ex: total_ratings > 0)
valid_movies = df_movies_kpis[df_movies_kpis["total_ratings"] > 5]
if not valid_movies.empty:
    avg_rating_global = (valid_movies["avg_rating"] * valid_movies["total_ratings"]).sum() / valid_movies["total_ratings"].sum()
else:
    avg_rating_global = df_movies_kpis["avg_rating"].mean()

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total de Filmes</div>
        <div class="metric-value">{total_movies:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total de Avaliações</div>
        <div class="metric-value">{total_ratings:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Nota Média Global</div>
        <div class="metric-value"> {avg_rating_global:.2f} / 5.0</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Usuários Ativos</div>
        <div class="metric-value"> {total_active_users:,}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ==========================================
# 6. Abas Analíticas
# ==========================================
tab_movies, tab_genres, tab_trends, tab_users = st.tabs([
    "Top Filmes & Qualidade",
    "Performance por Gênero",
    "Sazonalidade de Avaliações",
    "Perfil de Usuários"
])

# --- ABA 1: Filmes ---
with tab_movies:
    c1, c2 = st.columns([1, 1])
    
    with c1:
        st.subheader("Top 15 Filmes Mais Bem Avaliados")
        top_filtered = df_filtered_movies.sort_values(by=["avg_rating", "total_ratings"], ascending=[False, False]).head(15)
        
        if not top_filtered.empty:
            fig_top = px.bar(
                top_filtered,
                x="avg_rating",
                y="title",
                orientation="h",
                text="avg_rating",
                color="avg_rating",
                color_continuous_scale="Blues",
                hover_data=["genres", "total_ratings", "release_year"],
                labels={"avg_rating": "Nota Média", "title": "Filme"}
            )
            fig_top.update_layout(yaxis=dict(autorange="reversed"), template="plotly_dark", height=450)
            fig_top.update_traces(texttemplate="%{text:.2f}", textposition="outside")
            st.plotly_chart(fig_top, use_container_width=True)
        else:
            st.warning("Nenhum filme corresponde aos filtros selecionados.")

    with c2:
        st.subheader("Popularidade vs. Qualidade")
        if not df_filtered_movies.empty:
            fig_scatter = px.scatter(
                df_filtered_movies.head(300),
                x="total_ratings",
                y="avg_rating",
                size="total_ratings",
                color="avg_rating",
                hover_name="title",
                color_continuous_scale="Blues",
                labels={"total_ratings": "Volume de Avaliações (Popularidade)", "avg_rating": "Nota Média (Qualidade)"},
                template="plotly_dark",
                height=450
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.warning("Sem dados para exibir o gráfico.")

    st.subheader("Explorador de Dados")
    st.dataframe(
        df_filtered_movies[["movie_id", "title", "genres", "release_year", "total_ratings", "avg_rating"]].rename(
            columns={
                "movie_id": "ID",
                "title": "Título",
                "genres": "Gêneros",
                "release_year": "Ano",
                "total_ratings": "Avaliações",
                "avg_rating": "Nota Média"
            }
        ),
        use_container_width=True,
        height=300
    )

# --- ABA 2: Gêneros ---
with tab_genres:
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Total de Avaliações por Gênero")
        fig_genre_vol = px.bar(
            df_genres.head(15),
            x="total_ratings",
            y="genre",
            orientation="h",
            color="total_ratings",
            color_continuous_scale="Blues",
            template="plotly_dark",
            height=450,
            labels={"total_ratings": "Total de Avaliações", "genre": "Gênero"}
        )
        fig_genre_vol.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_genre_vol, use_container_width=True)

    with c2:
        st.subheader("Média de Nota por Gênero")
        df_genres_sorted_avg = df_genres.sort_values(by="avg_rating", ascending=False).head(15)
        fig_genre_avg = px.bar(
            df_genres_sorted_avg,
            x="avg_rating",
            y="genre",
            orientation="h",
            color="avg_rating",
            color_continuous_scale="Greys",
            template="plotly_dark",
            height=450,
            labels={"avg_rating": "Nota Média", "genre": "Gênero"}
        )
        fig_genre_avg.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_genre_avg, use_container_width=True)

# --- ABA 3: Sazonalidade ---
with tab_trends:
    st.subheader("Distribuição Mensal das Avaliações ao Longo dos Anos")
    if not df_heatmap.empty:
        fig_time = px.line(
            df_heatmap,
            x="month_number",
            y="total_ratings",
            color="year",
            markers=True,
            template="plotly_dark",
            labels={"month_number": "Mês", "total_ratings": "Volume de Avaliações", "year": "Ano"},
            height=450
        )
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("Sem dados de histórico de temporalidade.")

# --- ABA 4: Usuários ---
with tab_users:
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Distribuição de Notas dos Usuários")
        fig_user_dist = px.histogram(
            df_users,
            x="avg_rating",
            nbins=30,
            color_discrete_sequence=["#3B82F6"],
            template="plotly_dark",
            labels={"avg_rating": "Nota Média dada pelo Usuário"},
            height=400
        )
        st.plotly_chart(fig_user_dist, use_container_width=True)

    with c2:
        st.subheader("Top 10 Usuários Mais Críticos/Ativos")
        top_active_users = df_users.sort_values(by="total_ratings", ascending=False).head(10)
        st.dataframe(
            top_active_users[["user_id", "total_ratings", "distinct_movies_rated", "avg_rating"]].rename(
                columns={
                    "user_id": "Usuário ID",
                    "total_ratings": "Qtd Avaliações",
                    "distinct_movies_rated": "Filmes Distintos",
                    "avg_rating": "Nota Média Atribuída"
                }
            ),
            use_container_width=True,
            height=380
        )
