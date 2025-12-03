import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from bson import ObjectId
from geopy.geocoders import Nominatim
import certifi

st.set_page_config(
    page_title="Dinossauros NoSQL",
    page_icon="🦖",
    layout="wide"
)

# Título da aplicação
st.title("🦖 Dinossauros (MongoDB)")
st.markdown("Dashboard interativo sobre dinossauros utilizando dados de um banco NoSQL (MongoDB Atlas).")

# ==============================================================================
# CONEXÃO COM O MONGODB
# ==============================================================================
@st.cache_resource
def init_connection():
    load_dotenv(dotenv_path=".env")
    
    uri = os.getenv('MONGO_URI')
    db_name = os.getenv('DB_NAME')

    if not uri:
        st.error("A variável MONGO_URI não foi encontrada no .env")
        return None

    try:
        # --- AQUI ESTÁ A CORREÇÃO MÁGICA ---
        # O tlsCAFile força o uso dos certificados atualizados do pacote certifi
        ca = certifi.where()
        client = MongoClient(uri, tlsCAFile=ca)
        # ------------------------------------
        
        client.admin.command('ping')
        return client[db_name]
    except Exception as e:
        st.error(f"Erro na conexão com MongoDB: {e}")
        return None

db = init_connection()

# ==============================================================================
# FUNÇÕES DE BUSCA DE DADOS
# ==============================================================================

def get_dinosaur_names():
    if db is None: return []
    
    # Busca apenas os campos necessários e converte _id para string
    cursor = db.dinossauros.find({}, {"_id": 1, "nome_popular": 1}).sort("nome_popular", 1)
    
    lista_dinos = []
    for doc in cursor:
        lista_dinos.append({
            "id_dinossauro": str(doc["_id"]),
            "nome_popular": doc.get("nome_popular", "Desconhecido")
        })
    return lista_dinos

def get_dinosaur_by_id(dino_id_str):
    if db is None: return None

    try:
        oid = ObjectId(dino_id_str)
    except:
        st.error("ID de Dinossauro inválido.")
        return None

    pipeline = [
        { "$match": { "_id": oid } },

        {
            "$lookup": {
                "from": "tipos_alimentacao",
                "localField": "id_dieta",
                "foreignField": "_id",
                "as": "dieta_info"
            }
        },
        { "$unwind": { "path": "$dieta_info", "preserveNullAndEmptyArrays": True } },

        {
            "$lookup": {
                "from": "periodos_geologicos",
                "localField": "id_periodo",
                "foreignField": "_id",
                "as": "periodo_info"
            }
        },
        { "$unwind": { "path": "$periodo_info", "preserveNullAndEmptyArrays": True } },

        {
            "$lookup": {
                "from": "fosseis",
                "let": { "dinoId": "$_id" },
                "pipeline": [
                    { "$match": { "$expr": { "$eq": ["$id_dinossauro", "$$dinoId"] } } },
                    
                    {
                        "$lookup": {
                            "from": "localizacoes",
                            "localField": "id_localizacao_descoberta",
                            "foreignField": "_id",
                            "as": "loc"
                        }
                    },
                    { "$unwind": { "path": "$loc", "preserveNullAndEmptyArrays": True } },

                    {
                        "$lookup": {
                            "from": "descobridores",
                            "localField": "id_descobridor",
                            "foreignField": "_id",
                            "as": "desc"
                        }
                    },
                    { "$unwind": { "path": "$desc", "preserveNullAndEmptyArrays": True } },

                    {
                        "$lookup": {
                            "from": "museus",
                            "localField": "id_museu",
                            "foreignField": "_id",
                            "as": "mus"
                        }
                    },
                    { "$unwind": { "path": "$mus", "preserveNullAndEmptyArrays": True } },

                    {
                        "$lookup": {
                            "from": "ossos",
                            "localField": "_id",
                            "foreignField": "id_fossil",
                            "as": "lista_ossos_raw"
                        }
                    }
                ],
                "as": "lista_fosseis"
            }
        }
    ]

    resultado = list(db.dinossauros.aggregate(pipeline))

    if not resultado:
        return None

    data = resultado[0]

    # Mapeamento do JSON do Mongo para a estrutura exata que o Streamlit espera

    dinosaur_dict = {
        "id": str(data["_id"]),
        "nome_popular": data.get("nome_popular"),
        "nome_cientifico": data.get("nome_cientifico"),
        "significado_nome": data.get("significado_nome"),
        "altura_media_m": data.get("altura_media_m"),
        "comprimento_medio_m": data.get("comprimento_medio_m"),
        "peso_medio_kg": float(data.get("peso_medio_kg", 0)),
        "imagem": data.get("imagem"),
        
        # Dados das tabelas linkadas (usando .get com {} padrão para evitar erros)
        "nome_dieta": data.get("dieta_info", {}).get("nome_dieta", "Desconhecido"),
        "nome_periodo": data.get("periodo_info", {}).get("nome_periodo", "Desconhecido"),
        "ma_inicio": data.get("periodo_info", {}).get("ma_inicio"),
        "ma_fim": data.get("periodo_info", {}).get("ma_fim"),
        "clima": data.get("periodo_info", {}).get("clima"),
        
        "fossil": []
    }

    # Processa a lista de fósseis vinda do lookup
    for f in data.get("lista_fosseis", []):
        fossil_dict = {
            "codigo": f.get("codigo"),
            # Converte data para string se existir
            "data_descoberta": f.get("data_descoberta").strftime('%Y-%m-%d') if f.get("data_descoberta") else "N/A",
            "nome_descobridor": f.get("desc", {}).get("nome_descobridor", "Desconhecido"),
            "local_descoberta": {
                "cidade": f.get("loc", {}).get("cidade"),
                "estado": f.get("loc", {}).get("estado"),
                "pais": f.get("loc", {}).get("pais")
            },
            "museu": {
                "nome": f.get("mus", {}).get("nome_museu"),
                "cidade": f.get("mus", {}).get("cidade_museu"),
                "pais": f.get("mus", {}).get("pais_museu")
            },
            # Extrai apenas o nome da parte do osso da lista de objetos ossos
            "ossos": [o["nome_parte"] for o in f.get("lista_ossos_raw", [])]
        }
        dinosaur_dict["fossil"].append(fossil_dict)

    return dinosaur_dict

# ==============================================================================
# LÓGICA DA INTERFACE 
# ==============================================================================

def create_dinosaur_selector():
    st.sidebar.header("Seleção de Dinossauro")
    dinos = get_dinosaur_names()
    
    if not dinos:
        st.sidebar.warning("Nenhum dinossauro encontrado.")
        return None

    dino_dict = {d["nome_popular"]: d["id_dinossauro"] for d in dinos}
    sorted_names = sorted(dino_dict.keys())
    selected_name = st.sidebar.selectbox("Selecione um dinossauro", sorted_names)
    
    return dino_dict.get(selected_name)

def plot_peso_comparativo(dino):
    maior_peso = 80000
    df = pd.DataFrame({
        "Peso (kg)": [dino["peso_medio_kg"], maior_peso],
        "Categoria": [dino["nome_popular"], "Maior Peso Já Registrado"]
    })
    fig = px.bar(
        df, x="Categoria", y="Peso (kg)",
        color="Categoria", title="Peso Comparativo"
    )
    fig.update_layout(showlegend=False, height=300)
    return fig
    
def main():
    # Seleção de dinossauro
    dinosaur_id = create_dinosaur_selector()
    if not dinosaur_id:
        st.info("Conecte-se ao banco e selecione um dinossauro na barra lateral.")
        st.stop()
    
    # Busca dados no Mongo
    dinosaur = get_dinosaur_by_id(dinosaur_id)
    
    if not dinosaur:
        st.error("Erro ao carregar dados do dinossauro.")
        st.stop()

    # Detalhes do dinossauro selecionado
    col1, col2, col3 = st.columns(3)

    with col1:
        if dinosaur["imagem"]:
            st.image(dinosaur["imagem"], width=300, caption=dinosaur["nome_popular"])
        else:
            st.warning("Sem imagem disponível")

    with col2:
        st.header(f"{dinosaur['nome_popular']}")
        st.subheader(f"*{dinosaur['nome_cientifico']}*")
        st.markdown(f"**Significado do Nome:** {dinosaur['significado_nome']}")
        st.markdown(f"**Altura Média:** {dinosaur['altura_media_m']} m")
        st.markdown(f"**Comprimento Médio:** {dinosaur['comprimento_medio_m']} m")
        st.markdown(f"**Peso Médio:** {dinosaur['peso_medio_kg']} kg")

    with col3:
        if(dinosaur["nome_dieta"] == "Carnívoro"):
            st.header(f"**🥩 {dinosaur['nome_dieta']}**")
        elif(dinosaur["nome_dieta"] == "Herbívoro"):
            st.header(f"**🥬 {dinosaur['nome_dieta']}**")
        else:
            st.header(f"**🍽️ {dinosaur['nome_dieta']}**")
        st.plotly_chart(plot_peso_comparativo(dinosaur), use_container_width=True)
    
    # Criar abas
    tab1, tab2, tab3 = st.tabs([
        "Período Geológico", 
        "Fósseis e Descobertas",
        "Localização de Descoberta"])
    
    with tab1:
        st.subheader(f"**{dinosaur['nome_periodo']}**")
        st.markdown(f"**Início:** {dinosaur['ma_inicio']} Ma")
        st.markdown(f"**Fim:** {dinosaur['ma_fim']} Ma")
        st.markdown(f"**Clima:** {dinosaur['clima']}")

    with tab2:
        if not dinosaur["fossil"]:
            st.info("Nenhum fóssil encontrado para este dinossauro.")
        else:
            for fossil in dinosaur["fossil"]:
                with st.expander(f"Fóssil: {fossil['codigo']}"):
                    st.markdown(f"**Data de Descoberta:** {fossil['data_descoberta']}")
                    st.markdown(f"**Descobridor:** {fossil['nome_descobridor']}")
                    
                    local = fossil["local_descoberta"]
                    st.markdown(f"**Local:** {local['cidade']}, {local['estado']}, {local['pais']}")
                    
                    museu = fossil["museu"]
                    st.markdown(f"**Museu:** {museu['nome']} ({museu['cidade']}, {museu['pais']})")
                    
                    if fossil["ossos"]:
                        st.markdown("**Ossos encontrados:**")
                        for osso in fossil["ossos"]:
                            st.markdown(f"- {osso}")
                    else:
                        st.markdown("Nenhum osso registrado para este fóssil.")
    
    with tab3:
        if not dinosaur["fossil"]:
            st.info("Nenhum mapa disponível (sem fósseis).")
        else:
            fig = go.Figure()
            geolocator = Nominatim(user_agent="dino_app_mongo")

            found_location = False
            for fossil in dinosaur["fossil"]:
                local = fossil["local_descoberta"]
                # Validação simples para evitar chamadas vazias
                if local['cidade'] and local['pais']:
                    endereco = f"{local['cidade']}, {local['pais']}"
                    try:
                        location = geolocator.geocode(endereco, timeout=10)
                        if location:
                            found_location = True
                            lat, lon = location.latitude, location.longitude
                            fig.add_trace(go.Scattergeo(
                                lat=[lat],
                                lon=[lon],
                                text=[f"Fóssil {fossil['codigo']}"],
                                mode='markers+text',
                                marker=dict(size=10, color='red'),
                                textfont=dict(color="black"),
                                textposition="top center"
                            ))
                    except Exception as e:
                        print(f"Erro geo: {e}")
            
            if found_location:
                fig.update_geos(
                    projection_type="orthographic",
                    showcountries=True,
                    showland=True,
                    landcolor="rgb(243, 243, 243)",
                    oceancolor="rgb(204, 224, 255)",
                )
                fig.update_layout(height=500, margin={"r":0,"t":0,"l":0,"b":0})
                st.plotly_chart(fig)
            else:
                st.warning("Não foi possível gerar a localização no mapa.")

if __name__ == "__main__":
    main()