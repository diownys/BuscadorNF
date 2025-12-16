import streamlit as st
import requests
import io
import time
import concurrent.futures
from pypdf import PdfWriter, PdfReader
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="PharmUP Turbo Pro", layout="wide", page_icon="🛡️")

# --- CREDENCIAIS ---
PHARMUP_USER = st.secrets["PHARMUP_USER"]
PHARMUP_PASS = st.secrets["PHARMUP_PASS"]
API_BASE = "https://pharmup-industria-api.azurewebsites.net"

# --- STATES (Memória da Sessão) ---
if "dados_processados" not in st.session_state:
    st.session_state.dados_processados = None
if "buffers_normais" not in st.session_state:
    st.session_state.buffers_normais = []
if "buffers_simples" not in st.session_state:
    st.session_state.buffers_simples = []
if "log_data" not in st.session_state:
    st.session_state.log_data = []

# --- BACKEND ROBUSTO ---

def get_session():
    """Cria sessão com tentativas automáticas para falhas de rede"""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def api_login(session):
    try:
        url = f"{API_BASE}/Login"
        params = {"login": PHARMUP_USER, "senha": PHARMUP_PASS}
        response = session.post(url, params=params, timeout=20)
        if response.status_code == 200:
            return response.json().get("token")
        return None
    except:
        return None

def api_search_and_get_links(token, codigo_venda):
    """Worker com lógica de persistência (Retry)"""
    session = get_session()
    resultado = {
        "venda": codigo_venda,
        "pdf_normal": None,
        "pdf_simplificado": None,
        "status": "erro",
        "msg": ""
    }
    
    headers = {"Authorization": f"Bearer {token}", "PharmUpSession": token}
    
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            url_search = f"{API_BASE}/NotaFiscalSaida/List"
            params = {
                "filterKey": codigo_venda,
                "sortKey": "numero",
                "sortOrder": "desc",
                "pageIndex": 1,
                "pageSize": 50
            }
            
            res = session.get(url_search, headers=headers, params=params, timeout=25)
            
            if res.status_code != 200:
                time.sleep(2 * (attempt + 1))
                continue

            data = res.json()
            
            item_alvo = None
            if data and "list" in data and len(data["list"]) > 0:
                for item in data["list"]:
                    venda_api = str(item.get("codigoVenda", "")).strip()
                    nota_api = str(item.get("numero", "")).strip()
                    alvo = str(codigo_venda).strip()
                    
                    if (venda_api == alvo or nota_api == alvo) and item.get("tipo") == 2:
                        item_alvo = item
                        break
                
                if not item_alvo:
                    for item in data["list"]:
                        venda_api = str(item.get("codigoVenda", "")).strip()
                        if venda_api == str(codigo_venda).strip():
                            item_alvo = item
                            break
            
            if not item_alvo:
                if attempt < max_attempts - 1:
                    time.sleep(1)
                    continue
                else:
                    resultado["msg"] = "Venda não encontrada ou sem Nota Fiscal"
                    break 

            if item_alvo:
                link_normal = item_alvo.get("pdfLink")
                if link_normal:
                    try:
                        r_norm = session.get(link_normal, headers=headers, timeout=30)
                        if r_norm.status_code == 200 and r_norm.content.startswith(b'%PDF'):
                            resultado["pdf_normal"] = r_norm.content
                    except: pass 

                link_simples = item_alvo.get("pdfSimplificadoLink")
                if not link_simples: link_simples = item_alvo.get("pdfLink")
                
                if link_simples:
                    try:
                        r_simp = session.get(link_simples, headers=headers, timeout=30)
                        if r_simp.status_code == 200 and r_simp.content.startswith(b'%PDF'):
                            resultado["pdf_simplificado"] = r_simp.content
                    except: pass

                if resultado["pdf_normal"] or resultado["pdf_simplificado"]:
                    resultado["status"] = "sucesso"
                    resultado["nota"] = item_alvo.get("numero")
                    resultado["cliente"] = item_alvo.get("clienteNome")
                    break 
                else:
                    if attempt < max_attempts - 1:
                        time.sleep(1)
                        continue
                    resultado["msg"] = "Erro ao baixar PDF"
            
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep(2)
                continue
            resultado["msg"] = f"Erro conexão: {str(e)}"

    return resultado

def merge_pdfs(pdf_list):
    writer = PdfWriter()
    for pdf_data in pdf_list:
        try:
            reader = PdfReader(io.BytesIO(pdf_data))
            for page in reader.pages:
                writer.add_page(page)
        except: continue
    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    return output

# --- INTERFACE ---

st.title("🛡️ PharmUP Turbo Pro")
st.markdown(f"**Usuário:** {PHARMUP_USER} | **Status:** Sistema Blindado (Ordem Preservada)")

col_input, col_status = st.columns([1, 2])

with col_input:
    vendas_input = st.text_area("Cole os Códigos (Um por linha):", height=300, placeholder="114316\n112433")
    btn_process = st.button("🚀 Processar Vendas", type="primary", use_container_width=True)

if btn_process:
    if not vendas_input.strip():
        st.warning("Lista vazia.")
    else:
        # Limpa dados antigos
        st.session_state.buffers_normais = []
        st.session_state.buffers_simples = []
        st.session_state.log_data = []
        st.session_state.dados_processados = True

        codigos = [v.strip() for v in vendas_input.split('\n') if v.strip()]
        total_codigos = len(codigos)
        
        with col_status:
            status_bar = st.progress(0)
            status_text = st.empty()
            
            sessao_login = get_session()
            token = api_login(sessao_login)
            
            if not token:
                st.error("Erro fatal: Não foi possível logar.")
            else:
                status_text.info(f"Processando {total_codigos} vendas com segurança...")
                
                # Dicionário para armazenar resultados por índice para manter a ordem original
                resultados_ordenados = {}
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    # Mapeia cada busca ao seu índice original (posição na lista)
                    future_to_index = {executor.submit(api_search_and_get_links, token, cod): i for i, cod in enumerate(codigos)}
                    completed = 0
                    
                    for future in concurrent.futures.as_completed(future_to_index):
                        index_original = future_to_index[future]
                        data = future.result()
                        
                        # Armazena o resultado na posição correta
                        resultados_ordenados[index_original] = data
                        
                        completed += 1
                        status_bar.progress(completed / total_codigos)
                        if data['status'] == 'sucesso':
                             status_text.text(f"✅ {data['venda']} - OK")
                        else:
                             status_text.text(f"⚠️ {data['venda']} - {data['msg']}")

                # --- RECONSTRUÇÃO DA LISTA NA ORDEM ORIGINAL ---
                for i in range(total_codigos):
                    res = resultados_ordenados[i]
                    
                    # Salva logs (na ordem correta)
                    log_entry = {
                        "Venda": res["venda"],
                        "Nota": res.get("nota") if res["status"] == "sucesso" else "-",
                        "Cliente": res.get("cliente") if res["status"] == "sucesso" else res.get("msg"),
                        "Status": "✅ Sucesso" if res["status"] == "sucesso" else "❌ Falha"
                    }
                    st.session_state.log_data.append(log_entry)
                    
                    # Adiciona aos buffers seguindo a ordem da lista de entrada
                    if res["status"] == "sucesso":
                        if res["pdf_normal"]: st.session_state.buffers_normais.append(res["pdf_normal"])
                        if res["pdf_simplificado"]: st.session_state.buffers_simples.append(res["pdf_simplificado"])
                
                status_text.success("Processamento finalizado!")
                status_bar.empty()

# --- EXIBIÇÃO DE RESULTADOS E DOWNLOADS ---
if st.session_state.dados_processados:
    with col_status:
        st.divider()
        
        st.write("#### 📊 Relatório Geral")
        st.dataframe(st.session_state.log_data, use_container_width=True, height=250)
        
        lista_pendencias = [d["Venda"] for d in st.session_state.log_data if "Falha" in d["Status"]]
        
        if lista_pendencias:
            st.error(f"⚠️ **{len(lista_pendencias)} Vendas sem Nota Fiscal encontrada:**")
            st.caption("Passe o mouse no canto direito da caixa abaixo para copiar a lista para o Teams 👇")
            
            texto_copiar = "\n".join(lista_pendencias)
            st.code(texto_copiar, language="text")
        else:
            st.success("✅ Todas as notas foram encontradas! Nada para enviar ao financeiro.")

        st.divider()
        st.write("#### 📥 Baixar Arquivos (Ordem da Lista Original)")
        c1, c2 = st.columns(2)
        
        if st.session_state.buffers_normais:
            merged = merge_pdfs(st.session_state.buffers_normais)
            c1.download_button("📄 Baixar Notas (A4)", merged, f"Notas_{datetime.now().strftime('%H%M')}.pdf", "application/pdf", type="primary", use_container_width=True)
        else:
            c1.warning("Sem notas A4.")
            
        if st.session_state.buffers_simples:
            merged_s = merge_pdfs(st.session_state.buffers_simples)
            c2.download_button("🏷️ Baixar Etiquetas", merged_s, f"Etiquetas_{datetime.now().strftime('%H%M')}.pdf", "application/pdf", type="secondary", use_container_width=True)
        else:
            c2.warning("Sem etiquetas.")