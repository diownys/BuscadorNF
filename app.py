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
                # FILTRAGEM RIGOROSA: Busca apenas "Nota Fiscal"
                for item in data["list"]:
                    venda_api = str(item.get("codigoVenda", "")).strip()
                    nota_api = str(item.get("numero", "")).strip()
                    tipo_desc = str(item.get("tipoDescricao", "")).strip()
                    alvo = str(codigo_venda).strip()
                    
                    # Critério: O código deve bater E o tipo deve ser "Nota Fiscal"
                    if (venda_api == alvo or nota_api == alvo) and tipo_desc == "Nota Fiscal":
                        item_alvo = item
                        break
            
            if not item_alvo:
                if attempt < max_attempts - 1:
                    time.sleep(1)
                    continue
                else:
                    resultado["msg"] = "NF-e não encontrada (Apenas Notas de Serviço detectadas ou inexistente)"
                    break 

            if item_alvo:
                # Download dos PDFs (Normal e Simplificado)
                for key_link, res_key in [("pdfLink", "pdf_normal"), ("pdfSimplificadoLink", "pdf_simplificado")]:
                    link = item_alvo.get(key_link)
                    if link:
                        try:
                            r_pdf = session.get(link, headers=headers, timeout=30)
                            if r_pdf.status_code == 200 and r_pdf.content.startswith(b'%PDF'):
                                resultado[res_key] = r_pdf.content
                        except: pass

                if resultado["pdf_normal"] or resultado["pdf_simplificado"]:
                    resultado["status"] = "sucesso"
                    resultado["nota"] = item_alvo.get("numero")
                    resultado["cliente"] = item_alvo.get("clienteNome")
                    break 
                else:
                    resultado["msg"] = "Erro ao baixar arquivos da NF-e"
            
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
st.markdown(f"**Usuário:** {PHARMUP_USER} | **Filtro Ativo:** Apenas Notas Fiscais (Ignorando Serviços)")

col_input, col_status = st.columns([1, 2])

with col_input:
    vendas_input = st.text_area("Cole os Códigos (Um por linha):", height=300, placeholder="114316\n112433")
    btn_process = st.button("🚀 Processar Vendas", type="primary", use_container_width=True)

if btn_process:
    if not vendas_input.strip():
        st.warning("Lista vazia.")
    else:
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
                status_text.info(f"Processando {total_codigos} vendas...")
                
                resultados_ordenados = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_index = {executor.submit(api_search_and_get_links, token, cod): i for i, cod in enumerate(codigos)}
                    completed = 0
                    
                    for future in concurrent.futures.as_completed(future_to_index):
                        index_original = future_to_index[future]
                        data = future.result()
                        resultados_ordenados[index_original] = data
                        completed += 1
                        status_bar.progress(completed / total_codigos)
                        
                        if data['status'] == 'sucesso':
                             status_text.text(f"✅ {data['venda']} - NF-e OK")
                        else:
                             status_text.text(f"⚠️ {data['venda']} - {data['msg']}")

                for i in range(total_codigos):
                    res = resultados_ordenados[i]
                    log_entry = {
                        "Venda": res["venda"],
                        "Nota": res.get("nota") if res["status"] == "sucesso" else "-",
                        "Cliente": res.get("cliente") if res["status"] == "sucesso" else res.get("msg"),
                        "Status": "✅ NF-e" if res["status"] == "sucesso" else "❌ Ignorada/Erro"
                    }
                    st.session_state.log_data.append(log_entry)
                    
                    if res["status"] == "sucesso":
                        if res["pdf_normal"]: st.session_state.buffers_normais.append(res["pdf_normal"])
                        if res["pdf_simplificado"]: st.session_state.buffers_simples.append(res["pdf_simplificado"])
                
                status_text.success("Processamento finalizado!")
                status_bar.empty()

# --- EXIBIÇÃO DE RESULTADOS ---
if st.session_state.dados_processados:
    with col_status:
        st.divider()
        st.write("#### 📊 Relatório de NF-e")
        st.dataframe(st.session_state.log_data, use_container_width=True, height=250)
        
        lista_pendencias = [d["Venda"] for d in st.session_state.log_data if "❌" in d["Status"]]
        
        if lista_pendencias:
            st.error(f"⚠️ **{len(lista_pendencias)} Itens sem Nota Fiscal (Podem ser apenas Serviços):**")
            st.code("\n".join(lista_pendencias), language="text")

        st.divider()
        c1, c2 = st.columns(2)
        if st.session_state.buffers_normais:
            merged = merge_pdfs(st.session_state.buffers_normais)
            c1.download_button("📄 Notas NF-e (A4)", merged, f"NFe_{datetime.now().strftime('%H%M')}.pdf", "application/pdf", type="primary", use_container_width=True)
        if st.session_state.buffers_simples:
            merged_s = merge_pdfs(st.session_state.buffers_simples)
            c2.download_button("🏷️ Etiquetas NF-e", merged_s, f"Etiquetas_NFe_{datetime.now().strftime('%H%M')}.pdf", "application/pdf", type="secondary", use_container_width=True)
