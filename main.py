import os
import io
import json
import logging
import smtplib
import requests
import qrcode
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from apscheduler.schedulers.background import BackgroundScheduler
import anthropic

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
CLAUDE_API_KEY = (os.getenv("CLAUDE_API_KEY") or "").strip()
OMIE_APP_KEY = (os.getenv("OMIE_APP_KEY") or "").strip()
OMIE_APP_SECRET = (os.getenv("OMIE_APP_SECRET") or "").strip()
MP_ACCESS_TOKEN = (os.getenv("MP_ACCESS_TOKEN") or "").strip()

OMIE_BASE_URL = "https://app.omie.com.br/api/v1"
EMAIL_REMETENTE = os.getenv("EMAIL_REMETENTE")
EMAIL_SENHA_APP = os.getenv("EMAIL_SENHA_APP")
EMAIL_CONTADORA = os.getenv("EMAIL_CONTADORA")
MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN")
MP_API_URL = "https://api.mercadopago.com"

# Dados da fábrica terceirizada
FABRICA = {
    "razao_social": "VINICOLA GIARETTA LTDA",
    "cnpj": "08922937000126",
    "cep": "99200000",
    "estado": "RS",
    "cidade": "Guapore",
}

claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

SYSTEM_PROMPT = f"""Você é um assistente financeiro especializado, integrado ao sistema Omie ERP da empresa.
Seu papel é ser o braço direito do dono da empresa nas questões financeiras e de estoque.

Tom: direto, objetivo, profissional mas acessível. Sempre em português brasileiro.

Regras:
1. NUNCA execute ações destrutivas sem confirmação explícita do usuário
2. Sempre contextualize números quando possível
3. Em caso de erro na API, explique em linguagem simples
4. Nunca exponha credenciais em nenhuma resposta
5. Antes de qualquer ação, diga "Vou [ação]. Confirma?" e só execute após "sim", "confirma" ou equivalente.

Tipos de nota fiscal — entenda a diferença:
- VENDA: para clientes externos. Requer cliente, forma de pagamento (PIX/boleto/cartão) e emite NF-e de venda.
- REMESSA PARA INDUSTRIALIZAÇÃO (CFOP 5.901): enviamos matéria-prima para a fábrica terceirizada VINICOLA GIARETTA LTDA. NÃO é uma venda, NÃO tem forma de pagamento, o destinatário é SEMPRE a fábrica. Use a ferramenta emitir_nota_remessa.
- RETORNO DE INDUSTRIALIZAÇÃO (CFOP 5.902): a fábrica nos devolve o produto acabado. NÃO é uma venda. Use a ferramenta emitir_nota_retorno.

Quando o usuário pedir remessa para fábrica/industrializadora, use SEMPRE emitir_nota_remessa — nunca pergunte sobre cliente ou forma de pagamento.

Hoje é {datetime.now().strftime("%d/%m/%Y")}."""


# ── Omie API ──────────────────────────────────────────────────────────────────

def omie_request(endpoint, call, params):
    url = f"{OMIE_BASE_URL}/{endpoint}/"
    payload = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [params],
    }
    try:
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code >= 400:
            logger.error(f"Omie [{call}] status {r.status_code}: {r.text[:800]}")
        r.raise_for_status()
        data = r.json()
        # Omie retorna "faultstring" quando não há registros — tratar como lista vazia
        if "faultstring" in data:
            msg = data["faultstring"].lower()
            if "nenhum" in msg or "não encontrado" in msg or "nao encontrado" in msg:
                return {"registros": [], "total_de_registros": 0}
            return {"erro": data["faultstring"]}
        return data
    except requests.exceptions.Timeout:
        return {"erro": "O Omie demorou demais para responder. Tente novamente."}
    except requests.exceptions.ConnectionError:
        return {"erro": "Não consegui me conectar ao Omie. Verifique sua internet."}
    except Exception as e:
        logger.error(f"Omie erro [{call}]: {type(e).__name__}: {e}")
        return {"erro": str(e)}


def listar_contas_pagar(data_inicio=None, data_fim=None, apenas_vencidas=False):
    hoje = datetime.now()
    params = {
        "pagina": 1,
        "registros_por_pagina": 50,
        "filtrar_por_data_de": data_inicio or hoje.strftime("%d/%m/%Y"),
        "filtrar_por_data_ate": data_fim or (hoje + timedelta(days=7)).strftime("%d/%m/%Y"),
    }
    if apenas_vencidas:
        params["filtrar_por_status"] = "VENCIDO"
    return omie_request("financas/contapagar", "ListarContasPagar", params)


def listar_contas_receber(data_inicio=None, data_fim=None):
    hoje = datetime.now()
    params = {
        "pagina": 1,
        "registros_por_pagina": 50,
        "filtrar_por_data_de": data_inicio or hoje.strftime("%d/%m/%Y"),
        "filtrar_por_data_ate": data_fim or (hoje + timedelta(days=7)).strftime("%d/%m/%Y"),
    }
    return omie_request("financas/contareceber", "ListarContasReceber", params)


def consultar_estoque():
    return omie_request("geral/produtos", "ListarProdutos", {
        "pagina": 1,
        "registros_por_pagina": 50,
        "filtrar_apenas_omiepdv": "N",
    })


def listar_clientes_inadimplentes():
    hoje = datetime.now()
    params = {
        "pagina": 1,
        "registros_por_pagina": 50,
        "filtrar_por_data_de": (hoje - timedelta(days=90)).strftime("%d/%m/%Y"),
        "filtrar_por_data_ate": (hoje - timedelta(days=1)).strftime("%d/%m/%Y"),
    }
    return omie_request("financas/contareceber", "ListarContasReceber", params)


def _montar_dados_pessoa(razao_social, cnpj_cpf, email=None, telefone=None,
                          endereco=None, numero=None, bairro=None,
                          cidade=None, estado=None, cep=None, pessoa_fisica=None):
    """Monta o dict base para IncluirCliente no Omie."""
    # Remove formatação do CPF/CNPJ
    doc = "".join(c for c in (cnpj_cpf or "") if c.isdigit())
    # Detecta pessoa física automaticamente se não informado
    if pessoa_fisica is None:
        pessoa_fisica = "S" if len(doc) <= 11 else "N"

    dados = {
        "razao_social": razao_social,
        "cnpj_cpf": doc,
        "pessoa_fisica": pessoa_fisica,
        "codigo_cliente_integracao": doc,  # Omie exige código único de integração
    }

    if email:
        dados["email"] = email

    if telefone:
        digitos = "".join(c for c in telefone if c.isdigit())
        if len(digitos) >= 10:
            dados["telefone1_ddd"] = digitos[:2]
            dados["telefone1_numero"] = digitos[2:]

    if endereco:
        dados["endereco"] = endereco
    if numero:
        dados["endereco_numero"] = numero
    if bairro:
        dados["bairro"] = bairro
    if cidade:
        dados["cidade"] = cidade
    if estado:
        dados["estado"] = estado
    if cep:
        dados["cep"] = "".join(c for c in cep if c.isdigit())

    return dados


def cadastrar_cliente(razao_social, cnpj_cpf, email=None, telefone=None,
                      endereco=None, numero=None, bairro=None,
                      cidade=None, estado=None, cep=None, pessoa_fisica=None):
    dados = _montar_dados_pessoa(razao_social, cnpj_cpf, email, telefone,
                                  endereco, numero, bairro, cidade, estado, cep, pessoa_fisica)
    return omie_request("geral/clientes", "IncluirCliente", dados)


def cadastrar_fornecedor(razao_social, cnpj_cpf, email=None, telefone=None,
                          endereco=None, numero=None, bairro=None,
                          cidade=None, estado=None, cep=None, pessoa_fisica=None):
    dados = _montar_dados_pessoa(razao_social, cnpj_cpf, email, telefone,
                                  endereco, numero, bairro, cidade, estado, cep, pessoa_fisica)
    dados["tags"] = [{"tag": "fornecedor"}]
    return omie_request("geral/clientes", "IncluirCliente", dados)


# ── Cadastro de Produtos / Matérias-Primas ────────────────────────────────────

def cadastrar_produto(nome, unidade, preco, codigo_interno, tipo="produto", ncm=None, observacao=None):
    """Cadastra produto ou matéria-prima no Omie."""
    dados = {
        "descricao": nome,
        "unidade": unidade.upper(),
        "valor_unitario": round(float(preco), 2),
        "codigo_produto_integracao": str(codigo_interno),
        "tipo_item": "04" if tipo == "materia_prima" else "00",  # 04=Matéria-prima, 00=Mercadoria
    }
    if ncm:
        dados["ncm"] = "".join(c for c in ncm if c.isdigit())
    if observacao:
        dados["obs_internas"] = observacao
    return omie_request("geral/produtos", "IncluirProduto", dados)


# ── Vendas: Busca de dados ────────────────────────────────────────────────────

def buscar_cliente_por_nome(nome):
    """Busca cliente cadastrado no Omie pelo nome."""
    return omie_request("geral/clientes", "ListarClientes", {
        "pagina": 1,
        "registros_por_pagina": 10,
        "clientesFiltro": {"razao_social": nome},
    })


def buscar_produto_por_nome(nome):
    """Busca produto cadastrado no Omie pelo nome."""
    resultado = omie_request("geral/produtos", "ListarProdutos", {
        "pagina": 1,
        "registros_por_pagina": 50,
        "filtrar_apenas_omiepdv": "N",
    })
    # Filtra localmente pelo nome
    if isinstance(resultado, dict) and "produto_servico_cadastro" in resultado:
        nome_lower = nome.lower()
        filtrados = [
            p for p in resultado["produto_servico_cadastro"]
            if nome_lower in p.get("descricao", "").lower()
        ]
        return {"produto_servico_cadastro": filtrados, "total_de_registros": len(filtrados)}
    return resultado


# ── Vendas: Mercado Pago ──────────────────────────────────────────────────────

def _mp_headers():
    return {
        "Authorization": f"Bearer {MP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def criar_cobranca_pix(descricao, valor, email_cliente, nome_cliente, cpf_cnpj):
    """Gera cobrança PIX no Mercado Pago."""
    doc = "".join(c for c in cpf_cnpj if c.isdigit())
    tipo = "CPF" if len(doc) <= 11 else "CNPJ"
    payload = {
        "transaction_amount": round(float(valor), 2),
        "description": descricao,
        "payment_method_id": "pix",
        "payer": {
            "email": email_cliente or "sem@email.com",
            "first_name": nome_cliente.split()[0],
            "last_name": " ".join(nome_cliente.split()[1:]) or ".",
            "identification": {"type": tipo, "number": doc},
        },
    }
    r = requests.post(f"{MP_API_URL}/v1/payments", json=payload, headers=_mp_headers(), timeout=30)
    return r.json()


def criar_boleto(descricao, valor, email_cliente, nome_cliente, cpf_cnpj, cep="01310100"):
    """Gera boleto no Mercado Pago."""
    doc = "".join(c for c in cpf_cnpj if c.isdigit())
    tipo = "CPF" if len(doc) <= 11 else "CNPJ"
    payload = {
        "transaction_amount": round(float(valor), 2),
        "description": descricao,
        "payment_method_id": "bolbradesco",
        "payer": {
            "email": email_cliente or "sem@email.com",
            "first_name": nome_cliente.split()[0],
            "last_name": " ".join(nome_cliente.split()[1:]) or ".",
            "identification": {"type": tipo, "number": doc},
            "address": {"zip_code": "".join(c for c in cep if c.isdigit())},
        },
    }
    r = requests.post(f"{MP_API_URL}/v1/payments", json=payload, headers=_mp_headers(), timeout=30)
    return r.json()


def criar_link_cartao(descricao, valor):
    """Gera link de pagamento (cartão crédito/débito) via Mercado Pago."""
    payload = {
        "items": [{
            "title": descricao,
            "quantity": 1,
            "unit_price": round(float(valor), 2),
            "currency_id": "BRL",
        }],
        "payment_methods": {
            "excluded_payment_types": [{"id": "ticket"}, {"id": "bank_transfer"}],
        },
        "back_urls": {"success": "https://www.mercadopago.com.br"},
        "auto_return": "approved",
    }
    r = requests.post(f"{MP_API_URL}/checkout/preferences", json=payload, headers=_mp_headers(), timeout=30)
    return r.json()


def gerar_imagem_qrcode(texto_pix):
    """Gera imagem PNG do QR code PIX."""
    qr = qrcode.QRCode(box_size=8, border=2)
    qr.add_data(texto_pix)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf


# ── Vendas: NF-e no Omie (Simples Nacional) ──────────────────────────────────

def emitir_nfe(cod_cliente, cod_produto, quantidade, valor_unitario, id_pagamento_mp=None):
    """Emite NF-e no Omie vinculada à venda."""
    valor_total = round(float(quantidade) * float(valor_unitario), 2)
    dados = {
        "cabecalho": {
            "cOperacao": "V",          # Venda
            "cNatureza": "Venda de produto",
            "nCodCliente": cod_cliente,
        },
        "det": [{
            "produto": {
                "nCodProd": cod_produto,
                "nQtde": float(quantidade),
                "nValUnit": float(valor_unitario),
                "nValTotal": valor_total,
            },
            "imposto": {
                "cRegTrib": "1",       # 1 = Simples Nacional
                "cModBc": "3",
            },
        }],
        "infAdic": {
            "cInfCpl": f"Pagamento via Mercado Pago. ID: {id_pagamento_mp}" if id_pagamento_mp else "Venda registrada via Assistente Financeiro",
        },
    }
    return omie_request("produtos/nfe", "IncluirNF", dados)


def registrar_venda_omie(cod_cliente, valor_total, descricao, id_pagamento_mp=None):
    """Registra conta a receber no Omie para a venda."""
    hoje = datetime.now()
    dados = {
        "cabecCR": {
            "nCodCliente": cod_cliente,
            "dDtEmissao": hoje.strftime("%d/%m/%Y"),
            "dDtVenc": hoje.strftime("%d/%m/%Y"),
            "nValorTitulo": round(float(valor_total), 2),
            "cDescrTitulo": descricao,
            "cStatus": "ABERTO",
            "codigo_lancamento_integracao": f"VENDA-MP-{id_pagamento_mp or hoje.strftime('%Y%m%d%H%M%S')}",
        }
    }
    return omie_request("financas/contareceber", "IncluirContaReceber", dados)


# ── Notas de Remessa / Retorno Industrialização ───────────────────────────────

def buscar_ou_cadastrar_fabrica():
    """Garante que a fábrica está cadastrada no Omie como cliente/destinatário."""
    resultado = omie_request("geral/clientes", "ListarClientes", {
        "pagina": 1,
        "registros_por_pagina": 1,
        "clientesFiltro": {"cnpj_cpf": FABRICA["cnpj"]},
    })
    clientes = resultado.get("clientes_cadastro", []) if isinstance(resultado, dict) else []
    if clientes:
        return clientes[0].get("codigo_cliente_omie")

    # Cadastra a fábrica se não existir
    cadastro = omie_request("geral/clientes", "IncluirCliente", {
        "razao_social": FABRICA["razao_social"],
        "cnpj_cpf": FABRICA["cnpj"],
        "pessoa_fisica": "N",
        "codigo_cliente_integracao": FABRICA["cnpj"],
        "cep": FABRICA["cep"],
        "estado": FABRICA["estado"],
        "cidade": FABRICA["cidade"],
    })
    return cadastro.get("codigo_cliente_omie")


def emitir_nota_remessa(cod_produto, descricao_produto, quantidade, valor_unitario, observacao=None):
    """
    Emite NF-e de remessa para industrialização (CFOP 5.901).
    Remetente: sua empresa. Destinatário: fábrica terceirizada.
    """
    cod_fabrica = buscar_ou_cadastrar_fabrica()
    if not cod_fabrica:
        return {"erro": "Não foi possível localizar/cadastrar a fábrica no Omie."}

    valor_total = round(float(quantidade) * float(valor_unitario), 2)
    dados = {
        "cabecalho": {
            "cOperacao": "R",
            "cNatureza": "Remessa para Industrialização",
            "nCodCliente": cod_fabrica,
        },
        "det": [{
            "produto": {
                "nCodProd": cod_produto,
                "nQtde": float(quantidade),
                "nValUnit": float(valor_unitario),
                "nValTotal": valor_total,
                "cCFOP": "5901",
            },
            "imposto": {
                "cRegTrib": "1",  # Simples Nacional
            },
        }],
        "infAdic": {
            "cInfCpl": observacao or f"Remessa de {descricao_produto} para industrialização — {FABRICA['razao_social']} CNPJ {FABRICA['cnpj']}",
        },
    }
    return omie_request("produtos/nfe", "IncluirNF", dados)


def emitir_nota_retorno(cod_produto_pronto, descricao_produto, quantidade, valor_unitario, observacao=None):
    """
    Emite NF-e de retorno de industrialização (CFOP 5.902).
    Remetente: fábrica. Destinatário: sua empresa.
    Registra a entrada do produto acabado no Omie.
    """
    cod_fabrica = buscar_ou_cadastrar_fabrica()
    if not cod_fabrica:
        return {"erro": "Não foi possível localizar a fábrica no Omie."}

    valor_total = round(float(quantidade) * float(valor_unitario), 2)
    dados = {
        "cabecalho": {
            "cOperacao": "E",  # Entrada
            "cNatureza": "Retorno de Industrialização",
            "nCodCliente": cod_fabrica,
        },
        "det": [{
            "produto": {
                "nCodProd": cod_produto_pronto,
                "nQtde": float(quantidade),
                "nValUnit": float(valor_unitario),
                "nValTotal": valor_total,
                "cCFOP": "5902",
            },
            "imposto": {
                "cRegTrib": "1",
            },
        }],
        "infAdic": {
            "cInfCpl": observacao or f"Retorno de industrialização — {descricao_produto} — {FABRICA['razao_social']}",
        },
    }
    return omie_request("produtos/nfe", "IncluirNF", dados)


# ── Extrato Financeiro & Bancário ────────────────────────────────────────────

def buscar_extrato_financeiro(ano, mes):
    """Retorna contas pagas e recebidas no mês."""
    primeiro = f"01/{mes:02d}/{ano}"
    ultimo_dia = (datetime(ano, mes % 12 + 1, 1) - timedelta(days=1)).day if mes < 12 else 31
    ultimo = f"{ultimo_dia}/{mes:02d}/{ano}"

    pagas = omie_request("financas/contapagar", "ListarContasPagar", {
        "pagina": 1, "registros_por_pagina": 500,
        "filtrar_por_data_de": primeiro, "filtrar_por_data_ate": ultimo,
        "filtrar_por_status": "PAGO",
    })
    recebidas = omie_request("financas/contareceber", "ListarContasReceber", {
        "pagina": 1, "registros_por_pagina": 500,
        "filtrar_por_data_de": primeiro, "filtrar_por_data_ate": ultimo,
        "filtrar_por_status": "RECEBIDO",
    })
    return pagas, recebidas


def buscar_contas_correntes():
    """Lista contas bancárias cadastradas no Omie."""
    return omie_request("financas/contacorrente", "ListarContasCorrentes", {
        "pagina": 1, "registros_por_pagina": 50,
    })


def buscar_extrato_bancario(cod_conta, ano, mes):
    """Busca extrato bancário de uma conta no Omie."""
    primeiro = f"01/{mes:02d}/{ano}"
    ultimo_dia = (datetime(ano, mes % 12 + 1, 1) - timedelta(days=1)).day if mes < 12 else 31
    ultimo = f"{ultimo_dia}/{mes:02d}/{ano}"
    return omie_request("financas/extrato", "ListarExtrato", {
        "nCodCC": cod_conta,
        "dDtInicio": primeiro,
        "dDtFim": ultimo,
    })


# ── Notas Fiscais & E-mail ────────────────────────────────────────────────────

def buscar_nfe_mes(ano, mes):
    """Busca NF-e emitidas no mês/ano informado."""
    primeiro = f"01/{mes:02d}/{ano}"
    ultimo_dia = (datetime(ano, mes % 12 + 1, 1) - timedelta(days=1)).day if mes < 12 else 31
    ultimo = f"{ultimo_dia}/{mes:02d}/{ano}"
    params = {
        "pagina": 1,
        "registros_por_pagina": 500,
        "filtrar_por_data_de": primeiro,
        "filtrar_por_data_ate": ultimo,
    }
    return omie_request("produtos/nfe", "ListarNF", params)


def buscar_nfse_mes(ano, mes):
    """Busca NFS-e emitidas no mês/ano informado."""
    primeiro = f"01/{mes:02d}/{ano}"
    ultimo_dia = (datetime(ano, mes % 12 + 1, 1) - timedelta(days=1)).day if mes < 12 else 31
    ultimo = f"{ultimo_dia}/{mes:02d}/{ano}"
    params = {
        "pagina": 1,
        "registros_por_pagina": 500,
        "dDtInicial": primeiro,
        "dDtFinal": ultimo,
    }
    return omie_request("nfse", "ListarNFSe", params)


def montar_html_notas(ano, mes, nfe, nfse):
    """Monta o corpo HTML do e-mail com as notas do mês."""
    nome_mes = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"][mes - 1]

    linhas_nfe = ""
    total_nfe = 0.0
    nfe_lista = nfe.get("nfCadastro", []) if isinstance(nfe, dict) else []
    for n in nfe_lista:
        numero = n.get("compl", {}).get("nNF", "-")
        cliente = n.get("dest", {}).get("razao_social", n.get("dest", {}).get("nome_fantasia", "-"))
        valor = float(n.get("total", {}).get("vNF", 0))
        total_nfe += valor
        data = n.get("ide", {}).get("dEmi", "-")
        linhas_nfe += f"<tr><td>{numero}</td><td>{cliente}</td><td>{data}</td><td>R$ {valor:,.2f}</td></tr>"

    linhas_nfse = ""
    total_nfse = 0.0
    nfse_lista = nfse.get("nfseCadastro", []) if isinstance(nfse, dict) else []
    for n in nfse_lista:
        numero = n.get("nfse", {}).get("numero_nfse", "-")
        cliente = n.get("nfse", {}).get("tomador", {}).get("razao_social", "-")
        valor = float(n.get("nfse", {}).get("valor_servicos", 0))
        total_nfse += valor
        data = n.get("nfse", {}).get("data_emissao", "-")
        linhas_nfse += f"<tr><td>{numero}</td><td>{cliente}</td><td>{data}</td><td>R$ {valor:,.2f}</td></tr>"

    def tabela(titulo, linhas, total):
        if not linhas:
            corpo = "<p>Nenhuma nota encontrada.</p>"
        else:
            corpo = (
                '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%">'
                '<tr style="background:#f0f0f0"><th>Número</th><th>Cliente/Tomador</th><th>Data</th><th>Valor</th></tr>'
                + linhas
                + f'<tr style="background:#e8f5e9;font-weight:bold"><td colspan="3">Total</td><td>R$ {total:,.2f}</td></tr>'
                + "</table>"
            )
        return f"<h3>{titulo}</h3>" + corpo

    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#333">
    <h2>Fechamento Fiscal — {nome_mes}/{ano}</h2>
    <p>Segue o pacote de notas fiscais emitidas no mês de <strong>{nome_mes}/{ano}</strong>.</p>
    {tabela("NF-e (Produtos)", linhas_nfe, total_nfe)}
    <br>
    {tabela("NFS-e (Serviços)", linhas_nfse, total_nfse)}
    <br>
    <p style="color:#888;font-size:12px">Enviado automaticamente pelo Assistente Financeiro — {datetime.now().strftime("%d/%m/%Y %H:%M")}</p>
    </body></html>"""


def montar_html_extrato_financeiro(_ano, _mes, pagas, recebidas):
    """Gera seção HTML do extrato financeiro (entradas e saídas)."""
    def linhas(lista_key, data_key, valor_key, nome_key, dados):
        itens = dados.get(lista_key, []) if isinstance(dados, dict) else []
        html = ""
        total = 0.0
        for i in itens:
            data = i.get(data_key, "-")
            nome = i.get(nome_key, "-")
            valor = float(i.get(valor_key, 0))
            total += valor
            html += f"<tr><td>{data}</td><td>{nome}</td><td>R$ {valor:,.2f}</td></tr>"
        return html, total

    linhas_saidas, total_saidas = linhas(
        "conta_pagar_cadastro", "data_vencimento", "valor_documento", "nome_fornecedor", pagas)
    linhas_entradas, total_entradas = linhas(
        "conta_receber_cadastro", "data_vencimento", "valor_documento", "nome_cliente", recebidas)

    def tabela(titulo, cor, linhas_html, total):
        if not linhas_html:
            return f"<h3>{titulo}</h3><p>Nenhum lançamento encontrado.</p>"
        return f"""<h3>{titulo}</h3>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%">
          <tr style="background:#f0f0f0"><th>Data</th><th>Descrição</th><th>Valor</th></tr>
          {linhas_html}
          <tr style="background:{cor};font-weight:bold"><td colspan="2">Total</td><td>R$ {total:,.2f}</td></tr>
        </table>"""

    saldo = total_entradas - total_saidas
    cor_saldo = "#e8f5e9" if saldo >= 0 else "#ffebee"
    return f"""
    {tabela("💰 Entradas (contas recebidas)", "#e8f5e9", linhas_entradas, total_entradas)}
    <br>
    {tabela("💸 Saídas (contas pagas)", "#ffebee", linhas_saidas, total_saidas)}
    <br>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%">
      <tr style="background:{cor_saldo};font-weight:bold;font-size:16px">
        <td>Saldo do mês</td>
        <td>R$ {saldo:,.2f}</td>
      </tr>
    </table>"""


def montar_html_extrato_bancario(extratos_bancos):
    """Gera seção HTML dos extratos bancários."""
    if not extratos_bancos:
        return "<h3>🏦 Extrato Bancário</h3><p>Nenhuma conta bancária encontrada no Omie.</p>"

    html = "<h3>🏦 Extrato Bancário</h3>"
    for nome_conta, extrato in extratos_bancos:
        movimentos = extrato.get("movimentos", []) if isinstance(extrato, dict) else []
        if "erro" in (extrato or {}):
            html += f"<p><strong>{nome_conta}:</strong> não foi possível buscar o extrato.</p>"
            continue
        linhas = ""
        saldo = 0.0
        for m in movimentos:
            data = m.get("dData", "-")
            desc = m.get("cDescricao", "-")
            valor = float(m.get("nValor", 0))
            tipo = m.get("cTipo", "")
            saldo += valor
            cor = "#e8f5e9" if valor >= 0 else "#ffebee"
            linhas += f'<tr style="background:{cor}"><td>{data}</td><td>{desc}</td><td>{tipo}</td><td>R$ {valor:,.2f}</td></tr>'

        if linhas:
            corpo_conta = (
                '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%">'
                '<tr style="background:#f0f0f0"><th>Data</th><th>Descrição</th><th>Tipo</th><th>Valor</th></tr>'
                + linhas
                + f'<tr style="font-weight:bold"><td colspan="3">Saldo período</td><td>R$ {saldo:,.2f}</td></tr>'
                + "</table>"
            )
        else:
            corpo_conta = "<p>Sem movimentações no período.</p>"
        html += f"<h4>{nome_conta}</h4>" + corpo_conta
    return html


def enviar_email_contadora(ano=None, mes=None):
    """Busca notas, extratos do mês e envia para a contadora."""
    hoje = datetime.now()
    if ano is None or mes is None:
        primeiro_do_mes = hoje.replace(day=1)
        mes_anterior = primeiro_do_mes - timedelta(days=1)
        ano, mes = mes_anterior.year, mes_anterior.month

    nome_mes = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"][mes - 1]

    logger.info(f"Montando fechamento de {nome_mes}/{ano}...")

    # Notas fiscais
    nfe = buscar_nfe_mes(ano, mes)
    nfse = buscar_nfse_mes(ano, mes)
    html_notas = montar_html_notas(ano, mes, nfe, nfse)

    # Extrato financeiro
    pagas, recebidas = buscar_extrato_financeiro(ano, mes)
    html_financeiro = montar_html_extrato_financeiro(ano, mes, pagas, recebidas)

    # Extrato bancário
    contas = buscar_contas_correntes()
    extratos_bancos = []
    for conta in (contas.get("ListarContasCorrentes", []) if isinstance(contas, dict) else []):
        cod = conta.get("nCodCC")
        nome = conta.get("cDescricao", f"Conta {cod}")
        if cod:
            extrato = buscar_extrato_bancario(cod, ano, mes)
            extratos_bancos.append((nome, extrato))
    html_bancario = montar_html_extrato_bancario(extratos_bancos)

    html_completo = f"""<html><body style="font-family:Arial,sans-serif;color:#333">
    <h2>📋 Fechamento Mensal — {nome_mes}/{ano}</h2>
    <p>Segue o pacote completo de fechamento do mês de <strong>{nome_mes}/{ano}</strong>.</p>
    <hr>
    <h2>📄 Notas Fiscais</h2>
    {html_notas}
    <hr>
    <h2>📊 Extrato Financeiro</h2>
    {html_financeiro}
    <hr>
    {html_bancario}
    <hr>
    <p style="color:#888;font-size:12px">Enviado automaticamente pelo Assistente Financeiro — {datetime.now().strftime("%d/%m/%Y %H:%M")}</p>
    </body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Fechamento Mensal — {nome_mes}/{ano}"
    msg["From"] = EMAIL_REMETENTE
    msg["To"] = EMAIL_CONTADORA
    msg.attach(MIMEText(html_completo, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(EMAIL_REMETENTE, EMAIL_SENHA_APP)
            smtp.sendmail(EMAIL_REMETENTE, EMAIL_CONTADORA, msg.as_string())
        logger.info(f"Fechamento {nome_mes}/{ano} enviado para {EMAIL_CONTADORA}")
        return True
    except Exception as e:
        logger.error(f"Erro ao enviar e-mail: {e}")
        return False


# ── Claude Tools ──────────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "listar_contas_pagar",
        "description": "Lista contas a pagar da empresa no Omie, filtradas por vencimento.",
        "input_schema": {
            "type": "object",
            "properties": {
                "data_inicio": {"type": "string", "description": "Data inicial DD/MM/YYYY (padrão: hoje)"},
                "data_fim": {"type": "string", "description": "Data final DD/MM/YYYY (padrão: +7 dias)"},
                "apenas_vencidas": {"type": "boolean", "description": "Retornar só contas vencidas"},
            },
        },
    },
    {
        "name": "listar_contas_receber",
        "description": "Lista contas a receber da empresa no Omie, filtradas por vencimento.",
        "input_schema": {
            "type": "object",
            "properties": {
                "data_inicio": {"type": "string", "description": "Data inicial DD/MM/YYYY"},
                "data_fim": {"type": "string", "description": "Data final DD/MM/YYYY"},
            },
        },
    },
    {
        "name": "consultar_estoque",
        "description": "Consulta posição atual do estoque no Omie.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "listar_clientes_inadimplentes",
        "description": "Lista clientes com contas a receber vencidas (inadimplentes) nos últimos 90 dias.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "cadastrar_cliente",
        "description": "Cadastra um novo cliente no Omie. OBRIGATÓRIO: apresentar resumo dos dados e aguardar confirmação antes de chamar esta ferramenta.",
        "input_schema": {
            "type": "object",
            "required": ["razao_social", "cnpj_cpf"],
            "properties": {
                "razao_social": {"type": "string", "description": "Nome completo ou razão social"},
                "cnpj_cpf": {"type": "string", "description": "CPF ou CNPJ (com ou sem formatação)"},
                "email": {"type": "string", "description": "E-mail do cliente"},
                "telefone": {"type": "string", "description": "Telefone com DDD"},
                "endereco": {"type": "string", "description": "Logradouro"},
                "numero": {"type": "string", "description": "Número do endereço"},
                "bairro": {"type": "string", "description": "Bairro"},
                "cidade": {"type": "string", "description": "Cidade"},
                "estado": {"type": "string", "description": "Estado (sigla, ex: SP)"},
                "cep": {"type": "string", "description": "CEP"},
                "pessoa_fisica": {"type": "string", "description": "S para pessoa física, N para jurídica"},
            },
        },
    },
    {
        "name": "cadastrar_fornecedor",
        "description": "Cadastra um novo fornecedor no Omie. OBRIGATÓRIO: apresentar resumo dos dados e aguardar confirmação antes de chamar esta ferramenta.",
        "input_schema": {
            "type": "object",
            "required": ["razao_social", "cnpj_cpf"],
            "properties": {
                "razao_social": {"type": "string", "description": "Nome completo ou razão social"},
                "cnpj_cpf": {"type": "string", "description": "CPF ou CNPJ (com ou sem formatação)"},
                "email": {"type": "string", "description": "E-mail do fornecedor"},
                "telefone": {"type": "string", "description": "Telefone com DDD"},
                "endereco": {"type": "string", "description": "Logradouro"},
                "numero": {"type": "string", "description": "Número do endereço"},
                "bairro": {"type": "string", "description": "Bairro"},
                "cidade": {"type": "string", "description": "Cidade"},
                "estado": {"type": "string", "description": "Estado (sigla, ex: SP)"},
                "cep": {"type": "string", "description": "CEP"},
                "pessoa_fisica": {"type": "string", "description": "S para pessoa física, N para jurídica"},
            },
        },
    },
    {
        "name": "cadastrar_produto",
        "description": "Cadastra um produto ou matéria-prima no Omie. OBRIGATÓRIO: apresentar resumo e aguardar confirmação antes de executar.",
        "input_schema": {
            "type": "object",
            "required": ["nome", "unidade", "preco", "codigo_interno"],
            "properties": {
                "nome": {"type": "string", "description": "Nome do produto ou matéria-prima"},
                "unidade": {"type": "string", "description": "Unidade de medida (ex: KG, UN, L, G, CX)"},
                "preco": {"type": "number", "description": "Preço unitário"},
                "codigo_interno": {"type": "string", "description": "Código interno/CRM do produto"},
                "tipo": {"type": "string", "description": "produto ou materia_prima"},
                "ncm": {"type": "string", "description": "Código NCM (opcional)"},
                "observacao": {"type": "string", "description": "Observação interna (opcional)"},
            },
        },
    },
    {
        "name": "emitir_nota_remessa",
        "description": "Emite NF-e de remessa para industrialização (CFOP 5.901) da empresa para a fábrica terceirizada (VINICOLA GIARETTA LTDA). OBRIGATÓRIO: apresentar resumo e aguardar confirmação antes de executar.",
        "input_schema": {
            "type": "object",
            "required": ["cod_produto", "descricao_produto", "quantidade", "valor_unitario"],
            "properties": {
                "cod_produto": {"type": "integer", "description": "Código do produto no Omie"},
                "descricao_produto": {"type": "string", "description": "Descrição da matéria-prima"},
                "quantidade": {"type": "number", "description": "Quantidade a remeter"},
                "valor_unitario": {"type": "number", "description": "Valor unitário"},
                "observacao": {"type": "string", "description": "Observação adicional para a nota"},
            },
        },
    },
    {
        "name": "emitir_nota_retorno",
        "description": "Emite NF-e de retorno de industrialização (CFOP 5.902) da fábrica para a empresa com o produto acabado. OBRIGATÓRIO: apresentar resumo e aguardar confirmação antes de executar.",
        "input_schema": {
            "type": "object",
            "required": ["cod_produto_pronto", "descricao_produto", "quantidade", "valor_unitario"],
            "properties": {
                "cod_produto_pronto": {"type": "integer", "description": "Código do produto acabado no Omie"},
                "descricao_produto": {"type": "string", "description": "Descrição do produto acabado"},
                "quantidade": {"type": "number", "description": "Quantidade retornada"},
                "valor_unitario": {"type": "number", "description": "Valor unitário do produto acabado"},
                "observacao": {"type": "string", "description": "Observação adicional para a nota"},
            },
        },
    },
    {
        "name": "buscar_cliente_por_nome",
        "description": "Busca um cliente já cadastrado no Omie pelo nome para vincular a uma venda.",
        "input_schema": {
            "type": "object",
            "required": ["nome"],
            "properties": {
                "nome": {"type": "string", "description": "Nome ou parte do nome do cliente"},
            },
        },
    },
    {
        "name": "buscar_produto_por_nome",
        "description": "Busca um produto cadastrado no Omie pelo nome para incluir em uma venda.",
        "input_schema": {
            "type": "object",
            "required": ["nome"],
            "properties": {
                "nome": {"type": "string", "description": "Nome ou parte do nome do produto"},
            },
        },
    },
    {
        "name": "registrar_venda",
        "description": "Registra uma venda completa: gera cobrança no Mercado Pago (PIX, boleto ou cartão), lança conta a receber no Omie e emite NF-e. OBRIGATÓRIO: apresentar resumo e aguardar confirmação antes de executar.",
        "input_schema": {
            "type": "object",
            "required": ["cod_cliente", "nome_cliente", "cpf_cnpj_cliente", "cod_produto", "descricao_produto", "quantidade", "valor_unitario", "forma_pagamento"],
            "properties": {
                "cod_cliente": {"type": "integer", "description": "Código do cliente no Omie"},
                "nome_cliente": {"type": "string", "description": "Nome do cliente"},
                "cpf_cnpj_cliente": {"type": "string", "description": "CPF ou CNPJ do cliente"},
                "email_cliente": {"type": "string", "description": "E-mail do cliente (opcional)"},
                "cep_cliente": {"type": "string", "description": "CEP do cliente (necessário para boleto)"},
                "cod_produto": {"type": "integer", "description": "Código do produto no Omie"},
                "descricao_produto": {"type": "string", "description": "Descrição do produto"},
                "quantidade": {"type": "number", "description": "Quantidade vendida"},
                "valor_unitario": {"type": "number", "description": "Valor unitário do produto"},
                "forma_pagamento": {"type": "string", "description": "pix, boleto ou cartao"},
                "emitir_nfe": {"type": "boolean", "description": "Se deve emitir NF-e (padrão: true)"},
            },
        },
    },
]


def registrar_venda(cod_cliente, nome_cliente, cpf_cnpj_cliente, cod_produto,
                    descricao_produto, quantidade, valor_unitario, forma_pagamento,
                    email_cliente=None, cep_cliente="01310100", emitir_nfe=True):
    """Orquestra venda: cobrança MP + conta a receber Omie + NF-e."""
    valor_total = round(float(quantidade) * float(valor_unitario), 2)
    descricao = f"Venda: {quantidade}x {descricao_produto}"
    resultado = {}

    # 1. Gerar cobrança no Mercado Pago
    forma = forma_pagamento.lower().strip()
    if forma == "pix":
        mp = criar_cobranca_pix(descricao, valor_total, email_cliente, nome_cliente, cpf_cnpj_cliente)
    elif forma == "boleto":
        mp = criar_boleto(descricao, valor_total, email_cliente, nome_cliente, cpf_cnpj_cliente, cep_cliente)
    elif forma in ("cartao", "cartão", "credito", "crédito", "debito", "débito"):
        mp = criar_link_cartao(descricao, valor_total)
    else:
        return {"erro": f"Forma de pagamento inválida: {forma_pagamento}. Use pix, boleto ou cartao."}

    id_mp = mp.get("id") or mp.get("id_preference")
    resultado["mercado_pago"] = {
        "id": id_mp,
        "status": mp.get("status"),
    }

    # Extrair dados de pagamento
    if forma == "pix":
        pix_data = mp.get("point_of_interaction", {}).get("transaction_data", {})
        resultado["pix_copia_cola"] = pix_data.get("qr_code", "")
        resultado["pix_qr_base64"] = pix_data.get("qr_code_base64", "")
    elif forma == "boleto":
        resultado["boleto_url"] = mp.get("transaction_details", {}).get("external_resource_url", "")
        resultado["boleto_codigo"] = mp.get("barcode", {}).get("content", "")
    elif forma in ("cartao", "cartão", "credito", "crédito", "debito", "débito"):
        resultado["link_pagamento"] = mp.get("init_point", "")

    # 2. Lançar conta a receber no Omie
    cr = registrar_venda_omie(cod_cliente, valor_total, descricao, id_mp)
    resultado["conta_receber_omie"] = cr.get("cStatus", cr.get("erro", "registrado"))

    # 3. Emitir NF-e
    if emitir_nfe:
        nfe = emitir_nfe(cod_cliente, cod_produto, quantidade, valor_unitario, id_mp)
        resultado["nfe"] = nfe.get("cSitNF", nfe.get("erro", "emitida"))

    resultado["valor_total"] = valor_total
    resultado["forma_pagamento"] = forma_pagamento
    return resultado


def run_tool(name, inputs):
    if name == "listar_contas_pagar":
        return listar_contas_pagar(**inputs)
    if name == "listar_contas_receber":
        return listar_contas_receber(**inputs)
    if name == "consultar_estoque":
        return consultar_estoque()
    if name == "listar_clientes_inadimplentes":
        return listar_clientes_inadimplentes()
    if name == "cadastrar_cliente":
        return cadastrar_cliente(**inputs)
    if name == "cadastrar_fornecedor":
        return cadastrar_fornecedor(**inputs)
    if name == "cadastrar_produto":
        return cadastrar_produto(**inputs)
    if name == "emitir_nota_remessa":
        return emitir_nota_remessa(**inputs)
    if name == "emitir_nota_retorno":
        return emitir_nota_retorno(**inputs)
    if name == "buscar_cliente_por_nome":
        return buscar_cliente_por_nome(**inputs)
    if name == "buscar_produto_por_nome":
        return buscar_produto_por_nome(**inputs)
    if name == "registrar_venda":
        return registrar_venda(**inputs)
    return {"erro": "Ferramenta desconhecida"}


def chat_with_claude(messages):
    while True:
        response = claude_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            text = "\n".join(b.text for b in response.content if hasattr(b, "text"))
            return {"text": text, "pix_qr": None, "boleto_url": None, "link_cartao": None}

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            pix_qr = None
            boleto_url = None
            link_cartao = None
            for block in response.content:
                if block.type == "tool_use":
                    result = run_tool(block.name, block.input)
                    # Captura dados de pagamento para enviar como mídia
                    if block.name == "registrar_venda":
                        pix_copia = result.get("pix_copia_cola", "")
                        if pix_copia:
                            pix_qr = gerar_imagem_qrcode(pix_copia)
                        boleto_url = result.get("boleto_url") or result.get("boleto_codigo")
                        link_cartao = result.get("link_pagamento")
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, ensure_ascii=False),
                    })
            messages.append({"role": "user", "content": tool_results})
            # Guarda para retornar junto com o texto
            last_pix_qr = pix_qr
            last_boleto_url = boleto_url
            last_link_cartao = link_cartao
            continue

        return {"text": "Não consegui processar sua solicitação.", "pix_qr": None, "boleto_url": None, "link_cartao": None}

    # Nunca chega aqui mas satisfaz o linter
    return {"text": "", "pix_qr": locals().get("last_pix_qr"), "boleto_url": locals().get("last_boleto_url"), "link_cartao": locals().get("last_link_cartao")}


# ── Telegram Handlers ─────────────────────────────────────────────────────────

async def contadora_command(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    """Envia manualmente o pacote do mês anterior para a contadora."""
    await update.message.reply_text("Buscando notas e enviando para a contadora... aguarde.")
    ok = enviar_email_contadora()
    if ok:
        hoje = datetime.now().replace(day=1) - timedelta(days=1)
        nome_mes = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                    "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"][hoje.month - 1]
        await update.message.reply_text(
            f"E-mail enviado com sucesso!\n"
            f"Notas de *{nome_mes}/{hoje.year}* enviadas para `{EMAIL_CONTADORA}`",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text(
            "Tive um problema ao enviar o e-mail. Verifique o terminal para detalhes."
        )


async def start_command(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Olá! Sou seu *Assistente Financeiro* integrado ao Omie.\n\n"
        "Posso te ajudar com:\n"
        "• Contas a pagar e receber\n"
        "• Inadimplência\n"
        "• Estoque\n"
        "• Resumos financeiros\n\n"
        "O que você precisa hoje?",
        parse_mode="Markdown",
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "history" not in context.user_data:
        context.user_data["history"] = []

    context.user_data["history"].append({"role": "user", "content": update.message.text})

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    try:
        result = chat_with_claude(context.user_data["history"])
        text = result["text"]
        context.user_data["history"].append({"role": "assistant", "content": text})

        if len(context.user_data["history"]) > 20:
            context.user_data["history"] = context.user_data["history"][-20:]

        await update.message.reply_text(text)

        # Envia QR code PIX como imagem
        if result.get("pix_qr"):
            await update.message.reply_photo(
                photo=result["pix_qr"],
                caption="QR Code PIX — escaneie para pagar"
            )

        # Envia link do boleto
        if result.get("boleto_url"):
            await update.message.reply_text(
                f"Boleto: {result['boleto_url']}"
            )

        # Envia link do cartão
        if result.get("link_cartao"):
            await update.message.reply_text(
                f"Link para pagamento com cartão: {result['link_cartao']}"
            )

    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {e}")
        await update.message.reply_text(
            "Tive um problema técnico. Tenta novamente em instantes."
        )


def main():
    variaveis = {
        "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
        "CLAUDE_API_KEY": CLAUDE_API_KEY,
        "OMIE_APP_KEY": OMIE_APP_KEY,
        "OMIE_APP_SECRET": OMIE_APP_SECRET,
    }
    faltando = [k for k, v in variaveis.items() if not v]
    if faltando:
        raise EnvironmentError(f"Variáveis ausentes: {', '.join(faltando)}")

    # Agendamento: todo dia 1º do mês às 08:00 envia notas para a contadora
    scheduler = BackgroundScheduler()
    scheduler.add_job(enviar_email_contadora, "cron", day=1, hour=8, minute=0)
    scheduler.start()
    logger.info("Agendador iniciado — envio automático todo dia 1º às 08:00")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("contadora", contadora_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Assistente Financeiro iniciado!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
