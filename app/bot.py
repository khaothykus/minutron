import os, asyncio, traceback, logging, re
from datetime import datetime
from services.print_integration import (
    finalize_minuta_and_print,
    meuid_cmd,
    print_cmd,
    safe_delete_message,
    is_admin,
)

# opcional: sd_notify (só funciona quando rodando via systemd Type=notify)
try:
    from sdnotify import SystemdNotifier
except Exception:  # se lib não instalada, segue sem sd_notify
    SystemdNotifier = None

from telegram.error import BadRequest
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    InputFile,
)
from telegram.ext import (
    CallbackQueryHandler,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from config import BOT_TOKEN, ADMIN_TELEGRAM_ID
from services import storage, danfe_parser
# from services.excel_filler_spire import preencher_e_exportar_lote
from services.excel_filler_uno import preencher_e_exportar_lote
from services.rat_search import get_rat_for_ocorrencia
from services.validators import valida_qlid, valida_cidade
from keyboards import kb_cadastro, kb_main, kb_datas, kb_volumes
from services import etiqueta   # impressão de etiquetas

# =========================================
# LOGGING BÁSICO (vai pro journal)
# =========================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# =========================================
# HEALTH ENDPOINT (/health)
# =========================================
async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text(f"ok {datetime.now().isoformat(timespec='seconds')}")
    except Exception:
        pass

# =========================================
# SYSTEMD WATCHDOG (sd_notify)
# =========================================
def _watchdog_period_seconds() -> int:
    override = os.getenv("WATCHDOG_FORCE_SEC")
    if override:
        try:
            return max(1, int(float(override)))
        except Exception:
            return 0
    try:
        wd_usec = int(os.getenv("WATCHDOG_USEC", "0") or "0")
        return max(1, wd_usec // 2_000_000) if wd_usec > 0 else 0
    except Exception:
        return 0

async def _watchdog_loop(notifier, period: int, stop_event: asyncio.Event):
    # envia batidas “WATCHDOG=1” até receber o stop_event
    while not stop_event.is_set():
        try:
            await asyncio.sleep(period)
            notifier.notify("WATCHDOG=1")
        except Exception:
            # nunca deixa essa task derrubar o app
            pass

async def _post_init(app):
    notifier = SystemdNotifier() if SystemdNotifier else None
    app.bot_data["notifier"] = notifier

    if notifier:
        try:
            notifier.notify("READY=1")
            logging.info("[sd_notify] READY=1 enviado")
        except Exception as e:
            logging.warning(f"[sd_notify] READY falhou: {e}")

    period = _watchdog_period_seconds()
    if notifier and period > 0:
        stop_event = asyncio.Event()
        app.bot_data["wd_stop_event"] = stop_event
        app.bot_data["wd_task"] = asyncio.create_task(_watchdog_loop(notifier, period, stop_event))
        logging.info(f"[sd_notify] WATCHDOG ativado (a cada {period}s)")

async def _post_shutdown(app):
    notifier = app.bot_data.get("notifier")
    # encerra a task do watchdog com segurança
    stop_event = app.bot_data.get("wd_stop_event")
    if stop_event:
        stop_event.set()
    task = app.bot_data.get("wd_task")
    if task:
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except Exception:
            task.cancel()

    if notifier:
        try:
            notifier.notify("STOPPING=1")
            logging.info("[sd_notify] STOPPING=1 enviado")
        except Exception:
            pass

SESS = {}
RAT_TIMEOUT = int(os.getenv("RAT_FLOW_TIMEOUT", "90"))
_rat_cache = {}

# fila de impressão de etiquetas (apenas admin)
LABEL_QUEUE: dict[int, list[dict]] = {}  # { user_id: [ {codigo_tecnico, ocorrencia, codigo_produto, status, qtde}, ... ] }

# ========== AUTO-DELETE E PAINEL ==========

# auto-delete de mensagens comuns do bot
async def _del_msg_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    msg_id = data.get("message_id")
    if chat_id and msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass

async def send_temp(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, seconds: int = 10):
    """Envia msg e agenda auto-delete (padrão 10s)."""
    m = await context.bot.send_message(chat_id=chat_id, text=text)
    try:
        context.job_queue.run_once(_del_msg_job, when=seconds, data={"chat_id": chat_id, "message_id": m.message_id})
    except Exception:
        pass
    return m

# painel compacto de status (uma mensagem editada)
def _stats_text(st: dict) -> str:
    s = st.setdefault("stats", {"recv": 0, "ok": 0, "dup": 0, "bad": 0})
    return (
        "📊 **Status do lote**\n"
        f"📥 Recebidos: {s['recv']} | ✅ Válidos: {s['ok']} \n♻️ Repetidos: {s['dup']} | ❌ Inválidos: {s['bad']}"
    )

# async def panel_upsert(context: ContextTypes.DEFAULT_TYPE, chat_id: int, st: dict):
#     """Cria/edita o painel único de status."""
#     st.setdefault("stats", {"recv": 0, "ok": 0, "dup": 0, "bad": 0})
#     mid = st.get("panel_msg_id")
#     txt = _stats_text(st)
#     if mid:
#         try:
#             await context.bot.edit_message_text(chat_id=chat_id, message_id=mid, text=txt, parse_mode="Markdown")
#             return
#         except Exception:
#             st["panel_msg_id"] = None
#     m = await context.bot.send_message(chat_id=chat_id, text=txt, parse_mode="Markdown")
    # st["panel_msg_id"] = m.message_id

async def panel_upsert(context, chat_id: int, st: dict):
    txt = _stats_text(st)
    mid = st.get("panel_msg_id")
    if mid:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=mid, text=txt, parse_mode="Markdown")
            return
        except Exception:
            # se não conseguir editar, cai para criar novo
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
            st["panel_msg_id"] = None

    # criar novo
    msg = await context.bot.send_message(chat_id=chat_id, text=txt, parse_mode="Markdown")
    st["panel_msg_id"] = msg.message_id

async def reset_lote(uid: int, chat_id: int, context, st: dict | None = None, hard_delete_panel: bool = False):
    """
    Encerra completamente o lote do usuário:
      - apaga mensagem de progresso (se existir)
      - apaga painel (ou marca p/ recriar)
      - zera contadores e estruturas de duplicidade
      - limpa SID para forçar novo lote
    """
    st = st or SESS.setdefault(uid, {})

    # 1) apaga mensagem de progresso (botão "Gerar minuta", contador etc.)
    pmid = st.get("progress_msg_id")
    if pmid:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=pmid)
        except Exception:
            pass
    st.pop("progress_msg_id", None)
    st.pop("progress_sid", None)
    st.pop("progress_text", None)
    st.pop("last_danfe_count", None)
    st.pop("cleanup_ids", None)
    st.pop("warned_incomplete", None)

    # 2) painel
    mid = st.get("panel_msg_id")
    if hard_delete_panel and mid:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
        st["panel_msg_id"] = None
    else:
        # marca para recriar; se existir, quem apaga/edita é o panel_cleanup
        st["panel_msg_id"] = None

    # 3) zera contadores e duplicidade
    st["stats"] = {"recv": 0, "ok": 0, "dup": 0, "bad": 0}
    st["danfe_keys"] = set()
    st["danfe_hashes"] = set()

    # 4) zera controle do lote
    st["sid"] = ""
    st["volbuf"] = ""
    st["data"] = ""

def _panel_finalize_text(st: dict) -> str:
    s = st.get("stats", {"recv": 0, "ok": 0, "dup": 0, "bad": 0})
    return (
        "✅ **Lote finalizado**\n"
        f"📥 Recebidos: {s['recv']} | ✅ Válidos: {s['ok']} | ♻️ Repetidos: {s['dup']} | ❌ Inválidos: {s['bad']}"
    )

async def panel_cleanup(context, chat_id: int, st: dict, mode: str = "finalize", ttl: int = 20):
    """
    mode:
      - 'delete': apaga o painel imediatamente
      - 'finalize': troca o texto por “✅ Lote finalizado” e (opcional) apaga depois de ttl segundos
      - 'keep': não faz nada
    """
    mid = st.get("panel_msg_id")
    if not mid:
        return

    mode = (mode or "finalize").lower()
    if mode == "delete":
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
        st["panel_msg_id"] = None
        return

    if mode == "finalize":
        txt = _panel_finalize_text(st)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=mid, text=txt, parse_mode="Markdown"
            )
        except Exception:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
                st["panel_msg_id"] = None
                return
            except Exception:
                pass

        if ttl and ttl > 0:
            try:
                context.job_queue.run_once(
                    _del_msg_job,
                    when=ttl,
                    data={"chat_id": chat_id, "message_id": mid},
                )
            except Exception:
                pass
        return

    # keep -> não faz nada
    
# --- Fechamento automático do painel/lote ---
async def _maybe_cleanup_lote(context, chat_id: int, uid: int, st: dict):
    """
    Fecha o painel do lote quando:
      - minuta já foi entregue, e
      - já houve decisão de imprimir ou não a minuta, e
      - etiquetas: já houve decisão OU não estão habilitadas.
    """
    # flags do fluxo
    minuta_ok = st.get("minuta_entregue") is True
    minuta_decidida = st.get("minuta_decidida") is True
    labels_enabled = (os.getenv("LABELS_ENABLED", "0") == "1")
    labels_decididas = st.get("labels_decididas") is True

    if not minuta_ok:
        return  # ainda não entregou minuta (não fecha)

    if not minuta_decidida:
        return  # ainda não decidiu sobre impressão da minuta

    if labels_enabled and not labels_decididas:
        return  # etiquetas habilitadas, mas ainda sem decisão

    # chegou aqui? pode limpar painel e zerar lote
    mode = os.getenv("PANEL_CLEANUP_MODE", "finalize")  # 'finalize' | 'delete' | 'keep'
    ttl = int(os.getenv("PANEL_CLEANUP_TTL", "20") or "20")
    try:
        await panel_cleanup(context, chat_id, st, mode=mode, ttl=ttl)
    except Exception:
        pass

    # zera estado do lote para o próximo
    st["panel_msg_id"] = None
    st["stats"] = {"recv": 0, "ok": 0, "dup": 0, "bad": 0}
    st["danfe_keys"] = set()
    st["danfe_hashes"] = set()

    # limpa buffers na SESS do usuário correto (uid!)
    sess = SESS.setdefault(uid, {})
    for k in ("sid", "volbuf", "data"):
        sess[k] = ""
    for k in ("progress_msg_id", "progress_sid", "progress_text",
              "cleanup_ids", "last_danfe_count", "warned_incomplete"):
        sess.pop(k, None)



# ========== LIMPEZA ANTIGA (mantido, mas usado menos) ==========
async def limpar_mensagens_antigas(st, context, chat_id):
    for mid in st.get("cleanup_ids", []):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except:
            pass
    st["cleanup_ids"] = []

# ========== ORIENTAÇÃO DE ENVIO ==========
async def orientar_envio_pdf(context, chat_id):
    # agora como mensagem temporária
    await send_temp(
        context,
        chat_id,
        (
            "⚠️ Este tipo de arquivo não é aceito.\n\n"
            "Para enviar corretamente:\n"
            "1️⃣ Toque no 📎 *clipe de papel* (ou 'Anexar') no campo de mensagem.\n"
            "2️⃣ Escolha *Arquivo* (não Foto nem Galeria).\n"
            "3️⃣ Localize o seu arquivo *.PDF* no celular ou computador.\n"
            "4️⃣ Envie.\n\n"
            "💡 Dica: PDFs de DANFE geralmente vêm do sistema da transportadora ou do emissor da nota."
        ),
    )

# ========== START ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    st = SESS.setdefault(u.id, {})
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)
    qlid, rec = storage.users_find_by_tg(u.id)
    msg_id = st.get("msg_recebimento_id")
    
    st["stats"] = {"recv": 0, "ok": 0, "dup": 0, "bad": 0}
    st["danfe_keys"] = set()
    st["danfe_hashes"] = set()
    st["panel_msg_id"] = None
    st["sid"] = ""
    st["volbuf"] = ""
    st["data"] = ""
    # await panel_upsert(context, update.effective_chat.id, st)


    base = {
        "qlid": "",
        "cidade": "",
        "blocked": False,
        "sid": "",
        "volbuf": "",
        "data": "",
        "msg_recebimento_id": None,
        "stats": {"recv": 0, "ok": 0, "dup": 0, "bad": 0},
        "panel_msg_id": None,
        "danfe_keys": set(),
        "danfe_hashes": set(),
    }

    if rec:
        base["qlid"] = qlid
        base["cidade"] = rec.get("cidade", "")
        base["msg_recebimento_id"] = msg_id
        base["blocked"] = rec.get("blocked", False)
        SESS[u.id] = base
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"👋 Bem-vindo, {u.first_name}!\n\n📎 Envie suas DANFEs em PDF para começar.",
            reply_markup=None
        )
        # await panel_upsert(context, update.effective_chat.id, SESS[u.id])
    else:
        SESS[u.id] = base
        await update.message.reply_text(
            f"Olá, {u.first_name}! Vamos configurar seu acesso.",
            reply_markup=kb_cadastro(),
        )

# ========== OUTROS COMANDOS ==========
async def cmd_minutas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    st = SESS.setdefault(uid, {})
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)
    if not st.get("qlid"):
        msg = await update.message.reply_text("⚠️ Você ainda não está cadastrado. Use /start.")
        st.setdefault("cleanup_ids", []).append(msg.message_id)
        return
    files = storage.list_minutas(st["qlid"])
    if not files:
        msg = await update.message.reply_text("📂 Você ainda não tem minutas geradas.")
        st.setdefault("cleanup_ids", []).append(msg.message_id)
        return
    buttons = [
        [InlineKeyboardButton(f"📄 {os.path.basename(f)}", callback_data=f"minuta_{i}")]
        for i, f in enumerate(files[:5])
    ]
    msg = await update.message.reply_text("Selecione uma minuta:", reply_markup=InlineKeyboardMarkup(buttons))
    st.setdefault("cleanup_ids", []).append(msg.message_id)

async def cmd_alterar_cidade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    st = SESS.get(uid)
    if not st:
        await update.message.reply_text("⚠️ Você ainda não está cadastrado. Use /start.")
        return
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)
    msg = await update.message.reply_text("🏙️ Envie a nova cidade.")
    context.user_data["awaiting_cidade"] = True
    st.setdefault("cleanup_ids", []).append(msg.message_id)

async def cmd_cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    st = SESS.setdefault(uid, {})

    await limpar_mensagens_antigas(st, context, update.effective_chat.id)
    await reset_lote(uid, update.effective_chat.id, context, st, hard_delete_panel=True)

    if st.get("progress_msg_id"):
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=st["progress_msg_id"])
        except:
            pass
        st.pop("progress_msg_id", None)
        st.pop("progress_sid", None)
        st.pop("progress_text", None)

    context.user_data.clear()
    st.update({"awaiting_cidade": False, "sid": "", "volbuf": "", "data": ""})

    msg = await update.message.reply_text("✅ Operação cancelada. Você pode continuar enviando DANFEs ou usar /minutas.")
    st.setdefault("cleanup_ids", []).append(msg.message_id)

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_TELEGRAM_ID:
        await update.message.delete()
        return
    await update.message.reply_text("Admin: /usuarios, /broadcast <msg>")

async def admin_usuarios(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_TELEGRAM_ID:
        return
    users = storage.users_get_all()
    if not users:
        await update.message.reply_text("Nenhum usuário.")
        return
    lines = [
        f"{qlid} | TG:{rec.get('telegram_id')} | Cidade:{rec.get('cidade','')} | Blocked:{rec.get('blocked',False)}"
        for qlid, rec in users.items()
    ]
    await update.message.reply_text("\n".join(lines))

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_TELEGRAM_ID:
        return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("Uso: /broadcast sua mensagem")
        return
    users = storage.users_get_all()
    for qlid, rec in users.items():
        try:
            await context.bot.send_message(rec["telegram_id"], f"[Aviso]: {msg}")
        except:
            pass
    await update.message.reply_text("Broadcast enviado.")

# ========== TEXTO SOLTO ==========
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    uid = msg.from_user.id
    st = SESS.setdefault(uid, {"qlid": "", "cidade": "", "blocked": False, "sid": "", "volbuf": "", "data": ""})
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)
    text = msg.text.strip()

    if context.user_data.get("awaiting_qlid"):
        q = text.upper()
        if not valida_qlid(q):
            await msg.reply_text("❌ QLID inválido. Use o formato AA999999 e envie novamente.")
            return
        st["qlid"] = q
        storage.users_upsert(q, {"telegram_id": uid, "cidade": st.get("cidade", ""), "blocked": False})
        await msg.reply_text("✅ QLID cadastrado.")
        context.user_data["awaiting_qlid"] = False
        context.user_data["awaiting_cidade"] = True
        await msg.reply_text("🏙️ Agora informe a Cidade para preencher na minuta.")
        return

    if context.user_data.get("awaiting_cidade"):
        c = text
        if not valida_cidade(c):
            await msg.reply_text("❌ Cidade inválida. Digite apenas letras e espaços.")
            return
        st["cidade"] = c.title()
        if st.get("qlid"):
            storage.users_upsert(st["qlid"], {"telegram_id": uid, "cidade": st["cidade"], "blocked": False})
        await msg.reply_text(f"🏙️ Cidade definida: {st['cidade']}.\n\nAgora é só enviar as DANFEs (PDFs) para gerar a minuta!")
        context.user_data["awaiting_cidade"] = False
        return

    await msg.delete()

# --- helper: extrai chave 44 (robusto a espaços/quebras)
def _chave44_from_pdf(path: str) -> str | None:
    try:
        from pdfminer_high_level import extract_text  # fallback caso use nome antigo
    except Exception:
        from pdfminer.high_level import extract_text
    try:
        txt = extract_text(path) or ""
    except Exception:
        return None

    T = txt.upper()
    m = re.search(r"\b\d{44}\b", T)
    if m:
        return m.group(0)

    anchor = "CHAVE DE ACESSO"
    pos = T.find(anchor)
    if pos != -1:
        janela = T[pos: pos + 300]
        apenas_dig = re.sub(r"\D+", "", janela)
        if len(apenas_dig) >= 44:
            return apenas_dig[:44]
    return None

# ========== ANEXOS ==========
async def bloquear_anexo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    try:
        await msg.delete()
    finally:
        await orientar_envio_pdf(context, msg.chat.id)

# ===== DOCUMENTOS =====
async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    u = msg.from_user
    st = SESS.setdefault(
        u.id,
        {
            "qlid": "", "cidade": "", "blocked": False, "sid": "", "volbuf": "", "data": "",
            "danfe_keys": set(), "danfe_hashes": set(), "stats": {"recv": 0, "ok": 0, "dup": 0, "bad": 0},
            "panel_msg_id": None
        }
    )
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)

    if st["blocked"]:
        await msg.delete()
        return

    if not st["qlid"] or not st["cidade"]:
        if not st.get("warned_incomplete"):
            await send_temp(context, msg.chat.id, "⚠️ Finalize o cadastro primeiro.", seconds=6)
            st["warned_incomplete"] = True
        await msg.delete()
        return

    doc = msg.document
    if not doc.file_name.lower().endswith(".pdf"):
        await orientar_envio_pdf(context, msg.chat.id)
        await msg.delete()
        return

    if not st["sid"]:
        # zera e recomeça (não apaga nada extra, só garante estado inicial)
        st["stats"] = {"recv": 0, "ok": 0, "dup": 0, "bad": 0}
        st["danfe_keys"] = set()
        st["danfe_hashes"] = set()
        st["panel_msg_id"] = None
        st["last_danfe_count"] = 0
        st["sid"] = storage.new_session(st["qlid"])
        # cria painel zerado já
        await panel_upsert(context, msg.chat.id, st)

    dest = storage.save_pdf(st["qlid"], st["sid"], doc.file_name)
    file = await doc.get_file()
    await file.download_to_drive(dest)

    # contabiliza recebimento e atualiza painel
    st["stats"]["recv"] += 1

    if not danfe_parser.is_danfe(dest):
        st["stats"]["bad"] += 1
        await panel_upsert(context, msg.chat.id, st)
        await send_temp(context, msg.chat.id, "❌ Arquivo não é uma DANFE válida. Tente outro PDF.", seconds=8)
        os.remove(dest)
        await msg.delete()
        return

    # --- dedupe por CHAVE 44 dígitos + fallback por hash
    ch = _chave44_from_pdf(dest)

    import hashlib
    with open(dest, "rb") as _f:
        sha1 = hashlib.sha1(_f.read()).hexdigest()

    st.setdefault("danfe_keys", set())
    st.setdefault("danfe_hashes", set())

    if ch and ch in st["danfe_keys"]:
        st["stats"]["dup"] += 1
        await panel_upsert(context, msg.chat.id, st)
        await send_temp(context, msg.chat.id, "⚠️ DANFE repetida (mesma chave). Ignorando este arquivo.", seconds=6)
        try:
            os.remove(dest)
        except Exception:
            pass
        await msg.delete()
        return

    if (not ch) and (sha1 in st["danfe_hashes"]):
        st["stats"]["dup"] += 1
        await panel_upsert(context, msg.chat.id, st)
        await send_temp(context, msg.chat.id, "⚠️ DANFE repetida (mesmo arquivo). Ignorando este PDF.", seconds=6)
        try:
            os.remove(dest)
        except Exception:
            pass
        await msg.delete()
        return

    if ch:
        st["danfe_keys"].add(ch)
    st["danfe_hashes"].add(sha1)

    # passou: contabiliza válido
    st["stats"]["ok"] += 1
    await panel_upsert(context, msg.chat.id, st)

    count = len([f for f in os.listdir(os.path.dirname(dest)) if f.lower().endswith(".pdf")])
    last_count = st.get("last_danfe_count", 0)
    if count == last_count:
        await msg.delete()
        return
    st["last_danfe_count"] = count

    text = f"📄 Recebidas {count} DANFE{'s' if count > 1 else ''}.\n\nEnvie mais DANFEs ou toque abaixo para gerar a minuta."
    reply_markup = kb_main()

    msg_id = st.get("progress_msg_id")
    sid_ref = st.get("progress_sid")
    sid_now = st["sid"]

    try:
        if msg_id and sid_ref == sid_now and st.get("progress_text") != text:
            await context.bot.edit_message_text(chat_id=msg.chat.id, message_id=msg_id, text=text, reply_markup=reply_markup)
            st["progress_text"] = text
        else:
            raise Exception("Mensagem não modificada ou inválida")
    except:
        new_msg = await context.bot.send_message(chat_id=msg.chat.id, text=text, reply_markup=reply_markup)
        st["progress_msg_id"] = new_msg.message_id
        st["progress_sid"] = sid_now

    await msg.delete()

# ===== BLOQUEIO DE MÍDIA NÃO-PDF =====
async def bloquear_anexo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    try:
        await msg.delete()
    finally:
        await orientar_envio_pdf(context, msg.chat.id)

# ===== Helpers para admin com CallbackQuery =====
class _CQUpdateShim:
    """Shim simples para reutilizar is_admin(update) com uma CallbackQuery."""
    def __init__(self, cq):
        self.effective_user = type("U", (), {"id": cq.from_user.id})()
        self.effective_chat = type("C", (), {"id": cq.message.chat.id})()
        self.effective_message = cq.message

# ===== CALLBACKS =====
# --- Callbacks de impressão de etiquetas (apenas admin) ---
async def on_print_labels(update, context):
    cq = update.callback_query
    await cq.answer()

    if not is_admin(_CQUpdateShim(cq)):
        await send_temp(context, cq.message.chat.id, "⛔️ Impressão de etiquetas restrita a administradores.", seconds=6)
        await safe_delete_message(cq=cq)
        return

    uid = cq.from_user.id
    itens = LABEL_QUEUE.pop(uid, None)
    if not itens:
        await send_temp(context, cq.message.chat.id, "⚠️ Não encontrei itens para impressão deste lote.", seconds=6)
        await safe_delete_message(cq=cq)
        return

    try:
        total = getattr(etiqueta, "print_batch", None)
        if callable(total):
            total = etiqueta.print_batch(itens)
        else:
            # fallback: imprime um a um (aceita 'qtde' OU 'quantidade')
            total = 0
            copias_mult = max(1, int(os.getenv("LABEL_COPIES_PER_QTY", "1")))
            for it in itens:
                q = it.get("qtde", it.get("quantidade", 1))
                copias = max(1, int(q)) * copias_mult
                for _ in range(copias):
                    etiqueta.imprimir_etiqueta(
                        codigo_tecnico=it["codigo_tecnico"],
                        ocorrencia=it["ocorrencia"],
                        codigo_produto=it["codigo_produto"],
                        status=it.get("status", ""),
                        copias=1,
                    )
                    total += 1

        msg = f"✅ Enviado para impressão: {total} etiqueta(s)." if total > 0 else "⚠️ Nenhuma etiqueta foi impressa."
        await send_temp(context, cq.message.chat.id, msg, seconds=8)

    except Exception as e:
        await send_temp(context, cq.message.chat.id, f"❌ Erro ao imprimir: {e}", seconds=8)
    finally:
        await safe_delete_message(cq=cq)

async def on_skip_labels(update, context):
    cq = update.callback_query
    await cq.answer()
    LABEL_QUEUE.pop(cq.from_user.id, None)
    await send_temp(context, cq.message.chat.id, "✅ Ok, não vou imprimir etiqueta.", seconds=6)
    await safe_delete_message(cq=cq)

# —— callback de impressão da MINUTA ——
async def on_print_minuta_cb(update, context):
    """Trata clique nos botões 'Imprimir minuta' / 'Não imprimir' e apaga a mensagem de botões."""
    try:
        cq = update.callback_query
        await cq.answer()
        choice = cq.data.split(":")[1]  # yes | no
        pdf_path = context.user_data.get("last_minuta_pdf")
        msg_txt = "Minuta: opção não reconhecida."
        if choice == "yes":
            if pdf_path:
                try:
                    from services.print_integration import _lp_print, PRINT_ENABLE, is_admin
                except Exception:
                    from services.print_integration import _lp_print, PRINT_ENABLE, is_admin
                if PRINT_ENABLE and is_admin(update):
                    ok, m = _lp_print(str(pdf_path))
                    msg_txt = "🖨️ Minuta enviada para impressão." if ok else f"❌ Falha ao imprimir a minuta: {m}"
                else:
                    msg_txt = "🖨️ Minuta enviada para impressão."
            else:
                msg_txt = "🖨️ Minuta enviada para impressão."
        elif choice == "no":
            msg_txt = "✅ Ok, não vou imprimir minuta."
        try:
            await cq.message.delete()
        except Exception:
            pass
        await send_temp(context, cq.message.chat.id, msg_txt, seconds=8)
    except Exception as e:
        try:
            await send_temp(context, update.effective_chat.id, f"Erro no callback de impressão da minuta: {e}", seconds=8)
        except Exception:
            pass

# ===== PROCESSAR LOTE =====
async def processar_lote(cq, context, st, volumes: int):
    import traceback, os
    uid = cq.from_user.id

    if not isinstance(st, dict):
        st = SESS.setdefault(uid, {})

    chat_id = cq.message.chat.id
    qlid = st.get("qlid")
    sid = st.get("sid")

    if not sid:
        await cq.message.edit_text("Nenhuma DANFE no lote atual.")
        return

    pdfs_dir = f"{storage.user_dir(qlid)}/temp/{sid}/pdfs"
    pdfs = [os.path.join(pdfs_dir, f) for f in os.listdir(pdfs_dir) if f.lower().endswith(".pdf")]
    if not pdfs:
        await cq.message.edit_text("Nenhuma DANFE no lote atual.")
        return

    try:
        await cq.message.reply_text(f"🧐 Lendo {len(pdfs)} DANFEs…")
        header, produtos = danfe_parser.parse_lote(pdfs)

        await cq.message.reply_text("🔍 Fazendo a busca do RAT… isso pode levar alguns minutos.")
        for p in produtos:
            if not p.get("ocorrencia"):
                p["ocorrencia"] = "-"

            rat = None
            key = (p["ocorrencia"], p["codigo_prod"])
            rat = _rat_cache.get(key)

            if p["ocorrencia"] and p["ocorrencia"] != "-" and not rat:
                try:
                    rat = await asyncio.wait_for(
                        asyncio.to_thread(get_rat_for_ocorrencia, p["ocorrencia"], p["codigo_prod"]),
                        timeout=RAT_TIMEOUT + 10
                    )
                except asyncio.TimeoutError:
                    rat = None
                except Exception:
                    rat = None

            if not rat:
                s = (p.get("status") or "").upper()
                if s == "BOM":
                    rat = "GOOD"
                elif s == "DOA":
                    rat = "DOA"
                elif s == "RUIM":
                    rat = ""
                else:
                    rat = "-"

            p["rat"] = rat
            _rat_cache[key] = rat

        out_pdf = storage.output_pdf_path(qlid)
        await cq.message.reply_text("🧾 Preenchendo a minuta e gerando PDF…")
        await asyncio.to_thread(preencher_e_exportar_lote, qlid, st.get("cidade"), header, produtos, st.get("data"), volumes, out_pdf)

        shim = _CQUpdateShim(cq)
        await finalize_minuta_and_print(
            shim,
            context,
            minuta_pdf_path=out_pdf,
            danfe_paths=pdfs,
        )

        # Pergunta de etiquetas (apenas admin + habilitado)
        try:
            is_enabled = os.getenv("LABELS_ENABLED", "0") == "1"
            if is_enabled and is_admin(shim):
                from telegram import InlineKeyboardMarkup, InlineKeyboardButton
                itens = []
                cod_tec_fix = os.getenv("LABEL_CODIGO_TECNICO", "20373280")
                for p in produtos:
                    sraw = (p.get("status") or "").strip().upper()
                    if sraw == "BOM":
                        status_norm = "good"
                    elif sraw == "DOA":
                        status_norm = "doa"
                    elif sraw == "RUIM":
                        status_norm = "bad"
                    else:
                        status_norm = ""

                    itens.append({
                        "codigo_tecnico": cod_tec_fix,
                        "ocorrencia": p.get("ocorrencia") or "",
                        "codigo_produto": p.get("codigo_prod") or "",
                        "status": status_norm,
                        "qtde": int(float(p.get("qtde", 1) or 1)),
                    })

                LABEL_QUEUE[cq.from_user.id] = itens
                await cq.message.reply_text(
                    "🖨️ Deseja imprimir as etiquetas deste lote?",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🖨️ Imprimir etiqueta", callback_data="print_labels"),
                         InlineKeyboardButton("❌ Não imprimir",    callback_data="skip_labels")]
                    ])
                )
        except Exception:
            pass

    except Exception as e:
        await cq.message.reply_text(f"Ocorreu um erro ao gerar a minuta.\nDetalhes: {e}")
        traceback.print_exc()
    finally:
        try:
            if sid:
                storage.finalize_session(qlid, sid)
        except Exception:
            pass
            
        # --- limpeza do painel conforme .env ---
        mode = os.getenv("PANEL_CLEANUP_MODE", "finalize")  # 'finalize' | 'delete' | 'keep'
        ttl = int(os.getenv("PANEL_CLEANUP_TTL", "20") or "20")
        try:
            await panel_cleanup(context, chat_id, st, mode=mode, ttl=ttl)
        except Exception:
            pass
        
        # 2) reset geral do lote (apaga msg de progresso e zera tudo)
        try:
            await reset_lote(uid, chat_id, context, st, hard_delete_panel=(mode == "delete"))
        except Exception:
            pass
            
        # # zera para o próximo lote
        # st["panel_msg_id"] = None
        # st["stats"] = {"recv": 0, "ok": 0, "dup": 0, "bad": 0}
        # st["danfe_keys"] = set()
        # st["danfe_hashes"] = set()

        sess = SESS.setdefault(uid, {})
        for k in ("sid", "volbuf", "data"):
            sess[k] = ""
        for k in ("progress_msg_id","progress_sid","progress_text","cleanup_ids","last_danfe_count","warned_incomplete"):
            sess.pop(k, None)

# ===== MAIN =====
    
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cq = update.callback_query
    await cq.answer()
    uid = cq.from_user.id
    st = SESS.get(uid)

    if not st:
        # Se a mensagem não existir mais, manda nova
        try:
            await cq.message.edit_text("⚠️ Sessão expirada. Envie um PDF para reiniciar.")
        except BadRequest:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Sessão expirada. Envie um PDF para reiniciar.")
        return

    # ----- Cadastro QLID -----
    if cq.data == "cad_qlid":
        try:
            await cq.message.edit_text("🆔 Vamos cadastrar seu QLID!\n\nDigite no formato AA999999 e envie como mensagem.")
        except BadRequest:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🆔 Vamos cadastrar seu QLID!\n\nDigite no formato AA999999 e envie como mensagem."
            )
        context.user_data["awaiting_qlid"] = True
        return

    # ----- Cadastro Cidade -----
    if cq.data == "cad_cidade":
        try:
            await cq.message.edit_text("🏙️ Envie sua Cidade (apenas letras e espaços).")
        except BadRequest:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🏙️ Envie sua Cidade (apenas letras e espaços)."
            )
        context.user_data["awaiting_cidade"] = True
        return

    # ----- Alterar Cidade -----
    if cq.data == "alterar_cidade":
        await cmd_alterar_cidade(update, context)
        return

    # ----- Minhas Minutas (listar) -----
    if cq.data == "minhas_minutas":
        files = storage.list_minutas(st["qlid"])
        if not files:
            try:
                await cq.message.edit_text("📂 Você ainda não tem minutas geradas.\n\n📎 Envie suas DANFEs em PDF para começar.")
            except BadRequest:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="📂 Você ainda não tem minutas geradas.\n\n📎 Envie suas DANFEs em PDF para começar."
                )
            return

        buttons = [[InlineKeyboardButton(f"📄 {os.path.basename(f)}", callback_data=f"minuta_{i}")]
                   for i, f in enumerate(files[:5])]
        try:
            await cq.message.edit_text("Selecione uma minuta:", reply_markup=InlineKeyboardMarkup(buttons))
        except BadRequest:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Selecione uma minuta:",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        return

    # ----- Clique numa minuta específica -----
    if cq.data.startswith("minuta_"):
        idx = int(cq.data.split("_")[1])
        files = storage.list_minutas(st["qlid"])
        if idx < len(files):
            # Tenta apagar a mensagem da lista, mas ignora se não existir
            try:
                await cq.message.delete()
            except BadRequest as e:
                if "message to delete not found" not in str(e).lower():
                    # Se for outro erro, relança
                    raise

            # Envia o PDF
            try:
                with open(files[idx], "rb") as f:
                    await cq.message.reply_document(f, filename=os.path.basename(files[idx]))
            except FileNotFoundError:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Não encontrei a minuta: {os.path.basename(files[idx])}"
                )
            except Exception as e:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"⚠️ Erro ao enviar a minuta: {e}"
                )
        return

    # ----- Gerar Minuta (pedir data) -----
    if cq.data == "gerar_minuta":
        if not st.get("sid"):
            try:
                await cq.message.edit_text("⚠️ Você ainda não enviou nenhuma DANFE. Envie seus PDFs primeiro.")
            except BadRequest:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ Você ainda não enviou nenhuma DANFE. Envie seus PDFs primeiro."
                )
            return

        pdfs_dir = f"{storage.user_dir(st['qlid'])}/temp/{st['sid']}/pdfs"
        pdfs = [f for f in os.listdir(pdfs_dir) if f.lower().endswith(".pdf")] if os.path.exists(pdfs_dir) else []
        if not pdfs:
            try:
                await cq.message.edit_text("⚠️ Nenhuma DANFE encontrada no lote atual. Envie os arquivos antes de gerar a minuta.")
            except BadRequest:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ Nenhuma DANFE encontrada no lote atual. Envie os arquivos antes de gerar a minuta."
                )
            return

        try:
            await cq.message.edit_text("🗓️ Escolha a data:", reply_markup=kb_datas())
        except BadRequest:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🗓️ Escolha a data:",
                reply_markup=kb_datas()
            )
        return

    # ----- Data escolhida -----
    if cq.data.startswith("data_"):
        raw_data = cq.data[5:]
        try:
            data_formatada = datetime.strptime(raw_data, "%Y-%m-%d").strftime("%d/%m/%Y")
        except:
            data_formatada = raw_data  # fallback

        st["data"] = raw_data
        st["volbuf"] = ""
        try:
            await cq.message.edit_text(
                f"📅 Data escolhida: {data_formatada}\nAgora informe os volumes:",
                reply_markup=kb_volumes()
            )
        except BadRequest:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"📅 Data escolhida: {data_formatada}\nAgora informe os volumes:",
                reply_markup=kb_volumes()
            )
        return

    # ----- Teclado de volumes -----
    if cq.data.startswith("vol_"):
        # Formata a data salva para exibir
        try:
            data_formatada = datetime.strptime(st["data"], "%Y-%m-%d").strftime("%d/%m/%Y")
        except:
            data_formatada = st["data"]

        if cq.data == "vol_del":
            st["volbuf"] = st.get("volbuf", "")[:-1]
        elif cq.data == "vol_ok":
            vol = st.get("volbuf", "0")
            if not vol or vol == "0":
                try:
                    await cq.message.edit_text(
                        f"📅 Data escolhida: {data_formatada}\nVolumes deve ser inteiro > 0.",
                        reply_markup=kb_volumes(st["volbuf"])
                    )
                except BadRequest:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=f"📅 Data escolhida: {data_formatada}\nVolumes deve ser inteiro > 0.",
                        reply_markup=kb_volumes(st["volbuf"])
                    )
                return
            await cq.message.edit_reply_markup(reply_markup=None)
            await processar_lote(cq, context, st, int(vol))
            return
        else:
            st["volbuf"] = (st.get("volbuf", "") + cq.data.split("_")[1])[:4]

        try:
            await cq.message.edit_text(
                f"📅 Data escolhida: {data_formatada}\nVolumes: {st['volbuf'] or '-'}",
                reply_markup=kb_volumes(st["volbuf"])
            )
        except BadRequest:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"📅 Data escolhida: {data_formatada}\nVolumes: {st['volbuf'] or '-'}",
                reply_markup=kb_volumes(st["volbuf"])
            )
        return
    
# # —— callback de impressão da MINUTA ——
# async def on_print_minuta_cb(update, context):
#     """Trata clique nos botões 'Imprimir minuta' / 'Não imprimir' e apaga a mensagem de botões."""
#     try:
#         cq = update.callback_query
#         await cq.answer()
#         choice = cq.data.split(":")[1]  # yes | no
#         # pega caminho salvo no fluxo
#         pdf_path = context.user_data.get("last_minuta_pdf")
#         msg_txt = "Minuta: opção não reconhecida."
#         if choice == "yes":
#             if pdf_path:
#                 try:
#                     from services.print_integration import _lp_print, PRINT_ENABLE, is_admin
#                 except Exception:
#                     # caminho alternativo se o pacote estiver plano
#                     from services.print_integration import _lp_print, PRINT_ENABLE, is_admin
#                 if PRINT_ENABLE and is_admin(update):
#                     ok, m = _lp_print(str(pdf_path))
#                     msg_txt = "🖨️ Minuta enviada para impressão." if ok else f"❌ Falha ao imprimir a minuta: {m}"
#                 else:
#                     msg_txt = "🖨️ Minuta enviada para impressão."
#             else:
#                 msg_txt = "🖨️ Minuta enviada para impressão."
#         elif choice == "no":
#             msg_txt = "✅ Ok, não vou imprimir minuta."
#         # apaga a mensagem de botões
#         try:
#             await cq.message.delete()
#         except Exception:
#             pass
#         # confirma em nova mensagem
#         await cq.message.chat.send_message(msg_txt)
#     except Exception as e:
#         try:
#             await update.effective_chat.send_message(f"Erro no callback de impressão da minuta: {e}")
#         except Exception:
#             pass


# ===== PROCESSAR LOTE =====
async def processar_lote(cq, context, st, volumes: int):
    import traceback, os
    uid = cq.from_user.id

    # Garante que 'st' é um dict de sessão
    if not isinstance(st, dict):
        st = SESS.setdefault(uid, {})

    chat_id = cq.message.chat.id
    qlid = st.get("qlid")
    sid = st.get("sid")

    if not sid:
        await cq.message.edit_text("Nenhuma DANFE no lote atual.")
        return

    pdfs_dir = f"{storage.user_dir(qlid)}/temp/{sid}/pdfs"
    pdfs = [os.path.join(pdfs_dir, f) for f in os.listdir(pdfs_dir) if f.lower().endswith(".pdf")]
    if not pdfs:
        await cq.message.edit_text("Nenhuma DANFE no lote atual.")
        return

    try:
        await cq.message.reply_text(f"🧐 Lendo {len(pdfs)} DANFEs…")
        header, produtos = danfe_parser.parse_lote(pdfs)

        await cq.message.reply_text("🔍 Fazendo a busca do RAT… isso pode levar alguns minutos.")
        for p in produtos:
            # Se não tem ocorrência, define como "-"
            if not p.get("ocorrencia"):
                p["ocorrencia"] = "-"

            rat = None
            key = (p["ocorrencia"], p["codigo_prod"])
            rat = _rat_cache.get(key)

            if p["ocorrencia"] and p["ocorrencia"] != "-" and not rat:
                try:
                    rat = await asyncio.wait_for(
                        asyncio.to_thread(get_rat_for_ocorrencia, p["ocorrencia"], p["codigo_prod"]),
                        timeout=RAT_TIMEOUT + 10
                    )
                except asyncio.TimeoutError:
                    rat = None
                except Exception:
                    rat = None

            # Fallbacks
            if not rat:
                s = (p.get("status") or "").upper()
                if s == "BOM":
                    rat = "GOOD"
                elif s == "DOA":
                    rat = "DOA"
                elif s == "RUIM":
                    rat = ""
                else:
                    rat = "-"

            p["rat"] = rat
            _rat_cache[key] = rat

        out_pdf = storage.output_pdf_path(qlid)
        await cq.message.reply_text("🧾 Preenchendo a minuta e gerando PDF…")
        await asyncio.to_thread(preencher_e_exportar_lote, qlid, st.get("cidade"), header, produtos, st.get("data"), volumes, out_pdf)

        # === NOVO: envia (e imprime se habilitado) usando a integração ===
        # Passamos também a lista de DANFEs para, se configurado, juntar no final.
        shim = _CQUpdateShim(cq)
        await finalize_minuta_and_print(
            shim,
            context,
            minuta_pdf_path=out_pdf,
            danfe_paths=pdfs,
        )

        # --- Pergunta de etiquetas (apenas admin + habilitado) ---
        try:
            is_enabled = os.getenv("LABELS_ENABLED", "0") == "1"
            if is_enabled and is_admin(shim):
                from telegram import InlineKeyboardMarkup, InlineKeyboardButton  # garante import local
                itens = []
                cod_tec_fix = os.getenv("LABEL_CODIGO_TECNICO", "20373280")
                for p in produtos:
                    # status do DANFE (BOM/DOA/RUIM) -> good/doa/bad
                    sraw = (p.get("status") or "").strip().upper()
                    if sraw == "BOM":
                        status_norm = "good"
                    elif sraw == "DOA":
                        status_norm = "doa"
                    elif sraw == "RUIM":
                        status_norm = "bad"
                    else:
                        status_norm = ""  # não marca

                    itens.append({
                        "codigo_tecnico": cod_tec_fix,
                        "ocorrencia": p.get("ocorrencia") or "",
                        "codigo_produto": p.get("codigo_prod") or "",
                        "status": status_norm,
                        "quantidade": int(float(p.get("qtde", 1) or 1)),
                    })

                LABEL_QUEUE[cq.from_user.id] = itens
                # await cq.message.reply_text(
                #     "🖨️ Deseja imprimir as etiquetas deste lote?",
                #     reply_markup=InlineKeyboardMarkup([
                #         [InlineKeyboardButton("🖨️ Imprimir agora", callback_data="print_labels")],
                #         [InlineKeyboardButton("❌ Não imprimir",    callback_data="skip_labels")],
                #     ])
                # )
                await cq.message.reply_text(
                    "🖨️ Deseja imprimir as etiquetas deste lote?",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🖨️ Imprimir etiqueta", callback_data="print_labels"),
                        InlineKeyboardButton("❌ Não imprimir",    callback_data="skip_labels")]
                    ])
                )

        except Exception:
            # nunca derruba o fluxo por causa de etiqueta
            pass

    except Exception as e:
        await cq.message.reply_text(f"Ocorreu um erro ao gerar a minuta. Detalhes: {e}")
        traceback.print_exc()
    finally:
        # Limpeza segura
        try:
            if sid:
                storage.finalize_session(qlid, sid)
        except Exception:
            pass

        sess = SESS.setdefault(uid, {})
        for k in ("sid", "volbuf", "data"):
            sess[k] = ""
        for k in ("progress_msg_id","progress_sid","progress_text","cleanup_ids","last_danfe_count","warned_incomplete"):
            sess.pop(k, None)

# ===== MAIN =====
def main():
    app = ApplicationBuilder()\
        .token(BOT_TOKEN)\
        .post_init(_post_init)\
        .post_shutdown(_post_shutdown)\
        .build()

    # Comandos
    app.add_handler(CommandHandler("start", start))
    # app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_print_minuta_cb, pattern=r"^printminuta:(yes|no)$"))
    app.add_handler(CommandHandler("minutas", cmd_minutas))
    app.add_handler(CommandHandler("alterar", cmd_alterar_cidade))
    app.add_handler(CommandHandler("cancelar", cmd_cancelar))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("usuarios", admin_usuarios))
    app.add_handler(CommandHandler("broadcast", admin_broadcast))
    app.add_handler(CommandHandler("health", cmd_health))

    # NOVO: comandos utilitários
    app.add_handler(CommandHandler("meuid", meuid_cmd))
    # app.add_handler(CommandHandler(["print", "imprimir"], print_cmd))
    
    # Callbacks de impressão de etiquetas (apenas admin)
    app.add_handler(CallbackQueryHandler(on_print_labels, pattern=r"^print_labels$"))
    app.add_handler(CallbackQueryHandler(on_skip_labels,   pattern=r"^skip_labels$"))

    # Callbacks
    app.add_handler(CallbackQueryHandler(on_callback))

    # Mensagens
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # Bloqueio de mídia não-PDF
    app.add_handler(MessageHandler(filters.PHOTO, bloquear_anexo))
    app.add_handler(MessageHandler(filters.VIDEO, bloquear_anexo))
    app.add_handler(MessageHandler(filters.AUDIO, bloquear_anexo))
    app.add_handler(MessageHandler(filters.VOICE, bloquear_anexo))
    app.add_handler(MessageHandler(filters.ANIMATION, bloquear_anexo))

    app.run_polling()

if __name__ == "__main__":
    main()
