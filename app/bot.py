import os, asyncio, traceback, logging
from datetime import datetime

# opcional: sd_notify (só funciona quando rodando via systemd Type=notify)
try:
    from sdnotify import SystemdNotifier
except Exception:  # se lib não instalada, segue sem sd_notify
    SystemdNotifier = None

from telegram.error import BadRequest
from telegram import (
    Update,
    InputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
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
LABEL_QUEUE: dict[int, list[dict]] = {}  # { user_id: [ {codigo_tecnico, ocorrencia, codigo_produto, status, quantidade}, ... ] }

async def limpar_mensagens_antigas(st, context, chat_id):
    for mid in st.get("cleanup_ids", []):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except:
            pass
    st["cleanup_ids"] = []

async def orientar_envio_pdf(context, chat_id):
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "⚠️ Este tipo de arquivo não é aceito.\n\n"
            "Para enviar corretamente:\n"
            "1️⃣ Toque no 📎 *clipe de papel* (ou 'Anexar') no campo de mensagem.\n"
            "2️⃣ Escolha *Arquivo* (não Foto nem Galeria).\n"
            "3️⃣ Localize o seu arquivo *.PDF* no celular ou computador.\n"
            "4️⃣ Envie.\n\n"
            "💡 Dica: PDFs de DANFE geralmente vêm do sistema da transportadora ou do emissor da nota."
        ),
        parse_mode="Markdown"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    st = SESS.setdefault(u.id, {})
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)
    qlid, rec = storage.users_find_by_tg(u.id)
    msg_id = st.get("msg_recebimento_id")

    if rec:
        SESS[u.id] = {
            "qlid": qlid,
            "cidade": rec.get("cidade", ""),
            "blocked": rec.get("blocked", False),
            "sid": "",
            "volbuf": "",
            "data": "",
            "msg_recebimento_id": msg_id,
        }
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"👋 Bem-vindo, {u.first_name}!\n\n📎 Envie suas DANFEs em PDF para começar.",
            reply_markup=None
        )
    else:
        SESS[u.id] = {
            "qlid": "",
            "cidade": "",
            "blocked": False,
            "sid": "",
            "volbuf": "",
            "data": "",
            "msg_recebimento_id": None,
        }
        await update.message.reply_text(
            f"Olá, {u.first_name}! Vamos configurar seu acesso.",
            reply_markup=kb_cadastro(),
        )

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

# ===== DOCUMENTOS =====
async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    u = msg.from_user
    st = SESS.setdefault(u.id, {"qlid": "", "cidade": "", "blocked": False, "sid": "", "volbuf": "", "data": ""})
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)

    if st["blocked"]:
        await msg.delete()
        return

    if not st["qlid"] or not st["cidade"]:
        if not st.get("warned_incomplete"):
            await context.bot.send_message(chat_id=msg.chat.id, text="⚠️ Finalize o cadastro primeiro.", reply_markup=kb_cadastro())
            st["warned_incomplete"] = True
        await msg.delete()
        return

    doc = msg.document
    if not doc.file_name.lower().endswith(".pdf"):
        await orientar_envio_pdf(context, msg.chat.id)
        await msg.delete()
        return

    if not st["sid"]:
        st["sid"] = storage.new_session(st["qlid"])

    dest = storage.save_pdf(st["qlid"], st["sid"], doc.file_name)
    file = await doc.get_file()
    await file.download_to_drive(dest)

    if not danfe_parser.is_danfe(dest):
        await context.bot.send_message(chat_id=msg.chat.id, text="❌ Arquivo não é uma DANFE válida. Tente outro PDF.")
        os.remove(dest)
        await msg.delete()
        return

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

# ===== CALLBACKS =====
# --- Callbacks de impressão de etiquetas (apenas admin) ---
async def on_print_labels(update, context):
    cq = update.callback_query
    await cq.answer()
    uid = cq.from_user.id
    itens = LABEL_QUEUE.pop(uid, None)
    if not itens:
        return await cq.message.reply_text("⚠️ Não encontrei itens para impressão deste lote.")

    try:
        # usa print_batch se existir; senão, cai no fallback interno
        total = getattr(etiqueta, "print_batch", None)
        if callable(total):
            total = etiqueta.print_batch(itens)
        else:
            # fallback: imprime um a um respeitando quantidade
            total = 0
            copias_mult = max(1, int(os.getenv("LABEL_COPIES_PER_QTY", "1")))
            for it in itens:
                copias = max(1, int(it.get("quantidade", 1))) * copias_mult
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
        await cq.message.reply_text(msg)

    except Exception as e:
        await cq.message.reply_text(f"❌ Erro ao imprimir: {e}")

async def on_skip_labels(update, context):
    cq = update.callback_query
    await cq.answer()
    LABEL_QUEUE.pop(cq.from_user.id, None)
    await cq.message.reply_text("Ok, não vou imprimir etiquetas.")
    
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

        with open(out_pdf, "rb") as f:
            await cq.message.reply_document(
                InputFile(f, filename=os.path.basename(out_pdf)),
                caption="✅ Sua minuta está pronta.\n\n📩 Envie mais DANFEs para gerar outra minuta."
            )

        # --- Pergunta de etiquetas (apenas admin + habilitado) ---
        try:
            is_enabled = os.getenv("LABELS_ENABLED", "0") == "1"
            is_admin   = str(cq.from_user.id) == str(os.getenv("ADMIN_TELEGRAM_ID", ""))
            if is_enabled and is_admin:
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
                await cq.message.reply_text(
                    "🖨️ Deseja imprimir as etiquetas deste lote?",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🖨️ Imprimir agora", callback_data="print_labels")],
                        [InlineKeyboardButton("❌ Não imprimir",    callback_data="skip_labels")],
                    ])
                )
        except Exception:
            # nunca derruba o fluxo por causa de etiqueta
            pass

    except Exception as e:
        await cq.message.reply_text(f"Ocorreu um erro ao gerar a minuta.\nDetalhes: {e}")
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
    # app = ApplicationBuilder().token(BOT_TOKEN).build()
    app = ApplicationBuilder()\
        .token(BOT_TOKEN)\
        .post_init(_post_init)\
        .post_shutdown(_post_shutdown)\
        .build()

    # Comandos
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("minutas", cmd_minutas))
    app.add_handler(CommandHandler("alterar", cmd_alterar_cidade))
    app.add_handler(CommandHandler("cancelar", cmd_cancelar))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("usuarios", admin_usuarios))
    app.add_handler(CommandHandler("broadcast", admin_broadcast))
    app.add_handler(CommandHandler("health", cmd_health))
    
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
