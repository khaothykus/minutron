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

from config import (
    BOT_TOKEN,
    ADMIN_TELEGRAM_ID,
    # CODIGO_SAP,
    # MODO_IMPRESSAO,
    # NOME_IMPRESSORA,
)
from services import storage, danfe_parser
# from services.excel_filler_spire import preencher_e_exportar_lote
from services.excel_filler_uno import preencher_e_exportar_lote
from services.rat_search import get_rat_for_ocorrencia
from services.validators import valida_qlid, valida_cidade
from keyboards import kb_cadastro, kb_main, kb_datas, kb_volumes

# from etiqueta import (
#     gerar_comando_epl2,
#     enviar_comando_epl2,
#     # enviar_comando_usb,
# )

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

# =========================================
# HEALTH ENDPOINT (/health)
# =========================================
async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text(f"ok {datetime.now().isoformat(timespec='seconds')}")
    except Exception:
        pass

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

    # # Impressão de etiquetas
    # if cq.data == "imprimir_etiquetas":
    #     produtos = context.user_data.get("etiqueta_produtos", [])
    #     ocorrencia = context.user_data.get("etiqueta_ocorrencia", "-")
    #     for p in produtos:
    #         for _ in range(int(p.get("quantidade", 1))):
    #             cmd = gerar_comando_epl2(
    #                 ocorrencia=ocorrencia,
    #                 produto=p["codigo_prod"],
    #                 status=p["rat"]  # já convertido para GOOD/BAD/DOA
    #             )
    #             # enviar_comando_epl2(cmd)
    #             if MODO_IMPRESSAO == "USB":
    #                 #enviar_comando_usb(cmd, NOME_IMPRESSORA)
    #                 pass
    #             else:
    #                 enviar_comando_epl2(cmd)
    #     await cq.message.reply_text("✅ Etiquetas enviadas para impressão.")

    # elif cq.data == "nao_imprimir_etiquetas":
    #     await cq.message.reply_text("Ok, etiquetas não foram impressas.")


# ===== PROCESSAR LOTE =====
async def processar_lote(cq, context, st, volumes: int):
    chat_id = cq.message.chat.id
    qlid = st["qlid"]
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
                        timeout=RAT_TIMEOUT + 10  # um respiro acima do flow interno
                    )
                except asyncio.TimeoutError:
                    rat = None  # deixa cair nos fallbacks abaixo
                except Exception:
                    rat = None

            # fallbacks (como já tinha):
            if not rat:
                if p["status"] == "BOM":
                    rat = "GOOD"
                elif p["status"] == "DOA":
                    rat = "DOA"
                elif p["status"] == "RUIM":
                    rat = ""
                else:
                    rat = "-"

            p["rat"] = rat
            _rat_cache[key] = rat

        out_pdf = storage.output_pdf_path(qlid)
        await cq.message.reply_text("🧾 Preenchendo a minuta e gerando PDF…")
        await asyncio.to_thread(preencher_e_exportar_lote, qlid, st["cidade"], header, produtos, st["data"], volumes, out_pdf)

        with open(out_pdf, "rb") as f:
            await cq.message.reply_document(
                InputFile(f, filename=os.path.basename(out_pdf)),
                caption="✅ Sua minuta está pronta.\n\n📩 Envie mais DANFEs para gerar outra minuta."
            )
        # # ===== Impressão de etiquetas (somente para ADMIN) =====
        # if cq.from_user.id == ADMIN_TELEGRAM_ID:
        #     await cq.message.reply_text("🖨️ Deseja imprimir as etiquetas?", reply_markup=InlineKeyboardMarkup([
        #         [InlineKeyboardButton("Sim", callback_data="imprimir_etiquetas")],
        #         [InlineKeyboardButton("Não", callback_data="nao_imprimir_etiquetas")]
        #     ]))
        #     context.user_data["etiqueta_produtos"] = produtos
        #     context.user_data["etiqueta_ocorrencia"] = header.get("ocorrencia", "-")

    except Exception as e:
        await cq.message.reply_text(f"Ocorreu um erro ao gerar a minuta.\nDetalhes: {e}")
        traceback.print_exc()
    finally:
        storage.finalize_session(qlid, sid)
        st["sid"] = ""
        st["volbuf"] = ""
        st["data"] = ""
        st.pop("progress_msg_id", None)
        st.pop("progress_sid", None)
        st.pop("progress_text", None)
        st.pop("cleanup_ids", None)
        st.pop("last_danfe_count", None)
        st.pop("warned_incomplete", None)

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
