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

from config import (
    BOT_TOKEN,
    ADMIN_TELEGRAM_ID,
)
import os

from services import storage, danfe_parser
# from services.excel_filler_spire import preencher_e_exportar_lote
from services.excel_filler_uno import preencher_e_exportar_lote
from services.rat_search import get_rat_for_ocorrencia
from services.validators import valida_qlid, valida_cidade
from keyboards import kb_cadastro, kb_main, kb_datas, kb_volumes
from services import etiqueta   # impressão de etiquetas

import pypdfium2 as pdfium
from io import BytesIO
from telegram import InputMediaPhoto

from datetime import datetime as _dt
import re as _re, os as _os

from services import transportadora_db
from services.storage import user_set_transportadora_padrao
from telegram.ext import MessageHandler, filters

# === Configurações extras de comportamento ===
ENABLE_TRANSPORTADORA_PAINEL = False  # se True, mostra no painel a transportadora em uso

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
        
def _fmt_br_date(v: str | None) -> str:
    if not v:
        return "—"
    try:
        # espera "YYYY-MM-DD" que é como você guarda em st["data"]
        return _dt.strptime(v, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return v  # se vier em outro formato, devolve como está

def _escolher_transportadora_para_lote(header: dict, user_cfg: dict) -> tuple[str | None, list[str], bool]:
    """
    Retorna:
      (escolhida, opcoes_detectadas, precisa_confirmar)

    - Não faz pergunta, só sugere.
    - A lógica fina de "como perguntar" fica em processar_lote.
    """
    opcoes = header.get("_transportadoras_lote") or []
    opcoes = [o.strip() for o in opcoes if o and o.strip()]
    uniq: list[str] = []
    for o in opcoes:
        if o not in uniq:
            uniq.append(o)

    atual_nf = (header.get("transportador") or "").strip()
    padrao = (user_cfg.get("transportadora_padrao") or "").strip()

    # sem padrão do usuário
    if not padrao:
        if uniq:
            # sugere primeira e pede confirmação
            return uniq[0], uniq, True
        return (atual_nf or None), [], False

    # com padrão

    # nenhuma nas NFs -> usa padrão
    if not uniq:
        return padrao, [], False

    # se todas as encontradas batem com o padrão -> ok
    if all(t.upper() == padrao.upper() for t in uniq):
        return padrao, uniq, False

    # caso clássico: só 1 encontrada, diferente do padrão -> suspeito (NF emprestada)
    if len(uniq) == 1 and uniq[0].upper() != padrao.upper():
        # sugere uso do padrão, mas marca para confirmar (UI decide)
        return padrao, uniq, True

    # múltiplas diferentes, mas se padrão está entre elas -> usa padrão, OK
    for t in uniq:
        if t.upper() == padrao.upper():
            return padrao, uniq, False

    # múltiplas todas diferentes -> sugere padrão, mas pede confirmação
    return padrao, uniq, True


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

async def step_replace(context, chat_id: int, st: dict, text: str) -> None:
    """Apaga a box de etapa anterior (se existir) e cria a nova."""
    mid = st.pop("step_msg_id", None)
    if mid:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
    m = await context.bot.send_message(chat_id=chat_id, text=text)
    st["step_msg_id"] = m.message_id


async def step_clear(context, chat_id: int, st: dict) -> None:
    """Apaga qualquer box de etapa remanescente."""
    mid = st.pop("step_msg_id", None)
    if mid:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass


def _stats_text(st: dict) -> str:
    s = st.setdefault("stats", {"recv": 0, "ok": 0, "dup": 0, "bad": 0})
    linhas = [
        "📊 **Status do lote**",
        f"📥 Recebidos: {s['recv']} | ✅ Válidos: {s['ok']} \n♻️ Repetidos: {s['dup']} | ❌ Inválidos: {s['bad']}",
    ]

    # Data / Volumes (se já definidos)
    data_iso = st.get("data")
    vols = st.get("volumes")
    if data_iso or vols is not None:
        linhas.append(f"📅 Data escolhida: {_fmt_br_date(data_iso)}")
        linhas.append(f"📦 Volumes: {vols if vols is not None else '—'}")

    # # Transportadora em uso (se já decidida para este lote)
    # tp = (st.get("transportadora_escolhida") or "").strip()
    # if tp:
    #     # linhas.append(f"🚚 Transportadora em uso: {tp}")
    #     linhas.append(f"🚚 Transportadora: {tp}")


    return "\n".join(linhas)


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
    st["cleanup_ids"] = {}

    # 4) zera controle do lote
    st["sid"] = ""
    st["volbuf"] = ""
    st["data"] = ""

def _panel_finalize_text(st: dict) -> str:
    s = st.get("stats", {"recv": 0, "ok": 0, "dup": 0, "bad": 0})
    linhas = [
        "✅ **Lote finalizado**",
        f"📥 Recebidos: {s['recv']} | ✅ Válidos: {s['ok']} \n♻️ Repetidos: {s['dup']} | ❌ Inválidos: {s['bad']}",
    ]

    data_iso = st.get("data")
    vols = st.get("volumes")
    if data_iso or vols is not None:
        linhas.append(f"📅 Data escolhida: {_fmt_br_date(data_iso)}")
        linhas.append(f"📦 Volumes: {vols if vols is not None else '—'}")

    # tp = (st.get("transportadora_escolhida") or "").strip()
    # if tp:
    #     # linhas.append(f"🚚 Transportadora usada na minuta: {tp}")
    #     linhas.append(f"🚚 Transportadora: {tp}")

    return "\n".join(linhas)

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
                    _del_msg_job, when=ttl, data={"chat_id": chat_id, "message_id": mid}
                )
            except Exception:
                pass
        return
    # keep: não faz nada

async def _maybe_cleanup_lote(context, chat_id: int, uid: int, st: dict):
    """Fecha o painel quando as condições de conclusão do lote foram satisfeitas."""
    import os
    minuta_ok = st.get("minuta_entregue") is True
    minuta_decidida = st.get("minuta_decidida") is True
    labels_enabled = (os.getenv("LABELS_ENABLED", "0") == "1")
    labels_decididas = st.get("labels_decididas") is True

    # ainda não pode fechar?
    if not minuta_ok or not minuta_decidida or (labels_enabled and not labels_decididas):
        return

    # limpa/encerra painel conforme .env
    mode = os.getenv("PANEL_CLEANUP_MODE", "finalize")  # finalize|delete|keep
    ttl = int(os.getenv("PANEL_CLEANUP_TTL", "20") or "20")
    try:
        await panel_cleanup(context, chat_id, st, mode=mode, ttl=ttl)
    except Exception:
        pass

    # zera estado do lote
    st["panel_msg_id"] = None
    st["stats"] = {"recv": 0, "ok": 0, "dup": 0, "bad": 0}
    st["danfe_keys"] = set()
    st["danfe_hashes"] = set()
    # st["cleanup_ids"] = {}

    # zera buffers/sid no SESS do **uid**
    sess = SESS.setdefault(uid, {})
    for k in ("sid", "volbuf", "data", "volumes"):
        sess[k] = ""
    for k in ("progress_msg_id", "progress_sid", "progress_text",
              "cleanup_ids", "last_danfe_count", "warned_incomplete"):
        sess.pop(k, None)

async def _send_minuta_preview(context, chat_id: int, minuta_pdf: str, pages=(0,1), scale=2):
    """
    Renderiza páginas da minuta (somente do PDF da minuta) e envia como media group
    (ou uma única foto se só 1 página pedida).
    pages usa índice zero-based (0 = primeira página).
    """
    images = []
    try:
        doc = pdfium.PdfDocument(minuta_pdf)
        for p in pages:
            if p < 0 or p >= len(doc):
                continue
            page = doc.get_page(p)
            bitmap = page.render(scale=scale)  # 2x ~ 144dpi
            pil = bitmap.to_pil()
            bio = BytesIO()
            pil.save(bio, format="PNG")
            bio.seek(0)
            images.append(bio)
            page.close()
        doc.close()
    except Exception:
        return

    if not images:
        return

    if len(images) == 1:
        await context.bot.send_photo(chat_id=chat_id, photo=images[0], caption="Prévia da minuta (páginas iniciais)")
    else:
        # até 10 imagens por grupo; aqui só 2
        from telegram import InputMediaPhoto
        media = [InputMediaPhoto(images[0])]
        for im in images[1:]:
            media.append(InputMediaPhoto(im))
        await context.bot.send_media_group(chat_id=chat_id, media=media)
        
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
            "⚠️ Este tipo de arquivo não é aceito."
            # "\n\nPara enviar corretamente:\n"
            # "1️⃣ Toque no 📎 *clipe de papel* (ou 'Anexar') no campo de mensagem.\n"
            # "2️⃣ Escolha *Arquivo* (não Foto nem Galeria).\n"
            # "3️⃣ Localize o seu arquivo *.PDF* no celular ou computador.\n"
            # "4️⃣ Envie.\n\n"
            # "💡 Dica: PDFs de DANFE geralmente vêm do sistema da transportadora ou do emissor da nota."
        ),
    )


async def handle_tp_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Quando o usuário está em modo waiting_tp_manual, interpreta a mensagem como nome da transportadora."""
    if not update.message:
        return

    uid = update.effective_user.id
    st = SESS.setdefault(uid, {})

    if not st.get("waiting_tp_manual"):
        # não estamos nesse fluxo -> deixa outros handlers cuidarem (ou ignora)
        return

    text = (update.message.text or "").strip()
    if not text:
        await step_replace(context, update.effective_chat.id, st,
                           "Por favor, envie o nome da transportadora em texto.")
        return

    sid = st.get("pending_tp_sid")
    volumes = st.get("pending_tp_volumes") or 1
    if not sid:
        # perdeu o lote
        st["waiting_tp_manual"] = False
        await step_replace(context, update.effective_chat.id, st,
                           "Esse lote não está mais ativo. Envie as DANFEs novamente.")
        return

    # tenta normalizar com base no banco
    nome = (transportadora_db.best_match(text) or text).strip().upper()
    if not nome:
        await step_replace(context, update.effective_chat.id, st,
                           "Nome inválido. Tente novamente com o nome da transportadora.")
        return

    # salva como padrão do usuário
    user_set_transportadora_padrao(uid, nome)
    transportadora_db.add(nome)

    # fixa para este lote e libera para continuar
    st["transportadora_escolhida"] = nome
    st["waiting_tp_manual"] = False
    st["waiting_tp_choice"] = False
    st.pop("pending_tp_scenario", None)

    # 🔹 atualiza painel para exibir a transportadora em uso
    if ENABLE_TRANSPORTADORA_PAINEL:
        try:
            await panel_upsert(context, update.effective_chat.id, st)
        except Exception:
            pass

    await step_replace(
        context,
        update.effective_chat.id,
        st,
        f"✅ Transportadora padrão definida como:\n<b>{nome}</b>\n\n"
        "Gerando a minuta com esta transportadora.",
    )

    # reaproveita o lote pendente
    # usamos o mesmo esquema do finalize_minuta: _CQUpdateShim
    shim = _CQUpdateShim(update)
    await processar_lote(shim, context, st, volumes)


# ========== START ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    chat_id = update.effective_chat.id
    st = SESS.setdefault(u.id, {})
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)
    qlid, rec = storage.users_find_by_tg(u.id)
    msg_id = st.get("msg_recebimento_id")
    
    st["stats"] = {"recv": 0, "ok": 0, "dup": 0, "bad": 0}
    st["danfe_keys"] = set()
    st["danfe_hashes"] = set()
    # st["cleanup_ids"] = {}
    st["panel_msg_id"] = None
    st["sid"] = ""
    st["volbuf"] = ""
    st["data"] = ""

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

        # preserva flags existentes na sessão (ex: start_shown)
        prev = SESS.get(u.id, {})
        prev.update(base)
        SESS[u.id] = prev

        first = (u.first_name or "").strip() or "bem-vindo"
        await send_temp(
            context,
            chat_id,
            f"👋 Bem-vindo, {first}!\n\n📎 Envie suas DANFEs em PDF para começar.",
            seconds=20,
        )

    else:
        # usuário novo: marca que já mostramos o onboarding
        base["start_shown"] = True

        prev = SESS.get(u.id, {})
        prev.update(base)
        SESS[u.id] = prev

        await update.message.reply_text(
            f"Olá, {u.first_name}! Vamos configurar seu acesso.",
            reply_markup=kb_cadastro(),
        )


async def cmd_set_transportadora(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    args = context.args or []

    if not args:
        await update.message.reply_text(
            "Uso: /transportadora NOME_DA_TRANSPORTADORA\n\n"
            "Exemplo:\n"
            "/transportadora MINHA TRANSPORTADORA LTDA"
        )
        return

    nome_raw = " ".join(args).strip()
    # tenta casar com o banco de nomes extraídos das DANFEs
    sugerido = transportadora_db.best_match(nome_raw)
    nome = (sugerido or nome_raw).strip().upper()

    if not nome:
        await update.message.reply_text("Informe um nome válido de transportadora.")
        return

    ok = storage.user_set_transportadora_padrao(tg_id, nome)
    if not ok:
        await update.message.reply_text(
            "Não encontrei seu cadastro (QLID). "
            "Cadastre primeiro pelo menu /start ou botão de cadastro."
        )
        return

    # garante que esse nome também está no DB global
    transportadora_db.add(nome)

    await update.message.reply_text(
        f"Transportadora padrão atualizada para:\n<b>{nome}</b>",
        parse_mode="HTML",
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

    # --- aguardando transportadora digitada manualmente ---
    chat_id = update.effective_chat.id
    if st.get("waiting_tp_manual"):
        from services import transportadora_db
        from services.storage import user_set_transportadora_padrao

        nome_raw = text
        if not nome_raw:
            return

        sugerido = transportadora_db.best_match(nome_raw)
        nome = (sugerido or nome_raw).strip().upper()

        if not nome:
            warn = await msg.reply_text("Nome inválido. Digite novamente o nome completo da sua transportadora:")
            st["step_msg_id"] = warn.message_id
            return

        transportadora_db.add(nome)
        user_set_transportadora_padrao(uid, nome)

        st["transportadora_escolhida"] = nome
        st["waiting_tp_manual"] = False
        st.pop("waiting_tp_choice", None)

        try:
            await msg.delete()
        except Exception:
            pass

        await step_replace(
            context,
            chat_id,
            st,
            f"✅ Transportadora padrão definida como:\n<b>{nome}</b>\n\n"
            "Gerando a minuta com esta transportadora."
        )

        sid = st.get("pending_tp_sid")
        volumes = st.get("pending_tp_volumes") or 1
        if sid:
            class _MsgShim:
                def __init__(self, m):
                    self.message = m
                    self.from_user = m.from_user
            shim = _MsgShim(msg)
            await processar_lote(shim, context, st, volumes)

        return
    
    # --- aguardando QLID ---
    if context.user_data.get("awaiting_qlid"):
        q = text.upper()

        if not valida_qlid(q):
            await msg.reply_text("❌ QLID inválido. Use o formato AA999999 e envie novamente.")
            return

        # Verifica se QLID já está em uso
        try:
            users = storage.users_get_all()
        except Exception:
            users = {}

        existing = users.get(q)
        if existing and existing.get("telegram_id") and existing["telegram_id"] != uid:
            await msg.reply_text(
                "❌ Este QLID já está vinculado a outro usuário.\n"
                "Confira o código informado ou fale com o responsável pelo sistema."
            )
            return

        # Se já existe e é do mesmo usuário, apenas atualiza/sincroniza.
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

def _nf_from_chave(chave: str) -> int:
    """
    Extrai o número da NF (nNF) a partir da chave de 44 dígitos.
    Se não conseguir, devolve um valor alto para não bagunçar a ordenação.
    """
    try:
        if chave and len(chave) == 44:
            # cUF(2) + AAMM(4) + CNPJ(14) + mod(2) + série(3) = 25
            # nNF = próximos 9 dígitos -> posições 26–34 (1-based) => 25:34 (0-based)
            return int(chave[25:34])
    except Exception:
        pass
    return 999999999


def _ordenar_danfes_por_nf(danfe_paths):
    """
    Ordena a lista de PDFs de DANFE pelo número da nota (NF) ascendente,
    usando a chave 44 encontrada em cada PDF.
    """
    ordenado = []
    for p in danfe_paths:
        ch = _chave44_from_pdf(p)
        nf = _nf_from_chave(ch) if ch else 999999999
        ordenado.append((nf, p))
    ordenado.sort(key=lambda t: (t[0], t[1]))
    return [p for _, p in ordenado]


# ========== ANEXOS ==========
async def bloquear_anexo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    try:
        await msg.delete()
    finally:
        await orientar_envio_pdf(context, msg.chat.id)
        pass

# ===== DOCUMENTOS =====
async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    u = msg.from_user
    uid = u.id
    st = SESS.setdefault(
        uid,
        {
            "qlid": "", "cidade": "", "blocked": False, "sid": "", "volbuf": "", "data": "",
            "danfe_keys": set(), "danfe_hashes": set(), "stats": {"recv": 0, "ok": 0, "dup": 0, "bad": 0},
            "panel_msg_id": None,
            "start_shown": False,
        }
    )
    await limpar_mensagens_antigas(st, context, update.effective_chat.id)

    if st["blocked"]:
        await msg.delete()
        return

    # Se sessão ainda não tem QLID/cidade, tenta carregar do cadastro persistente.
    if not st.get("qlid") or not st.get("cidade"):
        try:
            qlid, rec = storage.users_find_by_tg(uid)
        except Exception:
            qlid, rec = None, None

        if qlid and rec:
            # Já existe vínculo no users.json
            st["qlid"] = qlid
            st["cidade"] = rec.get("cidade", "")

            if not st["cidade"]:
                # Tem QLID mas NÃO tem cidade -> cadastro incompleto.
                # Não pode aceitar DANFE ainda: descarta este PDF e reforça.
                try:
                    await msg.delete()
                except Exception:
                    pass

                await step_replace(
                    context,
                    msg.chat.id,
                    st,
                    "🏙️ Agora informe a Cidade para preencher na minuta."
                )
                return

            # Se chegou aqui: QLID + cidade OK -> segue o fluxo normalmente
        else:
            # Não tem QLID mesmo -> dispara /start uma única vez, as demais DANFEs só são descartadas.
            try:
                await msg.delete()
            except Exception:
                pass

            if not st.get("start_shown"):
                await start(update, context)
                st["start_shown"] = True

            return

    doc = msg.document
    if not doc.file_name.lower().endswith(".pdf"):
        await orientar_envio_pdf(context, msg.chat.id)
        await msg.delete()
        return
    
    if not st.get("sid"):  # novo lote
        st["stats"] = {"recv": 0, "ok": 0, "dup": 0, "bad": 0}
        st["danfe_keys"] = set()
        st["danfe_hashes"] = set()
        # st["cleanup_ids"] = {}
        st["panel_msg_id"] = None
        st["last_danfe_count"] = 0
        st["sid"] = storage.new_session(st["qlid"])

        # cria o painel zerado (sem argumentos extras!)
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
                q = it.get("qtde", it.get("qtde", 1))
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
        
        # --- marca decisão das ETIQUETAS e tenta fechar lote ---
        st = SESS.setdefault(cq.from_user.id, {})
        st["labels_decididas"] = True
        await _maybe_cleanup_lote(context, cq.message.chat.id, cq.from_user.id, st)


    except Exception as e:
        await send_temp(context, cq.message.chat.id, f"❌ Erro ao imprimir: {e}", seconds=8)
    finally:
        await safe_delete_message(cq=cq)

async def on_skip_labels(update, context):
    cq = update.callback_query
    await cq.answer()
    LABEL_QUEUE.pop(cq.from_user.id, None)
    # await send_temp(context, cq.message.chat.id, "✅ Ok, não vou imprimir etiqueta.", seconds=6)
    await safe_delete_message(cq=cq)
    
    # --- marca decisão das ETIQUETAS e tenta fechar lote ---
    st = SESS.setdefault(cq.from_user.id, {})
    st["labels_decididas"] = True
    await _maybe_cleanup_lote(context, cq.message.chat.id, cq.from_user.id, st)


# —— callback de impressão da MINUTA ——
async def on_print_minuta_cb(update, context):
    """Trata clique nos botões de impressão de minuta (sem/com DANFEs)."""
    try:
        cq = update.callback_query
        await cq.answer()

        data = cq.data or ""
        parts = data.split(":")
        kind = parts[0] if len(parts) > 0 else ""
        choice = parts[1] if len(parts) > 1 else ""

        # mapeia o tipo de botão para a chave correta em user_data
        if kind == "print_minuta_sem":
            key = "last_minuta_pdf_sem"
        elif kind == "print_minuta_com":
            key = "last_minuta_pdf_com"
        else:
            # compatibilidade com callback antigo "print_minuta:yes/no"
            key = "last_minuta_pdf"

        pdf_path = context.user_data.get(key) or context.user_data.get("last_minuta_pdf")
        msg_txt = ""

        if choice == "no":
            # só fecha os botões, sem imprimir
            msg_txt = ""
        elif choice == "yes":
            if pdf_path:
                try:
                    from services.print_integration import _lp_print, PRINT_ENABLE, is_admin

                    # usa o shim para compatibilizar CallbackQuery com is_admin()
                    if PRINT_ENABLE and is_admin(_CQUpdateShim(cq)):
                        ok, resp = _lp_print(pdf_path)
                        if ok:
                            msg_txt = "🖨️ Minuta enviada para impressão."
                        else:
                            msg_txt = f"⚠️ Erro ao enviar para impressão: {resp}"
                    else:
                        msg_txt = "⚠️ Impressão não está habilitada ou você não é admin."
                except Exception as e:
                    msg_txt = f"⚠️ Falha ao enviar a minuta para impressão: {e}"
            else:
                msg_txt = "⚠️ Não encontrei o PDF da minuta para impressão."

        # apaga a mensagem com os botões
        try:
            await cq.message.delete()
        except Exception:
            pass

        # responde algo opcional se tiver msg
        if msg_txt:
            try:
                await send_temp(context, cq.message.chat.id, msg_txt, seconds=8)
            except Exception:
                pass

        # marca como decidido e tenta limpar lote
        st = SESS.setdefault(cq.from_user.id, {})
        st["minuta_decidida"] = True
        await _maybe_cleanup_lote(context, cq.message.chat.id, cq.from_user.id, st)

    except Exception:
        # não deixa erro de callback quebrar o bot
        import traceback
        traceback.print_exc()


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
        except Exception:
            data_formatada = raw_data  # fallback

        st["data"] = raw_data
        st["volbuf"] = ""
        st["volumes"] = None  # só definimos quando confirmar

        # NÃO atualiza painel aqui; somente quando volumes forem confirmados
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
        except Exception:
            data_formatada = st["data"]

        if cq.data == "vol_del":
            st["volbuf"] = st.get("volbuf", "")[:-1]

        elif cq.data == "vol_ok":
            vol = st.get("volbuf", "0")
            if not vol or vol == "0":
                # mantém mesma mensagem com o teclado
                try:
                    await cq.message.edit_text(
                        f"📅 Data escolhida: {data_formatada}\n⚠️ Volumes deve ser inteiro > 0.",
                        reply_markup=kb_volumes(st["volbuf"])
                    )
                except BadRequest:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=f"📅 Data escolhida: {data_formatada}\n⚠️ Volumes deve ser inteiro > 0.",
                        reply_markup=kb_volumes(st["volbuf"])
                    )
                return

            # Confirma volumes
            st["volumes"] = int(vol)
            
            # Some com a caixa de seleção (não queremos acumular)
            try:
                await cq.message.delete()
            except Exception:
                try:
                    await cq.message.edit_reply_markup(reply_markup=None)
                except Exception:
                    pass
            
            # AGORA sim atualiza o painel com Data + Volumes
            await panel_upsert(context, cq.message.chat.id, st)

            # Inicia o processamento do lote
            await processar_lote(cq, context, st, int(vol))
            return

        else:
            st["volbuf"] = (st.get("volbuf", "") + cq.data.split("_")[1])[:4]

        # Enquanto digita volumes, só atualiza a mesma mensagem (sem painel)
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

    st.setdefault("waiting_tp_choice", False)
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
        await step_replace(context, chat_id, st, f"🤔 Lendo {len(pdfs)} DANFEs...")
        
        # 1) Lê DANFEs e alimenta o banco global de transportadoras
        header, produtos = danfe_parser.parse_lote(pdfs)

        # 2) Carrega config do usuário
        user_cfg = storage.user_get_config_by_tg(uid)

        # 3) Normaliza a transportadora padrão do usuário com base no banco global,
        #    garantindo que o nome completo seja usado já nesta minuta.
        from services import transportadora_db
        padrao = (user_cfg.get("transportadora_padrao") or "").strip()
        if padrao:
            sugerido = transportadora_db.best_match(padrao)
            if sugerido:
                sugerido = sugerido.strip().upper()
                if sugerido != padrao.upper():
                    from services.storage import user_set_transportadora_padrao
                    user_set_transportadora_padrao(uid, sugerido)
                    user_cfg["transportadora_padrao"] = sugerido
                    padrao = sugerido

        # 4) Decide a transportadora do lote já com padrão normalizado
        escolhida, opcoes, precisa_confirmar = _escolher_transportadora_para_lote(header, user_cfg)

        if escolhida:
            header["transportador"] = escolhida

        # se veio de uma confirmação anterior, respeita e não pergunta de novo
        fixed_tp = st.get("transportadora_escolhida")
        if fixed_tp:
            escolhida = fixed_tp
            opcoes = header.get("_transportadoras_lote") or []
            precisa_confirmar = False
            header["transportador"] = fixed_tp
        else:
            escolhida, opcoes, precisa_confirmar = _escolher_transportadora_para_lote(header, user_cfg)
            if escolhida:
                header["transportador"] = escolhida

        logging.info(
            "[minutron] uid=%s transportadoras_lote=%s padrao_user=%s escolhida=%s precisa_confirmar=%s",
            uid,
            opcoes,
            user_cfg.get("transportadora_padrao"),
            escolhida,
            precisa_confirmar,
        )

        if precisa_confirmar:
            from telegram import InlineKeyboardMarkup, InlineKeyboardButton

            # limpa a box "Lendo DANFEs..." antes de mostrar a pergunta
            try:
                await step_clear(context, chat_id, st)
            except Exception:
                pass

            padrao = (user_cfg.get("transportadora_padrao") or "").strip()
            uniq = opcoes or []

            st["waiting_tp_choice"] = True
            st["pending_tp_sid"] = sid
            st["pending_tp_volumes"] = volumes

            # CENÁRIO A: usuário JÁ TEM padrão e só 1 transportadora diferente nas NFs
            if padrao and len(uniq) == 1 and uniq[0].upper() != padrao.upper():
                nf_tp = uniq[0]
                st["pending_tp_scenario"] = "single_diff"
                st["pending_tp_nf"] = nf_tp

                botoes = [
                    [InlineKeyboardButton(
                        f"Usar padrão: {padrao}",
                        callback_data="tp_use_default"
                    )],
                    [InlineKeyboardButton(
                        f"Usar da NF: {nf_tp}",
                        callback_data="tp_use_nf"
                    )],
                ]

                texto = (
                    "🚚 A transportadora desta NF é diferente da sua padrão.\n\n"
                    f"NF: <b>{nf_tp}</b>\n"
                    f"Sua padrão: <b>{padrao}</b>\n\n"
                    "Qual deseja usar para esta minuta?"
                )

                await cq.message.reply_text(
                    texto,
                    reply_markup=InlineKeyboardMarkup(botoes),
                    parse_mode="HTML",
                )
                return

            # CENÁRIO B: ainda não tem padrão → escolher e já salvar
            st["pending_tp_scenario"] = "choose_padrao"
            st["pending_tp_opcoes"] = uniq
            st["pending_tp_escolhida"] = escolhida

            botoes = []

            for idx, nome in enumerate(uniq):
                botoes.append([
                    InlineKeyboardButton(
                        nome,
                        callback_data=f"set_tp_{idx}"
                    )
                ])

            # botão "outra" vai abrir lista do DB ou pedir nome novo
            botoes.append([
                InlineKeyboardButton(
                    "Outra transportadora...",
                    callback_data="set_tp_other"
                )
            ])

            texto = (
                "🚚 Encontrei transportadora(s) nas NFs.\n"
                "Escolha qual é a SUA transportadora padrão.\n"
                "Ela será usada nesta minuta e nas próximas."
            )

            m = await cq.message.reply_text(
                texto,
                reply_markup=InlineKeyboardMarkup(botoes),
                parse_mode="Markdown",
            )

            # registra como "mensagem de etapa" para ser removida depois
            st["step_msg_id"] = m.message_id
            return

        await step_replace(context, chat_id, st, "🔍 Fazendo a busca do RAT... isso pode levar alguns minutos.")
        for p in produtos:
            # Se não tem ocorrência, define como "-"
            if not p.get("ocorrencia"):
                p["ocorrencia"] = "-"

            rat = None
            key = (p["ocorrencia"], p["codigo_prod"])
            rat = _rat_cache.get(key)

            # if p["ocorrencia"] and p["ocorrencia"] != "-" and not rat:
            status = (p.get("status") or "").upper()

            # Regra nova: DOA → não chama RAT nunca
            if status == "DOA":
                rat = "DOA"
            
            # Regra nova: BOM → não chama RAT nunca
            elif status == "BOM":
                rat = "GOOD"

            # Só busca RAT se não for DOA ou BOM
            elif p["ocorrencia"] and p["ocorrencia"] != "-" and not rat:

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
                # s = (p.get("status") or "").upper()

                # if status == "BOM":
                #     rat = "GOOD"
                # elif status == "DOA":
                #     rat = "DOA"
                # elif status == "RUIM":
                if status == "RUIM":
                    rat = ""
                else:
                    rat = "-"

            p["rat"] = rat

            # Não deixa DOA/BOM sujarem o cache global
            if status not in ("DOA", "BOM"):
                _rat_cache[key] = rat

        out_pdf = storage.output_pdf_path(qlid)
        # injeta a data escolhida no nome do PDF
        data_tag = None
        if st.get("data"):
            try:
                # se vier aaaa/mm/dd normaliza para DDMMAAAA
                data_tag = _dt.strptime(st["data"], "%Y%m%d").strftime("%d/%m/%Y")
            except Exception:
                # fallback: só dígitos
                data_tag = _re.sub(r"\D+", "", st["data"])
                
        # caminho original
        out_pdf = storage.output_pdf_path(qlid)

        base, ext = os.path.splitext(out_pdf)
        if data_tag:
            # base, ext = _os.path.splitext(out_pdf)
            out_pdf = f"{base}_{data_tag}{ext}"
    
        # await step_replace(context, chat_id, st, "🧾 Preenchendo a minuta e gerando PDF...")
        # await asyncio.to_thread(preencher_e_exportar_lote, qlid, st.get("cidade"), header, produtos, st.get("data"), volumes, out_pdf)
    
        # # preview das 1–2 primeiras páginas da MINUTA (sem DANFEs)
        # await _send_minuta_preview(context, chat_id, out_pdf, pages=(0,1))

        # # === NOVO: envia (e imprime se habilitado) usando a integração ===
        # # Passamos também a lista de DANFEs para, se configurado, juntar no final.
        # shim = _CQUpdateShim(cq)
        # await finalize_minuta_and_print(
        #     shim,
        #     context,
        #     minuta_pdf_path=out_pdf,
        #     danfe_paths=pdfs,
        # )

        await step_replace(context, chat_id, st, "🧾 Preenchendo a minuta e gerando PDF...")
        await asyncio.to_thread(
            preencher_e_exportar_lote,
            qlid,
            st.get("cidade"),
            header,
            produtos,
            st.get("data"),
            volumes,
            out_pdf,
        )

        # preview das 1–2 primeiras páginas da MINUTA (sem DANFEs)
        await _send_minuta_preview(context, chat_id, out_pdf, pages=(0, 1))

        # === Disponibiliza MINUTA SEM DANFEs (download para todos) ===
        try:
            doc_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=open(out_pdf, "rb"),
                filename=os.path.basename(out_pdf),
                caption="🧾 Minuta gerada (sem DANFEs anexadas).",
            )
            # guarda para possíveis ações de impressão
            context.user_data["last_minuta_pdf_sem"] = out_pdf
            st.setdefault("cleanup_ids", []).append(doc_msg.message_id)
        except Exception:
            pass

        # === Ordena DANFEs por NF para o PDF final com anexos ===
        danfes_ordenadas = _ordenar_danfes_por_nf(pdfs)

        # === Gera/enfileira MINUTA COM DANFEs anexadas via integração existente ===
        shim = _CQUpdateShim(cq)
        merged_path = None
        try:
            # se finalize_minuta_and_print passar a devolver caminho, aproveitamos; senão, ignora
            maybe = await finalize_minuta_and_print(
                shim,
                context,
                minuta_pdf_path=out_pdf,
                danfe_paths=danfes_ordenadas,
            )
            if isinstance(maybe, str):
                merged_path = maybe
        except Exception:
            merged_path = None

        if merged_path:
            context.user_data["last_minuta_pdf_com"] = merged_path
        else:
            # fallback para compatibilidade: se integração não retorna caminho,
            # ainda podemos usar a minuta sem anexos como base
            context.user_data.setdefault("last_minuta_pdf_com", out_pdf)
        
        # === Botões de impressão separados (apenas admin) ===
        try:
            from telegram import InlineKeyboardMarkup, InlineKeyboardButton

            if is_admin(shim):
                rows = []

                if context.user_data.get("last_minuta_pdf_sem"):
                    rows.append([
                        InlineKeyboardButton(
                            "🖨️ Imprimir MINUTA (sem DANFEs)",
                            callback_data="print_minuta_sem:yes",
                        )
                    ])

                if context.user_data.get("last_minuta_pdf_com"):
                    rows.append([
                        InlineKeyboardButton(
                            "🖨️ Imprimir MINUTA (com DANFEs)",
                            callback_data="print_minuta_com:yes",
                        )
                    ])

                if rows:
                    # botão de não imprimir opcional para fechar o fluxo
                    rows.append([
                        InlineKeyboardButton(
                            "❌ Não imprimir MINUTA",
                            callback_data="print_minuta_sem:no",
                        )
                    ])

                    m = await context.bot.send_message(
                        chat_id=chat_id,
                        text="Escolha o que deseja imprimir:",
                        reply_markup=InlineKeyboardMarkup(rows),
                    )
                    st.setdefault("cleanup_ids", []).append(m.message_id)
        except Exception:
            # nunca quebrar o fluxo por causa de botão de impressão
            pass

        # === depois de enviar a minuta (lote) ===
        st["minuta_entregue"] = True

        # se não for admin, não terá botões => considera decidido
        is_admin_user = is_admin(shim)
        if not is_admin_user:
            st["minuta_decidida"] = True
            st["labels_decididas"] = True  # sem botões de etiqueta para não-admin

        # se etiquetas estão desativadas no .env, considera decidido também
        if os.getenv("LABELS_ENABLED", "0") != "1":
            st["labels_decididas"] = True

        # tenta fechar o lote agora; se for admin e houver botões pendentes,
        # o fechamento ocorrerá depois dos cliques nos callbacks (item 5)
        await _maybe_cleanup_lote(context, chat_id, uid, st)


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
                        "qtde": int(float(p.get("qtde", 1) or 1)),
                    })

                LABEL_QUEUE[cq.from_user.id] = itens
                await cq.message.reply_text(
                    "🖨️ Deseja imprimir etiqueta?",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🖨️ Imprimir ETIQUETA", callback_data="print_labels")],
                        [InlineKeyboardButton("❌ Não imprimir ETIQUETA", callback_data="skip_labels")],
                    ]),
                )


        except Exception:
            # nunca derruba o fluxo por causa de etiqueta
            pass

    except Exception as e:
        await cq.message.reply_text(f"Ocorreu um erro ao gerar a minuta. Detalhes: {e}")
        traceback.print_exc()
    finally:
        # se ainda estamos aguardando escolha ou digitação da transportadora,
        # não limpamos nem finalizamos o lote ainda.
        if st.get("waiting_tp_choice") or st.get("waiting_tp_manual"):
            return

        # fluxo concluído: pode limpar a última mensagem de etapa
        try:
            await step_clear(context, chat_id, st)
        except Exception:
            pass

        # Daqui pra baixo é só quando o lote acabou mesmo
        try:
            if sid:
                storage.finalize_session(qlid, sid)
        except Exception:
            pass

        sess = SESS.setdefault(uid, {})

        for k in ("sid", "volbuf"):
            sess[k] = ""

        for k in (
            "progress_msg_id",
            "progress_sid",
            "progress_text",
            "cleanup_ids",
            "last_danfe_count",
            "warned_incomplete",
            "transportadora_escolhida",
            "pending_tp_opcoes",
            "pending_tp_escolhida",
            "pending_tp_sid",
            "pending_tp_volumes",
            "pending_tp_scenario",
            "pending_tp_nf",
            "waiting_tp_choice",
            "waiting_tp_manual",
        ):
            sess.pop(k, None)


async def cb_escolher_transportadora(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton

    cq = update.callback_query
    await cq.answer()

    uid = cq.from_user.id
    chat_id = cq.message.chat.id
    st = SESS.setdefault(uid, {})

    data = cq.data or ""
    sid = st.get("pending_tp_sid")
    if not sid:
        try:
            await cq.message.edit_text("Esse lote não está mais ativo. Envie as DANFEs novamente.")
        except Exception:
            pass
        return

    volumes = st.get("pending_tp_volumes") or 1
    scenario = st.get("pending_tp_scenario")
    opcoes = st.get("pending_tp_opcoes") or []
    escolhida_sugerida = st.get("pending_tp_escolhida")
    nf_tp = (st.get("pending_tp_nf") or "").strip()

    # Config do usuário
    user_cfg = storage.user_get_config_by_tg(uid)
    padrao = (user_cfg.get("transportadora_padrao") or "").strip()

    # Normaliza o padrão com base no DB (usa nome mais completo se existir)
    if padrao:
        sugerido = transportadora_db.best_match(padrao)
        if sugerido:
            novo = sugerido.strip().upper()
            if novo and novo != padrao.upper():
                user_set_transportadora_padrao(uid, novo)
                padrao = novo
                user_cfg["transportadora_padrao"] = novo

    async def _finalizar_escolha(nome: str, msg_confirm: str | None = None):
        """
        Define transportadora escolhida para o lote atual,
        limpa estado temporário, atualiza painel, apaga botões e
        continua o processar_lote.
        """
        nome = (nome or "").strip()
        if not nome:
            try:
                await cq.message.edit_text("Transportadora inválida. Envie as DANFEs novamente.")
            except Exception:
                pass
            st["waiting_tp_choice"] = False
            for k in ("pending_tp_sid", "pending_tp_volumes",
                      "pending_tp_scenario", "pending_tp_nf",
                      "pending_tp_opcoes", "pending_tp_escolhida"):
                st.pop(k, None)
            return

        # grava no estado
        st["transportadora_escolhida"] = nome
        st["waiting_tp_choice"] = False

        for k in ("pending_tp_sid", "pending_tp_volumes",
                  "pending_tp_scenario", "pending_tp_nf",
                  "pending_tp_opcoes", "pending_tp_escolhida"):
            st.pop(k, None)

        # apaga a mensagem com os botões (pra não ficar lixo no chat)
        try:
            await cq.message.delete()
        except Exception:
            pass

        # atualiza painel com a transportadora escolhida (se ativado)
        if ENABLE_TRANSPORTADORA_PAINEL:
            try:
                await panel_upsert(context, chat_id, st)
            except Exception:
                pass

        # mensagem opcional de confirmação (temporária)
        if msg_confirm:
            try:
                await send_temp(context, chat_id, msg_confirm, seconds=8)
            except Exception:
                pass

        # segue o fluxo do lote com a transportadora definida
        await processar_lote(cq, context, st, volumes)

    # =========================
    # CENÁRIO A: single_diff
    # =========================
    if scenario == "single_diff":
        if data == "tp_use_default" and padrao:
            await _finalizar_escolha(
                padrao,
                f"✅ Usando sua transportadora padrão:\n{padrao}",
            )
            return

        if data == "tp_use_nf" and nf_tp:
            await _finalizar_escolha(
                nf_tp,
                f"✅ Usando a transportadora da NF apenas nesta minuta:\n{nf_tp}",
            )
            return

    # =========================
    # CENÁRIO B: choose_padrao
    # =========================
    if scenario == "choose_padrao":
        # escolheu uma das sugeridas
        if data.startswith("set_tp_") and data != "set_tp_other":
            try:
                idx = int(data.replace("set_tp_", ""))
                nome = (opcoes[idx] if opcoes else escolhida_sugerida) or ""
            except Exception:
                nome = ""

            if not nome.strip():
                try:
                    await cq.message.edit_text("Transportadora inválida. Gere a minuta novamente.")
                except Exception:
                    pass
                return

            user_set_transportadora_padrao(uid, nome)
            transportadora_db.add(nome)

            await _finalizar_escolha(
                nome,
                f"✅ Transportadora padrão definida como:\n{nome}\n\nGerando a minuta com esta transportadora.",
            )
            return

        # "Outra transportadora..."
        if data == "set_tp_other":
            lista = transportadora_db.all()

            if lista:
                # próximo passo: escolher a partir do DB
                st["pending_tp_scenario"] = "choose_padrao_db"

                botoes = []
                for i, nome in enumerate(lista[:20]):
                    botoes.append([
                        InlineKeyboardButton(
                            nome,
                            callback_data=f"tpdb_{i}"
                        )
                    ])
                botoes.append([
                    InlineKeyboardButton(
                        "Nenhuma destas (digitar nome)",
                        callback_data="tpdb_manual"
                    )
                ])

                try:
                    await cq.message.edit_text(
                        "Selecione sua transportadora na lista abaixo ou escolha digitar o nome:",
                        reply_markup=InlineKeyboardMarkup(botoes),
                    )
                except Exception:
                    pass
                return

            # se DB vazio, vai direto para manual
            st["waiting_tp_manual"] = True
            st["waiting_tp_choice"] = False
            st.pop("pending_tp_scenario", None)

            try:
                await cq.message.edit_text(
                    "Digite abaixo o nome da sua transportadora padrão:"
                )
            except Exception:
                pass
            st["step_msg_id"] = cq.message.message_id
            return

    # =========================
    # CENÁRIO C: choose_padrao_db
    # =========================
    if scenario == "choose_padrao_db":
        lista = transportadora_db.all()

        # escolheu uma das do DB
        if data.startswith("tpdb_") and data != "tpdb_manual":
            try:
                idx = int(data.replace("tpdb_", ""))
                nome = lista[idx]
            except Exception:
                nome = ""

            if not nome.strip():
                try:
                    await cq.message.edit_text("Transportadora inválida. Gere a minuta novamente.")
                except Exception:
                    pass
                return

            user_set_transportadora_padrao(uid, nome)
            transportadora_db.add(nome)

            await _finalizar_escolha(
                nome,
                f"✅ Transportadora padrão definida como:\n{nome}\n\nGerando a minuta com esta transportadora.",
            )
            return

        # "Nenhuma destas (digitar nome)"
        if data == "tpdb_manual":
            st["waiting_tp_manual"] = True
            st["waiting_tp_choice"] = False
            st.pop("pending_tp_scenario", None)

            try:
                await cq.message.edit_text(
                    "Digite abaixo o nome da sua transportadora padrão:"
                )
            except Exception:
                pass
            st["step_msg_id"] = cq.message.message_id
            return

    # =========================
    # Fallback
    # =========================
    st["waiting_tp_choice"] = False
    for k in ("pending_tp_sid", "pending_tp_volumes",
              "pending_tp_scenario", "pending_tp_nf",
              "pending_tp_opcoes", "pending_tp_escolhida"):
        st.pop(k, None)

    try:
        await cq.message.edit_text("Fluxo inválido ou expirado. Envie as DANFEs novamente.")
    except Exception:
        pass


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
    # app.add_handler(CallbackQueryHandler(on_print_minuta_cb, pattern=r"^printminuta:(yes|no)$"))
    app.add_handler(CallbackQueryHandler(on_print_minuta_cb, pattern=r"^print_minuta"))
    app.add_handler(CommandHandler("minutas", cmd_minutas))
    app.add_handler(CommandHandler("alterar", cmd_alterar_cidade))
    app.add_handler(CommandHandler("cancelar", cmd_cancelar))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("usuarios", admin_usuarios))
    app.add_handler(CommandHandler("broadcast", admin_broadcast))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("transportadora", cmd_set_transportadora))
    app.add_handler(CallbackQueryHandler(
        cb_escolher_transportadora,
        pattern=r"^(set_tp_|tp_use_|tpdb_)"
    ))

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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_tp_manual))


    # Bloqueio de mídia não-PDF
    app.add_handler(MessageHandler(filters.PHOTO, bloquear_anexo))
    app.add_handler(MessageHandler(filters.VIDEO, bloquear_anexo))
    app.add_handler(MessageHandler(filters.AUDIO, bloquear_anexo))
    app.add_handler(MessageHandler(filters.VOICE, bloquear_anexo))
    app.add_handler(MessageHandler(filters.ANIMATION, bloquear_anexo))

    # app.run_polling()
    app.run_polling(allowed_updates=Update.ALL_TYPES)  # evita warning de tipos não tratados

if __name__ == "__main__":
    main()
