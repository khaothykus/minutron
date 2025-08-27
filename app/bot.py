import os, asyncio, traceback
from telegram import Update, InputFile, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from config import BOT_TOKEN, ADMIN_TELEGRAM_ID
from services import storage, danfe_parser
from services.excel_filler_spire import preencher_e_exportar_lote
from services.rat_search import get_rat_for_ocorrencia
from services.validators import valida_qlid, valida_cidade
from keyboards import kb_cadastro, kb_main, kb_datas, kb_volumes

# Sessões em memória por Telegram ID
SESS = {}  # {tg_id: {"qlid":"", "cidade":"", "blocked": False, "sid": "", "volbuf":"", "data":""}}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    qlid, rec = storage.users_find_by_tg(u.id)

    if rec:
        SESS[u.id] = {
            "qlid": qlid,
            "cidade": rec.get("cidade", ""),
            "blocked": rec.get("blocked", False),
            "sid": "",
            "volbuf": "",
            "data": ""
        }
        await update.message.reply_text(
            f"Bem-vindo de volta, {u.first_name}! Seu QLID é {qlid} e sua cidade é {rec.get('cidade')}.",
            reply_markup=kb_main()
        )
    else:
        SESS[u.id] = {
            "qlid": "",
            "cidade": "",
            "blocked": False,
            "sid": "",
            "volbuf": "",
            "data": ""
        }
        await update.message.reply_text(
            f"Olá, {u.first_name}! Vamos configurar seu acesso.",
            reply_markup=kb_cadastro()
        )


# ADMIN
async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_TELEGRAM_ID:
        await update.message.delete()
        return
    await update.message.reply_text("Admin: /usuarios, /broadcast <msg>")

async def admin_usuarios(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_TELEGRAM_ID: return
    users = storage.users_get_all()
    if not users:
        await update.message.reply_text("Nenhum usuário.")
        return
    lines = []
    for qlid, rec in users.items():
        lines.append(f"{qlid} | TG:{rec.get('telegram_id')} | Cidade:{rec.get('cidade','')} | Blocked:{rec.get('blocked',False)}")
    await update.message.reply_text("\n".join(lines))

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_TELEGRAM_ID: return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("Uso: /broadcast sua mensagem")
        return
    # Envia a todos usuários conhecidos
    users = storage.users_get_all()
    for qlid, rec in users.items():
        try:
            await context.bot.send_message(rec["telegram_id"], f"[Aviso]: {msg}")
        except Exception:
            pass
    await update.message.reply_text("Broadcast enviado.")

# CADASTRO
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    uid = msg.from_user.id
    st = SESS.setdefault(uid, {"qlid":"", "cidade":"", "blocked": False, "sid":"", "volbuf":"", "data":""})

    if context.user_data.get("awaiting_qlid"):
        q = msg.text.strip().upper()
        if not valida_qlid(q):
            await msg.reply_text("QLID inválido. Use o formato AA999999.")
        else:
            st["qlid"] = q
            storage.users_upsert(q, {"telegram_id": uid, "cidade": st.get("cidade",""), "blocked": False})
            await msg.reply_text("QLID cadastrado.")

            # Já emenda pedindo a cidade
            context.user_data["awaiting_cidade"] = True
            await msg.reply_text("Agora envie sua Cidade (apenas letras e espaços).")
        context.user_data["awaiting_qlid"] = False
        return

    # if context.user_data.get("awaiting_qlid"):
    #     q = msg.text.strip().upper()
    #     if not valida_qlid(q):
    #         await msg.reply_text("QLID inválido. Use o formato AA999999.")
    #     else:
    #         st["qlid"] = q
    #         storage.users_upsert(q, {"telegram_id": uid, "cidade": st.get("cidade",""), "blocked": False})
    #         await msg.reply_text("QLID cadastrado.", reply_markup=kb_cadastro())
    #     context.user_data["awaiting_qlid"] = False
    #     return

    if context.user_data.get("awaiting_cidade"):
        c = msg.text.strip()
        if not valida_cidade(c):
            await msg.reply_text("Cidade inválida. Use apenas letras e espaços.")
        else:
            st["cidade"] = c.title()
            # Atualiza persistência se já houver QLID
            if st.get("qlid"):
                storage.users_upsert(st["qlid"], {"telegram_id": uid, "cidade": st["cidade"], "blocked": False})
            await msg.reply_text(f"Cidade definida: {st['cidade']}", reply_markup=kb_main())
        context.user_data["awaiting_cidade"] = False
        return

    text = msg.text.strip()

    if text == "Cadastrar QLID":
        await msg.reply_text("Envie seu QLID (AA999999) como mensagem.")
        context.user_data["awaiting_qlid"] = True
        return

    if text == "Cadastrar Cidade":
        await msg.reply_text("Envie sua Cidade (apenas letras e espaços) como mensagem.")
        context.user_data["awaiting_cidade"] = True
        return

    if text == "Gerar minuta":
        st = SESS.get(msg.from_user.id)
        sid = st.get("sid")
        if not sid:
            await msg.reply_text("⚠️ Você ainda não anexou nenhuma DANFE. Envie os arquivos antes de gerar a minuta.")
            return

        pdfs_dir = f"{storage.user_dir(st['qlid'])}/temp/{sid}/pdfs"
        pdfs = [f for f in os.listdir(pdfs_dir) if f.lower().endswith(".pdf")] if os.path.exists(pdfs_dir) else []

        if not pdfs:
            await msg.reply_text("⚠️ Nenhuma DANFE encontrada no lote atual. Envie os arquivos antes de gerar a minuta.")
            return

        await msg.reply_text("Escolha a data:", reply_markup=kb_datas())
        return

    if text == "Minhas minutas":
        st = SESS.get(msg.from_user.id)
        if not st.get("qlid"):
            await msg.reply_text("Cadastre um QLID primeiro.", reply_markup=kb_cadastro())
            return
        files = storage.list_minutas(st["qlid"])
        if not files:
            await msg.reply_text("Você ainda não tem minutas geradas.")
            return
        with open(files[0], "rb") as f:
            await msg.reply_document(f, filename=os.path.basename(files[0]))
        return

    if text == "Alterar cidade":
        await msg.reply_text("Envie sua nova Cidade (apenas letras e espaços).")
        context.user_data["awaiting_cidade"] = True
        return



    # Chat limpo
    await msg.delete()

async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    u = msg.from_user
    st = SESS.get(u.id)
    if not st or st.get("blocked"):
        await msg.delete()
        return
    if not st.get("qlid") or not st.get("cidade"):
        await msg.reply_text("Finalize o cadastro primeiro.", reply_markup=kb_cadastro())
        await msg.delete()
        return

    doc = msg.document
    if not doc.file_name.lower().endswith(".pdf"):
        await msg.reply_text("Envie apenas arquivos PDF.")
        await msg.delete()
        return

    if not st.get("sid"):
        st["sid"] = storage.new_session(st["qlid"])

    dest = storage.save_pdf(st["qlid"], st["sid"], doc.file_name)
    file = await doc.get_file()
    await file.download_to_drive(dest)

    if not danfe_parser.is_danfe(dest):
        await msg.reply_text("Arquivo não é uma DANFE válida. Tente novamente.")
        os.remove(dest)
        await msg.delete()
        return

    count = len([f for f in os.listdir(os.path.dirname(dest)) if f.lower().endswith(".pdf")])
    await msg.reply_text(f"Recebidas {count} DANFEs.", reply_markup=kb_main())
    await msg.delete()

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cq = update.callback_query
    await cq.answer()
    uid = cq.from_user.id
    st = SESS.setdefault(uid, {"qlid":"", "cidade":"", "blocked": False, "sid":"", "volbuf":"", "data":""})

    if cq.data == "cad_qlid":
        await cq.message.edit_text("Envie seu QLID (AA999999) como mensagem.")
        context.user_data["awaiting_qlid"] = True
        return
    if cq.data == "cad_cidade":
        await cq.message.edit_text("Envie sua Cidade (apenas letras e espaços) como mensagem.")
        context.user_data["awaiting_cidade"] = True
        return
    if cq.data == "gerar_minuta":
        sid = st.get("sid")
        if not sid:
            await cq.message.edit_text("⚠️ Você ainda não anexou nenhuma DANFE. Envie os arquivos antes de gerar a minuta.")
            return

        pdfs_dir = f"{storage.user_dir(st['qlid'])}/temp/{sid}/pdfs"
        pdfs = [f for f in os.listdir(pdfs_dir) if f.lower().endswith(".pdf")] if os.path.exists(pdfs_dir) else []

        if not pdfs:
            await cq.message.edit_text("⚠️ Nenhuma DANFE encontrada no lote atual. Envie os arquivos antes de gerar a minuta.")
            return
            
        await cq.message.edit_text("Escolha a data:", reply_markup=kb_datas())
        return
    if cq.data.startswith("data_"):
        st["data"] = cq.data[5:]  # ISO
        st["volbuf"] = ""
        await cq.message.edit_text(f"Data escolhida: {st['data']}\nInforme os volumes:", reply_markup=kb_volumes())
        return
    if cq.data.startswith("vol_"):
        if cq.data == "vol_del":
            st["volbuf"] = st.get("volbuf","")[:-1]
        elif cq.data == "vol_ok":
            vol = st.get("volbuf","0")
            if not vol or vol == "0":
                await cq.message.edit_text("Volumes deve ser inteiro > 0.", reply_markup=kb_volumes(st["volbuf"]))
                return
            await processar_lote(cq, st, int(vol))
            return
        else:
            st["volbuf"] = (st.get("volbuf","") + cq.data.split("_")[1])[:4]
        await cq.message.edit_text(f"Data escolhida: {st['data']}\nVolumes: {st['volbuf'] or '-'}", reply_markup=kb_volumes(st["volbuf"]))
        return
    if cq.data == "alterar_cidade":
        await cq.message.edit_text("Envie sua nova Cidade (apenas letras e espaços).")
        context.user_data["awaiting_cidade"] = True
        return
    if cq.data == "minhas_minutas":
        if not st.get("qlid"):
            await cq.message.edit_text("Cadastre um QLID primeiro.", reply_markup=kb_cadastro())
            return
        files = storage.list_minutas(st["qlid"])
        if not files:
            await cq.message.edit_text("Você ainda não tem minutas geradas.")
            return
        with open(files[0], "rb") as f:
            await cq.message.reply_document(f, filename=os.path.basename(files[0]))
        return

async def processar_lote(cq, st, volumes: int):
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
        await cq.message.edit_text(f"Lendo {len(pdfs)} DANFEs…")
        header, produtos = danfe_parser.parse_lote(pdfs)
        print("📦 Header extraído:")
        for k, v in header.items():
            print(f"{k}: {v}")

        await cq.message.reply_text("Fazendo scraping do RAT… isso pode levar alguns minutos.")
        for p in produtos:
            rat = get_rat_for_ocorrencia(p["ocorrencia"], p["codigo_prod"])
            if not rat:
                if p["status"] == "BOM":
                    rat = "GOOD"
                elif p["status"] == "DOA":
                    rat = "DOA"
                elif p["status"] == "RUIM":
                    rat = ""  # vazio
            p["rat"] = rat

        out_pdf = storage.output_pdf_path(qlid)
        await cq.message.reply_text("Preenchendo template e gerando PDF…")
        await asyncio.to_thread(preencher_e_exportar_lote, qlid, st["cidade"], header, produtos, st["data"], volumes, out_pdf)

        with open(out_pdf, "rb") as f:
            await cq.message.reply_document(InputFile(f, filename=os.path.basename(out_pdf)), caption="Sua minuta está pronta.")
    except Exception as e:
        await cq.message.reply_text(f"Ocorreu um erro ao gerar a minuta.\nDetalhes: {e}")
        traceback.print_exc()
    finally:
        storage.finalize_session(qlid, sid)
        st["sid"] = ""

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("usuarios", admin_usuarios))
    app.add_handler(CommandHandler("broadcast", admin_broadcast))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.Document.PDF, on_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.run_polling()

if __name__ == "__main__":
    main()