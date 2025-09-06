import os, asyncio, traceback
from telegram import (
    Update,
    InputFile,
    BotCommand,
    ReplyKeyboardMarkup,
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
from services.excel_filler_spire import preencher_e_exportar_lote
from services.rat_search import get_rat_for_ocorrencia
from services.validators import valida_qlid, valida_cidade
from keyboards import kb_cadastro, kb_main, kb_datas, kb_volumes, kb_opcoes, kb_menu

# Sessões em memória por Telegram ID
SESS = (
    {}
)  # {tg_id: {"qlid":"", "cidade":"", "blocked": False, "sid": "", "volbuf":"", "data":"", "msg_recebimento_id": int}}


# async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     u = update.effective_user
#     qlid, rec = storage.users_find_by_tg(u.id)

#     # Preserva msg_recebimento_id se já existir
#     prev = SESS.get(u.id, {})
#     msg_id = prev.get("msg_recebimento_id")

#     if rec:
#         SESS[u.id] = {
#             "qlid": qlid,
#             "cidade": rec.get("cidade", ""),
#             "blocked": rec.get("blocked", False),
#             "sid": "",
#             "volbuf": "",
#             "data": "",
#             "msg_recebimento_id": msg_id,  # preservado
#         }
#         # await update.message.reply_text(
#         #     f"Bem-vindo de volta, {u.first_name}! Seu QLID é {qlid} e a cidade para a minuta é {rec.get('cidade')}. Anexe as DANFEs para começar.",
#         #     # reply_markup=kb_main(),
#         # )
#         await context.bot.send_message(
#             chat_id=update.effective_chat.id,
#             text="👋 Bem-vindo! Envie suas DANFEs para gerar uma minuta ou acesse opções abaixo:",
#             reply_markup=kb_opcoes()
#         )

#     else:
#         SESS[u.id] = {
#             "qlid": "",
#             "cidade": "",
#             "blocked": False,
#             "sid": "",
#             "volbuf": "",
#             "data": "",
#             "msg_recebimento_id": None,
#         }
#         await update.message.reply_text(
#             f"Olá, {u.first_name}! Vamos configurar seu acesso.",
#             reply_markup=kb_cadastro(),
#         )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    qlid, rec = storage.users_find_by_tg(u.id)

    # Preserva msg_recebimento_id se já existir
    prev = SESS.get(u.id, {})
    msg_id = prev.get("msg_recebimento_id")

    if rec:
        SESS[u.id] = {
            "qlid": qlid,
            "cidade": rec.get("cidade", ""),
            "blocked": rec.get("blocked", False),
            "sid": "",
            "volbuf": "",
            "data": "",
            "msg_recebimento_id": msg_id,  # preservado
        }

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                f"👋 Bem-vindo, {u.first_name}!\n\n"
                # f"Seu QLID é {qlid} e a cidade para a minuta é {rec.get('cidade')}.\n\n"
                f"Envie suas DANFEs ou toque em 📋 Menu para mais opções."
            ),reply_markup=kb_menu()
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


# OPÇÕES
async def opcoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚙️ O que você deseja fazer?",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📂 Minhas minutas", callback_data="minhas_minutas"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🏙️ Alterar cidade", callback_data="alterar_cidade"
                    )
                ],
            ]
        ),
    )


# ADMIN
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
    lines = []
    for qlid, rec in users.items():
        lines.append(
            f"{qlid} | TG:{rec.get('telegram_id')} | Cidade:{rec.get('cidade','')} | Blocked:{rec.get('blocked',False)}"
        )
    await update.message.reply_text("\n".join(lines))


async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_TELEGRAM_ID:
        return
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
    st = SESS.setdefault(
        uid,
        {
            "qlid": "",
            "cidade": "",
            "blocked": False,
            "sid": "",
            "volbuf": "",
            "data": "",
        },
    )

    if context.user_data.get("awaiting_qlid"):
        q = msg.text.strip().upper()
        if not valida_qlid(q):
            await msg.reply_text("QLID inválido. Use o formato AA999999.")
        else:
            st["qlid"] = q
            storage.users_upsert(
                q,
                {"telegram_id": uid, "cidade": st.get("cidade", ""), "blocked": False},
            )
            await msg.reply_text("QLID cadastrado.")

            # Já emenda pedindo a cidade
            context.user_data["awaiting_cidade"] = True
            await msg.reply_text("Agora informe a Cidade para preencher na minuta.")
        context.user_data["awaiting_qlid"] = False
        return

    if context.user_data.get("awaiting_cidade"):
        c = msg.text.strip()
        if not valida_cidade(c):
            await msg.reply_text("Cidade inválida. Use apenas letras e espaços.")
        else:
            st["cidade"] = c.title()
            # Atualiza persistência se já houver QLID
            if st.get("qlid"):
                storage.users_upsert(
                    st["qlid"],
                    {"telegram_id": uid, "cidade": st["cidade"], "blocked": False},
                )
            await msg.reply_text(
                # f"Cidade definida: {st['cidade']}", reply_markup=kb_main()
                f"Cidade definida: {st['cidade']}.\nAgora é só você enviar as DANFEs (PDFs) para gerar a minuta!."
            )
        context.user_data["awaiting_cidade"] = False
        return

    text = msg.text.strip()

    if text == "Cadastrar QLID":
        await msg.reply_text("Envie seu QLID (Sem o 'C', ex: AB123456).")
        context.user_data["awaiting_qlid"] = True
        return

    if text == "Cadastrar Cidade":
        await msg.reply_text(
            "Envie sua Cidade (apenas letras e espaços) como mensagem."
        )
        context.user_data["awaiting_cidade"] = True
        return

    if text == "Gerar minuta":
        st = SESS.get(msg.from_user.id)
        sid = st.get("sid")
        if not sid:
            await msg.reply_text(
                "⚠️ Você ainda não anexou nenhuma DANFE. Envie os arquivos antes de gerar a minuta."
            )
            return

        pdfs_dir = f"{storage.user_dir(st['qlid'])}/temp/{sid}/pdfs"
        pdfs = (
            [f for f in os.listdir(pdfs_dir) if f.lower().endswith(".pdf")]
            if os.path.exists(pdfs_dir)
            else []
        )

        if not pdfs:
            await msg.reply_text(
                "⚠️ Nenhuma DANFE encontrada no lote atual. Envie os arquivos antes de gerar a minuta."
            )
            return

        await msg.reply_text("Escolha a data:", reply_markup=kb_datas())
        return

    if text == "Minhas minutas":
        st = SESS.get(msg.from_user.id)
        if not st.get("qlid"):
            await msg.reply_text(
                "Cadastre um QLID primeiro.", reply_markup=kb_cadastro()
            )
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


from datetime import datetime


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    u = msg.from_user
    st = SESS.setdefault(
        u.id,
        {
            "qlid": "",
            "cidade": "",
            "blocked": False,
            "sid": "",
            "volbuf": "",
            "data": "",
            "progress_msg_id": None,
            "progress_sid": None,
        },
    )

    if st["blocked"]:
        await msg.delete()
        return

    if not st["qlid"] or not st["cidade"]:
        await context.bot.send_message(
            chat_id=msg.chat.id,
            text="⚠️ Finalize o cadastro primeiro.",
            reply_markup=kb_cadastro(),
        )
        await msg.delete()
        return

    doc = msg.document
    if not doc.file_name.lower().endswith(".pdf"):
        await context.bot.send_message(
            chat_id=msg.chat.id, text="Envie apenas arquivos PDF."
        )
        await msg.delete()
        return

    if not st["sid"]:
        st["sid"] = storage.new_session(st["qlid"])

    dest = storage.save_pdf(st["qlid"], st["sid"], doc.file_name)
    file = await doc.get_file()
    await file.download_to_drive(dest)

    if not danfe_parser.is_danfe(dest):
        await context.bot.send_message(
            chat_id=msg.chat.id,
            text="❌ Arquivo não é uma DANFE válida. Tente outro PDF.",
        )
        os.remove(dest)
        await msg.delete()
        return

    count = len(
        [f for f in os.listdir(os.path.dirname(dest)) if f.lower().endswith(".pdf")]
    )
    # Antes de montar o texto
    last_count = st.get("last_danfe_count", 0)

    if count == last_count:
        print("[DEBUG] DANFE duplicada detectada — não atualiza mensagem.")
        await msg.delete()
        return

    # Atualiza o contador salvo
    st["last_danfe_count"] = count

    text = (
        f"📄 Recebidas {count} DANFE{'s' if count > 1 else ''}.\n\n"
        "Envie mais DANFEs ou toque abaixo para gerar a minuta."
    )
    reply_markup = kb_main()

    # Verifica se pode editar a mensagem anterior
    msg_id = st.get("progress_msg_id")
    sid_ref = st.get("progress_sid")
    sid_now = st["sid"]

    try:
        # Antes de editar
        if msg_id and sid_ref == sid_now and st.get("progress_text") != text:
            await context.bot.edit_message_text(
                chat_id=msg.chat.id,
                message_id=msg_id,
                text=text,
                reply_markup=reply_markup,
            )
            st["progress_text"] = text
            print(f"[DEBUG] Editou mensagem {msg_id} com {count} DANFEs")
        else:
            raise Exception("Mensagem não modificada ou inválida")
    except Exception as e:
        print(f"[DEBUG] Falha ao editar mensagem {msg_id}: {e}")
        new_msg = await context.bot.send_message(
            chat_id=msg.chat.id, text=text, reply_markup=reply_markup
        )
        st["progress_msg_id"] = new_msg.message_id
        st["progress_sid"] = sid_now
        print(f"[DEBUG] Criou nova mensagem {new_msg.message_id} após falha")

    await msg.delete()


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cq = update.callback_query
    await cq.answer()  # feedback rápido
    uid = cq.from_user.id

    st = SESS.get(uid)
    if not st:
        await cq.message.edit_text("⚠️ Sessão expirada. Envie um PDF para reiniciar.")
        return

    if cq.data == "cad_qlid":
        await cq.message.edit_text("Envie seu QLID (AA999999) como mensagem.")
        context.user_data["awaiting_qlid"] = True
        return
    if cq.data == "cad_cidade":
        await cq.message.edit_text("Envie sua Cidade como mensagem.")
        context.user_data["awaiting_cidade"] = True
        return
    if cq.data == "fechar_opcoes":
        await cq.message.edit_text(
            "✅ Pronto para continuar. Envie suas DANFEs ou toque em 📋 Menu para mais opções.",
            reply_markup=kb_menu()
        )
        return
    if cq.data == "gerar_minuta":
        # Sem DANFEs, pede para enviar
        if not st.get("sid"):
            await cq.message.edit_text(
                "⚠️ Você ainda não enviou nenhuma DANFE. Envie seus PDFs primeiro."
            )
            return

        # Verifica se ainda existe pelo menos 1 PDF
        pdfs_dir = f"{storage.user_dir(st['qlid'])}/temp/{st['sid']}/pdfs"
        pdfs = (
            [f for f in os.listdir(pdfs_dir) if f.lower().endswith(".pdf")]
            if os.path.exists(pdfs_dir)
            else []
        )

        if not pdfs:
            await cq.message.edit_text(
                "⚠️ Nenhuma DANFE encontrada no lote atual. Envie os arquivos antes de gerar a minuta."
            )
            return

        # Avança para escolha de data
        await cq.message.edit_text("🗓️ Escolha a data:", reply_markup=kb_datas())
        return

    from datetime import datetime

    if cq.data.startswith("data_"):
        st["data"] = cq.data[5:]  # ISO
        st["volbuf"] = ""

        data_br = datetime.strptime(st["data"], "%Y-%m-%d").strftime("%d/%m/%Y")
        novo_texto = f"Data escolhida: {data_br}\nInforme os volumes:"

        # Evita edição se o texto já está igual
        if cq.message.text != novo_texto:
            await cq.message.edit_text(novo_texto, reply_markup=kb_volumes())
        return

    if cq.data.startswith("vol_"):
        if cq.data == "vol_del":
            st["volbuf"] = st.get("volbuf", "")[:-1]
        elif cq.data == "vol_ok":
            vol = st.get("volbuf", "0")
            if not vol or vol == "0":
                data_br = datetime.strptime(st["data"], "%Y-%m-%d").strftime("%d/%m/%Y")
                await cq.message.edit_text(
                    f"Data escolhida: {data_br}\nVolumes deve ser inteiro > 0.",
                    reply_markup=kb_volumes(st["volbuf"]),
                )
                return

            # Remove o teclado antes de processar
            try:
                await cq.message.edit_reply_markup(reply_markup=None)
            except Exception as e:
                print(f"[DEBUG] Falha ao remover teclado: {e}")

            # Inicia o processamento
            await processar_lote(cq, context, st, int(vol))
            return
        else:
            st["volbuf"] = (st.get("volbuf", "") + cq.data.split("_")[1])[:4]

        data_br = datetime.strptime(st["data"], "%Y-%m-%d").strftime("%d/%m/%Y")
        novo_texto = f"Data escolhida: {data_br}\nVolumes: {st['volbuf'] or '-'}"

        if cq.message.text != novo_texto:
            await cq.message.edit_text(
                novo_texto, reply_markup=kb_volumes(st["volbuf"])
            )
        return
    # if cq.data == "alterar_cidade":
    #     await cq.message.edit_text("Envie sua nova Cidade.")
    #     context.user_data["awaiting_cidade"] = True
    #     return
    # if cq.data == "minhas_minutas":
    #     if not st.get("qlid"):
    #         await cq.message.edit_text(
    #             "Cadastre um QLID primeiro.", reply_markup=kb_cadastro()
    #         )
    #         return
    #     files = storage.list_minutas(st["qlid"])
    #     if not files:
    #         await cq.message.edit_text("Você ainda não tem minutas geradas.")
    #         return
    #     with open(files[0], "rb") as f:
    #         await cq.message.reply_document(f, filename=os.path.basename(files[0]))
    #     return
    if cq.data == "alterar_cidade":
        msg = await cq.message.edit_text(
            "🏙️ Envie sua nova Cidade (apenas letras e espaços)."
        )
        context.user_data["awaiting_cidade"] = True
        st.setdefault("cleanup_ids", []).append(msg.message_id)
        return

    if cq.data == "minhas_minutas":
        files = storage.list_minutas(st["qlid"])
        if not files:
            await cq.message.edit_text(
                "📂 Você ainda não tem minutas geradas.\n\nEnvie suas DANFEs em PDF para começar ou digite /opcoes para acessar outras funções."
            )
            return

        buttons = [
            [
                InlineKeyboardButton(
                    f"📄 {os.path.basename(f)}", callback_data=f"minuta_{i}"
                )
            ]
            for i, f in enumerate(files[:5])
        ]
        await cq.message.edit_text(
            "Selecione uma minuta:", reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    if cq.data.startswith("minuta_"):
        idx = int(cq.data.split("_")[1])
        files = storage.list_minutas(st["qlid"])
        if idx < len(files):
            with open(files[idx], "rb") as f:
                await cq.message.reply_document(
                    f, filename=os.path.basename(files[idx])
                )
        return


async def processar_lote(cq, context, st, volumes: int):
    chat_id = cq.message.chat.id
    qlid = st["qlid"]
    sid = st.get("sid")

    if not sid:
        await cq.message.edit_text("Nenhuma DANFE no lote atual.")
        return

    pdfs_dir = f"{storage.user_dir(qlid)}/temp/{sid}/pdfs"
    pdfs = [
        os.path.join(pdfs_dir, f)
        for f in os.listdir(pdfs_dir)
        if f.lower().endswith(".pdf")
    ]
    if not pdfs:
        await cq.message.edit_text("Nenhuma DANFE no lote atual.")
        return

    try:
        # Mensagem de leitura
        msg_lendo = await cq.message.reply_text(f"Lendo {len(pdfs)} DANFEs…")
        st.setdefault("cleanup_ids", []).append(msg_lendo.message_id)

        header, produtos = danfe_parser.parse_lote(pdfs)

        # Mensagem de busca do RAT
        msg_rat = await cq.message.reply_text(
            "Fazendo a busca do RAT… isso pode levar alguns minutos."
        )
        st["cleanup_ids"].append(msg_rat.message_id)

        for p in produtos:
            if not p.get("ocorrencia"):
                p["ocorrencia"] = "-"

            rat = get_rat_for_ocorrencia(p["ocorrencia"], p["codigo_prod"])
            if not rat:
                if p["status"] == "BOM":
                    rat = "GOOD"
                elif p["status"] == "DOA":
                    rat = "DOA"
                elif p["status"] == "RUIM":
                    rat = ""
            p["rat"] = rat

        out_pdf = storage.output_pdf_path(qlid)

        # Mensagem de geração
        msg_gerando = await cq.message.reply_text("Preenchendo template e gerando PDF…")
        st["cleanup_ids"].append(msg_gerando.message_id)

        await asyncio.to_thread(
            preencher_e_exportar_lote,
            qlid,
            st["cidade"],
            header,
            produtos,
            st["data"],
            volumes,
            out_pdf,
        )

        # Limpa mensagens anteriores
        ids = st.get("cleanup_ids", [])
        if st.get("progress_msg_id"):
            ids.append(st["progress_msg_id"])

        for mid in ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception as e:
                print(f"[DEBUG] Falha ao apagar mensagem {mid}: {e}")

        # Envia a minuta final
        with open(out_pdf, "rb") as f:
            # await cq.message.reply_document(InputFile(f, filename=os.path.basename(out_pdf)), caption="✅ Sua minuta está pronta. Caso precise, envie mais DANFEs para gerar outra.")
            await cq.message.reply_document(
                InputFile(f, filename=os.path.basename(out_pdf)),
                caption="✅ Sua minuta está pronta.\n\nEnvie mais DANFEs para gerar outra ou digite /opcoes para acessar outras funções.",
            )
            #     "✅ Sua minuta está pronta.\n\nEnvie mais DANFEs para gerar outra ou digite /opcoes para acessar outras funções."
            # )
    except Exception as e:
        await cq.message.reply_text(
            f"Ocorreu um erro ao gerar a minuta.\nDetalhes: {e}"
        )
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


async def set_bot_commands(app):
    await app.bot.set_my_commands([
        BotCommand("start", "Iniciar o bot"),
        BotCommand("opcoes", "Mostrar opções"),
        BotCommand("minutas", "Listar minutas anteriores"),
        BotCommand("alterar", "Alterar cidade"),
        BotCommand("cancelar", "Fechar menus ou teclados")
    ])

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(set_bot_commands).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("opcoes", opcoes))
    app.add_handler(CommandHandler("usuarios", admin_usuarios))
    app.add_handler(CommandHandler("broadcast", admin_broadcast))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.Document.PDF, on_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling()

if __name__ == "__main__":
    main()
