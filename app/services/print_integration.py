from telegram import InlineKeyboardButton, InlineKeyboardMarkup
# -*- coding: utf-8 -*-
import os
import shlex
import subprocess
from pathlib import Path
from typing import Sequence

from services.pdf_merge import merge_pdfs

# ——— ENV ———
PRINT_ENABLE = os.getenv("PRINT_ENABLE", "0") == "1"
PRINT_PRINTER_NAME = (os.getenv("PRINT_PRINTER_NAME") or "").strip() or None
PRINT_COPIES = int(os.getenv("PRINT_COPIES", "1") or "1")
PRINT_OPTIONS = (os.getenv("PRINT_OPTIONS") or "").strip()
PRINT_AUTO = os.getenv("PRINT_AUTO", "0") == "1"
MERGE_DANFES_WITH_MINUTA = os.getenv("MERGE_DANFES_WITH_MINUTA", "0") == "1"
PRINT_ADD_MARGIN_MM = float(os.getenv("PRINT_ADD_MARGIN_MM", "3") or "0")
PRINT_FIT_TO_PAGE = os.getenv("PRINT_FIT_TO_PAGE", "1") == "1"

_ADMIN_IDS = {
    int(x.strip())
    for x in (os.getenv("PRINT_ADMIN_CHAT_IDS") or "").split(",")
    if x.strip().isdigit()
}
PRINT_ANY_PDF_ENABLE = os.getenv("PRINT_ANY_PDF_ENABLE", "0") == "1"
PRINT_MAX_FILE_MB = int(os.getenv("PRINT_MAX_FILE_MB", "20") or "20")

# ——— Helpers de autorização ———
def _get_user_and_chat_ids(update) -> tuple[int | None, int | None]:
    uid = update.effective_user.id if update and update.effective_user else None
    cid = update.effective_chat.id if update and update.effective_chat else None
    return uid, cid

def is_admin(update) -> bool:
    uid, cid = _get_user_and_chat_ids(update)
    return (uid in _ADMIN_IDS) or (cid in _ADMIN_IDS)

# ——— Sumir com mensagens de inline keyboard ———
async def safe_delete_message(cq=None, msg=None):
    """
    Apaga a mensagem de botões após a decisão.
    Use: await safe_delete_message(cq=cq) num callback; ou safe_delete_message(msg=mensagem).
    Faz fallback para remover somente o teclado se não puder apagar.
    """
    try:
        if cq is not None and getattr(cq, "message", None):
            await cq.message.delete()
            return
    except Exception:
        pass
    try:
        if msg is not None:
            await msg.delete()
            return
    except Exception:
        pass
    try:
        if cq is not None and getattr(cq, "message", None):
            await cq.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

# ——— Execução do lp/CUPS ———

# ——— Ajuste de margens no PDF (evita corte nas bordas) ———
def _apply_margins_if_needed(pdf_path: str) -> str:
    """Se PRINT_ADD_MARGIN_MM > 0, cria um novo PDF com margem branca em volta."""
    if PRINT_ADD_MARGIN_MM <= 0:
        return pdf_path
    try:
        from pypdf import PdfReader, PdfWriter, Transformation
        from pypdf.generic import RectangleObject
    except Exception:
        return pdf_path
    src = Path(pdf_path)
    if not src.exists():
        return pdf_path
    margin_pts = float(PRINT_ADD_MARGIN_MM) * 2.83465
    try:
        reader = PdfReader(str(src))
        writer = PdfWriter()
        for page in reader.pages:
            w = float(page.mediabox.width)
            h = float(page.mediabox.height)
            new_w = w + 2 * margin_pts
            new_h = h + 2 * margin_pts
            writer.add_blank_page(width=new_w, height=new_h)
            page.add_transformation(Transformation().translate(margin_pts, margin_pts))
            writer.pages[-1].merge_page(page)
        out = src.with_name(src.stem + f"_m{int(PRINT_ADD_MARGIN_MM)}mm.pdf")
        with out.open("wb") as f:
            writer.write(f)
        return str(out)
    except Exception:
        return pdf_path

def _lp_print(pdf_path: str) -> tuple[bool, str]:
    if not PRINT_ENABLE:
        return False, "PRINT_ENABLE=0"
    if not PRINT_PRINTER_NAME:
        return False, "PRINT_PRINTER_NAME não definido"
    cmd = ["lp", "-d", PRINT_PRINTER_NAME, "-n", str(PRINT_COPIES)]
    eff_options = PRINT_OPTIONS
    if PRINT_FIT_TO_PAGE and ("fit-to-page" not in eff_options):
        eff_options = (eff_options + " fit-to-page").strip()
    if eff_options:
        for opt in shlex.split(eff_options):
            cmd.extend(["-o", opt])
    cmd.append(pdf_path)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, check=False)
        ok = r.returncode == 0
        msg = r.stdout.strip() or r.stderr.strip()
        return ok, msg or ("ok" if ok else "erro desconhecido")
    except Exception as e:
        return False, f"falha ao executar lp: {e}"

# ——— Função principal para seu fluxo ———
async def finalize_minuta_and_print(update, context, *, minuta_pdf_path: str, danfe_paths: Sequence[str] | None):
    """
    Chame esta função após gerar a MINUTA (sem DANFEs).

    Responsabilidades:
    - Se MERGE_DANFES_WITH_MINUTA=1 e houver DANFEs, junta ao final (na ordem recebida).
    - Aplica margem extra, se configurado.
    - Envia o PDF final no chat.
    - Guarda o caminho em context.user_data['last_minuta_pdf'] (para quem quiser usar).
    - Se PRINT_AUTO=1 e autor for admin, dispara impressão imediata.

    Não mostra botões de "imprimir agora" aqui.
    Os botões (sem/com DANFEs) são tratados no bot.py.
    """
    danfe_paths = list(danfe_paths or [])
    final_pdf = minuta_pdf_path

    # Merge opcional com DANFEs
    if MERGE_DANFES_WITH_MINUTA and danfe_paths:
        out = Path(minuta_pdf_path).with_name(Path(minuta_pdf_path).stem + "_COM_DANFES.pdf")
        try:
            # merge_pdfs respeita a ordem da lista fornecida
            final_pdf = merge_pdfs([minuta_pdf_path, *danfe_paths], out)
        except Exception as e:
            final_pdf = minuta_pdf_path
            try:
                import logging
                logging.getLogger(__name__).exception("Falha no merge DANFEs: %s", e)
            except Exception:
                pass

    # Aplica margem extra (se habilitado)
    final_pdf = _apply_margins_if_needed(final_pdf)

    # Envia o PDF final no chat (minuta com DANFEs, quando houver merge)
    try:
        caption = "🧾 Minuta gerada (com DANFEs anexadas)." if final_pdf != minuta_pdf_path else "🧾 Minuta gerada."
        await update.effective_message.reply_document(
            document=open(final_pdf, "rb"),
            caption=caption,
        )
    except Exception:
        # Não derruba o fluxo se falhar só no envio do arquivo
        pass

    # Guarda para possíveis callbacks externos (botões no bot.py)
    context.user_data["last_minuta_pdf"] = final_pdf

    # Impressão automática (sem teclado), se habilitado e admin
    if PRINT_ENABLE and PRINT_AUTO and is_admin(update):
        ok, msg = _lp_print(str(final_pdf))
        try:
            await update.effective_message.reply_text(
                f"Impressão automática: {'OK' if ok else 'ERRO'} — {msg}"
            )
        except Exception:
            pass
    elif PRINT_ENABLE and PRINT_AUTO and not is_admin(update):
        try:
            await update.effective_message.reply_text(
                "PDF gerado. Impressão automática restrita a administradores."
            )
        except Exception:
            pass

    return final_pdf

# ——— Comandos utilitários ———
async def meuid_cmd(update, context):
    uid, cid = _get_user_and_chat_ids(update)
    await update.effective_message.reply_text(f"user_id={uid}\nchat_id={cid}")

async def print_cmd(update, context):
    if not PRINT_ANY_PDF_ENABLE:
        await update.effective_message.reply_text("Impressão manual desativada (PRINT_ANY_PDF_ENABLE=0).")
        return
    if not PRINT_ENABLE:
        await update.effective_message.reply_text("CUPS desligado (PRINT_ENABLE=0).")
        return
    if not is_admin(update):
        await update.effective_message.reply_text("⛔️ Você não tem permissão para imprimir.")
        return

    msg = update.effective_message
    doc = getattr(msg, "document", None)
    if (doc is None) and msg.reply_to_message:
        doc = getattr(msg.reply_to_message, "document", None)
    if doc is None:
        await msg.reply_text("Envie um PDF ou responda a um PDF com /print.")
        return

    mime_ok = (doc.mime_type or "").lower() == "application/pdf"
    size_ok = (doc.file_size or 0) <= PRINT_MAX_FILE_MB * 1024 * 1024
    if not mime_ok:
        await msg.reply_text("O arquivo deve ser PDF (application/pdf).")
        return
    if not size_ok:
        await msg.reply_text(f"PDF muito grande (>{PRINT_MAX_FILE_MB} MB).")
        return

    try:
        f = await context.bot.get_file(doc.file_id)
        base = (doc.file_name or "arquivo.pdf")
        safe = "".join(ch for ch in base if ch.isalnum() or ch in "._-").strip(".")
        if not safe.lower().endswith(".pdf"):
            safe += ".pdf"
        out_path = Path("/tmp") / f"print_{safe}"
        await f.download_to_drive(custom_path=str(out_path))
    except Exception as e:
        await msg.reply_text(f"Falha ao baixar PDF: {e}")
        return

    ok, resp = _lp_print(str(out_path))
    await msg.reply_text(f"Impressão: {'OK' if ok else 'ERRO'} — {resp}")
