from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove

# def kb_main():
#     return ReplyKeyboardMarkup([["Gerar minuta"], ["Minhas minutas"], ["Alterar cidade"]], resize_keyboard=True)

# def kb_cadastro():
#     return ReplyKeyboardMarkup([["Cadastrar QLID"], ["Cadastrar Cidade"]], resize_keyboard=True)


def kb_cadastro():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Cadastrar QLID", callback_data="cad_qlid")],
        #[InlineKeyboardButton("Cadastrar Cidade", callback_data="cad_cidade")]
    ])

def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Gerar Minuta", callback_data="gerar_minuta")],
        #[InlineKeyboardButton("Alterar Cidade", callback_data="alterar_cidade")],
        #[InlineKeyboardButton("Minhas Minutas", callback_data="minhas_minutas")]
    ])

def kb_datas():
    hoje = datetime.now().date()
    btns = []
    for i in range(4):
        d = hoje + timedelta(days=i)
        btns.append([InlineKeyboardButton(d.strftime("%d/%m/%Y"), callback_data=f"data_{d.isoformat()}")])
    return InlineKeyboardMarkup(btns)

def kb_volumes(volbuf=""):
    rows = [
        [InlineKeyboardButton("1", callback_data="vol_1"),
         InlineKeyboardButton("2", callback_data="vol_2"),
         InlineKeyboardButton("3", callback_data="vol_3")],
        [InlineKeyboardButton("4", callback_data="vol_4"),
         InlineKeyboardButton("5", callback_data="vol_5"),
         InlineKeyboardButton("6", callback_data="vol_6")],
        [InlineKeyboardButton("7", callback_data="vol_7"),
         InlineKeyboardButton("8", callback_data="vol_8"),
         InlineKeyboardButton("9", callback_data="vol_9")],
        [InlineKeyboardButton("0", callback_data="vol_0"),
         InlineKeyboardButton("Apagar", callback_data="vol_del"),
         InlineKeyboardButton("OK", callback_data=f"vol_ok")]
    ]
    return InlineKeyboardMarkup(rows)
