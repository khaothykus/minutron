import os, uuid, shutil, json
from datetime import datetime
from typing import Dict, Any
from config import BASE_DATA_DIR

USERS_FILE = f"{BASE_DATA_DIR}/users.json"

def _load_users() -> Dict[str, Any]:
    if not os.path.exists(USERS_FILE): return {}
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        try: return json.load(f)
        except: return {}

def _save_users(data: Dict[str, Any]):
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def users_get_all():
    return _load_users()

def users_upsert(qlid: str, record: Dict[str, Any]):
    data = _load_users()
    data[qlid] = record
    _save_users(data)

def users_delete(qlid: str):
    data = _load_users()
    data.pop(qlid, None)
    _save_users(data)
    shutil.rmtree(user_dir(qlid), ignore_errors=True)

def users_find_by_tg(tg_id: int):
    data = _load_users()
    for qlid, rec in data.items():
        if rec.get("telegram_id") == tg_id:
            return qlid, rec
    return None, None

def user_dir(qlid: str) -> str:
    d = f"{BASE_DATA_DIR}/users/{qlid}"
    os.makedirs(d, exist_ok=True)
    os.makedirs(f"{d}/minutas", exist_ok=True)
    os.makedirs(f"{d}/temp", exist_ok=True)
    return d

def new_session(qlid: str) -> str:
    sid = uuid.uuid4().hex[:8]
    d = f"{user_dir(qlid)}/temp/{sid}"
    os.makedirs(f"{d}/pdfs", exist_ok=True)
    return sid

def save_pdf(qlid: str, sid: str, filename: str) -> str:
    d = f"{user_dir(qlid)}/temp/{sid}/pdfs"
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, filename)

def finalize_session(qlid: str, sid: str):
    d = f"{user_dir(qlid)}/temp/{sid}"
    shutil.rmtree(d, ignore_errors=True)

def output_pdf_path(qlid: str) -> str:
    ts = datetime.now().strftime("%d%m%Y_%H%M%S")
    return f"{user_dir(qlid)}/minutas/{qlid}_{ts}.pdf"

def list_minutas(qlid: str):
    d = f"{user_dir(qlid)}/minutas"
    if not os.path.isdir(d): return []
    files = [os.path.join(d, f) for f in os.listdir(d) if f.lower().endswith(".pdf")]
    return sorted(files, key=os.path.getmtime, reverse=True)

def user_set_transportadora_padrao(tg_id: int, nome: str) -> bool:
    """
    Define/atualiza a transportadora padrão do usuário (via telegram_id).
    Retorna True se salvou, False se não achou usuário.
    """
    data = _load_users()
    found = False

    for qlid, rec in data.items():
        if rec.get("telegram_id") == tg_id:
            rec["transportadora_padrao"] = nome
            data[qlid] = rec
            found = True
            break

    if found:
        _save_users(data)

    return found


def user_get_config_by_tg(tg_id: int) -> dict:
    """
    Retorna o registro do usuário (se cadastrado) ou {}.
    """
    _, rec = users_find_by_tg(tg_id)
    return rec or {}
