import os
import json

BASE_DIR = os.getenv(
    "BASE_DATA_DIR",
    os.path.join(os.path.dirname(__file__), "..", "data")
)
DB_PATH = os.path.join(BASE_DIR, "transportadoras.json")


def _load() -> list[str]:
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
        return []
    except FileNotFoundError:
        return []
    except Exception:
        return []


def _save(nomes: list[str]) -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # únicos, ordenados
    uniq = []
    for n in nomes:
        if n and n not in uniq:
            uniq.append(n)
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(uniq, f, ensure_ascii=False, indent=2)


# def add(nome: str) -> None:
#     """Adiciona nome de transportadora normalizado.

#     - Sempre trabalha em UPPERCASE.
#     - Se o novo nome for versão mais completa de um existente, substitui.
#     - Se for abreviação de um existente mais longo, é ignorado.
#     """
#     nome = (nome or "").strip().upper()
#     if not nome:
#         return

#     data = _load()
#     if not data:
#         _save([nome])
#         return

#     keep: list[str] = []
#     should_add = True

#     for existing in data:
#         e = (existing or "").strip().upper()
#         if not e:
#             continue

#         # já existe exatamente
#         if e == nome:
#             should_add = False
#             keep.append(existing)
#             continue

#         # novo é mais completo que o antigo (antigo contido no novo)
#         if e in nome and len(nome) > len(e):
#             # descarta o antigo; vamos adicionar o novo depois
#             should_add = True
#             continue

#         # antigo é mais completo que o novo (novo contido no antigo)
#         if nome in e and len(e) > len(nome):
#             # mantemos o antigo e não adicionamos o novo
#             should_add = False
#             keep.append(existing)
#             continue

#         # nomes independentes
#         keep.append(existing)

#     if should_add:
#         keep.append(nome)

#     _save(keep)

def add(nome: str) -> None:
    nome = (nome or "").strip().upper()
    if not nome:
        return

    data = _load()
    if not data:
        _save([nome])
        return

    keep = []
    should_add = True

    for existing in data:
        e = (existing or "").strip().upper()
        if not e:
            continue

        if e == nome:
            # já temos exatamente esse
            should_add = False
            keep.append(existing)
            continue

        # se o existente é contido no novo, e o novo é mais longo -> novo é mais completo
        if e in nome and len(nome) > len(e):
            # descarta o abreviado, vamos guardar o completo depois
            continue

        # se o novo é contido no existente, e o existente é mais longo -> já temos uma versão melhor
        if nome in e and len(e) > len(nome):
            should_add = False
            keep.append(existing)
            continue

        keep.append(existing)

    if should_add:
        keep.append(nome)

    _save(keep)


def add_many(nomes: list[str]) -> None:
    """Adiciona vários nomes aplicando a mesma lógica de normalização."""
    if not nomes:
        return
    for n in nomes:
        add(n)


def all() -> list[str]:
    return _load()


def best_match(query: str) -> str | None:
    """
    Tenta achar o nome completo com base no que o usuário digitou.
    Regras simples: case-insensitive, contém, começa com.
    """
    q = (query or "").strip()
    if not q:
        return None

    data = _load()
    if not data:
        return None

    q_up = q.upper()

    # match exato (case-insensitive)
    for n in data:
        if n.upper() == q_up:
            return n

    # começa com
    candidatos = [n for n in data if n.upper().startswith(q_up)]
    if len(candidatos) == 1:
        return candidatos[0]
    if len(candidatos) > 1:
        # pega o menor
        return sorted(candidatos, key=len)[0]

    # contém
    candidatos = [n for n in data if q_up in n.upper()]
    if len(candidatos) == 1:
        return candidatos[0]
    if len(candidatos) > 1:
        return sorted(candidatos, key=len)[0]

    return None
