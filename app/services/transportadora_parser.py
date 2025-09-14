# services/transportadora_parser.py

from collections import Counter

def analisar_transportadoras(documentos: list[dict]) -> dict:
    transportadoras = [doc.get("transportador", "").strip() for doc in documentos if doc.get("transportador")]
    transportadoras_unicas = list(set(transportadoras))

    if not transportadoras_unicas:
        return {"status": "vazio", "opcoes": []}

    if len(transportadoras_unicas) == 1:
        return {"status": "unico", "escolhida": transportadoras_unicas[0]}

    # Divergência detectada
    contagem = Counter(transportadoras)
    return {
        "status": "divergente",
        "opcoes": transportadoras_unicas,
        "sugestao": contagem.most_common(1)[0][0]
    }
