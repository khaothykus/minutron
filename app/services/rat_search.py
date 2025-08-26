# rat_search.py — frames recursivos + clique robusto + extração correta do RAT (alfanumérico via "Num Relat")
import re
import time
from typing import Optional, List, Tuple

from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

from config import RAT_URL, FIREFOX_BINARY, GECKODRIVER_PATH


# ---------- WebDriver ----------
def _build_driver(headless: bool = True):
    opts = FirefoxOptions()
    if headless:
        opts.add_argument("--headless")
    if FIREFOX_BINARY:
        opts.binary_location = FIREFOX_BINARY
    service = Service(executable_path=GECKODRIVER_PATH)
    drv = webdriver.Firefox(service=service, options=opts)
    drv.set_page_load_timeout(60)
    return drv


# ---------- Navegação por frames (frame + iframe), recursivo ----------
def _switch_path(drv, path: List[int]):
    """Vai para o frame indicado pela sequência de índices (ex.: [0,2])."""
    drv.switch_to.default_content()
    for idx in path:
        frames = drv.find_elements(By.CSS_SELECTOR, "frame,iframe")
        drv.switch_to.frame(frames[idx])


def _find_element_in_frames(drv, by, sel, max_depth=5):
    """Procura um elemento em raiz e frames recursivamente. Retorna (path, element) se achar."""
    def dfs(path, depth):
        try:
            _switch_path(drv, path)
            el = drv.find_element(by, sel)
            return path, el
        except Exception:
            pass
        if depth >= max_depth:
            return None
        try:
            frames = drv.find_elements(By.CSS_SELECTOR, "frame,iframe")
        except Exception:
            frames = []
        for i in range(len(frames)):
            res = dfs(path + [i], depth + 1)
            if res:
                return res
        return None

    return dfs([], 0)


def _collect_html_recursive(drv, max_depth=5) -> List[Tuple[str, str, List[int]]]:
    """Retorna [(HTML_UPPER, desc_path, path_indices)] para raiz + todos os frames/iframes (recursivo)."""
    out: List[Tuple[str, str, List[int]]] = []

    def dfs(path, depth):
        try:
            _switch_path(drv, path)
            html = drv.page_source.upper()
            desc = "root" if not path else "frame:" + "/".join(map(str, path))
            out.append((html, desc, path.copy()))
        except Exception:
            return
        if depth >= max_depth:
            return
        frames = drv.find_elements(By.CSS_SELECTOR, "frame,iframe")
        for i in range(len(frames)):
            dfs(path + [i], depth + 1)

    dfs([], 0)
    drv.switch_to.default_content()
    return out


def _collect_text_recursive(drv, max_depth=5) -> List[Tuple[str, str, List[int]]]:
    """Retorna [(TEXT_UPPER, desc_path, path_indices)] usando texto visível (innerText) de todos os frames."""
    out: List[Tuple[str, str, List[int]]] = []

    def dfs(path, depth):
        try:
            _switch_path(drv, path)
            text = drv.execute_script("return document.body ? document.body.innerText : ''") or ""
            text = text.upper()
            desc = "root" if not path else "frame:" + "/".join(map(str, path))
            out.append((text, desc, path.copy()))
        except Exception:
            return
        if depth >= max_depth:
            return
        frames = drv.find_elements(By.CSS_SELECTOR, "frame,iframe")
        for i in range(len(frames)):
            dfs(path + [i], depth + 1)

    dfs([], 0)
    drv.switch_to.default_content()
    return out


# ---------- Heurísticas de extração (alfanumérico; prioriza "Num Relat") ----------
def _extract_rat_number_from_text(text_upper: str) -> Optional[str]:
    """
    Extrai RAT como token alfanumérico (>=8), SEM hífen.
    Prioriza rótulos 'NUM RELAT' (com/sem acento/variação e com 'EXIGIDO' opcional).
    Ignora PDA (que costuma ter hífen/letras).
    """
    # 1) rótulos canônicos
    label_patterns = [
        r"\bNUM\s*RELAT(?:[ÓO]RIO)?\s*[:\-]?\s*(?:EXIGIDO\s*)?([A-Z0-9]{8,})\b",
        r"\bN[ºO]\s*RELAT(?:[ÓO]RIO)?\s*[:\-]?\s*(?:EXIGIDO\s*)?([A-Z0-9]{8,})\b",
        r"\bNUMERO\s*RELAT(?:[ÓO]RIO)?\s*[:\-]?\s*(?:EXIGIDO\s*)?([A-Z0-9]{8,})\b",
        r"\bNUM\.?\s*RELAT(?:[ÓO]RIO)?\s*[:\-]?\s*(?:EXIGIDO\s*)?([A-Z0-9]{8,})\b",
        r"\bRELAT\w*\s*[:\-]?\s*EXIGIDO\s*([A-Z0-9]{8,})\b",
    ]
    for pat in label_patterns:
        m = re.search(pat, text_upper)
        if m:
            tok = m.group(1)
            if "-" not in tok:
                return tok

    # 2) vizinhança de '66 SUBSTITUICAO/SUBSTITUIÇÃO'
    idx = -1
    for needle in ("66 SUBSTITUICAO", "66 SUBSTITUIÇÃO"):
        pos = text_upper.find(needle)
        if pos != -1:
            idx = pos
            break
    if idx != -1:
        win = text_upper[max(0, idx - 600): idx + 1200]
        # alfanumérico >=8, sem hífen, com pelo menos 1 dígito; ignora palavras de rótulo
        for tok in re.findall(r"\b(?=[A-Z0-9]*\d)[A-Z0-9]{8,}\b", win):
            if "-" in tok:
                continue
            if tok in ("SUBSTITUICAO", "SUBSTITUIÇÃO") or "RELAT" in tok or "PDA" in tok:
                continue
            return tok

    # 3) fallback geral
    m = re.search(r"\b(?=[A-Z0-9]*\d)[A-Z0-9]{8,}\b", text_upper)
    if m:
        tok = m.group(0)
        if "-" not in tok:
            return tok

    return None


def _page_has_66_and_rat(drv) -> Optional[str]:
    """
    Verifica todos os frames da página atual; se achar '66 SUBSTITUICAO/SUBSTITUIÇÃO' no TEXTO,
    tenta extrair um número de RAT (alfanumérico, sem hífen). Se não achar número, retorna 'RAT_ENCONTRADO'.
    """
    texts = _collect_text_recursive(drv, max_depth=5)  # usa texto visível
    has_66 = False
    for text, desc, _ in texts:
        if ("66 SUBSTITUICAO" in text) or ("66 SUBSTITUIÇÃO" in text):
            has_66 = True
            rat = _extract_rat_number_from_text(text)
            if rat:
                return rat
    if has_66:
        return "RAT_ENCONTRADO"
    return None


# ---------- Busca principal ----------
def buscar_rat(ocorrencia: str, codigo_produto: str, debug: bool = False, headless: bool = True) -> Optional[str]:
    drv = _build_driver(headless=headless)
    wait = WebDriverWait(drv, 25)
    try:
        drv.get(RAT_URL)

        # Campo de ocorrência (até 5 níveis)
        found = _find_element_in_frames(drv, By.NAME, "s_1_1_13_0", max_depth=5)
        if not found:
            if debug:
                print("[RAT] campo de ocorrência não encontrado.")
            return None
        path_campo, campo = found
        _switch_path(drv, path_campo)
        campo.clear()
        campo.send_keys(ocorrencia)

        # Botão pesquisar (mesmo frame se possível)
        try:
            btn = drv.find_element(By.ID, "s_1_1_19_0")
            path_btn = path_campo
        except Exception:
            found_btn = _find_element_in_frames(drv, By.ID, "s_1_1_19_0", max_depth=5)
            if not found_btn:
                found_btn = _find_element_in_frames(
                    drv,
                    By.XPATH,
                    "//button[contains(., 'Pesquis') or contains(., 'Search') or contains(., 'Consultar')]",
                    max_depth=5,
                )
            if not found_btn:
                if debug:
                    print("[RAT] botão de pesquisa não encontrado.")
                return None
            path_btn, btn = found_btn

        _switch_path(drv, path_btn)
        drv.execute_script("arguments[0].click();", btn)

        # dá tempo para frames renderizarem
        time.sleep(1.8)

        # 1) sem clicar em link: já veio 66?
        rat = _page_has_66_and_rat(drv)
        if debug:
            htmls = _collect_html_recursive(drv, max_depth=2)
            print(f"[RAT] contextos varridos pós-busca: {len(htmls)} (incl. frames)")
        if rat:
            if debug:
                print(f"[RAT] encontrado sem clicar link: {rat}")
            return rat

        # 2) clicar links candidatos em TODOS os frames
        # anchors típicos do Siebel + acessível por @role='link'
        candidate_xpath = (
            "//*[(self::a or @role='link') and "
            "("
            "contains(., 'Relat') or contains(., 'RELAT') or contains(., 'PDA') or "
            "contains(., 'Relatório') or contains(., 'RELATÓRIO')"
            ") or (self::a and starts-with(@id,'s_') and contains(@id,'_35_'))]"
        )

        paths = [p for _, _, p in _collect_html_recursive(drv, max_depth=3)]
        original = drv.current_window_handle

        for path in paths:
            _switch_path(drv, path)
            try:
                links_count = len(drv.find_elements(By.XPATH, candidate_xpath))
            except Exception:
                links_count = 0

            if debug:
                print(
                    f"[RAT] frame {('root' if not path else '/'.join(map(str, path)))}: "
                    f"{links_count} link(s) candidatos)"
                )

            for i in range(1, links_count + 1):
                _switch_path(drv, path)
                try:
                    link = wait.until(
                        EC.presence_of_element_located((By.XPATH, f"({candidate_xpath})[{i}]"))
                    )
                    drv.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
                except Exception as e:
                    if debug:
                        print(f"[RAT] não conseguiu localizar link {i}: {e}")
                    continue

                prev_handles = set(drv.window_handles)
                try:
                    try:
                        drv.execute_script("arguments[0].click();", link)
                    except StaleElementReferenceException:
                        link = drv.find_element(By.XPATH, f"({candidate_xpath})[{i}]")
                        drv.execute_script("arguments[0].click();", link)

                    # espera navegação/refresh
                    try:
                        WebDriverWait(drv, 5).until(EC.staleness_of(link))
                    except Exception:
                        pass
                    time.sleep(0.7)

                    cur_handles = set(drv.window_handles)
                    new_handles = list(cur_handles - prev_handles)
                    if new_handles:
                        drv.switch_to.window(new_handles[-1])

                    # usa TEXTO visível para detecção do 66 e número (alfanumérico, sem hífen)
                    rat = _page_has_66_and_rat(drv)
                    if rat:
                        if debug:
                            print(f"[RAT] encontrado após clique: {rat}")
                        return rat

                except Exception as e:
                    if debug:
                        print(f"[RAT] erro ao clicar link {i}: {e}")
                finally:
                    # volta para janela/origem e frame de origem
                    try:
                        if drv.current_window_handle != original:
                            drv.close()
                            drv.switch_to.window(original)
                    except Exception:
                        try:
                            drv.switch_to.window(original)
                        except Exception:
                            pass
                    _switch_path(drv, path)

        if debug:
            print("[RAT] não encontrado após clicar candidatos.")
        return None

    finally:
        try:
            drv.quit()
        except Exception:
            pass


def get_rat_for_ocorrencia(ocorrencia: str, codigo_produto: str, debug: bool = False) -> str | None:
    """
    Wrapper para integração com o bot.
    Recebe a ocorrência e o código do produto e retorna o RAT como string ou None.
    """
    try:
        return buscar_rat(ocorrencia, codigo_produto, debug=debug, headless=True)
    except Exception as e:
        if debug:
            print(f"[RAT] Erro ao buscar RAT: {e}")
        return None

