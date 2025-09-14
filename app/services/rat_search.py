import os, re, time, pathlib, unicodedata
from dataclasses import dataclass
from typing import Optional, Tuple, List

from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException, WebDriverException
)
from selenium.webdriver.firefox.service import Service as FirefoxService

# =======================
# Config (via .env)
# =======================
RAT_URL = os.getenv("RAT_URL", "").strip() or "https://servicos.ncratleos.com/consulta_ocorrencia/start.swe"

STEP_TIMEOUT          = int(float(os.getenv("RAT_STEP_TIMEOUT", "25")))
FLOW_TIMEOUT          = int(float(os.getenv("RAT_FLOW_TIMEOUT", "90")))
DEEP_SCAN             = os.getenv("RAT_DEEP_SCAN", "1").lower() not in ("0","false","no")
RESULT_STABILIZE_MS   = int(float(os.getenv("RAT_RESULT_STABILIZE_MS", "700")))
DETAIL_EXTRA_WAIT     = int(float(os.getenv("RAT_DETAIL_EXTRA_WAIT", "5")))
MAX_RATS_PER_OCC      = int(float(os.getenv("RAT_MAX_RATS_PER_OCC", "0")))  # 0 = sem limite

ARTIFACTS = os.getenv("RAT_SAVE_ARTIFACTS", "0").lower() not in ("0", "false", "")
ART_DIR   = pathlib.Path(os.getenv("RAT_ARTIFACTS_DIR", "/tmp/rat_artifacts"))
ART_DIR.mkdir(parents=True, exist_ok=True)

RAT_RE = re.compile(r"\b[0-9A-Z]{2}[0-9A-Z]\d{8,}\b")  # ex: 25H94225371444

def _now_stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S")

def _save_artifacts(driver: Firefox, tag: str) -> None:
    if not ARTIFACTS:
        return
    base = ART_DIR / f"{_now_stamp()}_{tag}"
    try: driver.save_screenshot(str(base.with_suffix(".png")))
    except: pass
    try:
        html = driver.page_source or ""
        base.with_suffix(".html").write_text(html, encoding="utf-8", errors="ignore")
    except: pass

def _log(msg: str):
    print(f"[RAT] {msg}", flush=True)

# =======================
# Scraper
# =======================
@dataclass
class RATScraper:
    headless: bool = True
    driver: Optional[Firefox] = None
    wait:   Optional[WebDriverWait] = None

    def __post_init__(self):
        if self.driver is None:
            opts = FirefoxOptions()
            if self.headless or os.getenv("MOZ_HEADLESS", "1").lower() not in ("0","false","no"):
                opts.add_argument("-headless")

            firefox_bin = os.getenv("FIREFOX_BINARY", "/usr/bin/firefox-esr")
            if os.path.exists(firefox_bin):
                opts.binary_location = firefox_bin

            gpath = os.getenv("GECKODRIVER_PATH", "/usr/local/bin/geckodriver")
            if not os.path.exists(gpath):
                raise RuntimeError(f"Geckodriver não encontrado em {gpath}. Defina GECKODRIVER_PATH no .env.")

            service = FirefoxService(executable_path=gpath)
            self.driver = Firefox(options=opts, service=service)

        self.wait = WebDriverWait(self.driver, STEP_TIMEOUT)

    def quit(self):
        try: self.driver.quit()
        except: pass

    # ---------- helpers ----------
    def _find_occ_input(self) -> Optional[Tuple[str, str]]:
        d = self.driver
        # padrão comum: input de texto com sufixo _13_0
        candidates = d.find_elements(By.CSS_SELECTOR, 'input[type="text"][id*="_13_"][id$="_0"]')
        for el in candidates:
            try:
                if el.is_displayed() and el.get_attribute("id"):
                    cid = el.get_attribute("id")
                    return (f'input[type="text"][id="{cid}"]', cid)
            except StaleElementReferenceException:
                continue
        # fallback amplo (primeiro input visível)
        for el in d.find_elements(By.CSS_SELECTOR, 'input[type="text"]'):
            try:
                if el.is_displayed() and el.get_attribute("id"):
                    cid = el.get_attribute("id")
                    return (f'input[type="text"][id="{cid}"]', cid)
            except:
                continue
        return None

    def _find_search_button(self) -> Optional[Tuple[str, str]]:
        d = self.driver
        btns = d.find_elements(By.CSS_SELECTOR, 'input[type="button"][value*="Pesquisar"]')
        for b in btns:
            try:
                if b.is_displayed() and b.get_attribute("id"):
                    bid = b.get_attribute("id")
                    return (f'input[type="button"][id="{bid}"]', bid)
            except StaleElementReferenceException:
                continue
        return None

    # ---------- fluxos ----------
    def open_search(self):
        _log(f"Abrindo {RAT_URL}")
        self.driver.set_page_load_timeout(STEP_TIMEOUT)
        self.driver.get(RAT_URL)
        self.wait.until(lambda d: self._find_occ_input() is not None)
        _save_artifacts(self.driver, "01_search_ready")
        _log("Tela de busca pronta.")

    def submit_ocorrencia(self, ocorrencia: str):
        found_input = self._find_occ_input()
        if not found_input:
            _save_artifacts(self.driver, "00_no_input")
            raise RuntimeError("Campo de ocorrência não encontrado.")
        css_input, _ = found_input
        inp = self.driver.find_element(By.CSS_SELECTOR, css_input)
        inp.clear()
        inp.send_keys(ocorrencia)
        inp.send_keys(Keys.TAB)
        time.sleep(0.2)

        found_btn = self._find_search_button()
        if not found_btn:
            _save_artifacts(self.driver, "00_no_button")
            raise RuntimeError("Botão 'Pesquisar/Query' não encontrado.")
        css_btn, _ = found_btn
        btn = self.driver.find_element(By.CSS_SELECTOR, css_btn)
        self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.1)

        # 3 estratégias: click, JS click, SWESubmitForm(...)
        for attempt in (1, 2, 3):
            try:
                if attempt == 1:
                    btn.click()
                elif attempt == 2:
                    self.driver.execute_script("arguments[0].click();", btn)
                else:
                    self.driver.execute_script(
                        """
                        try {
                          if (window.SWESubmitForm) {
                            SWESubmitForm(document.SWEForm1_0, arguments[0], arguments[0].id, "");
                          } else {
                            arguments[0].click();
                          }
                        } catch (e) { arguments[0].click(); }
                        """,
                        btn,
                    )
                break
            except WebDriverException as e:
                _log(f"click tentativa {attempt} falhou: {e}")
                time.sleep(0.2)

        _save_artifacts(self.driver, "02_sent")

        def _on_results(drv):
            html = drv.page_source or ""
            up = unicodedata.normalize("NFKD", html).encode("ascii","ignore").decode("ascii").upper()
            return ("REGISTROS 1 -" in up) or ("APONTAMENTOS" in up) or (RAT_RE.search(html) is not None)

        WebDriverWait(self.driver, STEP_TIMEOUT).until(_on_results)
        _save_artifacts(self.driver, "03_result")
        _log("Resultados carregados.")

    def _wait_for_detail_ready(self, rat_code: str) -> None:
        """Espera o detalhe do RAT exibir 'Apontamentos' ou a coluna 'Solução'."""
        try:
            def ok(drv):
                html = drv.page_source or ""
                up = unicodedata.normalize("NFKD", html).encode("ascii","ignore").decode("ascii").upper()
                return (rat_code in up) and ("APONTAMENTOS" in up or "SOLUCAO" in up)
            WebDriverWait(self.driver, STEP_TIMEOUT + DETAIL_EXTRA_WAIT).until(ok)
        except Exception:
            _save_artifacts(self.driver, f"10b_detail_wait_timeout_{rat_code}")
        time.sleep(RESULT_STABILIZE_MS / 1000.0)

    def _open_rat_detail_if_needed(self, rat_code: str) -> None:
        """Clica no <a> cujo texto começa com o número do RAT; se não houver, assume que já está no detalhe."""
        try:
            link = self.driver.find_element(
                By.XPATH, f'//a[starts-with(normalize-space(.), "{rat_code} ")]'
            )
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
            self.driver.execute_script("arguments[0].click();", link)
            _save_artifacts(self.driver, f"10_rat_open_{rat_code}")
        except Exception:
            pass
        self._wait_for_detail_ready(rat_code)

    def _row_has_produto_ok(self, row_html: str, produto: str) -> bool:
        """Checa produto + '66 SUBSTIT' na MESMA linha (normalizando acentos/dígitos)."""
        raw = re.sub(r"(?s)<[^>]+>", " ", row_html)
        up  = unicodedata.normalize("NFKD", raw).encode("ascii","ignore").decode("ascii").upper()
        prod_digits = "".join(ch for ch in produto if ch.isdigit())
        row_digits  = "".join(ch for ch in up if ch.isdigit())
        tem_produto = bool(prod_digits) and (prod_digits in row_digits)
        tem_solucao = "66 SUBSTIT" in up
        return tem_produto and tem_solucao

    def _scan_grid_for_hit(self, produto: str) -> bool:
        html = self.driver.page_source or ""
        rows = re.split(r"(?i)<tr[^>]*>", html)
        for chunk in rows:
            if self._row_has_produto_ok(chunk, produto):
                return True
        return False

    def _extract_rat_candidates(self) -> List[str]:
        html = self.driver.page_source or ""
        return list({m.group(0) for m in RAT_RE.finditer(html)})

    def find_first_valid_rat(self, ocorrencia: str, produto: str) -> Optional[str]:
        # prefixo da ocorrência (PH94225371 -> 25H94225371)
        def _letter_to_index(ch: str) -> int:
            ch = ch.upper()
            return int(ch) if ch.isdigit() else 10 + (ord(ch) - ord("A"))
        ocorrencia = (ocorrencia or "").strip().upper()
        rat_prefix = f"{_letter_to_index(ocorrencia[0])}{ocorrencia[1:]}" if ocorrencia else ""

        cands_all = self._extract_rat_candidates()
        cands = [r for r in cands_all if r.startswith(rat_prefix)]
        if MAX_RATS_PER_OCC > 0:
            cands = cands[:MAX_RATS_PER_OCC]
        _log(f"RATs candidatos (filtrados por prefixo {rat_prefix}): {cands}")

        if not cands:
            _save_artifacts(self.driver, "09_no_candidates")
            return None

        # PASSO 1 — rápido
        for rat in cands:
            try:
                self._open_rat_detail_if_needed(rat)
                if self._scan_grid_for_hit(produto):
                    _log(f"RAT {rat} contem produto {produto} com '66 SUBSTITUICAO'.")
                    _save_artifacts(self.driver, f"11_rat_row_hit_{rat}")
                    return rat
            except Exception as e:
                _log(f"Erro verificando RAT {rat} (passo1): {e}")
            finally:
                # volta (se houver subnavegação)
                try:
                    self.driver.back()
                    WebDriverWait(self.driver, STEP_TIMEOUT).until(
                        lambda d: rat not in (d.page_source or "") or "REGISTROS 1 -" in unicodedata.normalize("NFKD", d.page_source).encode("ascii","ignore").decode("ascii").upper()
                    )
                except Exception:
                    pass

        if not DEEP_SCAN:
            _save_artifacts(self.driver, "99_no_match")
            return None

        # PASSO 2 — deep scan (espera extra e rechecagem)
        for rat in cands:
            try:
                self._open_rat_detail_if_needed(rat)
                self._wait_for_detail_ready(rat)
                if self._scan_grid_for_hit(produto):
                    _log(f"[deep] RAT {rat} contem produto {produto} com '66 SUBSTITUICAO'.")
                    _save_artifacts(self.driver, f"11_rat_row_hit_deep_{rat}")
                    return rat
            except Exception as e:
                _log(f"Erro verificando RAT {rat} (deep): {e}")
            finally:
                try:
                    self.driver.back()
                    WebDriverWait(self.driver, STEP_TIMEOUT).until(
                        lambda d: rat not in (d.page_source or "") or "REGISTROS 1 -" in unicodedata.normalize("NFKD", d.page_source).encode("ascii","ignore").decode("ascii").upper()
                    )
                except Exception:
                    pass

        _save_artifacts(self.driver, "99_no_match")
        return None

# =======================
# API de alto nível
# =======================
def get_rat_for_ocorrencia(ocorrencia: str, produto: str) -> Optional[str]:
    start = time.time()
    sc = RATScraper(headless=os.getenv("RAT_HEADLESS","1").lower() not in ("0","false","no"))
    try:
        sc.open_search()
        sc.submit_ocorrencia(ocorrencia)
        rat = sc.find_first_valid_rat(ocorrencia, produto)
        _log(f"Resultado final: {rat}")
        return rat
    finally:
        _save_artifacts(sc.driver, "99_final")
        sc.quit()
        _log(f"Tempo total {time.time() - start:.1f}s")
