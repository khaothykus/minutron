# minutron

Bot de Telegram que gera **minutas em PDF** a partir de **DANFEs em PDF**. Feito para rodar no **Raspberry Pi 5 (Debian Bookworm, arm64)**, usando **LibreOffice/UNO** para preencher um template (XLSX/ODS), **overlay de logo** e **scraper de RAT** (Selenium + Firefox‑ESR + geckodriver).

> **Pasta de dados**: `/home/pi/minutron/data` (fora do código, não versionada)

---

## Recursos

* **Envio de DANFEs** (um ou várias) por chat do Telegram
* **Extração** de cabeçalho e itens (pdfplumber/pdfminer.six)
* **Geração da minuta** no **Excel via UNO** (mantém layout/colunas do template, auto‑ajuste A4 retrato, margens, linhas)
* **Logo no cabeçalho** do PDF final por overlay (pypdf + posição/escala configuráveis)
* **Busca de RAT** robusta (Selenium + Firefox‑ESR), com artefatos de debug opcionais (HTML/PNG)
* **Serviço systemd** (restart on‑failure) + **timer** opcional de limpeza de artefatos

---

## Requisitos

* Raspberry Pi 5 com Raspberry Pi OS/Debian Bookworm (arm64)
* Pacotes: `python3-venv`, `libreoffice-calc`, `python3-uno`, `default-jre`, `firefox-esr`, `geckodriver` (via apt ou download), fontes básicas
* Python 3.11 (venv local em `.venv`)

> O instalador abaixo cuida de tudo isso.

---

## Instalação rápida

```bash
# 1) Clonar o projeto (ou copiar seus arquivos atuais)
cd /home/pi
git clone https://github.com/khaothykus/bot-copilot minutron
cd minutron

# 2) (Opcional) renomeie/personalize seu template e logo em app/templates/
#    - minuta_template.xlsx
#    - logo.png

# 3) Criar e rodar o instalador
chmod +x install_minutron_pi5.sh
sudo ./install_minutron_pi5.sh

# 4) Preencher o .env com seu BOT_TOKEN e ADMIN_TELEGRAM_ID
nano /home/pi/minutron/.env

# 5) Subir e acompanhar
sudo systemctl start minutron
journalctl -u minutron -f
```

> O instalador cria o serviço **`minutron.service`** e um `.env` base. Ele também instala/atualiza o `requirements.txt` e prepara as pastas de dados.

---

## Atualização (deploy)

```bash
cd /home/pi/minutron
sudo systemctl stop minutron

# atualizar código (ajuste para o seu remoto/branch)
git pull

# atualizar deps no venv
source .venv/bin/activate
pip install -U -r requirements.txt
deactivate

sudo systemctl start minutron
journalctl -u minutron -f
```

---

## Estrutura de pastas

```
/home/pi/minutron
├── app/
│   ├── bot.py
│   ├── config.py
│   └── services/
│       ├── excel_filler_uno.py
│       ├── pdf_tools.py
│       ├── rat_search.py
│       ├── danfe_parser.py
│       └── ...
├── app/templates/
│   ├── minuta_template.xlsx  (ou .ods)
│   ├── logo.png              (opcional)
│   └── .keep
├── data/                     (runtime; NÃO versionado)
│   ├── users.json
│   ├── users/<qlid>/minutas/*.pdf
│   ├── users/<qlid>/temp/
│   └── rat_artifacts/*.html|*.png
├── .env
├── .venv/
├── requirements.txt
└── install_minutron_pi5.sh
```

---

## Variáveis de ambiente (.env)

### Telegram

| Variável            | Exemplo         | Observações                    |
| ------------------- | --------------- | ------------------------------ |
| `BOT_TOKEN`         | `123456:ABC...` | Obrigatório                    |
| `ADMIN_TELEGRAM_ID` | `123456789`     | Para mensagens administrativas |

### Dirs & Template

| Variável        | Exemplo                                                | Observações           |
| --------------- | ------------------------------------------------------ | --------------------- |
| `BASE_DATA_DIR` | `/home/pi/minutron/data`                               | Pasta raiz de dados   |
| `TEMPLATE_PATH` | `/home/pi/minutron/app/templates/minuta_template.xlsx` | `.xlsx` **ou** `.ods` |

### Logo (overlay opcional)

| Variável           | Exemplo                                    | Observações            |
| ------------------ | ------------------------------------------ | ---------------------- |
| `LOGO_HEADER_PATH` | `/home/pi/minutron/app/templates/logo.png` | Se vazio, não aplica   |
| `LOGO_ALIGN`       | `left` \| `center` \| `right`              | Alinhamento horizontal |
| `LOGO_WIDTH_MM`    | `35`                                       | Largura do logo (mm)   |
| `LOGO_TOP_MM`      | `30`                                       | Distância do topo (mm) |
| `LOGO_MARGIN_MM`   | `30`                                       | Margem lateral (mm)    |

### Página (impressão Calc via UNO)

| Variável              | Exemplo              | Observações                             |
| --------------------- | -------------------- | --------------------------------------- |
| `PAGE_FORMAT`         | `A4`                 | `A4`\|`A3`\|`LETTER`                    |
| `PAGE_ORIENTATION`    | `PORTRAIT`           | ou `LANDSCAPE`                          |
| `MARGIN_*_MM`         | `12`, `8`, `16`, `8` | Top/Bottom/Left/Right (mm)              |
| `PAGE_SCALE`          | `AUTO` ou vazio      | `AUTO` calcula zoom para ocupar a folha |
| `CENTER_H`/`CENTER_V` | `1`/`0`              | Centralização na página                 |
| `SCALE_TO_PAGES_X`    | `1`                  | Largura em 1 página                     |
| `SCALE_TO_PAGES_Y`    | `0`                  | Altura livre                            |

### RAT / Selenium

| Variável                  | Exemplo                                                        | Observações                         |
| ------------------------- | -------------------------------------------------------------- | ----------------------------------- |
| `FIREFOX_BINARY`          | `/usr/bin/firefox-esr`                                         | Firefox‑ESR headless                |
| `GECKODRIVER_PATH`        | `/usr/local/bin/geckodriver`                                   | Caminho do driver                   |
| `RAT_URL`                 | `https://servicos.ncratleos.com/consulta_ocorrencia/start.swe` | Endpoint de busca                   |
| `MOZ_HEADLESS`            | `1`                                                            | Headless on                         |
| `RAT_HEADLESS`            | `1`                                                            | Headless on                         |
| `RAT_PAGELOAD_TIMEOUT`    | `35`                                                           | Tempo máximo de carregamento (s)    |
| `RAT_STEP_TIMEOUT`        | `25`                                                           | Timeout por etapa (s)               |
| `RAT_FLOW_TIMEOUT`        | `90`                                                           | Timeout total (s)                   |
| `RAT_RESULT_STABILIZE_MS` | `900`                                                          | Debounce (ms)                       |
| `RAT_DETAIL_EXTRA_WAIT`   | `7`                                                            | Espera adicional ao abrir detalhe   |
| `RAT_DEEP_SCAN`           | `1`                                                            | Faz verificação profunda no detalhe |
| `RAT_MAX_RATS_PER_OCC`    | `80`                                                           | Limite de candidatos por ocorrência |
| `RAT_NAV_TIMEOUT_S`       | `15`                                                           | Timeout navegação por link          |
| `RAT_STEP_TIMEOUT_S`      | `12`                                                           | Timeout pequeno por ação            |
| `RAT_TOTAL_TIMEOUT_S`     | `60`                                                           | Timeout total auxiliar              |
| `RAT_NETWORK_IDLE_S`      | `2.5`                                                          | Aguardar rede ociosa (s)            |
| `RAT_SAVE_ARTIFACTS`      | `0`                                                            | `1` p/ salvar HTML/PNG              |
| `RAT_ARTIFACTS_DIR`       | `/home/pi/minutron/data/rat_artifacts`                         | Pasta de artefatos                  |

### Logs/Locale/TZ

| Variável          | Exemplo             |
| ----------------- | ------------------- |
| `LOG_LEVEL`       | `INFO`              |
| `LC_ALL` / `LANG` | `pt_BR.UTF-8`       |
| `TZ`              | `America/Sao_Paulo` |

> **Importante**: no `.env`, **não coloque comentários na mesma linha do valor** (ex.: `RAT_STEP_TIMEOUT=25  # comentário`). Use linhas iniciadas por `#` para comentar.

---

## Uso

* Abra o chat do seu bot no Telegram e envie `/start` uma única vez para cadastrar.
* Envie **uma ou várias DANFEs em PDF** (anexe no mesmo chat). O bot:

  1. Valida/parseia as DANFEs
  2. Busca RATs necessários
  3. Preenche o template via UNO
  4. Aplica logo (se configurado)
  5. Retorna a **minuta final em PDF**

> Futuro/planejado: opção de **agrupar as DANFEs originais** ao final da minuta (sem alterar o conteúdo). Quando disponível, haverá chave no `.env` e opção no fluxo.

---

## Comandos úteis

```bash
# status/logs
sudo systemctl status minutron
journalctl -u minutron -f

# reiniciar
sudo systemctl restart minutron

# checar variáveis que o systemd está lendo
systemctl show minutron -p EnvironmentFiles -p Environment
```

---

## Solução de problemas

* **Bot reiniciando sozinho**: por padrão não usamos watchdog do systemd. Se você habilitar `sd_notify`/watchdog no futuro, garanta que os "heartbeats" sejam enviados (ou o systemd pode matar o processo durante operações longas).
* **Geckodriver não encontrado**: confira `GECKODRIVER_PATH` e `which geckodriver`. O instalador baixa o binário arm64 se o apt não entregar.
* **Layout/escala divergindo do template**: ajuste `PAGE_SCALE=AUTO`, `MARGIN_*_MM`, `CENTER_V=0`, `SCALE_TO_PAGES_X=1`, `SCALE_TO_PAGES_Y=0`. O filler UNO respeita largura/colunas do template, e o AUTO busca ocupar a folha.
* **RAT intermitente**: ative `RAT_SAVE_ARTIFACTS=1` e verifique HTML/PNG em `data/rat_artifacts/` para entender o estado real da página.

---

## Segurança

* **NUNCA** commite o `.env`. O `.gitignore` já ignora `data/**` e `.env`.
* O token do bot dá controle da sua automação – trate como segredo.

---

## Licença

MIT – veja o arquivo `LICENSE`.
