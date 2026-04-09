# 外資半量化交易訊號系統（GitHub Actions）v12

> 整合「外資籌碼分點追蹤」×「半量化交易訊號」×「AI 深度分析」的全自動盤後報表系統。
> 每個交易日收盤後自動執行，產出次日可操作的進場價、停損價、停利目標，並寄出 HTML 報表。

---

## 核心功能

### 📡 外資籌碼追蹤
- 追蹤九大外資券商（大摩、小摩、美林、高盛、花旗、麥格理、匯豐、瑞銀等）的盤後分點買賣超資料
- 依各外資總淨超張數排序，每家列出 Top 10 持股明細
- 計算區間均價（成本）與現價乖離率

### 📊 半量化交易訊號（核心）
每筆外資持股自動計算以下訊號，**六大條件全部通過才亮 🔥可買**：

| 訊號 | 公式 / 條件 |
|------|------------|
| 進場價 | `MAX(今日收盤, 20日最高) × 1.01` |
| 停損價 | `進場價 × 0.92`（固定 -8%） |
| 停利1 | `進場價 × 1.20`（+20%） |
| 停利2 | `進場價 × 1.35`（+35%） |
| 移動停損 | 10 日均線（漲多後啟動） |
| 每張風險 | `(進場價 - 停損價) × 1000`（NT$） |
| 風報比 | `停利1獲利 ÷ 停損損失`（恆為 2.5） |

**六大進場條件（AND 邏輯，缺一不可）：**
1. `突破20日高`：今日最高價 > 20日最高價
2. `放量1.5倍`：今日成交量 > 5日均量 × 1.5
3. `流動性足夠`：5日均量 ≥ 500張（≈ 5,000,000 股）
4. `站上10日均`：現價 ≥ 10日均線
5. `大盤站月線`：加權指數 > 20日均線
6. `外資淨買超`：近 N 日淨超張數 > 0

> ⚠️ 注意：AI 分析報告的「可買」判斷是質化分析（多家外資共同買超、籌碼集中度等），與上述六條件量化訊號為**不同邏輯**，兩者可以不同，相互補充。

### 🌐 大盤籌碼資料
- 大盤指數 + 成交金額（近 6 日趨勢，自動判斷放量/縮量倍率）
- 三大法人現貨買賣超（外資、投信、自營商）累計趨勢
- 期貨籌碼（三大法人台指期淨未平倉口數）
- 千張大戶持股比例（針對 Top 觀察個股）

### 🤖 AI 深度分析（Gemini）
- 大盤環境研判（A 段）
- 外資力量深度剖析 Top 3（B 段）
- 明日操作清單，優先引用🔥可買訊號的系統計算價格（C 段）
- 風控與資金配置（D 段）
- 一句話摘要含可買訊號數量（E 段）
- 外資交叉比對亮點（F 段）

### 📧 Email 報表
- HTML 格式，包含：大盤速覽 → 🔥可買訊號專區 → 外資分點明細 → AI 分析
- 附件：Excel（Report / TopByBroker / 可買訊號 三個工作表）+ PDF

---

## 檔案結構
```
├── .github/workflows/
│   └── daily_report.yml        # GitHub Actions 排程（週一～週五 TW 18:00）
├── src/
│   ├── config.py               # 外資分點設定（九大外資代號 + 標籤）
│   ├── run_report.py           # 主報表（籌碼爬取 + 半量化訊號 + Excel/PDF）
│   ├── market_data.py          # 大盤籌碼抓取模組（三大法人/期貨/大戶）
│   ├── ai_analyze_gemini.py    # Gemini AI 分析（v12，含半量化訊號 context）
│   └── mailer.py               # Email 發送（HTML + 附件）
├── fonts/
│   └── NotoSansTC-Regular.ttf  # PDF 中文字型（必要）
├── requirements.txt
└── README.md
```

---

## GitHub Secrets 設定
Repo → Settings → Secrets and variables → Actions

| Secret | 說明 |
|--------|------|
| `SMTP_USER` | Gmail 帳號（例：yourname@gmail.com） |
| `SMTP_PASS` | Gmail **App Password**（16碼，非一般登入密碼） |
| `MAIL_FROM` | 寄件者（通常同 SMTP_USER） |
| `MAIL_TO` | 收件者（可用逗號分隔多人） |
| `MAIL_BCC` | BCC 名單（可選，逗號分隔） |
| `GEMINI_API_KEY` | Google AI Studio API Key |

> ⚠️ Gmail SMTP 需使用 **App Password**，一般密碼會收到 535 Authentication 錯誤。
> 開啟方式：Google 帳號 → 安全性 → 兩步驟驗證（須開啟）→ [應用程式密碼](https://myaccount.google.com/apppasswords)

---

## 環境變數（Workflow 內可調整）

| 變數 | 預設 | 說明 |
|------|------|------|
| `DAYS` | `5` | 外資分點查詢區間天數 |
| `TOP_N` | `10` | 每家外資顯示前 N 檔 |
| `MARKET_DATA` | `1` | 大盤籌碼抓取（0=停用） |
| `MARKET_HISTORY_DAYS` | `6` | 大盤歷史資料天數 |
| `GEMINI_MODEL` | `gemini-2.5-pro` | AI 模型（免費 25 RPD） |
| `GEMINI_ENABLE_FIXUP` | `1` | AI 二次品質修正（0=停用省 1 次 API） |
| `GEMINI_TEMPERATURE` | `0.3` | AI 輸出溫度（0=保守，1=創意） |
| `TECH_WORKERS` | `10` | yfinance 並行抓取 thread 數量 |
| `YF_RETRIES` | `3` | yfinance 失敗重試次數 |
| `YF_BACKOFF` | `1.5` | 重試指數退避底數（秒） |
| `MIN_VOL5` | `500` | 流動性門檻（5日均量最低張數） |
| `DEBUG_HTML` | `1` | 輸出爬蟲 HTML 到 output/debug/ |

---

## 本地測試

```bash
# 安裝依賴（curl_cffi 為 yfinance 1.2.x 必要套件）
pip install -r requirements.txt

# 步驟一：產生報表 + 大盤資料 + 半量化訊號
python src/run_report.py

# 步驟二：AI 分析（需設定 GEMINI_API_KEY）
export GEMINI_API_KEY="your_key_here"
python src/ai_analyze_gemini.py

# 步驟三：寄信（需設定 SMTP 環境變數）
export SMTP_USER="your@gmail.com"
export SMTP_PASS="xxxx xxxx xxxx xxxx"   # 16碼 App Password
export MAIL_FROM="your@gmail.com"
export MAIL_TO="recipient@example.com"
python src/mailer.py
```

---

## requirements.txt 必要套件

```
requests
pandas
openpyxl
beautifulsoup4
yfinance>=1.0.0
curl_cffi>=0.7.0     # yfinance 1.2.x 必要，缺少會導致技術面資料全為 0
reportlab
google-generativeai
```

---

## 常見問題

### Excel / PDF 技術面欄位全部為 0
原因：`yfinance 1.2.x` 改用 `curl_cffi` 做 TLS 指紋，傳入舊版 `requests.Session` 會靜默失敗。
解法：確認 `requirements.txt` 包含 `curl_cffi>=0.7.0` 並重新安裝。

### Gemini 429 Quota Exceeded
- `gemini-2.5-pro` 免費限額：5 RPM / 25 RPD
- 超過時自動 fallback → `gemini-2.5-flash` → `gemini-2.5-flash-lite`
- 若連 flash-lite 都 429，表示當日額度耗盡，等 PT 午夜重置

### Gmail SMTP 535 Authentication Error
使用的是一般 Gmail 密碼，需改用 16 碼 **App Password**。
設定路徑：[myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)

### AI 說「可買」但 Excel 顯示「觀察」
這是正常現象，兩套邏輯不同：
- **系統訊號（量化）**：六大條件 AND 嚴格判斷，任一不過就是「觀察」
- **AI 分析（質化）**：根據籌碼面、外資共識度、乖離率分析，是補充參考

---

## 版本歷史

| 版本 | 重點更新 |
|------|---------|
| v12 | 半量化交易訊號整合（進場/停損/停利/風報比）；curl_cffi Session；NaN 防禦；AI Prompt 對齊訊號欄位 |
| v11 | AI Prompt 升級為 A~F 六段專業報告；大盤月線判斷；多執行緒預載（ThreadPoolExecutor） |
| v9  | 大盤籌碼模組（三大法人/期貨/千張大戶）；Email 大盤速覽區塊 |
