# 外資分點狙擊分析（GitHub Actions）v9

## 功能
- 週一～週五 **台灣時間 14:30** 自動執行
- 固定抓 5 日區間（可用環境變數 DAYS 覆蓋）
- 產出 Excel/PDF：`output/IKE_Report_YYYYMMDD.xlsx` / `.pdf`
- **v9 新增：大盤籌碼資料抓取**
  - 三大法人買賣超（外資、投信、自營商）近 6 天趨勢
  - 大盤指數＋成交金額（自動判斷放量/縮量）
  - 融資融券變化（融資增減/餘額、融券增減/餘額）
  - 期貨籌碼（三大法人台指期淨未平倉）
  - 千張大戶持股比例（針對觀察清單個股）
- AI 分析（Gemini 2.0 Flash）含大盤環境判斷
- Email 報表寄出（含大盤籌碼摘要 + AI 分析）

## 檔案結構
```
├── .github/workflows/daily_report.yml   # GitHub Actions 排程
├── src/
│   ├── config.py               # 外資分點設定
│   ├── run_report.py           # 主報表產生（含呼叫 market_data）
│   ├── market_data.py          # v9 大盤籌碼抓取模組（新增）
│   ├── ai_analyze_gemini.py    # v9 AI 分析（含大盤 context）
│   └── mailer.py               # v9 Email（含大盤摘要區塊）
├── fonts/
│   └── NotoSansTC-Regular.ttf  # PDF 中文字型
├── requirements.txt
└── README.md
```

## GitHub Secrets 設定
Repo → Settings → Secrets and variables → Actions

| Secret | 說明 |
|--------|------|
| SMTP_USER | Gmail 帳號 |
| SMTP_PASS | Gmail App Password（16 碼） |
| MAIL_FROM | 寄件者（通常同 SMTP_USER） |
| MAIL_TO | 收件者 |
| MAIL_BCC | BCC 名單（逗號分隔，可選） |
| GEMINI_API_KEY | Google AI Studio API Key |

## 環境變數（Workflow 內可調整）
| 變數 | 預設 | 說明 |
|------|------|------|
| DAYS | 5 | 外資分點查詢天數 |
| TOP_N | 10 | 每家外資顯示前 N 檔 |
| MARKET_DATA | 1 | 大盤籌碼抓取（0=停用） |
| MARKET_HISTORY_DAYS | 6 | 大盤資料歷史天數 |
| GEMINI_MODEL | gemini-2.0-flash | AI 模型（付費帳戶可改 gemini-2.5-pro） |
| GEMINI_ENABLE_FIXUP | 1 | AI 二次修正（0=停用省 1 次 API） |

## 手動測試
```bash
pip install -r requirements.txt
python src/run_report.py          # 產生報表 + 大盤資料
python src/ai_analyze_gemini.py   # AI 分析（需設 GEMINI_API_KEY）
python src/mailer.py              # 寄信（需設 SMTP 環境變數）
```

## AI 分析 429 排查
- gemini-2.0-flash 免費額度 1500 RPM，通常不會爆
- 若 429，程式自動 fallback 到 gemini-1.5-flash
- 付費帳戶可改用 gemini-2.5-pro（品質更好但額度低）
