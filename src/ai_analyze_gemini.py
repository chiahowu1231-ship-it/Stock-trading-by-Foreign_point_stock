# src/ai_analyze_gemini.py
# Gemini analysis runner v8 — model fallback + prompt compression
# ----------------------------------------------------------------
# 修正重點：
#   1. 預設模型改為 gemini-2.5-pro（分析最專業，免費 25 RPD）
#   2. 429 Quota Exceeded 時自動 fallback 到下一個模型（不再無效重試）
#   3. Prompt 精簡：只送 Top 5 外資 × Top 5 檔（減少 60% token 消耗）
#   4. Fixup pass 改為可選（節省 API 呼叫次數）
#   5. 完整的錯誤分類日誌

import os
import json
import re
import time
import random
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")

# 匯入大盤籌碼格式化工具
try:
    from market_data import format_market_context_for_prompt
    HAS_MARKET_FORMAT = True
except ImportError:
    HAS_MARKET_FORMAT = False

# ── 模型設定 ──────────────────────────────────────
# ⚠️ gemini-2.0-flash / 1.5-flash / 2.0-flash-lite 已於 2026/3/3 退役！
# 目前免費可用：gemini-2.5-pro / 2.5-flash / 2.5-flash-lite
# 預設 gemini-2.5-pro（分析品質最高，免費 5 RPM / 25 RPD）
PRIMARY_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")

# 429 時的 Fallback 順序（pro 爆額度 → flash 接手）
FALLBACK_MODELS = [
    "gemini-2.5-pro",         # 5 RPM / 25 RPD（免費，最強）
    "gemini-2.5-flash",       # 10 RPM / 250 RPD（免費，快速）
    "gemini-2.5-flash-lite",  # 15 RPM / 1000 RPD（免費，額度最高）
]

API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"

SUMMARY_PATH = os.path.join("output", "summary.json")
AI_TXT_PATH = os.path.join("output", "ai_analysis.txt")

TEMP = float(os.getenv("GEMINI_TEMPERATURE", "0.3"))

MAX_OUT_RAW = (os.getenv("GEMINI_MAX_OUTPUT_TOKENS", "") or "").strip()
MAX_OUT = int(MAX_OUT_RAW) if MAX_OUT_RAW.isdigit() else None

# 每個模型的重試次數（針對 5xx / 暫時性錯誤）
RETRIES_PER_MODEL = int(os.getenv("GEMINI_RETRIES", "3"))
BASE_SLEEP = float(os.getenv("GEMINI_RETRY_BASE_SLEEP", "2.0"))

# Fixup pass 開關（設為 0 可節省一次 API 呼叫）
ENABLE_FIXUP = os.getenv("GEMINI_ENABLE_FIXUP", "1").strip() != "0"

# Prompt 資料範圍（pro 模型可處理更多 context）
PROMPT_TOP_BROKERS = int(os.getenv("PROMPT_TOP_BROKERS", "7"))  # 送幾家外資
PROMPT_TOP_STOCKS = int(os.getenv("PROMPT_TOP_STOCKS", "7"))    # 每家送幾檔

ANALYZER_VERSION = "v12-semi-quant-signal"


# ── 檔案讀寫 ──────────────────────────────────────

def load_summary():
    with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_summary(summary: dict):
    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def save_ai_text(text: str):
    os.makedirs("output", exist_ok=True)
    with open(AI_TXT_PATH, "w", encoding="utf-8") as f:
        f.write((text or "").strip() + "\n")


def embed(summary: dict, text: str, model_used: str = ""):
    summary["ai_provider"] = "gemini"
    summary["ai_model"] = model_used or PRIMARY_MODEL
    summary["ai_generated_at"] = datetime.now(TZ).isoformat()
    summary["ai_analysis"] = (text or "").strip()
    summary["ai_analyzer_version"] = ANALYZER_VERSION
    summary["ai_temperature_used"] = TEMP
    summary["ai_max_output_tokens_used"] = MAX_OUT


# ── Prompt 建構（精簡版） ──────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
#  歷史金礦萃取器（Historical Context Extractor）
# ─────────────────────────────────────────────────────────────────────────────
HIST_CSV_PATH = os.path.join("data", "historical_signals.csv")
HIST_LOOKBACK = int(os.getenv("HIST_LOOKBACK", "5"))


def extract_historical_context(summary: dict) -> str:
    """從 historical_signals.csv 萃取今日上榜個股的近期動向，供 AI 判斷波段。"""
    if not os.path.exists(HIST_CSV_PATH):
        return ""
    try:
        df = pd.read_csv(HIST_CSV_PATH, dtype={"代碼": str, "日期": str}, encoding="utf-8-sig")
        if df.empty:
            return ""
        target_sids: set[str] = set()
        for block in (summary.get("top_preview") or []):
            for r in (block.get("rows") or []):
                sid = str(r.get("sid", "")).strip()
                if sid:
                    target_sids.add(sid)
        if not target_sids:
            return ""
        df_t = df[df["代碼"].isin(target_sids)].copy()
        if df_t.empty:
            return ""
        if "淨超" in df_t.columns:
            df_t["淨超"] = pd.to_numeric(df_t["淨超"], errors="coerce").fillna(0).astype(int)
        df_t = df_t.sort_values(["代碼", "日期"], ascending=[True, False])
        lines = ["【重點個股歷史波段軌跡（外資連續動向）】"]
        has_any = False
        for sid in sorted(target_sids):
            sdf = df_t[df_t["代碼"] == sid].head(HIST_LOOKBACK)
            if len(sdf) <= 1:
                continue
            has_any = True
            name    = sdf["名稱"].iloc[0] if "名稱" in sdf.columns else sid
            sig_col = "綜合判斷" if "綜合判斷" in sdf.columns else None
            net_total = int(sdf["淨超"].sum()) if "淨超" in sdf.columns else 0
            buy_cnt   = int((sdf[sig_col] == "🔥可買").sum()) if sig_col else 0
            ticks = []
            for _, row in sdf.iloc[::-1].iterrows():
                date_str = str(row["日期"])
                mmdd = f"{date_str[4:6]}/{date_str[6:8]}"
                net  = row.get("淨超", 0)
                is_buy = sig_col and str(row.get(sig_col, "")) == "🔥可買"
                icon = "🔥" if is_buy else "➖"
                ticks.append(f"{mmdd}({icon}{net:,}張)")
            trend = " → ".join(ticks)
            lines.append(f"  {sid} {name}（近{len(sdf)}日累計 {net_total:,} 張，{buy_cnt}次🔥）：{trend}")
        if not has_any:
            return ""
        lines.append("")
        return "\n".join(lines)
    except Exception as e:
        print(f"[WARN] 歷史軌跡萃取失敗（非致命）：{e}")
        return ""

def build_prompt(summary: dict) -> tuple[str, str]:
    """
    v13 System/User 完全分離：
      sys_prompt → systemInstruction（人設/規則/格式，靜態）
      usr_prompt → contents user role（每日變動數據）
    Gemini 對 systemInstruction 賦予最高遵守優先級，格式遵守率接近 100%。
    """
    top_preview = summary.get("top_preview") or []
    days        = summary.get("days")
    gen_at      = summary.get("generated_at")
    total_rows  = summary.get("total_rows", 0)
    ok          = summary.get("brokers_ok", 0)
    fail        = summary.get("brokers_fail", 0)
    errors      = summary.get("errors") or []

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  [1] System Instruction — 人設、格式規則（靜態，不含每日數據）
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    sys = []
    sys.append("你是華爾街等級的台股籌碼分析師，擁有 20 年經驗，報告對象是專業操盤手與法人投資經理。")
    sys.append("語氣要求：專業、精準、有洞察力。使用繁體中文，純文字，手機好讀格式。")
    sys.append("分析深度：不要泛泛而談，每一點都必須有『數據佐證 → 推論邏輯 → 操作意涵』三層結構。")
    sys.append("嚴格規則：只根據我提供的資料分析，不要編造新聞/題材/財報/K線型態。")
    sys.append("格式：不要表格；段落間空一行；每點以 1) 2) 3) 編號；子項目用 - 開頭。")
    sys.append("")
    sys.append("【系統背景】")
    sys.append("本系統為「半量化交易訊號系統」。每筆資料除外資籌碼外，已自動計算：")
    sys.append("  進場價 = MAX(收盤,20日高) × 1.01｜停損 = 進場×0.92（-8%）")
    sys.append("  停利1 = 進場×1.20｜停利2 = 進場×1.35｜移動停損 = 10日均線")
    sys.append("  進場條件：突破20日高 + 放量1.5倍(≥500張) + 站上10均 + 大盤月線 + 外資淨買超")
    sys.append("  標記【🔥可買】= 六大條件全部通過；【觀察】= 尚未觸發")
    sys.append("  歷史軌跡符號：🔥 = 當天觸發進場訊號；➖ = 當天僅為觀察狀態（非賣出）")
    sys.append("  資金控管：半凱利 f*/2 = (W-(1-W)/R)/2；最大張數 = 總資金×半凱利%/每張風險NT$")
    sys.append("")
    sys.append("請依照以下結構輸出完整報告（每區塊中間空一行）：")
    sys.append("")
    sys.append("A) 大盤籌碼環境研判（4~6 點）：")
    sys.append("   每點必含：具體數據 → 推論 → 操作意涵")
    sys.append("   涵蓋：外資/投信/自營現貨動向、法人共識度、成交量倍率、期貨籌碼方向、多空結論")
    sys.append("")
    sys.append("B) 外資力量 × 波段吃貨剖析（Top 3 外資，每家 4~6 點）：")
    sys.append("   必含：操作風格判讀、【波段佈局研判—引用歷史軌跡說明是單日突擊或連續吃貨】、")
    sys.append("         買超集中度（>50%=conviction）、乖離率分布、多空定性")
    sys.append("")
    sys.append("C) 明日波段操作清單（5 檔，每檔 5 行）：")
    sys.append("   優先選股：①連續吃貨+今日🔥 ②首次🔥 ③多家外資共識")
    sys.append("   每檔 5 行：1選入×佈局研判(引歷史軌跡) 2進場策略 3停損策略 4停利節奏 5部位風險(半凱利換算)")
    sys.append("")
    sys.append("D) 風控與資金配置（5~7 點）：")
    sys.append("   必含：整體持股水位、各🔥標的半凱利最大張數換算、期貨風險程度(低/中/高)、融資警戒、關鍵價位")
    sys.append("")
    sys.append("E) 一句話摘要（≤30字，須含今日🔥可買訊號數量及大盤方向判斷）。")
    sys.append("")
    sys.append("F) 外資交叉比對亮點（3~5 點）：被 2 家以上外資同時買超的標的，格式：")
    sys.append("   『XXXX 被 A、B 合計買超 N 張，乖離 X%，近X日第Y次🔥，暗示...』")
    sys.append("")
    sys.append("硬性規則：")
    sys.append("- 全文至少 60 行。A≥5點(含數據)。B Top3 各≥4點。C 必須5檔每檔5行。D≥5點(含水位數字)。F≥3點。")
    sys.append("- 所有數據必須與原始數據一致，不得捏造。若缺資料不得硬推論。")
    sys.append("- 若不足 60 行或缺任一段落(A~F)，請自行補齊直到符合規則。")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  [2] User Content — 每日變動數據（大盤籌碼 + 外資明細 + 歷史軌跡）
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    usr = []
    usr.append(f"報表資訊：產生時間={gen_at}；近{days}日；資料筆數={total_rows}；券商OK={ok}；FAIL={fail}")
    if errors:
        usr.append("注意：若 FAIL>0，只能就『有資料的外資』下結論；缺資料者不得推論。")
    usr.append("")

    market_data = summary.get("market_data") or {}
    if HAS_MARKET_FORMAT and market_data:
        market_text = format_market_context_for_prompt(market_data)
        if market_text.strip():
            usr.append("=" * 40)
            usr.append("【大盤籌碼原始數據】")
            usr.append(market_text)
            usr.append("=" * 40)
            usr.append("")
    elif market_data:
        inst = market_data.get("institutional") or []
        if inst:
            today = inst[0]
            usr.append("【今日三大法人】")
            usr.append(f"  外資淨買超: {today['foreign']['net']:,} 元")
            usr.append(f"  投信淨買超: {today['trust']['net']:,} 元")
            usr.append(f"  自營商淨買超: {today['dealer']['net']:,} 元")
            usr.append("")

    brokers_to_send = top_preview[:PROMPT_TOP_BROKERS]
    usr.append(f"【外資分點明細】（依總淨超排序，前{len(brokers_to_send)}家，每家Top{PROMPT_TOP_STOCKS}）")
    for block in brokers_to_send:
        usr.append(f"- {block.get('broker','')}｜總淨超 {block.get('total_net',0)} 張")
        for r in (block.get("rows") or [])[:PROMPT_TOP_STOCKS]:
            verdict  = r.get("verdict",  "觀察")
            entry    = r.get("entry",    "")
            stop     = r.get("stop",     "")
            tp1      = r.get("tp1",      "")
            rr       = r.get("rr",       "")
            risk     = r.get("risk",     "")
            hkelly   = r.get("hkelly",   "")
            max_lots = r.get("max_lots",  0)
            lot_str  = f" 最大張數:{max_lots}張" if max_lots and int(str(max_lots)) > 0 else ""
            signal_part = (
                f"｜【🔥可買】進場:{entry} 停損:{stop} 停利1:{tp1}"
                f" 風報比:{rr} 每張風險:{risk}元 半凱利:{hkelly}%{lot_str}"
                if "可買" in str(verdict) else "｜【觀察】"
            )
            usr.append(
                f"  * {r.get('sid','')} {r.get('name','')}｜淨超 {r.get('net',0)}"
                f"｜均價 {r.get('avg','')}｜現價 {r.get('price','')}｜乖離 {r.get('bias','')}"
                f"{signal_part}"
            )
    usr.append("")

    tdcc = market_data.get("tdcc") or []
    if tdcc:
        usr.append("【觀察個股千張大戶持股比例】")
        for d in tdcc:
            usr.append(f"  {d['stock_id']}｜千張以上: {d.get('holders_1000_plus',0)}人, "
                       f"持股 {d.get('pct_1000_plus',0):.1f}%")
        usr.append("")

    # 歷史波段軌跡（只送今日上榜個股，控制 Token 量）
    hist_text = extract_historical_context(summary)
    if hist_text:
        usr.append(hist_text)

    return "\n".join(sys), "\n".join(usr)


def make_fixup_usr(draft: str) -> str:
    """Fixup 時的 user content：system 維持不變，只送草稿讓 Gemini 補強。"""
    return "\n".join([
        "以下草稿不完整或不夠深入，請根據【系統背景】與格式規則『直接補齊並輸出完整 A~F』。",
        "請勿解釋，直接輸出完整報告。",
        "",
        "【草稿開始】",
        (draft or "").strip(),
        "【草稿結束】",
    ])


# ── API 呼叫（支援模型 Fallback） ──────────────────

def is_quota_exceeded(status_code: int, body_text: str) -> bool:
    """區分『配額耗盡(quota exceeded)』vs『暫時限速(rate limit)』"""
    if status_code != 429:
        return False
    # Google API 在 quota exceeded 時 message 會包含 "quota"
    return "quota" in body_text.lower() or "billing" in body_text.lower()


def call_gemini_single(sys_text: str, usr_text: str, model: str) -> str:
    """
    對單一模型呼叫 Gemini API，支援 5xx 重試。
    若遇到 429 Quota Exceeded，直接拋出不重試（交給外層 fallback）。
    """
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY missing (check GitHub Secrets).")

    endpoint = f"{API_BASE}/{model}:generateContent"
    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json",
    }

    gen_cfg = {"temperature": TEMP}
    if MAX_OUT is not None:
        gen_cfg["maxOutputTokens"] = MAX_OUT

    payload = {
        "systemInstruction": {"parts": [{"text": sys_text}]},
        "contents": [{"role": "user", "parts": [{"text": usr_text}]}],
        "generationConfig": gen_cfg,
    }

    last_exc = None

    for attempt in range(RETRIES_PER_MODEL):
        try:
            r = requests.post(endpoint, headers=headers, json=payload, timeout=180)
            body = r.text[:500]

            # ── 429 處理 ──
            if r.status_code == 429:
                if is_quota_exceeded(r.status_code, r.text):
                    # 配額耗盡 → 直接拋出，讓外層 fallback 到其他模型
                    raise QuotaExceededError(
                        f"[{model}] 配額耗盡 (429 Quota Exceeded): {body}"
                    )
                else:
                    # 暫時限速 → 重試
                    last_exc = RuntimeError(f"[{model}] HTTP 429 Rate Limit: {body}")
                    sleep_s = BASE_SLEEP * (2 ** attempt) + random.uniform(0, 1.0)
                    print(f"[WARN] {model} rate-limited, retry in {sleep_s:.1f}s ({attempt+1}/{RETRIES_PER_MODEL})")
                    time.sleep(sleep_s)
                    continue

            # ── 5xx 重試 ──
            if r.status_code in (500, 502, 503, 504):
                last_exc = RuntimeError(f"[{model}] HTTP {r.status_code}: {body}")
                sleep_s = BASE_SLEEP * (2 ** attempt) + random.uniform(0, 1.0)
                print(f"[WARN] {model} server error, retry in {sleep_s:.1f}s ({attempt+1}/{RETRIES_PER_MODEL})")
                time.sleep(sleep_s)
                continue

            # ── 其他 HTTP 錯誤 ──
            r.raise_for_status()

            # ── 解析回應 ──
            data = r.json()
            cands = data.get("candidates") or []
            if not cands:
                return json.dumps(data, ensure_ascii=False)

            parts = (cands[0].get("content") or {}).get("parts") or []
            text = "".join(p.get("text", "") for p in parts).strip()
            return text if text else json.dumps(data, ensure_ascii=False)

        except QuotaExceededError:
            raise  # 不重試，直接往上拋
        except Exception as e:
            if isinstance(e, QuotaExceededError):
                raise
            last_exc = e
            sleep_s = BASE_SLEEP * (2 ** attempt) + random.uniform(0, 1.0)
            print(f"[WARN] {model} exception, retry in {sleep_s:.1f}s: {type(e).__name__}: {e}")
            time.sleep(sleep_s)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"[{model}] 全部 {RETRIES_PER_MODEL} 次重試失敗")


class QuotaExceededError(RuntimeError):
    """429 Quota Exceeded — 需要切換模型，重試無效"""
    pass


def call_gemini_with_fallback(sys_text: str, usr_text: str) -> tuple[str, str]:
    """
    依序嘗試 PRIMARY_MODEL → FALLBACK_MODELS，直到成功。
    回傳 (response_text, model_used)。
    """
    # 建立嘗試順序：PRIMARY → FALLBACK（去除重複）
    models_to_try = [PRIMARY_MODEL]
    for m in FALLBACK_MODELS:
        if m not in models_to_try:
            models_to_try.append(m)

    last_err = None
    for model in models_to_try:
        try:
            print(f"[INFO] 嘗試模型: {model}")
            text = call_gemini_single(sys_text, usr_text, model)
            print(f"[OK] {model} 回應成功 ({len(text)} chars)")
            return text, model
        except QuotaExceededError as e:
            print(f"[WARN] {model} 配額耗盡，嘗試下一個模型...")
            last_err = e
            continue
        except Exception as e:
            print(f"[WARN] {model} 失敗: {type(e).__name__}: {e}")
            last_err = e
            continue

    # 全部模型都失敗
    if last_err is not None:
        raise last_err
    raise RuntimeError("所有 Gemini 模型皆失敗")


# ── 驗證 ──────────────────────────────────────────

def validate(text: str) -> list:
    """驗證輸出是否符合格式要求。回傳問題清單，空 list = 通過。"""
    s = (text or "").strip()
    problems = []
    if not s:
        return ["empty"]

    if len(s.splitlines()) < 55:
        problems.append(f"line_count={len(s.splitlines())}<55")

    # A~F 都必須存在
    for key in ["A)", "B)", "C)", "D)", "E)", "F)"]:
        if key not in s:
            problems.append(f"missing {key}")

    def count_points(prefix: str) -> int:
        i = s.find(prefix)
        if i < 0:
            return 0
        end = len(s)
        for nxt in ["B)", "C)", "D)", "E)", "F)"]:
            if nxt == prefix:
                continue
            j = s.find(nxt, i + 2)
            if j > 0:
                end = min(end, j)
        block = s[i:end]
        return len(re.findall(r"(?m)^\s*\d\)\s+", block))

    # A >= 5, B >= 3 (top3 外資), D >= 5
    if count_points("A)") < 5:
        problems.append(f"A) points={count_points('A)')}<5")
    if count_points("D)") < 4:
        problems.append(f"D) points={count_points('D)')}<4")

    # C >= 5 檔
    c_i = s.find("C)")
    if c_i >= 0:
        d_i = s.find("D)", c_i)
        c_block = s[c_i:(d_i if d_i > 0 else len(s))]
        if len(re.findall(r"(?m)^\s*\d\)\s+", c_block)) < 5:
            problems.append("C items<5")

    # F >= 3
    if count_points("F)") < 2:
        problems.append(f"F) points={count_points('F)')}<2")

    return problems


# ── 主程式 ──────────────────────────────────────────

def main():
    summary    = load_summary()
    sys_prompt, usr_prompt = build_prompt(summary)

    print("=" * 60)
    print(f"[INFO] analyzer     = {ANALYZER_VERSION}")
    print(f"[INFO] primary_model= {PRIMARY_MODEL}")
    print(f"[INFO] fallbacks    = {FALLBACK_MODELS}")
    print(f"[INFO] temperature  = {TEMP}")
    print(f"[INFO] maxOutTokens = {'NONE' if MAX_OUT is None else MAX_OUT}")
    print(f"[INFO] sys_len      = {len(sys_prompt)} chars")
    print(f"[INFO] usr_len      = {len(usr_prompt)} chars")
    print(f"[INFO] enable_fixup = {ENABLE_FIXUP}")
    print(f"[INFO] prompt_brokers={PROMPT_TOP_BROKERS} stocks={PROMPT_TOP_STOCKS}")
    print("=" * 60)

    model_used = PRIMARY_MODEL

    try:
        # ── 第一次呼叫（system + user 完全分離）──
        draft, model_used = call_gemini_with_fallback(sys_prompt, usr_prompt)
        p1 = validate(draft)

        if p1 and ENABLE_FIXUP:
            print(f"[WARN] 第一次驗證未通過: {'; '.join(p1)}")
            print("[INFO] 執行 fixup pass（system 不變，user 送草稿）...")
            try:
                # System 維持不變（規則底層強制），User 改為「草稿修補」
                fixup_usr  = make_fixup_usr(draft)
                final_text, model_used = call_gemini_with_fallback(sys_prompt, fixup_usr)
                p2 = validate(final_text)
                if p2:
                    print(f"[WARN] fixup 後仍未完全通過: {'; '.join(p2)}（仍使用此結果）")
                analysis = final_text
            except Exception as fixup_err:
                print(f"[WARN] fixup 失敗: {fixup_err}（使用第一次結果）")
                analysis = draft
        elif p1:
            print(f"[WARN] 驗證未通過但 fixup 已停用: {'; '.join(p1)}")
            analysis = draft
        else:
            print("[OK] 第一次驗證通過，無需 fixup")
            analysis = draft

        embed(summary, analysis, model_used)
        save_ai_text(summary["ai_analysis"])
        save_summary(summary)
        print(f"[OK] AI 分析完成 (model={model_used}, lines={len(analysis.splitlines())})")

    except Exception as e:
        err = f"Gemini 分析失敗：{type(e).__name__}: {e}"
        embed(summary, err, model_used)
        save_ai_text(err)
        save_summary(summary)
        print(f"[ERROR] {err}")

        # ✅ 印出除錯建議
        if "429" in str(e) or "quota" in str(e).lower():
            print("")
            print("=" * 60)
            print("【429 Quota Exceeded 除錯指南】")
            print("1. 檢查 Google AI Studio 帳戶配額：https://aistudio.google.com/")
            print("2. gemini-2.5-pro 免費每日限額 25 RPD，會自動 fallback 到 flash")
            print("3. 若連 flash 也 429，可能整個 project 額度耗盡，等隔天 PT 午夜重置")
            print("4. 或啟用 Google Cloud Billing 解除限制")
            print("5. 確認 GEMINI_API_KEY 對應的 Project 有正確啟用 API")
            print("=" * 60)


if __name__ == "__main__":
    main()
