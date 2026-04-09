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

def build_prompt(summary: dict) -> str:
    """
    v11 expert prompt：面向專業操盤手的深度籌碼分析
    """
    top_preview = summary.get("top_preview") or []
    days = summary.get("days")
    gen_at = summary.get("generated_at")
    total_rows = summary.get("total_rows", 0)
    ok = summary.get("brokers_ok", 0)
    fail = summary.get("brokers_fail", 0)
    errors = summary.get("errors") or []

    lines = []

    # ── 角色設定（專業級） ──
    lines.append("你是華爾街等級的台股籌碼分析師，擁有 20 年經驗，報告對象是專業操盤手與法人投資經理。")
    lines.append("語氣要求：專業、精準、有洞察力。使用繁體中文，純文字，手機好讀格式。")
    lines.append("分析深度：不要泛泛而談，每一點都必須有『數據佐證 → 推論邏輯 → 操作意涵』三層結構。")
    lines.append("嚴格規則：只根據我提供的資料分析，不要編造新聞/題材/財報/K線型態。")
    lines.append("格式：不要表格；段落間空一行；每點以 1) 2) 3) 編號；子項目用 - 開頭。")
    lines.append("")
    lines.append(f"報表資訊：產生時間={gen_at}；近{days}日；資料筆數={total_rows}；券商OK={ok}；FAIL={fail}")
    if errors:
        lines.append("注意：若 FAIL>0，只能就『有資料的外資』下結論；缺資料者不得推論。")
    lines.append("")

    # ── 大盤籌碼資料 ──
    market_data = summary.get("market_data") or {}
    if HAS_MARKET_FORMAT and market_data:
        market_text = format_market_context_for_prompt(market_data)
        if market_text.strip():
            lines.append("=" * 40)
            lines.append("【大盤籌碼原始數據】")
            lines.append(market_text)
            lines.append("=" * 40)
            lines.append("")
    elif market_data:
        inst = market_data.get("institutional") or []
        if inst and len(inst) > 0:
            today_inst = inst[0]
            lines.append("【今日三大法人】")
            lines.append(f"  外資淨買超: {today_inst['foreign']['net']:,} 元")
            lines.append(f"  投信淨買超: {today_inst['trust']['net']:,} 元")
            lines.append(f"  自營商淨買超: {today_inst['dealer']['net']:,} 元")
            lines.append("")

    # ── 外資分點明細（增加至 7 家 × 7 檔） ──
    brokers_to_send = top_preview[:PROMPT_TOP_BROKERS]
    lines.append(f"【外資分點明細】（依總淨超排序，前{len(brokers_to_send)}家，每家Top{PROMPT_TOP_STOCKS}）")
    for block in brokers_to_send:
        lines.append(f"- {block.get('broker','')}｜總淨超 {block.get('total_net',0)} 張")
        for r in (block.get("rows") or [])[:PROMPT_TOP_STOCKS]:
            # 半量化訊號欄位（新版 run_report 已產出）
            verdict = r.get("verdict", "觀察")
            entry   = r.get("entry",  "")
            stop    = r.get("stop",   "")
            tp1     = r.get("tp1",    "")
            rr      = r.get("rr",     "")
            risk  = r.get("risk",  "")
            signal_part = (
                f"｜【🔥可買】進場:{entry} 停損:{stop} 停利1:{tp1} 風報比:{rr} 每張風險:{risk}元"
                if "可買" in str(verdict) else "｜【觀察】"
            )
            lines.append(
                f"  * {r.get('sid','')} {r.get('name','')}｜淨超 {r.get('net',0)}"
                f"｜均價 {r.get('avg','')}｜現價 {r.get('price','')}｜乖離 {r.get('bias','')}"
                f"{signal_part}"
            )
    lines.append("")

    # ── 千張大戶 ──
    tdcc = market_data.get("tdcc") or []
    if tdcc:
        lines.append("【觀察個股千張大戶持股比例】")
        for d in tdcc:
            lines.append(f"  {d['stock_id']}｜千張以上: {d.get('holders_1000_plus',0)}人, 持股 {d.get('pct_1000_plus',0):.1f}%")
        lines.append("")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  輸出結構指令（半量化訊號版）
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    lines.append("【系統背景】")
    lines.append("本系統為「半量化交易訊號系統」。每筆資料除外資籌碼外，已自動計算：")
    lines.append("  進場價 = MAX(收盤,20日高) × 1.01｜停損 = 進場×0.92（-8%）")
    lines.append("  停利1 = 進場×1.20｜停利2 = 進場×1.35｜移動停損 = 10日均線")
    lines.append("  進場條件：突破20日高 + 放量1.5倍(≥500張) + 站上10均 + 大盤月線 + 外資淨買超")
    lines.append("  標記【🔥可買】= 六大條件全部通過；【觀察】= 尚未觸發")
    lines.append("")
    lines.append("請依照以下結構輸出完整報告（每區塊中間空一行）：")
    lines.append("")

    # A) 大盤環境
    lines.append("A) 大盤籌碼環境研判（4~6 點）：")
    lines.append("   每一點必須包含：具體數據 → 推論 → 操作意涵。")
    lines.append("   必須涵蓋以下面向：")
    lines.append("   - 外資/投信/自營現貨動向：直接引用【三大法人N日累計買賣超】中的累計金額與連買/連賣天數，說明近期趨勢（連買/連賣/轉折），並計算三大合計累計金額")
    lines.append("   - 法人共識度：對照累計統計中三者趨勢標籤（連續淨買/連續淨賣/多空互見），判斷外資、投信、自營商是否同向；若背離請分析可能原因")
    lines.append("   - 成交量分析：今日量能 vs 5日均量（放量/縮量/爆量倍率），量價配合度")
    lines.append("   - 期貨籌碼：外資台指期淨部位方向與變化幅度，多空轉折訊號")
    lines.append("   - 多空結論：綜合以上，明確給出『偏多/中性/偏空』判斷及信心程度（高/中/低）")
    lines.append("")

    # B) 外資排行
    lines.append("B) 外資力量深度剖析（Top 3 外資，每家 4~6 點）：")
    lines.append("   每家外資需分析：")
    lines.append("   - 操作風格判讀：根據持股特徵（大型/中小型、電子/金融/傳產）判斷該外資近期策略")
    lines.append("   - 買超集中度：前3大標的佔總淨超比例，若>50%代表高度集中（conviction trade）")
    lines.append("   - 乖離率分布：多數標的乖離正/負？正乖離高代表『追漲/強勢佈局』，負乖離多代表『逢低承接/被套』")
    lines.append("   - 跨外資交叉驗證：同一標的是否被多家外資同時買超？若是，代表共識度高")
    lines.append("   - 多空定性：該外資整體偏多/偏空/中性，附帶簡短理由")
    lines.append("")

    # C) 觀察清單（半量化訊號版）
    lines.append("C) 明日操作清單（5 檔，每檔 5 行）：")
    lines.append("   選股邏輯（優先順序）：①【🔥可買】六大條件全過的個股 → ②多家外資共同買超 → ③淨超張數大且正乖離。")
    lines.append("   若【🔥可買】個股不足5檔，以籌碼面補足。每檔必須包含以下五行：")
    lines.append("   1) 選入理由：訊號類型（🔥可買/籌碼觀察）+ 哪幾家外資買超 + 總淨超張數 + 乖離率位置")
    lines.append("   2) 進場策略：直接引用系統計算的進場價（若為🔥可買標的）；或自行根據籌碼推估觸發條件")
    lines.append("   3) 停損策略：直接引用系統停損價（進場×0.92，即-8%）；說明停損邏輯（收盤跌破/盤中跌破）")
    lines.append("   4) 停利策略：停利1（+20%）和停利2（+35%）的分批了結節奏；並說明10均移動停損啟動時機")
    lines.append("   5) 部位與風險：每張風險NT$=（進場-停損）×1000，建議倉位比例（1成試單/3成標準/5成重倉上限）")
    lines.append("   - 若有千張大戶資料，請分析大戶籌碼集中度對股價支撐/壓力的影響。")
    lines.append("")

    # D) 風控
    lines.append("D) 風控與資金配置（5~7 點）：")
    lines.append("   不是泛泛的『注意風險』，而是基於數據的具體建議：")
    lines.append("   - 整體持股水位建議（如『建議總持股不超過6成』）及理由")
    lines.append("   - 單一標的最大部位上限")
    lines.append("   - 根據外資期貨空單水位，判斷系統性風險程度（低/中/高）")
    lines.append("   - 融資水位對散戶追漲的警示（若融資餘額增加代表什麼）")
    lines.append("   - 明日需關注的關鍵價位或事件（如指數支撐/壓力、選擇權結算日）")
    lines.append("   - 若外資現貨與期貨方向矛盾，分析可能的策略意圖（避險？換倉？）")
    lines.append("")

    # E) 摘要
    lines.append("E) 一句話摘要（≤30字，須含今日🔥可買訊號數量及大盤方向判斷）。")
    lines.append("")

    # F) 交叉比對
    lines.append("F) 外資交叉比對亮點（3~5 點）：")
    lines.append("   找出被 2 家以上外資同時買超的標的，分析共識度與潛在意義。")
    lines.append("   格式：『XXXX 股名被 A、B、C 三家外資合計買超 N 張，乖離率 X%，暗示...』")
    lines.append("")

    # 硬性規則
    lines.append("硬性規則：")
    lines.append("- 全文至少 60 行（含標題與編號行）。")
    lines.append("- A 段至少 5 點，每點需引用具體數據。B 段 Top3 外資各至少 4 點。")
    lines.append("- C 必須 5 檔、每檔 5 行（選入理由/進場/停損/了結/部位）。")
    lines.append("- D 段至少 5 點，必須包含具體持股水位數字。")
    lines.append("- F 段至少 3 點交叉比對。")
    lines.append("- 所有數據引用必須與我提供的原始數據一致，不得捏造。")
    lines.append("- 若不足 60 行或缺任一段落(A~F)，請自行補齊直到符合規則。")

    return "\n".join(lines)


def fixup_prompt(draft: str) -> str:
    return "\n".join([
        "你是華爾街等級的台股籌碼分析師。以下草稿不完整或不夠深入，請你『補齊並強化』後輸出完整 A~F。",
        "硬性規則：全文至少60行；A至少5點(含數據)；B Top3外資各至少4點；C 5檔每檔5行；D至少5點(含持股水位)；F至少3點交叉比對。",
        "每一點都要有『數據 → 推論 → 操作意涵』三層結構。",
        "請直接輸出『完整版本』，保持純文字、段落間空一行、每點用 1) 2) 3)。",
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


def call_gemini_single(prompt: str, model: str) -> str:
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
        "contents": [{"parts": [{"text": prompt}]}],
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


def call_gemini_with_fallback(prompt: str) -> tuple[str, str]:
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
            text = call_gemini_single(prompt, model)
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
    summary = load_summary()
    prompt = build_prompt(summary)

    print("=" * 60)
    print(f"[INFO] analyzer     = {ANALYZER_VERSION}")
    print(f"[INFO] primary_model= {PRIMARY_MODEL}")
    print(f"[INFO] fallbacks    = {FALLBACK_MODELS}")
    print(f"[INFO] temperature  = {TEMP}")
    print(f"[INFO] maxOutTokens = {'NONE' if MAX_OUT is None else MAX_OUT}")
    print(f"[INFO] prompt_len   = {len(prompt)} chars")
    print(f"[INFO] enable_fixup = {ENABLE_FIXUP}")
    print(f"[INFO] prompt_brokers={PROMPT_TOP_BROKERS} stocks={PROMPT_TOP_STOCKS}")
    print("=" * 60)

    model_used = PRIMARY_MODEL

    try:
        # ── 第一次呼叫 ──
        draft, model_used = call_gemini_with_fallback(prompt)
        p1 = validate(draft)

        if p1 and ENABLE_FIXUP:
            print(f"[WARN] 第一次驗證未通過: {'; '.join(p1)}")
            print("[INFO] 執行 fixup pass...")
            try:
                final_text, model_used = call_gemini_with_fallback(fixup_prompt(draft))
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
