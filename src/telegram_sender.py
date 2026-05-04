#!/usr/bin/env python3
# src/telegram_sender.py
# ─────────────────────────────────────────────────────────────────────────────
# 把 summary.json 的精華 + AI 分析摘要 + PDF 附件 推送到 Telegram。
#
# Telegram 訊息有 4096 字元限制，所以分兩段：
#   1. 摘要訊息（HTML 格式）：大盤、Top 3 外資、AI 摘要
#   2. PDF 附件（TAIWAN外資分點狙擊分析報告_YYYYMMDD.pdf）
#
# 環境變數：
#   TELEGRAM_BOT_TOKEN  — 必填
#   TELEGRAM_CHAT_ID    — 必填（個人 chat id 或 group chat id）
#   PAGES_URL           — 選填（會在訊息底部加上網頁版按鈕）
# ─────────────────────────────────────────────────────────────────────────────

import os
import json
import glob
import re
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

TZ = ZoneInfo("Asia/Taipei")
SUMMARY_PATH = os.path.join("output", "summary.json")
TG_API = "https://api.telegram.org"


# ─── 工具函式 ───────────────────────────────────────────
def _fb(n):
    """格式化金額：±X.XB / ±X.X億"""
    try:
        v = int(n)
        if abs(v) >= 1_000_000_000:
            return f"{v/1e9:+.2f}B"
        elif abs(v) >= 100_000_000:
            return f"{v/1e8:+.1f}億"
        elif abs(v) >= 10_000:
            return f"{v/1e4:+.0f}萬"
        return f"{v:+,}"
    except Exception:
        return str(n)


def _fmt_date(d: str) -> str:
    d = str(d).strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}/{d[4:6]}/{d[6:]}"
    return d


def _esc(s):
    """Telegram HTML mode 需要轉義 < > &"""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _arrow(v):
    try:
        n = float(str(v).replace(",", ""))
        if n > 0: return "🔴"
        elif n < 0: return "🟢"
    except Exception:
        pass
    return "⚪"


def _strip_md(text: str) -> str:
    """移除 markdown 粗體斜體標記，保留純文字"""
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    return text


# ─── 抽取 AI 各段落摘要（每段第一句）────────────────────
def _extract_ai_highlights(ai_text: str) -> dict:
    """
    從 AI 分析中抽取 A/B/C/D/F 各段的「結論句」
    回傳 {"A": "...", "C": "...", ...}
    """
    if not ai_text:
        return {}
    sections = {}
    cur_letter = None
    cur_lines = []

    for line in ai_text.split("\n"):
        s = line.strip()
        if not s:
            continue
        m = re.match(r'^\*{0,2}\s*([A-F])\s*[)）.：:、]\s*(.*)', s)
        if m:
            if cur_letter and cur_lines:
                sections[cur_letter] = cur_lines
            cur_letter = m.group(1)
            cur_lines = []
            continue
        if cur_letter:
            cur_lines.append(s)
    if cur_letter and cur_lines:
        sections[cur_letter] = cur_lines

    # 取每段前 5 行（足以涵蓋 1~2 個重點）
    return {k: "\n".join(v[:8]) for k, v in sections.items()}


# ─── 組合 Telegram 訊息（HTML 模式）────────────────────
def build_telegram_message(summary: dict) -> str:
    market   = summary.get("market_data") or {}
    inst     = market.get("institutional") or []
    futures  = market.get("futures") or []
    taiex    = market.get("taiex") or []
    top_p    = summary.get("top_preview") or []
    ai_text  = summary.get("ai_analysis") or ""

    gen_at   = summary.get("generated_at", "")[:16].replace("T", " ")
    rpt_date = (summary.get("generated_at", "") or "")[:10] or "—"
    ok       = summary.get("success", False)

    lines = []

    # ── Header ──
    status = "✅ 正常" if ok else "⚠️ 部分失敗"
    lines.append(f"<b>📈 TAIWAN 外資分點狙擊分析</b>")
    lines.append(f"📅 {rpt_date}　{status}")
    lines.append(f"⏰ {gen_at}")
    lines.append("")

    # ── 大盤指數 ──
    if taiex:
        t = taiex[0]
        chg   = t.get("change", 0)
        sign  = "+" if chg > 0 else ""
        chg_s = f"{sign}{chg:,.2f}" if isinstance(chg, float) else f"{sign}{chg}"
        emoji = "🔴" if chg > 0 else "🟢" if chg < 0 else "⚪"
        close = t.get("close", 0)
        amt   = t.get("amount_billion", 0)
        close_s = f"{close:,.2f}" if isinstance(close, float) else str(close)
        lines.append(f"<b>📊 大盤</b>")
        lines.append(f"  指數 <code>{close_s}</code> {emoji}{_esc(chg_s)}")
        lines.append(f"  量能 <code>{int(amt):,}</code> 億")
        lines.append("")

    # ── 三大法人（今日 + 6日累計）──
    if inst:
        today = inst[0]
        n     = min(len(inst), 6)
        cum_fg = sum(d["foreign"]["net"] for d in inst[:n])
        cum_tr = sum(d["trust"]["net"]   for d in inst[:n])
        cum_dl = sum(d["dealer"]["net"]  for d in inst[:n])

        lines.append(f"<b>🏦 三大法人（今日）</b>")
        lines.append(f"  外資 {_arrow(today['foreign']['net'])} <code>{_fb(today['foreign']['net'])}</code>")
        lines.append(f"  投信 {_arrow(today['trust']['net'])} <code>{_fb(today['trust']['net'])}</code>")
        lines.append(f"  自營 {_arrow(today['dealer']['net'])} <code>{_fb(today['dealer']['net'])}</code>")
        lines.append(f"<b>📊 {n} 日累計</b>")
        lines.append(f"  外資 <code>{_fb(cum_fg)}</code>　投信 <code>{_fb(cum_tr)}</code>　自營 <code>{_fb(cum_dl)}</code>")
        lines.append("")

    # ── 期貨（外資、投信、變化） ──
    if futures and len(futures) >= 2:
        f0, f1 = futures[0], futures[1]
        d_fg = f0.get("foreign_net_oi", 0) - f1.get("foreign_net_oi", 0)
        d_tr = f0.get("trust_net_oi", 0)   - f1.get("trust_net_oi", 0)
        lines.append(f"<b>📉 期貨淨部位（口）</b>")
        lines.append(f"  外資 <code>{f0.get('foreign_net_oi',0):+,}</code> ({d_fg:+,})")
        lines.append(f"  投信 <code>{f0.get('trust_net_oi',0):+,}</code> ({d_tr:+,})")
        lines.append("")

    # ── 外資 Top 3（含淨超）──
    if top_p:
        lines.append(f"<b>🏛 外資分點 Top 3</b>")
        medals = {0: "🥇", 1: "🥈", 2: "🥉"}
        for idx, b in enumerate(top_p[:3]):
            medal  = medals.get(idx, "  ")
            broker = _esc(b.get("broker", ""))
            net    = b.get("total_net", 0)
            lines.append(f"  {medal} {broker}  <code>{net:+,}</code> 張")

            # 列出每家 Top 2 標的
            for r in (b.get("rows") or [])[:2]:
                sid   = _esc(r.get("sid", ""))
                name  = _esc(r.get("name", ""))
                rnet  = r.get("net", 0)
                price = _esc(str(r.get("price", "")))
                bias  = _esc(str(r.get("bias", "")))
                lines.append(f"     · <b>{sid}</b> {name}　{rnet:+,}張  {price} ({bias})")
        lines.append("")

    # ── AI 分析摘要（A 段 + C 段觀察清單）──
    highlights = _extract_ai_highlights(ai_text)
    if highlights.get("A"):
        a_text = _strip_md(highlights["A"])[:500]
        lines.append(f"<b>🤖 AI 分析摘要｜A) 大盤研判</b>")
        lines.append(_esc(a_text))
        lines.append("")

    if highlights.get("C"):
        c_text = _strip_md(highlights["C"])[:600]
        lines.append(f"<b>🎯 C) 明日觀察清單</b>")
        lines.append(_esc(c_text))
        lines.append("")

    # ── 連結 ──
    pages_url = (os.environ.get("PAGES_URL") or "").strip()
    if pages_url:
        lines.append(f'<a href="{pages_url}">🌐 開啟網頁版完整報告</a>')

    msg = "\n".join(lines)

    # Telegram HTML 上限 4096 字元
    if len(msg) > 4000:
        msg = msg[:3950] + "\n\n…（內容過長已截斷，詳見網頁版/PDF）"

    return msg


# ─── Telegram API 呼叫 ──────────────────────────────────
def send_telegram_message(token: str, chat_id: str, text: str) -> bool:
    url = f"{TG_API}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code == 200:
            print("[telegram] ✓ 訊息已送出")
            return True
        print(f"[telegram] ✗ HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[telegram] ✗ 例外: {e}")
    return False


def send_telegram_document(token: str, chat_id: str, file_path: str, caption: str = "") -> bool:
    if not file_path or not os.path.exists(file_path):
        print(f"[telegram] ⚠ 檔案不存在: {file_path}")
        return False

    url = f"{TG_API}/bot{token}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            files = {"document": (os.path.basename(file_path), f)}
            data  = {"chat_id": chat_id, "caption": caption[:1000], "parse_mode": "HTML"}
            r = requests.post(url, data=data, files=files, timeout=60)
        if r.status_code == 200:
            print(f"[telegram] ✓ 附件已送出: {os.path.basename(file_path)}")
            return True
        print(f"[telegram] ✗ HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[telegram] ✗ 附件例外: {e}")
    return False


def pick_latest(pattern: str):
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


# ─── 主入口 ─────────────────────────────────────────────
def _parse_chat_ids(raw: str) -> list:
    """
    支援多個 chat_id，分隔符：逗號 / 分號 / 空白 / 換行
    範例：
      "123456,-1009876"   → ["123456", "-1009876"]
      "123456 -1009876"   → ["123456", "-1009876"]
      "123456;-1009876"   → ["123456", "-1009876"]
    """
    if not raw:
        return []
    # 用 regex 拆分多種分隔符
    parts = re.split(r"[,;\s]+", raw.strip())
    # 過濾空字串並去重（保留順序）
    seen = set()
    result = []
    for p in parts:
        p = p.strip()
        if p and p not in seen:
            seen.add(p)
            result.append(p)
    return result


def main():
    token       = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    raw_chat_id = (os.environ.get("TELEGRAM_CHAT_ID")   or "").strip()
    chat_ids    = _parse_chat_ids(raw_chat_id)

    if not token or not chat_ids:
        print("[telegram] 跳過：TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 未設定")
        return

    print(f"[telegram] 將推送至 {len(chat_ids)} 個對象：{chat_ids}")

    if not os.path.exists(SUMMARY_PATH):
        print(f"[telegram] ⚠ {SUMMARY_PATH} 不存在")
        return

    with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
        summary = json.load(f)

    # 1. 組合摘要文字
    msg = build_telegram_message(summary)
    print(f"[telegram] 訊息長度 {len(msg)} 字元")

    # 2. 找 PDF 分析報告
    pdf_path = pick_latest(os.path.join("output", "TAIWAN外資分點狙擊分析報告_*.pdf"))
    ymd      = datetime.now(TZ).strftime("%Y-%m-%d")

    # 3. 逐一推送（單一對象失敗不影響其他對象）
    success_count = 0
    fail_count    = 0
    for cid in chat_ids:
        print(f"[telegram] ──→ 推送至 {cid}")
        ok_msg = send_telegram_message(token, cid, msg)

        if pdf_path:
            send_telegram_document(
                token, cid, pdf_path,
                caption=f"📎 <b>{ymd} 完整分析報告</b>"
            )
        else:
            print(f"[telegram]   ⚠ 找不到分析 PDF，跳過附件")

        if ok_msg:
            success_count += 1
        else:
            fail_count += 1

    print(f"[telegram] 完成：成功 {success_count} 個 / 失敗 {fail_count} 個")

    # 全部失敗才回傳錯誤碼
    if success_count == 0 and fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
