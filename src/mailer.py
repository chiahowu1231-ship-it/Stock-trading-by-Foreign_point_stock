# src/mailer.py
# v11 — 全 HTML 專業版 Email（修復 taiex/margin/tdcc 缺失渲染，統一視覺風格）
# ─────────────────────────────────────────────────────────────────────────────
# 修正清單：
#   1. 新增 大盤指數(TAIEX) HTML 表格渲染
#   2. 新增 融資融券 HTML 表格渲染
#   3. 新增 千張大戶(TDCC) HTML 卡片渲染
#   4. 修正期貨表格（補上 header row）
#   5. AI 分析：支援 **bold** markdown、E) 一句話摘要獨立卡片
#   6. 統一配色系統、版面結構更清晰

import os
import json
import glob
import re
import smtplib
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")

# Email 表格顯示天數：預設 3 日，手機不會太長
# 在 daily_report.yml env 加 EMAIL_TABLE_DAYS: "6" 可恢復完整顯示
EMAIL_TABLE_DAYS = int(os.getenv("EMAIL_TABLE_DAYS", "3"))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  通用工具函式
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_summary():
    path = os.path.join("output", "summary.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "generated_at": datetime.now(TZ).isoformat(),
        "timezone": "Asia/Taipei",
        "days": int(os.getenv("DAYS", "5")),
        "success": False,
        "errors": ["summary.json 不存在"],
        "total_rows": 0, "brokers_total": 0, "brokers_ok": 0, "brokers_fail": 0,
        "top_preview": [], "ai_analysis": "",
    }


def pick_latest(pattern: str):
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def _fi(n):
    """格式化整數（千分位）"""
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _fb(n):
    """格式化帶正負號、億/萬換算的買賣超數字"""
    try:
        v = int(n)
        if abs(v) >= 1_000_000_000:
            return f"{v / 1e9:+.2f}B"
        elif abs(v) >= 1_0000_0000:
            return f"{v / 1e8:+.1f}億"
        elif abs(v) >= 1_0000:
            return f"{v / 1e4:+.0f}萬"
        return f"{v:+,}"
    except Exception:
        return str(n)


def _fbi(n):
    """格式化不帶億換算的整數（口數/張數，千分位）"""
    try:
        v = int(n)
        if v > 0:
            return f"+{v:,}"
        return f"{v:,}"
    except Exception:
        return str(n)


def _color(val, zero_color="#555"):
    """根據數值正負回傳台股慣例顏色（正=紅, 負=綠）"""
    try:
        raw = str(val).replace(",", "").replace("%", "").replace("億", "").replace("萬", "").replace("+", "").strip()
        v = float(raw)
        if v > 0:
            return "#C0392B"  # 台股紅
        elif v < 0:
            return "#27AE60"  # 台股綠
    except Exception:
        pass
    return zero_color


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _md_inline(text: str) -> str:
    """將 **bold** 和 *italic* markdown 轉為 HTML（用於 AI 輸出後處理）"""
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = re.sub(r'\*([^*]+?)\*', r'<em>\1</em>', text)
    return text


def _style_keywords(text: str) -> str:
    """高亮操作關鍵字（在 _esc 後調用）"""
    kws = {
        "進場": "#2E86C1", "停損": "#E74C3C", "了結": "#8E44AD",
        "突破": "#2980B9", "回測": "#E67E22", "不跌破": "#27AE60",
        "放量": "#C0392B", "縮量": "#27AE60", "偏多": "#C0392B",
        "偏空": "#27AE60", "觀察": "#7F8C8D", "風險": "#E74C3C",
        "連買": "#C0392B", "連賣": "#27AE60", "中性": "#7F8C8D",
        "高度集中": "#8E44AD", "強勢佈局": "#C0392B", "逢低承接": "#2980B9",
        "共識度高": "#C0392B", "被套": "#E74C3C",
    }
    for kw, c in kws.items():
        text = text.replace(kw, f'<b style="color:{c};">{kw}</b>')
    return text


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  共用 HTML 元件（專業版）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SECTION_COLORS = {
    "blue":   {"bg": "#1A5276", "hdr": "#1F618D", "light": "#EBF5FB", "border": "#AED6F1"},
    "navy":   {"bg": "#1A2A3A", "hdr": "#2C3E50", "light": "#EAF0F6", "border": "#AEB6BF"},
    "gold":   {"bg": "#7D6608", "hdr": "#9A7D0A", "light": "#FEF9E7", "border": "#F9E79F"},
    "green":  {"bg": "#1A6B3A", "hdr": "#1E8449", "light": "#EAFAF1", "border": "#A9DFBF"},
    "red":    {"bg": "#7B241C", "hdr": "#922B21", "light": "#FDEDEC", "border": "#F5B7B1"},
    "purple": {"bg": "#5B2C6F", "hdr": "#6C3483", "light": "#F4ECF7", "border": "#D7BDE2"},
    "teal":   {"bg": "#0E6655", "hdr": "#148F77", "light": "#E8F8F5", "border": "#A2D9CE"},
    "gray":   {"bg": "#424949", "hdr": "#515A5A", "light": "#F2F3F4", "border": "#BFC9CA"},
}


def _fmt_date(d: str) -> str:
    """20260330 → 2026/03/30"""
    d = str(d).strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}/{d[4:6]}/{d[6:]}"
    return d


def _arrow(v) -> str:
    """回傳趨勢箭頭 HTML（▲紅 / ▼綠）"""
    try:
        n = float(str(v).replace(",", ""))
        if n > 0:
            return '<span style="color:#C0392B;font-size:10px;"> ▲</span>'
        elif n < 0:
            return '<span style="color:#27AE60;font-size:10px;"> ▼</span>'
    except Exception:
        pass
    return ""


def _badge(text: str, bg: str, color: str = "#fff", px: int = 7) -> str:
    return (
        f'<span style="display:inline-block;padding:1px {px}px;background:{bg};'
        f'color:{color};border-radius:3px;font-size:10.5px;font-weight:700;'
        f'vertical-align:middle;white-space:nowrap;">{text}</span>'
    )


def _sec_hdr(icon: str, title: str, color_key: str = "blue", subtitle: str = "") -> str:
    c = SECTION_COLORS.get(color_key, SECTION_COLORS["blue"])
    sub_html = (
        f'<span style="font-size:11px;color:rgba(255,255,255,.7);'
        f'margin-left:10px;font-weight:400;">{_esc(subtitle)}</span>'
        if subtitle else ""
    )
    return (
        f'<div style="margin:22px 0 0;padding:10px 16px;'
        f'background:{c["bg"]};'
        f'border-radius:5px 5px 0 0;border-left:5px solid {c["hdr"]};">'
        f'<span style="font-size:13.5px;font-weight:700;color:#fff;letter-spacing:.5px;">'
        f'{icon}&nbsp; {_esc(title)}</span>{sub_html}</div>'
    )


def _table_open(col_styles: list = None) -> str:
    """開啟表格，外層加 overflow-x:auto 讓手機端可橫向滑動。"""
    cols = ""
    if col_styles:
        cols = "<colgroup>" + "".join(
            f'<col style="width:{w};{s}">' for w, s in col_styles
        ) + "</colgroup>"
    return (
        '<div style="width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;">'
        f'<table style="width:100%;border-collapse:collapse;font-size:12.5px;'
        f'border:1px solid #D5D8DC;border-top:none;margin-bottom:0;">{cols}'
    )


TABLE_CLOSE = "</table></div>"


def _th_row(*cols, bg: str = "#2C3E50", color: str = "#FFFFFF") -> str:
    """表頭列 — cols = (label, align) tuples"""
    cells = "".join(
        f'<th style="padding:7px 11px;text-align:{align};background:{bg};'
        f'color:{color};font-size:11.5px;font-weight:600;'
        f'white-space:nowrap;border-right:1px solid rgba(255,255,255,.12);">'
        f'{label}</th>'
        for label, align in cols
    )
    return f"<tr>{cells}</tr>"


def _td(text, align="left", bold=False, color="", extra="", border=True) -> str:
    b  = "font-weight:700;" if bold else ""
    c  = f"color:{color};" if color else ""
    br = "border-right:1px solid #EAECEE;" if border else ""
    bt = "border-top:1px solid #EAECEE;"
    return (
        f'<td style="padding:6px 11px;text-align:{align};color:#1A1A1A;{bt}{br}{b}{c}{extra}">'
        f'{text}</td>'
    )


def _tr(*cells, bg="#FFF", today=False) -> str:
    if today:
        bg = "#FFFDE7"  # 今日列淡黃高亮
    return f'<tr style="background:{bg};">{"".join(cells)}</tr>'


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  大盤資料 HTML 渲染（專業版）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_taiex(taiex: list) -> str:
    if not taiex:
        return ""
    # 計算5日均量（用第2~6筆）供量能比較
    amts = [d.get("amount_billion", 0) for d in taiex]
    avg5 = sum(amts[1:6]) / max(len(amts[1:6]), 1) if len(amts) > 1 else 0

    hdr = _sec_hdr("📊", "大盤指數 ＋ 成交量", "navy", "近 6 日")
    tbl = _table_open([("100px",""), ("90px",""), ("90px",""), ("100px",""), ("90px","")])
    tbl += _th_row(
        ("日期", "left"), ("收盤指數", "right"), ("漲跌點", "right"),
        ("成交金額", "right"), ("量比(5日均)", "right"),
        bg="#2C3E50",
    )
    for i, d in enumerate(taiex[:6]):
        chg   = d.get("change", 0)
        close = d.get("close", 0)
        amt   = d.get("amount_billion", 0)
        chg_c = _color(chg)
        sign  = "+" if chg > 0 else ""
        chg_s = f"{sign}{chg:,.2f}" if isinstance(chg, float) else f"{sign}{chg}"

        # 量比
        ratio     = amt / avg5 if avg5 > 0 else 0
        ratio_s   = f"{ratio:.2f}x" if avg5 > 0 else "—"
        ratio_c   = "#C0392B" if ratio > 1.2 else "#27AE60" if ratio < 0.8 else "#555"
        ratio_txt = (
            f'<span style="color:{ratio_c};font-weight:{"700" if i==0 else "400"};">'
            f'{ratio_s}</span>'
            + (_badge("放量", "#E74C3C") if ratio > 1.2 and i == 0 else
               _badge("縮量", "#27AE60") if ratio < 0.8 and i == 0 else "")
        )

        # 今日標籤
        date_html = _fmt_date(d.get("date", ""))
        if i == 0:
            date_html += "&nbsp;" + _badge("最新", "#1A5276")

        close_s = f"{close:,.2f}" if isinstance(close, float) else str(close)
        bg      = "#FFFDE7" if i == 0 else ("#F8F9FA" if i % 2 else "#FFF")

        tbl += _tr(
            _td(date_html, bold=(i == 0)),
            _td(f'<span style="font-weight:{"700" if i==0 else "600"};color:#333;">{close_s}</span>', "right"),
            _td(f'<span style="color:{chg_c};font-weight:{"700" if i==0 else "500"};">{chg_s}</span>{_arrow(chg)}', "right"),
            _td(f'<span style="{"font-weight:700;" if i==0 else ""}">{int(amt):,} 億</span>', "right"),
            _td(ratio_txt, "right"),
            bg=bg,
        )
    tbl += TABLE_CLOSE
    return hdr + tbl


def _validate_institutional(inst: list) -> list:
    """
    驗證三大法人資料品質，回傳警示訊息清單（空清單=正常）。
    用於在 Email 頂部顯示黃色警示條，讓你一眼識別資料異常。
    """
    if not inst:
        return ["三大法人資料為空，可能 TWSE API 查詢失敗或今日非交易日"]

    # 日期重複（最嚴重：dayDate 失效時6天全部相同日期）
    dates = [x.get("date") for x in inst if x.get("date")]
    if len(dates) != len(set(dates)):
        return ["⚠️ 三大法人歷史日期有重複，dayDate 查詢參數可能失效，數值不可信"]

    # 數值完全相同（正常市場幾乎不可能出現）
    if len(inst) > 1:
        nets = tuple(
            (x.get("foreign", {}).get("net", 0),
             x.get("trust",   {}).get("net", 0),
             x.get("dealer",  {}).get("net", 0))
            for x in inst
        )
        if len(set(nets)) == 1:
            return ["⚠️ 近多日三大法人數值完全相同，疑似 API 持續回傳最新單日資料"]

    return []


def _render_kpi_inst(inst: list) -> str:
    """
    手機友善 KPI 摘要卡片（4格）：只取最新一日三大法人數字。
    置頂顯示，讓手機使用者不需滾動表格就能看到今日重點。
    """
    if not inst:
        return ""
    d0  = inst[0]
    fg  = (d0.get("foreign") or {}).get("net", 0)
    tr  = (d0.get("trust")   or {}).get("net", 0)
    dl  = (d0.get("dealer")  or {}).get("net", 0)
    tot = d0.get("total_net", fg + tr + dl)
    date_lbl = _fmt_date(d0.get("date", ""))

    def _card(title, val):
        c = _color(val)
        arrow = "▲" if val > 0 else ("▼" if val < 0 else "")
        return (
            f'<td style="padding:8px 6px;text-align:center;border:1px solid #D5D8DC;'
            f'border-radius:6px;background:#F8FAFC;">'
            f'<div style="font-size:11px;color:#6B7280;">{_esc(title)}</div>'
            f'<div style="font-size:15px;font-weight:800;color:{c};">'
            f'{arrow}{_fb(val)}</div></td>'
        )

    return (
        f'<div style="margin:10px 0 4px;font-size:13px;font-weight:700;color:#1F2D3D;">'
        f'📌 今日三大法人摘要（{date_lbl}）</div>'
        '<div style="font-size:11px;color:#888;margin-bottom:6px;">'
        '手機版先看重點摘要，完整多日明細在下方表格，詳細報告請開附件 PDF。</div>'
        '<table style="width:100%;border-collapse:separate;border-spacing:4px;">'
        f'<tr>{_card("外資",fg)}{_card("投信",tr)}{_card("自營",dl)}{_card("合計",tot)}</tr>'
        '</table>'
    )


def _render_institutional(inst: list) -> str:
    if not inst:
        return ""
    n = len(inst[:6])
    # 累計加總
    cum_fg  = sum(d["foreign"]["net"]   for d in inst[:EMAIL_TABLE_DAYS])
    cum_tr  = sum(d["trust"]["net"]     for d in inst[:EMAIL_TABLE_DAYS])
    cum_dl  = sum(d["dealer"]["net"]    for d in inst[:EMAIL_TABLE_DAYS])
    cum_tot = sum(d.get("total_net", 0) for d in inst[:EMAIL_TABLE_DAYS])
    date_start = _fmt_date(inst[min(n-1, 5)]["date"])
    date_end   = _fmt_date(inst[0]["date"])

    def _streak_badge(key):
        vals  = [d[key]["net"] for d in inst[:6]]
        b_cnt = sum(1 for v in vals if v > 0)
        s_cnt = sum(1 for v in vals if v < 0)
        if b_cnt > s_cnt:
            return _badge(f"買超{b_cnt}日", "#C0392B")
        elif s_cnt > b_cnt:
            return _badge(f"賣超{s_cnt}日", "#27AE60")
        else:
            return _badge("多空互見", "#888")

    hdr = _sec_hdr("🏦", "三大法人買賣超（元）", "blue", f"近 {n} 日　{date_start} ～ {date_end}")
    tbl = _table_open([("100px",""), ("110px",""), ("110px",""), ("110px",""), ("110px","")])
    tbl += _th_row(
        ("日期", "left"), ("外資買賣超", "right"), ("投信買賣超", "right"),
        ("自營買賣超", "right"), ("三大合計", "right"),
        bg="#1F618D",
    )
    for i, d in enumerate(inst[:EMAIL_TABLE_DAYS]):
        fg  = d["foreign"]["net"]
        tr  = d["trust"]["net"]
        dl  = d["dealer"]["net"]
        tot = d.get("total_net", fg + tr + dl)
        bg  = "#FFFDE7" if i == 0 else ("#F8F9FA" if i % 2 else "#FFF")

        def _mc(v, is_today):
            col  = _color(v)
            bld  = "font-weight:700;" if is_today else "font-weight:500;"
            return _td(
                f'<span style="color:{col};{bld}">{_fb(v)}</span>{_arrow(v) if is_today else ""}',
                "right"
            )

        date_html = _fmt_date(d["date"]) + ("&nbsp;" + _badge("最新", "#1A5276") if i == 0 else "")
        tbl += _tr(
            _td(date_html, bold=(i == 0)),
            _mc(fg, i == 0), _mc(tr, i == 0), _mc(dl, i == 0),
            _td(
                f'<span style="color:{_color(tot)};font-weight:{"800" if i==0 else "600"};">'
                f'{_fb(tot)}</span>{_arrow(tot) if i==0 else ""}',
                "right", bold=(i == 0)
            ),
            bg=bg,
        )

    # ── 累計統計列（分隔線 + 深色底） ──
    def _cum_cell(v):
        col = _color(v)
        return _td(
            f'<span style="color:{col};font-weight:800;font-size:12px;">{_fb(v)}</span>',
            "right"
        )

    tbl += (
        f'<tr style="background:#1F618D;">' +
        _td(
            f'<span style="color:#fff;font-weight:700;font-size:11.5px;">{n}日累計</span>',
            bold=True, color="#fff", extra="border-top:2px solid #1A5276;"
        ) +
        _cum_cell(cum_fg) + _cum_cell(cum_tr) + _cum_cell(cum_dl) +
        _td(
            f'<span style="color:{"#FFCDD2" if cum_tot>0 else "#C8E6C9"};font-weight:800;font-size:12.5px;">{_fb(cum_tot)}</span>',
            "right", extra="border-top:2px solid #1A5276;"
        ) +
        '</tr>'
    )
    # 趨勢說明列
    fg_badge  = _streak_badge("foreign")
    tr_badge  = _streak_badge("trust")
    dl_badge  = _streak_badge("dealer")
    tbl += (
        f'<tr><td colspan="5" style="padding:5px 12px;background:#EBF5FB;'
        f'border-top:1px solid #AED6F1;font-size:11.5px;color:#1A2A3A;">'
        f'外資&nbsp;{fg_badge}&nbsp;&nbsp;投信&nbsp;{tr_badge}&nbsp;&nbsp;自營&nbsp;{dl_badge}'
        f'</td></tr>'
    )
    tbl += TABLE_CLOSE
    return hdr + tbl


def _render_margin(margin: list) -> str:
    if not margin:
        return ""
    hdr = _sec_hdr("💳", "融資融券（張）", "gray", "近 6 日｜散戶籌碼動向")
    tbl = _table_open([
        ("100px",""), ("90px",""), ("110px",""),
        ("90px",""), ("110px",""), ("80px",""),
    ])
    tbl += _th_row(
        ("日期", "left"),
        ("融資增減", "right"), ("融資餘額", "right"),
        ("融券增減", "right"), ("融券餘額", "right"),
        ("券資比", "right"),
        bg="#515A5A",
    )
    for i, d in enumerate(margin[:EMAIL_TABLE_DAYS]):
        mc = d.get("margin_change", 0)
        mb = d.get("margin_balance", 0)
        sc = d.get("short_change", 0)
        sb = d.get("short_balance", 0)
        # 券資比（融券餘額 / 融資餘額）
        sr_ratio = (sb / mb * 100) if mb > 0 else 0
        sr_c     = "#C0392B" if sr_ratio > 15 else "#27AE60" if sr_ratio < 5 else "#555"

        mc_c = "#C0392B" if mc > 0 else "#27AE60" if mc < 0 else "#555"
        sc_c = "#27AE60" if sc > 0 else "#C0392B" if sc < 0 else "#555"
        bg   = "#FFFDE7" if i == 0 else ("#F8F9FA" if i % 2 else "#FFF")

        is_t = (i == 0)
        date_html = _fmt_date(d.get("date","")) + ("&nbsp;" + _badge("最新","#424949") if is_t else "")

        tbl += _tr(
            _td(date_html, bold=is_t),
            _td(f'<span style="color:{mc_c};font-weight:{"700" if is_t else "500"};">'
                f'{_fbi(mc)}</span>{_arrow(mc) if is_t else ""}', "right"),
            _td(f'<span style="{"font-weight:600;" if is_t else "color:#555;"}">{_fi(mb)}</span>', "right"),
            _td(f'<span style="color:{sc_c};font-weight:{"700" if is_t else "500"};">'
                f'{_fbi(sc)}</span>{_arrow(sc) if is_t else ""}', "right"),
            _td(f'<span style="{"font-weight:600;" if is_t else "color:#555;"}">{_fi(sb)}</span>', "right"),
            _td(f'<span style="color:{sr_c};font-weight:{"700" if is_t else "400"};">'
                f'{sr_ratio:.1f}%</span>', "right"),
            bg=bg,
        )
    # 說明列
    tbl += (
        '<tr><td colspan="6" style="padding:5px 11px;font-size:11px;color:#888;'
        'background:#F8F9FA;border-top:1px solid #EAECEE;">'
        '券資比說明：&lt;5% 偏多（軋空潛力高）｜5~15% 中性｜&gt;15% 偏空（放空壓力大）'
        '</td></tr>'
    )
    tbl += TABLE_CLOSE
    return hdr + tbl


def _render_futures(futures: list) -> str:
    if not futures:
        return ""
    hdr = _sec_hdr("📉", "期貨三大法人台指期淨部位（口）", "purple", "近 6 日｜未平倉淨口數")
    tbl = _table_open([("100px",""), ("130px",""), ("130px",""), ("120px","")])
    tbl += _th_row(
        ("日期", "left"),
        ("外資淨口數", "right"), ("投信淨口數", "right"), ("自營淨口數", "right"),
        bg="#6C3483",
    )
    for i, d in enumerate(futures[:EMAIL_TABLE_DAYS]):
        fg = d.get("foreign_net_oi", 0)
        tr = d.get("trust_net_oi", 0)
        dl = d.get("dealer_net_oi", 0)
        bg = "#FFFDE7" if i == 0 else ("#F8F9FA" if i % 2 else "#FFF")
        is_t = (i == 0)

        def _fc(v):
            col = _color(v)
            bld = "font-weight:700;" if is_t else "font-weight:500;"
            return _td(
                f'<span style="color:{col};{bld}">{_fbi(v)}</span>'
                + (_arrow(v) if is_t else ""), "right"
            )

        date_html = _fmt_date(d.get("date","")) + ("&nbsp;" + _badge("最新","#6C3483") if is_t else "")
        tbl += _tr(_td(date_html, bold=is_t), _fc(fg), _fc(tr), _fc(dl), bg=bg)

    # 說明列
    tbl += (
        '<tr><td colspan="4" style="padding:5px 11px;font-size:11px;color:#888;'
        'background:#F8F9FA;border-top:1px solid #EAECEE;">'
        '正值=淨多單（看漲）｜負值=淨空單（看跌）｜自營商主要以選擇權避險，期貨部位通常接近 0'
        '</td></tr>'
    )
    tbl += TABLE_CLOSE
    return hdr + tbl


def _render_tdcc(tdcc: list) -> str:
    if not tdcc:
        return ""
    hdr = _sec_hdr("🏆", "千張大戶持股比例", "teal", "集保中心資料｜法人籌碼集中度")
    tbl = _table_open([("80px",""), ("90px",""), ("80px",""), ("90px",""), ("80px","")])
    tbl += _th_row(
        ("股票代號", "left"), ("千張以上人數", "right"), ("持股比例", "right"),
        ("400~999張人數", "right"), ("400~999張佔比", "right"),
        bg="#148F77",
    )
    for i, d in enumerate(tdcc):
        bg    = "#F8F9FA" if i % 2 else "#FFF"
        pct   = d.get("pct_1000_plus", 0)
        cnt   = d.get("holders_1000_plus", 0)
        p400  = d.get("pct_400_999", 0)
        c400  = d.get("holders_400_999", 0)
        # 持股>60% = 籌碼高度集中 = 紅色；<40% = 分散 = 綠色
        pct_c = "#C0392B" if pct >= 60 else "#27AE60" if pct < 40 else "#E67E22"
        # 視覺比例 bar（max假設 80%）
        bar_w = min(int(pct / 80 * 60), 60)
        bar   = (
            f'<span style="color:{pct_c};font-weight:700;">{pct:.1f}%</span>'
            f'&nbsp;<span style="display:inline-block;width:{bar_w}px;height:8px;'
            f'background:{pct_c};border-radius:2px;vertical-align:middle;opacity:.7;"></span>'
        )
        tbl += _tr(
            _td(f'<span style="font-weight:700;color:#1A5276;">{_esc(d.get("stock_id",""))}</span>'),
            _td(_fi(cnt), "right"),
            _td(bar, "right"),
            _td(_fi(c400) if c400 else "—", "right", color="#555"),
            _td(f'{p400:.1f}%' if p400 else "—", "right", color="#888"),
            bg=bg,
        )
    # 說明列
    tbl += (
        '<tr><td colspan="5" style="padding:5px 11px;font-size:11px;color:#888;'
        'background:#F8F9FA;border-top:1px solid #EAECEE;">'
        '千張大戶持股 &gt;60% = 籌碼高度集中（支撐強）｜&lt;40% = 籌碼分散（浮額多）'
        '</td></tr>'
    )
    tbl += TABLE_CLOSE
    return hdr + tbl


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  外資分點渲染（專業版）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _render_broker_block(block: dict, rank: int) -> str:
    broker    = _esc(block.get("broker", ""))
    total_net = block.get("total_net", 0)
    rows      = block.get("rows") or []
    nc        = _color(total_net, "#555")
    medal     = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"#{rank}")

    # 外資券商 header 列
    net_badge_bg = "#C0392B" if total_net > 0 else "#27AE60" if total_net < 0 else "#888"
    header = (
        f'<div style="margin:12px 0 0;padding:9px 14px;'
        f'background:#1A2A3A;'
        f'border-radius:5px 5px 0 0;'
        f'display:flex;align-items:center;justify-content:space-between;">'
        f'<span style="font-size:14px;font-weight:700;color:#FFFFFF;">'
        f'{medal}&nbsp; {broker}</span>'
        f'<span style="font-size:12px;">'
        f'<span style="color:#D5E8F5;">總淨超&nbsp;</span>'
        f'<span style="color:{nc};font-weight:800;font-size:14px;">{_fi(total_net)}</span>'
        f'<span style="color:#D5E8F5;font-size:11px;">&nbsp;張</span>'
        f'</span></div>'
    )

    if not rows:
        return header + '<div style="padding:8px 14px;background:#F8F9FA;border:1px solid #D5D8DC;border-top:none;border-radius:0 0 5px 5px;font-size:12px;color:#888;">本期無明細資料</div>'

    # 欄寬：# / 代號名稱 / 淨超 / 均價 / 現價 / 乖離率 / 進場價 / 停損 / 停利1 / 風報比 / 訊號
    tbl = _table_open([
        ("25px",""), ("auto",""), ("70px",""), ("65px",""), ("65px",""), ("60px",""),
        ("65px",""), ("65px",""), ("65px",""), ("55px",""), ("60px",""),
    ])
    tbl += _th_row(
        ("#", "center"), ("代號　股票名稱", "left"), ("淨超(張)", "right"),
        ("均價", "right"), ("現價", "right"), ("乖離率", "right"),
        ("進場價", "right"), ("停損價", "right"), ("停利1", "right"),
        ("風報比", "right"), ("訊號", "center"),
        bg="#1A252F",
    )
    for j, r in enumerate(rows[:5], 1):
        nv       = r.get("net", 0)
        rc       = _color(nv)
        bias_raw = str(r.get("bias", "")).replace("%", "").strip()
        try:
            bv   = float(bias_raw)
            if bv > 5:   bias_c, bias_bg = "#922B21", "#FADBD8"
            elif bv > 1: bias_c, bias_bg = "#C0392B", "#FDF2F2"
            elif bv < -5:bias_c, bias_bg = "#1A5632", "#D5F5E3"
            elif bv < -1:bias_c, bias_bg = "#27AE60", "#EAFAF1"
            else:         bias_c, bias_bg = "#555",    "#F8F9FA"
            bias_html = (
                f'<span style="display:inline-block;padding:1px 5px;'
                f'background:{bias_bg};color:{bias_c};border-radius:3px;'
                f'font-size:11px;font-weight:700;">{r.get("bias","")}</span>'
            )
        except Exception:
            bias_html = _esc(str(r.get("bias", "")))

        # 訊號欄：🔥可買 → 金黃亮燈；其他 → 灰色觀察
        verdict = str(r.get("verdict", "觀察"))
        if "可買" in verdict:
            sig_html = '<span style="display:inline-block;padding:1px 6px;background:#FFD700;color:#7D4000;border-radius:3px;font-size:11px;font-weight:800;">🔥可買</span>'
        else:
            sig_html = '<span style="color:#AAA;font-size:11px;">觀察</span>'

        # 風報比
        rr = r.get("rr", "")
        rr_html = f'<span style="color:#2874A6;font-size:11px;font-weight:700;">{_esc(str(rr))}</span>' if rr else "-"

        # 進場/停損/停利（取自 top_preview rows）
        entry = r.get("entry", "")
        stop  = r.get("stop",  "")
        tp1   = r.get("tp1",   "")

        bg = "#FDFEFE" if j % 2 else "#F2F3F4"
        tbl += _tr(
            _td(f'<span style="color:#888;font-size:11px;">{j}</span>', "center"),
            _td(f'<span style="font-weight:700;color:#1A5276;">{_esc(r.get("sid",""))}</span>'
                f'&nbsp;<span style="font-weight:600;color:#2C3E50;">{_esc(r.get("name",""))}</span>'),
            _td(f'<span style="color:{rc};font-weight:700;">{_fi(nv)}</span>', "right"),
            _td(f'<span style="color:#7F8C8D;">{_esc(str(r.get("avg","")))}</span>', "right"),
            _td(f'<span style="font-weight:700;color:#333;">{_esc(str(r.get("price","")))}</span>', "right"),
            _td(bias_html, "right"),
            _td(f'<span style="color:#1A5276;font-weight:700;">{_esc(str(entry))}</span>', "right") if entry else _td("-", "right"),
            _td(f'<span style="color:#C0392B;font-weight:700;">{_esc(str(stop))}</span>', "right")  if stop  else _td("-", "right"),
            _td(f'<span style="color:#1A7A4A;font-weight:700;">{_esc(str(tp1))}</span>', "right")   if tp1   else _td("-", "right"),
            _td(rr_html, "right"),
            _td(sig_html, "center"),
            bg=bg,
        )
    tbl += TABLE_CLOSE
    return header + tbl


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  AI 分析 HTML 格式化（v11：支援 **bold**、E) 獨立卡片）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SECTION_STYLES = {
    # header_bg = 全寬 banner 背景色, content_bg = 內容區淡底色, accent = 左邊線/數字圓圈色
    "A": {"header_bg": "#1A6FA8", "content_bg": "#EBF5FB", "accent": "#2E86C1",
          "icon": "📊", "label": "大盤籌碼環境研判"},
    "B": {"header_bg": "#9A6F00", "content_bg": "#FEF9E7", "accent": "#D4AC0D",
          "icon": "🏦", "label": "外資力量深度剖析"},
    "C": {"header_bg": "#1A7A40", "content_bg": "#EAFAF1", "accent": "#27AE60",
          "icon": "🎯", "label": "明日觀察清單"},
    "D": {"header_bg": "#A93226", "content_bg": "#FDEDEC", "accent": "#E74C3C",
          "icon": "⚠️", "label": "風控與資金配置"},
    "E": {"header_bg": "#6C3483", "content_bg": "#F4ECF7", "accent": "#8E44AD",
          "icon": "💡", "label": "一句話摘要"},
    "F": {"header_bg": "#1A5276", "content_bg": "#EAF2FB", "accent": "#2471A3",
          "icon": "🔍", "label": "外資交叉比對亮點"},
}


def _format_ai_html(ai_text: str) -> str:
    if not ai_text or not ai_text.strip():
        return '<p style="color:#888;font-size:13px;">本次尚未取得 AI 分析。</p>'

    # 錯誤訊息顯示
    if "失敗" in ai_text[:80] or "error" in ai_text[:80].lower():
        return (
            f'<div style="background:#FDF2F2;border-left:4px solid #E74C3C;'
            f'padding:12px 16px;border-radius:4px;color:#922;font-size:13px;">'
            f'{_esc(ai_text[:800])}</div>'
        )

    lines      = ai_text.strip().split("\n")
    html       = []
    in_section = False
    cur_letter = None
    e_summary  = ""

    def _close_section():
        nonlocal in_section
        if in_section:
            html.append('</div></div>')  # close content + wrapper
            in_section = False

    def _pi(raw: str) -> str:
        return _style_keywords(_md_inline(_esc(raw)))

    def _num_badge(num: str, color: str, shape: str = "circle") -> str:
        radius = "50%" if shape == "circle" else "3px"
        return (
            f'<span style="display:inline-block;min-width:18px;height:18px;'
            f'background:{color};color:#fff;border-radius:{radius};text-align:center;'
            f'line-height:18px;font-size:10px;font-weight:700;margin-right:8px;'
            f'vertical-align:middle;opacity:0.85;">{num}</span>'
        )

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # ══════════════════════════════════════════
        #  Section header  A) … F)
        # ══════════════════════════════════════════
        hm = re.match(r'^([A-F])\s*[)）:：]\s*(.*)', stripped)
        if hm:
            _close_section()
            letter = hm.group(1)
            ai_title = hm.group(2).strip().rstrip("：:").strip()
            st = SECTION_STYLES.get(letter, {
                "header_bg": "#555", "content_bg": "#F5F5F5",
                "accent": "#999", "icon": "▪", "label": ""
            })
            display_title = ai_title if ai_title else st["label"]

            # ── E) 一句話摘要：獨立漸層卡片，不走通用模板 ──
            if letter == "E":
                e_summary = display_title
                html.append(
                    f'<div style="margin:26px 0 4px;">'
                    # 全寬 banner
                    f'<div style="background:{st["header_bg"]};'
                    f'padding:11px 18px;border-radius:6px 6px 0 0;">'
                    f'<span style="font-size:15px;font-weight:800;color:#fff;letter-spacing:.5px;">'
                    f'{st["icon"]}&nbsp; E）{st["label"]}</span></div>'
                    # 摘要卡片內容
                    f'<div style="background:{st["content_bg"]};border:1px solid #D2B4DE;'
                    f'border-top:none;border-radius:0 0 6px 6px;padding:14px 20px;">'
                )
                if display_title:
                    html.append(
                        f'<p style="margin:0;font-size:16px;font-weight:700;color:#4A235A;'
                        f'line-height:1.7;">{_pi(display_title)}</p>'
                    )
                html.append('</div></div>')
                in_section = True
                cur_letter = "E"
                continue

            # ── 通用 Section banner ──
            html.append(
                # 外層 wrapper
                f'<div style="margin:26px 0 0;border-radius:6px;overflow:hidden;'
                f'box-shadow:0 2px 6px rgba(0,0,0,.10);">'
                # ★ 全寬深色 banner 標題
                f'<div style="background:{st["header_bg"]};padding:12px 18px;">'
                f'<span style="font-size:16px;font-weight:800;color:#fff;letter-spacing:.5px;">'
                f'{st["icon"]}&nbsp; {letter}）{_esc(display_title)}</span></div>'
                # 內容區
                f'<div style="background:{st["content_bg"]};padding:12px 18px 16px;'
                f'font-size:13.5px;line-height:1.9;color:#333;">'
            )
            in_section = True
            cur_letter = letter
            continue

        # E) 後面的純文字行（摘要在下一行的情況）
        if cur_letter == "E":
            if not e_summary:
                e_summary = stripped
                html.append(f'<p style="margin:0;font-size:16px;font-weight:700;color:#4A235A;">{_pi(stripped)}</p>')
            continue

        if not in_section:
            html.append(f'<p style="font-size:13px;color:#666;margin:4px 0;">{_pi(stripped)}</p>')
            continue

        st = SECTION_STYLES.get(cur_letter, {"accent": "#555", "content_bg": "#FFF"})
        accent = st["accent"]

        # ══════════════════════════════════════════
        #  編號項目  1) 2) 3)
        # ══════════════════════════════════════════
        nm = re.match(r'^(\d+)\s*[)）.]\s*(.*)', stripped)
        if nm:
            num, text = nm.group(1), nm.group(2)

            if cur_letter == "B":
                # 金色券商名稱卡片
                html.append(
                    f'<div style="margin:10px 0 3px;padding:7px 12px;'
                    f'background:#FFFBEE;border-left:3px solid #D4AC0D;border-radius:0 4px 4px 0;">'
                    + _num_badge(num, "#D4AC0D", "square")
                    + f'<span style="font-weight:600;font-size:12.5px;color:#7D6608;'
                    f'vertical-align:middle;">{_pi(text)}</span></div>'
                )
            elif cur_letter == "C":
                stock_m = re.match(r'^(\d{4,5})\s+(.+)', text)
                if stock_m:
                    sid, rest = stock_m.groups()
                    html.append(
                        f'<div style="margin:8px 0 3px;padding:6px 12px;'
                        f'background:#E8F8F5;border-left:3px solid #1ABC9C;border-radius:0 4px 4px 0;">'
                        + _num_badge(num, "#1ABC9C")
                        + f'<span style="font-weight:700;font-size:12.5px;color:#0E6655;'
                        f'vertical-align:middle;">{_esc(sid)}&nbsp;</span>'
                        f'<span style="font-weight:500;font-size:12.5px;color:#333;vertical-align:middle;">'
                        f'{_pi(rest)}</span></div>'
                    )
                else:
                    html.append(
                        f'<div style="margin:6px 0 2px;padding:5px 10px 5px 8px;'
                        f'border-left:2px solid #27AE60;border-radius:0 3px 3px 0;">'
                        + _num_badge(num, "#27AE60")
                        + f'<span style="font-size:12.5px;color:#333;line-height:1.7;vertical-align:middle;">{_pi(text)}</span></div>'
                    )
            elif cur_letter == "D":
                html.append(
                    f'<div style="margin:6px 0;padding:5px 10px 5px 8px;'
                    f'border-left:2px solid #E74C3C;border-radius:0 3px 3px 0;">'
                    + _num_badge(num, "#E74C3C")
                    + f'<span style="font-size:12.5px;color:#333;line-height:1.7;vertical-align:middle;">{_pi(text)}</span></div>'
                )
            elif cur_letter == "F":
                html.append(
                    f'<div style="margin:6px 0 3px;padding:5px 10px 5px 8px;'
                    f'border-left:2px solid #2471A3;border-radius:0 3px 3px 0;">'
                    + _num_badge(num, "#2471A3", "square")
                    + f'<span style="font-size:12.5px;color:#333;line-height:1.7;vertical-align:middle;">{_pi(text)}</span></div>'
                )
            else:
                # A 區（及預設）— 縮小字體，明顯低於 section banner
                html.append(
                    f'<div style="margin:6px 0;padding:5px 10px 5px 8px;'
                    f'border-left:2px solid {accent};border-radius:0 3px 3px 0;">'
                    + _num_badge(num, accent)
                    + f'<span style="font-size:12.5px;color:#333;line-height:1.7;vertical-align:middle;">{_pi(text)}</span></div>'
                )
            continue

        # ══════════════════════════════════════════
        #  子項目  - / • / *
        # ══════════════════════════════════════════
        sm = re.match(r'^[-•\*＊]\s*(.*)', stripped)
        if sm:
            html.append(
                f'<div style="margin:3px 0 3px 42px;padding:3px 10px;'
                f'border-left:2px solid #C8D6E5;font-size:13px;color:#555;">'
                f'{_pi(sm.group(1))}</div>'
            )
            continue

        # ── 一般內文 ──
        html.append(
            f'<div style="margin:4px 0 4px 6px;color:#444;font-size:13.5px;line-height:1.85;">'
            f'{_pi(stripped)}</div>'
        )

    _close_section()
    return "\n".join(html)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HTML Email 主體建構
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_html(summary: dict) -> str:
    ok = summary.get("success", False)
    sc = "#27AE60" if ok else "#E74C3C"
    st = "✅ 成功" if ok else "⚠️ 失敗/部分失敗"

    p = []

    # ── Header Banner ─────────────────────────────────
    gen_at   = summary.get("generated_at", "")
    days     = summary.get("days", 5)
    tot_rows = summary.get("total_rows", 0)
    brk_ok   = summary.get("brokers_ok", 0)
    brk_fail = summary.get("brokers_fail", 0)

    # 大盤月線狀態（從 summary 取）
    taiex_ma = summary.get("taiex_above_ma20")
    taiex_ma_badge = (
        '<span style="color:#58D68D;font-weight:700;">✅ 大盤站月線</span>'
        if taiex_ma else
        '<span style="color:#EC7063;font-weight:700;">❌ 大盤跌月線</span>'
        if taiex_ma is not None else ""
    )

    p.append(
        f'<div style="background:#0F2744;border-bottom:3px solid #1F6FAB;'
        f'padding:22px 26px 18px;border-radius:8px 8px 0 0;">'
        f'<h1 style="margin:0 0 4px;font-size:22px;color:#fff;font-weight:800;'
        f'letter-spacing:.5px;">📊 半量化交易訊號系統</h1>'
        f'<p style="margin:0 0 6px;font-size:12px;color:#7FB3D3;line-height:1.6;">'
        f'外資籌碼 × 技術突破 × 流動性 × 風報比 × 大盤月線&nbsp;→&nbsp;自動產出次日進場訊號</p>'
        f'<p style="margin:0;font-size:12.5px;color:#B0C8E8;line-height:1.8;">'
        f'產生時間：{_esc(gen_at)}&nbsp;｜&nbsp;近 {days} 日&nbsp;｜&nbsp;'
        f'資料筆數：{tot_rows:,}&nbsp;｜&nbsp;'
        f'<span style="color:{sc};font-weight:700;">{st}</span>&nbsp;｜&nbsp;'
        f'OK <span style="color:#58D68D;">{brk_ok}</span> / '
        f'FAIL <span style="color:#EC7063;">{brk_fail}</span>'
        + (f'&nbsp;｜&nbsp;{taiex_ma_badge}' if taiex_ma_badge else '')
        + f'</p></div>'
    )

    # ── GitHub Actions 連結 ───────────────────────────
    srv  = os.getenv("GITHUB_SERVER_URL")
    repo = os.getenv("GITHUB_REPOSITORY")
    rid  = os.getenv("GITHUB_RUN_ID")
    if srv and repo and rid:
        link = f"{srv}/{repo}/actions/runs/{rid}"
        p.append(
            f'<div style="padding:7px 20px;background:#E8ECF0;border-bottom:1px solid #D0D8E4;">'
            f'<a href="{link}" style="font-size:12px;color:#2E86C1;text-decoration:none;">'
            f'🔗 GitHub Actions Workflow 執行記錄</a></div>'
        )

    p.append('<div style="padding:16px 20px 20px;">')

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  一、大盤環境速覽（TAIEX / 三大法人 / 融資融券 / 期貨）
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    market = summary.get("market_data") or {}

    taiex   = market.get("taiex") or []
    inst    = market.get("institutional") or []
    margin  = market.get("margin") or []
    futures = market.get("futures") or []
    tdcc    = market.get("tdcc") or []

    # ── 手機友善：KPI 摘要卡片 + 資料異常警示（置頂顯示）────────────────────
    if inst:
        p.append(_render_kpi_inst(inst))
        warns = _validate_institutional(inst)
        if warns:
            items = "".join(f"<li>{_esc(w)}</li>" for w in warns[:4])
            p.append(
                '<div style="margin:8px 0;background:#FFFBEB;border:1px solid #F59E0B;'
                'border-radius:8px;padding:10px 12px;">'
                '<div style="font-weight:800;color:#92400E;">⚠️ 資料異常偵測</div>'
                f'<ul style="margin:4px 0 0 16px;color:#92400E;line-height:1.6;">{items}</ul>'
                '</div>'
            )

    has_market = any([taiex, inst, margin, futures])
    if has_market:
        p.append(
            '<div style="margin:8px 0 4px;padding:6px 14px;'
            'background:#1A2A3A;border-radius:4px;">'
            '<span style="font-size:13px;font-weight:700;color:#ECF0F1;'
            'letter-spacing:1px;">一、大盤環境速覽</span></div>'
        )

    if taiex:
        p.append(_render_taiex(taiex))

    if inst:
        p.append(_render_institutional(inst))

    # 融資融券已移除（數據抓取不穩定，移除顯示）

    if futures:
        p.append(_render_futures(futures))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  二、外資分點明細 Top N
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    top_preview = summary.get("top_preview") or []
    if top_preview:
        p.append(
            '<div style="margin:20px 0 4px;padding:6px 14px;'
            'background:#1A2A3A;border-radius:4px;">'
            '<span style="font-size:13px;font-weight:700;color:#ECF0F1;'
            'letter-spacing:1px;">二、外資分點明細（淨超 Top '
            f'{len(top_preview)} 家）</span></div>'
        )
        for rank, block in enumerate(top_preview, 1):
            p.append(_render_broker_block(block, rank))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  三、千張大戶持股（原本未渲染）
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if tdcc:
        p.append(
            '<div style="margin:20px 0 4px;padding:6px 14px;'
            'background:#1A2A3A;border-radius:4px;">'
            '<span style="font-size:13px;font-weight:700;color:#ECF0F1;'
            'letter-spacing:1px;">三、千張大戶持股比例</span></div>'
        )
        p.append(_render_tdcc(tdcc))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  二點五、🔥 半量化進場訊號（🔥可買 個股）
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 從 top_preview 撈出所有 verdict=="🔥可買" 的個股
    buy_signals = []
    for block in top_preview:
        broker_name = block.get("broker", "")
        for r in (block.get("rows") or []):
            if "可買" in str(r.get("verdict", "")):
                buy_signals.append({**r, "_broker": broker_name})

    if buy_signals:
        p.append(
            '<div style="margin:20px 0 4px;padding:6px 14px;'
            'background:#7D6608;border-radius:4px;">'
            '<span style="font-size:13px;font-weight:700;color:#FFD700;'
            'letter-spacing:1px;">🔥 半量化進場訊號（今日符合進場條件）</span></div>'
        )
        sig_tbl = _table_open([
            ("auto",""), ("48px",""), ("58px",""), ("58px",""), ("58px",""),
            ("58px",""), ("42px",""), ("62px",""), ("44px",""), ("54px",""), ("58px",""), ("60px",""),
        ])
        sig_tbl += _th_row(
            ("代號　名稱", "left"), ("現價", "right"), ("進場價", "right"),
            ("停損價", "right"), ("停利1", "right"), ("停利2", "right"),
            ("風報比", "right"), ("每張風險NT$", "right"),
            ("半凱利%", "right"), ("最大張數", "right"),
            ("零股數 ★", "right"),   # 前導測試核心欄位，取整至10倍數
            ("外資券商", "center"),
            bg="#7D6608",
        )
        for si, r in enumerate(buy_signals):
            bg = "#FFFDE7" if si % 2 == 0 else "#FFF9C4"
            entry    = r.get("entry",    "-"); stop     = r.get("stop",    "-")
            tp1      = r.get("tp1",      "-"); tp2      = r.get("tp2",     "-")
            rr       = r.get("rr",       "-"); price    = r.get("price",   "-")
            risk     = r.get("risk",     "-"); hkelly   = r.get("hkelly",  "-")
            max_lots = r.get("max_lots",  0)
            shares   = r.get("shares",    0)   # 建議零股數（已取整至10的倍數）
            # 每張風險色階
            try:
                risk_num   = int(str(risk).replace(",",""))
                risk_color = "#C0392B" if risk_num > 50000 else "#E67E22" if risk_num > 20000 else "#27AE60"
            except Exception:
                risk_color = "#555"
            # 半凱利%色階
            try:
                hk_num   = float(str(hkelly).replace("%",""))
                hk_color = "#1A5276" if hk_num >= 15 else "#E67E22" if hk_num < 10 else "#555"
            except Exception:
                hk_color = "#555"
            # 最大張數
            if max_lots and int(str(max_lots)) > 0:
                ml_num   = int(str(max_lots))
                ml_color = "#1A7A4A" if ml_num > 10 else "#E67E22"
                ml_html  = f'<span style="color:{ml_color};font-weight:800;font-size:13px;">{ml_num} 張</span>'
            else:
                ml_html  = '<span style="color:#AAA;font-size:10px;">未設定</span>'
            # 建議零股數（已取整至10倍數，0=未設定或風險過高）
            if shares and int(str(shares)) > 0:
                sh_num = int(str(shares))
                # ≥1000股=深綠（可分批），100~999=綠，10~99=橙（小量試單），<10=警示
                sh_color = ("#1A7A4A" if sh_num >= 1000 else
                            "#27AE60" if sh_num >= 100 else
                            "#E67E22" if sh_num >= 10 else "#C0392B")
                sh_html = (f'<span style="color:{sh_color};font-weight:800;font-size:14px;">'
                           f'{sh_num} 股</span>')
            else:
                sh_html = '<span style="color:#AAA;font-size:10px;">0（風險超限）</span>'
            sig_tbl += _tr(
                _td(f'<span style="font-weight:700;color:#1A5276;">{_esc(r.get("sid",""))}</span>'
                    f'&nbsp;<span style="font-weight:600;">{_esc(r.get("name",""))}</span>'),
                _td(f'<b>{_esc(str(price))}</b>', "right"),
                _td(f'<span style="color:#1A5276;font-weight:700;">{_esc(str(entry))}</span>', "right"),
                _td(f'<span style="color:#C0392B;font-weight:700;">{_esc(str(stop))}</span>',  "right"),
                _td(f'<span style="color:#1A7A4A;font-weight:700;">{_esc(str(tp1))}</span>',   "right"),
                _td(f'<span style="color:#6C3483;font-weight:700;">{_esc(str(tp2))}</span>',   "right"),
                _td(f'<span style="color:#2874A6;font-weight:700;">{_esc(str(rr))}</span>',    "right"),
                _td(f'<span style="color:{risk_color};font-weight:700;">{_esc(str(risk))}</span>', "right"),
                _td(f'<span style="color:{hk_color};font-weight:700;">{_esc(str(hkelly))}%</span>', "right"),
                _td(ml_html, "right"),
                _td(sh_html, "right"),
                _td(f'<span style="font-size:11px;color:#555;">{_esc(r.get("_broker",""))}</span>', "center"),
                bg=bg,
            )
        sig_tbl += TABLE_CLOSE
        p.append(sig_tbl)
        p.append(
            '<div style="margin:4px 0 4px;padding:6px 12px;background:#FFF9C4;'
            'border-left:4px solid #FFD700;font-size:11px;color:#7D4000;line-height:1.7;">'
            '⚠️ 六大進場條件：突破20日高＋放量1.5倍(≥500張)＋站上10均＋大盤月線＋外資淨買超'
            '&nbsp;｜&nbsp;停損 -8%&nbsp;｜&nbsp;停利1 +20%&nbsp;｜&nbsp;停利2 +35%</div>'
        )
        p.append(
            '<div style="margin:0 0 16px;padding:5px 12px;background:#EBF5FB;'
            'border-left:4px solid #2E86C1;font-size:11px;color:#1A5276;line-height:1.7;">'
            '📐 <b>半凱利資金控管</b>：f*/2 = (W−(1−W)÷R)÷2&nbsp;｜&nbsp;'
            '勝率W=55%、風報比R=2.5 → 半凱利 18.5%&nbsp;｜&nbsp;'
            '<b>最大張數 = 總資金 × 半凱利% ÷ 每張風險NT$</b>&nbsp;｜&nbsp;'
            '在 daily_report.yml 的 env 加入 TOTAL_CAPITAL 與 WIN_RATE 即可自動計算張數</div>'
        )
    else:
        p.append(
            '<div style="margin:16px 0;padding:8px 14px;background:#F4F4F4;'
            'border-radius:4px;font-size:12px;color:#888;">'
            '今日無符合半量化六大進場條件的個股。</div>'
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  四、AI 深度分析
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ai_text     = summary.get("ai_analysis", "")
    ai_model    = summary.get("ai_model", "")
    ai_provider = summary.get("ai_provider", "")
    ai_version  = summary.get("ai_analyzer_version", "")

    p.append(
        '<div style="margin:24px 0 4px;padding:6px 14px;'
        'background:#1A2A3A;border-radius:4px;">'
        '<span style="font-size:13px;font-weight:700;color:#ECF0F1;'
        'letter-spacing:1px;">四、🤖 AI 深度分析</span></div>'
    )
    # 模型資訊 pill
    if ai_provider or ai_model:
        parts_info = []
        if ai_provider:
            parts_info.append(_esc(ai_provider))
        if ai_model:
            parts_info.append(_esc(ai_model))
        if ai_version:
            parts_info.append(_esc(ai_version))
        p.append(
            f'<div style="margin:6px 0 12px;display:flex;flex-wrap:wrap;gap:6px;">'
            + "".join(
                f'<span style="display:inline-block;padding:2px 10px;background:#EBF5FB;'
                f'border:1px solid #AED6F1;border-radius:12px;font-size:11px;color:#2E86C1;">'
                f'{part}</span>'
                for part in parts_info
            )
            + "</div>"
        )

    p.append(_format_ai_html(ai_text))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  錯誤摘要
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    errors = summary.get("errors") or []
    market_errors = market.get("fetch_errors") or []
    all_errors = errors + market_errors

    if all_errors:
        p.append(
            '<div style="margin-top:18px;padding:10px 16px;'
            'background:#FDF2F2;border:1px solid #FADBD8;'
            'border-radius:4px;font-size:12px;color:#922;">'
            '<b>⚠️ 錯誤摘要：</b><br>'
            + "".join(f'&bull; {_esc(e)}<br>' for e in all_errors[:15])
            + '</div>'
        )

    p.append('</div>')  # close padding div

    # ── 免責聲明 ──────────────────────────────────────
    p.append(
        '<div style="padding:14px 22px;background:#FFF9E6;'
        'border-top:2px solid #F0E0A0;font-size:11.5px;color:#8B7500;line-height:1.7;">'
        '<b>[免責聲明]</b> '
        '帶進帶出是違法的行為，此資料皆為網上根據盤後資訊大數據所取得訊息，'
        '非作為或被視為買進或售出標的的邀請或意象，'
        '請自行依據取得資訊評估風險與獲利，<b>有賺有賠請斟酌</b>。'
        '</div>'
    )

    # ── Footer ────────────────────────────────────────
    ymd_now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    p.append(
        f'<div style="padding:10px 22px;background:#1A2A3A;'
        f'border-radius:0 0 8px 8px;font-size:11px;color:#9DB5CC;text-align:center;">'
        f'此信由 GitHub Actions 自動寄出 ｜ {ymd_now} (TW) ｜ 半量化交易訊號系統</div>'
    )

    body = "\n".join(p)
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<style>'
        'table{border-spacing:0!important;}'
        'td,th{border-spacing:0!important;font-size:13px;line-height:1.5;}'
        'a{color:#2E86C1;}'
        '@media only screen and (max-width:520px){'
        '.wrap{padding:8px!important;}'
        'td,th{font-size:11px!important;}'
        'h1{font-size:18px!important;}'
        '}'
        '</style></head>'
        '<body style="margin:0;padding:14px 10px;background:#DDE4EC;'
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,"
        "'Noto Sans TC','PingFang TC','Microsoft JhengHei',sans-serif;\">"
        '<div class="wrap" style="max-width:680px;margin:0 auto;background:#FFF;'
        'border-radius:10px;overflow:hidden;box-shadow:0 3px 12px rgba(0,0,0,0.15);">'
        f'{body}'
        '</div>'
        '<div style="max-width:680px;margin:10px auto 0;font-size:11px;'
        'color:#888;text-align:center;line-height:1.8;">'
        '📎 手機版建議先看「今日重點摘要」與「AI 分析」；完整明細請開附件 PDF / Excel。'
        '</div>'
        '</body></html>'
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Plain Text 備用版
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_plain(summary: dict) -> str:
    ok = summary.get("success", False)
    market = summary.get("market_data") or {}

    lines = [
        "【外資分點狙擊分析】",
        f"狀態：{'成功' if ok else '失敗/部分失敗'}",
        f"產生時間：{summary.get('generated_at','')}",
        f"資料筆數：{summary.get('total_rows',0)}",
        "",
    ]

    # 大盤概況
    inst = market.get("institutional") or []
    if inst:
        today = inst[0]
        lines.append("【三大法人今日買賣超】")
        lines.append(f"  外資：{_fb(today['foreign']['net'])}")
        lines.append(f"  投信：{_fb(today['trust']['net'])}")
        lines.append(f"  自營：{_fb(today['dealer']['net'])}")
        lines.append(f"  合計：{_fb(today.get('total_net',0))}")
        lines.append("")

    taiex = market.get("taiex") or []
    if taiex:
        t = taiex[0]
        chg = t.get("change", 0)
        sign = "+" if chg > 0 else ""
        lines.append(f"【大盤指數】 收盤={t.get('close',0)}  漲跌={sign}{chg}  成交={t.get('amount_billion',0):.0f}億")
        lines.append("")

    # AI 分析
    ai_text = summary.get("ai_analysis", "")
    if ai_text:
        lines.append("【AI 深度分析】")
        lines.append(ai_text[:4000])
        lines.append("")

    lines.append(
        "[免責聲明] 帶進帶出是違法的行為，此資料皆為網上根據盤後資訊大數據所取得訊息，"
        "非作為或被視為買進或售出標的的邀請或意象，請自行依據取得資訊評估風險與獲利，有賺有賠請斟酌。"
    )
    lines.append("")
    lines.append("（此信由 GitHub Actions 自動寄出）")
    return "\n".join(lines)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  A4 分析報告 PDF（reportlab）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _pdf_font():
    """回傳 (font_name, bold_name)。嘗試 NotoSansTC；失敗回 Helvetica。"""
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    for path in [
        os.path.join("fonts", "NotoSansTC-Regular.ttf"),
        os.path.join("fonts", "NotoSansCJK-Regular.ttc"),
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    ]:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont("NotoTC", path))
                pdfmetrics.registerFont(TTFont("NotoTC-B", path))
                pdfmetrics.registerFontFamily("NotoTC", normal="NotoTC", bold="NotoTC-B")
                return "NotoTC", "NotoTC-B"
            except Exception:
                pass
    return "Helvetica", "Helvetica-Bold"


def build_analysis_pdf(summary: dict, pdf_path: str):
    """
    專業 A4 分析報告 PDF：
    - BaseDocTemplate + PageTemplate（每頁固定頁首 / 頁尾 / 頁碼）
    - 封面（深藍漸層、報告資訊表格）
    - 一、大盤環境速覽
    - 二、外資分點明細
    - 三、千張大戶持股
    - 四、AI 深度分析（A~F 分段彩色 banner）
    - 免責聲明
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import (
        BaseDocTemplate, PageTemplate, Frame,
        Table, TableStyle, Paragraph, Spacer,
        HRFlowable, KeepTogether, PageBreak, NextPageTemplate,
    )
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics

    os.makedirs(os.path.dirname(pdf_path) if os.path.dirname(pdf_path) else ".", exist_ok=True)

    FN, FNB = _pdf_font()        # normal / bold font name
    PW, PH  = A4                 # 595 x 842 pt
    ML = MR = 18 * mm
    MT = 28 * mm                 # top margin（留給 header）
    MB = 22 * mm                 # bottom margin（留給 footer）
    CW = PW - ML - MR            # content width ≈ 559 pt

    # ─── 顏色 ────────────────────────────────────────
    HEX = colors.HexColor
    C = dict(
        navy   = HEX("#0D1F3C"),
        blue   = HEX("#1A4B7A"),
        blue2  = HEX("#1F6FB2"),
        sky    = HEX("#D6E8F7"),
        green  = HEX("#145A32"),
        green2 = HEX("#1A7A40"),
        lt_grn = HEX("#D5F5E3"),
        red    = HEX("#7B241C"),
        red2   = HEX("#C0392B"),
        lt_red = HEX("#FADBD8"),
        purple = HEX("#4A235A"),
        purple2= HEX("#7D3C98"),
        gold   = HEX("#6E4B00"),
        gold2  = HEX("#B7950B"),
        teal   = HEX("#0B5345"),
        teal2  = HEX("#148F77"),
        gray   = HEX("#2C3E50"),
        lt_gray= HEX("#F2F3F4"),
        lt_yel = HEX("#FFFDE7"),
        border = HEX("#BFC9CA"),
        white  = colors.white,
        pos    = HEX("#C0392B"),
        neg    = HEX("#1A7A40"),
        neu    = HEX("#555555"),
    )

    def vc(v) -> colors.Color:
        """正值=紅, 負值=綠, 零=灰"""
        try:
            n = float(str(v).replace(",","").replace("+","")
                      .replace("億","").replace("萬","").replace("口",""))
            return C["pos"] if n > 0 else C["neg"] if n < 0 else C["neu"]
        except Exception:
            return C["neu"]

    def hex_of(col: colors.Color) -> str:
        try:
            return "#{:02X}{:02X}{:02X}".format(
                int(col.red*255), int(col.green*255), int(col.blue*255))
        except Exception:
            return "#333333"

    # ─── ParagraphStyle 工廠 ─────────────────────────
    _ps_cache: dict = {}
    def ps(name, **kw) -> ParagraphStyle:
        key = name + str(sorted(kw.items()))
        if key not in _ps_cache:
            d = dict(fontName=FN, fontSize=9, leading=13,
                     textColor=HEX("#222222"), spaceAfter=0, spaceBefore=0)
            d.update(kw)
            _ps_cache[key] = ParagraphStyle(name + str(len(_ps_cache)), **d)
        return _ps_cache[key]

    def p(txt, **kw) -> Paragraph:
        return Paragraph(str(txt), ps("_", **kw))

    def pv(val, bold=False, size=8.5) -> Paragraph:
        """帶顏色的數值段落"""
        col  = vc(val)
        hc   = hex_of(col)
        fn   = FNB if bold else FN
        return p(f'<font color="{hc}" name="{fn}">{val}</font>',
                 fontSize=size, leading=size+3, fontName=fn)

    # ─── 共用 TableStyle ─────────────────────────────
    def ts_base(hdr_bg, alt="#F8F9FA") -> list:
        return [
            ("BACKGROUND",   (0,0),(-1,0),   hdr_bg),
            ("TEXTCOLOR",    (0,0),(-1,0),   C["white"]),
            ("TEXTCOLOR",    (0,1),(-1,-1),  HEX("#222222")),  # 資料列強制深色
            ("FONTNAME",     (0,0),(-1,0),   FNB),
            ("FONTNAME",     (0,1),(-1,-1),  FN),
            ("FONTSIZE",     (0,0),(-1,-1),  8),
            ("TOPPADDING",   (0,0),(-1,-1),  4),
            ("BOTTOMPADDING",(0,0),(-1,-1),  4),
            ("LEFTPADDING",  (0,0),(-1,-1),  6),
            ("RIGHTPADDING", (0,0),(-1,-1),  6),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [C["white"], HEX(alt)]),
            ("LINEBELOW",    (0,0),(-1,0),   0.8, C["border"]),
            ("LINEABOVE",    (0,0),(-1,0),   0,   C["white"]),
            ("INNERGRID",    (0,1),(-1,-1),  0.25,C["border"]),
            ("BOX",          (0,0),(-1,-1),  0.5, C["border"]),
            ("VALIGN",       (0,0),(-1,-1),  "MIDDLE"),
        ]

    def today_row(ts: list, row=1):
        """把指定 row 塗淡黃（今日最新）"""
        ts.append(("BACKGROUND",(0,row),(-1,row), C["lt_yel"]))
        ts.append(("FONTNAME",  (0,row),(-1,row), FNB))

    # ─── 區塊標題 bar ────────────────────────────────
    def sec_bar(label: str, bg: colors.Color, accent: colors.Color,
                subtitle: str = "") -> list:
        """回傳 [accent_bar_tbl, title_tbl] flowable list"""
        # 左側 3pt 色塊 + 標題文字
        inner = [
            p(label, fontName=FNB, fontSize=13, leading=18,
              textColor=C["white"]),
        ]
        if subtitle:
            inner.append(p(subtitle, fontName=FN, fontSize=8,
                           textColor=HEX("#CCE4F7")))

        title_tbl = Table([inner], colWidths=[CW])
        title_tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0,0),(-1,-1), bg),
            ("LEFTPADDING",  (0,0),(-1,-1), 14),
            ("TOPPADDING",   (0,0),(-1,-1), 7),
            ("BOTTOMPADDING",(0,0),(-1,-1), 7),
            ("LINEAFTER",    (0,0),(0,-1),  4, accent),  # 左邊色條
        ]))
        return [Spacer(1, 3.5*mm), title_tbl, Spacer(1, 1.5*mm)]

    # ─── 子標題（表格前） ────────────────────────────
    def sub_hdr(label: str) -> list:
        tbl = Table([[p(label, fontName=FNB, fontSize=9.5,
                        textColor=C["blue2"])]],
                    colWidths=[CW])
        tbl.setStyle(TableStyle([
            ("LINEBELOW", (0,0),(-1,-1), 1.2, C["blue2"]),
            ("TOPPADDING",(0,0),(-1,-1), 6),
            ("BOTTOMPADDING",(0,0),(-1,-1), 3),
            ("LEFTPADDING",(0,0),(-1,-1), 2),
        ]))
        return [Spacer(1, 2.5*mm), tbl, Spacer(1, 1*mm)]

    # ─── Page template（每頁頁首頁尾） ───────────────
    report_date = datetime.now(TZ).strftime("%Y-%m-%d")
    gen_at      = summary.get("generated_at", "")
    days        = summary.get("days", 5)
    ok          = summary.get("success", False)

    _page_num = [0]   # mutable counter

    def _draw_page(canvas, doc):
        _page_num[0] += 1
        canvas.saveState()

        # ── 頁首 ──
        y_hdr = PH - 13 * mm
        canvas.setFillColor(C["navy"])
        canvas.rect(ML, y_hdr, CW, 9*mm, fill=1, stroke=0)
        canvas.setFillColor(C["white"])
        canvas.setFont(FNB, 8.5)
        canvas.drawString(ML + 4*mm, y_hdr + 3*mm, "外資分點狙擊分析報告")
        canvas.setFont(FN, 7.5)
        canvas.drawRightString(ML + CW - 4*mm, y_hdr + 3*mm,
                               f"報告日期：{report_date}  ｜  產生時間：{gen_at}")
        # 頁首底線
        canvas.setStrokeColor(C["blue2"])
        canvas.setLineWidth(1.5)
        canvas.line(ML, y_hdr, ML + CW, y_hdr)

        # ── 頁尾 ──
        y_ftr = MB - 6*mm
        canvas.setStrokeColor(C["border"])
        canvas.setLineWidth(0.5)
        canvas.line(ML, y_ftr + 5*mm, ML + CW, y_ftr + 5*mm)
        canvas.setFillColor(C["neu"])
        canvas.setFont(FN, 7)
        canvas.drawString(ML, y_ftr + 2*mm,
                          "IKE Analysis System  ｜  此報告由 GitHub Actions 自動產生")
        canvas.setFont(FNB, 7.5)
        canvas.drawRightString(ML + CW, y_ftr + 2*mm,
                               f"第 {_page_num[0]} 頁")

        canvas.restoreState()

    frame = Frame(ML, MB, CW, PH - MT - MB, id="main")
    template = PageTemplate(id="default", frames=[frame], onPage=_draw_page)

    doc = BaseDocTemplate(
        pdf_path,
        pagesize=A4,
        pageTemplates=[template],
        title="外資分點狙擊分析報告",
        author="IKE Analysis System",
        leftMargin=ML, rightMargin=MR,
        topMargin=MT,  bottomMargin=MB,
    )

    story = []

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  封面 Header Block
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    market  = summary.get("market_data") or {}
    taiex   = market.get("taiex") or []
    inst    = market.get("institutional") or []
    futures = market.get("futures") or []
    tdcc    = market.get("tdcc") or []
    top_p   = summary.get("top_preview") or []
    ai_text = (summary.get("ai_analysis") or "").strip()

    status_color = C["green2"] if ok else C["red2"]
    status_txt   = "執行成功" if ok else "部分失敗"

    # 主標題區
    title_block = Table([
        [p("外資分點狙擊分析報告", fontName=FNB, fontSize=20, leading=26,
           textColor=C["white"]),
         p(f'<font color="{hex_of(status_color)}">{status_txt}</font>',
           fontName=FNB, fontSize=10, textColor=C["white"])],
        [p(f"近 {days} 日分析  ｜  {report_date}", fontName=FN, fontSize=9.5,
           textColor=HEX("#A9CCE3")),
         ""],
    ], colWidths=[CW*0.72, CW*0.28])
    title_block.setStyle(TableStyle([
        ("BACKGROUND",   (0,0),(-1,-1), C["navy"]),
        ("TOPPADDING",   (0,0),(-1,-1), 10),
        ("BOTTOMPADDING",(0,0),(-1,-1), 10),
        ("LEFTPADDING",  (0,0),(-1,-1), 14),
        ("RIGHTPADDING", (0,0),(-1,-1), 10),
        ("VALIGN",       (0,0),(-1,-1), "MIDDLE"),
        ("ALIGN",        (1,0),(-1,-1), "RIGHT"),
        ("SPAN",         (1,1),(-1,1)),
    ]))
    story.append(title_block)

    # 資訊列
    info_bar = Table([[
        p(f"產生時間：{gen_at}", fontName=FN, fontSize=8, textColor=HEX("#555")),
        p(f"資料筆數：{summary.get('total_rows',0):,}", fontName=FN, fontSize=8, textColor=HEX("#555")),
        p(f"券商 OK {summary.get('brokers_ok',0)} / FAIL {summary.get('brokers_fail',0)}",
          fontName=FN, fontSize=8, textColor=HEX("#555")),
    ]], colWidths=[CW*0.45, CW*0.25, CW*0.30])
    info_bar.setStyle(TableStyle([
        ("BACKGROUND",   (0,0),(-1,-1), HEX("#EAF0F6")),
        ("TOPPADDING",   (0,0),(-1,-1), 5),
        ("BOTTOMPADDING",(0,0),(-1,-1), 5),
        ("LEFTPADDING",  (0,0),(-1,-1), 8),
        ("LINEABOVE",    (0,0),(-1,0),  1, C["blue2"]),
        ("LINEBELOW",    (0,0),(-1,-1), 0.5, C["border"]),
        ("INNERGRID",    (0,0),(-1,-1), 0.3, C["border"]),
    ]))
    story.append(info_bar)
    story.append(Spacer(1, 4*mm))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  一、大盤環境速覽
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    story += sec_bar("一、大盤環境速覽", C["navy"], C["blue2"])

    # 大盤指數
    if taiex:
        story += sub_hdr("■ 大盤指數 ＋ 成交量（近 6 日）")
        amts = [d.get("amount_billion", 0) for d in taiex]
        avg5 = sum(amts[1:6]) / max(len(amts[1:6]), 1) if len(amts) > 1 else 0
        hdr  = [p("日期", fontName=FNB, fontSize=8, textColor=C["white"]),
                p("收盤指數", fontName=FNB, fontSize=8, textColor=C["white"]),
                p("漲跌點", fontName=FNB, fontSize=8, textColor=C["white"]),
                p("成交金額(億)", fontName=FNB, fontSize=8, textColor=C["white"]),
                p("量比(5日均)", fontName=FNB, fontSize=8, textColor=C["white"])]
        rows = [hdr]
        for i, d in enumerate(taiex[:6]):
            chg  = d.get("change", 0)
            cls  = d.get("close", 0)
            amt  = d.get("amount_billion", 0)
            sign = "+" if chg > 0 else ""
            chg_s = f"{sign}{chg:,.2f}" if isinstance(chg, float) else f"{sign}{chg}"
            ratio = f"{amt/avg5:.2f}x" if avg5 > 0 else "—"
            date_s = _fmt_date(d.get("date",""))
            rows.append([
                p("<b>" + date_s + "</b>" if i==0 else date_s, fontName=FNB if i==0 else FN, fontSize=8),
                p(f"{cls:,.2f}" if isinstance(cls,float) else str(cls), fontName=FNB if i==0 else FN, fontSize=8),
                pv(chg_s, bold=(i==0)),
                p(f"{int(amt):,}", fontName=FN, fontSize=8),
                p(ratio, fontName=FNB if i==0 else FN, fontSize=8),
            ])
        tbl = Table(rows, colWidths=[CW*0.18, CW*0.20, CW*0.18, CW*0.22, CW*0.22])
        ts  = ts_base(C["blue2"])
        today_row(ts)
        ts += [("ALIGN",(1,0),(-1,-1),"RIGHT")]
        tbl.setStyle(TableStyle(ts))
        story.append(tbl)

    # 三大法人
    if inst:
        story += sub_hdr("■ 三大法人買賣超（元，近 6 日）")
        hdr = [p(t, fontName=FNB, fontSize=8, textColor=C["white"]) for t in
               ["日期","外資買賣超","投信買賣超","自營買賣超","三大合計"]]
        rows = [hdr]
        for i, d in enumerate(inst[:EMAIL_TABLE_DAYS]):
            fg  = d["foreign"]["net"]
            tr  = d["trust"]["net"]
            dl  = d["dealer"]["net"]
            tot = d.get("total_net", fg+tr+dl)
            date_s = _fmt_date(d["date"])
            rows.append([
                p("<b>" + date_s + "</b>" if i==0 else date_s,
                  fontName=FNB if i==0 else FN, fontSize=8),
                pv(_fb(fg), bold=(i==0)), pv(_fb(tr), bold=(i==0)),
                pv(_fb(dl), bold=(i==0)), pv(_fb(tot), bold=True),
            ])
        tbl = Table(rows, colWidths=[CW*0.18]+[CW*0.205]*4)
        ts  = ts_base(C["blue"])
        today_row(ts)
        ts += [("ALIGN",(1,0),(-1,-1),"RIGHT"),
               ("LINEAFTER",(3,1),(3,-1),0.8,C["border"]),
               ("FONTNAME",(-1,1),(-1,-1),FNB)]  # 合計欄加粗
        tbl.setStyle(TableStyle(ts))
        story.append(tbl)

    # 期貨
    if futures:
        story += sub_hdr("■ 期貨三大法人台指期淨部位（口，近 6 日）")
        hdr = [p(t, fontName=FNB, fontSize=8, textColor=C["white"]) for t in
               ["日期","外資淨口數","投信淨口數","自營淨口數"]]
        rows = [hdr]
        for i, d in enumerate(futures[:EMAIL_TABLE_DAYS]):
            fg = d.get("foreign_net_oi", 0)
            tr = d.get("trust_net_oi", 0)
            dl = d.get("dealer_net_oi", 0)
            rows.append([
                p(_fmt_date(d.get("date","")), fontName=FNB if i==0 else FN, fontSize=8),
                pv(_fbi(fg), bold=(i==0)), pv(_fbi(tr), bold=(i==0)), pv(_fbi(dl), bold=(i==0)),
            ])
        tbl = Table(rows, colWidths=[CW*0.22]+[CW*0.26]*3)
        ts  = ts_base(C["purple"])
        today_row(ts)
        ts += [("ALIGN",(1,0),(-1,-1),"RIGHT")]
        tbl.setStyle(TableStyle(ts))
        story.append(tbl)
        note = p("＊ 正值=淨多單（看漲）｜負值=淨空單（看跌）｜自營商主要以選擇權避險，期貨部位通常接近 0",
                 fontName=FN, fontSize=7, textColor=HEX("#888"))
        story += [Spacer(1,1*mm), note]

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  二、外資分點明細
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if top_p:
        story += sec_bar(f"二、外資分點明細（淨超 Top {len(top_p)} 家）", C["navy"], C["blue2"])
        MEDALS = {1:"1.", 2:"2.", 3:"3."}
        for rank, block in enumerate(top_p, 1):
            broker    = block.get("broker","")
            total_net = block.get("total_net",0)
            rows_r    = block.get("rows") or []
            medal     = MEDALS.get(rank, f"{rank}.")
            nc        = hex_of(vc(total_net))

            # 券商 header bar
            broker_bar = Table([[
                p(f"{medal}  {broker}", fontName=FNB, fontSize=10.5,
                  leading=15, textColor=C["white"]),
                p(f'總淨超 <font color="{nc}"><b>{_fi(total_net)}</b></font> 張',
                  fontName=FN, fontSize=9, textColor=C["white"]),
            ]], colWidths=[CW*0.65, CW*0.35])
            broker_bar.setStyle(TableStyle([
                ("BACKGROUND",   (0,0),(-1,-1), C["blue"]),   # 深藍底
                ("TEXTCOLOR",    (0,0),(-1,-1), C["white"]),  # 強制白字（兜底）
                ("TOPPADDING",   (0,0),(-1,-1), 7),
                ("BOTTOMPADDING",(0,0),(-1,-1), 7),
                ("LEFTPADDING",  (0,0),(-1,-1), 12),
                ("RIGHTPADDING", (0,0),(-1,-1), 12),
                ("ALIGN",        (1,0),(-1,-1), "RIGHT"),
                ("VALIGN",       (0,0),(-1,-1), "MIDDLE"),
                ("LINEABOVE",    (0,0),(-1,-1), 2.5, C["blue2"]),  # 上方亮藍條
                ("LINEBELOW",    (0,0),(-1,-1), 0.5, C["border"]),
            ]))

            if not rows_r:
                story += [Spacer(1,2*mm), broker_bar]
                continue

            hdr = [p(t, fontName=FNB, fontSize=8, textColor=C["white"]) for t in
                   ["#","代號  股票名稱","淨超(張)","區間均價","現　　價","乖離率"]]
            tbl_data = [hdr]
            for j, r in enumerate(rows_r[:5], 1):
                bias_raw = str(r.get("bias","")).replace("%","").strip()
                try:
                    bv = float(bias_raw)
                    if bv > 5:    bc = hex_of(C["red2"])
                    elif bv > 1:  bc = hex_of(C["pos"])
                    elif bv < -5: bc = hex_of(C["green"])
                    elif bv < -1: bc = hex_of(C["neg"])
                    else:         bc = "#555555"
                    bias_p = p(f'<font color="{bc}"><b>{r.get("bias","")}</b></font>',
                               fontName=FN, fontSize=8)
                except Exception:
                    bias_p = p(str(r.get("bias","")), fontName=FN, fontSize=8)

                nv = r.get("net", 0)
                tbl_data.append([
                    p(str(j), fontName=FN, fontSize=8),
                    p(f'<font color="#1A3A5C"><b>{r.get("sid","")}</b></font>  {r.get("name","")}',
                      fontName=FN, fontSize=8, textColor=HEX("#222222")),
                    pv(_fi(nv), bold=True),
                    p(str(r.get("avg","")), fontName=FN, fontSize=8),
                    p(f'<b>{r.get("price","")}</b>', fontName=FNB, fontSize=8),
                    bias_p,
                ])
            tbl = Table(tbl_data, colWidths=[
                CW*0.05, CW*0.31, CW*0.14, CW*0.16, CW*0.16, CW*0.18
            ])
            ts = ts_base(HEX("#1A252F"))
            ts += [("ALIGN",(0,0),(0,-1),"CENTER"),
                   ("ALIGN",(2,0),(-1,-1),"RIGHT")]
            tbl.setStyle(TableStyle(ts))
            story += [Spacer(1,3*mm),
                      KeepTogether([broker_bar, tbl])]

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  三、千張大戶
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if tdcc:
        story += sec_bar("三、千張大戶持股比例", C["teal"], C["teal2"],
                         "集保中心資料｜法人籌碼集中度")
        story.append(Spacer(1, 2*mm))
        hdr = [p(t, fontName=FNB, fontSize=8, textColor=C["white"]) for t in
               ["股票代號","千張以上人數","持股比例","400~999張人數","400~999張佔比"]]
        tbl_data = [hdr]
        for d in tdcc:
            pct = d.get("pct_1000_plus", 0)
            pc  = hex_of(C["red2"] if pct>=60 else C["neg"] if pct<40 else HEX("#E67E22"))
            tbl_data.append([
                p(f'<b>{d.get("stock_id","")}</b>', fontName=FNB, fontSize=8),
                p(_fi(d.get("holders_1000_plus",0)), fontName=FN, fontSize=8),
                p(f'<font color="{pc}"><b>{pct:.1f}%</b></font>', fontName=FN, fontSize=8),
                p(_fi(d.get("holders_400_999",0)) if d.get("holders_400_999") else "—",
                  fontName=FN, fontSize=8),
                p(f'{d.get("pct_400_999",0):.1f}%' if d.get("pct_400_999") else "—",
                  fontName=FN, fontSize=8),
            ])
        tbl = Table(tbl_data, colWidths=[CW*0.18]+[CW*0.205]*4)
        ts = ts_base(C["teal"])
        ts += [("ALIGN",(1,0),(-1,-1),"RIGHT")]
        tbl.setStyle(TableStyle(ts))
        story.append(tbl)
        story.append(p("＊ 千張大戶持股 >60% = 籌碼高度集中（支撐強）｜<40% = 籌碼分散（浮額多）",
                       fontName=FN, fontSize=7, textColor=HEX("#888")))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  四、AI 深度分析
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    SEC_CFG = {
        "A": (C["blue"],   C["blue2"],   "A）大盤籌碼環境研判"),
        "B": (C["gold"],   C["gold2"],   "B）外資力量深度剖析"),
        "C": (C["green"],  C["green2"],  "C）明日觀察清單"),
        "D": (C["red"],    C["red2"],    "D）風控與資金配置"),
        "E": (C["purple"], C["purple2"], "E）一句話摘要"),
        "F": (C["blue"],   C["blue2"],   "F）外資交叉比對亮點"),
    }

    if ai_text:
        story.append(PageBreak())
        story += sec_bar("四、AI 深度分析（完整版）", C["navy"], C["blue2"])
        story.append(Spacer(1, 2*mm))

        cur_letter = None
        in_sec     = False

        for line in ai_text.split("\n"):
            s = line.strip()
            if not s:
                continue

            # A)~F) Section header
            hm = re.match(r'^([A-F])\s*[)）:：]\s*(.*)', s)
            if hm:
                letter    = hm.group(1)
                ai_title  = hm.group(2).strip().rstrip("：:").strip()
                cur_letter = letter
                bg, acc, default_lbl = SEC_CFG.get(letter, (C["blue"], C["blue2"], f"{letter}）"))
                lbl = f"{letter}）{ai_title}" if ai_title else default_lbl

                # E) 摘要：特大字體
                if letter == "E":
                    summary_txt = ai_title
                    sec_tbl = Table([[
                        p(lbl, fontName=FNB, fontSize=11, leading=16, textColor=C["white"]),
                    ]], colWidths=[CW])
                    sec_tbl.setStyle(TableStyle([
                        ("BACKGROUND",   (0,0),(-1,-1), bg),
                        ("LEFTPADDING",  (0,0),(-1,-1), 14),
                        ("TOPPADDING",   (0,0),(-1,-1), 8),
                        ("BOTTOMPADDING",(0,0),(-1,-1), 8),
                        ("LINEAFTER",    (0,0),(0,-1),  4, acc),
                    ]))
                    story += [Spacer(1,3.5*mm), sec_tbl]
                    if summary_txt:
                        story.append(p(
                            re.sub(r'\*\*(.+?)\*\*', r'\1', summary_txt),
                            fontName=FNB, fontSize=10.5, leading=16,
                            textColor=C["purple2"],
                            spaceBefore=4, spaceAfter=4, leftIndent=8,
                        ))
                    in_sec = True
                    continue

                sec_tbl = Table([[
                    p(lbl, fontName=FNB, fontSize=11, leading=16, textColor=C["white"]),
                ]], colWidths=[CW])
                sec_tbl.setStyle(TableStyle([
                    ("BACKGROUND",   (0,0),(-1,-1), bg),
                    ("LEFTPADDING",  (0,0),(-1,-1), 14),
                    ("TOPPADDING",   (0,0),(-1,-1), 7),
                    ("BOTTOMPADDING",(0,0),(-1,-1), 7),
                    ("LINEAFTER",    (0,0),(0,-1),  4, acc),
                ]))
                story += [Spacer(1,3.5*mm), sec_tbl, Spacer(1,1*mm)]
                in_sec = True
                continue

            # E) 下面的行（摘要文字）
            if cur_letter == "E":
                txt = re.sub(r'\*\*(.+?)\*\*', r'\1', s)
                story.append(p(txt, fontName=FNB, fontSize=10, leading=15,
                               textColor=C["purple2"],
                               spaceBefore=2, spaceAfter=2, leftIndent=8))
                continue

            # 前言（無區段）
            if not in_sec:
                txt = re.sub(r'\*\*(.+?)\*\*', r'\1', s)
                story.append(p(txt, fontName=FN, fontSize=8.5, leading=13,
                               textColor=HEX("#666")))
                continue

            # 編號項目 1) 2) 3)
            nm = re.match(r'^(\d+)\s*[)）.]\s*(.*)', s)
            if nm:
                num, txt = nm.group(1), nm.group(2)
                txt = re.sub(r'\*\*(.+?)\*\*', r'\1', txt)
                bg_cfg = SEC_CFG.get(cur_letter, (C["blue"], C["blue2"], ""))
                lt_bg  = {
                    "A": "#EBF5FB", "B": "#FEF9E7", "C": "#EAFAF1",
                    "D": "#FDEDEC", "E": "#F4ECF7", "F": "#EAF2FB",
                }.get(cur_letter, "#F8F9FA")
                num_c  = hex_of(bg_cfg[1])

                item_row = Table([[
                    p(f'<font color="{num_c}"><b>{num}</b></font>',
                      fontName=FNB, fontSize=9),
                    p(txt, fontName=FN, fontSize=8.5, leading=13),
                ]], colWidths=[8*mm, CW - 8*mm])
                item_row.setStyle(TableStyle([
                    ("BACKGROUND",   (0,0),(-1,-1), HEX(lt_bg)),
                    ("TOPPADDING",   (0,0),(-1,-1), 4),
                    ("BOTTOMPADDING",(0,0),(-1,-1), 4),
                    ("LEFTPADDING",  (0,0),(0,-1),  8),
                    ("LEFTPADDING",  (1,0),(1,-1),  4),
                    ("RIGHTPADDING", (0,0),(-1,-1), 6),
                    ("VALIGN",       (0,0),(-1,-1), "TOP"),
                    ("LINEBELOW",    (0,0),(-1,-1), 0.3, C["border"]),
                ]))
                story += [Spacer(1,0.5*mm), item_row]
                continue

            # 子項目 - / • / *
            sm = re.match(r'^[-•\*＊]\s*(.*)', s)
            if sm:
                txt = re.sub(r'\*\*(.+?)\*\*', r'\1', sm.group(1))
                story.append(p(f"• {txt}", fontName=FN, fontSize=8,
                               leading=12, textColor=HEX("#444"),
                               leftIndent=16, spaceBefore=1, spaceAfter=1))
                continue

            # 一般文字
            txt = re.sub(r'\*\*(.+?)\*\*', r'\1', s)
            story.append(p(txt, fontName=FN, fontSize=8.5, leading=13,
                           textColor=HEX("#333"),
                           leftIndent=4, spaceBefore=2, spaceAfter=2))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  免責聲明
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    story.append(Spacer(1, 8*mm))
    story.append(HRFlowable(width="100%", thickness=0.8,
                            color=C["gold2"], spaceAfter=3*mm))
    disc_tbl = Table([[
        p("【免責聲明】帶進帶出是違法的行為，此資料皆為網上根據盤後資訊大數據所取得訊息，"
          "非作為或被視為買進或售出標的的邀請或意象，請自行依據取得資訊評估風險與獲利，"
          "有賺有賠請斟酌。",
          fontName=FN, fontSize=7.5, leading=12, textColor=HEX("#7D5A00")),
    ]], colWidths=[CW])
    disc_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0),(-1,-1), HEX("#FFF9E6")),
        ("TOPPADDING",   (0,0),(-1,-1), 6),
        ("BOTTOMPADDING",(0,0),(-1,-1), 6),
        ("LEFTPADDING",  (0,0),(-1,-1), 10),
        ("RIGHTPADDING", (0,0),(-1,-1), 10),
        ("BOX",          (0,0),(-1,-1), 0.5, HEX("#F0D060")),
    ]))
    story.append(disc_tbl)

    doc.build(story)
    print(f"[OK] Analysis PDF: {pdf_path}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SMTP 發送
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _safe_int_env(name, default):
    v = (os.environ.get(name, "") or "").strip()
    try:
        return int(v) if v else default
    except Exception:
        return default


def main():
    smtp_host = (os.environ.get("SMTP_HOST", "smtp.gmail.com") or "").strip() or "smtp.gmail.com"
    smtp_port = _safe_int_env("SMTP_PORT", 587)
    smtp_user = (os.environ.get("SMTP_USER") or "").strip()
    smtp_pass = (os.environ.get("SMTP_PASS") or "").strip()
    mail_from = (os.environ.get("MAIL_FROM") or smtp_user).strip()
    mail_to   = (os.environ.get("MAIL_TO") or "").strip()
    mail_bcc  = (os.environ.get("MAIL_BCC") or "").strip()

    if not smtp_user or not smtp_pass:
        raise RuntimeError("SMTP_USER/SMTP_PASS 未設定")
    if not mail_to:
        raise RuntimeError("MAIL_TO 未設定")

    summary = load_summary()
    ymd     = datetime.now(TZ).strftime("%Y-%m-%d")
    subject = os.environ.get("MAIL_SUBJECT", f"【外資分點狙擊分析】{ymd}（TW 18:00）")

    xlsx = pick_latest(os.path.join("output", "IKE_Report_*.xlsx"))
    pdf  = pick_latest(os.path.join("output", "IKE_Report_*.pdf"))

    # ── 產生分析報告 PDF ──────────────────────────────
    analysis_pdf_path = os.path.join("output", f"TAIWAN外資分點狙擊分析報告_{datetime.now(TZ).strftime('%Y%m%d')}.pdf")
    try:
        build_analysis_pdf(summary, analysis_pdf_path)
    except Exception as e:
        print(f"[WARN] 分析 PDF 產生失敗（仍繼續寄信）: {e}")
        analysis_pdf_path = None

    msg             = EmailMessage()
    msg["From"]     = mail_from
    msg["To"]       = mail_to
    if mail_bcc:
        msg["Bcc"]  = mail_bcc
    msg["Subject"]  = subject

    msg.set_content(build_plain(summary))
    msg.add_alternative(build_html(summary), subtype="html")

    for fpath, mime in [
        (xlsx,             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        (pdf,              "application/pdf"),
        (analysis_pdf_path,"application/pdf"),
    ]:
        if fpath and os.path.exists(fpath):
            with open(fpath, "rb") as f:
                data = f.read()
            mt, st = mime.split("/", 1)
            msg.add_attachment(data, maintype=mt, subtype=st, filename=os.path.basename(fpath))

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)

    print("[OK] HTML Email sent.")


if __name__ == "__main__":
    main()
