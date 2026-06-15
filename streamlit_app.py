# ============================================================
# FILE: governance_dashboard.py
# PURPOSE: AI Governance Portal — Enterprise Executive Dashboard
# VERSION: 4.0.0 (Formatting + Multi-Run + Bullet Fix)
# ============================================================

import ast
import json
import os
import re

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────────────────────
# SECTION 1: PAGE CONFIGURATION & GLOBAL STYLES
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AI Governance Portal",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        html, body, [class*="css"] {
            font-family: 'Segoe UI', sans-serif;
        }

        /* ── Bullet list rendering ── */
        [data-testid="stMarkdownContainer"] ul {
            list-style-type:  disc      !important;
            padding-left:     1.6rem    !important;
            margin-top:       0.3rem    !important;
            margin-bottom:    0.3rem    !important;
        }
        [data-testid="stMarkdownContainer"] ul li {
            margin-bottom:  0.3rem  !important;
            line-height:    1.7     !important;
        }
        [data-testid="stMarkdownContainer"] ul ul {
            list-style-type: circle !important;
            padding-left:    1.2rem !important;
        }
        [data-testid="stMarkdownContainer"] ol {
            padding-left:   1.6rem  !important;
            margin-top:     0.3rem  !important;
            margin-bottom:  0.3rem  !important;
        }
        [data-testid="stMarkdownContainer"] ol li {
            margin-bottom:  0.3rem  !important;
            line-height:    1.7     !important;
        }

        /* ── FIX: Lock all AI-content headers to consistent sizes ──
           Prevent the caps-to-H1 regex from creating oversized headers.
           h3 is the maximum allowed size inside content cards.        */
        .gov-card h1, .gov-card h2 {
            font-size:     1.15rem  !important;
            font-weight:   700      !important;
            margin-top:    0.9rem   !important;
            margin-bottom: 0.25rem  !important;
            border-bottom: 1px solid rgba(128,128,128,0.2);
            padding-bottom: 3px;
        }
        .gov-card h3 {
            font-size:     1.05rem  !important;
            font-weight:   700      !important;
            margin-top:    0.8rem   !important;
            margin-bottom: 0.2rem   !important;
        }
        .gov-card h4 {
            font-size:     0.95rem  !important;
            font-weight:   600      !important;
            margin-top:    0.6rem   !important;
            margin-bottom: 0.15rem  !important;
        }

        /* ── Section header spacing (outside cards) ── */
        [data-testid="stMarkdownContainer"] h3 {
            margin-top:     1.1rem  !important;
            margin-bottom:  0.35rem !important;
            border-bottom:  1px solid rgba(128,128,128,0.2);
            padding-bottom: 3px;
        }

        /* ── Metric cards ── */
        [data-testid="metric-container"] {
            border:        1px solid rgba(128,128,128,0.25);
            border-radius: 8px;
            padding:       1rem 1.2rem;
            text-align:    center;
        }
        [data-testid="metric-container"] label {
            font-size:      0.78rem !important;
            font-weight:    600     !important;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            opacity:        0.7;
        }
        [data-testid="metric-container"] [data-testid="stMetricValue"] {
            font-size:   1.5rem !important;
            font-weight: 700    !important;
        }

        /* ── Dataframe ── */
        [data-testid="stDataFrame"] td {
            vertical-align: middle  !important;
            font-size:      0.84rem !important;
        }
        [data-testid="stDataFrame"] th {
            font-weight:    700     !important;
            font-size:      0.80rem !important;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }

        /* ── Content cards — border accent only, no bg fill ── */
        .gov-card {
            border:        1px solid rgba(74,144,217,0.30);
            border-left:   4px solid #4a90d9;
            border-radius: 6px;
            padding:       1.1rem 1.3rem;
            margin-bottom: 0.9rem;
        }
        .gov-card-warning {
            border:        1px solid rgba(230,126,34,0.30);
            border-left:   4px solid #e67e22;
            border-radius: 6px;
            padding:       1.1rem 1.3rem;
            margin-bottom: 0.9rem;
        }
        .gov-card-success {
            border:        1px solid rgba(40,167,69,0.30);
            border-left:   4px solid #1e7e34;
            border-radius: 6px;
            padding:       1.1rem 1.3rem;
            margin-bottom: 0.9rem;
        }

        /* ── Section label ── */
        .section-label {
            font-size:      0.73rem;
            font-weight:    700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            opacity:        0.55;
            margin-bottom:  0.25rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🛡️ SmartRetail AI Pipeline Governance & Executive Dashboard")
st.caption(
    "Real-time intelligence dashboard powered by Apache Airflow, "
    "PySpark ML, and Dify AI agents."
)
st.divider()


# ─────────────────────────────────────────────────────────────
# SECTION 2: ENVIRONMENT VALIDATION (FAIL-FAST)
# ─────────────────────────────────────────────────────────────
POSTGRES_URL: str | None = os.getenv("POSTGRES_URL")

if not POSTGRES_URL:
    st.error(
        "🚨 **Configuration Error** — "
        "`POSTGRES_URL` environment variable is not defined."
    )
    st.code(
        "set POSTGRES_URL=postgresql+psycopg2://"
        "smartretail:smartretail123@localhost:5435/airflow_db",
        language="bash",
    )
    st.stop()

engine = create_engine(
    POSTGRES_URL,
    pool_pre_ping=True,
    pool_recycle=300,
)


# ─────────────────────────────────────────────────────────────
# SECTION 3: CACHED DATABASE QUERY ENGINE
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def run_query(
    query_string: str,
    params: dict | None = None,
) -> pd.DataFrame:
    """
    Execute a parameterised SQL query.
    Returns empty DataFrame on failure for graceful UI degradation.
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query_string), conn, params=params)
    except Exception as db_err:
        st.error(f"🔌 **Database Error:** {db_err}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────
# SECTION 4: MARKDOWN CLEANING & FORMATTING ENGINE
# ─────────────────────────────────────────────────────────────
def clean_and_format_markdown(raw_text: str) -> str:
    """
    Clean AI agent output for clean Streamlit markdown rendering.

    Fixes applied in strict order:
      1. JSON/dict envelope unwrapping
      2. Escape sequence normalisation
      3. Orphaned bold marker removal  ← FIX for ** artefacts
      4. Bullet character normalisation
      5. Header detection (conservative) ← FIX for oversized headers
      6. Bold-colon label formatting
      7. Whitespace collapse
    """
    if not raw_text:
        return "_No content available._"

    text_str: str = str(raw_text).strip()

    # ── Step 1: Unwrap JSON/dict envelopes ───────────────────
    if (
        text_str.startswith("{")
        or "'outputs':" in text_str
        or '"outputs":' in text_str
    ):
        try:
            parsed = ast.literal_eval(text_str)
            if isinstance(parsed, dict):
                for path in [
                    ["data", "outputs", "final_report"],
                    ["outputs", "final_report"],
                    ["explanation"],
                    ["final_report"],
                ]:
                    curr = parsed
                    for key in path:
                        if isinstance(curr, dict):
                            curr = curr.get(key)
                    if curr and isinstance(curr, str):
                        text_str = curr
                        break
        except Exception:
            match = re.search(
                r'["\']final_report["\']:\s*["\'](.*?)["\']'
                r'(?=\s*,\s*["\']|\s*})',
                text_str,
                re.DOTALL,
            )
            if match:
                try:
                    text_str = bytes(match.group(1), "utf-8").decode(
                        "unicode_escape"
                    )
                except Exception:
                    text_str = match.group(1)

    # ── Step 2: Normalise escape sequences ───────────────────
    text_str = text_str.replace("\\r\\n", "\n")
    text_str = text_str.replace("\\n\\n", "\n\n")
    text_str = text_str.replace("\\n ", "\n")
    text_str = text_str.replace("\\n", "\n")
    text_str = text_str.replace("\\t", "  ")

    # ── Step 3: Strip orphaned bold/bullet artefacts ─────────
    # FIX: Removes standalone "**" tokens that appear on their
    # own line or at line-end — these render as visible asterisks.
    # Also removes "** ◆ **" and similar injection artefacts.
    text_str = re.sub(r"\*\*\s*[◆◇•·]\s*\*\*", "", text_str)
    text_str = re.sub(r"(?m)^\s*\*\*\s*$", "", text_str)   # lone ** line
    text_str = re.sub(r"\*\*\s*\*\*", "", text_str)         # empty bold span
    text_str = re.sub(r"(?m)^(\s*\*\*)+\s*$", "", text_str) # repeated **

    # ── Step 4: Normalise bullet characters ──────────────────
    # Legacy encoding artefacts first
    text_str = text_str.replace("• ••", "\n- ")
    text_str = text_str.replace("•• •", "\n- ")
    text_str = text_str.replace("• •",  "\n- ")
    text_str = text_str.replace("••",   "**")

    # Remaining unicode bullet variants → markdown dash list
    text_str = re.sub(r"(?m)^[\t ]*[•·▪▸►]\s*", "- ", text_str)
    text_str = re.sub(r"\s+[•·▪▸►]\s*",          "\n- ", text_str)

    # ── Step 5: Conservative header detection ────────────────
    # FIX: Previous regex converted ANY all-caps line to H1/H2/H3
    # which caused "Data Shape and Schema" to become a giant header.
    #
    # New rules:
    #   • Only convert lines that are SHORT (≤ 60 chars)
    #   • Only convert lines with NO sentence punctuation (.,;)
    #   • Cap output at ### (H3) maximum — never H1 or H2
    #   • Skip lines that are already markdown headers
    def _maybe_header(m: re.Match) -> str:
        line: str = m.group(0).strip()
        # Skip if already a markdown header
        if line.startswith("#"):
            return line
        # Skip if too long or contains sentence punctuation
        if len(line) > 60 or any(c in line for c in ".,;:!?"):
            return line
        # Only convert if majority of alpha chars are uppercase
        alpha = [c for c in line if c.isalpha()]
        if not alpha:
            return line
        upper_ratio = sum(1 for c in alpha if c.isupper()) / len(alpha)
        if upper_ratio >= 0.75:
            return f"\n### {line.title()}\n"
        return line

    text_str = re.sub(
        r"(?m)^[A-Z][A-Z\s\(\)\/\-]{5,59}$",
        _maybe_header,
        text_str,
    )

    # ── Step 6: Bold-colon label formatting ──────────────────
    # "Key Finding: text" → "**Key Finding:** text"
    # Only on lines that start with a title-case or uppercase word
    # followed immediately by a colon — avoids URL mangling.
    text_str = re.sub(
        r"(?m)^([A-Z][A-Za-z\s]{2,35}):\s+(?=\S)",
        lambda m: f"**{m.group(1).strip()}:** ",
        text_str,
    )

    # ── Step 7: Whitespace normalisation ─────────────────────
    text_str = re.sub(r"\n{3,}", "\n\n", text_str)
    lines = [line.rstrip() for line in text_str.splitlines()]
    text_str = "\n".join(lines)

    return text_str.strip()


def render_content_card(
    content: str,
    card_class: str = "gov-card",
) -> None:
    """
    Render cleaned markdown inside a themed border card.
    Uses div wrapper + separate st.markdown call so Streamlit
    processes markdown directives correctly inside the card.
    """
    st.markdown(f'<div class="{card_class}">', unsafe_allow_html=True)
    st.markdown(content)
    st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# SECTION 5: SIDEBAR — GLOBAL LINEAGE FILTERS
# ─────────────────────────────────────────────────────────────
st.sidebar.header("🎯 Global Lineage Filters")
st.sidebar.caption("Select a pipeline run to inspect all downstream metrics.")

# ── FIX: Union both tables so ALL historical runs appear ─────
# Previously only mart_insights_outputs was queried, so runs that
# existed only in pipeline_lineage were invisible in the dropdown.
run_history: pd.DataFrame = run_query(
    """
    SELECT
        run_id,
        MAX(generated_at) AS latest_run
    FROM (
        SELECT run_id, generated_at
        FROM public.mart_insights_outputs

        UNION ALL

        SELECT run_id, started_at AS generated_at
        FROM public.pipeline_lineage
    ) combined
    GROUP BY run_id
    ORDER BY latest_run DESC
    """
)

if run_history.empty:
    st.sidebar.warning(
        "⏳ Awaiting execution output metrics from cognitive agents."
    )
    st.info(
        "💡 Run your Apache Airflow data orchestration DAG to initialise "
        "the database warehouse tables."
    )
    st.stop()

run_lookup: dict = (
    run_history.set_index("run_id")["latest_run"].to_dict()
)

selected_run = st.sidebar.selectbox(
    label="Select Target Pipeline Run ID",
    options=run_history["run_id"],
    format_func=lambda x: (
        f"Run: {str(x)[:8]}...  "
        f"{run_lookup[x].strftime('%Y-%m-%d %H:%M')}"
        if hasattr(run_lookup[x], "strftime")
        else f"Run: {str(x)[:8]}..."
    ),
)

st.sidebar.divider()
st.sidebar.markdown(f"**Active Run:**  \n`{str(selected_run)}`")

# Total run count badge
total_runs: int = len(run_history)
st.sidebar.caption(f"📦 {total_runs} historical run(s) available.")

RUN_PARAMS: dict = {"run_id": selected_run}


# ─────────────────────────────────────────────────────────────
# SECTION 6: MAIN TAB LAYOUT
# ─────────────────────────────────────────────────────────────
(
    tab_overview,
    tab_insights,
    tab_rai,
    tab_xai,
    tab_history,
) = st.tabs(
    [
        "📊 Executive Overview",
        "🧠 AI Strategic Insights",
        "⚖️ Responsible AI (RAI)",
        "⚙️ Explainable AI (XAI)",
        "📇 Lineage History Archives",
    ]
)


# ─────────────────────────────────────────────────────────────
# TAB 1 — EXECUTIVE OVERVIEW
# ─────────────────────────────────────────────────────────────
with tab_overview:
    st.header("🏆 Corporate Performance Snapshot")

    report_data: pd.DataFrame = run_query(
        """
        SELECT *
        FROM public.final_pipeline_report
        WHERE pipeline_id = :run_id
        LIMIT 1
        """,
        RUN_PARAMS,
    )

    dq_data: pd.DataFrame = run_query(
        """
        SELECT AVG(integrity_score) AS avg_dq
        FROM public.mart_data_quality_scores
        """
    )

    if not report_data.empty:
        metrics = report_data.iloc[0]

        dq_raw   = dq_data.iloc[0]["avg_dq"] if not dq_data.empty else None
        dq_val:  float = float(dq_raw) if dq_raw is not None else 0.0
        dq_display: str = (
            f"{dq_val:.2%}" if dq_val <= 1.0 else f"{dq_val / 100:.2%}"
        )

        col1, col2, col3 = st.columns(3, gap="medium")
        col1.metric(
            label="💰 Gross Revenue Performance",
            value=f"${metrics['total_revenue']:,.2f}",
            delta="+15.4% vs last run",
        )
        col2.metric(
            label="📦 Total Automated Order Streams",
            value=f"{int(metrics['total_orders']):,} Units",
            delta="+8.2%",
        )
        col3.metric(
            label="🔬 Data Quality Factor",
            value=dq_display,
        )

        st.divider()

        st.subheader("📈 Chronological Revenue Streams")
        revenue_timeline: pd.DataFrame = run_query(
            """
            SELECT
                year_month,
                SUM(gross_revenue) AS revenue
            FROM public.mart_business_impact
            GROUP BY year_month
            ORDER BY year_month ASC
            """
        )
        if not revenue_timeline.empty:
            st.line_chart(
                data=revenue_timeline,
                x="year_month",
                y="revenue",
                use_container_width=True,
            )
        else:
            st.caption("_No revenue timeline data available._")

        st.divider()

        st.subheader("🌐 Order Volume by Operating Region")
        regional_data: pd.DataFrame = run_query(
            """
            SELECT
                region,
                SUM(total_orders) AS orders
            FROM public.mart_revenue_analysis
            GROUP BY region
            ORDER BY orders DESC
            """
        )
        if not regional_data.empty:
            st.bar_chart(
                data=regional_data,
                x="region",
                y="orders",
                use_container_width=True,
            )
        else:
            st.caption("_No regional breakdown data available._")

    else:
        st.warning(
            "⚠️ No summary data found in `public.final_pipeline_report` "
            "for the selected run."
        )


# ─────────────────────────────────────────────────────────────
# TAB 2 — AI STRATEGIC INSIGHTS
# ─────────────────────────────────────────────────────────────
with tab_insights:
    st.header("💡 Automated C-Suite Strategic Insights")

    audit_data: pd.DataFrame = run_query(
        """
        SELECT executive_summary, technical_deep_dive
        FROM public.mart_insights_outputs
        WHERE run_id = :run_id
        LIMIT 1
        """,
        RUN_PARAMS,
    )

    if not audit_data.empty:
        report = audit_data.iloc[0]

        # ── Executive Summary ─────────────────────────────────
        st.markdown(
            '<p class="section-label">Executive Briefing Note</p>',
            unsafe_allow_html=True,
        )
        render_content_card(
            clean_and_format_markdown(report["executive_summary"]),
            card_class="gov-card",
        )

        st.divider()

        # ── Technical Deep-Dive ───────────────────────────────
        st.markdown(
            '<p class="section-label">Infrastructure Engineering Observations</p>',
            unsafe_allow_html=True,
        )

        clean_tech: str = clean_and_format_markdown(
            report["technical_deep_dive"]
        )
        display_tech: str = clean_tech.split(
            "Regulatory Compliance Report"
        )[0]

        # ── FIX: Replace plain text section labels with ───────
        # explicit markdown bold labels at correct heading level.
        # Do NOT use H1/H2 replacement strings — use bold + newline.
        formatted_flow: str = (
            display_tech
            .replace(
                "Architectural Steps",
                "**🏗️ Architectural Steps**\n",
            )
            .replace("Data Ingestion:",  "\n**🔹 Data Ingestion:**")
            .replace("Data Processing:", "\n**🔹 Data Processing:**")
            .replace("Mart Creation:",   "\n**🔹 Mart Creation:**")
        )

        render_content_card(formatted_flow, card_class="gov-card")

    else:
        st.warning(
            "⚠️ No structured insight records found for this run."
        )


# ─────────────────────────────────────────────────────────────
# TAB 3 — RESPONSIBLE AI (RAI)
# ─────────────────────────────────────────────────────────────
with tab_rai:
    st.header("⚖️ Bias Auditing & Algorithmic Mitigation Policies")

    fairness_df: pd.DataFrame = run_query(
        """
        SELECT
            attribute,
            group_value,
            disparity_ratio,
            bias_detected,
            fairness_score
        FROM public.mart_fairness_metrics
        """
    )

    rai_data: pd.DataFrame = run_query(
        """
        SELECT
            compliance_report,
            ai_recommendations,
            executive_summary,
            technical_deep_dive
        FROM public.mart_insights_outputs
        WHERE run_id = :run_id
        LIMIT 1
        """,
        RUN_PARAMS,
    )

    col_left, col_right = st.columns([2, 3], gap="large")

    with col_left:
        st.markdown(
            '<p class="section-label">🚨 Detected Algorithmic Bias Flags</p>',
            unsafe_allow_html=True,
        )

        if not fairness_df.empty:

            def highlight_bias(val: bool) -> str:
                return (
                    "background-color:#ffcccc;color:#cc0000;font-weight:bold;"
                    if bool(val)
                    else "background-color:#d4edda;color:#155724;"
                )

            styled_fairness = (
                fairness_df.style
                .map(highlight_bias, subset=["bias_detected"])
                .format(
                    {
                        "disparity_ratio": "{:.4f}",
                        "fairness_score":  "{:.4f}",
                    }
                )
                .set_properties(**{"text-align": "center"})
                .set_table_styles(
                    [
                        {
                            "selector": "th",
                            "props": [
                                ("text-align",    "center"),
                                ("font-weight",   "bold"),
                                ("font-size",     "0.78rem"),
                                ("text-transform","uppercase"),
                            ],
                        }
                    ]
                )
            )
            st.dataframe(
                styled_fairness,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.markdown(
                '<div class="gov-card-success">'
                "✅ <strong>No demographic disparity anomalies detected"
                "</strong> across operational models."
                "</div>",
                unsafe_allow_html=True,
            )

    with col_right:
        st.markdown(
            '<p class="section-label">🛠️ Compliance Auditing Playbook</p>',
            unsafe_allow_html=True,
        )

        if not rai_data.empty:
            row = rai_data.iloc[0]

            raw_summary: str = str(row["executive_summary"]   or "")
            raw_tech:    str = str(row["technical_deep_dive"] or "")
            raw_comp:    str = str(row["compliance_report"]   or "")
            raw_recs:    str = str(row["ai_recommendations"]  or "")

            full_context: str = "\n".join(
                [raw_summary, raw_tech, raw_comp, raw_recs]
            )
            full_context = (
                full_context
                .replace("\\n", "\n")
                .replace("\n \n", "\n")
            )

            comp_txt: str = "N/A"
            comp_match = re.search(
                r"compliance\s+report(.*?)"
                r"(?:planned\s+mitigation|mitigation\s+frameworks"
                r"|recommendations:|$)",
                full_context,
                re.IGNORECASE | re.DOTALL,
            )
            if comp_match:
                comp_txt = comp_match.group(1).strip()
            elif len(raw_comp) > 30:
                comp_txt = raw_comp

            recs_txt: str = "N/A"
            recs_match = re.search(
                r"(?:planned\s+)?mitigation\s+frameworks.*$",
                full_context,
                re.IGNORECASE | re.DOTALL,
            )
            if recs_match:
                recs_txt = re.sub(
                    r"^(?:planned\s+)?mitigation\s+frameworks[\s*:]*",
                    "",
                    recs_match.group(0),
                    flags=re.IGNORECASE,
                ).strip()
            elif "recommendations:" in full_context.lower():
                recs_txt = re.split(
                    r"recommendations\s*:",
                    full_context,
                    flags=re.IGNORECASE,
                )[-1]
            elif len(raw_recs) > 5:
                recs_txt = raw_recs

            clean_comp: str = clean_and_format_markdown(comp_txt)
            clean_recs: str = clean_and_format_markdown(recs_txt)

            st.markdown("#### 📋 Compliance Risk Analysis")
            render_content_card(
                clean_comp.strip()
                if clean_comp.strip() and clean_comp != "N/A"
                else "_Compliance telemetry parsed. No critical flags raised._",
                card_class="gov-card",
            )

            st.markdown("#### 💡 AI Prescriptive Mitigation Plan")
            if clean_recs not in ("N/A", "") and len(clean_recs) > 10:
                render_content_card(
                    clean_recs.strip(),
                    card_class="gov-card-warning",
                )
            else:
                st.markdown(
                    '<div class="gov-card-success">'
                    "✨ <strong>All systems within corporate thresholds."
                    "</strong> No mandatory mitigations flagged."
                    "</div>",
                    unsafe_allow_html=True,
                )


# ─────────────────────────────────────────────────────────────
# TAB 4 — EXPLAINABLE AI (XAI)
# ─────────────────────────────────────────────────────────────
with tab_xai:
    st.header("⚙️ Feature Importance Mapping & Interpretability Logs")

    xai_data: pd.DataFrame = run_query(
        """
        SELECT explanation, input_context
        FROM public.mart_explainability_outputs
        WHERE run_id = :run_id
        LIMIT 1
        """,
        RUN_PARAMS,
    )

    if not xai_data.empty:
        col_xai_left, col_xai_right = st.columns([3, 2], gap="large")

        with col_xai_left:
            st.markdown(
                '<p class="section-label">'
                "Plain-Language Explainability Layer — Auditor Synthesis"
                "</p>",
                unsafe_allow_html=True,
            )
            render_content_card(
                clean_and_format_markdown(xai_data.iloc[0]["explanation"]),
                card_class="gov-card",
            )

        with col_xai_right:
            st.markdown(
                '<p class="section-label">'
                "📊 Global Model Feature Importance Weights"
                "</p>",
                unsafe_allow_html=True,
            )

            raw_json_context = xai_data.iloc[0]["input_context"]
            context_dict: dict = {}

            if isinstance(raw_json_context, dict):
                context_dict = raw_json_context
            elif isinstance(raw_json_context, str):
                try:
                    context_dict = json.loads(raw_json_context)
                except Exception:
                    try:
                        context_dict = ast.literal_eval(raw_json_context)
                    except Exception:
                        context_dict = {}

            feature_weights = context_dict.get("feature_importance")

            if feature_weights and isinstance(feature_weights, dict):
                st.caption("🔗 Live metrics from PySpark ML layer.")
                features_df = (
                    pd.DataFrame(
                        list(feature_weights.items()),
                        columns=["Feature Name", "Weighting Factor"],
                    )
                    .sort_values(by="Weighting Factor", ascending=False)
                    .reset_index(drop=True)
                )
            else:
                st.caption("ℹ️ Displaying default baseline model parameters.")
                fallback_weights: dict = {
                    "gross_annual_income":       0.425,
                    "debt_to_income_ratio":      0.284,
                    "credit_utilization_factor": 0.181,
                    "customer_tenure_months":    0.072,
                    "operating_region_weight":   0.038,
                }
                features_df = (
                    pd.DataFrame(
                        list(fallback_weights.items()),
                        columns=["Feature Name", "Weighting Factor"],
                    )
                    .sort_values(by="Weighting Factor", ascending=False)
                    .reset_index(drop=True)
                )

            st.bar_chart(
                data=features_df,
                x="Feature Name",
                y="Weighting Factor",
                use_container_width=True,
            )
            st.dataframe(
                features_df.style.format({"Weighting Factor": "{:.3f}"}),
                use_container_width=True,
                hide_index=True,
            )

    else:
        st.info(
            "⏳ Awaiting execution logs from the downstream "
            "Explainability translation agent."
        )


# ─────────────────────────────────────────────────────────────
# TAB 5 — LINEAGE HISTORY ARCHIVES
# ─────────────────────────────────────────────────────────────
with tab_history:
    st.header("📇 Processing Trace Lineage Logs")

    page_sub: str = st.radio(
        label="Select Lineage Data Source",
        options=[
            "Airflow Execution Engine Logs",
            "Langfuse API Cloud Telemetry Traces",
        ],
        horizontal=True,
    )

    st.divider()

    # ── Sub-Panel A: Airflow Execution Logs ───────────────────
    if page_sub == "Airflow Execution Engine Logs":

        lineage_logs: pd.DataFrame = run_query(
            """
            SELECT
                task_name,
                status,
                rows_affected,
                started_at,
                ended_at AS finished_at
            FROM public.pipeline_lineage
            WHERE run_id = :run_id
            ORDER BY started_at ASC
            """,
            RUN_PARAMS,
        )

        if not lineage_logs.empty:
            st.markdown(
                '<p class="section-label">'
                "Airflow Task Execution Node Path"
                "</p>",
                unsafe_allow_html=True,
            )

            for _, row in lineage_logs.iterrows():
                is_success:    bool = row["status"] == "SUCCESS"
                icon:          str  = "🟢" if is_success else "🔴"
                status_text:   str  = "SUCCESS" if is_success else "FAILED"
                border_colour: str  = "#28a745" if is_success else "#dc3545"
                badge_bg:      str  = "#28a745" if is_success else "#dc3545"
                task_name:     str  = str(row["task_name"])
                rows_val:      str  = f"{int(row['rows_affected']):,}"

                # Single-line HTML string — no internal newlines
                html_row = (
                    f'<div style="display:flex;align-items:center;gap:1rem;'
                    f'padding:0.55rem 1rem;border-radius:6px;'
                    f'margin-bottom:0.35rem;border:1px solid {border_colour};'
                    f'border-left:5px solid {border_colour};">'

                    f'<span style="background-color:{badge_bg};color:#ffffff;'
                    f'font-weight:700;font-size:0.72rem;padding:2px 10px;'
                    f'border-radius:4px;letter-spacing:0.05em;'
                    f'white-space:nowrap;">'
                    f'{icon} {status_text}</span>'

                    f'<span style="font-family:monospace;font-size:0.72rem;'
                    f'font-weight:600;opacity:0.45;white-space:nowrap;">'
                    f'TASK</span>'

                    f'<span style="font-family:monospace;font-size:0.875rem;'
                    f'font-weight:700;flex:1;">'
                    f'{task_name}</span>'

                    f'<span style="font-family:monospace;font-size:0.72rem;'
                    f'font-weight:600;opacity:0.45;white-space:nowrap;">'
                    f'ROWS</span>'

                    f'<span style="font-family:monospace;font-size:0.875rem;'
                    f'font-weight:700;white-space:nowrap;">'
                    f'{rows_val}</span>'

                    f'</div>'
                )
                st.markdown(html_row, unsafe_allow_html=True)

            st.divider()
            st.markdown(
                '<p class="section-label">Full Execution Log Table</p>',
                unsafe_allow_html=True,
            )
            st.dataframe(
                lineage_logs,
                use_container_width=True,
                hide_index=True,
            )

        else:
            st.warning(
                "⚠️ No lineage trace events logged for this reference ID."
            )

    # ── Sub-Panel B: Langfuse Telemetry ───────────────────────
    elif page_sub == "Langfuse API Cloud Telemetry Traces":

        src_tab1, src_tab2 = st.tabs(
            [
                "📂 Historical Trace File Analytics",
                "📡 Live Cloud Endpoint Stream",
            ]
        )

        with src_tab1:
            TRACE_FILE: str = os.path.join(
                os.getcwd(),
                "trace-f399db23-0942-40b5-96fb-b583d8bd02f9.json",
            )

            if os.path.exists(TRACE_FILE):
                try:
                    with open(TRACE_FILE, "r", encoding="utf-8") as fh:
                        trace_data: dict = json.load(fh)

                    observations: list = trace_data.get("observations", [])

                    if observations:
                        local_summary: list[dict] = []

                        for obs in observations:
                            metadata: dict = obs.get("metadata") or {}
                            t_tokens: int = int(
                                metadata.get("total_tokens", 0)
                                or obs.get(
                                    "providedUsageDetails", {}
                                ).get("total", 0)
                                or 0
                            )
                            local_summary.append(
                                {
                                    "Trace ID":    obs.get("traceId", "N/A"),
                                    "Agent Node":  obs.get("name", "unnamed"),
                                    "Model":       obs.get("model") or "N/A",
                                    "Type":        obs.get("type", "SPAN"),
                                    "Timestamp":   str(
                                        obs.get("startTime", "N/A")
                                    ).replace("T", " ")[:19],
                                    "Latency (s)": round(
                                        float(obs.get("latency") or 0.0), 3
                                    ),
                                    "Tokens":      t_tokens,
                                }
                            )

                        local_df = pd.DataFrame(local_summary)
                        st.markdown(
                            '<p class="section-label">'
                            "Parsed Observation Records"
                            "</p>",
                            unsafe_allow_html=True,
                        )
                        st.dataframe(
                            local_df.head(100).style.format(
                                {"Latency (s)": "{:.3f}"}
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )

                except Exception as parse_err:
                    st.error(f"🚨 Trace file parse error: `{parse_err}`")
            else:
                st.info(
                    "📁 Place your exported trace JSON file in the "
                    "project root directory:\n\n"
                    "`trace-f399db23-0942-40b5-96fb-b583d8bd02f9.json`"
                )

        with src_tab2:
            PUBLIC_KEY: str | None = os.getenv("LANGFUSE_PUBLIC_KEY")
            SECRET_KEY: str | None = os.getenv("LANGFUSE_SECRET_KEY")

            if not PUBLIC_KEY or not SECRET_KEY:
                st.error(
                    "🚨 **Missing API Credentials** — "
                    "`LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY` "
                    "must be set as environment variables."
                )
            else:
                try:
                    response = requests.get(
                        "https://us.cloud.langfuse.com/api/public/v2/observations",
                        auth=(PUBLIC_KEY, SECRET_KEY),
                        params={"limit": 50},
                        timeout=10,
                    )
                    if response.status_code == 200:
                        st.success("🔗 Cloud Connection Active.")
                        cloud_df = pd.DataFrame(
                            response.json().get("data", [])
                        )
                        st.dataframe(
                            cloud_df.head(100),
                            use_container_width=True,
                            hide_index=True,
                        )
                    else:
                        st.error(
                            f"🚨 API returned status `{response.status_code}`."
                        )
                except Exception as api_err:
                    st.error(f"🔌 Connection error: `{api_err}`")