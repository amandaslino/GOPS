from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

import pandas as pd
import requests
import streamlit as st
from requests.auth import HTTPBasicAuth

# ─────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────
PAGE_SIZE = 50
REQUEST_TIMEOUT = 30
SERVICENOW_RE = re.compile(r"\b(RITM\d+|INC\d+)\b", re.IGNORECASE)
DEFAULT_WARNING_DAYS = 7

SEMAPHORE_EMOJI = {
    "VERDE": "🟢",
    "AMARELO": "🟡",
    "VERMELHO": "🔴",
    "SEM DATA": "⚪",
}

# ─────────────────────────────────────────────
# Utilitários
# ─────────────────────────────────────────────
def to_ascii(value: str | None) -> str:
    return unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")

def normalize_text(value: str | None) -> str:
    return to_ascii(value).lower().strip()

def parse_date(value: Any) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw_value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(raw_value, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).date()
    except ValueError:
        return None

def format_date(value: date | None) -> str:
    return value.strftime("%d/%m/%Y") if value else "-"

def format_datetime(value: Any) -> str:
    return format_date(parse_date(value))

def pick_display_name(user_data: dict[str, Any] | None) -> str:
    if not user_data:
        return "Sem responsável"
    return (
        user_data.get("displayName")
        or user_data.get("emailAddress")
        or user_data.get("name")
        or user_data.get("accountId")
        or "Sem responsável"
    )

def get_status_name(fields: dict[str, Any]) -> str:
    return ((fields.get("status") or {}).get("name")) or "Sem status"

def extract_service_now_ref(summary: str) -> str:
    match = SERVICENOW_RE.search(summary or "")
    return match.group(1).upper() if match else "-"

def is_done_issue(fields: dict[str, Any]) -> bool:
    status = fields.get("status") or {}
    done_names = {"done", "concluido", "concluida", "fechado", "resolvido", "resolved"}
    return normalize_text((status.get("statusCategory") or {}).get("key")) == "done" or normalize_text(
        status.get("name")
    ) in done_names

def extract_due_date(fields: dict[str, Any], due_field_id: str | None) -> date | None:
    for field_name in [due_field_id, "duedate"]:
        if not field_name:
            continue
        parsed = parse_date(fields.get(field_name))
        if parsed:
            return parsed
    return None

def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]

# ─────────────────────────────────────────────
# Config via Secrets
# ─────────────────────────────────────────────
@dataclass
class Config:
    base_url: str
    email: str
    api_token: str
    scope_names: list[str]
    warning_days: int
    deadline_field: str | None
    board_ids: list[str]

    @classmethod
    def from_secrets(cls) -> "Config":
        try:
            jira = st.secrets["jira"]
            return cls(
                base_url=jira["base_url"].rstrip("/"),
                email=jira["email"],
                api_token=jira["api_token"],
                scope_names=split_csv(jira.get("scope_names", "")),
                warning_days=int(jira.get("warning_days", DEFAULT_WARNING_DAYS)),
                deadline_field=jira.get("deadline_field", "").strip() or None,
                board_ids=split_csv(jira.get("board_ids", "")),
            )
        except KeyError as e:
            st.error(f"Secret ausente na configuração do Streamlit: {e}. Verifique o arquivo secrets.toml.")
            st.stop()

# ─────────────────────────────────────────────
# JiraClient
# ─────────────────────────────────────────────
class JiraClient:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(config.email, config.api_token)
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        self._field_cache: list[dict[str, Any]] | None = None
        self._search_mode: str | None = None

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.session.request(
            method=method,
            url=f"{self.config.base_url}{path}",
            timeout=REQUEST_TIMEOUT,
            **kwargs,
        )
        if response.status_code >= 400:
            try:
                payload = response.json()
                message = payload.get("errorMessages") or payload.get("message") or payload.get("errors") or ""
            except ValueError:
                message = response.text.strip()
            raise RuntimeError(f"Erro Jira {response.status_code} em {path}: {message}")
        return None if response.status_code == 204 or not response.text.strip() else response.json()

    def paged_get(self, path: str, params: dict[str, Any], list_key: str = "issues") -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        start_at = 0
        while True:
            payload = self.request("GET", path, params={**params, "startAt": start_at, "maxResults": PAGE_SIZE})
            batch = payload.get(list_key, [])
            items.extend(batch)
            start_at += len(batch)
            if start_at >= payload.get("total", 0):
                return items

    def get_all_fields(self) -> list[dict[str, Any]]:
        if self._field_cache is None:
            try:
                self._field_cache = self.request("GET", "/rest/api/3/field")
            except RuntimeError:
                self._field_cache = self.request("GET", "/rest/api/2/field")
        return self._field_cache

    def resolve_deadline_field(self) -> str | None:
        if self.config.deadline_field:
            return self.config.deadline_field
        candidate_names = {
            normalize_text(n) for n in [
                "duedate", "due date", "data de vencimento", "data fim",
                "data final", "prazo", "fim previsto", "target end", "target end date", "end date",
            ]
        }
        for field in self.get_all_fields():
            if field.get("id") == "duedate":
                return "duedate"
            if normalize_text(field.get("name")) in candidate_names:
                return field.get("id")
        return "duedate"

    def iter_projects(self) -> Iterable[dict[str, Any]]:
        start_at = 0
        while True:
            try:
                payload = self.request("GET", "/rest/api/3/project/search", params={"startAt": start_at, "maxResults": PAGE_SIZE})
            except RuntimeError:
                payload = self.request("GET", "/rest/api/2/project/search", params={"startAt": start_at, "maxResults": PAGE_SIZE})
            for project in payload.get("values", []):
                yield project
            page_size = payload.get("maxResults", PAGE_SIZE)
            total = payload.get("total", 0)
            if payload.get("isLast") or start_at + page_size >= total:
                return
            start_at += page_size

    def find_scope_projects(self) -> list[dict[str, Any]]:
        normalized_scopes = [normalize_text(item) for item in self.config.scope_names if item.strip()]

        # Se scope_names vazio, retorna todos os projetos automaticamente
        if not normalized_scopes:
            return list(self.iter_projects())

        projects = []
        for project in self.iter_projects():
            haystacks = [
                normalize_text(project.get("name")),
                normalize_text(project.get("key")),
                normalize_text((project.get("projectCategory") or {}).get("name")),
            ]
            if any(
                scope == haystack or scope in haystack or haystack in scope
                for scope in normalized_scopes
                for haystack in haystacks
                if scope and haystack
            ):
                projects.append(project)
        return projects or [{"key": item, "name": item} for item in self.config.scope_names]

    def _search_v3(self, jql: str, fields: list[str]) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        next_page_token: str | None = None
        while True:
            payload = self.request(
                "POST", "/rest/api/3/search/jql",
                json={"jql": jql, "fields": fields, "maxResults": PAGE_SIZE,
                      **({"nextPageToken": next_page_token} if next_page_token else {})},
            )
            issues.extend(payload.get("issues", []))
            next_page_token = payload.get("nextPageToken")
            if not next_page_token:
                return issues

    def _search_v2(self, jql: str, fields: list[str]) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        start_at = 0
        while True:
            payload = self.request(
                "POST", "/rest/api/2/search",
                json={"jql": jql, "fields": fields, "startAt": start_at, "maxResults": PAGE_SIZE},
            )
            batch = payload.get("issues", [])
            issues.extend(batch)
            start_at += len(batch)
            if start_at >= payload.get("total", 0):
                return issues

    def search_issues(self, jql: str, fields: list[str]) -> list[dict[str, Any]]:
        if self._search_mode == "v3":
            return self._search_v3(jql, fields)
        if self._search_mode == "v2":
            return self._search_v2(jql, fields)
        for mode_name, search_fn in (("v3", self._search_v3), ("v2", self._search_v2)):
            try:
                issues = search_fn(jql, fields)
                self._search_mode = mode_name
                return issues
            except RuntimeError:
                continue
        raise RuntimeError("Não foi possível consultar issues no Jira usando os endpoints suportados.")

    def get_project_epics(self, project_key: str, fields: list[str]) -> list[dict[str, Any]]:
        return self.search_issues(f'project = "{project_key}" AND issuetype = Epic ORDER BY key', fields)

    def get_epic_children(self, epic_key: str, fields: list[str]) -> list[dict[str, Any]]:
        try:
            return self.paged_get(f"/rest/agile/1.0/epic/{epic_key}/issue", {"fields": ",".join(fields)})
        except RuntimeError:
            for jql in [
                f'parentEpic = "{epic_key}" ORDER BY key',
                f'parent = "{epic_key}" ORDER BY key',
                f'"Epic Link" = "{epic_key}" ORDER BY key',
            ]:
                try:
                    return self.search_issues(jql, fields)
                except RuntimeError:
                    continue
            raise RuntimeError(f"Não foi possível listar os filhos do épico {epic_key}.")

    def get_board_issues(self, board_id: str, fields: list[str]) -> list[dict[str, Any]]:
        return self.paged_get(f"/rest/agile/1.0/board/{board_id}/issue", {"fields": ",".join(fields)})


# ─────────────────────────────────────────────
# Regras de negócio
# ─────────────────────────────────────────────
def calculate_completion(epic_fields: dict[str, Any], children: list[dict[str, Any]]) -> int:
    if not children:
        return 100 if is_done_issue(epic_fields) else 0
    done_count = sum(1 for child in children if is_done_issue(child.get("fields") or {}))
    return round((done_count / len(children)) * 100)

def calculate_due_date(epic_fields: dict[str, Any], children: list[dict[str, Any]], due_field_id: str | None) -> date | None:
    epic_due = extract_due_date(epic_fields, due_field_id)
    if epic_due:
        return epic_due
    child_dates = [extract_due_date(child.get("fields") or {}, due_field_id) for child in children]
    valid_dates = [d for d in child_dates if d is not None]
    return max(valid_dates) if valid_dates else None

def calculate_semaphore(completion: int, due_date_value: date | None, warning_days: int) -> str:
    if completion >= 100:
        return "VERDE"
    if due_date_value is None:
        return "SEM DATA"
    days_until_due = (due_date_value - date.today()).days
    if days_until_due < 0:
        return "VERMELHO"
    if days_until_due <= warning_days:
        return "AMARELO"
    return "VERDE"

def completion_from_status(fields: dict[str, Any]) -> int:
    status_name = normalize_text(get_status_name(fields))
    status_category = normalize_text((fields.get("status") or {}).get("statusCategory", {}).get("key"))
    mapping = {
        "backlog": 0, "a fazer": 0, "to do": 0, "novo": 0, "classificado": 0,
        "em analise": 0, "aguardando informacoes": 0,
        "em andamento": 50, "in progress": 50, "em execucao": 50,
        "bloqueado": 25, "blocked": 25,
        "aceito": 100, "concluido": 100, "concluida": 100,
        "done": 100, "resolved": 100, "resolvido": 100,
    }
    if status_name in mapping:
        return mapping[status_name]
    if status_category == "done":
        return 100
    if status_category == "indeterminate":
        return 50
    return 0


# ─────────────────────────────────────────────
# Builders de DataFrame
# ─────────────────────────────────────────────
def build_epic_df(client: JiraClient, projects: list[dict[str, Any]]) -> pd.DataFrame:
    due_field_id = client.resolve_deadline_field()
    epic_fields = ["summary", "assignee", "status", "duedate"]
    child_fields = ["status", "duedate"]
    if due_field_id and due_field_id not in epic_fields:
        epic_fields.append(due_field_id)
        child_fields.append(due_field_id)

    rows = []
    total_projects = len(projects)
    progress_bar = st.progress(0, text="Carregando épicos...")

    for i, project in enumerate(projects):
        project_key = project.get("key") or ""
        project_name = project.get("name") or project_key
        progress_bar.progress((i + 1) / total_projects, text=f"Carregando {project_name}...")

        for epic in client.get_project_epics(project_key, epic_fields):
            fields = epic.get("fields") or {}
            children = client.get_epic_children(epic.get("key") or "", child_fields)
            completion = calculate_completion(fields, children)
            due_date_value = calculate_due_date(fields, children, due_field_id)
            semaphore = calculate_semaphore(completion, due_date_value, client.config.warning_days)
            rows.append({
                "Espaço": project_name,
                "Épico": epic.get("key") or "-",
                "Título": fields.get("summary") or "-",
                "Filhos": len(children),
                "Responsável": pick_display_name(fields.get("assignee")),
                "Status": get_status_name(fields),
                "% Completude": completion,
                "Prazo": format_date(due_date_value),
                "Semáforo": SEMAPHORE_EMOJI.get(semaphore, "⚪") + " " + semaphore,
                "_semaphore_raw": semaphore,
            })

    progress_bar.empty()
    return pd.DataFrame(rows)


def build_board_df(client: JiraClient, board_ids: list[str]) -> pd.DataFrame:
    rows = []
    seen: set[tuple[str, str]] = set()
    total = len(board_ids)
    progress_bar = st.progress(0, text="Carregando board...")

    for i, board_id in enumerate(board_ids):
        progress_bar.progress((i + 1) / total, text=f"Carregando board {board_id}...")
        for issue in client.get_board_issues(board_id, ["summary", "assignee", "status", "updated"]):
            issue_key = issue.get("key") or "-"
            if (board_id, issue_key) in seen:
                continue
            seen.add((board_id, issue_key))
            fields = issue.get("fields") or {}
            summary = fields.get("summary") or "-"
            rows.append({
                "Board": board_id,
                "Item": issue_key,
                "Resumo": summary,
                "Responsável": pick_display_name(fields.get("assignee")),
                "Status": get_status_name(fields),
                "% Conclusão": completion_from_status(fields),
                "ServiceNow": extract_service_now_ref(summary),
                "Última Atualização": format_datetime(fields.get("updated")),
            })

    progress_bar.empty()
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# UI helpers
# ─────────────────────────────────────────────
def metrics_row_epic(df: pd.DataFrame) -> None:
    total = len(df)
    verde = (df["_semaphore_raw"] == "VERDE").sum()
    amarelo = (df["_semaphore_raw"] == "AMARELO").sum()
    vermelho = (df["_semaphore_raw"] == "VERMELHO").sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total de Épicos", total)
    c2.metric("🟢 Verde", int(verde))
    c3.metric("🟡 Amarelo", int(amarelo))
    c4.metric("🔴 Vermelho", int(vermelho))

def metrics_row_board(df: pd.DataFrame) -> None:
    total = len(df)
    done = (df["% Conclusão"] == 100).sum()
    in_progress = ((df["% Conclusão"] > 0) & (df["% Conclusão"] < 100)).sum()
    avg = int(df["% Conclusão"].mean()) if total else 0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total de Itens", total)
    c2.metric("✅ Concluídos", int(done))
    c3.metric("🔄 Em andamento", int(in_progress))
    c4.metric("📊 Média conclusão", f"{avg}%")

def render_epic_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Nenhum épico encontrado para os filtros selecionados.")
        return
    semaphore_order = {"VERMELHO": 0, "AMARELO": 1, "SEM DATA": 2, "VERDE": 3}
    display_df = df.copy()
    display_df["_order"] = display_df["_semaphore_raw"].map(semaphore_order).fillna(99)
    display_df = display_df.sort_values(["Espaço", "_order", "Prazo", "Épico"]).drop(columns=["_order", "_semaphore_raw"])
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "% Completude": st.column_config.ProgressColumn("% Completude", min_value=0, max_value=100, format="%d%%"),
            "Título": st.column_config.TextColumn("Título", width="large"),
            "Épico": st.column_config.TextColumn("Épico", width="small"),
        }
    )

def render_board_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Nenhum item encontrado para os filtros selecionados.")
        return
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "% Conclusão": st.column_config.ProgressColumn("% Conclusão", min_value=0, max_value=100, format="%d%%"),
            "Resumo": st.column_config.TextColumn("Resumo", width="large"),
        }
    )


# ─────────────────────────────────────────────
# App principal
# ─────────────────────────────────────────────
def main() -> None:
    st.set_page_config(
        page_title="Jira Dashboard",
        page_icon="📋",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown("""
    <style>
    [data-testid="stAppViewContainer"] { background: #f8fafc; }
    [data-testid="collapsedControl"] { display: none; }
    .stMetric { background: white; border-radius: 12px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }
    </style>
    """, unsafe_allow_html=True)

    # Header
    col_title, col_btn = st.columns([6, 1])
    with col_title:
        st.markdown("# 📋 Jira Dashboard")
        st.markdown("Acompanhamento de épicos e boards em tempo real.")
    with col_btn:
        st.markdown("<div style='padding-top:24px'>", unsafe_allow_html=True)
        load_btn = st.button("🔄 Atualizar", type="primary", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Carregamento automático na primeira visita ou ao clicar em Atualizar
    if load_btn or "epic_df" not in st.session_state:
        config = Config.from_secrets()
        client = JiraClient(config)

        with st.spinner("Conectando ao Jira..."):
            try:
                projects = client.find_scope_projects()
                st.session_state["epic_df"] = build_epic_df(client, projects) if projects else pd.DataFrame()
            except Exception as e:
                st.session_state["epic_df"] = pd.DataFrame()
                st.error(f"Erro ao carregar épicos: {e}")

            if config.board_ids:
                try:
                    st.session_state["board_df"] = build_board_df(client, config.board_ids)
                except Exception as e:
                    st.session_state["board_df"] = pd.DataFrame()
                    st.error(f"Erro ao carregar board: {e}")
            else:
                st.session_state["board_df"] = pd.DataFrame()

    # Abas
    tab_epic, tab_board = st.tabs(["🗂️ Épicos por Projeto", "📌 Board de Issues"])

    with tab_epic:
        epic_df: pd.DataFrame = st.session_state.get("epic_df", pd.DataFrame())
        if epic_df.empty:
            st.info("Nenhum épico encontrado nos projetos disponíveis.")
        else:
            with st.expander("🔍 Filtros", expanded=False):
                fc1, fc2, fc3 = st.columns(3)
                with fc1:
                    espacos = sorted(epic_df["Espaço"].dropna().unique().tolist())
                    sel_espaco = st.multiselect("Espaço / Projeto", options=espacos, default=espacos, key="f_espaco")
                with fc2:
                    resps = sorted(epic_df["Responsável"].dropna().unique().tolist())
                    sel_resp = st.multiselect("Responsável", options=resps, default=resps, key="f_resp_epic")
                with fc3:
                    statuses = sorted(epic_df["Status"].dropna().unique().tolist())
                    sel_status = st.multiselect("Status", options=statuses, default=statuses, key="f_status_epic")

            filtered = epic_df.copy()
            if sel_espaco:
                filtered = filtered[filtered["Espaço"].isin(sel_espaco)]
            if sel_resp:
                filtered = filtered[filtered["Responsável"].isin(sel_resp)]
            if sel_status:
                filtered = filtered[filtered["Status"].isin(sel_status)]

            metrics_row_epic(filtered)
            st.markdown("---")
            st.markdown(f"**{len(filtered)} épico(s)**")
            render_epic_table(filtered)

            if not filtered.empty:
                csv = filtered.drop(columns=["_semaphore_raw"], errors="ignore").to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Exportar CSV", data=csv, file_name="epicos_jira.csv", mime="text/csv")

    with tab_board:
        board_df: pd.DataFrame = st.session_state.get("board_df", pd.DataFrame())
        if board_df.empty:
            st.info("Nenhum item encontrado. Verifique os `board_ids` nos secrets.")
        else:
            with st.expander("🔍 Filtros", expanded=False):
                fc1, fc2, fc3 = st.columns(3)
                with fc1:
                    resps_b = sorted(board_df["Responsável"].dropna().unique().tolist())
                    sel_resp_b = st.multiselect("Responsável", options=resps_b, default=resps_b, key="f_resp_board")
                with fc2:
                    statuses_b = sorted(board_df["Status"].dropna().unique().tolist())
                    sel_status_b = st.multiselect("Status", options=statuses_b, default=statuses_b, key="f_status_board")
                with fc3:
                    boards_b = sorted(board_df["Board"].dropna().unique().tolist())
                    sel_board = st.multiselect("Board", options=boards_b, default=boards_b, key="f_board")

            filtered_b = board_df.copy()
            if sel_resp_b:
                filtered_b = filtered_b[filtered_b["Responsável"].isin(sel_resp_b)]
            if sel_status_b:
                filtered_b = filtered_b[filtered_b["Status"].isin(sel_status_b)]
            if sel_board:
                filtered_b = filtered_b[filtered_b["Board"].isin(sel_board)]

            metrics_row_board(filtered_b)
            st.markdown("---")
            st.markdown(f"**{len(filtered_b)} item(s)**")
            render_board_table(filtered_b)

            if not filtered_b.empty:
                csv = filtered_b.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Exportar CSV", data=csv, file_name="board_jira.csv", mime="text/csv")


if __name__ == "__main__":
    main()
