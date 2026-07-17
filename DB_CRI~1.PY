#!/usr/bin/env python3
"""
db_critical_watchdog.py
========================
AI Database Critical Watchdog — monitora de perto a saude de um banco
Postgres de producao e manda alerta direto no Telegram, ja com o
diagnostico feito por uma IA gratuita (Groq).

Por que isso e critico: banco de dados e o ponto unico de falha da
maioria dos sistemas. Replicacao atrasando, pool de conexao lotando ou
disco enchendo silenciosamente sao os sintomas que aparecem HORAS antes
do outage. Esse script fica de olho nisso de perto (a cada 1-2 minutos)
e avisa no Telegram assim que algo sai da faixa segura -- com prioridade
e causa provavel ja definidas pela IA, nao so um numero cru.

O que ele checa a cada execucao:
  1. Replication lag (segundos atras do primario)
  2. Conexoes ativas vs. max_connections (percentual de saturacao)
  3. Espaco em disco do dispositivo do banco
  4. Queries rodando ha mais tempo que o limite (possivel lock/travamento)

Se tudo estiver dentro do normal, ele nao chama a IA nem manda mensagem
nenhuma -- so sai em silencio (economiza cota gratis e nao gera fadiga
de alerta).

Uso manual:
    export DATABASE_URL="postgresql://user:senha@host:5432/banco"
    export GROQ_API_KEY="gsk_..."
    export TELEGRAM_BOT_TOKEN="123456:ABC-..."
    export TELEGRAM_CHAT_ID="-100123456789"

    python db_critical_watchdog.py

    # Ajustar limites
    python db_critical_watchdog.py --max-lag-seconds 10 --max-conn-pct 85 --max-disk-pct 90

Rodando no crontab (a cada 2 minutos -- e critico, precisa de perto):
    */2 * * * * DATABASE_URL="..." GROQ_API_KEY="..." TELEGRAM_BOT_TOKEN="..." \\
        TELEGRAM_CHAT_ID="..." /usr/bin/python3 /caminho/db_critical_watchdog.py \\
        >> /var/log/db-watchdog/cron.log 2>&1

Requisitos:
    pip install groq psycopg2-binary requests --break-system-packages
"""

import argparse
import os
import shutil
import sys
from datetime import datetime, timezone

try:
    import psycopg2
    import requests
    from groq import Groq
except ImportError:
    sys.exit("Instale as dependencias primeiro: pip install groq psycopg2-binary requests")

MODEL = "llama-3.3-70b-versatile"  # modelo gratis na Groq

SYSTEM_PROMPT = """Voce e um DBA/SRE senior fazendo triagem de saude de um banco Postgres critico
de producao. Dadas as metricas abaixo, responda SEMPRE neste formato, em portugues, direto ao ponto:

SEVERIDADE: P1/P2/P3/P4 numa linha, com uma frase curta justificando.

CAUSA PROVAVEL: 1-2 frases sobre o que provavelmente esta acontecendo.

ACAO IMEDIATA: 1-3 passos concretos, do mais urgente primeiro.

Seja extremamente direto, isso vai para um alerta de Telegram lido as pressas.
Nao ultrapasse 6 linhas no total."""


def get_metrics(dsn: str, disk_path: str) -> dict:
    conn = psycopg2.connect(dsn, connect_timeout=5)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        SELECT CASE WHEN pg_is_in_recovery()
            THEN EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))
            ELSE 0 END;
    """)
    lag_seconds = cur.fetchone()[0] or 0

    cur.execute("SELECT count(*) FROM pg_stat_activity;")
    active_conn = cur.fetchone()[0]

    cur.execute("SHOW max_connections;")
    max_conn = int(cur.fetchone()[0])

    cur.execute("""
        SELECT pid, now() - query_start AS duration, left(query, 120)
        FROM pg_stat_activity
        WHERE state = 'active' AND now() - query_start > interval '30 seconds'
        ORDER BY duration DESC
        LIMIT 5;
    """)
    long_queries = cur.fetchall()

    cur.close()
    conn.close()

    disk = shutil.disk_usage(disk_path)
    disk_pct = round(disk.used / disk.total * 100, 1)

    return {
        "lag_seconds": round(float(lag_seconds), 1),
        "active_conn": active_conn,
        "max_conn": max_conn,
        "conn_pct": round(active_conn / max_conn * 100, 1),
        "disk_pct": disk_pct,
        "long_queries": long_queries,
    }


def breaches(metrics: dict, max_lag: int, max_conn_pct: float, max_disk_pct: float) -> list:
    problems = []
    if metrics["lag_seconds"] > max_lag:
        problems.append(f"replication lag de {metrics['lag_seconds']}s (limite: {max_lag}s)")
    if metrics["conn_pct"] > max_conn_pct:
        problems.append(
            f"{metrics['conn_pct']}% das conexoes em uso "
            f"({metrics['active_conn']}/{metrics['max_conn']})"
        )
    if metrics["disk_pct"] > max_disk_pct:
        problems.append(f"disco em {metrics['disk_pct']}% de uso (limite: {max_disk_pct}%)")
    if metrics["long_queries"]:
        problems.append(f"{len(metrics['long_queries'])} query(s) travada(s) ha mais de 30s")
    return problems


def analyze(metrics: dict, problems: list) -> str:
    client = Groq()
    long_q = "\n".join(f"  pid {p[0]}: {p[1]} rodando -- {p[2]}" for p in metrics["long_queries"])
    payload = (
        f"Replication lag: {metrics['lag_seconds']}s\n"
        f"Conexoes: {metrics['active_conn']}/{metrics['max_conn']} ({metrics['conn_pct']}%)\n"
        f"Disco: {metrics['disk_pct']}%\n"
        f"Queries longas:\n{long_q or '  nenhuma'}\n\n"
        f"Problemas detectados: {', '.join(problems)}"
    )
    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=300,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": payload},
        ],
    )
    return response.choices[0].message.content


def send_telegram(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
    resp.raise_for_status()


def main():
    parser = argparse.ArgumentParser(description="Watchdog critico de banco com IA + Telegram.")
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--disk-path", default="/var/lib/postgresql")
    parser.add_argument("--max-lag-seconds", type=float, default=30)
    parser.add_argument("--max-conn-pct", type=float, default=80)
    parser.add_argument("--max-disk-pct", type=float, default=85)
    args = parser.parse_args()

    if not args.dsn:
        sys.exit("Defina DATABASE_URL ou passe --dsn")

    timestamp = datetime.now(timezone.utc).isoformat()
    metrics = get_metrics(args.dsn, args.disk_path)
    problems = breaches(metrics, args.max_lag_seconds, args.max_conn_pct, args.max_disk_pct)

    if not problems:
        print(f"[{timestamp}] Tudo dentro do normal. Nenhum alerta enviado.")
        return

    print(f"[{timestamp}] {len(problems)} problema(s) detectado(s). Consultando IA...")
    diagnosis = analyze(metrics, problems)

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    message = f"Alerta de banco de dados -- {timestamp}\n\n{diagnosis}"

    if token and chat_id:
        send_telegram(token, chat_id, message)
        print("Alerta enviado ao Telegram.")
    else:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID nao configurados, imprimindo apenas:\n")
        print(message)


if __name__ == "__main__":
    main()
