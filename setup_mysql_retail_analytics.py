#!/usr/bin/env python3
"""setup_mysql_retail_analytics.py

Cria um MySQL local (via Docker Compose), aplica o schema empresarial
(retail_analytics) e gera credenciais para uso no DBeaver e no backend.

O que este script faz:
  1) Verifica pré-requisitos (Docker rodando, porta disponível)
  2) Gera senhas aleatórias (root e app user) se ainda não existem
  3) Sobe um container MySQL 8 via docker compose
  4) Aguarda o banco ficar pronto (health check)
  5) Verifica que as tabelas foram criadas
  6) Salva credenciais em .env e credentials.json
  7) Imprime instruções de conexão (local + LAN para DBeaver)

Pré-requisitos:
  - Docker Desktop instalado e rodando
  - Python 3.9+

Uso:
  python3 setup_mysql_retail_analytics.py

Opcional:
  python3 setup_mysql_retail_analytics.py --port 3308
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import socket
import string
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Diretório base do projeto (onde este script está)
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def run(cmd: list[str], *, check: bool = True, cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Executa um comando e retorna o resultado."""
    return subprocess.run(
        cmd, check=check,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, cwd=cwd,
    )


def gen_password(length: int = 24) -> str:
    """Gera uma senha aleatória segura (sem caracteres que quebram YAML/shell)."""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def get_lan_ip() -> str:
    """Descobre o IP da máquina na rede local."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ---------------------------------------------------------------------------
# Verificações
# ---------------------------------------------------------------------------

def require_docker() -> None:
    """Garante que o Docker está instalado e rodando."""
    try:
        run(["docker", "info"], check=True)
    except FileNotFoundError:
        print("Docker nao encontrado. Instale o Docker Desktop e tente novamente.")
        sys.exit(1)
    except subprocess.CalledProcessError:
        print("Docker nao esta rodando. Inicie o Docker Desktop e tente novamente.")
        sys.exit(1)


def check_port_available(port: int) -> bool:
    """Verifica se a porta está livre."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


# ---------------------------------------------------------------------------
# Credenciais
# ---------------------------------------------------------------------------

def load_or_create_env(env_path: Path, port: int, db_name: str, app_user: str) -> dict[str, str]:
    """Carrega .env existente ou gera novas credenciais."""
    if env_path.exists():
        print(f"Arquivo {env_path.name} ja existe. Reutilizando credenciais.")
        env = {}
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
        return env

    root_pass = gen_password()
    app_pass = gen_password()

    env = {
        "MYSQL_ROOT_PASSWORD": root_pass,
        "MYSQL_DATABASE": db_name,
        "MYSQL_USER": app_user,
        "MYSQL_PASSWORD": app_pass,
        "DB_HOST": "127.0.0.1",
        "DB_PORT": str(port),
    }

    lines = [f"{k}={v}" for k, v in env.items()]
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Credenciais geradas e salvas em {env_path.name}")
    return env


# ---------------------------------------------------------------------------
# Docker Compose
# ---------------------------------------------------------------------------

def update_compose_port(compose_path: Path, port: int) -> None:
    """Atualiza a porta no docker-compose.yml se diferente de 3307."""
    content = compose_path.read_text(encoding="utf-8")
    # Substitui a porta mapeada (formato "0.0.0.0:XXXX:3306")
    import re
    content = re.sub(
        r'"0\.0\.0\.0:\d+:3306"',
        f'"0.0.0.0:{port}:3306"',
        content,
    )
    compose_path.write_text(content, encoding="utf-8")


def start_services(project_dir: Path) -> None:
    """Sobe os serviços via docker compose."""
    print("Subindo container MySQL 8 via Docker Compose...")
    p = run(
        ["docker", "compose", "up", "-d"],
        check=False, cwd=project_dir,
    )
    if p.returncode != 0:
        print(f"Erro ao subir Docker Compose:\n{p.stderr}")
        sys.exit(1)


def wait_mysql_ready(container: str, timeout_s: int = 120) -> None:
    """Espera o MySQL ficar pronto via health check do container."""
    print("Aguardando o MySQL ficar pronto...")
    start = time.time()
    while time.time() - start < timeout_s:
        p = run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
            check=False,
        )
        status = p.stdout.strip()
        if status == "healthy":
            return
        time.sleep(3)
    print(f"Timeout ({timeout_s}s) esperando o MySQL ficar pronto.")
    # Mostra logs para debug
    logs = run(["docker", "logs", "--tail", "30", container], check=False)
    print(f"Ultimos logs do container:\n{logs.stdout}\n{logs.stderr}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Verificação do banco
# ---------------------------------------------------------------------------

def verify_tables(container: str, root_pass: str, db_name: str) -> list[str]:
    """Conecta ao MySQL e lista as tabelas criadas."""
    sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema='{db_name}' ORDER BY table_name;"
    p = run([
        "docker", "exec", container,
        "mysql", "-uroot", f"-p{root_pass}",
        "--skip-column-names", "-e", sql,
    ], check=False)

    if p.returncode != 0:
        print(f"Aviso: nao foi possivel listar tabelas.\n{p.stderr}")
        return []

    tables = [line.strip() for line in p.stdout.splitlines() if line.strip()]
    return tables


# ---------------------------------------------------------------------------
# Saída / Credenciais
# ---------------------------------------------------------------------------

def write_credentials_json(path: Path, env: dict[str, str], lan_ip: str, port: int) -> None:
    """Gera credentials.json com todas as infos para conexão DBeaver."""
    credentials = {
        "connection_name": "YMED Retail Analytics",
        "driver": "MySQL 8.0",
        "local": {
            "host": "localhost",
            "port": port,
        },
        "remote": {
            "host": lan_ip,
            "port": port,
        },
        "database": env.get("MYSQL_DATABASE", "retail_analytics"),
        "app_user": {
            "username": env.get("MYSQL_USER", "app_user"),
            "password": env.get("MYSQL_PASSWORD", ""),
        },
        "root_user": {
            "username": "root",
            "password": env.get("MYSQL_ROOT_PASSWORD", ""),
        },
        "dbeaver_settings": {
            "allowPublicKeyRetrieval": "true",
            "useSSL": "false",
        },
    }
    path.write_text(json.dumps(credentials, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def print_summary(env: dict[str, str], lan_ip: str, port: int, tables: list[str]) -> None:
    """Imprime resumo com instruções de conexão."""
    db = env.get("MYSQL_DATABASE", "retail_analytics")
    user = env.get("MYSQL_USER", "app_user")
    password = env.get("MYSQL_PASSWORD", "")
    root_pass = env.get("MYSQL_ROOT_PASSWORD", "")

    print()
    print("=" * 55)
    print("  MYSQL RETAIL ANALYTICS - PRONTO!")
    print("=" * 55)
    print()
    print("--- Conexao Local (esta maquina) ---")
    print(f"  Host:     localhost")
    print(f"  Port:     {port}")
    print(f"  Database: {db}")
    print(f"  User:     {user}")
    print(f"  Password: {password}")
    print()
    print("--- Conexao Remota (outra pessoa na mesma rede) ---")
    print(f"  Host:     {lan_ip}")
    print(f"  Port:     {port}")
    print(f"  Database: {db}")
    print(f"  User:     {user}")
    print(f"  Password: {password}")
    print()
    print("--- Credenciais Root (admin) ---")
    print(f"  User:     root")
    print(f"  Password: {root_pass}")
    print()
    print("--- Como conectar no DBeaver ---")
    print("  1. Database > New Connection > MySQL")
    print(f"  2. Host: localhost (ou {lan_ip} se for outra maquina)")
    print(f"  3. Port: {port}")
    print(f"  4. Database: {db}")
    print(f"  5. Username: {user}")
    print(f"  6. Password: (veja acima)")
    print("  7. Em 'Driver Properties':")
    print("     - allowPublicKeyRetrieval = true")
    print("     - useSSL = false")
    print()
    print("--- String de conexao Python (SQLAlchemy + PyMySQL) ---")
    print(f"  mysql+pymysql://{user}:{password}@127.0.0.1:{port}/{db}")
    print()

    if tables:
        print(f"--- Tabelas criadas ({len(tables)}) ---")
        for t in tables:
            print(f"  - {t}")
    else:
        print("--- Nenhuma tabela encontrada (verifique sql/001_schema.sql) ---")

    print()
    print("--- Arquivos gerados ---")
    print(f"  .env              -> credenciais (NAO commitar)")
    print(f"  credentials.json  -> infos de conexao para equipe")
    print()
    print("--- Comandos uteis ---")
    print("  docker compose down          -> para o container (dados mantidos)")
    print("  docker compose up -d         -> reinicia o container")
    print("  docker compose down -v       -> APAGA tudo (reset completo)")
    print("  docker compose logs -f       -> ver logs em tempo real")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Setup MySQL Retail Analytics via Docker Compose",
    )
    parser.add_argument("--port", type=int, default=3307, help="Porta local para o MySQL (default: 3307)")
    parser.add_argument("--db", type=str, default="retail_analytics", help="Nome do database")
    parser.add_argument("--app-user", type=str, default="app_user", help="Usuario da aplicacao")
    args = parser.parse_args()

    container_name = "ymed-retail-mysql"

    # 1) Pré-requisitos
    require_docker()

    # Verifica se a porta está livre (ignora se o container já está rodando)
    p = run(["docker", "ps", "--format", "{{.Names}}"], check=False)
    container_running = container_name in (p.stdout or "")

    if not container_running and not check_port_available(args.port):
        print(f"Porta {args.port} ja esta em uso. Use --port para escolher outra.")
        sys.exit(1)

    # 2) Credenciais
    env_path = PROJECT_DIR / ".env"
    env = load_or_create_env(env_path, args.port, args.db, args.app_user)

    # 3) Atualiza porta no docker-compose.yml se necessário
    compose_path = PROJECT_DIR / "docker-compose.yml"
    if not compose_path.exists():
        print(f"Arquivo docker-compose.yml nao encontrado em {PROJECT_DIR}")
        sys.exit(1)

    if args.port != 3307:
        update_compose_port(compose_path, args.port)

    # 4) Sobe os serviços
    start_services(PROJECT_DIR)

    # 5) Aguarda MySQL ficar pronto
    wait_mysql_ready(container_name)

    print("MySQL esta pronto!")

    # 6) Verifica tabelas
    root_pass = env.get("MYSQL_ROOT_PASSWORD", "")
    tables = verify_tables(container_name, root_pass, args.db)

    # 7) Gera credentials.json
    lan_ip = get_lan_ip()
    port = int(env.get("DB_PORT", str(args.port)))
    creds_path = PROJECT_DIR / "credentials.json"
    write_credentials_json(creds_path, env, lan_ip, port)

    # 8) Resumo
    print_summary(env, lan_ip, port, tables)


if __name__ == "__main__":
    main()
