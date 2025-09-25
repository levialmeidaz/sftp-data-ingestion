import os
import sys
import time
import logging
from pathlib import Path
import paramiko

SFTP_HOST = "sftp.mytracking.com.br"
SFTP_PORT = 22
SFTP_USER = "pro_emp_096_jb"
SFTP_PASS = "2a1d1d6345d219e208ed77f58cda78ad"
SFTP_DIR  = "/diretorio/extracoes/relatorio_083"

DEST_DIR  = Path(r"C:\Users\atend\OneDrive - grupojb.log.br\STORAGE_SFTP\rel_83\novos")
LOG_FILE  = DEST_DIR / "ingest_sftp_rel83.log"

def setup_logging():
    DEST_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

def connect_sftp():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return transport, sftp

def list_remote_files(sftp, remote_dir):
    entries = sftp.listdir_attr(remote_dir)
    # mantém somente arquivos regulares e que terminam com .csv
    files = [e for e in entries if not stat_is_dir(e) and e.filename.lower().endswith(".csv")]
    return files

def stat_is_dir(attr):
    # bitmask POSIX para diretório
    import stat as _stat
    return _stat.S_ISDIR(attr.st_mode)

def safe_download(sftp, remote_path, local_path: Path):
    tmp_path = local_path.with_suffix(local_path.suffix + ".part")
    # baixa para .part e renomeia atômico ao concluir
    sftp.get(remote_path, str(tmp_path))
    tmp_path.replace(local_path)

def main():
    setup_logging()
    start = time.time()
    logging.info("Início da ingestão SFTP")

    # coleta nomes locais existentes
    existing = {p.name for p in DEST_DIR.glob("*.csv")}
    logging.info("Arquivos locais existentes: %d", len(existing))

    transport = None
    sftp = None
    new_count = 0
    skipped = 0
    try:
        transport, sftp = connect_sftp()
        logging.info("Conectado ao SFTP %s", SFTP_HOST)

        remote_files = list_remote_files(sftp, SFTP_DIR)
        logging.info("Arquivos .csv no SFTP: %d", len(remote_files))

        for f in sorted(remote_files, key=lambda x: x.filename):
            fname = f.filename
            if fname in existing:
                skipped += 1
                continue

            remote_path = f"{SFTP_DIR}/{fname}"
            local_path = DEST_DIR / fname

            try:
                safe_download(sftp, remote_path, local_path)
                new_count += 1
                logging.info("Copiado: %s  ->  %s", remote_path, local_path)
            except Exception as e:
                logging.error("Falha ao copiar %s: %s", fname, e)

    except Exception as e:
        logging.error("Erro de conexão ou listagem: %s", e)
        sys.exit(1)
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

    elapsed = time.time() - start
    logging.info("Concluído. Novos: %d | Já existiam: %d | Tempo: %.2fs", new_count, skipped, elapsed)

if __name__ == "__main__":
    main()
