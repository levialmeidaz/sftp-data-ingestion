# requisitos:
#   pip install paramiko python-dotenv

import os
import sys
import time
import logging
from pathlib import Path
import paramiko
from dotenv import load_dotenv

# .env/.credenciais
load_dotenv(dotenv_path=Path(".env") / "sftp.env")

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")
SFTP_DIR  = os.getenv("SFTP_DIR")

DEST_DIR  = Path(r"C:\Users\atend\OneDrive - grupojb.log.br\STORAGE_SFTP\rel_83\novos")
LOG_DIR   = Path(r"C:\Users\atend\OneDrive - grupojb.log.br\STORAGE_SFTP\rel_83\logs")
LOG_FILE  = LOG_DIR / "ingest_sftp_rel83.log"

RETRIES = 3
SLEEP_BETWEEN = 2  # segundos

def setup_logging():
    DEST_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
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
    import stat as _stat
    entries = sftp.listdir_attr(remote_dir)
    return [e for e in entries if not _stat.S_ISDIR(e.st_mode) and e.filename.upper().endswith(".CSV")]

def cleanup_part_files():
    removed = 0
    for p in DEST_DIR.glob("*.part"):
        try:
            p.unlink()
            removed += 1
        except Exception:
            pass
    if removed:
        logging.info("Removidos %d arquivos .part antigos", removed)

def download_with_verify(sftp, remote_path: str, local_path: Path, expected_size: int):
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            sftp.get(remote_path, str(local_path))
            # valida tamanho
            if local_path.exists() and local_path.stat().st_size == expected_size:
                return True
            else:
                # remove corrompido
                try:
                    local_path.unlink(missing_ok=True)
                except Exception:
                    pass
                last_err = RuntimeError(f"tamanho divergente: esperado {expected_size} bytes")
        except Exception as e:
            last_err = e
            # remove arquivo parcial se criado
            try:
                local_path.unlink(missing_ok=True)
            except Exception:
                pass
        if attempt < RETRIES:
            time.sleep(SLEEP_BETWEEN)
    # falhou
    logging.error("Falha ao baixar %s: %s", remote_path, last_err)
    return False

def main():
    setup_logging()
    cleanup_part_files()
    start = time.time()
    logging.info("Início da ingestão SFTP")

    existing = {p.name for p in DEST_DIR.glob("*.csv")} | {p.name for p in DEST_DIR.glob("*.CSV")}
    logging.info("Arquivos locais existentes: %d", len(existing))

    transport = None
    sftp = None
    new_count = 0
    skipped = 0
    try:
        transport, sftp = connect_sftp()
        logging.info("Conectado ao SFTP %s", SFTP_HOST)

        remote_files = list_remote_files(sftp, SFTP_DIR)
        logging.info("Arquivos .CSV no SFTP: %d", len(remote_files))

        for f in sorted(remote_files, key=lambda x: x.filename):
            fname = f.filename
            if fname in existing:
                skipped += 1
                continue

            remote_path = f"{SFTP_DIR}/{fname}"
            local_path = DEST_DIR / fname

            ok = download_with_verify(sftp, remote_path, local_path, f.st_size)
            if ok:
                new_count += 1
                logging.info("Copiado: %s  ->  %s (%d bytes)", remote_path, local_path, f.st_size)

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
