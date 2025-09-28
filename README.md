# Projeto de Ingestão de Dados SFTP para PostgreSQL

Pipeline para ingestão de arquivos **CSV** recebidos via **SFTP**, com armazenamento em **OneDrive local**, processamento em **staging**, atualização incremental no **DW** e arquivamento de histórico.  

---

## 📂 Estrutura de Pastas

```
/STORAGE_SFTP
 └── rel_83/
     ├── novos/   # arquivos recém-baixados do SFTP
     ├── lidos/   # arquivos já processados
     └── erros/   # arquivos corrompidos, zerados ou com cabeçalho inesperado
```

---

## ⚙️ Fluxo da Pipeline

1. **Ingestão (ingestion_sftp_pedidos.py)**  
   - Conecta ao SFTP usando credenciais de `.env`.  
   - Copia arquivos novos para `novos/` no OneDrive local.  
   - Não exclui nada do SFTP.  

2. **Carga Staging (load_stg_pedidos.py)**  
   - Lê os arquivos de `novos/`.  
   - Faz parse dos CSVs e insere dados em `staging.stg_pedidos`.  
   - Arquivos válidos → movidos para `lidos/`.  
   - Arquivos inválidos → movidos para `erros/`.  

3. **Upsert DW (load_dw_pedidos.py)**  
   - Lê dados de `staging.stg_pedidos`.  
   - Aplica **UPSERT** para manter no DW apenas o **último registro** de cada chave (`pedido + numero_nfe + roteiro` ou `chave_nfe`).  

4. **Histórico (load_hist_pedidos.py)**  
   - Move os registros de `staging.stg_pedidos` para `archive.hist_pedidos`.  
   - Evita duplicação entre DW e histórico.  

---

## 🗄️ Estrutura de Banco

### Tabela Staging (`staging.stg_pedidos`)
- Recebe todos os arquivos brutos do SFTP.  
- Mantém colunas padronizadas em snake_case.  

### Tabela DW (`dw.fat_pedidos`)
- Mantém apenas a **última ocorrência** de cada pedido.  
- Chave única baseada em `chave_nfe` (ou composta quando necessário).  

### Tabela Histórico (`archive.hist_pedidos`)
- Contém todas as ocorrências vindas do staging.  
- Usada para auditoria e rastreabilidade.  

---

## 🔑 Configuração de Ambiente

As credenciais ficam em arquivos `.env` ignorados pelo Git:

- **banco.env**
  ```
  PGHOST=localhost
  PGPORT=5432
  PGUSER=postgres
  PGPASSWORD=admin
  PGDATABASE=banco_prod
  ```

- **sftp.env**
  ```
  SFTP_HOST=example.com
  SFTP_PORT=22
  SFTP_USER=usuario
  SFTP_PASS=senha
  ```

### .gitignore
```
.env
*.env
__pycache__/
*.pyc
```

---

## 🛠️ Execução Manual

Rodar os scripts na ordem:

```bash
python ingestion_sftp_pedidos.py
python load_stg_pedidos.py
python load_dw_pedidos.py
python load_hist_pedidos.py
```

---

## ⏱️ Agendamento

Exemplo com **cron** (Linux) a cada 2h:

```cron
0 */2 * * * python /scripts/ingestion_sftp_pedidos.py
5 */2 * * * python /scripts/load_stg_pedidos.py
10 */2 * * * python /scripts/load_dw_pedidos.py
15 */2 * * * python /scripts/load_hist_pedidos.py
```

---

## 🧰 Boas Práticas Aplicadas

- Separação **staging → DW → histórico**.  
- Uso de **UPSERT** para incremental (sem `TRUNCATE`).  
- Padronização de colunas com **snake_case** e **de-para**.  
- Movimentação de arquivos para garantir **idempotência**.  
- Arquivos inválidos tratados e armazenados em `erros/`.  
- **.env** fora do versionamento.  

---

## 📈 Extensões Futuras

- Orquestração com **Apache Airflow / Apache Hop / KNIME**.  
- Monitoramento com **logs detalhados**.  
- Incremental refresh no **Power BI**.  

---

# SFTP Data Ingestion Project for PostgreSQL

Pipeline for ingesting **CSV** files received via **SFTP**, storing them in **local OneDrive**, processing in **staging**, performing incremental updates in the **DW**, and archiving historical data.  

---

## 📂 Folder Structure

```
/STORAGE_SFTP
 └── rel_83/
     ├── novos/   # newly downloaded files from SFTP
     ├── lidos/   # already processed files
     └── erros/   # corrupted, empty, or malformed header files
```

---

## ⚙️ Pipeline Flow

1. **Ingestion (ingestion_sftp_pedidos.py)**  
   - Connects to SFTP using `.env` credentials.  
   - Copies new files to `novos/` in local OneDrive.  
   - Does not delete anything from SFTP.  

2. **Staging Load (load_stg_pedidos.py)**  
   - Reads files from `novos/`.  
   - Parses CSVs and inserts into `staging.stg_pedidos`.  
   - Valid files → moved to `lidos/`.  
   - Invalid files → moved to `erros/`.  

3. **DW Upsert (load_dw_pedidos.py)**  
   - Reads data from `staging.stg_pedidos`.  
   - Applies **UPSERT** to keep only the **latest record** for each key (`pedido + numero_nfe + roteiro` or `chave_nfe`).  

4. **Archive (load_hist_pedidos.py)**  
   - Moves records from `staging.stg_pedidos` to `archive.hist_pedidos`.  
   - Prevents duplication between DW and history.  

---

## 🗄️ Database Structure

### Staging Table (`staging.stg_pedidos`)
- Receives all raw files from SFTP.  
- Uses snake_case column naming.  

### DW Table (`dw.fat_pedidos`)
- Stores only the **latest occurrence** of each order.  
- Unique key based on `chave_nfe` (or composite when needed).  

### Archive Table (`archive.hist_pedidos`)
- Stores all occurrences from staging.  
- Used for auditing and traceability.  

---

## 🔑 Environment Configuration

Credentials are stored in `.env` files ignored by Git:

- **banco.env**
  ```
  PGHOST=localhost
  PGPORT=5432
  PGUSER=postgres
  PGPASSWORD=admin
  PGDATABASE=banco_prod
  ```

- **sftp.env**
  ```
  SFTP_HOST=example.com
  SFTP_PORT=22
  SFTP_USER=user
  SFTP_PASS=password
  ```

### .gitignore
```
.env
*.env
__pycache__/
*.pyc
```

---

## 🛠️ Manual Execution

Run scripts in order:

```bash
python ingestion_sftp_pedidos.py
python load_stg_pedidos.py
python load_dw_pedidos.py
python load_hist_pedidos.py
```

---

## ⏱️ Scheduling

Example with **cron** (Linux) every 2h:

```cron
0 */2 * * * python /scripts/ingestion_sftp_pedidos.py
5 */2 * * * python /scripts/load_stg_pedidos.py
10 */2 * * * python /scripts/load_dw_pedidos.py
15 */2 * * * python /scripts/load_hist_pedidos.py
```

---

## 🧰 Best Practices Applied

- Clear separation **staging → DW → history**.  
- **UPSERT** for incremental load (no `TRUNCATE`).  
- Standardized **snake_case** naming with mapping.  
- File movement ensures **idempotency**.  
- Invalid files stored in `erros/`.  
- **.env** excluded from version control.  

---

## 📈 Future Extensions

- Orchestration with **Apache Airflow / Apache Hop / KNIME**.  
- Monitoring with **detailed logs**.  
- Incremental refresh in **Power BI**.  
