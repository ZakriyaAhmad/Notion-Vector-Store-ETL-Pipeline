# Notion → Vector Store ETL Pipeline

**Production-Grade Sequential ETL Pipeline for Building AI Knowledge Bases from Notion**

A scalable, **AWS Glue–friendly ETL pipeline** that extracts documents from Notion workspaces and loads them into a Vector Store for **AI search, RAG systems, and LLM assistants**.

Designed for **enterprise knowledge platforms**, **low memory environments**, and **incremental processing at scale**.

---

## Key Capabilities

✔ Crawl Notion workspace automatically
✔ Extract files & images from pages
✔ Sequential low-memory processing
✔ Incremental ETL with manifest tracking
✔ Glue-ready architecture
✔ Safe re-runs
✔ Resume capability
✔ Dry-run testing mode
✔ Partial dataset processing
✔ Detailed logging

---

## Enterprise Architecture

```
                Notion Workspace
                        │
                        ▼
                Recursive Page Crawler
                        │
                        ▼
                 File Discovery Layer
                        │
                        ▼
              Sequential ETL Processor
        ┌─────────────┬─────────────┬─────────────┐
        │ Download    │ Extract     │ Chunk       │
        │ Files       │ Text        │ Content     │
        └─────────────┴─────────────┴─────────────┘
                        │
                        ▼
                 Vector Store Upload
                        │
                        ▼
                  Manifest Storage
                  (Local / S3)
```

---<img width="2000" height="2000" alt="Notion ETL Pipeline for-2026-02-20-222027" src="https://github.com/user-attachments/assets/2e8e030e-6008-478e-a9af-ef4b088a84d9" />


## Processing Workflow

Pipeline processes **one file at a time**:

```
1. Crawl Notion Pages
2. Discover Files
3. Download File
4. Extract Text
5. Chunk Text
6. Upload to Vector Store
7. Update Manifest
```

### Why Sequential Processing?

This design ensures:

* Predictable memory usage
* Glue compatibility
* Stability for large workspaces
* Fault tolerance
* Easy debugging

Ideal for:

* AWS Glue
* Databricks Jobs
* Airflow DAGs
* Batch processing

---

## Incremental ETL Design

The pipeline uses a **manifest file** to track processed documents.

### Benefits

✔ No duplicate processing
✔ Resume interrupted runs
✔ Fast incremental updates
✔ Production safe

Example:

```json
{
  "file_id": "file_123",
  "file_name": "pricing_guide.pdf",
  "page_id": "page_456",
  "processed": true,
  "processed_at": "2026-02-01T12:00:00"
}
```

Manifest storage options:

* Local disk
* Amazon S3

---

## Environment Variables

### Required

```
NOTION_API_KEY=xxxxxxxx
OPENAI_API_KEY=xxxxxxxx
```

---

### Optional Configuration

## Root Page Seed

Start crawling from a root page:

```
ROOT_PAGE_ID=xxxxxxxx
```

or

```
ROOT_PAGE_URL=https://notion.so/page
```

---

## Vector Store

Use an existing vector store:

```
VECTOR_STORE_ID=xxxxxxxx
```

If omitted, a new store may be created.

---

## Manifest Storage

Local manifest:

```
MANIFEST_PATH=manifest.json
```

Default:

```
CACHE
```

S3 manifest:

```
MANIFEST_S3=s3://bucket/manifest.json
```

Recommended for production.

---

## Download Directory

Temporary file storage:

```
DOWNLOAD_DIR=notion_downloads_v2
```

---

## Safe Execution Controls

### Dry Run

Discover files without processing:

```
DRY_RUN=true
```

Useful for:

* Validation
* Testing
* Estimation

---

### Limits

Limit pages:

```
MAX_PAGES=50
```

Limit files:

```
MAX_FILES=100
```

---

### Page Filtering

Process only selected pages:

```
PAGE_ID_FILTER=id1,id2,id3
```

---

### Detailed Logs

```
LOG_DETAILS=true
```

Shows:

* Page titles
* Files discovered
* Processing progress

---

## Installation

### Clone Repository

```
git clone <repo>
cd notion-vector-etl
```

---

### Install Dependencies

```
pip install -r requirements.txt
```

Example requirements:

```
requests
boto3
openai
python-dotenv
```

---

## Running the Pipeline

### Basic Run

```
python notion_vector_etl.py
```

---

### Full Configuration Example

```
NOTION_API_KEY=xxx \
OPENAI_API_KEY=xxx \
ROOT_PAGE_ID=xxx \
VECTOR_STORE_ID=xxx \
MANIFEST_S3=s3://etl-bucket/manifest.json \
python notion_vector_etl.py
```

---

## Example Output

```
Starting Notion Crawl...

Discovered Pages: 37
Discovered Files: 112

Processing File 1/112
File: onboarding.pdf

Downloading...
Extracting text...
Chunking content...
Uploading vectors...

Manifest Updated
```

---

## AWS Glue Deployment

### Recommended Configuration

Worker Type:

```
G.1X
```

Workers:

```
2
```

Python Version:

```
3.10
```

---

### Glue Job Parameters

Example:

```
--NOTION_API_KEY=xxx
--OPENAI_API_KEY=xxx
--ROOT_PAGE_ID=xxx
--MANIFEST_S3=s3://etl/manifest.json
```

---

## Performance Characteristics

| Feature             | Behavior   |
| ------------------- | ---------- |
| Memory Usage        | Low        |
| Processing Mode     | Sequential |
| Incremental Support | Yes        |
| Fault Tolerance     | High       |
| Glue Compatible     | Yes        |
| Restart Safe        | Yes        |

---

## Typical Use Cases

### AI Knowledge Base

Convert Notion workspace into:

* AI assistant knowledge
* Searchable knowledge base
* Documentation AI

---

### RAG Systems

Designed for:

* OpenAI Assistants
* LangChain
* LlamaIndex
* Custom AI platforms

---

### Enterprise AI Platforms

Ideal for:

* Internal knowledge assistants
* Customer support AI
* Documentation AI
* Enterprise search

---

## Design Principles

### Production First

Designed for:

* Long-running jobs
* Large workspaces
* Enterprise reliability

---

### Safe ETL

Pipeline **never deletes data automatically**.

This prevents:

* Data loss
* Accidental overwrites
* Vector store corruption

---

### Glue-Friendly Engineering

Key optimizations:

* Sequential processing
* Controlled memory usage
* Deterministic execution
* Minimal dependencies

---

## Directory Structure

```
notion-vector-etl/
│
├── notion_vector_etl.py
├── README.md
├── requirements.txt
│
├── notion_downloads_v2/
│
└── manifest.json
```

---

## Incremental Behavior

| Scenario         | Result          |
| ---------------- | --------------- |
| First Run        | Full Processing |
| New Files        | Processed       |
| Existing Files   | Skipped         |
| Interrupted Run  | Resumes         |
| Manifest Missing | Full Run        |

---

## Future Enhancements

Planned improvements:

* Parallel file processing
* Change detection via Notion timestamps
* Automatic vector cleanup
* Metadata enrichment
* Version tracking
* Airflow DAG version

---

## Author

**Zakriya Ahmad**
Senior Data Engineer

Specializations:

* Enterprise ETL Systems
* AWS Data Platforms
* AI Data Pipelines
* Vector Databases
* RAG Architectures
 $50–100/hr clients look for.**
