# Direct Marketing Materials Collection & Annotation Pipeline

## Task 1: Requirements Analysis (10%)

### Assumptions on Scale & Volume

| Parameter | Assumption |
|-----------|-----------|
| Daily ingestion volume | ~5,000 marketing materials (flyers, posters, brochures, digital ads) |
| Average file size | ~2 MB (mix of scanned prints and digital screenshots) |
| Daily storage growth | ~10 GB/day raw, ~15 GB/day with derivatives |
| Monthly volume | ~150K items, ~450 GB |
| Regions covered | 50 states, ~200 metro areas |
| Industries tracked | 20+ (retail, real estate, food, automotive, healthcare, etc.) |
| Annotation turnaround | < 24 hours from ingestion to annotated record |
| Concurrent annotators | 10–30 human annotators |
| Query latency (search) | < 2 seconds for metadata queries |
| Retention | 2 years minimum |

### Functional Requirements

| Component | Description |
|-----------|-------------|
| **Ingestion** | Accept images/PDFs via mobile app upload, email forwarding, web scraping, and partner API feeds. Support batch and real-time ingestion. |
| **Pre-processing** | Normalize formats (convert to PNG/JPEG), resize, de-duplicate (perceptual hashing), validate file integrity. |
| **OCR** | Extract all text from marketing materials — headlines, body copy, contact info, offers, disclaimers. |
| **Metadata Extraction** | Classify industry, detect logos/brands, extract colors, layout type, geographic origin, language, campaign type. |
| **Annotation** | Human-in-the-loop labeling: creative type, tone, target audience, call-to-action style, effectiveness rating. |
| **Storage** | Store raw files, processed files, OCR text, ML-extracted metadata, and human annotations in queryable form. |
| **Search & Analytics** | Full-text search on OCR content, filter by industry/region/date, trend dashboards. |

### Non-Functional Requirements

| Requirement | Target |
|-------------|--------|
| **Throughput** | Sustain 5K items/day ingestion; burst to 20K during campaign seasons. |
| **Availability** | 99.5% uptime for ingestion and annotation UI; 99.9% for storage. |
| **Scalability** | Horizontal scaling of OCR/ML workers; storage scales linearly with volume. |
| **Security/Compliance** | Encryption at rest and in transit; RBAC for annotators; audit logs; GDPR-aware PII redaction in OCR output. |
| **Cost** | Target < $3,000/month at steady-state scale. |
| **Latency** | Ingestion-to-searchable < 15 min for real-time; < 4 hours for batch. |

---

## Task 2: Raw Solution Diagram (15%)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                        │
│  [Mobile App]  [Email Ingest]  [Web Scraper]  [Partner API]                 │
└──────┬──────────────┬──────────────┬──────────────┬─────────────────────────┘
       │              │              │              │
       ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      INGESTION GATEWAY                                       │
│         (API Gateway / Message Queue / Event Bus)                            │
│         - Rate limiting, auth, dedup check                                   │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      RAW OBJECT STORE                                        │
│              (Blob Storage — immutable raw files)                             │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   ORCHESTRATOR / WORKFLOW ENGINE                              │
│            (Schedules and coordinates pipeline stages)                        │
└───────┬──────────────┬───────────────┬──────────────┬───────────────────────┘
        │              │               │              │
        ▼              ▼               ▼              ▼
┌────────────┐ ┌─────────────┐ ┌────────────┐ ┌──────────────┐
│ PRE-PROCESS│ │    OCR      │ │  ML/NLP    │ │  ANNOTATION  │
│            │ │   ENGINE    │ │ METADATA   │ │    TOOL      │
│- Normalize │ │- Text       │ │- Industry  │ │- Human-in-   │
│- Resize    │ │  extraction │ │  classify  │ │  the-loop    │
│- Dedup     │ │- Layout     │ │- Logo      │ │- Labeling UI │
│- Validate  │ │  detection  │ │  detect    │ │- QA review   │
└─────┬──────┘ └──────┬──────┘ └─────┬──────┘ └──────┬───────┘
      │               │              │               │
      ▼               ▼              ▼               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PROCESSED OBJECT STORE                                     │
│          (Processed images, thumbnails, derivatives)                          │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     METADATA DATABASE                                         │
│    (Structured store: OCR text, labels, annotations, relationships)          │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   SEARCH & ANALYTICS LAYER                                   │
│         (Full-text search index + dashboards + API)                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Data Flow Summary:**
1. Sources → Ingestion Gateway → Raw Store
2. Orchestrator triggers: Pre-process → OCR → ML Metadata (parallel where possible)
3. Results land in Metadata DB
4. Annotation Tool pulls items needing human review → writes back to Metadata DB
5. Search/Analytics layer indexes Metadata DB for end-user queries

---

## Task 3: Service Analysis (30%)

### Service 1: OCR Engine

#### Option A: Open-Source — Tesseract + EasyOCR

| Aspect | Details |
|--------|---------|
| **What you get** | Free OCR engine; supports 100+ languages; EasyOCR adds deep-learning models for better accuracy on stylized text. |
| **What you still build** | Pre/post-processing pipeline, GPU infrastructure, scaling workers, accuracy monitoring, model fine-tuning for marketing fonts. |
| **Operational burden** | High — manage GPU instances, model updates, handle edge cases (rotated text, overlays). |
| **Lock-in risk** | None — fully portable. |

#### Option B: SaaS — Google Cloud Vision API (Document AI)

| Aspect | Details |
|--------|---------|
| **What you get** | Production-grade OCR, layout detection, handwriting recognition, logo detection, label detection — all via API. Auto-scales. |
| **What you still build** | API integration, result parsing, cost monitoring, fallback logic. |
| **Operational burden** | Low — fully managed, no infra to maintain. |
| **Lock-in risk** | Medium — API-specific response format; switching requires re-integration but data is portable. |

#### Option C: Build from Scratch

| Aspect | Details |
|--------|---------|
| **What you get** | Full control over model architecture, training data, output format. |
| **Cost** | 3–6 months ML engineer time (~$60K–$120K), plus ongoing GPU costs ($500–$2K/month). |
| **When it makes sense** | Only if marketing-specific fonts/layouts cause >20% error rate on existing solutions AND volume justifies investment. |

**Recommendation:** Google Cloud Vision API — accuracy on stylized marketing text is superior, zero ops burden, cost-effective at 150K images/month (~$225/month at $1.50/1000 images).

---

### Service 2: Annotation Tool

#### Option A: Open-Source — Label Studio

| Aspect | Details |
|--------|---------|
| **What you get** | Web UI for image/text annotation, customizable labeling templates, ML-assisted pre-labeling, REST API, multi-user support. |
| **What you still build** | Deployment infrastructure, user auth integration, backup, scaling for concurrent annotators, custom export pipelines. |
| **Operational burden** | Medium — self-hosted on GKE or Compute Engine; need to manage upgrades, DB backups. |
| **Lock-in risk** | Low — open-source, data exportable in standard formats (JSON, COCO, etc.). |

#### Option B: SaaS — Google Vertex AI Data Labeling (or Labelbox)

| Aspect | Details |
|--------|---------|
| **What you get** | Managed annotation workforce, built-in QA workflows, active learning integration, IAM integration. |
| **What you still build** | Task configuration, ontology design, export integration. |
| **Operational burden** | Very low — fully managed. |
| **Lock-in risk** | Medium-High — proprietary formats, workforce tied to platform. |

#### Option C: Build from Scratch

| Aspect | Details |
|--------|---------|
| **What you get** | Fully custom UI tailored to marketing material taxonomy. |
| **Cost** | 2–4 months full-stack dev (~$40K–$80K), ongoing maintenance. |
| **When it makes sense** | Only if annotation taxonomy is highly specialized and no existing tool supports the workflow. |

**Recommendation:** Label Studio (self-hosted on GKE) — flexible, no per-label cost, supports custom ML backends for pre-annotation, low lock-in.

---

### Service 3: Workflow Orchestration

#### Option A: Open-Source — Apache Airflow

| Aspect | Details |
|--------|---------|
| **What you get** | DAG-based scheduling, rich operator ecosystem, monitoring UI, retry/backfill logic, community plugins. |
| **What you still build** | Infrastructure (or use managed), custom operators for GCS/Vision API, alerting integration. |
| **Operational burden** | Medium if self-hosted; Low if using Cloud Composer. |
| **Lock-in risk** | Low — open-source standard, portable DAGs. |

#### Option B: SaaS — Google Cloud Composer (Managed Airflow)

| Aspect | Details |
|--------|---------|
| **What you get** | Fully managed Airflow: auto-scaling workers, integrated IAM, GCS/BigQuery operators pre-installed, monitoring. |
| **What you still build** | DAG code, custom operators if needed. |
| **Operational burden** | Low — Google manages infra, upgrades, HA. |
| **Lock-in risk** | Low — DAGs are standard Airflow; can migrate to self-hosted. |

#### Option C: Build from Scratch (Custom scheduler)

| Aspect | Details |
|--------|---------|
| **What you get** | Minimal task runner tailored to pipeline. |
| **Cost** | 1–2 months dev, ongoing maintenance for reliability. |
| **When it makes sense** | Almost never for this use case — orchestration is a solved problem. |

**Recommendation:** Google Cloud Composer — eliminates ops burden, native GCP integration, DAGs remain portable.

---

### Service 4: Object Storage

#### Option A: Google Cloud Storage (GCS)

| Aspect | Details |
|--------|---------|
| **What you get** | Unlimited scalable blob storage, lifecycle policies, versioning, event notifications (Pub/Sub), IAM, encryption at rest. |
| **Operational burden** | Near zero — fully managed. |
| **Lock-in risk** | Low — S3-compatible API available; data easily exportable. |

#### Option B: Self-hosted MinIO

| Aspect | Details |
|--------|---------|
| **What you get** | S3-compatible API, runs on own infra. |
| **Operational burden** | High — manage disks, replication, backups, scaling. |
| **When it makes sense** | On-prem requirements or extreme cost sensitivity at petabyte scale. |

**Recommendation:** GCS — cost-effective at this scale, zero ops, native integration with Vision API and Composer.

---

## Task 4: Detailed Comparison Table (20%)

### Annotation Tool Comparison

| Criteria | Label Studio (Self-hosted on GKE) | Vertex AI Data Labeling | Labelbox (SaaS) |
|----------|-----------------------------------|------------------------|-----------------|
| **Pricing Model** | Free (open-source) + infra cost | Per-label: $0.08–$0.12/label (human) | Per-seat: $500/user/month |
| **Est. Monthly Cost** | ~$150 (e2-standard-4 + Cloud SQL) | ~$12,000 (150K items × $0.08) | ~$5,000 (10 seats) |
| **Labeling UI** | ✅ Customizable templates | ✅ Standard templates | ✅ Advanced UI |
| **ML Pre-labeling** | ✅ Custom ML backend | ✅ AutoML integration | ✅ Model-assisted |
| **Multi-user** | ✅ RBAC | ✅ IAM | ✅ Teams |
| **QA/Review Workflow** | ✅ (basic, needs config) | ✅ Built-in | ✅ Advanced |
| **API Access** | ✅ REST API | ✅ gRPC/REST | ✅ GraphQL |
| **Export Formats** | JSON, COCO, VOC, CSV | BigQuery, JSON | JSON, COCO, custom |
| **Custom Taxonomy** | ✅ Fully flexible | ⚠️ Limited templates | ✅ Flexible |
| **Active Learning** | ✅ (with custom ML backend) | ✅ Native | ✅ Native |
| **Data Residency** | ✅ You control | GCP regions | Labelbox cloud |
| **Lock-in Risk** | Low | Medium-High | Medium |
| **Scalability** | Manual (GKE autoscale) | Automatic | Automatic |

### Cost Breakdown at Scale (150K items/month, 10 annotators)

| Component | Label Studio | Vertex AI | Labelbox |
|-----------|-------------|-----------|----------|
| Platform | $0 | $0 | $5,000 |
| Infra (compute) | $100 | $0 | $0 |
| Database | $50 | $0 | $0 |
| Per-label cost | $0 | $12,000 | $0 |
| **Total** | **~$150/month** | **~$12,000/month** | **~$5,000/month** |

### Recommendation

**Label Studio on GKE** is the justified choice because:
1. **Cost**: 80× cheaper than Vertex AI Data Labeling at our volume
2. **Flexibility**: Fully customizable taxonomy for marketing-specific labels (creative type, tone, CTA style)
3. **ML Backend**: Supports custom pre-annotation models — we can feed Vision API results as pre-labels to speed up human annotation
4. **Lock-in**: Zero vendor lock-in; data stays in our GCS buckets
5. **Scale**: GKE handles autoscaling for burst annotation periods
6. **Trade-off accepted**: Medium ops burden is manageable with GKE + Helm chart deployment

---

## Task 5: Final Architecture with Specific Services (25%)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                            │
│  [Mobile App]    [Cloud Functions    [Scrapy on        [Partner webhooks         │
│   (Flutter)       email trigger]      GCE/GKE]          via Cloud Endpoints]     │
└──────┬───────────────┬───────────────────┬───────────────────┬──────────────────┘
       │               │                   │                   │
       ▼               ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│              GOOGLE CLOUD PUB/SUB  [Managed]                                     │
│   Justification: Decouples ingestion from processing; handles burst traffic      │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│          GOOGLE CLOUD STORAGE (GCS)  [Managed]                                   │
│          Buckets: raw/, processed/, thumbnails/                                   │
│   Justification: Unlimited scale, $0.02/GB, native Vision API integration        │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│        GOOGLE CLOUD COMPOSER (Managed Airflow)  [Managed]                        │
│   Justification: Zero-ops orchestration; portable DAGs; native GCP operators     │
└────────┬─────────────┬────────────────┬─────────────────┬───────────────────────┘
         │             │                │                 │
         ▼             ▼                ▼                 ▼
┌──────────────┐ ┌───────────────┐ ┌──────────────┐ ┌──────────────────┐
│ PRE-PROCESS  │ │ GOOGLE CLOUD  │ │ VERTEX AI    │ │ LABEL STUDIO     │
│ Cloud Func.  │ │ VISION API    │ │ (Custom      │ │ on GKE           │
│ [Managed]    │ │ [Managed]     │ │  Model)      │ │ [Self-hosted]    │
│              │ │               │ │ [Managed]    │ │                  │
│- ImageMagick │ │- OCR          │ │- Industry    │ │- Human annotation│
│- pHash dedup │ │- Logo detect  │ │  classifier  │ │- QA workflows    │
│- Validation  │ │- Label detect │ │- Layout type │ │- Pre-labeling    │
│              │ │- Text layout  │ │  detection   │ │  from Vision API │
│Justification:│ │Justification: │ │Justification:│ │Justification:    │
│Serverless,   │ │Best accuracy  │ │Custom models │ │Free, flexible,   │
│scales to 0   │ │on marketing   │ │for domain-   │ │no per-label cost │
│              │ │text; $1.50/1K │ │specific tasks│ │at 150K items/mo  │
└──────┬───────┘ └───────┬───────┘ └──────┬───────┘ └────────┬─────────┘
       │                 │                │                   │
       ▼                 ▼                ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│              GOOGLE CLOUD SQL (PostgreSQL)  [Managed]                             │
│   Tables: materials, ocr_results, ml_metadata, annotations                       │
│   Justification: ACID compliance for annotations; familiar SQL; managed HA       │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│              ELASTICSEARCH on GKE  [Self-hosted]                                  │
│   Justification: Full-text search on OCR content; faceted filtering;             │
│   self-hosted avoids Elastic Cloud cost at our index size                         │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│              LOOKER STUDIO + Custom API (Cloud Run)  [Managed]                    │
│   Justification: Free dashboards on GCP data; Cloud Run for custom search API    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Service Choices Summary

| Component | Service | Managed/Self-hosted | Justification (traced to Task 1) |
|-----------|---------|--------------------|---------------------------------|
| Event Bus | Cloud Pub/Sub | Managed | Handles 20K burst ingestion (throughput req); decouples sources |
| Object Storage | GCS | Managed | 99.999% durability (availability req); $0.02/GB (cost req) |
| Orchestration | Cloud Composer | Managed | Reliable scheduling (availability req); portable DAGs (lock-in) |
| Pre-processing | Cloud Functions | Managed | Auto-scales to 0 (cost req); handles burst (scalability req) |
| OCR | Cloud Vision API | Managed | <2s per image (latency req); no GPU ops (cost/ops req) |
| ML Classification | Vertex AI | Managed | Custom industry classifier; auto-scaling endpoints (scalability) |
| Annotation | Label Studio on GKE | Self-hosted | $150 vs $12K/mo (cost req); custom taxonomy (functional req) |
| Metadata DB | Cloud SQL (PostgreSQL) | Managed | ACID for annotations (security req); managed HA (availability) |
| Search | Elasticsearch on GKE | Self-hosted | Full-text OCR search <2s (latency req); avoids $800/mo Elastic Cloud |
| Dashboards | Looker Studio | Managed | Free; connects to Cloud SQL/BigQuery natively |
| Search API | Cloud Run | Managed | Scales to 0 (cost req); serves search UI |

### Estimated Monthly Cost

| Service | Monthly Cost |
|---------|-------------|
| GCS (500 GB) | $10 |
| Cloud Pub/Sub | $5 |
| Cloud Composer (small) | $400 |
| Cloud Functions | $20 |
| Cloud Vision API (150K calls) | $225 |
| Vertex AI endpoint | $200 |
| Cloud SQL (db-f1-micro + storage) | $50 |
| GKE cluster (Label Studio + ES) | $250 |
| Cloud Run | $15 |
| Looker Studio | $0 |
| **Total** | **~$1,175/month** |

✅ Well within the $3,000/month cost target from Task 1.
