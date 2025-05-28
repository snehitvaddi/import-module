Project Documentation: CCKM Consolidation Clustering Pipeline

## Overview

This multi-stage pipeline processes internal knowledge articles and groups them based on textual similarity using embeddings and hierarchical clustering, while adhering to business constraints on cluster composition and size. It spans five key notebooks—each handling a distinct part of the transformation process from raw article ingestion to rebalanced, validated output clusters. The ultimate goal is to enable accurate grouping of articles that discuss similar topics or content, making downstream discovery and knowledge management more efficient.
The pipeline works on three text feilds: Title, Summary, and Full Content. At each level, vector embeddings are created and similarity-based clustering is performed. Clusters are further refined to respect limits such as maximum article repetitions and ideal size thresholds.

## Notebook 1: Load Batch for Job

**Purpose:** Load a batch of articles from the enterprise knowledge store into a structured DataFrame using a targeted SQL query.

**Functionality Summary:**

* Executes a Databricks SQL query to fetch the most recent batch of internal articles from the CCKM (Centralized Content & Knowledge Management) system.
* Filters results based on `CreatedDate`, `BusinessUnit`, or other metadata to scope the batch appropriately.
* The resulting dataset contains fields like `Title`, `Summary`, `Content`, `ArticleNumber`, and related metadata, which are used in downstream processing.
* Also extracts the batch date from the result filename using regex (e.g., `20250522` from `"Batch_20250522_C1_summary.json"`).

**Key Logic:**
> The SQL query executed (as visible in your screenshot) performs a targeted `SELECT` with filtering on metadata columns to extract articles within scope for the current clustering job. The `WHERE` clause ensures that only valid, non-archived, recently updated knowledge articles are returned.

## Notebook 2: Preprocessing

**Purpose:** Prepare raw article data into a clean, standardized format for downstream embedding and clustering.

**Functionality Summary:**

* **Reads** JSON batch files into a Pandas DataFrame (`pd.read_json()`), each file containing knowledge articles selected in Notebook 1.
* **Cleans the dataset** by removing rows with missing or empty values in key fields such as `Title`, `Summary_txt`, and `Content_txt`.
* **Normalizes and renames columns** (e.g., removing special characters from column names).
* **Extracts key fields**: `ArticleNumber`, `Title`, `Summary_txt`, `Content_txt`, `URL`, `Channel`, `LastPublishedDate`, etc.
* Ensures the resulting DataFrame has a uniform schema to simplify future steps.

**Key Logic:**

* Applies `dropna()` and conditional filters like `df[df["Summary_txt"] != ""]` to ensure all records passed forward contain valid text for embedding.
* Adds derived columns such as lowercase versions of text or cleaned label fields, preparing the content for vectorization.
* Creates a `Title_emb` placeholder column (initially empty), setting the stage for embeddings in Notebook 3.

> What are Keywords and Acronyms here mean?
Here's the updated, concise yet informative documentation for **Notebook 3**, in the same Hugging Face-style, human-readable format:



## Notebook 3: Title-Based Clustering

**Purpose:** Cluster knowledge articles based on the semantic similarity of their **titles** using embedding vectors and agglomerative clustering.

**Functionality Summary:**

* **Generates embeddings** for the `Title` field using an external inference API (`generate_embeddings()`), powered by an ONNX-based Dragon encoder.
* Stores each article's embedding in a new column `Title_emb`.
* **Converts embeddings** into a NumPy matrix and applies **Agglomerative Clustering** with:

  * **Cosine affinity**
  * **Complete linkage**
  * A configured `distance_threshold` (`topic_distance_threshold`, e.g., 1.05)

**Key Logic:**

```python
AgglomerativeClustering(
    n_clusters=None,
    affinity='cosine',
    linkage='complete',
    distance_threshold=topic_distance_threshold
)
```

This unsupervised algorithm groups titles that convey similar semantic meaning into clusters, regardless of exact phrasing.

* Each resulting cluster is saved as a separate JSON file:

  ```
  link_center_True_20250514_center_retail_article_C1.json
  link_center_True_20250514_center_retail_article_C2.json
  ...
  ```

## Notebook 4: Summary and Content-Based Clustering

**Purpose:** Deepen article clustering accuracy using semantic embeddings from both **summaries** and **full article content**, refining the initial title-based groups from Notebook 3.



### Functionality Summary (Three-Stage Clustering):

This notebook enhances semantic grouping across three progressive layers:

1. **Summary-level clustering**
2. **Content chunking + embedding**
3. **Chunk-level similarity and re-clustering**

It builds on the cluster files from Notebook 3 and writes out enriched versions:

* `*_summary.json`
* `*_content.json`
* `*_chunk.json`



### Step 1: Summary Embedding and Clustering

* Reads cluster files like:
  `link_center_True_20250514_center_retail_article_C7.json`

* **Embeds** each article’s `Summary_txt` using `generate_embeddings()`.

* Filters out empty summaries, applies:

  ```python
  AgglomerativeClustering(..., affinity="cosine", distance_threshold=summary_threshold)
  ```

* Singleton clusters are dropped to ensure meaningful groupings.

* Output saved to:
  `Batch_20250514_C7_summary.json`

> Provides a stronger semantic signal than titles by using richer contextual information in summaries.



### Step 2: Full Content Chunking + Embedding

* For each article's `Content_txt`, splits into fixed-size chunks using:

  ```python
  RecursiveCharacterTextSplitter (chunk_size=2000)
  ```

* **Embeds each chunk** independently.

* Stores chunk arrays and their embeddings in:

  * `content_chunks`
  * `content_chunks_emb`



### Step 3: Chunk-to-Chunk Similarity and Final Clustering

* For each pair of articles:

  * Computes cosine similarity between all chunk embeddings.
  * Calculates a custom distance metric:

    ```
    inter_article_dist = 1 - avg(inter_chunk_sim_score)
    ```

* Clusters articles based on these **content-level distances** using agglomerative clustering again.

* Extracts top `k` most similar chunk pairs (e.g., top 3) **above a set similarity threshold** (`chunk_sim_score_threshold = 0.9`), using a heap-based scoring system.

* Results saved as:

  * `highlight_chunk`: most representative section
  * `chunk_sim_score`: strength of chunk-level similarity
  * `final_sim_score`: overall semantic agreement

* Final clusters written to:
  `Batch_20250514_C7_chunk.json`... etc

> This last layer offers **document-level comprehension**, identifying meaningful overlaps in real content, even across articles with different writing styles or structures.



### Final Output Columns

* `Title`, `Summary_txt`, `Content_txt`
* `highlight_chunk`: Best-matching text excerpt

## Notebook 5: Rebalancing Clusters

**Purpose:** Enforce business constraints and optimize cluster sizes by intelligently splitting, merging, and cleaning article groupings from Notebook 4.

### Functionality Summary:

This notebook ensures each cluster:

* Falls within the ideal size range (30–70 articles)
* Has no more than **2 articles per `final_label`** (business rule)
* Maintains traceability to its original cluster

### Step 1: Input Processing

* Loads files like:
  `Batch_20250514_C7_chunk.json`,  `Batch_20250514_C8_chunk.json`,  `Batch_20250514_C9_chunk.json` ..etc

* Extracts cluster ID (e.g., `C7`) and appends it to each `final_label` → `12345_C7`

* Filters out low-confidence rows (`final_sim_score < 0.92`)


### Step 2: Cluster Classification

* Groups clusters based on size:

  * ✅ **Ideal (30–70)** → kept as-is
  * 🔀 **Large (>70)** → split into smaller parts (\~65 each)
  * 🔻 **Small (<30)** → queued for merging

### Step 3: Smart Merging

* Merges small clusters while:

  * Ensuring no more than 2 rows share the same `final_label`
  * Prioritizing valid merges that reach ideal size

> Invalid merges (label conflict or oversized) are skipped automatically.

### Step 4: Constraint Fixes

* Scans for violations: clusters with >2 entries for a label
* Fixes by:

  * Moving extras to eligible clusters
  * Creating mini-clusters for leftovers

### Step 5: Output Generation

* Writes rebalanced clusters to new JSON files:

  * `rebalanced_cluster_1.json`, `rebalanced_cluster_2.json`, etc.
* Generates a report summarizing:

  * Sizes
  * Constraint status
  * Source cluster traceability
  * Remaining violations (if any)

### Why It Matters

This notebook is the **compliance layer** of the pipeline. It ensures that clusters are not only semantically valid—but also **clean, constrained, and production-ready**.

> Without this step, the outputs would break business rules or overwhelm databricks cluster or frontend app with too-large groups.


Let me know if you’d like me to stitch all notebook sections into a unified final version or export to Markdown/PDF.

* `chunk_sim_score`, `final_sim_score`
* `final_label`: Cluster identifier for downstream merging

This is the **core intelligence layer** of the CCKM clustering pipeline. It transforms flat groupings based on titles into **deep, semantically meaningful clusters** using summaries and full article content. It also ensures interpretability through `highlight_chunk` outputs.

> If Notebook 3 builds clusters, Notebook 4 gives them **depth, meaning, and justification**.
