# ARCHITECTURE NORTHSTAR: STANDARD OPERATING PROCEDURE (SOP)
## Documentation of Real Cloud Computing Use Cases & Web Distribution

This document is the official benchmark and operating procedure (**Northstar Architecture & Rule of Engagement**) that must be activated and followed whenever an instruction includes the keyword: **"sesuai SOP"** or references SOP.

---

### 1. Core Principles & Language Requirement
* **Language Standard:** **English Only**. All generated documentation, HTML web pages, index listings, and explanations must be strictly written in **English**.
* **Trigger Keyword:** Any prompt mentioning **"sesuai SOP"** triggers this guideline automatically.
* **Non-Fictional Guarantee:** All technical use cases must be grounded in verified, real-world industry implementations, official cloud documentation, or open-source software documentation (Never AI hallucinations/fictions unless flagged).

---

### 2. Content Standards & Sources

1. **Real-World Cases:**
   * Sourced from verifiable industry implementations (e.g., Spotify, Shopify, PayPal, Netflix, Twitter/X, Lyft, Google, etc.), official Architecture Centers (Google Cloud, AWS, Azure), and official engineering blogs.
2. **Acceptable Sources:**
   * Official vendor/open-source documentation (e.g., Apache Beam, Apache Spark, GCP Pub/Sub, AWS SNS/SQS).
   * Verified community forums and technical discussions (Stack Overflow, Reddit r/dataengineering, GitHub).
   * Conference talk subtitles and transcripts (Google Cloud Next, AWS re:Invent, Beam Summit, QCon).
3. **AI Fallback Disclaimer:**
   * If no public real-world reference exists after exhaustive research, explicitly state:
     > ⚠️ **Data Integrity Note:** *"The following case study is AI-generated as no public industry reference was found for this specific scenario."*
4. **Citations & References:**
   * Include a dedicated section with the header **References** at the end of each document with links (do not use verbose labels like "References & Official Sources (Non-AI Generated)").

---

### 3. Web UI & Interface Specifications

1. **Header (All Pages):**
   * Brand text: **Cloud Architecture Use Cases** (Do NOT include "SOP" badges or tags).
   * Theme switcher: **Light & Dark Mode** toggle button with persistent state stored in `localStorage`.
   * Navigation buttons: **Back** and **Next** buttons dynamically mapped to adjacent articles.
2. **Index Page (`index.html`):**
   * Header: **Cloud Architecture Use Cases**.
   * Concise list layout: Display only numbered article titles (e.g., `1. Fan-Out vs Single Subscriber Architecture Patterns in Cloud Computing`). Omit verbose subheadings, hero descriptions, and disclaimers.
3. **Article Pages (Pages other than `index.html`):**
   * **Breadcrumb Navigation:** The root link in the breadcrumb path must be labeled **Home** (linking to `index.html`), e.g., `Home / Category / Article Title`. Never use `Cloud Architecture Use Cases` as the breadcrumb root.
   * **No Domain Badges / Meta Tags:** Do not include metadata/domain badge banners under the title (e.g., omit labels like `Domain: Event-Driven Architecture ...`).
   * **Section Heading for Citations:** Use the clean title **References** for the sources/citations section.
4. **Footer:**
   * **No Footer:** Footers must be completely omitted from all HTML pages (`index.html` and article pages).

---

### 4. Dual Deployment Pipeline

Every new or updated HTML file must be deployed through two channels:
1. **GitHub Repository:**
   * Directory: `_source/<filename>.html`.
   * Keep `manifest.json` updated with article order and titles.
   * Commit and push to `origin/main`.
2. **AWS S3 Static Website:**
   * Synchronize `_source/` to `s3://trainocate-global` with `--profile admin` and `--content-type "text/html"`.
   * Public endpoint: `http://trainocate-global.s3-website-us-east-1.amazonaws.com/<filename>.html`
