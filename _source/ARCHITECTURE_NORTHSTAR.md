# ARCHITECTURE NORTHSTAR: STANDARD OPERATING PROCEDURE (SOP)
## Dokumentasi Real Cloud Computing Use Cases & Web Distribution

Dokumen ini adalah acuan resmi (**Northstar Architecture & Rule of Engagement**) yang wajib diaktifkan dan dipatuhi setiap kali ada instruksi dengan kata kunci: **"sesuai SOP"**.

---

### 1. Prinsip Dasar & Definisi "Sesuai SOP"
Setiap permintaan teknis (konsep arsitektur, teknik data processing, pipeline design, dsb.) yang menyertakan frasa **"sesuai SOP"** harus menghasilkan dokumentasi berbasis **Real World Production Use Case**, bukan skenario fiktif atau rekayasa AI.

---

### 2. Standar Konten Use Case (Grounding & Verifiable Sources)

1. **Kasus Nyata (Non-Fiktif):**
   * Use case harus berasal dari implementasi nyata di industri / perusahaan tertentu (misal: Spotify, Twitter/X, Lyft, King/Candy Crush, PayPal, Google, dll.), atau arsitektur resmi yang dipublikasikan dalam Google Cloud Architecture Center, AWS Architecture Center, engineering blog resmi perusahaan, atau tech conference talk.
2. **Kategori Sumber yang Valid:**
   * **Dokumentasi Resmi Vendor / Open-Source:** Dokumentasi resmi GCP, AWS, Apache Beam, Apache Spark, Apache Airflow, Flink, Kafka, dll.
   * **Engineering Blog / Whitepapers Perusahaan:** Blog rekayasa perangkat lunak resmi dari pelaku industri.
   * **Forum / Diskusi Komunitas Terverifikasi:** Pembahasan pengalaman arsitek/engineer nyata di Stack Overflow, Reddit (r/dataengineering, r/googlecloud, r/aws), Google Cloud Community, atau GitHub issues/discussions.
   * **Subtitle Video / Tech Talks:** Transkrip / subtitle video konferensi teknis (Google Cloud Next, AWS re:Invent, Beam Summit, QCon, Flink Forward).
3. **Pemberian Catatan Khusus (AI Disclaimer):**
   * Apabila setelah pencarian mendalam tidak ditemukan satupun studi kasus nyata dari industri untuk topik tertentu, **WAJIB** secara eksplisit memberikan label/notice:
     > ⚠️ **Catatan Integritas Data:** *"Studi kasus berikut merupakan hasil analisa/generasi AI dikarenakan belum ditemukan referensi implementasi publik resmi dari industri untuk skenario spesifik ini."*
4. **Sitasi & Tautan:**
   * Di akhir setiap materi/dokumen, cantumkan bab **"Referensi & Sumber Data"** berisi tautan URL atau rujukan spesifik (termasuk judul video/artikel).

---

### 3. Standar Antarmuka & Desain Halaman Web (HTML Template UI)

Setiap dokumen use case disusun dalam format **file HTML** mandiri (*single-file responsive HTML*) yang memiliki standar desain modern:

1. **Light & Dark Theme Toggle:**
   * Diletakkan di bagian atas (**Header**).
   * Preferensi tema disimpan di `localStorage` browser pengunjung agar persisten saat berpindah halaman.
2. **Navigasi Dinamis (Prev / Next):**
   * Diletakkan di header dan footer setiap halaman.
   * Terhubung otomatis antar halaman sesuai urutan katalog materi di folder `_source`.
   * Jika halaman adalah halaman pertama, tombol **Back** dinonaktifkan (`disabled`). Jika halaman terakhir, tombol **Next** dinonaktifkan (`disabled`).
3. **Komponen Visual:**
   * Typography modern (misal: *Plus Jakarta Sans* / *Inter*).
   * Box arsitektur, callout/alert informatif, tabel perbandingan, dan code snippet dengan styling yang rapi dan responsif di mobile maupun desktop.

---

### 4. Alur Distribusi Otomatis (Dual Deployment)

Setiap kali file HTML baru dibuat atau diperbarui, lakukan 2 langkah deployment berikut:

1. **Git Repository GitHub:**
   * Simpan file HTML ke direktori lokal: `D:\OneDrive - Trainocate\Documents\GitHub\data-engineer\_source\<nama-halaman>.html`.
   * Perbarui navigasi file-file HTML sebelumnya agar tombol `Back` dan `Next` saling terhubung secara runtut.
   * Update katalog/index di `_source/index.html` dan `README.md`.
   * Lakukan `git add`, `git commit`, dan `git push origin main`.
2. **AWS S3 Static Website:**
   * Upload file HTML ke bucket S3 dengan profile `admin`:
     ```bash
     & "C:\Program Files\Amazon\AWSCLIV2\aws.exe" s3 sync "D:\OneDrive - Trainocate\Documents\GitHub\data-engineer\_source" s3://trainocate-global/ --content-type "text/html" --profile admin
     ```
   * Akses publik langsung tersedia melalui endpoint:
     `http://trainocate-global.s3-website-us-east-1.amazonaws.com/<nama-halaman>.html`

---

### 5. Skema Registry Halaman (`manifest.json`)
Untuk memastikan tombol **Back** dan **Next** selalu sinkron secara otomatis:
* Buat file registry `D:\OneDrive - Trainocate\Documents\GitHub\data-engineer\_source\manifest.json`.
* File ini mencatat urutan materi:
  ```json
  [
    {
      "id": "1",
      "slug": "dataflow-windowing-use-cases",
      "filename": "dataflow-windowing-use-cases.html",
      "title": "Fixed, Sliding, & Session Windows di Dataflow (Real Use Cases)"
    }
  ]
  ```
* Setiap kali ada halaman baru, sistem membaca `manifest.json`, menyisipkan item baru, dan memperbarui atribut link tombol `<a id="prevBtn">` dan `<a id="nextBtn">` pada file-file HTML terkait sebelum di-push dan di-sync ke S3.
