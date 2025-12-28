# Kurumsal Chatbot Veri Seti Hazırlama ve Temizleme Pipeline'ı

Modüler ve ölçeklenebilir bir pretrain veri seti hazırlama pipeline'ı. Çeşitli kaynaklardan toplanan ham verileri, kurumsal chatbot eğitimi için hazır, temiz ve kaliteli veri setlerine dönüştürür.

## 📋 İçindekiler

- [Proje Hakkında](#-proje-hakkında)
- [Özellikler](#-özellikler)
- [Kullanılan Teknolojiler](#-kullanılan-teknolojiler)
- [Pipeline Nasıl Çalışır?](#-pipeline-nasıl-çalışır)
- [Kurulum](#-kurulum)
- [Hızlı Başlangıç](#-hızlı-başlangıç)
- [Modül Yapısı](#-modül-yapısı)
- [Konfigürasyon](#-konfigürasyon)
- [Detaylı Kullanım](#-detaylı-kullanım)

## 🎯 Proje Hakkında

Bu proje, kurumsal chatbot modellerinin eğitimi için gerekli olan yüksek kaliteli pretrain veri setlerini hazırlamak amacıyla geliştirilmiştir. Pipeline, çeşitli kaynaklardan (Türkçe/İngilizce Wikipedia, OSCAR-TR, Common Crawl, teknik dokümanlar vb.) toplanan ham verileri işleyerek:

- **Standart formata** dönüştürür
- **Kalite kontrolünden** geçirir
- **Tekrarları** temizler
- **Kişisel bilgileri** filtreler
- **Dil bazlı** filtreleme yapar
- **Hedef oranlarda** veri karışımı oluşturur

Sonuç olarak, LLM fine-tuning için hazır, temiz ve güvenli bir veri seti üretir.

## ✨ Özellikler

### 🔄 Format Normalizasyonu
- Tüm farklı kaynak formatlarını standart `{"text": "..."}` JSONL formatına dönüştürür
- Çoklu veri kaynağı desteği (HuggingFace datasets, JSONL, CSV vb.)

### 🧹 Temel Temizleme
- HTML/XML etiketlerini kaldırır
- Fazla whitespace ve özel karakterleri normalize eder
- Çok kısa/uzun metinleri filtreler
- Spam ve düşük kaliteli içerikleri tespit eder

### 🌍 Dil Filtreleme
- FastText dil modeli ile otomatik dil tespiti
- Sadece belirtilen dilleri (TR/EN) tutar
- Kaynak bazlı dil filtresi (güvenilir kaynaklar için atlanabilir)

### 🔍 Deduplication (Tekrar Temizleme)
- **Exact Dedup**: MD5 hash ile birebir aynı metinleri tespit eder
- **Fuzzy Dedup**: MinHash LSH algoritması ile %90+ benzer metinleri bulur
- Global ve kaynak bazlı deduplication desteği
- Büyük veri setleri için optimize edilmiş

### 🔒 PII (Kişisel Bilgi) Filtreleme
- TC Kimlik Numarası tespiti
- Telefon numarası filtreleme
- E-posta adresi tespiti
- Kredi kartı numarası filtreleme
- Canary string desteği (ezber kontrolü için)

### 📊 Kalite Kontrolü
- Tekrar oranı analizi
- Unique word ratio kontrolü
- Minimum cümle sayısı kontrolü
- Gelişmiş risk skorlama (opsiyonel LLM judge ile)
- Çince karakter filtreleme

### 🔀 Veri Karışımı ve Oran Yönetimi
- Kaynak bazlı hedef oran belirleme
- Otomatik veri karışımı
- Overfetch mekanizması (filtreleme kayıplarını telafi eder)
- Paralel işleme desteği

## 🛠 Kullanılan Teknolojiler

- **Python 3.8+** - Ana programlama dili
- **FastText** - Dil tespiti ve filtreleme
- **DataSketch (MinHash LSH)** - Fuzzy deduplication için
- **HuggingFace Datasets** - Veri kaynaklarından yükleme
- **NumPy** - Sayısal işlemler
- **Multiprocessing** - Paralel işleme desteği

## 🔄 Pipeline Nasıl Çalışır?

Pipeline, verileri aşağıdaki sırayla işler:

```
┌─────────────────────────────────────────────────────────────┐
│  1. Format Normalization                                     │
│     → Tüm inputlar {"text": "..."} formatına çevrilir       │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  2. Basic Cleaning                                          │
│     → HTML/XML temizleme, whitespace normalizasyonu         │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  3. Basic Filter                                            │
│     → Çok kısa/uzun metinler, spam kontrolü                 │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  4. Language Filter                                         │
│     → FastText ile dil tespiti, sadece TR/EN tutulur        │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  5. Exact Deduplication                                     │
│     → MD5 hash ile birebir aynı metinler atılır             │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  6. Fuzzy Deduplication (Opsiyonel)                         │
│     → MinHash LSH ile %90+ benzer metinler atılır           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  7. PII Filter                                              │
│     → Kişisel bilgiler içeren metinler atılır               │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  8. Quality Filter                                          │
│     → Düşük kaliteli metinler atılır                        │
│     → Risk skorlama (opsiyonel LLM judge)                   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  9. Dataset Mixing                                          │
│     → Hedef oranlara göre veri karışımı                     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  10. Output                                                 │
│      → train.jsonl dosyasına yazılır                        │
└─────────────────────────────────────────────────────────────┘
```

## 📦 Kurulum

### Gereksinimler

- Python 3.8 veya üzeri
- pip paket yöneticisi

### Adımlar

1. **Repository'yi klonlayın:**
```bash
git clone https://github.com/farukalptuzun/Chatbot_pre_train_data_cleaning.git
cd Chatbot_pre_train_data_cleaning
```

2. **Bağımlılıkları yükleyin:**
```bash
pip install -r requirements.txt
```

3. **FastText dil modelini indirin (opsiyonel - otomatik indirilebilir):**
```bash
# Manuel indirme (opsiyonel)
wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
```

## 🚀 Hızlı Başlangıç

### Örnek 1: Tek Dosya İşleme

En basit kullanım senaryosu:

```python
from pipeline import process_jsonl_file

process_jsonl_file(
    input_file="raw_data/my_data.jsonl",
    output_file="output/cleaned.jsonl"
)
```

### Örnek 2: Çoklu Veri Kaynağı ile Tam Pipeline

HuggingFace datasetlerinden veri yükleyip işleme:

```python
from data_loaders import load_oscar_tr, load_wikipedia_tr
from pipeline import run_full_pipeline

# Verileri yükle
oscar_file = load_oscar_tr("raw_data/oscar_tr_raw.jsonl")
wiki_file = load_wikipedia_tr("raw_data/wiki_tr_raw.jsonl")

# Pipeline'dan geçir
data_sources = {
    "mc4_tr": oscar_file,
    "wiki_tr": wiki_file,
}

run_full_pipeline(
    data_sources=data_sources,
    output_file="output/train.jsonl",
    use_parallel=True  # Paralel işleme
)
```

### Örnek 3: Birden Fazla Dosyayı Birleştirme

```python
from pipeline import process_multiple_files

process_multiple_files(
    input_files=[
        "raw_data/file1.jsonl",
        "raw_data/file2.jsonl",
        "raw_data/file3.jsonl",
    ],
    output_file="output/combined.jsonl",
    reset_dedup_between=False  # Tüm dosyalar arasında dedup yap
)
```

## 📁 Modül Yapısı

```
Chatbot_pre_train_data_cleaning/
├── config.py                  # Tüm konfigürasyonlar ve ayarlar
├── data_loaders.py            # Veri kaynaklarından yükleme (Wiki, OSCAR, CC)
├── format_normalizer.py       # Format normalizasyonu
├── basic_cleaner.py           # Temel temizlik işlemleri
├── language_filter.py         # Dil filtresi (FastText)
├── deduplication.py           # Deduplication (exact + fuzzy)
├── pii_filter.py              # PII filtreleme
├── quality_filter.py          # Kalite filtresi
├── pipeline.py                # Ana pipeline koordinatörü
├── example_usage.py           # Kullanım örnekleri
├── quality/                   # Gelişmiş kalite kontrol modülleri
│   ├── quality_pipeline.py    # Kalite pipeline'ı
│   ├── risk_scoring.py        # Risk skorlama
│   ├── llm_judge.py           # LLM tabanlı kalite değerlendirme
│   ├── rules.py               # Kalite kuralları
│   └── thresholds.py          # Eşik değerleri
├── output/                    # İşlenmiş veri çıktıları
│   └── train_cleaned.jsonl
└── requirements.txt           # Python bağımlılıkları
```

## ⚙️ Konfigürasyon

Tüm parametreler `config.py` dosyasından özelleştirilebilir:

### Temel Filtreler

```python
from config import config

# Metin uzunluk sınırları
config.min_text_length = 200        # Minimum karakter sayısı
config.max_text_length = 50000      # Maximum karakter sayısı
config.max_http_count = 3           # Maximum HTTP link sayısı
```

### Dil Filtresi

```python
# İzin verilen diller
config.allowed_languages = ["tr", "en"]

# Minimum dil tespit güveni
config.min_lang_confidence = 0.7
```

### Deduplication

```python
# Exact deduplication (varsayılan: açık)
config.exact_dedup_enabled = True

# Fuzzy deduplication (varsayılan: kapalı - performans için)
config.fuzzy_dedup_enabled = False
config.fuzzy_similarity_threshold = 0.9  # %90 benzer metinleri at
```

### Kalite Filtresi

```python
# Minimum unique word ratio
config.min_unique_ratio = 0.3

# Minimum cümle sayısı
config.min_sentence_count = 3

# Gelişmiş risk skorlama (opsiyonel)
config.use_quality_module = True
config.quality_risk_threshold = 0.4
```

### Veri Karışım Oranları

```python
from config import DATASET_MIX, TOTAL_TARGET_EXAMPLES

# Hedef veri seti boyutu
TOTAL_TARGET_EXAMPLES = 10_000_000  # 10M örnek

# Kaynak bazlı hedef oranlar
DATASET_MIX = {
    "mc4_tr": {"target": 0.30},      # %30
    "wiki_tr": {"target": 0.125},    # %12.5
    "wiki_en": {"target": 0.225},    # %22.5
    "tech_docs": {"target": 0.175},  # %17.5
    "c4_en": {"target": 0.075},      # %7.5
}
```

## 📖 Detaylı Kullanım

### Paralel İşleme

Büyük veri setleri için paralel işleme kullanabilirsiniz:

```python
from pipeline import process_and_mix_files_parallel

stats = process_and_mix_files_parallel(
    input_files_with_sources=[
        ("mc4_tr", "raw_data/mc4_tr.jsonl"),
        ("wiki_tr", "raw_data/wiki_tr.jsonl"),
        ("wiki_en", "raw_data/wiki_en.jsonl"),
    ],
    output_file="output/train.jsonl",
    processes=4  # Paralel process sayısı
)
```

### Gelişmiş Kalite Kontrolü

LLM tabanlı kalite değerlendirme kullanımı:

```python
from quality.quality_pipeline import quality_pass

# İlk pipeline'dan geçmiş veriyi kalite kontrolünden geçir
quality_pass(
    input_file="output/cleaned.jsonl",
    output_file="output/final.jsonl",
    dropped_file="output/dropped.jsonl",
    progress_interval=1000
)
```

### PII ve Canary Test

Pipeline, eğitim datasına canary string eklenmesini destekler. Eğitim sonrası modele bu string sorulduğunda, eğer model biliyorsa PII/ezber problemi var demektir:

```python
from pii_filter import CANARY_STRING, add_canary_to_text

# Test için canary ekle
text_with_canary = add_canary_to_text(processed_text)
```

## 📊 Veri Kaynağı Önerileri

Önerilen pretrain veri karışım oranları:

- **%35 Türkçe** - Wiki-TR + OSCAR-TR (MC4-TR)
- **%35 İngilizce** - Wiki-EN + Common Crawl (filtered)
- **%20 Teknik Dokümanlar** - API docs, technical documentation
- **%10 High-Quality Curated** - Manuel seçilmiş yüksek kaliteli metinler

## ⚠️ Notlar ve Sınırlamalar

- Büyük datasetler için fuzzy dedup shard'lanarak yapılabilir (şu an tüm dataset için tek LSH)
- Language model (`lid.176.bin`) ilk kullanımda otomatik indirilmeye çalışılır
- Memory kullanımı için büyük dosyaları parçalara bölerek işleyebilirsiniz
- Fuzzy deduplication varsayılan olarak kapalıdır (performans için)

## 📄 Lisans

Bu proje eğitim ve araştırma amaçlı kullanılabilir.

## 🤝 Katkıda Bulunma

Katkılarınızı bekliyoruz! Lütfen pull request göndermeden önce mevcut kod yapısına uygun olduğundan emin olun.

---

**Geliştirici:** [Faruk Alp Tuzun](https://github.com/farukalptuzun)
