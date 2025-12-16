# Pretrain Data Pipeline

Modüler pretrain data hazırlama pipeline'ı. Çeşitli kaynaklardan (TR Wiki, OSCAR-TR, EN Wiki, Common Crawl, teknik dokümanlar) temiz ve kaliteli pretrain datası oluşturur.

## Özellikler

✅ **Format Normalization** - Tüm kaynakları standart `{"text": "..."}` formatına çevirir  
✅ **Basic Cleaning** - HTML, fazla whitespace, çok kısa/uzun metinleri temizler  
✅ **Language Filter** - Sadece belirtilen dilleri (TR/EN) tutar (fasttext ile)  
✅ **Deduplication** - Exact ve fuzzy dedup (MinHash LSH ile %90 benzerlik)  
✅ **PII Filter** - Kişisel bilgileri (TC, telefon, email, kredi kartı) filtreler  
✅ **Quality Filter** - Düşük kaliteli metinleri (tekrar, spam) filtreler  

## Kurulum

```bash
# Bağımlılıkları yükle
pip install -r requirements.txt

# Fasttext dil modelini indir (opsiyonel - otomatik indirilebilir)
# wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
```

## Modül Yapısı

```
├── config.py              # Tüm konfigürasyonlar
├── data_loaders.py        # Veri kaynaklarından yükleme (Wiki, OSCAR, CC)
├── format_normalizer.py   # Format normalizasyonu
├── basic_cleaner.py       # Temel temizlik
├── language_filter.py     # Dil filtresi
├── deduplication.py       # Deduplication (exact + fuzzy)
├── pii_filter.py          # PII filtreleme
├── quality_filter.py      # Kalite filtresi
├── pipeline.py            # Ana pipeline
└── example_usage.py       # Kullanım örnekleri
```

## Hızlı Başlangıç

### Örnek 1: Tek dosya işleme

```python
from pipeline import process_jsonl_file

process_jsonl_file(
    input_file="raw_data/my_data.jsonl",
    output_file="output/cleaned.jsonl"
)
```

### Örnek 2: HuggingFace datasetlerinden yükleme ve işleme

```python
from data_loaders import load_oscar_tr, load_wikipedia_tr
from pipeline import run_full_pipeline

# Verileri yükle
oscar_file = load_oscar_tr("raw_data/oscar_tr_raw.jsonl")
wiki_file = load_wikipedia_tr("raw_data/wiki_tr_raw.jsonl")

# Pipeline'dan geçir
data_sources = {
    "oscar_tr": oscar_file,
    "wiki_tr": wiki_file,
}

run_full_pipeline(
    data_sources=data_sources,
    output_file="output/train.jsonl"
)
```

### Örnek 3: Birden fazla dosyayı birleştirme

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

## Konfigürasyon

`config.py` dosyasından tüm parametreleri özelleştirebilirsiniz:

```python
from config import config

# Temel filtreler
config.min_text_length = 200        # Minimum karakter sayısı
config.max_text_length = 50000      # Maximum karakter sayısı

# Dil filtresi
config.allowed_languages = ["tr", "en"]
config.min_lang_confidence = 0.7

# Deduplication
config.fuzzy_similarity_threshold = 0.9  # %90 benzer metinleri at

# Kalite filtresi
config.min_unique_ratio = 0.3       # Minimum unique word ratio
config.min_sentence_count = 3       # Minimum cümle sayısı
```

## Pipeline Adımları

Pipeline şu sırayla çalışır:

1. **Format Normalization** → Tüm inputlar `{"text": "..."}` formatına çevrilir
2. **Basic Cleaning** → HTML temizleme, whitespace normalizasyonu
3. **Basic Filter** → Çok kısa/uzun metinler, spam kontrolü
4. **Language Filter** → Sadece belirtilen diller tutulur
5. **Exact Dedup** → MD5 hash ile birebir aynı metinler atılır
6. **Fuzzy Dedup** → MinHash LSH ile benzer metinler atılır (%90+ benzerlik)
7. **PII Filter** → Kişisel bilgiler içeren metinler atılır
8. **Quality Filter** → Düşük kaliteli metinler atılır
9. **Output** → `train.jsonl` dosyasına yazılır

## Veri Kaynağı Önerileri

Önerilen pretrain mix oranları:

- **%35 TR** - Wiki-TR + OSCAR-TR
- **%35 EN** - Wiki-EN + Common Crawl (filtered)
- **%20 Teknik Dokümanlar** - API docs, technical documentation
- **%10 High-Quality Curated** - Manuel seçilmiş yüksek kaliteli metinler

## PII ve Canary Test

Pipeline, eğitim datasına canary string (`ZXQJ_CANARY_492837`) eklenmesini destekler. Eğitim sonrası modele bu string sorulduğunda, eğer model biliyorsa PII/ezber problemi var demektir.

```python
from pii_filter import CANARY_STRING, add_canary_to_text

# Test için canary ekle
text_with_canary = add_canary_to_text(processed_text)
```

## Notlar

- Büyük datasetler için fuzzy dedup shard'lanarak yapılabilir (şu an tüm dataset için tek LSH)
- Language model (`lid.176.bin`) ilk kullanımda otomatik indirilmeye çalışılır
- Memory kullanımı için büyük dosyaları parçalara bölerek işleyebilirsiniz

## Lisans

Bu proje eğitim ve araştırma amaçlı kullanılabilir.
