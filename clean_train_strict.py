"""
Strict mode veri temizleme script'i
Daha sıkı filtreleme kriterleri ile daha temiz ve küçük veri seti üretir

Özellikler:
- Daha yüksek minimum metin uzunluğu (400 karakter)
- Daha sıkı kalite kontrolleri
- HTML/JavaScript kod filtreleme
- Gelişmiş e-ticaret spam tespiti
- Çince karakter filtreleme (herhangi bir Çince karakter → drop)
- Daha agresif risk skorlaması (threshold: 0.25)
"""
from pipeline import process_jsonl_file
from config import config
import os

# Strict mode ayarları - config'deki sıkı eşik değerleri kullanılacak
# Bu değerler config.py'de zaten güncellenmiş durumda:
# - min_text_length: 400
# - max_text_length: 30000
# - min_unique_ratio: 0.45
# - min_sentence_count: 5
# - quality_risk_threshold: 0.25
# - max_http_count: 1

# Quality module aktif (tüm yeni filtreler)
config.use_quality_module = True
config.quality_risk_threshold = 0.25  # Sıkı threshold (default config'den)

# Çıktı dizinini oluştur
os.makedirs("output", exist_ok=True)

print("=" * 70)
print("STRICT MODE - Sıkı Filtreleme ile Veri Temizleme")
print("=" * 70)
print(f"Giriş dosyası: train.jsonl")
print(f"Çıkış dosyası: output/train_cleaned_strict.jsonl")
print()
print("Aktif Filtreler:")
print(f"  ✓ Minimum metin uzunluğu: {config.min_text_length} karakter")
print(f"  ✓ Maximum metin uzunluğu: {config.max_text_length} karakter")
print(f"  ✓ Minimum unique word ratio: {config.min_unique_ratio}")
print(f"  ✓ Minimum cümle sayısı: {config.min_sentence_count}")
print(f"  ✓ Maximum HTTP link sayısı: {config.max_http_count}")
print(f"  ✓ Quality risk threshold: {config.quality_risk_threshold}")
print()
print("Özel Filtreler:")
print("  ✓ HTML/JavaScript kod filtreleme")
print("  ✓ Paragraf ve cümle kalitesi kontrolleri")
print("  ✓ Çince karakter filtreleme (strict: any → drop)")
print("  ✓ Gelişmiş e-ticaret spam tespiti")
print("  ✓ Gelişmiş fiyat ve ürün spec pattern tespiti")
print("=" * 70)
print()

# Tek dosyayı strict mode ile temizle
process_jsonl_file(
    input_file="train.jsonl",
    output_file="output/train_cleaned_strict.jsonl",
    reset_dedup=True,
    progress_interval=10000,  # Her 10K örnekte progress göster
    language_filter_enabled=True,
    dedup_enabled=True,
    use_quality_module=True,  # Tüm yeni filtreler aktif
)

print()
print("=" * 70)
print("✅ STRICT MODE temizleme tamamlandı!")
print("=" * 70)
print(f"Çıktı dosyası: output/train_cleaned_strict.jsonl")
print()
print("Not: Bu mod normal moda göre daha fazla veri filtreler,")
print("ancak daha yüksek kaliteli bir veri seti üretir.")
print("=" * 70)

