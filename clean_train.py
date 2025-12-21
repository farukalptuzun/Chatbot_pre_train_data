"""
6M veriyi temizlemek için basit script
train.jsonl -> train_cleaned.jsonl
"""
from pipeline import process_jsonl_file
from config import config
import os

# Quality module aktif (tüm yeni filtreler)
config.use_quality_module = True
config.quality_risk_threshold = 0.4

# Çıktı dizinini oluştur
os.makedirs("output", exist_ok=True)

print("=" * 60)
print("6M Veri Temizleme Başlatılıyor...")
print("=" * 60)
print(f"Giriş: train.jsonl")
print(f"Çıkış: output/train_cleaned.jsonl")
print(f"Quality Module: Aktif (Risk threshold: {config.quality_risk_threshold})")
print("=" * 60)
print()

# Tek dosyayı temizle
process_jsonl_file(
    input_file="train.jsonl",
    output_file="output/train_cleaned.jsonl",
    reset_dedup=True,
    progress_interval=10000,  # Her 10K örnekte progress göster
    language_filter_enabled=True,
    dedup_enabled=True,
    use_quality_module=True,  # Tüm yeni filtreler aktif
)

print()
print("=" * 60)
print("✅ Temizleme tamamlandı!")
print("=" * 60)
print(f"Çıktı dosyası: output/train_cleaned.jsonl")

