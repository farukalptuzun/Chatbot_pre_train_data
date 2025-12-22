"""
LLM Judge ile 2. temizlik
train_cleaned.jsonl -> train_final.jsonl
"""
from quality.quality_pipeline import quality_pass
import os

# Çıktı dizinini oluştur
os.makedirs("output", exist_ok=True)

print("=" * 60)
print("LLM Judge ile 2. Temizlik Başlatılıyor...")
print("=" * 60)
print(f"Giriş: output/train_cleaned.jsonl")
print(f"Çıkış: output/train_final.jsonl")
print(f"Dropped: output/train_dropped.jsonl")
print()
print("Not: LLM API çağrıları yapılacak.")
print("quality/llm_judge.py dosyasındaki call_llm_api() fonksiyonunu")
print("kendi API anahtarlarınızla doldurun.")
print("=" * 60)
print()

# Quality pass çalıştır
quality_pass(
    input_file="output/train_cleaned.jsonl",
    output_file="output/train_final.jsonl",
    dropped_file="output/train_dropped.jsonl",
    progress_interval=1000,  # Her 1000 örnekte progress göster
)

print()
print("=" * 60)
print("✅ LLM Judge temizliği tamamlandı!")
print("=" * 60)
print(f"Final çıktı: output/train_final.jsonl")
print(f"Filtrelenen: output/train_dropped.jsonl")

