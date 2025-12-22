import json
from .risk_scoring import compute_risk_score
from .thresholds import KEEP_THRESHOLD, LLM_THRESHOLD
from .llm_judge import run_llm_judge


def quality_pass(
    input_file: str,
    output_file: str,
    dropped_file: str,
    progress_interval: int = 1000,
):
    """
    Quality pass with LLM judge
    
    Args:
        input_file: Input JSONL file
        output_file: Output file for kept items
        dropped_file: Output file for dropped items
        progress_interval: Print progress every N items
    """
    kept = dropped = llm_checked = 0
    total = 0

    with open(input_file, "r", encoding="utf-8") as inp, \
         open(output_file, "w", encoding="utf-8") as out, \
         open(dropped_file, "w", encoding="utf-8") as drop:

        for line in inp:
            total += 1
            text = json.loads(line)["text"]
            score = compute_risk_score(text)

            # Güvenli
            if score < KEEP_THRESHOLD:
                out.write(json.dumps({"text": text}, ensure_ascii=False) + "\n")
                kept += 1
                
                # Progress
                if total % progress_interval == 0:
                    print(f"Progress: {total:,} | Kept: {kept:,} | Dropped: {dropped:,} | LLM checks: {llm_checked:,}")
                
                continue

            # Gri alan → LLM
            if score < LLM_THRESHOLD:
                llm_checked += 1
                
                # LLM progress
                if llm_checked % 100 == 0:
                    print(f"LLM Analizi: {llm_checked:,} / Score: {score:.2f}")
                
                # Risk score'u LLM'e gönder (daha katı karar vermesi için)
                action, new_text, reason = run_llm_judge(text, risk_score=score)
                if action == "KEEP":
                    out.write(json.dumps({"text": new_text}, ensure_ascii=False) + "\n")
                    kept += 1
                else:
                    drop.write(line)
                    dropped += 1
                    # Dropped için reason'ı loglayabiliriz (opsiyonel)
                    if llm_checked % 100 == 0:
                        print(f"  → Dropped (reason: {reason})")
                
                # Progress
                if total % progress_interval == 0:
                    print(f"Progress: {total:,} | Kept: {kept:,} | Dropped: {dropped:,} | LLM checks: {llm_checked:,}")
                
                continue

            # Yüksek risk → DROP
            drop.write(line)
            dropped += 1
            
            # Progress
            if total % progress_interval == 0:
                print(f"Progress: {total:,} | Kept: {kept:,} | Dropped: {dropped:,} | LLM checks: {llm_checked:,}")

    print("\n" + "=" * 60)
    print("=== QUALITY PASS REPORT ===")
    print("=" * 60)
    print(f"Total processed: {total:,}")
    print(f"Kept           : {kept:,} ({kept/total*100:.1f}%)")
    print(f"Dropped        : {dropped:,} ({dropped/total*100:.1f}%)")
    print(f"LLM checks     : {llm_checked:,} ({llm_checked/total*100:.1f}%)")
    print("=" * 60)

