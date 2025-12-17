import json
from .risk_scoring import compute_risk_score
from .thresholds import KEEP_THRESHOLD, LLM_THRESHOLD
from .llm_judge import run_llm_judge


def quality_pass(
    input_file: str,
    output_file: str,
    dropped_file: str,
):
    kept = dropped = llm_checked = 0

    with open(input_file, "r", encoding="utf-8") as inp, \
         open(output_file, "w", encoding="utf-8") as out, \
         open(dropped_file, "w", encoding="utf-8") as drop:

        for line in inp:
            text = json.loads(line)["text"]
            score = compute_risk_score(text)

            # Güvenli
            if score < KEEP_THRESHOLD:
                out.write(json.dumps({"text": text}, ensure_ascii=False) + "\n")
                kept += 1
                continue

            # Gri alan → LLM
            if score < LLM_THRESHOLD:
                llm_checked += 1
                action, new_text, _ = run_llm_judge(text)
                if action == "KEEP":
                    out.write(json.dumps({"text": new_text}, ensure_ascii=False) + "\n")
                    kept += 1
                else:
                    drop.write(line)
                    dropped += 1
                continue

            # Yüksek risk → DROP
            drop.write(line)
            dropped += 1

    print("=== QUALITY PASS REPORT ===")
    print(f"Kept      : {kept}")
    print(f"Dropped   : {dropped}")
    print(f"LLM checks: {llm_checked}")

