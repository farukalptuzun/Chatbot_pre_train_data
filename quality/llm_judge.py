import json

JUDGE_PROMPT = """Sen bir veri kalite denetçisisin.
Aşağıdaki metin kurumsal chatbot pretrain datasına girecek.

Kurallar:
- Metni yeniden yazma
- Yeni bilgi ekleme
- Sadece hata varsa minimal temizlik yap

Kararlar:
- PII → CLEAN veya DROP
- Boilerplate / policy / footer → DROP
- Spam / SEO / anlamsız tekrar → DROP
- Toxic / adult / hate → DROP
- Temiz → KEEP

Sadece JSON döndür:
{
  "action": "KEEP" | "DROP" | "CLEAN",
  "reason": "ok|pii|boilerplate|spam|toxic|low_info",
  "clean_text": ""
}

Metin:
<<<{text}>>>
"""


def call_llm_api(text: str) -> dict:
    """
    BURAYI kendi LLM API'nle dolduracaksın.
    Şimdilik stub.
    """
    # ÖRNEK DUMMY ÇIKTI
    return {
        "action": "KEEP",
        "reason": "ok",
        "clean_text": ""
    }


def run_llm_judge(text: str) -> tuple[str, str, str]:
    prompt = JUDGE_PROMPT.format(text=text)
    result = call_llm_api(prompt)

    action = result.get("action", "DROP")
    reason = result.get("reason", "unknown")

    if action == "CLEAN":
        return "KEEP", result.get("clean_text", ""), reason
    if action == "KEEP":
        return "KEEP", text, reason

    return "DROP", "", reason

