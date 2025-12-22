import json

JUDGE_PROMPT = """Sen çok KATI bir veri kalite denetçisisin.
Aşağıdaki metin kurumsal chatbot pretrain datasına girecek.

RISK SCORE: {risk_score:.2f} (0.4-0.7 arası = şüpheli içerik)

KATI KURALLAR - ŞÜPHELİ İSE MUTLAKA DROP:
- PII (telefon, email, TC kimlik, adres) → DROP
- Boilerplate / policy / footer / çerez uyarıları / disclaimer → DROP  
- Spam / SEO / keyword stuffing / link farm / backlink → DROP
- E-ticaret ürün listeleri / fiyat listeleri / "sepete ekle" → DROP
- Forum yorumları / kısa yorumlar ("teşekkürler", "güzel", "beğendim") → DROP
- Astroloji / burç içeriği / fal / tarot → DROP
- Tekrarlayan karakterler (aaaa, !!!!, ...) → DROP
- Sosyal medya spam (hashtag spam, mention spam, "follow", "like") → DROP
- Düşük bilgi değeri / anlamsız içerik / boş yorumlar → DROP
- Toxic / adult / hate / küfür → DROP
- Çince karakterler / aşırı karışık dil → DROP
- Haber başlıkları listesi / sadece başlıklar → DROP
- Ürün teknik özellikleri / boyut tabloları → DROP

SADECE BU DURUMLARDA KEEP:
- Gerçekten yüksek kaliteli, bilgilendirici içerik
- Anlamlı, bütünlüklü metin
- Eğitimsel / bilgilendirici değeri olan içerik
- Forum yorumu değil, asıl içerik

ÖNEMLİ: Risk score {risk_score:.2f} olduğu için bu içerik şüpheli. 
Çok seçici ol ve şüpheli ise MUTLAKA DROP ver.

Sadece JSON döndür:
{{
  "action": "KEEP" | "DROP",
  "reason": "ok|pii|boilerplate|spam|toxic|low_info|forum|seo|ecommerce|astrology|repetitive|social_media|mixed_lang|news_list|product_spec",
  "clean_text": ""
}}

Metin:
<<<{{text}}>>>
"""


def call_llm_api(text: str, risk_score: float = 0.0) -> dict:
    """
    LLM API çağrısı - OpenAI kullanıyor
    API key'i OPENAI_API_KEY environment variable'ından alıyor
    
    Args:
        text: Kontrol edilecek metin
        risk_score: Risk skoru (0.0-1.0), LLM'e bilgi vermek için
    """
    import os
    import json as json_lib
    
    # API KEY'i environment variable'dan oku
    api_key = os.getenv("OPENAI_API_KEY")
    
    if not api_key:
        raise ValueError(
            "API key bulunamadı! "
            "OPENAI_API_KEY environment variable'ını ayarlayın.\n"
            "Örnek: export OPENAI_API_KEY='sk-proj-...'"
        )
    
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError(
            "openai paketi yüklü değil. "
            "Yüklemek için: pip install openai"
        )
    
    client = OpenAI(api_key=api_key)
    
    prompt = JUDGE_PROMPT.format(text=text, risk_score=risk_score)
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # veya "gpt-4o" daha iyi sonuçlar için
            messages=[
                {"role": "system", "content": "Sen çok katı bir veri kalite denetçisisin. Şüpheli içerikleri DROP et. Sadece JSON döndür."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,  # Daha deterministik, katı kararlar
            response_format={"type": "json_object"}
        )
        
        result = json_lib.loads(response.choices[0].message.content)
        return result
        
    except Exception as e:
        # API hatası durumunda güvenli varsayılan (DROP)
        print(f"LLM API hatası: {e}")
        return {
            "action": "DROP",
            "reason": "api_error",
            "clean_text": ""
        }


def run_llm_judge(text: str, risk_score: float = 0.0) -> tuple[str, str, str]:
    """
    LLM judge ile metni değerlendir
    
    Args:
        text: Kontrol edilecek metin
        risk_score: Risk skoru (0.0-1.0), LLM'e bilgi vermek için
    
    Returns:
        (action, text, reason) tuple
        - action: "KEEP" veya "DROP"
        - text: Temizlenmiş metin (KEEP ise)
        - reason: Karar nedeni
    """
    result = call_llm_api(text, risk_score=risk_score)

    action = result.get("action", "DROP")
    reason = result.get("reason", "unknown")

    # CLEAN artık yok, sadece KEEP veya DROP
    if action == "KEEP":
        clean_text = result.get("clean_text", text)
        if clean_text:  # Temizlenmiş metin varsa kullan
            return "KEEP", clean_text, reason
        return "KEEP", text, reason

    # Her durumda DROP
    return "DROP", "", reason

