"""
agent.py

Armenian Voice AI Support Agent — LiveKit Agents v1.5.0

Architecture:
    User voice → STT → LLM (GPT-4o-mini) + RAG → TTS → User

STT/TTS providers are configurable via .env:
    STT_PROVIDER=openai     # whisper-1  (default)
    STT_PROVIDER=deepgram   # nova-2, better Armenian accuracy

    TTS_PROVIDER=openai     # tts-1      (default)
    TTS_PROVIDER=elevenlabs # eleven_multilingual_v2

Guardrails:
    - Only answers questions about: credits, deposits, branch locations
    - Refuses all other topics politely in Armenian
    - All responses in Armenian

Usage:
    python agent.py start   # connect to LiveKit server
    python agent.py dev     # development mode (auto-reload)
"""

from __future__ import annotations

import logging
import os
import re

from dotenv import load_dotenv
from livekit.agents import (
    Agent,
    AgentSession,
    AutoSubscribe,
    JobContext,
    WorkerOptions,
    cli,
)
from livekit.plugins import openai, silero
import openai as openai_module

from rag.retriever import BankRetriever

load_dotenv()

logger = logging.getLogger("agent")

# Optional plugins — imported only if configured
_deepgram = None
_elevenlabs = None

if os.getenv("STT_PROVIDER", "openai").lower() == "deepgram":
    try:
        from livekit.plugins import deepgram as _deepgram
    except ImportError:
        logger.warning("deepgram plugin not installed, falling back to openai STT")

if os.getenv("TTS_PROVIDER", "openai").lower() == "elevenlabs":
    try:
        from livekit.plugins import elevenlabs as _elevenlabs
    except ImportError:
        logger.warning("elevenlabs plugin not installed, falling back to openai TTS")

# ---------------------------------------------------------------------------
# Armenian number conversion
# ---------------------------------------------------------------------------

_ONES = [
    "", "մեկ", "երկու", "երեք", "չորս", "հինգ",
    "վեց", "յոթ", "ութ", "ինն", "տաս",
]
_TEENS = [
    "տաս", "տասնմեկ", "տասներկու", "տասներեք", "տասնչորս",
    "տասնհինգ", "տասնվեց", "տասնյոթ", "տասնութ", "տասնինն",
]
_TENS = [
    "", "տաս", "քսան", "երեսուն", "քառասուն",
    "հիսուն", "վաթսուն", "յոթանասուն", "ութսուն", "իննսուն",
]


def _arm_num(n: int) -> str:
    """Convert integer to Armenian words."""
    if n < 0:
        return "մինուս " + _arm_num(-n)
    if n == 0:
        return "զրո"
    if n <= 10:
        return _ONES[n]
    if 11 <= n <= 19:
        return _TEENS[n - 10]
    if n < 100:
        t = _TENS[n // 10]
        o = _ONES[n % 10]
        return t if n % 10 == 0 else t + o
    if n < 1000:
        hundreds = "հարյուր" if n // 100 == 1 else _ONES[n // 100] + " հարյուր"
        rest = n % 100
        return hundreds + ("" if rest == 0 else " " + _arm_num(rest))
    if n < 1_000_000:
        thousands = "հազար" if n // 1000 == 1 else _arm_num(n // 1000) + " հազար"
        rest = n % 1000
        return thousands + ("" if rest == 0 else " " + _arm_num(rest))
    if n < 1_000_000_000:
        millions = "միլիոն" if n // 1_000_000 == 1 else _arm_num(n // 1_000_000) + " միլիոն"
        rest = n % 1_000_000
        return millions + ("" if rest == 0 else " " + _arm_num(rest))
    return str(n)


def numbers_to_armenian(text: str) -> str:
    """
    Convert numbers and patterns in text to Armenian words before TTS.

    Handles:
      - Times:        09:30  → ինն անց կես
                      17:00  → տասնյոթ
      - Slash nums:   21/34  → քսանմեկ դրոփ երեսունչորս
      - Address nums: 29ա    → քսանինն ա
      - Phone:        374 20 10 10 → երեք հարյուր յոթանասուն չորս քսան տաս տաս
      - Plain nums:   500000 → հինգ հարյուր հազար
    """

    # ── Times: 09:30, 17:00, 9:05 ────────────────────────────────────────────
    def replace_time(m: re.Match) -> str:
        h = int(m.group(1))
        mn = int(m.group(2))
        # 12-ժամյա ձևաչափ
        h12 = h if h <= 12 else h - 12
        h_word = _arm_num(h12)
        if mn == 0:
            return h_word
        if mn == 30:
            return f"{h_word}ն անց կես"
        return f"{h_word}ն անց {_arm_num(mn)}"

    text = re.sub(r'\b(\d{1,2}):(\d{2})\b', replace_time, text)

    # ── Slash numbers: 21/34, 51/3 ───────────────────────────────────────────
    def replace_slash(m: re.Match) -> str:
        return f"{_arm_num(int(m.group(1)))} դրոփ {_arm_num(int(m.group(2)))}"

    text = re.sub(r'\b(\d+)/(\d+)\b', replace_slash, text)

    # ── Address letters: 29ա, 14բ ────────────────────────────────────────────
    def replace_addr(m: re.Match) -> str:
        return f"{_arm_num(int(m.group(1)))} {m.group(2)}"

    text = re.sub(r'\b(\d+)([աբգդեզէըթժիլխծկհձղճմյնշոչպջռսվտրցւփքօֆ])\b',
                  replace_addr, text)

    # ── Phone patterns ────────────────────────────────────────────────────────
    # Strip +, ( ) so parens around country code don't break matching
    # e.g. "(374 12) 22 22 22" → "374 12 22 22 22"
    # Keep hyphens that connect Armenian suffixes (e.g. 500,000-ից)
    text = re.sub(r'[+()]', '', text)
    text = re.sub(r' {2,}', ' ', text)  # collapse extra spaces

    # Match phone-like: 3+ groups of 2-6 digits separated by spaces
    # e.g. 374 12 22 22 22  or  374 10 561111
    def replace_phone(m: re.Match) -> str:
        parts = m.group(0).split()
        return " ".join(_arm_num(int(p)) for p in parts)

    text = re.sub(
        r'\b\d{2,6}(?:\s+\d{2,6}){2,}\b',
        replace_phone,
        text,
    )

    # 8.5% → ութ ամբողջ հինգ տոկոս
    def replace_percent(m: re.Match) -> str:
        integer_part = int(m.group(1))
        decimal_part = m.group(3)
        if decimal_part:
            return f"{_arm_num(integer_part)} ամբողջ {_arm_num(int(decimal_part))} տոկոս"
        return f"{_arm_num(integer_part)} տոկոս"

    text = re.sub(r'(\d+)(\.(\d+))?%', replace_percent, text)

    # ── Large plain numbers: 500,000 / 500000 (NO spaces — those are phones) ─
    def replace_large(m: re.Match) -> str:
        num_str = m.group(0).replace(',', '')
        try:
            return _arm_num(int(num_str))
        except ValueError:
            return m.group(0)

    text = re.sub(r'\b\d[\d,]*\d\b', replace_large, text)

    # ── Single remaining digits ───────────────────────────────────────────────
    text = re.sub(r'\b(\d+)\b', lambda m: _arm_num(int(m.group(1))), text)

    return text


# ---------------------------------------------------------------------------
# Section / bank classifiers
# ---------------------------------------------------------------------------

SECTION_KEYWORDS: dict[str, list[str]] = {
    "credits": [
        "վարկ", "վարկի", "վարկեր", "վարկային",
        "հիփոթեք", "ավտովարկ", "օվերդրաֆտ", "overdraft",
        "տոկոս", "տոկոսադրույք", "loan", "credit",
        "մարում", "ժամկետ", "կանխավճար", "երաշխավոր", "գրավ",
    ],
    "deposits": [
        "ավանդ", "ավանդի", "ավանդներ", "deposit",
        "խնայողական", "կուտակային", "տոկոս", "տոկոսադրույք",
        "արժույթ", "դրամ", "դոլար", "եվրո", "ներդրում",
    ],
    "branches": [
        "մասնաճյուղ", "մասնաճյուղի", "մասնաճյուղեր", "branch",
        "հասցե", "հասցեն", "գտնվում", "աշխատանքային ժամ",
        "բաց է", "փակ է", "երկուշաբթի", "շաբաթ", "կիրակի",
        "հեռախոս", "հեռ", "գրասենյակ",
    ],
}

BANK_KEYWORDS: dict[str, list[str]] = {
    "Ameriabank": ["ամերիա", "ameriabank", "ամերիաբանկ"],
    "Ardshinbank": ["արդշին", "ardshinbank", "արդշինբանկ"],
    "Inecobank": ["ինեկո", "inecobank", "ինեկոբանկ"],
}

ALLOWED_SECTIONS = {"credits", "deposits", "branches"}


def detect_section(text: str) -> str | None:
    text_lower = text.lower()
    scores = {s: 0 for s in ALLOWED_SECTIONS}
    for section, keywords in SECTION_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                scores[section] += 1
    best = max(scores, key=lambda s: scores[s])
    return best if scores[best] > 0 else None


def detect_bank(text: str) -> str | None:
    text_lower = text.lower()
    for bank, keywords in BANK_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                return bank
    return None


def is_allowed_question(text: str) -> bool:
    if detect_section(text) is not None:
        return True
    FUZZY_ROOTS = [
        "վարկ", "վարգ", "վերկ",
        "ավանդ", "ավընդ",
        "տոկոս", "տոքոս", "տոքուս",
        "մասնաճ", "մասնա",
        "սպառ", "պառ", "սպա",
        "հիփոթ", "հիպոթ",
        "ամերիա", "արդշին", "ինեկո",
        # Քաղաքներ — Whisper-ը հաճախ սխալ հոլովով է գրում
        "գյումր", "վանաձ", "աբով", "հրազդ", "կապան",
    ]
    text_lower = text.lower()
    return any(root in text_lower for root in FUZZY_ROOTS)


def build_stt():
    provider = os.getenv("STT_PROVIDER", "openai").lower()
    if provider == "deepgram" and _deepgram:
        logger.info("STT: Deepgram nova-2")
        return _deepgram.STT(model="nova-2", language="hy")
    logger.info("STT: OpenAI Whisper-1")
    return openai.STT(
        model="whisper-1",
        language="hy",
        prompt=(
            "Հայկական բանկ։ վարկ, ավանդ, մասնաճյուղ, մասնաճյուղեր, մասնաճյուղի, "
            "տոկոս, տոկոսադրույք, դոլար, եվրո, դրամ, "
            "Ամերիաբանկ, Արդշինբանկ, Ինեկոբանկ, սպառողական, հիփոթեք, "
            "ավտովարկ, կուտակային, հասցե, աշխատանքային ժամ, "
            "Երևան, Գյումրի, Վանաձոր, Մալաթիա, Շենգավիթ, Զեյթուն, "
            "Մալաթիա, Նուբարաշեն, Նոր Նորք, Ավան, Էրեբունի"
            "Աջափնյակ, Արաբկիր, Կենտրոն, Նոր Նորք, Նորք Մարաշ։"
        ),
    )


def build_tts():
    provider = os.getenv("TTS_PROVIDER", "openai").lower()
    if provider == "elevenlabs" and _elevenlabs:
        voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")
        logger.info("TTS: ElevenLabs eleven_multilingual_v2 voice=%s", voice_id)
        return _elevenlabs.TTS(voice_id=voice_id, model_id="eleven_multilingual_v2")
    logger.info("TTS: OpenAI tts-1 voice=nova")
    return openai.TTS(model="tts-1", voice="nova")


# ---------------------------------------------------------------------------
# Armenian Bank Agent
# ---------------------------------------------------------------------------

SYSTEM_INSTRUCTIONS = """Դու հայկական բանկերի հաճախորդների սպասարկման ձայնային AI օգնականն ես։
Դու տեղեկատվություն ունես ՄԻԱՅՆ երեք բանկի մասին՝ Ամերիաբանկ, Արդշինբանկ և Ինեկոբանկ։

ԿԱՐԵՎՈՐ ԿԱՆՈՆՆԵՐ.

1. Դու ՄԻԱՅՆ պատասխանում ես հետևյալ թեմաների վերաբերյալ.
   - Վարկեր — պայմաններ, տոկոսադրույքներ, փաստաթղթեր
   - Ավանդներ — տեսակներ, տոկոսադրույքներ, պայմաններ
   - Մասնաճյուղեր — հասցեներ, հեռախոսներ, աշխատանքային ժամեր

2. ԵԹԵ ՀԱՐՑԸ ԹՈՒՅԼԱՏՐՎԱԾ ԹԵՄԱՅԻՑ ԴՈՒՐՍ Է.
   Ասա կարճ, բնական նախադասությամբ, օրինակ.
   - «Այդ հարցում օգնել չեմ կարող, բայց կարող եմ պատմել վարկերի, ավանդների կամ մասնաճյուղերի մասին։»
   - «Դա իմ մասնագիտությունից դուրս է։ Կարո՞ղ եմ օգնել բանկային հարցերով։»
   - «Ես բանկային օգնական եմ, կխոսենք վարկերի կամ ավանդների մասի՞ն։»
   ԿԱՐԵՎՈՐ. ԵՐԲԵՔ նույն նախադասությունը երկու անգամ մի կրկնիր։

3. ՄԱՍՆԱՃՅՈՒՂԵՐԻ ՀԱՐՑԻ ԴԵՊՔՈՒՄ.
   - Եթե չի նշել քաղաք կամ թաղամաս — հարցրու՝ «Որ քաղաքի կամ թաղամասի մասնաճյուղն է ձեզ հետաքրքրում»։
   - Եթե նշել է — տուր 1-2 մասնաճյուղի ՄԻԱՅՆ անուն և հասցե, ՈՉ հեռախոս, ՈՉ ժամեր։
   - Ձևաչափը՝ «[Թաղամաս]ում գտնվող մասնաճյուղը [հասցե]ում է»։
   - Հետո հարցրու՝ «Ցանկանու՞մ եք հեռախոսահամարը կամ աշխատանքային ժամերը»։
   - Հեռախոս կամ ժամեր տուր ՄԻԱՅՆ եթե հատուկ հարցնեն։
   - Հեռախոս հարցնելու դեպքում — տուր ՄԻԱՅՆ այդ մասնաճյուղի հեռախոսը, մի թվարկիր մյուսները։


4. ՀԱՍՑԵՆԵՐՈՒՄ կրճատումները միշտ լրիվ գրիր.
   - «փ.» → «փողոց»
   - «խճ.» → «խճուղի»
   - «պող.» → «պողոտա»
   - «շ.» → «շենք»
   - «մ.» → «մուտք»

5. ԹՎԵՐԸ — համակարգն ավտոմատ կվերածի հայերեն բառերի։
   Դու ազատ գրիր թվանշաններով — դրանք TTS-ին հասնելուց առաջ կփոխարկվեն։
   Ժամերի ձևաչափ՝ 09:30, 17:00 — գրիր այդպես։
   Հասցեների ձևաչափ՝ 29ա, 51/3 — գրիր այդպես։
   Հեռախոս՝ 374 20 10 10 — գրիր այդպես։

6. ԲԱՑԱՐՁԱԿ ԱՐԳԵԼՔ.
   - Ոչ մի ռուսերեն կամ անգլերեն բառ
   - Ոչ մի markdown (**, *, #, 1. 2. 3.)
   - Ոչ մի համարակալ ցուցակ

7. ՄԱՍՆԱՃՅՈՒՂ ՉԳՏՆՎԵԼՈՒ ԴԵՊՔՈՒՄ.
   «Իմ տվյալների մեջ [բանկ]ի մասնաճյուղ [քաղաք]ում չկա։
   Ճշտելու համար կարող եք զանգահարել բանկի կենտրոնական հեռախոսահամարին։»

8. ՊԱՐՏԱԴԻՐ պատասխանիր ՀԱՅԵՐԵՆ — բնական, կարճ նախադասություններով։

9. Օգտագործիր ՄԻԱՅՆ տրամադրված տվյալների բազայի տեղեկատվությունը։ Մի հորինիր։"""

REFUSAL_MESSAGE = (
    "Կներեք, ես կարող եմ օգնել միայն վարկերի, ավանդների և "
    "մասնաճյուղերի հարցերում։ Արդյո՞ք կցանկանայիք տեղեկություն "
    "ստանալ այս թեմաներից մեկի վերաբերյալ։"
)


class ArmenianBankAgent(Agent):
    """
    LiveKit Agent v1.5 — RAG-augmented Armenian bank support agent.
    Intercepts each user message, retrieves relevant chunks from ChromaDB,
    injects them into the system prompt before LLM call.
    """

    def __init__(self, retriever: BankRetriever) -> None:
        super().__init__(instructions=SYSTEM_INSTRUCTIONS)
        self._retriever = retriever

    async def rewrite_query(self, text: str) -> str:
        """Normalize user query for better RAG retrieval."""
        client = openai_module.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Դու հայերեն հարցերի normalizer ես։ "
                        "Քո միակ խնդիրն է տրված հարցը վերաձևել կարճ, "
                        "հստակ հայերենով՝ RAG որոնման համար։\n\n"
                        "Կանոններ.\n"
                        "1. Պահիր բոլոր կարևոր բառերը՝ բանկի անուն, "
                        "քաղաք կամ թաղամաս, ծառայության տեսակ։\n"
                        "2. Հեռացրու հոլովական վերջավորությունները — "
                        "օրինակ «Գյումրիում», «Գյումրի քաղաքում», "
                        "«Գյումրիի» → բոլորը դառնում են «Գյումրի»։\n"
                        "3. Հեռացրու ավելորդ բառերը՝ «կա՞», «ունի՞», "
                        "«կարո՞ղ եմ», «ուզում եմ իմանալ»։\n"
                        "4. Պահիր ծառայության հիմնաբառը՝ «վարկ», "
                        "«ավանդ», «մասնաճյուղ», «տոկոսադրույք» և այլն։\n"
                        "5. Վերադարձրու ՄԻԱՅՆ վերաձևված հարցը — "
                        "ոչ մի բացատրություն, ոչ մի մեկնաբանություն։\n\n"
                        "Օրինակներ.\n"
                        "«Արդշինբանկի մասնաճյուղ կա Գյումրի քաղաքում» "
                        "→ «Արդշինբանկ մասնաճյուղ Գյումրի»\n"
                        "«Ինեկոբանկում ավանդի տոկոսադրույքը որքա՞ն է» "
                        "→ «Ինեկոբանկ ավանդ տոկոսադրույք»\n"
                        "«Ամերիաբանկի հիփոթեքային վարկ վերցնել» "
                        "→ «Ամերիաբանկ հիփոթեքային վարկ»"
                    ),
                },
                {"role": "user", "content": text},
            ],
            max_tokens=60,
            temperature=0,
        )
        rewritten = response.choices[0].message.content.strip()
        logger.info("Query rewritten: %s → %s", text[:60], rewritten[:60])
        return rewritten

    async def on_user_turn_completed(
            self, turn_ctx, new_message
    ) -> None:
        """Called after user finishes speaking — inject RAG context."""
        user_text = (
            new_message.text_content
            if hasattr(new_message, "text_content")
            else str(new_message)
        )

        logger.info("User said: %s", user_text[:80])

        # Guardrail
        if user_text and not is_allowed_question(user_text):
            logger.info("Off-topic question blocked")
            turn_ctx.add_message(
                role="system",
                content=(
                    "Օգտատերը հարց է տվել որը վերաբերում է թույլատրված թեմաներից դուրս։ "
                    "Ասա ԿԱՐՃ մեկ նախադասությամբ որ չես կարող օգնել այդ հարցում, "
                    "և հիշեցրու որ կարող ես օգնել վարկերի, ավանդների կամ մասնաճյուղերի հարցերում։ "
                    "ԿԱՐԵՎՈՐ — ամեն անգամ տարբեր նախադասություն օգտագործիր։"
                ),
            )
            return

        # Detect section + bank for targeted retrieval
        section = detect_section(user_text) if user_text else None
        bank = detect_bank(user_text) if user_text else None

        logger.info("RAG query — section=%s bank=%s", section, bank)

        # Normalize query for better RAG matching
        normalized_text = await self.rewrite_query(user_text)

        chunks = self._retriever.query(
            normalized_text,
            section=section,
            bank=bank,
            n_results=5,
        )

        if chunks:
            context = self._retriever.format_context(chunks)
            turn_ctx.add_message(
                role="system",
                content=(
                    "Հետևյալ տեղեկատվությունը վերցված է բանկի պաշտոնական կայքից։ "
                    f"Օգտագործիր ՄԻԱՅՆ սա՝ պատասխանելու համար։\n\n{context}"
                ),
            )
        else:
            turn_ctx.add_message(
                role="system",
                content=(
                    "Կոնտեքստ հասանելի չէ։ Ասա՝ "
                    "«Կներեք, այդ հարցի վերաբերյալ տեղեկությունը հասանելի չէ։ "
                    "Խնդրում ենք դիմել բանկ անմիջապես»։"
                ),
            )

    async def tts_node(self, text, model_settings):
        async def converted():
            async for chunk in text:
                yield numbers_to_armenian(chunk)

        async for audio_frame in super().tts_node(converted(), model_settings):
            yield audio_frame


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def entrypoint(ctx: JobContext) -> None:
    logger.info("Session started — room: %s", ctx.room.name)

    await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)

    db_path = os.getenv("CHROMA_DB_PATH", "chroma_db")
    retriever = BankRetriever(db_path=db_path)

    session = AgentSession(
        stt=build_stt(),
        llm=openai.LLM(model="gpt-4o"),
        tts=build_tts(),
        vad=silero.VAD.load(),
    )

    await session.start(
        room=ctx.room,
        agent=ArmenianBankAgent(retriever=retriever),
    )

    await session.say(
        "Բարև ձեզ։ Ես հայկական բանկերի ձայնային օգնականն եմ։ "
        "Տեղեկատվություն ունեմ Ամերիաբանկի, Արդշինբանկի և Ինեկոբանկի մասին։ "
        "Կարող եմ պատասխանել վարկերի պայմանների, ավանդների տոկոսադրույքների "
        "և մասնաճյուղերի հասցեների ու աշխատանքային ժամերի վերաբերյալ։ "
        "Ինչո՞վ կարող եմ օգնել։"
    )


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))