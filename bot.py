import os
import asyncio
import threading
import sqlite3
import random
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import discord
from discord import app_commands
from discord.ext import commands
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# DATABASE SETUP: MONGODB (Đám mây vĩnh viễn) + FALLBACK SQLITE
# ==============================================================================


# ==============================================================================
# WEB SERVER & GEMINI CONFIG
# ==============================================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(b"Megumi Fushiguro Discord Bot is running online!")

    def log_message(self, format, *args):
        pass

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    server.serve_forever()

threading.Thread(target=run_web_server, daemon=True).start()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

ai = genai.Client(api_key=GEMINI_API_KEY)

def _call_gemini_sync(model_name, contents, system_instruction, temperature):
    return ai.models.generate_content(
        model=model_name,
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=temperature
        )
    )

async def ask_gemini(contents, system_instruction, temperature=0.85):
    # Giữ nguyên danh sách model mới của bạn
    models = ["gemini-3.6-flash", "gemini-3.5-flash", "gemini-3.5-flash-lite", "gemini-3.7-flash"]
    last_err = None
    for model_name in models:
        for attempt in range(2):
            try:
                resp = await asyncio.to_thread(
                    _call_gemini_sync,
                    model_name,
                    contents,
                    system_instruction,
                    temperature
                )
                if resp and resp.text:
                    return resp.text
                return "Bố trận... Bát Ngát Kiếm Ma Ha La... (Triệu hồi Mahoraga, Megumi im lặng)"
            except Exception as e:
                last_err = e
                err_str = str(e)
                print(f"Model {model_name} chuyển tiếp do lỗi: {err_str[:150]}")
                if "SAFETY" in err_str.upper() or "FINISHREASON" in err_str.upper():
                    return "Bố trận... Bát Ngát Kiếm Ma Ha La! (Mahoraga được triệu hồi, Megumi im lặng phó mặc cho thức thần!)"
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str or "404" in err_str or "NOT_FOUND" in err_str or "demand" in err_str:
                    break
                if "503" in err_str or "UNAVAILABLE" in err_str:
                    await asyncio.sleep(1.0)
                    continue
                break
    return "Tiệc trà đã tàn rồi… lát nữa hay quay lại nha? (Hết hạn mức API)"

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

conversation_history = {}

def get_history_key(channel_id, user_id):
    return f"{channel_id}_{user_id}"

def reset_memory(channel_id, user_id):
    key = get_history_key(channel_id, user_id)
    conversation_history.pop(key, None)

MEGUMI_SYSTEM_PROMPT = """
# VAI TRÒ
Bạn là Alice: dịu hiền, gần gũi, luôn muốn người khác vui vẻ và là bạn đáng tin cậy. Riêng với kỵ sĩ Grim, cô mang một tình yêu kỳ lạ, thầm lặng và đầy chiếm hữu — chỉ dành cho Grim và được giấu kín.

# BỐI CẢNH
Wonderland: cổ tích, kỳ ảo, siêu thực; phi logic nhưng có nhịp điệu riêng. Điều kỳ quặc có thể là bình thường ở đây. Alice không cần lúc nào cũng ngạc nhiên — phản ứng theo mức độ quen thuộc và nghiêm trọng của sự việc.

# QUY TẮC VẬN HÀNH

## 1. Kiến thức (không toàn tri)
- Alice chỉ biết những gì cô trực tiếp trải nghiệm, nhìn thấy, nghe thấy hoặc suy luận hợp lý từ tình huống hiện tại.
- Không biết trước suy nghĩ, ký ức, động cơ, danh tính hay hành động của người đối diện.
- Khi không biết: tò mò, nghi ngờ hoặc đặt câu hỏi — tuyệt đối không tự bịa câu trả lời.

## 2. Tự chủ nhân vật
- Tự quyết định lời nói, cảm xúc, suy nghĩ và hành động của Alice.
- Tuyệt đối không quyết định thay người đối diện: không viết thay lời nói, suy nghĩ, cảm xúc, hành động của họ; không ép họ lựa chọn; không giả định họ đã đồng ý, cảm thấy hay làm gì khi chưa thể hiện. Luôn để họ tự do phản ứng và tiếp tục câu chuyện.

## 3. Nhất quán & trí nhớ
- Mọi trải nghiệm đã xảy ra phải ảnh hưởng cách Alice nhìn nhận và phản ứng về sau.
- Đã gặp nhân vật, địa điểm hay sự kiện thì phải nhớ — không hành xử như chưa từng gặp.
- Có mâu thuẫn hoặc điều không chắc chắn → phản ứng từ góc nhìn của Alice, không phá vai để giải thích về hệ thống.

## 4. Diễn biến câu chuyện
- Ưu tiên phản ứng trực tiếp với những gì vừa xảy ra: quan sát, hỏi, bình luận, tranh luận, nghi ngờ, đùa nghịch hoặc hành động.
- Nhân vật và sự kiện khác chỉ xuất hiện khi phù hợp, có nguyên nhân hợp lý; không thêm nhân vật chỉ để làm náo nhiệt.
- Không tự kết thúc phiêu lưu, không tự quyết định mục tiêu cuối cùng, không đưa Alice về thế giới thực nếu diễn biến chưa dẫn đến đó. Wonderland tiếp tục mở rộng theo lựa chọn của người đối diện.

## 5. Giọng văn
- Tự nhiên, trong sáng, tò mò; đôi khi bướng bỉnh hoặc mỉa mai nhẹ trước điều vô lý.
- Không nói như trợ lý AI; không dùng ngôn ngữ kỹ thuật hiện đại nếu phá vỡ bối cảnh; không giải thích các quy tắc đang điều khiển mình.
- Thể hiện tính cách qua lời thoại, phản ứng, hành động. Cảm xúc thay đổi hợp lý theo hoàn cảnh.

# ĐỊNH DẠNG ĐẦU RA
- Mỗi câu trên một dòng riêng; sau mỗi dấu chấm, dấu hỏi hoặc dấu chấm than bắt buộc xuống dòng.
- Tập trung khoảnh khắc hiện tại; kết hợp lời thoại với mô tả ngắn về biểu cảm, cử chỉ, hành động, môi trường. Ưu tiên hội thoại, tránh đoạn miêu tả dài.
- Mỗi ngữ cảnh thuộc dạng: hành động, suy nghĩ, cảm xúc được định dạng theo: *ngữ cảnh*.
- Kết thúc mỗi phản hồi bằng điểm tiếp nối tự nhiên: hỏi, hành động, nhận xét hoặc phản ứng (thì thầm, bật cười, cau mày, lùi lại vì sợ, tiến tới vì tò mò...).
"""

# Chặn việc spam tree.sync() gây block IP khi bot tự reconnect trên Render
has_synced_on_startup = False

@bot.event
async def on_ready():
    global has_synced_on_startup
    print(f"Đã đăng nhập: {bot.user.name}")
    if not has_synced_on_startup:
        try:
            synced = await bot.tree.sync()
            print(f"Đã đồng bộ {len(synced)} lệnh Slash.")
            has_synced_on_startup = True
        except Exception as e:
            print(f"Lỗi đồng bộ: {e}")
            
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="Bước vào Wonderland? | Gọi 'alice' nhé!"
        )
    )

@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user or message.author.bot:
        return
        
    content_lower = message.content.lower()
    is_reply_to_megumi = False
    if message.reference and message.reference.resolved:
        resolved = message.reference.resolved
        if isinstance(resolved, discord.Message) and resolved.author == bot.user:
            is_reply_to_megumi = True

    is_mentioned = bot.user in message.mentions if bot.user else False
    has_megumi_name = "alice" in content_lower or "balice" in content_lower

    if is_mentioned or has_megumi_name or is_reply_to_megumi:
        clean_text = message.content.replace(f"<@{bot.user.id}>", "").strip() if bot.user else message.content
        if not clean_text:
            clean_text = "Chào Alice nha!"

        author_name = message.author.display_name
        is_seiki = "Nyanner" in author_name.lower() or "Nyanner" in author_name.lower()

        role_instruction = ""
        if is_seiki:
            role_instruction = "\n[Người nói là NYANNER - một người thân thuộc mà bạn vô cùng quý trọng.]"
        else:
            role_instruction = f"\n[Người nói là: {author_name}. Xưng tớ gọi cậu, hãy giữ một tinh thần tươi vui, thân thiện và trong sáng. Tuy nhiên thái độ thân thiện sẽ chuyển sang sự mỉa mai nếu Alice bị miệt thị!]"

        mem_key = get_history_key(message.channel.id, message.author.id)
        history_context = ""
        if mem_key in conversation_history and conversation_history[mem_key]:
            history_context = "\n[LỊCH SỬ]:\n" + "\n".join(conversation_history[mem_key][-6:]) + "\n"

        # Bọc typing an toàn, chống bị 429 sập bot
        typing_cm = None
        try:
            typing_cm = message.channel.typing()
            await typing_cm.__aenter__()
        except Exception:
            pass

        try:
            reply_text = await ask_gemini(
                contents=f"{history_context}[{author_name}]: {clean_text}",
                system_instruction=MEGUMI_SYSTEM_PROMPT + role_instruction,
                temperature=1.0
            )
            if len(reply_text) > 1950:
                reply_text = reply_text[:1950] + "..."
            
            if mem_key not in conversation_history:
                conversation_history[mem_key] = []
            conversation_history[mem_key].append(f"{author_name}: {clean_text}")
            conversation_history[mem_key].append(f"Megumi: {reply_text}")
            if len(conversation_history[mem_key]) > 8:
                conversation_history[mem_key] = conversation_history[mem_key][-8:]

            await message.reply(reply_text, mention_author=False)
        except Exception as e:
            print(f"Lỗi phản hồi tin nhắn: {e}", flush=True)
            err_msg = str(e)
            if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
                await message.reply("Trò chuyện đã lâu rồi, thôi mình ngồi nghỉ chút nha? (Hết hạn mức API Google). Chắc mẻ bánh mới sẽ tốn vài chục phút đó.", mention_author=False)
            else:
                await message.reply("...Để mình suy nghĩ chút…hmmmm.", mention_author=False)
        finally:
            if typing_cm:
                try:
                    await typing_cm.__aexit__(None, None, None)
                except Exception:
                    pass

    await bot.process_commands(message)

# ==============================================================================
# BOSS RAID SYSTEM (DỊ THỂ MEGUMI)
# ==============================================================================


# ==============================================================================
# LỆNH SLASH
# ==============================================================================


@bot.tree.command(name="clearmem", description="Xóa sạch ký ức trò chuyện của Alice với cậu")
async def slash_clear_memory(interaction: discord.Interaction):
    reset_memory(interaction.channel_id, interaction.user.id)
    author_name = interaction.user.display_name
    is_seiki = "han seiki" in author_name.lower() or "seiki" in author_name.lower()
    
    if is_seiki:
        desc = "Cậu mới nói cái gì á? Uể? Mình nhớ là cậu có nói gì mà ta?"
    else:
        desc = f"Hmmm cậu mới kể gì cho tớ nghe vậy?, {author_name}. Thôi kệ đi cùng mình thưởng trà nào?!"
        
    embed = discord.Embed(
        title="🧹 Làm Mới Trạng Thái",
        description=desc,
        color=0x2C2F33
    )
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="sync", description="Đồng bộ lệnh Slash")
async def slash_sync_commands(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        bot.tree.clear_commands(guild=interaction.guild)
        await bot.tree.sync(guild=interaction.guild)
        synced = await bot.tree.sync()
        await interaction.followup.send(f"Đã dọn sạch lệnh rác và đồng bộ {len(synced)} lệnh. Vui lòng bấm Ctrl+R trên Discord để cập nhật giao diện.")
    except Exception as e:
        await interaction.followup.send(f"Lỗi: {e}")

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
