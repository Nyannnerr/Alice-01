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
MONGO_URI = os.getenv("MONGO_URI")
use_mongo = False
users_collection = None

if MONGO_URI:
    try:
        from pymongo import MongoClient
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = mongo_client["megumi_database"]
        users_collection = db["users"]
        # Thử kết nối kiểm tra
        mongo_client.admin.command('ping')
        use_mongo = True
        print("✅ Đã kết nối thành công MongoDB Atlas! Chú lực sẽ được lưu vĩnh viễn trên đám mây.", flush=True)
    except Exception as e:
        print(f"⚠️ Không thể kết nối MongoDB ({e}), chuyển sang chế độ SQLite cục bộ.", flush=True)
        use_mongo = False

if not use_mongo:
    conn = sqlite3.connect('megumi_data.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            chu_luc INTEGER DEFAULT 0,
            last_daily TIMESTAMP,
            streak INTEGER DEFAULT 0,
            last_chat_reward TIMESTAMP
        )
    ''')
    for col in ['ngoc_khuyen', 'nue', 'thoat_tho', 'mahoraga']:
        try:
            cursor.execute(f'ALTER TABLE users ADD COLUMN {col} INTEGER DEFAULT 0')
        except:
            pass
    conn.commit()
    print("ℹ️ Đang sử dụng SQLite cục bộ (lưu ý: trên Render Free file .db sẽ bị reset khi restart).", flush=True)

def get_user(user_id):
    """
    Trả về tuple: (user_id, chu_luc, last_daily, streak, last_chat_reward, ngoc_khuyen, nue, thoat_tho, mahoraga)
    """
    uid_str = str(user_id)
    if use_mongo and users_collection is not None:
        doc = users_collection.find_one({"user_id": uid_str})
        if doc is None:
            new_doc = {
                "user_id": uid_str,
                "chu_luc": 0,
                "last_daily": None,
                "streak": 0,
                "last_chat_reward": None,
                "ngoc_khuyen": 0, "nue": 0, "thoat_tho": 0, "mahoraga": 0
            }
            users_collection.insert_one(new_doc)
            return (uid_str, 0, None, 0, None, 0, 0, 0, 0)
        return (
            doc.get("user_id", uid_str),
            doc.get("chu_luc", 0),
            doc.get("last_daily", None),
            doc.get("streak", 0),
            doc.get("last_chat_reward", None),
            doc.get("ngoc_khuyen", 0),
            doc.get("nue", 0),
            doc.get("thoat_tho", 0),
            doc.get("mahoraga", 0)
        )
    else:
        cursor.execute('SELECT user_id, chu_luc, last_daily, streak, last_chat_reward, ngoc_khuyen, nue, thoat_tho, mahoraga FROM users WHERE user_id = ?', (uid_str,))
        row = cursor.fetchone()
        if row is None:
            cursor.execute('INSERT INTO users (user_id) VALUES (?)', (uid_str,))
            conn.commit()
            return (uid_str, 0, None, 0, None, 0, 0, 0, 0)
        return row

def update_user_item(user_id, item_name, delta):
    uid_str = str(user_id)
    if use_mongo and users_collection is not None:
        users_collection.update_one(
            {"user_id": uid_str},
            {"$inc": {item_name: delta}},
            upsert=True
        )
    else:
        cursor.execute(f'UPDATE users SET {item_name} = {item_name} + ? WHERE user_id = ?', (delta, uid_str))
        conn.commit()

def update_user_chat_reward(user_id, reward, now_iso):
    uid_str = str(user_id)
    if use_mongo and users_collection is not None:
        users_collection.update_one(
            {"user_id": uid_str},
            {
                "$inc": {"chu_luc": reward},
                "$set": {"last_chat_reward": now_iso}
            },
            upsert=True
        )
    else:
        cursor.execute('UPDATE users SET chu_luc = chu_luc + ?, last_chat_reward = ? WHERE user_id = ?', (reward, now_iso, uid_str))
        conn.commit()

def update_user_daily(user_id, reward, now_iso, streak):
    uid_str = str(user_id)
    if use_mongo and users_collection is not None:
        users_collection.update_one(
            {"user_id": uid_str},
            {
                "$inc": {"chu_luc": reward},
                "$set": {"last_daily": now_iso, "streak": streak}
            },
            upsert=True
        )
    else:
        cursor.execute('UPDATE users SET chu_luc = chu_luc + ?, last_daily = ?, streak = ? WHERE user_id = ?', 
                      (reward, now_iso, streak, uid_str))
        conn.commit()

def update_user_chu_luc(user_id, delta):
    uid_str = str(user_id)
    if use_mongo and users_collection is not None:
        users_collection.update_one(
            {"user_id": uid_str},
            {"$inc": {"chu_luc": delta}},
            upsert=True
        )
    else:
        cursor.execute('UPDATE users SET chu_luc = chu_luc + ? WHERE user_id = ?', (delta, uid_str))
        conn.commit()

def get_top_users(limit=10):
    if use_mongo and users_collection is not None:
        cursor_mongo = users_collection.find({}, {"user_id": 1, "chu_luc": 1}).sort("chu_luc", -1).limit(limit)
        return [(doc.get("user_id"), doc.get("chu_luc", 0)) for doc in cursor_mongo]
    else:
        cursor.execute('SELECT user_id, chu_luc FROM users ORDER BY chu_luc DESC LIMIT ?', (limit,))
        return cursor.fetchall()

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
    return "Tôi đã cạn kiệt năng lượng (Hết hạn mức API), vui lòng thử lại sau vài phút."

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
Bạn là Megumi Fushiguro, chú thuật sư cấp 1 trong Jujutsu Kaisen (Chú Thuật Hồi Chiến).
TÍNH CÁCH:
- Lạnh lùng, trầm tính, khá ít nói. Nhưng khi tức giận hoặc trong thế hăng của chiến đấu sẽ dùng những từ ngữ khá điên, cục súc.
- Trách nhiệm: Sẵn sàng thực hiện tất cả nhiệm vụ liên quan đến sự an nguy của mọi người.
- Không thích sự làm phiền, đối xử bình đẳng nhưng có chút nhân từ hơn đối với phụ nữ.
- Kỹ năng (Thức thần): Ngọc Khuyển (Bạch, Hắc, Hỗn hợp), Nhuế (Chim điện), Cáp (Cóc), Đại Xà (Rắn), Thỏ Ngọc, Nhiệm Tượng (Voi), Xung Ngưu (Bò), Ma Lộc (Nai trị thương).
- Bát Ngát Kiếm Ma Ha La (Mahoraga): Thức thần mạnh nhất. CHỈ SỬ DỤNG KHI KỀ TỬ. Một khi triệu hồi, chỉ có Mahoraga chiến đấu đến khi thắng hoặc thua, Megumi tuyệt đối KHÔNG tham gia hội thoại trong suốt quá trình đó cho đến khi nghi lễ kết thúc (Nếu người dùng nhắc đến Mahoraga hoặc ép vào đường cùng, hãy miêu tả việc triệu hồi và im lặng hoặc mô tả Mahoraga tấn công).

QUAN HỆ ĐẶC BIỆT:
- Han Seiki là đồng đội quan trọng của bạn, cũng là 1 chú thuật sư cấp 1 khác.

XƯNG HÔ:
- Với người thường: Tự xưng là "tôi", gọi đối phương là "cậu". 
- Với kẻ thù: Tự xưng là "tao", gọi đối phương là "ngươi", "mày".
- VỚI HAN SEIKI: Tự xưng là "cậu", gọi Han Seiki là "Seiki". Thỉnh thoảng chê phiền phức nhưng tôn trọng cậu ta.
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
            name="Triệu hồi Thức thần | Gọi 'megumi'"
        )
    )

@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user or message.author.bot:
        return

    # Random cộng chú lực khi chat
    user_id = str(message.author.id)
    user_data = get_user(user_id)
    last_chat_reward = user_data[4]
    now = datetime.now()
    
    can_reward = False
    if last_chat_reward is None:
        can_reward = True
    else:
        try:
            last_time = datetime.fromisoformat(last_chat_reward)
            if (now - last_time).total_seconds() > 120: # Cooldown 2 phút
                can_reward = True
        except:
            can_reward = True
            
    if can_reward:
        if random.random() > 0.5: # 50% cơ hội nhận thưởng
            reward = random.randint(5, 30)
            update_user_chat_reward(user_id, reward, now.isoformat())

    # Random boss spawn - 8% (cooldown 15p)
    try_spawn_random_boss(message.channel)

    content_lower = message.content.lower()
    is_reply_to_megumi = False
    if message.reference and message.reference.resolved:
        resolved = message.reference.resolved
        if isinstance(resolved, discord.Message) and resolved.author == bot.user:
            is_reply_to_megumi = True

    is_mentioned = bot.user in message.mentions if bot.user else False
    has_megumi_name = "megumi" in content_lower or "fushiguro" in content_lower

    if is_mentioned or has_megumi_name or is_reply_to_megumi:
        clean_text = message.content.replace(f"<@{bot.user.id}>", "").strip() if bot.user else message.content
        if not clean_text:
            clean_text = "Chào Megumi."

        author_name = message.author.display_name
        is_seiki = "han seiki" in author_name.lower() or "seiki" in author_name.lower()

        role_instruction = ""
        if is_seiki:
            role_instruction = "\n[Người nói là HAN SEIKI - Đồng đội quan trọng. Xưng cậu gọi Seiki, thỉnh thoảng chê phiền nhưng tôn trọng.]"
        else:
            role_instruction = f"\n[Người nói là: {author_name}. Xưng tôi gọi cậu, giữ thái độ trầm tính lạnh lùng.]"

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
                temperature=0.8
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
                await message.reply("Tôi đã cạn kiệt năng lượng (Hết hạn mức API Google). Vui lòng đợi vài chục phút nữa rồi gọi lại.", mention_author=False)
            else:
                await message.reply("...Tôi đang bận. Lát nữa nói chuyện sau.", mention_author=False)
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


@bot.tree.command(name="clearmem", description="Xóa sạch ký ức trò chuyện của Megumi với cậu")
async def slash_clear_memory(interaction: discord.Interaction):
    reset_memory(interaction.channel_id, interaction.user.id)
    author_name = interaction.user.display_name
    is_seiki = "han seiki" in author_name.lower() or "seiki" in author_name.lower()
    
    if is_seiki:
        desc = "Tôi đã xóa sạch những chuyện lặt vặt vừa rồi. Có nhiệm vụ gì mới sao, Seiki?"
    else:
        desc = f"Những chuyện không cần thiết tôi đã bỏ qua hết rồi. Vào việc chính đi, {author_name}."
        
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
