import asyncio
import pandas as pd
import os
import re
from datetime import datetime, date, timedelta
from telegram import Update # Changed from telegram.ext import Updater
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
# import asyncio # Can be removed if not used for other async tasks
import logging
from collections import defaultdict, deque
from sentence_transformers import SentenceTransformer, util
import torch
import requests
from bs4 import BeautifulSoup

import config, mapping

model = SentenceTransformer('all-MiniLM-L6-v2')

user_History = defaultdict(lambda: deque(maxlen=5))

# --- Cache Configuration ---
# This dictionary will hold the scraped news and the time of the scrape.
NEWS_CACHE = {
    "data": None,
    "timestamp": None
}
# We'll refresh the news if the cached data is older than 1 hour.
CACHE_DURATION = timedelta(hours=1)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
try:
    from config import TELEGRAM_BOT_TOKEN
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_ACTUAL_TOKEN_HERE":
        logging.critical("TELEGRAM_BOT_TOKEN not set or is placeholder in config.py. Please update it.")
        TELEGRAM_BOT_TOKEN = None
except ImportError:
    logging.critical("config.py not found. Please create it and add your TELEGRAM_BOT_TOKEN.")
    TELEGRAM_BOT_TOKEN = None
except Exception as e:
    logging.critical(f"Error importing token from config.py: {e}")
    TELEGRAM_BOT_TOKEN = None

CITY = ["Pune,IN","Solapur,IN","Nagpur,IN","Mumbai,IN","Nashik,IN"]
DATA_DIR = "priceData"  # Directory containing your Excel files
ITEM_MAPPING_CONFIG = mapping.ITEM_MAPPING_CONFIG

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class AgriBot:
    def __init__(self, data_dir=DATA_DIR, item_mapping=ITEM_MAPPING_CONFIG):
        self.data = {}
        self.data_dir = data_dir
        self.item_mapping = item_mapping
        self.load_data()

    def load_data(self):
        """Loads and consolidates data, storing multiple entries per item."""
        logger.info(f"Loading data from directory: {self.data_dir}")
        if not os.path.isdir(self.data_dir):
            logger.error(f"Data directory '{self.data_dir}' not found. Please create it and add your Excel files.")
            return

        for filename in os.listdir(self.data_dir): # Read from the specified data_dir
            # Check if the file starts with the expected prefix and is an Excel file
            if filename.startswith("Pune_Market_Rates_") and filename.endswith(".xlsx"):
                filepath = os.path.join(self.data_dir, filename)
                try:
                    df = pd.read_excel(filepath)
                    required_columns = {"Date", "Market", "शेतिमाल", "किमान", "कमाल","परिमाण"}
                    if not required_columns.issubset(df.columns):
                        logger.warning(f"Skipping {filepath}: Incomplete data format. Missing columns: {required_columns - set(df.columns)}")
                        continue
                    for _, row in df.iterrows():
                        date_str = row["Date"]
                        try:
                            data_date = datetime.strptime(str(date_str), "%d-%m-%Y").date()
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Skipping row in {filepath}: Invalid date '{date_str}'. Error: {e}")
                            continue

                        item = row["शेतिमाल"]
                        if item and pd.notna(item):
                            if item not in self.data:
                                self.data[item] = []
                            self.data[item].append({
                                "date": data_date,
                                "min_rate": row["किमान"],
                                "max_rate": row["कमाल"],
                                "market": row["Market"],
                                "quantity": row["परिमाण"]
                            })
                        else:
                            logger.warning(f"Skipping row in {filepath}: Empty item name.")
                except Exception as e:
                    logger.error(f"Error reading {filepath}: {e}")

        for item, entries in self.data.items():
            entries.sort(key=lambda x: x["date"], reverse=True)
            self.data[item] = entries[:5]

        if not self.data:
            logger.warning("No valid data loaded. Check your Excel files in the '%s' directory.", self.data_dir)
        else:
            logger.info(f"Loaded data for {len(self.data)} items.")

    def _scrape_news_from_source(self,url: str, category: str) -> list[str]:
        """
        Scrapes the top 5 headlines from a single Agrowon category page.
        This is a private helper function.
        """
        logging.info(f"Scraping '{category}' from {url}")
        headlines = []
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # The class name 'headline-m_headline__...' is dynamic.
            # We use a "contains" selector `[class*='...']` to make it more stable.
            headline_tags = soup.select("h6[class*='headline-m_headline__']")

            if not headline_tags:
                logging.warning(f"No headline tags found for '{category}'. The website selector might be outdated.")
                return []

            for h6 in headline_tags[:5]:  # Limit to the top 5 headlines
                # The link is in the parent 'a' tag of the 'h6'
                link_tag = h6.find_parent('a')
                if link_tag and link_tag.get('href'):
                    title = h6.get_text(strip=True)
                    link = link_tag['href']

                    # Links on the site are relative (e.g., /weather-news/...). We must make them absolute.
                    if not link.startswith('http'):
                        base_url = "https://agrowon.esakal.com"
                        link = f"{base_url}{link}"

                    # Format for Telegram HTML links
                    headlines.append(f'• <a href="{link}">{title}</a>')

            return headlines

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch URL {url}: {e}")
            return []  # Return an empty list on network error
        except Exception as e:
            logging.error(f"An unexpected error occurred while scraping {url}: {e}", exc_info=True)
            return []

    def get_latest_agrowon_news(self):
        """
        Fetches the latest news from Agrowon, using a 1-hour cache.
        This is the main function your bot should call.
        """
        # 1. Check if we have valid, non-expired data in the cache
        if NEWS_CACHE["data"] and NEWS_CACHE["timestamp"]:
            if datetime.now() - NEWS_CACHE["timestamp"] < CACHE_DURATION:
                logging.info("Returning fresh news from cache.")
                return NEWS_CACHE["data"]

        # 2. If cache is empty or expired, perform a fresh scrape
        logging.info("Cache is stale or empty. Performing a fresh scrape.")

        all_news_parts = ["📰 **Latest Agricultural News**\n"]
        has_news = False

        for category, url in mapping.NEWS_SOURCES.items():
            scraped_headlines = self._scrape_news_from_source(url, category)
            if scraped_headlines:
                has_news = True
                # Add a bold category header
                all_news_parts.append(f"\n<b>{category}</b>")
                all_news_parts.extend(scraped_headlines)

        if not has_news:
            return "Sorry, I couldn't retrieve any news at the moment. Please try again later."

        final_response = "\n".join(all_news_parts)

        # 3. Update the cache with the newly scraped data and the current time
        NEWS_CACHE["data"] = final_response
        NEWS_CACHE["timestamp"] = datetime.now()

        return "📰\n"+final_response+"\n📰"

    def get_rate(self, item):
        """Retrieves rates for an item, returning the last 5 entries."""
        standardized_item = item.lower()
        item_marathi = self.item_mapping.get(standardized_item, item)

        if item_marathi in self.data:
            entries = self.data[item_marathi]
            if entries:
                # Determine the market from the most recent entry for display
                market = entries[0]['market'] if entries else "N/A"

                response_parts = [f"📊 Recent rates for {item_marathi}: \nMarket: 📍{market}📍\n"]
                # Using a simple pre-formatted text for table-like appearance
                # Telegram's Markdown for tables can be tricky and might require escaping
                header = f"{'Date 📅':<10} | {'Min 📉':<8} | {'Max 📈':<8}"
                response_parts.append(header)
                response_parts.append("-" * (len(header) + 2)) # Separator line
                quantity=""
                for entry in entries:
                    date_str = entry['date'].strftime('%d %b')
                    # Ensure rates are strings for consistent formatting
                    min_rate_str = str(entry['min_rate'])
                    max_rate_str = str(entry['max_rate'])
                    quantity = str(entry['quantity'])
                    row_str = f"{date_str:<10} | {min_rate_str:<8} | {max_rate_str:<8}"
                    response_parts.append(row_str)
                response_parts.append("-" * (len(header) + 2))
                if quantity == "शेकडा":
                    response_parts.append("❗️Rates of 100 Piece❗")
                else:
                    response_parts.append(f"️❗️Rates of 100 Kg❗")
                response_parts.append("\n🌾Anything else I can assist with?🌾\n💬")
                return "\n".join(response_parts)
            else:
                return f"No rate information found for {item_marathi}. Perhaps it's not traded recently? Anything else?"
        else:
            return f"Could not find any rate information for {item_marathi}. Are you sure it's a common crop? What else can I look up?"

    def get_weather(self):
        result = ""
        for ct in CITY:
            URL = f"https://api.openweathermap.org/data/2.5/weather?q={ct}&appid={config.OPENWTHR_API_KEY}&units=metric"
            response = requests.get(URL)
            data = response.json()
            if response.status_code == 200:
                weather = data['weather'][0]['main']  # e.g., Rain, Clear, Clouds
                temp = data['main']['temp']  # current temp
                feels_like = data['main']['feels_like']
                humidity = data['main']['humidity']
                # Simplified interpretation
                status = {
                    "Rain": "🌧Rain expected⛈",
                    "Clear": "☀️Sunny☀️",
                    "Clouds": "⛅️Cloudy🌤"
                }.get(weather, weather)
                result = result + f"☀️Weather in {ct}📍:\nTemperature: {temp}°C🌡 (Feels like {feels_like}°C🌡)\nWeather: {status} \tHumidity: {humidity}%\n\n"
            else:
                print("Could not fetch weather info.")
        return result

    def respond_to_query(self, query: str) -> str:
        """Analyzes the user's query and calls the appropriate function."""
        query_lower = str(query).lower()
        #item_match = re.search(r"(?:rate|price)\s+(?:of)?\s*(.+)", query_lower)
        # Match: "rate of xyz", "price xyz", "xyz rate", or just "xyz"
        item_match = re.search(r"(?:rate|price)\s+(?:of\s+)?(.+)|(.+)\s*(?:rate|price)?$",query_lower)
        if "weather" in query_lower:
            return self.get_weather()
            #return ("I currently don't support weather 🌤 but I can help with crop🌾 rates.")
        elif "news" in query_lower:
            news_html = self.get_latest_agrowon_news()
            # This part simulates how the bot would see the text, without the HTML tags
            if '<a href=' in news_html:
                return (BeautifulSoup(news_html, "html.parser").get_text())
            else:
                return (news_html)
        elif item_match:
            item = item_match.group(1) if item_match.group(1) else item_match.group(2)
            # Clean up date/time text like "on date", "for date", "today", etc.
            item = re.sub(r'\s+(on|for)\s+(date\s*)?.*$', '', item, flags=re.IGNORECASE).strip()
            item = re.sub(r'\s+today$', '', item, flags=re.IGNORECASE).strip()
        # if item_match:
        #     item = item_match.group(1).strip()
        #     item = re.sub(r'\s+(on|for)\s+date.*$', '', item, flags=re.IGNORECASE).strip()
        #     item = re.sub(r'\s+today$', '', item, flags=re.IGNORECASE).strip()
            if not item:
                 return ("It seems you asked for a rate, but I couldn't identify the item. "
                        "Could you please specify it? For example: 'Rate of Kanda'")
            return self.get_rate(item)
        else:
            return ("I understand you're asking about prices, but could you please specify the item? "
                    "For example, you could ask 'What is the rate of tomato?'")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE): # Changed type hint to Update
    user_message = update.message.text
    user_id = update.effective_user.id
    user_History[user_id].append(user_message)
    bot_instance = context.bot_data.get('agri_bot')
    if not bot_instance:
        logger.error("AgriBot instance not found in bot_data.")
        await update.message.reply_text("🚧 Sorry, I'm having some technical difficulties. Please try again later.")
        return

    try:
        logger.info(f"Received message from {update.effective_user.username if update.effective_user else 'UnknownUser'}: {user_message}")
        response = bot_instance.respond_to_query(user_message)
        # For pre-formatted text, usually no specific parse_mode is needed,
        # but if you use Markdown characters, you'd set parse_mode=ParseMode.MARKDOWN_V2
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
        await asyncio.sleep(1.5)
        await update.message.reply_text(response)

    except Exception as e:
        logger.error(f"Error handling message: {e}", exc_info=True)
        await update.message.reply_text("Oops! Something went wrong on my end. Please try again.")

def main():
    if not TELEGRAM_BOT_TOKEN:
        # Logger already prints a critical message if token is None
        return

    agri_bot_instance = AgriBot()

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.bot_data['agri_bot'] = agri_bot_instance
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot starting to poll...")
    app.run_polling()

if __name__ == "__main__":
    main()