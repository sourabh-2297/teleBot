import pandas as pd
import os
import re
from datetime import datetime, date
from telegram import Update # Changed from telegram.ext import Updater
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
# import asyncio # Can be removed if not used for other async tasks
import logging

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

DATA_DIR = "priceData"  # Directory containing your Excel files
ITEM_MAPPING_CONFIG = {
    "kanda": "कांदा", "onion": "कांदा", "batata": "बटाटा", "potato": "बटाटा",
    "bhindi": "भेंडी", "ladyfinger": "भेंडी", "ghevda": "घेवडा", "beans": "घेवडा",
    "gajar": "गाजर", "carrot": "गाजर", "vangi": "वांगी", "brinjal": "वांगी",
    "lasun": "लसूण", "garlic": "लसूण", "aale": "आले", "ginger": "आले",
    "tamatar": "टोमॅटो", "tomato": "टोमॅटो",
}

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
            if filename.startswith("Pune_market_rates_") and filename.endswith(".xlsx"):
                filepath = os.path.join(self.data_dir, filename)
                try:
                    df = pd.read_excel(filepath)
                    required_columns = {"Date", "Market", "शेतिमाल", "किमान", "कमाल"}
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
                                "market": row["Market"]
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

                for entry in entries:
                    date_str = entry['date'].strftime('%d %b')
                    # Ensure rates are strings for consistent formatting
                    min_rate_str = str(entry['min_rate'])
                    max_rate_str = str(entry['max_rate'])
                    row_str = f"{date_str:<10} | {min_rate_str:<8} | {max_rate_str:<8}"
                    response_parts.append(row_str)

                response_parts.append("-" * (len(header) + 2))
                response_parts.append("\n🌾Anything else I can assist with?🌾\n💬")
                return "\n".join(response_parts)
            else:
                return f"No rate information found for {item_marathi}. Perhaps it's not traded recently? Anything else?"
        else:
            return f"Could not find any rate information for {item_marathi}. Are you sure it's a common crop? What else can I look up?"

    def respond_to_query(self, query: str) -> str:
        """Analyzes the user's query and calls the appropriate function."""
        query_lower = str(query).lower()
        item_match = re.search(r"(?:rate|price)\s+(?:of)?\s*(.+)", query_lower)
        if item_match:
            item = item_match.group(1).strip()
            item = re.sub(r'\s+(on|for)\s+date.*$', '', item, flags=re.IGNORECASE).strip()
            item = re.sub(r'\s+today$', '', item, flags=re.IGNORECASE).strip()
            if not item:
                 return ("It seems you asked for a rate, but I couldn't identify the item. "
                        "Could you please specify it? For example: 'Rate of Kanda'")
            return self.get_rate(item)
        else:
            return ("I understand you're asking about prices, but could you please specify the item? "
                    "For example, you could ask 'What is the rate of tomato?'")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE): # Changed type hint to Update
    user_message = update.message.text
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