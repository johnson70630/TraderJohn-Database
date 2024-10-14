import os
import dotenv 
import pandas as pd
from sqlalchemy import create_engine
from telegram import Update, InputFile, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, filters, CallbackContext

dotenv.load_dotenv()

database_url = os.getenv('DATABASE_URL')
# Initialize the database engine
engine = create_engine(database_url)


def start(update: Update, context: CallbackContext) -> None:
    keyboard = [
        ['Upload Data', 'Query Data'], 
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard)

    update.message.reply_text('Hi! Welcome to TraderJohns\'s Bot. You can upload your data, create your database, and interact with your data!', reply_markup=reply_markup)


def handle_file(update: Update, context: CallbackContext) -> None:
    file = context.bot.getFile(update.message.document.file_id)
    file_path = os.path.join('/tmp', update.message.document.file_name)
    file.download(file_path)
    
    # Convert CSV to MySQL
    df = pd.read_csv(file_path)
    df.to_sql('uploaded_data', con=engine, if_exists='replace', index=False)
    
    update.message.reply_text('File has been uploaded and converted to MySQL database.')


def query(update: Update, context: CallbackContext) -> None:
    query_text = ' '.join(context.args)
    result = pd.read_sql(query_text, con=engine)
    update.message.reply_text(result.to_string())


def main() -> None:
    token = os.getenv('BOT_TOKEN')
    updater = Updater(token)
    dispatcher = updater.dispatcher

    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(MessageHandler(filters.document.mime_type("text/csv"), handle_file))
    dispatcher.add_handler(CommandHandler("query", query))

    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()