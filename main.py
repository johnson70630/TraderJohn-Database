import os
import json
import logging
from datetime import datetime
import dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackContext, ConversationHandler, filters

from utils.data_processing import upload_json_to_mongodb, upload_csv_to_mysql
from utils.query_data import get_mysql_tables, get_mongodb_collections, format_nested_fields
from utils.query_generator import QueryGenerator
from utils.execute_query import QueryExecutor
from utils.format import format_table

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
dotenv.load_dotenv()

# Initialize query generator and executor
query_generator = QueryGenerator()
query_executor = QueryExecutor(
    mysql_config={
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME')
    },
    mongodb_url=os.getenv('MONGODB_URL'),
    mongodb_name=os.getenv('MONGODB_NAME')
)

# States for ConversationHandler
CHOOSING, UPLOAD_FILE, QUERY_DATA = range(3)

async def start(update: Update, context: CallbackContext) -> int:
    """Start the conversation and show main menu."""
    keyboard = [
        ["Upload Data", "Query Data"],
        ["Help", "Exit"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    
    welcome_message = (
        "👋 Welcome to TraderJohn's Database Bot!\n\n"
        "This bot helps you manage and query your data efficiently. Here's what you can do:\n\n"
        
        "📂 **Upload Data**\n"
        "• Upload CSV files → MySQL\n"
        "• Upload JSON files → MongoDB\n\n"
        
        "📊 **Query Data**\n"
        "• View all tables and collections\n"
        "• Query using natural language\n"
        "• Get data insights\n\n"
        
        "🤔 **Natural Language Query Examples**:\n"
        "• 'Show all orders where price > 100'\n"
        "• 'Find top 5 customers with highest total_spent'\n"
        "• 'Calculate average price from products'\n"
        "• 'Count orders by customer_id'\n\n"
        
        "Please select an option to continue:"
    )
    
    await update.message.reply_text(welcome_message, reply_markup=reply_markup, parse_mode="Markdown")
    return CHOOSING

async def help_command(update: Update, context: CallbackContext) -> int:
    """Show help information."""
    help_text = (
        "🔍 **Query Examples**:\n\n"
        "1. Basic Queries:\n"
        "   • 'Show all users'\n"
        "   • 'Display products'\n\n"
        
        "2. Filtered Queries:\n"
        "   • 'Find orders where total > 1000'\n"
        "   • 'Show users where country is USA'\n\n"
        
        "3. Aggregations:\n"
        "   • 'Calculate average order total'\n"
        "   • 'Count orders by status'\n"
        "   • 'Sum sales by product'\n\n"
        
        "4. Sorting:\n"
        "   • 'Show top 10 customers by spend'\n"
        "   • 'List products ordered by price desc'\n\n"
        
        "5. Data Upload:\n"
        "   • CSV files go to MySQL tables\n"
        "   • JSON files go to MongoDB collections\n\n"
        
        "Use /start to return to main menu"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")
    return CHOOSING

async def handle_upload_data(update: Update, context: CallbackContext) -> int:
    """Handle the upload data option."""
    keyboard = [
        ["CSV", "JSON"],
        ["Back to Menu"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    
    await update.message.reply_text(
        "Please choose the type of file you want to upload:",
        reply_markup=reply_markup
    )
    return UPLOAD_FILE

async def handle_file_type_selection(update: Update, context: CallbackContext) -> int:
    """Handle file type selection for upload."""
    file_type = update.message.text.upper()
    context.user_data['file_type'] = file_type
    
    keyboard = [["Back to Menu"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    
    if file_type == "CSV":
        await update.message.reply_text(
            "Please upload your CSV file. It will be stored in MySQL.",
            reply_markup=reply_markup
        )
    elif file_type == "JSON":
        await update.message.reply_text(
            "Please upload your JSON file. It will be stored in MongoDB.",
            reply_markup=reply_markup
        )
    return UPLOAD_FILE

async def handle_file_upload(update: Update, context: CallbackContext) -> int:
    """Handle file upload and processing."""
    try:
        file = await context.bot.get_file(update.message.document.file_id)
        file_name = update.message.document.file_name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_path = f'/tmp/{timestamp}_{file_name}'
        
        # Download the file
        await file.download_to_drive(temp_path)
        
        # Process based on file type
        file_type = context.user_data.get('file_type', '').upper()
        if file_type == "CSV":
            table_name = file_name.split('.')[0]
            upload_csv_to_mysql(temp_path, table_name)
            await update.message.reply_text(f"CSV data has been uploaded to MySQL table '{table_name}'.")
        elif file_type == "JSON":
            collection_name = file_name.split('.')[0]
            upload_json_to_mongodb(temp_path, collection_name)
            await update.message.reply_text(f"JSON data has been uploaded to MongoDB collection '{collection_name}'.")
        
        # Clean up temporary file
        os.remove(temp_path)
        return CHOOSING
        
    except Exception as e:
        await update.message.reply_text(f"Error processing file: {str(e)}")
        return CHOOSING

async def show_data_overview(update: Update, context: CallbackContext) -> int:
    """Show overview of available data and accept natural language queries."""
    try:
        # Get available data sources
        mongodb_details = get_mongodb_collections()
        mysql_details = get_mysql_tables()
        
        response = "📊 **Available Data Sources**:\n\n"
        
        # MongoDB collections
        response += "MongoDB Collections:\n"
        for collection, fields in mongodb_details.items():
            response += f"\n🔹 {collection}:\n"
            if fields:
                response += format_nested_fields(fields)
            else:
                response += "  (Empty collection)\n"
        
        # MySQL tables
        response += "\nMySQL Tables:\n"
        for table, columns in mysql_details.items():
            response += f"\n🔹 {table}:\n"
            for column_name, data_type in columns:
                response += f"  • {column_name}: {data_type}\n"
        
        # Query examples
        response += "\n🔍 Example Queries:\n"
        response += "• 'Show all records from [table_name]'\n"
        response += "• 'Find items where [field] > [value]'\n"
        response += "• 'Calculate average [field] from [table_name]'\n\n"
        response += "Type your query or use 'Back to Menu' to return:"
        
        keyboard = [["Back to Menu"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)
        
        await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="Markdown")
        return QUERY_DATA
        
    except Exception as e:
        await update.message.reply_text(f"Error retrieving data overview: {str(e)}")
        return CHOOSING

async def handle_query(update: Update, context: CallbackContext) -> int:
    """Handle natural language queries with table format output."""
    try:
        user_input = update.message.text
        
        if user_input.lower() == 'back to menu':
            return await start(update, context)
        
        # Extract query components
        components = query_generator.extract_query_components(user_input)
        
        if not components['from']:
            await update.message.reply_text(
                "Please specify which table or collection you want to query. "
                "You can see available options with 'Query Data'."
            )
            return QUERY_DATA
        
        # Determine target database
        mongodb_collections = get_mongodb_collections()
        mysql_tables = get_mysql_tables()
        
        target = components['from']
        results = None
        query_text = ""
        
        if target in mongodb_collections:
            # MongoDB query
            mongo_query = query_generator.generate_mongodb_query(components)
            results = query_executor.execute_mongodb_query(target, mongo_query)
            query_text = f"MongoDB Query:\n```json\n{json.dumps(mongo_query, indent=2)}\n```\n"
            
        elif target in mysql_tables:
            # SQL query
            sql_query = query_generator.generate_sql_query(components)
            results = query_executor.execute_sql_query(sql_query)
            query_text = f"SQL Query:\n```sql\n{sql_query}\n```\n"
        
        else:
            await update.message.reply_text(f"Could not find table or collection named '{target}'")
            return QUERY_DATA
        
        # Format results as table
        if results:
            results_list = list(results)
            displayed_results = results_list[:10]  # Limit to 10 rows
            
            response = query_text + "\nResults:\n```\n"
            response += format_table(displayed_results)
            
            if len(results_list) > 10:
                response += f"\n(Showing first 10 of {len(results_list)} rows)\n"
            
            response += "```"
            
            # Handle long messages
            if len(response) > 4000:
                # Send query first
                await update.message.reply_text(query_text, parse_mode="Markdown")
                
                # Then send results with fewer rows
                results_text = "Results:\n```\n"
                results_text += format_table(displayed_results[:5])  # Show only 5 rows
                results_text += f"\n(Showing first 5 of {len(results_list)} rows)\n```"
                
                await update.message.reply_text(results_text, parse_mode="Markdown")
            else:
                await update.message.reply_text(response, parse_mode="Markdown")
        else:
            response = query_text + "\n```\nEmpty set\n```"
            await update.message.reply_text(response, parse_mode="Markdown")
            
        return QUERY_DATA
        
    except Exception as e:
        logger.error(f"Query error: {str(e)}", exc_info=True)
        await update.message.reply_text(f"Error processing query: {str(e)}")
        return QUERY_DATA

async def cancel(update: Update, context: CallbackContext) -> int:
    """Cancel conversation."""
    await update.message.reply_text(
        "Goodbye! Type /start to begin again.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END

def main() -> None:
    """Run the bot."""
    # Create the Application and pass it your bot's token
    application = Application.builder().token(os.getenv('BOT_TOKEN')).build()
    
    # Set up conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            CHOOSING: [
                MessageHandler(filters.Regex('^Upload Data$'), handle_upload_data),
                MessageHandler(filters.Regex('^Query Data$'), show_data_overview),
                MessageHandler(filters.Regex('^Help$'), help_command),
                MessageHandler(filters.Regex('^Exit$'), cancel),
                MessageHandler(filters.Regex('^Back to Menu$'), start),
                CommandHandler('start', start),
            ],
            UPLOAD_FILE: [
                MessageHandler(filters.Regex('^(CSV|JSON)$'), handle_file_type_selection),
                MessageHandler(filters.Regex('^Back to Menu$'), start),
                MessageHandler(filters.Document.ALL, handle_file_upload),
                CommandHandler('start', start),
            ],
            QUERY_DATA: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_query),
                CommandHandler('start', start),
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('help', help_command),
        ],
    )
    
    # Add conversation handler and help command
    application.add_handler(conv_handler)
    
    # Start the Bot
    application.run_polling()

if __name__ == '__main__':
    main()