import os
import json
import logging
import re
from datetime import datetime
import dotenv
from textwrap import dedent
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackContext, ConversationHandler, filters

from utils.data_processing import upload_json_to_mongodb, upload_csv_to_mysql
from utils.query_data import test_database_connections, get_mysql_tables, get_mongodb_collections
from utils.query_generator import QueryGenerator
from utils.mongodb_query_generator import MongoDBQueryGenerator
from utils.execute_query import QueryExecutor
from utils.format import format_nested_fields
from utils.samples import get_sample_data, get_sample_queries
from utils.query_processer import process_mongodb_results

from typing import Dict, List, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Create logger for this module
logger = logging.getLogger(__name__)

# Load environment variables
dotenv.load_dotenv()

# Initialize query generator and executor
query_generator = QueryGenerator()
mongodb_query_generator = MongoDBQueryGenerator()
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
CHOOSING, SAMPLE_QUERIES, UPLOAD_FILE, QUERY_DATA = range(4)

async def start(update: Update, context: CallbackContext) -> int:
    """Start the conversation and show main menu."""
    keyboard = [
        ["Upload Data", "Query Data"],
        ["Sample Queries", "Exit"]
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
        mongodb_details = {}
        mysql_details = {}

        # Get MongoDB collections
        try:
            mongodb_details = get_mongodb_collections()
            logger.info(f"MongoDB collections found: {list(mongodb_details.keys())}")
        except Exception as e:
            logger.error(f"MongoDB error: {str(e)}")
            mongodb_details = {}

        # Get MySQL tables
        try:
            mysql_details = get_mysql_tables()
            logger.info(f"MySQL tables found: {list(mysql_details.keys())}")
        except Exception as e:
            logger.error(f"MySQL error: {str(e)}")
            mysql_details = {}

        # Store in user context
        context.user_data.clear()  # Clear previous data
        context.user_data['mongodb_collections'] = list(mongodb_details.keys())
        context.user_data['mysql_tables'] = list(mysql_details.keys())
        
        keyboard = [
            ["Query MongoDB", "Query MySQL"],
            ["Back to Menu"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        response_parts = []
        response_parts.append("📊 Available Data Sources:\n")
        
        # MongoDB collections section
        response_parts.append("\nMongoDB Collections:")
        if mongodb_details:
            for collection, fields in mongodb_details.items():
                response_parts.append(f"\n🔹 {collection}:")
                if fields:
                    formatted_fields = format_nested_fields(fields)
                    formatted_fields = formatted_fields.replace('*', '').replace('_', '').replace('`', '')
                    response_parts.append(formatted_fields)
                else:
                    response_parts.append("  (Empty collection)")
        else:
            response_parts.append("\n(No collections available)")
        
        # MySQL tables section
        response_parts.append("\nMySQL Tables:")
        if mysql_details:
            for table, columns in mysql_details.items():
                response_parts.append(f"\n🔹 {table}:")
                for column_name, data_type in columns:
                    response_parts.append(f"  • {column_name}: {data_type}")
        else:
            response_parts.append("\n(No tables available)")
        
        response_parts.append("\n\n🔍 To query data, first select the database type you want to query from the keyboard below.")
        
        response = "\n".join(response_parts)
        
        try:
            await update.message.reply_text(
                response,
                reply_markup=reply_markup
            )
            return QUERY_DATA
            
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
            max_length = 4000
            for i in range(0, len(response), max_length):
                chunk = response[i:i + max_length]
                await update.message.reply_text(chunk, reply_markup=reply_markup if i == 0 else None)
            return QUERY_DATA
            
    except Exception as e:
        logger.error(f"Error in show_data_overview: {str(e)}", exc_info=True)
        await update.message.reply_text(
            "An error occurred while retrieving data sources. Please try again.",
            reply_markup=ReplyKeyboardMarkup([["Back to Menu"]], resize_keyboard=True)
        )
        return CHOOSING
    
async def handle_database_selection(update: Update, context: CallbackContext) -> int:
    """Handle database type selection for querying."""
    try:
        selection = update.message.text.strip()
        logger.info(f"Database selection: {selection}")

        if selection == "Back to Menu":
            return await start(update, context)

        # Verify context data exists
        if 'mongodb_collections' not in context.user_data or 'mysql_tables' not in context.user_data:
            logger.error("Missing database info in context")
            # Try to refresh the data
            mongodb_details = get_mongodb_collections()
            mysql_details = get_mysql_tables()
            context.user_data['mongodb_collections'] = list(mongodb_details.keys())
            context.user_data['mysql_tables'] = list(mysql_details.keys())

        if selection == "Query MongoDB":
            collections = context.user_data.get('mongodb_collections', [])
            if not collections:
                await update.message.reply_text("No MongoDB collections available.")
                return QUERY_DATA

            response = (
                "📚 Available MongoDB Collections:\n\n" +
                "\n".join([f"• `{coll}`" for coll in collections]) +
                "\n\n🔍 Query Examples:\n" +
                "• Type collection name to see all documents\n" +
                "• `find in [collection] where field > value`\n" +
                "• `count documents in [collection] grouped by field`"
            )

        elif selection == "Query MySQL":
            tables = context.user_data.get('mysql_tables', [])
            if not tables:
                await update.message.reply_text("No MySQL tables available.")
                return QUERY_DATA

            response = (
                "📚 Available MySQL Tables:\n\n" +
                "\n".join([f"• `{table}`" for table in tables]) +
                "\n\n🔍 Query Examples:\n" +
                "• Type table name to see first 10 rows\n" +
                "• `select * from [table] where [condition]`\n" +
                "• `show enginetype from cars group by enginetype having average price > 10000`\n" +
                "• `show cars where price > 20000`\n"
            )

        context.user_data['selected_db'] = selection
        keyboard = [["Back to Menu"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)
        
        try:
            await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed to send message with Markdown: {str(e)}")
            # Try sending without Markdown formatting
            await update.message.reply_text(response.replace('`', ''), reply_markup=reply_markup)
            
        return QUERY_DATA

    except Exception as e:
        logger.error(f"Database selection error: {str(e)}", exc_info=True)
        await update.message.reply_text(
            "An error occurred. Please select 'Query Data' from the main menu.",
            reply_markup=ReplyKeyboardMarkup([["Back to Menu"]], resize_keyboard=True)
        )
        return CHOOSING
    
async def handle_query(update: Update, context: CallbackContext) -> int:
    """Handle natural language queries."""
    try:
        user_input = update.message.text.strip()
        logger.info(f"Processing query: {user_input}")
        
        if user_input.lower() == 'back to menu':
            return await start(update, context)
        
        selected_db = context.user_data.get('selected_db')
        if not selected_db:
            await update.message.reply_text(
                "Please select a database type first.",
                reply_markup=ReplyKeyboardMarkup([["Query MongoDB", "Query MySQL"], ["Back to Menu"]], resize_keyboard=True)
            )
            return QUERY_DATA

        # Handle MongoDB Query
        if selected_db == "Query MongoDB":
            available_collections = context.user_data.get('mongodb_collections', [])
            
            # First use QueryGenerator to convert natural language to SQL components
            query_components = query_generator.extract_query_components(user_input, available_collections)
            
            if not query_components['from']:
                await update.message.reply_text(
                    f"Please specify a valid collection name.\n\n"
                    f"Available collections:\n" +
                    "\n".join([f"• {collection}" for collection in available_collections])
                )
                return QUERY_DATA

            try:
                # Generate SQL query from components
                sql_query = query_generator.generate_sql_query(query_components)
                logger.info(f"Generated SQL query: {sql_query}")
                
                # Convert SQL to MongoDB query
                mongo_query = mongodb_query_generator.generate_mongo_query(sql_query)
                collection_name = mongo_query['collection']
                pipeline = mongo_query['pipeline']
                
                # Format the MongoDB shell command
                mongo_command = f"db.{collection_name}.aggregate({json.dumps(pipeline, indent=2)})"
                
                # Send the formatted command
                await update.message.reply_text(
                    f"```javascript\n{mongo_command}\n```",
                    parse_mode="Markdown"
                )

                # Execute MongoDB query and get results
                results = query_executor.execute_mongodb_query(collection_name, {"aggregate": pipeline})

                if not results:
                    await update.message.reply_text("No results found.")
                    return QUERY_DATA

                # Process results in smaller chunks
                reply_message = process_mongodb_results(results)
                await update.message.reply_text(reply_message[0], parse_mode="Markdown")

            except Exception as e:
                logger.error(f"MongoDB execution error: {str(e)}")
                await update.message.reply_text(f"Error executing query: {str(e)}")

            return QUERY_DATA
        
        # Handle MySQL Query
        if selected_db == "Query MySQL":
            available_tables = context.user_data.get('mysql_tables', [])
            
            # Extract query components using the available tables
            query_components = query_generator.extract_query_components(user_input, available_tables)
            
            if not query_components['from']:
                await update.message.reply_text(
                    f"Please specify a valid table name.\n\n"
                    f"Available tables:\n" +
                    "\n".join([f"• {table}" for table in available_tables])
                )
                return QUERY_DATA

            # Generate and execute SQL query
            sql_query = query_generator.generate_sql_query(query_components)
            
            try:
                # Send query
                await update.message.reply_text(
                    f"SQL Query:\n```sql\n{sql_query}\n```",
                    parse_mode="Markdown"
                )
                
                # Execute query and get results
                results = query_executor.execute_sql_query(sql_query)
                
                if not results:
                    await update.message.reply_text("No results found.")
                    return QUERY_DATA

                # Process results in smaller chunks
                await process_and_send_results(update, results)
                
            except Exception as e:
                logger.error(f"MySQL execution error: {str(e)}")
                await update.message.reply_text(f"Error executing query: {str(e)}")
            
            return QUERY_DATA
            
    except Exception as e:
        logger.error(f"Query error: {str(e)}", exc_info=True)
        await update.message.reply_text(f"Error processing query: {str(e)}")
        return QUERY_DATA
            
async def process_and_send_results(update: Update, results: List[Dict[str, Any]]) -> None:
    """Helper function to process and send query results in chunks"""
    # Get columns
    columns = list(results[0].keys())
    
    # Process results in chunks of 5 columns
    for col_start in range(0, len(columns), 5):
        # Get current column chunk (up to 5 columns)
        current_columns = columns[col_start:col_start + 5]
        
        # Calculate max width for each column in this chunk
        col_widths = {}
        for col in current_columns:
            max_width = len(col)
            for row in results[:10]:  # Look at first 10 rows for width calculation
                width = len(str(row[col]))
                max_width = min(max(max_width, width), 20)  # Cap at 20 chars
            col_widths[col] = max_width

        # Create formatted rows for this chunk
        formatted_rows = []
        
        # Add header for this chunk
        chunk_header = " | ".join(f"{col:{col_widths[col]}}" for col in current_columns)
        chunk_separator = "-" * len(chunk_header)
        formatted_rows.append(f"Columns {col_start + 1}-{col_start + len(current_columns)}:")
        formatted_rows.append(chunk_header)
        formatted_rows.append(chunk_separator)
        
        # Add data rows for this chunk
        for row in results[:10]:  # Show first 10 rows
            row_values = [f"{str(row[col])[:col_widths[col]]:{col_widths[col]}}" for col in current_columns]
            formatted_rows.append(" | ".join(row_values))
        
        # Send this column chunk
        chunk_message = "```\n" + "\n".join(formatted_rows) + "\n```"
        await update.message.reply_text(chunk_message, parse_mode="Markdown")
    
    if len(results) > 10:
        await update.message.reply_text(f"Showing first 10 of {len(results)} results")
    else:
        await update.message.reply_text(f"Total rows: {len(results)}")
        
async def cancel(update: Update, context: CallbackContext) -> int:
    """Cancel conversation."""
    await update.message.reply_text(
        "Goodbye! Type /start to begin again.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END

async def show_sample_queries(update: Update, context: CallbackContext) -> int:
    """Show sample data and ask the user which kind of query sample they want."""
    sample_data = get_sample_data()
    
    # Display sample data
    await update.message.reply_text(
        "📊 Sample Data for MySQL:\n\n"
        f"```{sample_data['MySQL']}\n```"
        "📊 Sample Data for MongoDB:\n\n"
        f"```json\n{sample_data['MongoDB']}\n```",
        parse_mode="Markdown"
    )
    
    # Prepare keyboard options for query type selection
    keyboard = [
        ["Group By", "Join", "Sum"],
        ["Order By", "Where", "Random"],
        ["Back to Menu"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    
    await update.message.reply_text(
        "Please choose the type of query sample you want to see:",
        reply_markup=reply_markup
    )
    return SAMPLE_QUERIES

async def handle_query_sample_selection(update: Update, context: CallbackContext) -> int:
    """Handle the user's query type selection and show a sample query."""
    query_type = update.message.text.strip()
    
    if query_type == "Back to Menu":
        return await start(update, context)
    
    if query_type not in ["Group By", "Join", "Sum", "Order By", "Where", "Random"]:
        await update.message.reply_text("Invalid selection. Please choose a valid query type.")
        return SAMPLE_QUERIES
    
    # Get the selected sample query
    if query_type == "Random":
        sample_query = get_sample_queries()
    else:
        sample_query = get_sample_queries(type=query_type)
    
    # Show the sample query and results
    await update.message.reply_text(
        f"🔍 **Sample Query - `{sample_query['type']}`**\n\n"
        f"💬 **Natural Language Description:**\n"
        f"`{sample_query['natural_language']}`\n\n"
        
        f"🗄 **MySQL Query:**\n"
        f"```\n{dedent(sample_query['mysql'])}\n```\n"
        f"📋 **MySQL Query Result:**\n"
        f"```{dedent(sample_query['mysql_result'])}\n```\n\n"
        
        f"🗄 **MongoDB Query:**\n"
        f"```{dedent(sample_query['mongodb'])}\n```\n"
        f"📋 **MongoDB Query Result:**\n"
        f"```{dedent(sample_query['mongodb_result'])}```",
        parse_mode="Markdown", reply_markup=ReplyKeyboardMarkup([["Back to Menu"]], resize_keyboard=True)
    )
    return SAMPLE_QUERIES

def main() -> None:
    """Run the bot."""
    # Create the Application and pass it your bot's token
    application = Application.builder().token(os.getenv('BOT_TOKEN')).build()
    
    # Set up conversation handler with explicit handler for database selection
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            CHOOSING: [
                MessageHandler(filters.Regex('^Upload Data$'), handle_upload_data),
                MessageHandler(filters.Regex('^Query Data$'), show_data_overview),
                MessageHandler(filters.Regex('^Sample Queries$'), show_sample_queries),
                MessageHandler(filters.Regex('^Exit$'), cancel),
                MessageHandler(filters.Regex('^Back to Menu$'), start),
                CommandHandler('start', start),
            ],
            SAMPLE_QUERIES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_query_sample_selection),
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
                # Explicitly handle both MongoDB and MySQL selection
                MessageHandler(filters.Regex('^Query MongoDB$'), handle_database_selection),
                MessageHandler(filters.Regex('^Query MySQL$'), handle_database_selection),
                MessageHandler(filters.Regex('^Back to Menu$'), start),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_query),
                CommandHandler('start', start),
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
        ],
    )
    
    application.add_handler(conv_handler)
    application.run_polling()

if __name__ == '__main__':
    test_database_connections()
    main()