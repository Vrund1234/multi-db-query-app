import streamlit as st
import psycopg2
import pymongo
import mysql.connector
import pymssql
import pandas as pd
import google.generativeai as genai
import json
from bson import ObjectId
import traceback

# Configure Gemini API Key
genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

st.title("Multi-Database AI Query Tool")

# Step 1: Select Database Type
db_type = st.selectbox("Select Database Type", ["PostgreSQL", "MongoDB", "MySQL", "MSSQL"])

# Step 2: Enter Connection Details
st.subheader("Enter Database Connection Details")

host = st.text_input("Host", "localhost")

# Default ports per DB type if empty
default_ports = {
    "PostgreSQL": "5432",
    "MongoDB": "27017",
    "MySQL": "3306",
    "MSSQL": "1433"
}
port = st.text_input("Port", default_ports.get(db_type, ""))

database = st.text_input("Database Name", "")
user = st.text_input("Username", "")
password = st.text_input("Password", type="password")

# For MongoDB, also ask collection name explicitly (optional)
collection_name = None
if db_type == "MongoDB":
    collection_name = st.text_input("Collection Name (required for MongoDB queries)")

# Helper Classes & Functions
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

def generate_sql(nl_query, db_type):
    try:
        model = genai.GenerativeModel(model_name="gemini-2.0-flash")

        prompt = f"""
        Convert this user query into a **valid** SQL query for **{db_type}**.
        ❌ Do NOT include database names (like `DemoDB.table_name`).
        ✅ Just return SQL for a **single** database at a time.
        ❌ Do NOT use cross-database references.

        User Query: {nl_query}
        SQL:
        """
        response = model.generate_content(prompt)
        sql_query = response.text.strip().replace("```sql", "").replace("```", "").strip()
        return sql_query
    except Exception as e:
        return f"Error generating SQL: {str(e)}"

def execute_mongo_query(collection_name, db_config):
    try:
        client = pymongo.MongoClient(db_config["host"], int(db_config["port"]))
        db = client[db_config["database"]]
        collection = db[collection_name]
        data = list(collection.find({}, {"_id": 0}))  # Exclude _id
        return {"data": data}
    except Exception as e:
        return {"error": str(e)}

def execute_sql_query(sql_query, db_type, db_config):
    try:
        if db_type == "PostgreSQL":
            conn = psycopg2.connect(**db_config)
        elif db_type == "MySQL":
            conn = mysql.connector.connect(**db_config)
        elif db_type == "MSSQL":
            conn = pymssql.connect(
                server=db_config["host"],
                user=db_config["user"],
                password=db_config["password"],
                database=db_config["database"],
                port=int(db_config.get("port", 1433))
            )
        else:
            return {"error": "Invalid database type"}

        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        return {"columns": column_names, "data": [list(row) for row in result]}
    except Exception as e:
        return {"error": str(e)}

# Step 3: Connect to Database
if st.button("Connect"):
    # Use defaults for port if empty
    use_port = port if port else default_ports.get(db_type)
    db_config = {
        "host": host,
        "port": int(use_port) if use_port else None,
        "database": database,
        "user": user,
        "password": password,
    }

    # Remove port from dict if None (some drivers don't want it)
    if db_config["port"] is None:
        db_config.pop("port")

    try:
        if db_type == "PostgreSQL":
            psycopg2.connect(**db_config).close()
        elif db_type == "MySQL":
            mysql.connector.connect(**db_config).close()
        elif db_type == "MSSQL":
            conn = pymssql.connect(
                server=db_config["host"],
                user=db_config["user"],
                password=db_config["password"],
                database=db_config["database"],
                port=int(db_config.get("port", 1433))
            )
            conn.close()
        elif db_type == "MongoDB":
            client = pymongo.MongoClient(host, int(use_port))
            client[database].command("ping")
        else:
            st.error("Unsupported database type.")
            st.stop()

        st.success(f"Connected to {db_type} successfully!")
        st.session_state["db_type"] = db_type
        st.session_state["db_config"] = db_config
        if db_type == "MongoDB":
            st.session_state["collection_name"] = collection_name

    except Exception as e:
        st.error(f"Connection failed: {str(e)}")

# Step 4: Query Processing
if "db_type" in st.session_state:
    st.subheader(f"Ask a question for {st.session_state['db_type']}")
    user_query = st.text_input("Enter your question:")

    if st.button("Get Answer"):
        try:
            db_type = st.session_state["db_type"]
            db_config = st.session_state["db_config"]

            if db_type == "MongoDB":
                coll_name = st.session_state.get("collection_name")
                if not coll_name:
                    st.error("Please enter a collection name to query MongoDB.")
                    st.stop()

                result = execute_mongo_query(coll_name, db_config)
                if "error" in result:
                    st.error(result["error"])
                else:
                    df = pd.DataFrame(result["data"])
                    st.dataframe(df)
            else:
                sql_query = generate_sql(user_query, db_type)
                if sql_query.startswith("Error"):
                    st.error(sql_query)
                else:
                    st.subheader("Generated SQL Query:")
                    st.code(sql_query, language="sql")
                    result = execute_sql_query(sql_query, db_type, db_config)
                    if "error" in result:
                        st.error(result["error"])
                    else:
                        df = pd.DataFrame(result["data"], columns=result["columns"])
                        st.dataframe(df)

        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")
            st.text(traceback.format_exc())
