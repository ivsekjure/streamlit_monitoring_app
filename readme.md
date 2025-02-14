# Streamlit App Setup and Deployment to Snowflake

## Clone the Git Repository
```sh
git clone <repository-url>
```

## Create and Activate a Python Virtual Environment

### Windows
```sh
python -m venv venv
venv\Scripts\activate
```

### macOS/Linux
```sh
python3 -m venv venv
source venv/bin/activate
```

## Install Dependencies
```sh
pip install -r requirements.txt
```

### Snowflake Configuration Files

#### Modify Snowflake Connections File (`snowflake/connections.toml`)
Edit the `connections.toml` file located at `~/.snowflake/` or create one if it doesn't exist:

```toml
[connection_name]
account = "your_snowflake_account"
user = "your_username"
authenticator = "snowflake"
password = "your_password"
role = "your_role"
database = "your_database"
schema = "your_schema"
warehouse = "your_warehouse"
```

## Deploying Streamlit App to Snowflake (using Snowflake CLI)

Documentation: [Create and Deploy a Streamlit App Using the Snowflake CLI](https://docs.snowflake.com/en/developer-guide/streamlit/create-streamlit-snowflake-cli).

### Step 1: Create a Stage

### Step 2: Upload App to Snowflake Stage
```sh
snow streamlit deploy -c <connection_name>
```
For deploying changes add --replace
```sh
snow streamlit deploy --replace -c <connection_name>
```
