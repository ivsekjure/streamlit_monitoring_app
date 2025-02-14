# Streamlit App Setup and Deployment to Snowflake

## Prerequisites
Ensure you have the following installed:
- Python (>= 3.8)
- Git
- Snowflake CLI
- Streamlit

## Clone the Git Repository
```sh
git clone <repository-url>
cd <repository-name>
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

## Set Up Environment Configuration
Create an `environment.yml` file for Snowflake Streamlit deployment:

```yaml
name: snowflake-streamlit-env
channels:
  - conda-forge
dependencies:
  - python=3.8
  - streamlit
  - pip
  - pip:
      - snowflake-connector-python
      - snowflake-cli-labs
```

Install the environment:
```sh
conda env create -f environment.yml
conda activate snowflake-streamlit-env
```

### Snowflake Configuration Files
#### 1. `snowflake.yml`
Create a `snowflake.yml` file in your project root:

```yaml
connections:
  default:
    account: <your_snowflake_account>
    user: <your_username>
    role: <your_role>
    database: <your_database>
    schema: <your_schema>
    warehouse: <your_warehouse>
```

#### 2. Modify Snowflake Connections File (`snowflake/connections.toml`)
Edit the `connections.toml` file located at `~/.snowflake/` or create one if it doesn't exist:

```toml
[connections.default]
account = "your_snowflake_account"
user = "your_username"
password = "your_password"
role = "your_role"
database = "your_database"
schema = "your_schema"
warehouse = "your_warehouse"
```

## Deploying Streamlit App to Snowflake

### Step 1: Create a Stage
```sh
snow sql -q "CREATE OR REPLACE STAGE my_streamlit_stage;"
```

### Step 2: Upload App to Snowflake Stage
```sh
snow streamlit create my_streamlit_app
snow streamlit deploy
```

### Step 3: Verify Deployment
Check the deployment status:
```sh
snow streamlit list
```

## Running Streamlit Locally
To test your Streamlit app locally:
```sh
streamlit run app.py
```

## Additional Snowflake CLI Commands
- View deployed apps: `snow streamlit list`
- Update an existing app: `snow streamlit deploy`
- Delete an app: `snow streamlit drop my_streamlit_app`

## Troubleshooting
- Ensure your Snowflake credentials are correctly configured.
- Use `streamlit run app.py --server.port 8501` if you need to specify a different port.
- If facing dependency issues, recreate the environment using `conda remove --name snowflake-streamlit-env --all` before reinstalling.

---
### Need Help?
For more details, visit the [Streamlit Documentation](https://docs.streamlit.io/) or [Snowflake Documentation](https://docs.snowflake.com/).
