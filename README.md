# prefect-firebolt-demo

This repo contains an example flow for loading a dataset into a Firebolt database with Prefect

### Running the example locally

I recommend running the example in a virtual environment created by the virtual environment manager of your choice.

1. Clone the repo
2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Add AWS and Firebolt credentials to your Prefect config file (default location is `~/.prefect/config.toml`):
    ```toml
    [context.secrets]
    FIREBOLT_USERNAME = "me@example.com"
    FIREBOLT_PASSWORD = "my_password"

    [context.secrets.AWS_CREDENTIALS]
    ACCESS_KEY = "access_key"
    SECRET_ACCESS_KEY = "secret_access_key"
    ```

4. Run the flow
    ```bash
    python flow.py
    ```