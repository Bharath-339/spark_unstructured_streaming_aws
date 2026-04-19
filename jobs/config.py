import os

from dotenv import load_dotenv

# Load a .env file located next to this config.py so values are available via os.getenv
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

configs = {
    "AWS_ACCESS_KEY": os.getenv("AWS_ACCESS_KEY", "your_aws_access_key"),
    "AWS_SECRET_KEY": os.getenv("AWS_SECRET_KEY", "your_aws_secret_key"),
}
