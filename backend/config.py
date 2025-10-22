import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your_openai_api_key_here")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./demo.db")

# Application Configuration
DEBUG = os.getenv("DEBUG", "True").lower() == "true"
