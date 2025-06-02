from functools import lru_cache
from pathlib import Path

from pydantic import AnyHttpUrl, BaseModel, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine.url import URL

PROJECT_DIR = Path(__file__).parent.parent.parent


class Security(BaseModel):
    jwt_issuer: str = "my-app"
    jwt_secret_key: SecretStr
    jwt_access_token_expire_secs: int = 24 * 3600  # 1d
    refresh_token_expire_secs: int = 28 * 24 * 3600  # 28d
    password_bcrypt_rounds: int = 12
    allowed_hosts: list[str] = ["localhost", "127.0.0.1"]
    backend_cors_origins: list[AnyHttpUrl] = []


class Database(BaseModel):
    hostname: str = "postgres"
    username: str = "postgres"
    password: SecretStr
    port: int = 5432
    db: str = "postgres"


class SVM(BaseModel):
    url_bulk: str = "http://127.0.0.1:8001/items/"
    url_single: str = "http://127.0.0.1:8001/items/"


class BlobStorage(BaseModel):
    connection_string: SecretStr
    container_name: str = "generated-reports"


class OpenAIConfig(BaseModel):
    azure_endpoint: AnyHttpUrl
    api_key: SecretStr
    config: str = "DEMO"


class ScraperConfig(BaseModel):
    scraper_url: AnyHttpUrl


class Urls(BaseModel):
    frontend: str
    analysis_orchestration: str
    application_backend: str
    orbis_engine: str
    news_backend: str
    news_scraper: str

class GraphDb(BaseModel):
    uri: str
    user: str
    password: str


class Settings(BaseSettings):
    security: Security
    database: Database
    svm: SVM = SVM()
    blob_storage: BlobStorage
    openai: OpenAIConfig
    scraper: ScraperConfig
    urls: Urls
    graphdb: GraphDb

    @computed_field  # type: ignore[prop-decorator]
    @property
    def sqlalchemy_database_uri(self) -> URL:
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.database.username,
            password=self.database.password.get_secret_value(),
            host=self.database.hostname,
            port=self.database.port,
            database=self.database.db,
        )

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_DIR}/.env",
        case_sensitive=False,
        env_nested_delimiter="__",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore
