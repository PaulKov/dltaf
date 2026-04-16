"""Загрузчик YAML манифестов с кэшированием и валидацией.

Этот модуль предоставляет класс ManifestLoader для эффективной загрузки
и кэширования YAML манифестов в процессе парсинга Airflow DAG.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from dlt_utils.manifest_runner import load_manifest, validate_manifest

logger = logging.getLogger(__name__)


class ManifestLoadError(Exception):
    """Ошибка при загрузке манифеста."""
    pass


class ManifestLoader:
    """Загрузчик YAML манифестов с кэшированием.
    
    Кэширует загруженные манифесты в рамках одного экземпляра (один DAG parse).
    При следующем парсинге Airflow (каждые ~30 секунд) создается новый экземпляр.
    
    Attributes:
        base_dir: Базовая директория для поиска манифестов
        _manifest_cache: Кэш загруженных манифестов
        _yaml_files_cache: Кэш списка YAML файлов в директории
    """
    
    def __init__(self, base_dir: Optional[Path] = None):
        """Инициализирует загрузчик манифестов.
        
        Args:
            base_dir: Базовая директория. Если не указана, используется директория 
                     относительно текущего файла: dlt_pipelines/manifests/
        """
        if base_dir is None:
            current = Path(__file__).parent.parent
            base_dir = current / "dlt_pipelines" / "manifests"
        
        self.base_dir = Path(base_dir).resolve()
        
        if not self.base_dir.exists():
            raise ManifestLoadError(f"Директория с манифестами не найдена: {self.base_dir}")
        
        self._manifest_cache: Dict[Path, Dict[str, Any]] = {}
        self._yaml_files_cache: Optional[List[Path]] = None
        
        logger.debug(f"ManifestLoader инициализирован с base_dir: {self.base_dir}")
    
    def get_yaml_files(self) -> List[Path]:
        """Получает список всех YAML файлов в базовой директории.
        
        Результат кэширует для повторных вызовов.
        
        Returns:
            Список путей к YAML файлам
        """
        if self._yaml_files_cache is None:
            self._yaml_files_cache = sorted(self.base_dir.glob("*.yaml"))
            logger.debug(f"Найдено {len(self._yaml_files_cache)} YAML файлов")
        
        return self._yaml_files_cache
    
    def load(self, manifest_path: Path | str) -> Dict[str, Any]:
        """Загружает манифест с кэшированием.
        
        Args:
            manifest_path: Путь к манифесту (абсолютный или относительный)
        
        Returns:
            Словарь с данными манифеста
        
        Raises:
            ManifestLoadError: Если манифест не найден или невалиден
        """
        # Преобразуем в Path
        path = Path(manifest_path)
        
        # Если путь относительный, разрешаем относительно base_dir
        if not path.is_absolute():
            path = (self.base_dir / path).resolve()
        
        # Проверяем кэш
        if path in self._manifest_cache:
            logger.debug(f"Манифест загружен из кэша: {path.name}")
            return self._manifest_cache[path]
        
        # Проверяем существование
        if not path.exists():
            raise ManifestLoadError(f"Манифест не найден: {path}")
        
        try:
            # Загружаем и валидируем
            manifest = load_manifest(path)
            validate_manifest(manifest)
            
            # Кэшируем
            self._manifest_cache[path] = manifest
            logger.debug(f"Манифест загружен и провалидирован: {path.name}")
            
            return manifest
            
        except Exception as e:
            raise ManifestLoadError(f"Ошибка загрузки манифеста {path}: {e}") from e
    
    def get_pipeline_name(self, manifest_path: Path | str) -> str:
        """Извлекает имя пайплайна из манифеста.
        
        Args:
            manifest_path: Путь к манифесту
        
        Returns:
            Имя пайплайна
        """
        manifest = self.load(manifest_path)
        pipeline = manifest.get("pipeline", {})
        name = pipeline.get("name")
        
        if not name:
            raise ManifestLoadError(f"pipeline.name не найден в манифесте: {manifest_path}")
        
        return str(name)
    
    def find_manifest_by_name(self, pipeline_name: str) -> Optional[Path]:
        """Находит манифест по имени пайплайна.
        
        Args:
            pipeline_name: Имя пайплайна для поиска
        
        Returns:
            Путь к манифесту или None, если не найден
        """
        # Сначала пробуем прямое совпадение с именем файла
        direct_path = self.base_dir / f"{pipeline_name}.yaml"
        if direct_path.exists():
            return direct_path
        
        # Иначе ищем в кэше или сканируем все файлы
        for yaml_file in self.get_yaml_files():
            try:
                manifest = self.load(yaml_file)
                if manifest.get("pipeline", {}).get("name") == pipeline_name:
                    return yaml_file
            except Exception:
                continue
        
        return None
    
    def clear_cache(self) -> None:
        """Очищает кэш загруженных манифестов."""
        self._manifest_cache.clear()
        self._yaml_files_cache = None
        logger.debug("Кэш манифестов очищен")