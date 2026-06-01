"""CLI утилиты для управления dlt пайплайнами и Airflow DAG'ами.

Этот пакет содержит производственные command-line инструменты для работы с dlt:
  - show_lineage: Визуализация и анализ зависимостей между пайплайнами (lineage graph)
  - generate_dags: Автоматическая генерация Airflow DAG файлов из YAML манифестов

Примеры использования:
    # Показать lineage (граф зависимостей пайплайнов)
    python -m cli.show_lineage
    python -m cli.show_lineage --format json      # JSON формат для программной обработки
    python -m cli.show_lineage --format mermaid   # Mermaid диаграмма для визуализации
    
    # Генерация Airflow DAG файлов из YAML манифестов
    python -m cli.generate_dags                   # инкрементальная генерация
    python -m cli.generate_dags --clean           # удалить старые DAG'и, сгенерировать заново
    
    # Через установленные CLI команды (после pip install -e .):
    dlt-show-lineage
    dlt-show-lineage --format mermaid
    dlt-generate-dags --clean
"""

import cli.show_lineage  # noqa: F401
import cli.generate_dags  # noqa: F401

__all__ = ["show_lineage", "generate_dags"]
