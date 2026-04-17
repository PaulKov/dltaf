from __future__ import annotations

from .base import CommandGroup
from .contracts.test import ContractTestCommand
from .dags.generate import DagsGenerateCommand
from .dev.build_package import DevBuildPackageCommand
from .dev.publish_package import DevPublishPackageCommand
from .dev.repo_split_inventory import DevRepoSplitInventoryCommand
from .dev.run_import_rehearsal import DevRunImportRehearsalCommand
from .dev.export_consumer_skeleton import DevExportConsumerSkeletonCommand
from .dev.export_cutover_handoff import DevExportCutoverHandoffCommand
from .dev.export_cutover_dry_run import DevExportCutoverDryRunCommand
from .dev.export_import_rehearsal import DevExportImportRehearsalCommand
from .dev.export_framework_skeleton import DevExportFrameworkSkeletonCommand
from .dev.export_repo_split_plan import DevExportRepoSplitPlanCommand
from .dev.smoke_installed_package import DevSmokeInstalledPackageCommand
from .docs.generate_cli_reference import GenerateCliReferenceCommand
from .docs.update_dev_metrics import UpdateDevMetricsCommand
from .infra_checks.list import InfraChecksListCommand
from .infra_checks.run import InfraChecksRunCommand
from .lineage.show import LineageShowCommand
from .manifest.doctor import ManifestDoctorCommand
from .manifest.lint import ManifestLintCommand
from .manifest.run import ManifestRunCommand
from .manifest.schema import ManifestSchemaCommand
from .runs.list import RunsListCommand
from .scaffold.integration import ScaffoldIntegrationCommand


def command_groups() -> list[CommandGroup]:
    return [
        CommandGroup(
            name="manifest",
            help="Запуск, проверка и анализ manifest-driven pipelines.",
            description="Команды жизненного цикла manifest.",
            commands=(
                ManifestRunCommand(),
                ManifestLintCommand(),
                ManifestDoctorCommand(),
                ManifestSchemaCommand(),
            ),
        ),
        CommandGroup(
            name="dev",
            help="Сборка и проверка dev package artifacts без release tags.",
            description="Поток сборки, публикации и установки dev package artifacts.",
            commands=(
                DevBuildPackageCommand(),
                DevPublishPackageCommand(),
                DevSmokeInstalledPackageCommand(),
                DevRepoSplitInventoryCommand(),
                DevExportFrameworkSkeletonCommand(),
                DevExportConsumerSkeletonCommand(),
                DevExportRepoSplitPlanCommand(),
                DevExportCutoverDryRunCommand(),
                DevExportCutoverHandoffCommand(),
                DevExportImportRehearsalCommand(),
                DevRunImportRehearsalCommand(),
            ),
        ),
        CommandGroup(
            name="docs",
            help="Генерация и проверка документации проекта.",
            description="Инструменты документации.",
            commands=(GenerateCliReferenceCommand(), UpdateDevMetricsCommand()),
        ),
        CommandGroup(
            name="dags",
            help="Генерация Airflow DAG wrappers из manifests.",
            description="Утилиты генерации DAG.",
            commands=(DagsGenerateCommand(),),
        ),
        CommandGroup(
            name="infra-checks",
            help="Просмотр и запуск инфраструктурных проверок.",
            description="Онлайн-проверки инфраструктуры.",
            commands=(InfraChecksListCommand(), InfraChecksRunCommand()),
        ),
        CommandGroup(
            name="contracts",
            help="Проверка payload contracts и samples.",
            description="Команды тестирования payload contracts.",
            commands=(ContractTestCommand(),),
        ),
        CommandGroup(
            name="lineage",
            help="Показ pipeline lineage и зависимостей.",
            description="Команды анализа lineage и зависимостей.",
            commands=(LineageShowCommand(),),
        ),
        CommandGroup(
            name="runs",
            help="Просмотр audit-записей запусков pipeline.",
            description="Команды истории запусков.",
            commands=(RunsListCommand(),),
        ),
        CommandGroup(
            name="scaffold",
            help="Создание каркаса новой интеграции.",
            description="Утилиты scaffold.",
            commands=(ScaffoldIntegrationCommand(),),
        ),
    ]
