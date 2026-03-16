from __future__ import annotations

import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProjectConfig:
    name: str
    path: str
    repo: str = ""
    branch: str = ""
    deploy: str = ""
    description: str = ""


@dataclass(frozen=True)
class ProjectRegistry:
    default_project: str | None
    projects: dict[str, ProjectConfig]


def load_project_registry(config_path: str) -> ProjectRegistry:
    if not os.path.exists(config_path):
        raise RuntimeError(
            f"Missing project registry: {config_path}. "
            "Create TELECODE_PROJECTS_FILE with one or more named projects."
        )

    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    if not isinstance(raw, dict):
        raise RuntimeError("Project registry must be a JSON object.")

    default_project = raw.get("default_project")
    projects_raw = raw.get("projects")
    if not isinstance(projects_raw, (list, dict)):
        raise RuntimeError("Project registry must include a 'projects' list or object.")

    projects: dict[str, ProjectConfig] = {}
    if isinstance(projects_raw, list):
        for item in projects_raw:
            project = _load_project(item)
            projects[project.name] = project
    else:
        for name, item in projects_raw.items():
            if not isinstance(item, dict):
                raise RuntimeError(f"Project entry for '{name}' must be an object.")
            data = dict(item)
            data.setdefault("name", name)
            project = _load_project(data)
            projects[project.name] = project

    if not projects:
        raise RuntimeError("Project registry must define at least one project.")

    if default_project is not None and default_project not in projects:
        raise RuntimeError(f"Default project '{default_project}' is not defined.")

    if default_project is None and len(projects) == 1:
        default_project = next(iter(projects))

    return ProjectRegistry(default_project=default_project, projects=projects)


def format_project_list(registry: ProjectRegistry, current: str | None = None) -> str:
    lines = ["Available projects:"]
    for name in registry.projects:
        marker = " (current)" if name == current else ""
        lines.append(f"- {name}{marker}")
    return "\n".join(lines)


def project_keyboard(registry: ProjectRegistry) -> dict[str, list[list[dict[str, str]]]]:
    rows: list[list[dict[str, str]]] = []
    for name in registry.projects:
        rows.append(
            [
                {
                    "text": name,
                    "callback_data": f"project:{name}",
                }
            ]
        )
    return {"inline_keyboard": rows}


def _load_project(raw: object) -> ProjectConfig:
    if not isinstance(raw, dict):
        raise RuntimeError("Project entries must be JSON objects.")

    name = str(raw.get("name") or "").strip()
    path = str(raw.get("path") or "").strip()
    if not name:
        raise RuntimeError("Project entry is missing 'name'.")
    if not path:
        raise RuntimeError(f"Project '{name}' is missing 'path'.")

    return ProjectConfig(
        name=name,
        path=path,
        repo=str(raw.get("repo") or "").strip(),
        branch=str(raw.get("branch") or "").strip(),
        deploy=str(raw.get("deploy") or "").strip(),
        description=str(raw.get("description") or "").strip(),
    )
