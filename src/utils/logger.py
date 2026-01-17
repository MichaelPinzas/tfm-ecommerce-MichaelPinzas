"""
Sistema de logging centralizado para el proyecto.
"""

import logging
import sys
from pathlib import Path


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configura y retorna un logger.

    Args:
        name: Nombre del logger (generalmente __name__ del módulo)
        level: Nivel de logging (INFO, DEBUG, WARNING, ERROR)

    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Evitar duplicar handlers si ya existe
    if logger.handlers:
        return logger

    # Formato de los mensajes
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger