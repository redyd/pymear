import logging


class VerboseLogger:
    def __init__(self, name: str, verbose: bool = False) -> None:
        self._logger = logging.getLogger(name)
        self.verbose = verbose

    def info(self, message: str, *args) -> None:
        self._logger.info(message, *args)

    def warning(self, message: str, *args) -> None:
        self._logger.warning(message, *args)

    def error(self, message: str, *args) -> None:
        self._logger.error(message, *args)

    def info_v(self, message: str, *args) -> None:
        self._logger.info(message, *args) if self.verbose else self._logger.debug(message)

    def warning_v(self, message: str, *args) -> None:
        self._logger.warning(message, *args) if self.verbose else self._logger.debug(message)

    def error_v(self, message: str, *args) -> None:
        self._logger.error(message, *args) if self.verbose else self._logger.debug(message)
