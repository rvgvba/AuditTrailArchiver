import logging


class CustomLogger:
    def __init__(self, log_file_name, log_type, is_streamer=None):
        self.__log_file_name = log_file_name
        self.__log_type = log_type
        self.__is_streamer = is_streamer

        self.__logger_engine = logging.getLogger(self.__loger_name)
        self.__logger_engine.setLevel(self.__manage_type())
        self.__create_log_file()
        self.__check_if_streamer()

    def __manage_type(self):
        """
        set the type of the logger at the runtime
        """
        log_types = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'critical': logging.CRITICAL
        }
        return log_types.get(self.__log_type, log_types['debug'])

    @property
    def __loger_name(self):
        return '{}_{}'.format(self.__log_file_name, self.__log_type)

    @property
    def __log_file_format(self):
        return '{0}.log'.format(self.__log_file_name)

    def __create_log_file(self):
        """
        creates the logger file
        sets the format for the logged messages
        """
        f_handler = logging.FileHandler(self.__log_file_format)
        log_formatter = CustomLogger.__log_format_str()
        f_handler.setFormatter(log_formatter)
        self.__logger_engine.addHandler(f_handler)

    def __check_if_streamer(self):
        """
        checks if user want to log data to console as well
        """
        if self.__is_streamer:
            log_Streamer = logging.StreamHandler()
            self.__logger_engine.addHandler(log_Streamer)

    def log_message(self, log_msg):
        """
        log data considering the type of the logger
        """
        if self.__log_type == 'info':
            self.__logger_engine.info(log_msg)
        if self.__log_type == 'debug':
            self.__logger_engine.debug(log_msg)
        if self.__log_type == 'warning':
            self.__logger_engine.warning(log_msg)
        if self.__log_type == 'critical':
            self.__logger_engine.critical(log_msg)


    @staticmethod
    def __log_format_str() -> logging.Formatter:
        return logging.Formatter('%(asctime)s -- %(name)s -- %(message)s')