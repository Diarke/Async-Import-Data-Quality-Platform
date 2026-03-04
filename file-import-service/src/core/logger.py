import logging


logger = logging.getLogger('ExtendedLogger')
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('extended.log', 'a')
file_handler.setLevel(logging.ERROR)

logger.error('Обнаружена ошибка? Выделите её в отдельный лог.')

log_format = logging.Formatter('%(asctime)s – %(name)s – %(levelname)s – %(message)s', datefmt='%H:%M:%S')
file_handler.setFormatter(log_format)

logger.addHandler(file_handler)
