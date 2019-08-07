import configparser


def parse_config():
    """
    Parse the app.cfg configuration file
    :return:
    """
    config = configparser.ConfigParser()
    with open('app.cfg') as configfile:
        config.read_file(configfile)

        config_vars = {
            'timescaledb': {
                'host': config.get('TIMESCALEDB', 'HOST'),
                'db': config.get('TIMESCALEDB', 'DB_NAME'),
                'user': config.get('TIMESCALEDB', 'DB_USER'),
                'password': config.get('TIMESCALEDB', 'DB_PASSWORD'),
                'port': config.get('TIMESCALEDB', 'DB_PORT')
            },
            'spark': {
                'app_name': config.get('SPARK', 'APP_NAME')
            },
            'data': {
                'raw_path': config.get('DATA', 'RAW_PATH')
            },
            'streaming': {
                'processing_time': config.get('STREAMING', 'PROCESSING_TIME'),
                'checkpoint_location': config.get('STREAMING', 'CHECKPOINT_LOCATION'),
                'max_files_per_trigger': config.get('STREAMING', 'MAX_FILES_PER_TRIGGER')
            }
        }

        return config_vars
    