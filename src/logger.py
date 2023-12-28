class Logger:
    def __init__(self, log_file_name):
        self.log_file_name = log_file_name
        self.log_file = open(self.log_file_name, 'w')
        self.log_file.write('')

    def log(self, message):
        self.log_file.write(message + '\n')
        self.log_file.flush()

    def close(self):
        self.log_file.close()