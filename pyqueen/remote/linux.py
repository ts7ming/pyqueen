class Linux:
    def __init__(self, host, port, username, password):
        self.__host = host
        self.__port = port
        self.__username = username
        self.__password = password
        self.__ssh = None
        self.__keep_conn = False

    def __get_conn(self):
        import paramiko
        if self.__ssh is None:
            self.__ssh = paramiko.SSHClient()
            self.__ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.__ssh.connect(hostname=self.__host, port=self.__port, username=self.__username, password=self.__password)

    def keep_conn(self):
        self.__keep_conn = True

    def close_conn(self):
        try:
            self.__ssh.close()
            self.__ssh = None
            self.__keep_conn = False
        except:
            pass

    def exec(self, command):
        import paramiko
        try:
            self.__get_conn()
            stdin, stdout, stderr = self.__ssh.exec_command(command)
            output = stdout.read().decode()
            error = stderr.read().decode()
            if self.__keep_conn is False:
                self.close_conn()
            return output, error
        except paramiko.AuthenticationException:
            print("Authentication failed, please verify your credentials")
        except paramiko.SSHException as sshException:
            print(f"Unable to establish SSH connection: {sshException}")
        except paramiko.BadHostKeyException as badHostKeyException:
            print(f"Unable to verify server's host key: {badHostKeyException}")
        except Exception as e:
            print(f"An error occurred: {e}")
