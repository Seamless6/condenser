import config_reader
import psycopg2, mysql.connector
import os, pathlib, re, urllib, subprocess, os.path, json, getpass, time, sys, datetime
import sshtunnel
import paramiko

class DbConnect:

    def __init__(self, db_type, connection_info):
        requiredKeys = [
            'user_name',
            'host',
            'db_name',
            'port'
        ]

        for r in requiredKeys:
            if r not in connection_info.keys():
                raise Exception('Missing required key in database connection info: ' + r)
        if 'password' not in connection_info.keys():
            connection_info['password'] = getpass.getpass('Enter password for {0} on host {1}: '.format(connection_info['user_name'], connection_info['host']))

        self.user = connection_info['user_name']
        self.password = connection_info['password']
        self.host = connection_info['host']
        self.port = connection_info['port']
        self.db_name = connection_info['db_name']
        self.ssl_mode = connection_info['ssl_mode'] if 'ssl_mode' in connection_info else None
        self.__db_type = db_type.lower()
        
        # SSH tunnel configuration
        self.ssh_tunnel = None
        self.ssh_config = connection_info.get('ssh_tunnel', None)
        if self.ssh_config:
            if 'ssh_host' not in self.ssh_config:
                raise Exception('Missing required key in ssh_tunnel config: ssh_host')
            if 'ssh_username' not in self.ssh_config:
                raise Exception('Missing required key in ssh_tunnel config: ssh_username')
            # Only prompt for password if no authentication method is specified
            # (no password, no private key, and not using SSH agent)
            if ('ssh_password' not in self.ssh_config and 
                'ssh_private_key' not in self.ssh_config and 
                not self.ssh_config.get('use_ssh_agent', False)):
                self.ssh_config['ssh_password'] = getpass.getpass('Enter SSH password for {0}@{1}: '.format(
                    self.ssh_config['ssh_username'], self.ssh_config['ssh_host']))

    def get_db_connection(self, read_repeatable=False):
        # Start SSH tunnel if configured
        if self.ssh_config:
            if self.ssh_tunnel is None:
                # Get the remote database host to connect to through the tunnel
                # If remote_host is specified in ssh_tunnel config, use it
                # Otherwise, use the database host specified in the main connection config
                remote_host = self.ssh_config.get('remote_host', self.host)
                
                # Get the remote port for the database connection
                # If remote_port is specified in ssh_tunnel config, use it
                # Otherwise, use the port specified in the main connection config
                remote_port = self.ssh_config.get('remote_port', self.port)
                
                # Initialize the tunnel
                tunnel_kwargs = {
                    'ssh_address_or_host': (self.ssh_config['ssh_host'], self.ssh_config.get('ssh_port', 22)),
                    'ssh_username': self.ssh_config['ssh_username'],
                    'remote_bind_address': (remote_host, remote_port),
                    'local_bind_address': ('127.0.0.1', self.ssh_config.get('local_port', 0))  # 0 = random free port
                }
                
                # Add authentication method (password, key, or SSH agent)
                using_ssh_agent = self.ssh_config.get('use_ssh_agent', False)
                
                if using_ssh_agent:
                    # When using SSH agent, explicitly avoid loading any key files
                    tunnel_kwargs['allow_agent'] = True
                    # Get keys from SSH agent without the tuple-creating comma
                    keys = paramiko.agent.Agent().get_keys()
                    if keys:
                        tunnel_kwargs['ssh_pkey'] = keys[0]  # Use first key from agent
                        print(f"Using SSH agent key for authentication to {self.ssh_config['ssh_host']}")
                    else:
                        print(f"Warning: No keys found in SSH agent. Falling back to other authentication methods.")
                        tunnel_kwargs['look_for_keys'] = True
                elif 'ssh_password' in self.ssh_config:
                    tunnel_kwargs['ssh_password'] = self.ssh_config['ssh_password']
                    # Don't use agent when password is explicitly provided
                    tunnel_kwargs['allow_agent'] = False
                    tunnel_kwargs['look_for_keys'] = False
                elif 'ssh_private_key' in self.ssh_config:
                    tunnel_kwargs['ssh_pkey'] = self.ssh_config['ssh_private_key']
                    if 'ssh_private_key_password' in self.ssh_config:
                        tunnel_kwargs['ssh_private_key_password'] = self.ssh_config['ssh_private_key_password']
                    # Don't use agent when key is explicitly provided
                    tunnel_kwargs['allow_agent'] = False
                
                try:
                    # Start the tunnel
                    self.ssh_tunnel = sshtunnel.SSHTunnelForwarder(**tunnel_kwargs)
                    self.ssh_tunnel.start()
                    
                    # Override host and port with tunnel's local endpoint
                    self.tunnel_host = '127.0.0.1'  # Use 127.0.0.1 instead of localhost to avoid IPv6 issues
                    self.tunnel_port = self.ssh_tunnel.local_bind_port
                    print(f"SSH tunnel established: {self.tunnel_host}:{self.tunnel_port} -> {remote_host}:{remote_port}")
                except Exception as e:
                    print(f"Error establishing SSH tunnel: {str(e)}")
                    raise
            else:
                # Tunnel already established
                self.tunnel_host = '127.0.0.1'
                self.tunnel_port = self.ssh_tunnel.local_bind_port
        else:
            # No tunnel, use direct connection
            self.tunnel_host = self.host
            self.tunnel_port = self.port

        if self.__db_type == 'postgres':
            return PsqlConnection(self, read_repeatable)
        elif self.__db_type == 'mysql':
            return MySqlConnection(self, read_repeatable)
        else:
            raise ValueError('unknown db_type ' + self.__db_type)
    
    def close_tunnel(self):
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.close()
            self.ssh_tunnel = None
            print("SSH tunnel closed")

class DbConnection:
    def __init__(self, connection):
        self.connection = connection

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


class LoggingCursor:
    def __init__(self, cursor):
        self.inner_cursor = cursor

    def execute(self, query):
        start_time = time.time()
        if config_reader.verbose_logging():
            print('Beginning query @ {}:\n\t{}'.format(str(datetime.datetime.now()), query))
            sys.stdout.flush()
        retval = self.inner_cursor.execute(query)
        if config_reader.verbose_logging():
            print('\tQuery completed in {}s'.format(time.time() - start_time))
            sys.stdout.flush()
        return retval

    def __getattr__(self, name):
        return self.inner_cursor.__getattribute__(name)

    def __exit__(self, a, b, c):
        return self.inner_cursor.__exit__(a, b, c)

    def __enter__(self):
        return LoggingCursor(self.inner_cursor.__enter__())

# small wrapper to the connection class that gives us a common interface to the cursor()
# method across MySQL and Postgres. This one is for Postgres
class PsqlConnection(DbConnection):
    def __init__(self,  connect, read_repeatable):
        connection_string = 'dbname=\'{0}\' user=\'{1}\' password=\'{2}\' host={3} port={4}'.format(connect.db_name, connect.user, connect.password, connect.tunnel_host, connect.tunnel_port)

        if connect.ssl_mode :
            connection_string = connection_string + ' sslmode={0}'.format(connect.ssl_mode)

        DbConnection.__init__(self, psycopg2.connect(connection_string))
        if read_repeatable:
            self.connection.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ

    def cursor(self, name=None, withhold=False):
        return LoggingCursor(self.connection.cursor(name=name, withhold=withhold))


# small wrapper to the connection class that gives us a common interface to the cursor()
# method across MySQL and Postgres. This one is for MySQL
class MySqlConnection(DbConnection):
    def __init__(self,  connect, read_repeatable):
        DbConnection.__init__(self, mysql.connector.connect(host=connect.tunnel_host, port=connect.tunnel_port, user=connect.user, password=connect.password, database=connect.db_name))

        self.db_name = connect.db_name

        if read_repeatable:
            self.connection.start_transaction(isolation_level='REPEATABLE READ')

    def cursor(self, name=None, withhold=False):
        return LoggingCursor(self.connection.cursor())
