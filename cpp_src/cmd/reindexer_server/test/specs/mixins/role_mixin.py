from base64 import b64encode


class RoleMixin(object):
    ROLE_DEFAULT = 'owner'

    def role_token(self, role):
        token = ''
        if role == 'data_read':
            token = self._role_data_read_token()
        if role == 'data_write':
            token = self._role_data_write_token()
        if role == 'db_admin':
            token = self._role_db_admin_token()
        if role == 'owner':
            token = self._role_owner_token()
        return token

    def _role_data_read_token(self):
        return self._role_token_by_user('user_data_read')

    def _role_data_write_token(self):
        return self._role_token_by_user('user_data_write')

    def _role_db_admin_token(self):
        return self._role_token_by_user('user_db_admin')

    def _role_owner_token(self):
        return self._role_token_by_user('user_owner')

    def _role_token_by_user(self, user):
        passw = self.USERS[user]['hash']

        auth_data = "{user}:{passw}".format(
            user=user, passw=passw)

        token_bytes = b64encode(auth_data.encode())
        token = token_bytes.decode()

        return token
