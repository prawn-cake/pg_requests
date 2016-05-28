# -*- coding: utf-8 -*-


class PgQueryException(Exception):
    """ Base exception library class"""

    def __str__(self):
        return str(self.message)


class TokenError(PgQueryException):
    """Raised when token is failed"""
    pass