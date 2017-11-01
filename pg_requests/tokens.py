# -*- coding: utf-8 -*-
import abc
import re
from pg_requests.exceptions import TokenError
from pg_requests.operators import ConditionOperator, And, QueryOperator, Evaluable


class TokenValue(Evaluable):
    def __init__(self, value):
        self.value = self.validate(value)

    @abc.abstractmethod
    def eval(self):
        """Evaluate value depends on the type of it
        For example:
            IterableValue --> ','.join(self.value)

        """
        pass

    def validate(self, value):
        """Validation method. Override it if needed.
        It must return validated value
        """
        return value

    def update(self, value):
        """Update method. By default just replaces old value with a given one

        :param value:
        """
        self.value = self.validate(value)

    def __repr__(self):
        return "%s(value=%s)" % (self.__class__.__name__, self.value)


class Token(Evaluable):
    """Query token representation"""

    TEMPLATE_RE = re.compile(r'\{.*\}')

    def __init__(self, template, value_type, required=False, subtoken=None):
        """

        :param template: str: token template
        :param value: any token value
        """
        self._value = None
        self.value_type = self._validate_value_type(value_type)
        self.template = self._validate_template(template, self.value_type)
        self.is_set = False
        self.required = required
        self.subtoken = self._validate_subtoken(subtoken)

    @classmethod
    def _validate_template(cls, template, value_type):
        """Validate template value

        :param template: str
        :param value_type: TokenValue subclass
        :return: :raise ValueError:
        """
        # Exclude null value checks
        if issubclass(value_type, NullValue):
            return template
        elif not cls.TEMPLATE_RE.search(template):
            raise ValueError("Template must contain '{}'")
        return template

    @classmethod
    def _validate_value_type(cls, value_type):
        if not issubclass(value_type, TokenValue):
            raise ValueError(
                "Wrong value_type class '%s', must be subclass of '%s'" % (
                    value_type.__name__, TokenValue.__name__))
        return value_type

    @classmethod
    def _validate_subtoken(cls, subtoken):
        if subtoken and not isinstance(subtoken, Token):
            raise ValueError("Wrong sub-token instance '%r', must be '%r'" %
                             (subtoken, Token.__name__))
        return subtoken

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        # Transform value to value_type instance
        self._value = self.value_type(value=val)
        self.is_set = True

    def eval(self):
        """Evaluate current_token value with template and value

        :return: str | tuple
        """
        if self.required and not self.is_set:
            raise TokenError("Token %s is not set and required" % self)

        val = self.value.eval()
        if isinstance(val, tuple):
            if len(val) == 2:
                subst, values = val[0], val[1]
            else:
                raise TokenError(
                    "Unexpected tuple value length %d, must me 2: %s" %
                    (len(val), val))
        else:
            subst, values = val, ()

        if isinstance(subst, (list, tuple)):
            # Can come from TupleValue
            template_str = self.template.format(*subst)
        elif isinstance(subst, dict):
            # Can come from DictValue
            template_str = self.template.format(**subst)
        else:
            template_str = self.template.format(subst)

        # Evaluate sub-tokens
        current_token = self
        while current_token.subtoken is not None:
            eval_result = current_token.subtoken.eval()
            if isinstance(eval_result, str):
                template_str = ''.join([template_str, eval_result])
            elif isinstance(eval_result, tuple):
                template_str = ''.join([template_str, eval_result[0]])
                values += eval_result[1]
            current_token = current_token.subtoken

        if values:
            return template_str, values
        else:
            return template_str

    def reset(self):
        """Reset token value and state"""
        self._value = None
        self.is_set = False

    def __repr__(self):
        return '%s(template=%s, value=%s)' % (
            self.__class__.__name__, self.template, self._value)


class StringValue(TokenValue):
    """ Simple string token value. It is appropriate for table_name or
    for options which don't require any parameters to substitute
    """
    def eval(self):
        return str(self.value)


class NullValue(TokenValue):
    """ Null (None) token value"""

    def eval(self):
        return None


class CommaValue(TokenValue):
    """Comma-separated value is the value which can be evaluated with simple
    ','.join() operation"""

    @classmethod
    def validate(cls, value):
        if not isinstance(value, (list, tuple)):
            raise ValueError("Wrong value type for '%s' instance, must be list"
                             "or tuple" % cls.__name__)
        return value

    def eval(self):
        return ', '.join(self.value)


class TupleValue(TokenValue):
    """Useful for InsertQuery builder VALUES clause when we just need to form
    string template with tuple substitution values. The output is represented
    as a tuple."""

    @classmethod
    def validate(cls, value):
        if not isinstance(value, (list, tuple)):
            raise ValueError("Wrong value type for '%s' instance, must be list"
                             "or tuple" % cls.__name__)
        return value

    def eval(self):
        """Evaluate tuple value

        :rtype : tuple
        :return: tuple of str substitution template + actual values tuple

        Example: ('%s, %s', ('a', 'b')
        """
        n = len(self.value)
        return tuple([', '.join(['%s'] * n), tuple(self.value)])


class DictValue(TokenValue):
    """Dict value"""

    @classmethod
    def validate(cls, value):
        if not isinstance(value, dict):
            raise ValueError("Wrong value type for '%s' instance, "
                             "must be dict" % (cls.__name__, ))
        return value

    def eval(self):
        return self.value


class CommaDictValue(TokenValue):
    """Substitution key value token value
    Use case: dict(a=1, b=2) --> "a=%s, b=%s", (1, 2)
    """

    def eval(self):
        """Evaluate dict as a string and values tuple

        :return: tuple
        """
        keys, values = ConditionOperator.parse_conditions(self.value)
        return tuple([', '.join(keys), tuple(values)])

    validate = DictValue.validate


class FilterValue(TokenValue):
    """Complex value type is user in WHERE clause"""

    @classmethod
    def validate(cls, value):
        """Validate conditional value

        :rtype : ConditionOperator
        """
        if not isinstance(value, (ConditionOperator, dict, QueryOperator)):
            raise ValueError(
                "Wrong value type for '%s' instance, must be %s,"
                "dict or %s" % (cls.__name__, ConditionOperator.__name__,
                                QueryOperator.__name__))
        if isinstance(value, dict):
            value = And(value)
        elif isinstance(value, QueryOperator):
            # unpack Q operator, extract condition
            value = value.condition

        return value

    def eval(self):
        """Evaluate conditional value

        :rtype : tuple
        :return: 'name = %s AND visits >= %s', ('Username', 1)
        """
        sql_str, values = self.value.eval()
        return sql_str, values

    def update(self, value):
        """Update conditional value with And operator.
        This is used by .filter() operation, so if

        :param value:
        """
        validated = self.validate(value)
        self.value = And(self.value, validated)
