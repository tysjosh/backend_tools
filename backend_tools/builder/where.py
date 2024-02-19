import re
from datetime import datetime
from typing import Dict, Tuple, Union, List, Callable, Optional

ValueType = Union[str, int, float, datetime, Tuple, None]

OP_MAP = {
    'IN': 'in',
    'NOT IN': 'notin',
    '=': 'eq',
    '!=': 'neq',
    '>': 'gt',
    '<': 'lt',
    '>=': 'gte',
    '<=': 'lte',
    'LIKE': 'like',
    'ILIKE': 'ilike',
    '@>': 'contains',
    '<@': 'contained_by',
    '&&': 'overlaps',
}

EXPR_OPS = set(OP_MAP.keys()) | {'AND', 'OR', 'empty', 'emptytuple', 'raw', 'unary'}

RE_LIKE_ESCAPE = re.compile(r"([\\%_()?|\"\'])")

TABLE_ALIAS = Union[str, List[str], Dict[str, Dict[str, str]]]


def escape_like(arg):
    return re.sub(RE_LIKE_ESCAPE, lambda x: '\\' + x.group(1), arg)


class Expr(object):

    def __init__(
            self, sql: str = '', params: Dict = None, op: str = 'raw',
            empty: bool = False, emptytuple: bool = False):
        # TODO::COMPATIBILITY Remove empty/emptytuple after updating all related code
        if empty:
            op = 'empty'
        if emptytuple:
            op = 'emptytuple'

        assert op in EXPR_OPS, "Expr._op should be one of EXPR_OPS"
        self._op = op
        self._sql = sql
        self._param_name_prefix = 'autogen'
        self.__params = params or {}

    @property
    def empty(self) -> bool:
        return self._op == 'empty'

    @property
    def emptytuple(self) -> bool:
        return self._op == 'emptytuple'

    @property
    def params(self) -> Dict:
        return self.__params

    def __str__(self) -> str:
        return self._sql

    __repr__ = __str__

    def _generate_param_expr(self, op: str, other: ValueType) -> 'Expr':
        """Wrap right part of condition to param expr with unique name"""
        param_name = '{name}-{op}-{idx}'.format(name=self._param_name_prefix, op=OP_MAP[op], idx=Param.n_params + 1)
        return Param(param_name, other)

    def _prepare_other(self, op: str, other: Union[ValueType, 'Expr']) -> 'Expr':
        """Wrap any operand into Expr object"""
        if other is None:
            return Expr('', op='empty')
        if isinstance(other, tuple) and not other:
            return Expr('', op='emptytuple')
        if isinstance(other, Expr):
            return other
        return self._generate_param_expr(op, other)

    def __prepare_iterable(self, op, *others) -> 'Expr':
        """Wrap iterable arg (or multiple args) into Expr object"""
        item_count = len(others)
        other = Expr('', op='empty')

        if item_count == 0 or (item_count == 1 and others[0] is None):
            return other

        if item_count == 1:
            # it may be Expr object (subquery) returned by `q()` or iterable
            return Expr('({})'.format(others[0]), op=op) if isinstance(others[0], Expr) \
                else self._prepare_other(op, tuple(others[0]))
        return self._prepare_other(op, others)

    def __compose_parts(self, op: str, other: 'Expr') -> 'Expr':
        """Compose WHERE-clause expression parts using either OR or AND operator"""
        if self.empty and other.empty:
            return Expr('', op='empty')
        if self.empty or other.empty:
            replace_expr = Expr('TRUE' if op == 'AND' else 'FALSE', op=op)
            if op == 'AND':
                result_expr = replace_expr & other if self.empty else self & replace_expr
            else:
                result_expr = replace_expr | other if self.empty else self | replace_expr
            sql = str(result_expr)
        else:
            # And or or have the smallest priority compared to any other operators 
            # Therefore, brackets next to them are needed only if combined and or or, 
            # and also in the case of the transmission of a raw request
            left_par = '({})' if self._op == 'raw' or {self._op, op} == {'AND', 'OR'} else '{}'
            right_par = '({})' if other._op == 'raw' or {other._op, op} == {'AND', 'OR'} else '{}'
            sql = '{} {} {}'.format(left_par, '{}', right_par).format(self, op, other)
        return Expr(sql, params={**self.params, **other.params}, op=op)

    def __compose_in(self, op: str, *others) -> 'Expr':
        """Compose expression with IN (NOT IN) operator"""
        other = self.__prepare_iterable(op, *others)
        if other.empty:
            return Expr('', op='empty')
        # see test case comments
        if other.emptytuple:
            return Expr('FALSE') if op == 'IN' else self.notnull()
        return Expr('{} {} {}'.format(self, op, other), params=other.params, op=op)

    def __compose_cmp(self, op: str, other: Union[ValueType, 'Expr']) -> 'Expr':
        """Compose comparison expression with =, !=, >, <, >=, <= operators"""
        other = self._prepare_other(op, other)
        if other.empty:
            return Expr('', op='empty')
        return Expr('{} {} {}'.format(self, op, other), params=other.params, op=op)

    def __compose_any(self, op: str, other: Union[ValueType, 'Expr']) -> 'Expr':
        """Compose comparison expression with =, !=  operators FOR Postgres array fields"""
        expr = []
        if isinstance(other, list):
            expr.extend([self._prepare_other(op, i) for i in other])
            query = ' OR '.join(['{} {} ANY({})'.format(i, op, self) for i in expr])
            params = {}
            for i in expr:
                params.update(i.params)
            return Expr(query, params=params, op=op)

        other = self._prepare_other(op, other)
        if other.empty:
            return Expr('', op='empty')
        return Expr('{} {} ANY({})'.format(other, op, self), params=other.params, op=op)

    def isnull(self) -> 'Expr':
        return Expr('{} IS NULL'.format(self))

    def notnull(self) -> 'Expr':
        return Expr('{} IS NOT NULL'.format(self))

    def nottrue(self) -> 'Expr':
        return Expr('{} IS NOT TRUE'.format(self))

    def __anyof(self, other: str):
        return self.__compose_any('=', other)

    def __and__(self, other: 'Expr') -> 'Expr':
        return self.__compose_parts('AND', other)

    def anyof(self, field) -> 'Expr':
        return self.__anyof(field)

    def __or__(self, other: 'Expr') -> 'Expr':
        return self.__compose_parts('OR', other)

    def isin(self, *others) -> 'Expr':
        return self.__compose_in('IN', *others)

    def notin(self, *others) -> 'Expr':
        return self.__compose_in('NOT IN', *others)

    def __eq__(self, other: Union[ValueType, 'Expr']) -> 'Expr':
        return self.__compose_cmp('=', other)

    def __ne__(self, other: Union[ValueType, 'Expr']) -> 'Expr':
        return self.__compose_cmp('!=', other)

    def __gt__(self, other: Union[ValueType, 'Expr']) -> 'Expr':
        return self.__compose_cmp('>', other)

    def __ge__(self, other: Union[ValueType, 'Expr']) -> 'Expr':
        return self.__compose_cmp('>=', other)

    def __lt__(self, other: Union[ValueType, 'Expr']) -> 'Expr':
        return self.__compose_cmp('<', other)

    def __le__(self, other: Union[ValueType, 'Expr']) -> 'Expr':
        return self.__compose_cmp('<=', other)

    def __invert__(self):
        return Expr('(NOT {})'.format(self), params=self.params, op='unary')

    def between(self, from_: Union[ValueType, 'Expr'], to: Union[ValueType, 'Expr']) -> 'Expr':
        return (self >= from_) & (self <= to)

    def like(self, other):
        return self.__compose_cmp('LIKE', other)

    def ilike(self, other):
        return self.__compose_cmp('ILIKE', other)

    def exists(self):
        return Expr('(EXISTS ({}))'.format(self), params=self.params, op='unary')

    def contains(self, other):
        return self.__compose_cmp('@>', other)

    def contained_by(self, other):
        return self.__compose_cmp('<@', other)

    def overlaps(self, other):
        # have elements in common (intersection is not empty)
        return self.__compose_cmp('&&', other)


class Param(Expr):
    n_params = 0

    def __init__(self, name: str, value: ValueType):
        super().__init__(sql='%({})s'.format(name), params={name: value}, op='unary')
        Param.n_params += 1


class ExprExt(Expr):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__param_transformer = None

    def __getitem__(self, key: int) -> 'ExprExt':
        invalid_key_type_msg = 'Invalid key type, expected <int> got {}'
        invalid_key_value_msg = 'Invalid key value, expected key >= 1 got {}'
        if not isinstance(key, int):
            raise TypeError(invalid_key_type_msg.format(type(key)))
        if key <= 0:
            raise TypeError(invalid_key_value_msg.format(key))
        return ExprExt("{}[{}]".format(self, key))

    def _prepare_other(self, op: str, other: Union[ValueType, Expr]) -> Expr:
        if self.__param_transformer is not None and not isinstance(other, Expr):
            other = self.__param_transformer(other)
            self.__param_transformer = None
        return super()._prepare_other(op, other)

    def cast(self, type_: str) -> 'ExprExt':
        return ExprExt("{}::{}".format(self, type_))

    @property
    def timestamp(self) -> 'ExprExt':
        self.__param_transformer = lambda p: datetime.utcfromtimestamp(int(p)) if isinstance(p, (int, float)) else p
        return self

    def between(self, from_: Union[ValueType, 'Expr'], to: Union[ValueType, 'Expr']) -> 'Expr':
        if self.__param_transformer:
            from_ = self.__param_transformer(from_)
            to = self.__param_transformer(to)
            self.__param_transformer = None
        return super().between(from_, to)


class Field(ExprExt):
    def __init__(self, name, table_alias: str = None, field_alias: str = None):
        name = name if field_alias is None else field_alias
        super().__init__(sql=name, op='unary')
        self._param_name_prefix = self.name
        self.__table_alias = table_alias

    def __str__(self):
        return '{t}{name}'.format(t='' if not self.__table_alias else self.__table_alias + '.', name=self.name)

    @property
    def name(self):
        return self._sql


class T(object):

    def __init__(self, alias: str = None, namespace: Dict = None, fieldmap: Dict = None):
        self.__alias = alias
        self.__namespace = namespace or {}
        self.fieldmap = fieldmap or {}

    def __getattr__(self, item: str) -> Field:
        f = Field(item, table_alias=self.__alias, field_alias=self.fieldmap.get(item))
        setattr(self, item, f)
        return f

    def __getitem__(self, item: str) -> ExprExt:
        return ExprExt(item.format(**self.__namespace))


def field_mapper(expr_func: Callable, fieldmap: Dict) -> Callable:
    """
    Create where-lambda with fieldnames replaced according to fieldmap dict
    ! Attention - maps fields for all tables in query.
    ! Use "where" with alias = Dict if you need separate mapping for multiple tables

    :param expr_func: source where-lambda
    :param fieldmap: mapping from source fieldnames to wanted fieldnames
    :return: where-lambda which generates Expr with replaced fieldnames

    Example:

        >>> some_lambda = lambda t: t.tx_hash == '123'
        >>> new_lambda = field_mapper(some_lambda, {'tx_hash': 'hash'})
        >>> where(new_lambda)
        >>> ('hash = %(hash-eq-1)s', {'hash-eq-1': '123'})
    """

    def expr_func_env(*tables: Tuple[T]):
        for t in tables:
            t.fieldmap = fieldmap
        return expr_func(*tables)

    return expr_func_env


def _prepare_t(alias: TABLE_ALIAS = '') -> Tuple[T]:
    if isinstance(alias, str):
        namespace = {'t': alias}
        alias = [alias]
    else:
        namespace = {'t{}'.format(i + 1): key for i, key in enumerate(alias)}

    if isinstance(alias, dict):
        return tuple(T(alias=t, namespace=namespace, fieldmap=fields) for t, fields in alias.items())
    else:
        return tuple(T(alias=t, namespace=namespace) for t in alias)


def where(expr_func: Callable = None, alias: TABLE_ALIAS = None) -> Tuple[str, Dict]:
    """
    Build WHERE expression.
    Return tuple of SQL WHERE-clause and query params.

    :param expr_func:
        Function used to build where expression. Examples:

        sql, params = where(lambda t: t.fieldname == value)
        sql, params = where(lambda t: t.time.timestamp.between(123456789, 987654321))
        sql, params = where(
            lambda t1, t2: (t1.value > 5) & ((t2.name == 'ivan') | (t2.name == 'andrey')), alias=['t', 'ta'])
        sql, params = where(lambda t: t.user_id.isin(q("SELECT user_id FROM user WHERE role = 'dev'"))
        sql, params = where(lambda t1, t2: t["(CASE WHEN {t2}.state = 'ok' THEN {t1}.field1 ELSE {t1}.field2 END)"] > 1)

    :param alias:
        Aliases for tables and fields. Could be
        str - alias for one table
        list - alias for more than one tables
        dict - {
            key - alias for table (order matters)
            values - dict {
                key - field name (order doesn't matter)
                value - field alias
            }
    :return:
        (<str> SQL WHERE-clause, <dict> query params)
    """
    true = ('TRUE', {})
    if expr_func is None:
        return true

    tables = _prepare_t(alias or '')
    expr = expr_func(*tables)
    return true if expr.empty else (str(expr), expr.params)


def q(sql: str, params: Dict = None) -> Expr:
    """
    Wrap SQL query or any expression to Expr object.
    Useful for subqueries:
        sql, params = where(lambda t: t.user_id.isin(q("SELECT user_id FROM user WHERE role = 'dev'"))
    """
    return Expr(sql, params=params)


def dnf(expr_func: Optional[Callable], on_empty=True) -> Callable:
    """
    Generate expression which represents disjunctive normal form

    :param expr_func:
        function which returns list of tuples.
        each tuple represent one item of DNF
    :param on_empty: <bool> or <Expr>

    :return: function, which could be passed to `where` (where-lambda)

    Example:
        dnf(lambda t: [(t.field1 == 1, t.field2.isin([2, 3]), (t.field1 == 2, t.field2.isin([3, 4]))])
        returns following Expr:
        (field1 = 1 AND field2 IN (2, 3)) OR (field1 = 2 AND field2 IN (3, 4))
    """
    on_empty = q('TRUE') if on_empty is True or on_empty is None else q('FALSE') if on_empty is False else on_empty
    if expr_func is None:
        return lambda *tt: on_empty

    def dnf_check_fn_result(monomial_seq):
        items_type_err_msg = 'Result of expr func should have <list> or <tuple>, got {}'
        monomial_type_err_msg = 'Items of resulting sequence should be <list> or <tuple>, got {}'
        monomial_item_type_err_msg = 'Elements of each item of resulting sequence should be <Expr>, got {}'

        if not isinstance(monomial_seq, (tuple, list)):
            raise TypeError(items_type_err_msg.format(type(monomial_seq)))

        for item in monomial_seq:
            if not isinstance(item, (tuple, list)):
                raise TypeError(monomial_type_err_msg.format(type(item)))
            for expr in item:
                if not isinstance(expr, Expr):
                    raise TypeError(monomial_item_type_err_msg.format(type(expr)))

    def dnf_generate(*tables: Tuple[T]) -> Expr:
        items = expr_func(*tables)
        if not items:
            return on_empty

        # check typing
        dnf_check_fn_result(items)

        # build conjunctive monomial list of DNF
        # построим список конъюнктивных одночленов ДНФ
        monomials = []
        for item in items:
            if not item:
                continue
            expr = item[0]
            for expr_ in item[1:]:
                expr &= expr_
            monomials.append(expr)

        if not monomials:
            return on_empty

        # build DNF from monomial list
        expr = monomials[0]
        for expr_ in monomials[1:]:
            expr |= expr_

        return expr

    return dnf_generate


def _merge_wheres(left_where: Optional[callable], right_where: Optional[callable], _and=True) -> Optional[callable]:
    if left_where and right_where:
        if _and:
            return lambda t: left_where(t) & right_where(t)
        else:
            return lambda t: left_where(t) | right_where(t)
    elif left_where:
        return left_where
    elif right_where:
        return right_where
    return None


def and_wheres(left_where: Optional[callable], right_where: Optional[callable]) -> Optional[callable]:
    return _merge_wheres(left_where, right_where)


def or_wheres(left_where: Optional[callable], right_where: Optional[callable]) -> Optional[callable]:
    return _merge_wheres(left_where, right_where, _and=False)


def create_nullable_where_in(tab) -> callable:
    """
    Returns an analogue `(Field1, Field2) in ((Value1, Value2), (Value3, Value4))` for nullable fields 
    the names of the fields are taken from the columns of the tabular. Values - from lines 
    will return FALSE if you transfer an empty tabular 
     : Param Tab: Tabular 
     : Return: Callable
    """
    def one_col(col_name, value):
        return lambda t: (getattr(t, col_name) == value if value is not None else getattr(t, col_name).isnull())

    def one_row(row):
        def row_lambda(t):
            result = q('TRUE')
            for col in row.cols:
                result = result & (one_col(col, getattr(row, col)))(t)
            return result

        return row_lambda

    def where_lambda(t):
        result = q('FALSE')
        for row in tab:
            result = result | one_row(row)(t)
        return result

    return where_lambda
