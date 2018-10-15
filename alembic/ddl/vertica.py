import re

from ..util import compat
from .. import util
from .base import compiles, alter_column, alter_table, format_table_name, \
    format_type, AlterColumn, RenameTable
from .impl import DefaultImpl
from sqlalchemy.dialects.postgresql import INTEGER, BIGINT
from ..autogenerate import render
from sqlalchemy import text, Numeric, Column
from sqlalchemy.sql.expression import ColumnClause
from sqlalchemy.types import NULLTYPE
from sqlalchemy import types as sqltypes

from ..operations.base import Operations
from ..operations.base import BatchOperations
from ..operations import ops
from ..util import sqla_compat
from ..operations import schemaobj

import logging

from sqlalchemy.sql.expression import UnaryExpression
from sqlalchemy.schema import CreateColumn
from sqlalchemy.ext.compiler import compiles

@compiles(CreateColumn, 'vertica')
def use_identity(element, compiler, **kw):
    text = compiler.visit_create_column(element, **kw)
    text = text.replace("SERIAL", "IDENTITY")
    return text

log = logging.getLogger(__name__)


class VerticaImpl(DefaultImpl):
    __dialect__ = 'vertica'
    transactional_ddl = False

    def prep_table_for_batch(self, table):
        for constraint in table.constraints:
            if constraint.name is not None:
                self.drop_constraint(constraint)

    def compare_server_default(self, inspector_column,
                               metadata_column,
                               rendered_metadata_default,
                               rendered_inspector_default):
        # don't do defaults for SERIAL columns
        if metadata_column.primary_key and \
                metadata_column is metadata_column.table._autoincrement_column:
            return False

        conn_col_default = rendered_inspector_default

        defaults_equal = conn_col_default == rendered_metadata_default
        if defaults_equal:
            return False

        if None in (conn_col_default, rendered_metadata_default):
            return not defaults_equal

        if metadata_column.server_default is not None and \
            isinstance(metadata_column.server_default.arg,
                       compat.string_types) and \
                not re.match(r"^'.+'$", rendered_metadata_default) and \
                not isinstance(inspector_column.type, Numeric):
                # don't single quote if the column type is float/numeric,
                # otherwise a comparison such as SELECT 5 = '5.0' will fail
            rendered_metadata_default = re.sub(
                r"^u?'?|'?$", "'", rendered_metadata_default)

        return not self.connection.scalar(
            "SELECT %s = %s" % (
                conn_col_default,
                rendered_metadata_default
            )
        )

    def alter_column(self, table_name, column_name,
                     nullable=None,
                     server_default=False,
                     name=None,
                     type_=None,
                     schema=None,
                     autoincrement=None,
                     existing_type=None,
                     existing_server_default=None,
                     existing_nullable=None,
                     existing_autoincrement=None,
                     **kw
                     ):

        using = kw.pop('vertica_using', None)

        if using is not None and type_ is None:
            raise util.CommandError(
                "vertica_using must be used with the type_ parameter")

        if type_ is not None:
            self._exec(VerticaColumnType(
                table_name, column_name, type_, schema=schema,
                using=using, existing_type=existing_type,
                existing_server_default=existing_server_default,
                existing_nullable=existing_nullable,
            ))

        super(VerticaImpl, self).alter_column(
            table_name, column_name,
            nullable=nullable,
            server_default=server_default,
            name=name,
            schema=schema,
            autoincrement=autoincrement,
            existing_type=existing_type,
            existing_server_default=existing_server_default,
            existing_nullable=existing_nullable,
            existing_autoincrement=existing_autoincrement,
            **kw)

    def autogen_column_reflect(self, inspector, table, column_info):
        if column_info.get('default') and \
                isinstance(column_info['type'], (INTEGER, BIGINT)):
            seq_match = re.match(
                r"nextval\('(.+?)'::regclass\)",
                column_info['default'])
            if seq_match:
                info = inspector.bind.execute(text(
                    "select c.relname, a.attname "
                    "from pg_class as c join pg_depend d on d.objid=c.oid and "
                    "d.classid='pg_class'::regclass and "
                    "d.refclassid='pg_class'::regclass "
                    "join pg_class t on t.oid=d.refobjid "
                    "join pg_attribute a on a.attrelid=t.oid and "
                    "a.attnum=d.refobjsubid "
                    "where c.relkind='S' and c.relname=:seqname"
                ), seqname=seq_match.group(1)).first()
                if info:
                    seqname, colname = info
                    if colname == column_info['name']:
                        log.info(
                            "Detected sequence named '%s' as "
                            "owned by integer column '%s(%s)', "
                            "assuming SERIAL and omitting",
                            seqname, table.name, colname)
                        # sequence, and the owner is this column,
                        # its a SERIAL - whack it!
                        del column_info['default']

    def correct_for_autogen_constraints(self, conn_unique_constraints,
                                        conn_indexes,
                                        metadata_unique_constraints,
                                        metadata_indexes):

        conn_uniques_by_name = dict(
            (c.name, c) for c in conn_unique_constraints)
        conn_indexes_by_name = dict(
            (c.name, c) for c in conn_indexes)

        if not util.sqla_100:
            doubled_constraints = set(
                conn_indexes_by_name[name]
                for name in set(conn_uniques_by_name).intersection(
                    conn_indexes_by_name)
            )
        else:
            doubled_constraints = set(
                index for index in
                conn_indexes if index.info.get('duplicates_constraint')
            )

        for ix in doubled_constraints:
            conn_indexes.remove(ix)

        for idx in list(metadata_indexes):
            if idx.name in conn_indexes_by_name:
                continue
            exprs = idx.expressions
            for expr in exprs:
                while isinstance(expr, UnaryExpression):
                    expr = expr.element
                if not isinstance(expr, Column):
                    util.warn(
                        "autogenerate skipping functional index %s; "
                        "not supported by SQLAlchemy reflection" % idx.name
                    )
                    metadata_indexes.discard(idx)

    def render_type(self, type_, autogen_context):
        mod = type(type_).__module__
        if not mod.startswith("sqlalchemy.dialects.vertica"):
            return False

        if hasattr(self, '_render_%s_type' % type_.__visit_name__):
            meth = getattr(self, '_render_%s_type' % type_.__visit_name__)
            return meth(type_, autogen_context)

        return False

    def _render_HSTORE_type(self, type_, autogen_context):
        return render._render_type_w_subtype(
            type_, autogen_context, 'text_type', r'(.+?\(.*text_type=)'
        )

    def _render_ARRAY_type(self, type_, autogen_context):
        return render._render_type_w_subtype(
            type_, autogen_context, 'item_type', r'(.+?\()'
        )

    def _render_JSON_type(self, type_, autogen_context):
        return render._render_type_w_subtype(
            type_, autogen_context, 'astext_type', r'(.+?\(.*astext_type=)'
        )

    def _render_JSONB_type(self, type_, autogen_context):
        return render._render_type_w_subtype(
            type_, autogen_context, 'astext_type', r'(.+?\(.*astext_type=)'
        )


class VerticaColumnType(AlterColumn):

    def __init__(self, name, column_name, type_, **kw):
        using = kw.pop('using', None)
        super(VerticaColumnType, self).__init__(name, column_name, **kw)
        self.type_ = sqltypes.to_instance(type_)
        self.using = using


@compiles(RenameTable, "vertica")
def visit_rename_table(element, compiler, **kw):
    return "%s RENAME TO %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_table_name(compiler, element.new_table_name, None)
    )


@compiles(VerticaColumnType, "vertica")
def visit_column_type(element, compiler, **kw):
    return "%s %s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        "TYPE %s" % format_type(compiler, element.type_),
        "USING %s" % element.using if element.using else ""
    )

def _vertica_autogenerate_prefix(autogen_context):

    imports = autogen_context.imports
    if imports is not None:
        imports.add("from sqlalchemy.dialects import vertica")
    return "vertica."


def _render_potential_column(value, autogen_context):
    if isinstance(value, ColumnClause):
        template = "%(prefix)scolumn(%(name)r)"

        return template % {
            "prefix": render._sqlalchemy_autogenerate_prefix(autogen_context),
            "name": value.name
        }

    else:
        return render._render_potential_expr(value, autogen_context, wrap_in_text=False)
