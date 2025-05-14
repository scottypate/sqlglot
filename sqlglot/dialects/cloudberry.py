from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.postgres import Postgres
from sqlglot.dialects.dialect import Dialect


class Cloudberry(Postgres):
    """Cloudberry dialect.

    Cloudberry is based on PostgreSQL but with small differences, particularly
    in its support for CREATE EXTERNAL TABLE syntax similar to Greenplum.
    """

    class Tokenizer(Postgres.Tokenizer):
        """Cloudberry tokenizer."""

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "EXTERNAL": tokens.TokenType.TEMPORARY,
            "LOCATION": tokens.TokenType.PARTITION,
            "FORMAT": tokens.TokenType.FORMAT,
            "ENCODING": tokens.TokenType.CHARACTER_SET,
            "ON": tokens.TokenType.ON,
            "ALL": tokens.TokenType.ALL,
        }

    class Parser(Postgres.Parser):
        """Cloudberry parser."""

        def __init__(self, *args, **kwargs):
            """Initialize the Cloudberry parser with proper statement parsers setup."""
            super().__init__(*args, **kwargs)
            # Explicitly update the STATEMENT_PARSERS at instance level
            self.STATEMENT_PARSERS = {
                **self.STATEMENT_PARSERS,
                tokens.TokenType.CREATE: lambda self: self._parse_create(),
            }

        def _parse_create(self) -> exp.Create | exp.Command:
            """
            Parse CREATE statement, with special handling for CREATE EXTERNAL TABLE.
            """
            start = self._prev
            
            is_external_table = self._match_text_seq("EXTERNAL") and self._match_text_seq("TABLE")
            
            if is_external_table:
                exists = self._parse_exists(not_=True)
                this = self._parse_table_parts(schema=True)
                this = self._parse_schema(this=this)
                
                if self._match(tokens.TokenType.L_PAREN):
                    expressions = self._parse_csv(lambda: self._parse_column_def(self._parse_id_var()))
                    self._match_r_paren()
                    this.set("expressions", expressions)
                
                properties = []
                
                if self._match_text_seq("LOCATION"):
                    if self._match(tokens.TokenType.L_PAREN):
                        location_value = self._parse_string()
                        self._match_r_paren()
                        properties.append(self.expression(exp.LocationProperty, this=location_value))
                
                on_all = False
                if self._match_text_seq("ON") and self._match_text_seq("ALL"):
                    on_all = True
                    properties.append(self.expression(exp.Property, this="ON ALL", value=self.expression(exp.Literal, this="ON ALL", is_string=True)))
                
                if self._match_text_seq("FORMAT"):
                    format_value = self._parse_string()
                    format_options = None
                    
                    if self._match(tokens.TokenType.L_PAREN):
                        format_options = []
                        while True:
                            if self._match(tokens.TokenType.R_PAREN):
                                break
                            
                            if self._match(tokens.TokenType.COMMA):
                                continue
                            
                            if self._match_text_seq("NULL"):
                                key = "NULL"
                            elif self._match_text_seq("QUOTE"):
                                key = "QUOTE"
                            elif self._match_text_seq("ESCAPE"):
                                key = "ESCAPE"
                            elif self._match_text_seq("NEWLINE"):
                                key = "NEWLINE"
                            elif self._match_text_seq("FILL"):
                                key = "FILL"
                            else:
                                key = self._parse_var()
                                
                            self._match(tokens.TokenType.EQ)
                            value = self._parse_string()
                            
                            key_str = str(key) if hasattr(key, "__str__") else key
                            prop = self.expression(exp.Property, this=key_str, value=value)
                            format_options.append(prop)
                    
                    properties.append(self.expression(exp.FileFormatProperty, this=format_value, expressions=format_options))
                
                if self._match_text_seq("ENCODING"):
                    encoding_value = self._parse_string()
                    properties.append(self.expression(exp.Property, this="ENCODING", value=encoding_value))
                
                properties.append(self.expression(exp.ExternalProperty))
                
                return self.expression(
                    exp.Create,
                    this=this,
                    kind="TABLE",
                    exists=exists,
                    properties=self.expression(exp.Properties, expressions=properties) if properties else None,
                )
            
            return super()._parse_create()

    class Generator(Postgres.Generator):
        """Cloudberry generator."""
        
        def sql(self, expression, key=None, comment=True):
            """Override sql method to handle boolean values."""
            if isinstance(expression, bool):
                return self.TRUE_LITERAL if expression else self.FALSE_LITERAL
            return super().sql(expression, key, comment)
        
        def property_sql(self, expression: exp.Property) -> str:
            """Generate SQL for a property, with special handling for ON ALL."""
            if expression.name == "ON ALL" and isinstance(expression.value, exp.Literal) and expression.value.this == "ON ALL":
                return "ON ALL"
            return super().property_sql(expression)

        def create_sql(self, expression: exp.Create) -> str:
            """
            Generate SQL for CREATE statements, with special handling for CREATE EXTERNAL TABLE.
            """
            is_external_table = (
                expression.kind == "TABLE"
                and expression.args.get("properties")
                and any(isinstance(prop, exp.ExternalProperty) for prop in expression.args["properties"].expressions)
            )

            if is_external_table:
                this_part = self.sql(expression, "this")
                
                exists_clause = "IF NOT EXISTS " if expression.args.get("exists") else ""
                replace_clause = "OR REPLACE " if expression.args.get("replace") else ""

                location_clause_str = ""
                on_all_clause_str = ""
                format_clause_str = ""
                encoding_clause_str = ""

                if expression.args.get("properties"):
                    for prop in expression.args["properties"].expressions:
                        if isinstance(prop, exp.LocationProperty):
                            loc_uri_sql = self.sql(prop, 'this')
                            location_clause_str = f"LOCATION ({loc_uri_sql})"
                        elif isinstance(prop, exp.Property) and prop.name == "ON ALL":
                            on_all_clause_str = "ON ALL"
                        elif isinstance(prop, exp.FileFormatProperty):
                            format_name_sql = self.sql(prop, 'this')
                            format_clause_str = f"FORMAT {format_name_sql}"
                            if prop.expressions:
                                format_options_sql = ", ".join(self.sql(option) for option in prop.expressions)
                                format_clause_str += f" ({format_options_sql})"
                        elif isinstance(prop, exp.Property) and prop.name.upper() == "ENCODING":
                            encoding_value_sql = self.sql(prop, 'value')
                            encoding_clause_str = f"ENCODING {encoding_value_sql}"
                        # ExternalProperty is a marker, not directly rendered here.
                        # Other properties are ignored for CREATE EXTERNAL TABLE.
                
                clauses = []
                if location_clause_str: clauses.append(location_clause_str)
                if on_all_clause_str: clauses.append(on_all_clause_str)
                if format_clause_str: clauses.append(format_clause_str)
                if encoding_clause_str: clauses.append(encoding_clause_str)
                
                clauses_sql = " ".join(clauses)

                return f"CREATE EXTERNAL TABLE {replace_clause}{exists_clause}{this_part} {clauses_sql}".strip()

            return super().create_sql(expression)

# --- Minimal patch to ensure SQLMesh uses Cloudberry for 'postgres' connections ---
# Cloudberry class is defined above and auto-registered under "cloudberry".
# This makes "postgres" also point to our Cloudberry class.
try:
    from sqlglot.dialects.dialect import Dialect
    dialect_registry = Dialect.classes
    if isinstance(dialect_registry, dict):
        dialect_registry["postgres"] = Cloudberry
except Exception:
    # Silently fail if patching doesn't work in some edge case,
    # though it's crucial for the user's current setup.
    # Consider logging to a standard logger if this were a library.
    pass
