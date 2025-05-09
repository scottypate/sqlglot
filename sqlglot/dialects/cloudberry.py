"""Cloudberry dialect.

Cloudberry is based on PostgreSQL but with small differences, particularly
in its support for CREATE EXTERNAL TABLE syntax similar to Greenplum.
"""

from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.postgres import Postgres


class Cloudberry(Postgres):
    """Cloudberry dialect.

    Cloudberry is based on PostgreSQL but with small differences, particularly
    in its support for CREATE EXTERNAL TABLE syntax similar to Greenplum.
    """

    class Tokenizer(Postgres.Tokenizer):
        """Cloudberry tokenizer."""

        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "EXTERNAL": tokens.TokenType.TEMPORARY,  # Reuse TEMPORARY token type for EXTERNAL
            "LOCATION": tokens.TokenType.PARTITION,  # Reuse PARTITION token type for LOCATION
            "FORMAT": tokens.TokenType.FORMAT,
            "ENCODING": tokens.TokenType.CHARACTER_SET,  # Reuse CHARACTER_SET token type for ENCODING
            "ON": tokens.TokenType.ON,
            "ALL": tokens.TokenType.ALL,
        }

    class Parser(Postgres.Parser):
        """Cloudberry parser."""

        def _parse_create(self) -> exp.Create | exp.Command:
            """
            Parse CREATE statement, with special handling for CREATE EXTERNAL TABLE.
            """
            start = self._prev
            
            # Check if this is CREATE EXTERNAL TABLE
            if self._match_text_seq("EXTERNAL") and self._match_text_seq("TABLE"):
                # This is a CREATE EXTERNAL TABLE statement
                exists = self._parse_exists(not_=True)
                this = self._parse_table_parts(schema=True)
                this = self._parse_schema(this=this)
                
                # Parse column definitions
                if self._match(parser.TokenType.L_PAREN):
                    expressions = self._parse_csv(lambda: self._parse_column_def(self._parse_id_var()))
                    self._match_r_paren()
                    this.set("expressions", expressions)
                
                # Parse properties
                properties = []
                
                # Parse LOCATION clause
                if self._match_text_seq("LOCATION"):
                    if self._match(parser.TokenType.L_PAREN):
                        location_value = self._parse_string()
                        self._match_r_paren()
                        properties.append(self.expression(exp.LocationProperty, this=location_value))
                
                # Parse ON ALL clause if present
                on_all = False
                if self._match_text_seq("ON") and self._match_text_seq("ALL"):
                    on_all = True
                    properties.append(self.expression(exp.Property, this="ON ALL", value=True))
                
                # Parse FORMAT clause
                if self._match_text_seq("FORMAT"):
                    format_value = self._parse_string()
                    format_options = None
                    
                    # Parse format options if present
                    if self._match(parser.TokenType.L_PAREN):
                        format_options = []
                        while True:
                            if self._match(parser.TokenType.R_PAREN):
                                break
                            
                            if self._match(parser.TokenType.COMMA):
                                continue
                            
                            key = self._parse_var()
                            self._match(parser.TokenType.EQ)
                            value = self._parse_string()
                            format_options.append(self.expression(exp.Property, this=key, value=value))
                    
                    properties.append(self.expression(exp.FileFormatProperty, this=format_value, expressions=format_options))
                
                # Parse ENCODING clause
                if self._match_text_seq("ENCODING"):
                    encoding_value = self._parse_string()
                    properties.append(self.expression(exp.Property, this="ENCODING", value=encoding_value))
                
                # Add an ExternalProperty to the properties list
                properties.append(self.expression(exp.ExternalProperty))
                
                # Create the final expression
                return self.expression(
                    exp.Create,
                    this=this,
                    kind="TABLE",
                    exists=exists,
                    properties=self.expression(exp.Properties, expressions=properties) if properties else None,
                )
            
            # If not CREATE EXTERNAL TABLE, use the standard PostgreSQL CREATE parsing
            return super()._parse_create()

    class Generator(Postgres.Generator):
        """Cloudberry generator."""

        def create_sql(self, expression: exp.Create) -> str:
            """
            Generate SQL for CREATE statements, with special handling for CREATE EXTERNAL TABLE.
            """
            if (expression.kind == "TABLE" and expression.args.get("properties") and 
                any(isinstance(prop, exp.ExternalProperty) for prop in expression.args["properties"].expressions)):
                # Handle CREATE EXTERNAL TABLE
                this = self.sql(expression, "this")
                exists = "IF NOT EXISTS " if expression.args.get("exists") else ""
                replace = "OR REPLACE " if expression.args.get("replace") else ""

                # Generate schema definition
                schema = ""
                if isinstance(expression.this, exp.Schema) and expression.this.expressions:
                    schema = f"({self.expressions(expression.this, key='expressions')})"

                # Generate properties
                props = []
                has_on_all = False
                
                if expression.args.get("properties"):
                    for prop in expression.args["properties"].expressions:
                        if isinstance(prop, exp.LocationProperty):
                            props.append(f"LOCATION ({self.sql(prop, 'this')})")
                        elif isinstance(prop, exp.Property) and prop.name == "ON ALL":
                            has_on_all = True
                        elif isinstance(prop, exp.FileFormatProperty):
                            format_str = f"FORMAT {self.sql(prop, 'this')}"
                            if prop.expressions:
                                format_options = ", ".join(self.sql(option) for option in prop.expressions)
                                format_str += f" ({format_options})"
                            props.append(format_str)
                        elif isinstance(prop, exp.Property) and prop.name.upper() == "ENCODING":
                            props.append(f"ENCODING {self.sql(prop, 'value')}")
                        elif not isinstance(prop, exp.ExternalProperty):  # Skip ExternalProperty
                            props.append(self.sql(prop))
                
                # Insert ON ALL after LOCATION if needed
                if has_on_all:
                    for i, prop in enumerate(props):
                        if prop.startswith("LOCATION"):
                            props.insert(i+1, "ON ALL")
                            break

                props_sql = " ".join(props)

                # Make sure we don't repeat the schema
                table_parts = this.split(' ', 1)
                table_name = table_parts[0]
                
                return f"CREATE EXTERNAL TABLE {replace}{exists}{table_name} {schema} {props_sql}"

            # For other CREATE statements, use the standard PostgreSQL generator
            return super().create_sql(expression)
