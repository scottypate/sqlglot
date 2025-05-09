from sqlglot import parse_one, transpile
from tests.dialects.test_dialect import Validator


class TestCloudberry(Validator):
    maxDiff = None
    dialect = "cloudberry"

    def test_cloudberry(self):
        # Test basic CREATE EXTERNAL TABLE syntax
        external_table_sql = """
        CREATE EXTERNAL TABLE schema.table (
            household_data json
        ) LOCATION (
            'pxf://some_bucket/some_path/some_file.gz/?PROFILE=s3:text&COMPRESSION_CODEC=gzip'
        )
        FORMAT 'custom' (formatter = 'pxfwritable_import')
        ENCODING 'UTF8'
        """
        
        # Parse and regenerate the SQL to verify it works
        parsed = parse_one(external_table_sql, dialect="cloudberry")
        self.assertIsNotNone(parsed)
        
        # Verify the SQL can be regenerated
        regenerated = parsed.sql(dialect="cloudberry")
        self.assertIn("CREATE EXTERNAL TABLE", regenerated)
        self.assertIn("LOCATION", regenerated)
        self.assertIn("FORMAT", regenerated)
        self.assertIn("ENCODING", regenerated)
        
        # Parse the regenerated SQL to ensure it's valid
        reparsed = parse_one(regenerated, dialect="cloudberry")
        self.assertIsNotNone(reparsed)
        
        # Test transpilation from Cloudberry to PostgreSQL
        postgres_sql = transpile(external_table_sql, read="cloudberry", write="postgres")[0]
        # In PostgreSQL, we keep the EXTERNAL keyword but it's treated as a regular table
        self.assertIn("CREATE TABLE", postgres_sql.replace("EXTERNAL ", ""))
        
        # Test transpilation from PostgreSQL to Cloudberry
        postgres_create_table = """
        CREATE TABLE schema.table (
            household_data json
        )
        """
        cloudberry_sql = transpile(postgres_create_table, read="postgres", write="cloudberry")[0]
        self.assertNotIn("EXTERNAL", cloudberry_sql)
        
    def test_cloudberry_with_on_all(self):
        # Test CREATE EXTERNAL TABLE with ON ALL clause
        external_table_sql = """
        CREATE EXTERNAL TABLE schema.table (
            household_data json
        ) LOCATION (
            'pxf://some_bucket/some_path/some_file.gz/?PROFILE=s3:text&COMPRESSION_CODEC=gzip'
        ) ON ALL
        FORMAT 'custom' (formatter = 'pxfwritable_import')
        ENCODING 'UTF8'
        """
        
        # Parse and regenerate the SQL to verify it works
        parsed = parse_one(external_table_sql, dialect="cloudberry")
        self.assertIsNotNone(parsed)
        
        # Verify the SQL can be regenerated
        regenerated = parsed.sql(dialect="cloudberry")
        self.assertIn("CREATE EXTERNAL TABLE", regenerated)
        self.assertIn("LOCATION", regenerated)
        self.assertIn("ON ALL", regenerated)
        self.assertIn("FORMAT", regenerated)
        self.assertIn("ENCODING", regenerated)
        
        # Parse the regenerated SQL to ensure it's valid
        reparsed = parse_one(regenerated, dialect="cloudberry")
        self.assertIsNotNone(reparsed)
