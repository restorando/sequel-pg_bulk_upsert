require 'test_helper'

class DatasetExtensionTest < MiniTest::Test

  def setup
    @db = Sequel.
      connect('mock://postgres').
      extension(:pg_bulk_upsert)

    def @db.schema(table)
      if table == :target
        [
          [:id, { primary: true, db_type: :serial }],
          [:updatable_column,  { db_type: :text   }],
          [:insertable_column, { db_type: :text   }]
        ]
      end
    end

    @db.create_table(:target) do
      primary_key :id
      String :updatable_column
      String :insertable_column
    end

    @ds = @db.dataset.from(:target)

    @upsert_columns = [:updatable_column, :insertable_column]
    @upsert_data = [%w[foo bar]]

    # Clear setup sqls
    @db.sqls
  end

  def test_temp_table_creation
    sqls = do_upsert
    temp_table_name   = extract_temp_table_name(sqls[0])

    temp_table_creation = sqls[0]
    assert_equal temp_table_creation, strip_heredoc(<<-SQL).gsub("\n", "")
      CREATE TEMPORARY TABLE "#{temp_table_name}"
       ("id" serial PRIMARY KEY, "updatable_column" text, "insertable_column" text) ON COMMIT DROP
    SQL
  end

  def test_temp_table_batch_loading
    sqls = do_upsert
    temp_table_name   = extract_temp_table_name(sqls[0])
    temp_table_insert = @db.from(temp_table_name).multi_insert_sql(@upsert_columns, @upsert_data)

    assert_equal temp_table_insert, [sqls[2]]
  end

  def test_upsert_from_temp
    sqls = do_upsert
    temp_table_name   = extract_temp_table_name(sqls[0])
    temp_table_insert = strip_heredoc(<<-SQL).gsub("\n", "")
      WITH "update_cte" AS
       (UPDATE "target" SET "updatable_column" = "#{temp_table_name}"."updatable_column"
       FROM "#{temp_table_name}" WHERE ("target"."id" = "#{temp_table_name}"."id") RETURNING "target"."id")
       INSERT INTO "target" (\"updatable_column\", \"insertable_column\")
       SELECT "updatable_column", "insertable_column"
       FROM "#{temp_table_name}" LEFT JOIN "update_cte" USING ("id")
       WHERE ("update_cte"."id" IS NULL)
       RETURNING "target"."id"
    SQL

    assert_equal temp_table_insert, sqls[3]
  end

  def test_upsert_inside_transaction
    sqls = do_upsert

    assert_equal "BEGIN",  sqls[1]
    assert_equal "COMMIT", sqls[4]
  end

  private

  def do_upsert(columns = @upsert_columns, data = @upsert_data)
    @ds.on_duplicate_key_update(:updatable_column).import(columns, data)

    @db.sqls
  end

  def extract_temp_table_name(create_sql)
    create_sql[/CREATE TEMPORARY TABLE "(\w+)"/i, 1]
  end

  def strip_heredoc(heredoc)
    indent = (heredoc.scan(/^[ \t]*(?=\S)/).min || "").size
    heredoc.gsub(/^[ \t]{#{indent}}/, '')
  end

end
