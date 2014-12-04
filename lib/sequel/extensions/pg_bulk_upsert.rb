require "sequel"

module Sequel
  Dataset::NON_SQL_OPTIONS << :on_duplicate_key_update

  module PgBulkUpsert
    def on_duplicate_key_update(*args)
      clone(on_duplicate_key_update: args)
    end

    def multi_insert_sql(columns, values)
      if duplicate_keys = @opts[:on_duplicate_key_update]
        from_table_name = @opts[:from].first # XXX How: can we ensure theres only one?
        temp_table_name = "#{from_table_name}_tmp_#{Time.now.strftime("%s%L")}"
        upsert_on = @db.fetch(<<-SQL).map { |row| row[:attname] }
          SELECT
            pg_attribute.attname
          FROM pg_index, pg_class, pg_attribute
          WHERE
            pg_class.oid = '#{from_table_name}'::regclass AND
            indrelid = pg_class.oid AND
            pg_attribute.attrelid = pg_class.oid AND
            pg_attribute.attnum = any(pg_index.indkey)
            AND indisprimary;
        SQL
        raise "missing primary_key for #{from_table_name}" unless upsert_on.any?

        [
          create_temp_table_from_existing_sql(from_table_name, temp_table_name),
          multi_insert_without_duplicates_sqls(temp_table_name, columns, values),
          upsert_from_to_sql(temp_table_name, from_table_name, upsert_on.map(&:to_sym), duplicate_keys, columns)
        ].flatten
      else
        super
      end
    end

    private

    def multi_insert_without_duplicates_sqls(target_table, columns, values)
      clone(on_duplicate_key_update: nil).
        from(target_table).
        multi_insert_sql(columns, values)
    end

    def upsert_from_to_sql(source_name, target_name, join_on, update_columns, insert_columns)
      target = @db.from(target_name)
      source = @db.from(source_name)

      nil_join_on = join_on.each_with_object({}) do |join_col, hash|
        hash[Sequel.qualify(:update_cte, join_col)] = nil
      end
      source_ds = source.
        select(*insert_columns).
        left_join(:update_cte, join_on).
        where(nil_join_on)

      update_hash = update_columns.each_with_object({}) do |column, hash|
        hash[column] = Sequel.qualify(source_name, column)
      end

      qualified_target_join = join_on.map { |join_col| Sequel.qualify(target_name, join_col) }
      qualified_source_join = join_on.map { |join_col| Sequel.qualify(source_name, join_col) }

      target.with(:update_cte,
        target.from(target_name, source_name).
          where(Hash[qualified_target_join.zip(qualified_source_join)]).
          returning(*qualified_target_join).with_sql(:update_sql, update_hash)).
        returning(*qualified_target_join).insert_sql(insert_columns, source_ds)
    end

    def create_temp_table_from_existing_sql(base_table, temp_table_name)
      columns_information = @db.schema(base_table)

      generator = @db.create_table_generator do
        columns_information.each do |col, data|
          column col, data[:db_type], primary_key: data[:primary]
        end
      end

      @db.send :create_table_sql, temp_table_name, generator, temp: true, on_commit: :drop
    end

  end

  module PgDatabaseBulkUpsert
    def self.extended(db)
      db.extend_datasets(PgBulkUpsert)
    end
  end

  Dataset.register_extension :pg_bulk_upsert, PgBulkUpsert
  Database.register_extension :pg_bulk_upsert, PgDatabaseBulkUpsert
end

